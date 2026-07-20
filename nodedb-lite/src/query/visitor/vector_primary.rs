// SPDX-License-Identifier: Apache-2.0
//! SQL-visitor lowering for vector-primary SqlPlan variants: VectorPrimaryInsert.

use nodedb_physical::PhysicalTaskVisitor;
use nodedb_physical::physical_plan::VectorOp;
use nodedb_sql::types::plan::VectorPrimaryRow;
use nodedb_sql::types_expr::SqlValue;
use nodedb_types::value::Value;
use nodedb_types::{PayloadIndexKind, VectorQuantization, VectorStorageDtype};

use crate::error::LiteError;
use crate::query::engine::LiteQueryEngine;
use crate::query::filter_convert::sql_value_to_value;
use crate::query::physical_visitor::LiteDataPlaneVisitor;
use crate::storage::engine::StorageEngine;

use super::adapter::LiteFut;

/// Encode payload fields (non-vector columns) as MessagePack bytes.
fn encode_payload(
    payload_fields: &std::collections::HashMap<String, SqlValue>,
) -> Result<Vec<u8>, LiteError> {
    if payload_fields.is_empty() {
        return Ok(Vec::new());
    }
    let value_map: std::collections::HashMap<String, Value> = payload_fields
        .iter()
        .map(|(k, sv)| Ok((k.clone(), sql_value_to_value(sv)?)))
        .collect::<Result<_, LiteError>>()?;
    zerompk::to_msgpack_vec(&value_map).map_err(|e| LiteError::Serialization {
        detail: format!("encode vector primary payload: {e}"),
    })
}

// ── VectorPrimaryInsert ───────────────────────────────────────────────────────

/// Lower `SqlPlan::VectorPrimaryInsert` to repeated `VectorOp::DirectUpsert`.
///
/// Each row in `rows` becomes one `DirectUpsert`. Lite processes them
/// sequentially — there is no batch allocator; each upsert is idempotent
/// and the last write wins on duplicate surrogate.
pub(super) fn lower_vector_primary_insert<'a, S: StorageEngine + 'a>(
    engine: &'a LiteQueryEngine<S>,
    collection: &str,
    field: &str,
    quantization: &VectorQuantization,
    storage_dtype: &VectorStorageDtype,
    payload_indexes: &[(String, PayloadIndexKind)],
    rows: &[VectorPrimaryRow],
) -> Result<LiteFut<'a>, LiteError> {
    // Encode each row's payload at plan time so the async body is clean.
    struct EncodedRow {
        surrogate: nodedb_types::Surrogate,
        vector: Vec<f32>,
        payload: Vec<u8>,
    }

    let encoded_rows: Vec<EncodedRow> = rows
        .iter()
        .map(|row| {
            let payload = encode_payload(&row.payload_fields)?;
            Ok(EncodedRow {
                surrogate: row.surrogate,
                vector: row.vector.clone(),
                payload,
            })
        })
        .collect::<Result<Vec<_>, LiteError>>()?;

    let collection = collection.to_string();
    let field = field.to_string();
    let quantization = *quantization;
    let storage_dtype = *storage_dtype;
    let payload_indexes = payload_indexes.to_vec();

    Ok(Box::pin(async move {
        use nodedb_types::result::QueryResult;
        let mut rows_affected = 0usize;
        for row in encoded_rows {
            let op = VectorOp::DirectUpsert {
                collection: collection.clone(),
                field: field.clone(),
                surrogate: row.surrogate,
                vector: row.vector,
                payload: row.payload,
                quantization,
                storage_dtype,
                payload_indexes: payload_indexes.clone(),
            };
            let mut phys = LiteDataPlaneVisitor { engine };
            let fut = phys.vector(&op)?;
            fut.await?;
            rows_affected += 1;
        }
        Ok(QueryResult {
            columns: vec!["rows_affected".to_string()],
            rows: vec![vec![Value::Integer(rows_affected as i64)]],
            rows_affected: rows_affected as u64,
        })
    }))
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::sync::Arc;

    use nodedb_sql::types::plan::VectorPrimaryRow;
    use nodedb_types::{Surrogate, VectorQuantization, VectorStorageDtype};

    use crate::PagedbStorageMem;
    use crate::query::engine::LiteQueryEngine;

    async fn make_engine() -> LiteQueryEngine<PagedbStorageMem> {
        use std::sync::Mutex;
        let storage = Arc::new(
            PagedbStorageMem::open_in_memory()
                .await
                .expect("in-memory pagedb"),
        );
        let crdt = Arc::new(Mutex::new(
            crate::engine::crdt::CrdtEngine::new(1).expect("crdt"),
        ));
        let strict = Arc::new(crate::engine::strict::StrictEngine::new(Arc::clone(
            &storage,
        )));
        let columnar = Arc::new(crate::engine::columnar::ColumnarEngine::new(Arc::clone(
            &storage,
        )));
        let htap = Arc::new(crate::engine::htap::HtapBridge::new());
        let timeseries = Arc::new(Mutex::new(
            crate::engine::timeseries::engine::TimeseriesEngine::new(),
        ));
        let vector_state = Arc::new(crate::engine::vector::VectorState::new(
            Arc::clone(&storage),
            100,
        ));
        let array_state = Arc::new(tokio::sync::Mutex::new(
            crate::engine::array::engine::ArrayEngineState::open(&storage)
                .await
                .expect("array"),
        ));
        let fts_state = Arc::new(crate::engine::fts::FtsState::new());
        let spatial = Arc::new(Mutex::new(
            crate::engine::spatial::SpatialIndexManager::new(),
        ));
        LiteQueryEngine::new(
            crdt,
            strict,
            columnar,
            htap,
            storage,
            timeseries,
            vector_state,
            array_state,
            fts_state,
            Arc::new(crate::engine::sparse_vector::SparseVectorState::new()),
            spatial,
            Arc::new(Mutex::new(std::collections::HashMap::new())),
        )
    }

    #[tokio::test]
    async fn test_vector_primary_insert_single_row() {
        let engine = make_engine().await;
        let rows = vec![VectorPrimaryRow {
            surrogate: Surrogate(1u32),
            vector: vec![0.1f32, 0.2, 0.3, 0.4],
            payload_fields: HashMap::new(),
        }];
        let result = super::lower_vector_primary_insert(
            &engine,
            "embeddings",
            "vec",
            &VectorQuantization::None,
            &VectorStorageDtype::F32,
            &[],
            &rows,
        );
        assert!(result.is_ok());
        let qr = result.unwrap().await.expect("insert should succeed");
        assert_eq!(qr.rows_affected, 1);
    }

    #[tokio::test]
    async fn test_vector_primary_insert_multiple_rows() {
        let engine = make_engine().await;
        let rows: Vec<VectorPrimaryRow> = (1..=3u32)
            .map(|i| VectorPrimaryRow {
                surrogate: Surrogate(i),
                vector: vec![i as f32 * 0.1, i as f32 * 0.2],
                payload_fields: HashMap::new(),
            })
            .collect();
        let result = super::lower_vector_primary_insert(
            &engine,
            "embeddings",
            "emb",
            &VectorQuantization::None,
            &VectorStorageDtype::F32,
            &[],
            &rows,
        );
        assert!(result.is_ok());
        let qr = result.unwrap().await.expect("batch insert should succeed");
        assert_eq!(qr.rows_affected, 3);
    }
}
