// SPDX-License-Identifier: Apache-2.0

//! `TRUNCATE` shared by the SQL visitor and the physical-plan adapters.
//!
//! One engine-level clear per engine lives in its `*_ops` module. This
//! module holds the pieces every entry point shares: the bare `TRUNCATE`
//! result, the `RESTART IDENTITY` sequence reset, and the dispatch from a
//! planner `EngineType` to the engine clear.

use nodedb_sql::types::query::EngineType;
use nodedb_types::result::QueryResult;

use crate::error::LiteError;
use crate::query::engine::LiteQueryEngine;
use crate::query::{columnar_ops, document_ops, kv_ops, timeseries_ops};
use crate::storage::engine::StorageEngine;

/// The result every `TRUNCATE` answers with. A SQL `TRUNCATE` reports no
/// row count: Postgres tags it bare, so `rows_affected` stays zero.
pub(crate) fn truncated() -> QueryResult {
    QueryResult {
        columns: Vec::new(),
        rows: Vec::new(),
        rows_affected: 0,
        command: Some("TRUNCATE".into()),
    }
}

/// Apply `RESTART IDENTITY`: every sequence named `<collection>_<field>_seq`
/// restarts at its start value. A no-op when `restart_identity` is false.
pub(crate) fn restart_identity<S: StorageEngine>(
    engine: &LiteQueryEngine<S>,
    collection: &str,
    restart_identity: bool,
) {
    if restart_identity {
        engine.sequences.restart_collection_sequences(collection);
    }
}

/// Empty every R-tree entry `collection` owns. Each removed entry is staged
/// as a delete on the spatial outbound queue, the path `SpatialOp::Delete`
/// takes, so Origin drops the same entries.
pub(crate) fn clear_spatial<S: StorageEngine>(
    engine: &LiteQueryEngine<S>,
    collection: &str,
) -> Result<(), LiteError> {
    let removed = engine
        .spatial
        .lock()
        .map_err(|_| LiteError::LockPoisoned)?
        .truncate_collection(collection);
    #[cfg(not(target_arch = "wasm32"))]
    if let Some(q) = &engine.spatial_outbound {
        for (field, doc_id) in &removed {
            q.stage_delete(collection, field, doc_id);
        }
    }
    #[cfg(target_arch = "wasm32")]
    drop(removed);
    Ok(())
}

/// The message the planner refuses `TRUNCATE` on an array with. Reached
/// only when a plan names the array engine directly.
const ARRAY_REFUSAL: &str = "TRUNCATE is not supported on the array engine: \
     use DROP ARRAY <name> to remove the array, or \
     DELETE FROM ARRAY <name> WHERE COORDS IN (...) to remove cells";

/// Clear `collection` through the engine `engine_type` names. Exhaustive
/// over `EngineType`: a new engine is a compile error here.
pub(crate) async fn truncate_engine<S: StorageEngine>(
    engine: &LiteQueryEngine<S>,
    collection: &str,
    engine_type: EngineType,
) -> Result<QueryResult, LiteError> {
    match engine_type {
        EngineType::DocumentSchemaless | EngineType::DocumentStrict => {
            document_ops::writes::truncate(engine, collection).await
        }
        EngineType::KeyValue => kv_ops::writes::kv_truncate(engine, collection).await,
        // Lite keeps spatial rows in the columnar engine under the spatial
        // profile; the columnar clear also empties the R-tree entries.
        EngineType::Columnar | EngineType::Spatial => {
            columnar_ops::writes::truncate(engine, collection).await
        }
        EngineType::Timeseries => timeseries_ops::truncate::truncate(engine, collection).await,
        EngineType::Array => Err(LiteError::Unsupported {
            detail: ARRAY_REFUSAL.into(),
        }),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::query::engine::test_engine;
    use crate::sequence::LiteSequenceDef;

    fn seq(name: &str) -> LiteSequenceDef {
        LiteSequenceDef {
            name: name.to_string(),
            start_value: 10,
            increment: 1,
            min_value: 1,
            max_value: i64::MAX,
            cycle: false,
        }
    }

    #[test]
    fn truncated_is_the_bare_tag() {
        let r = truncated();
        assert_eq!(r.rows_affected, 0);
        assert_eq!(r.command.as_deref(), Some("TRUNCATE"));
        assert!(r.columns.is_empty() && r.rows.is_empty());
    }

    #[tokio::test]
    async fn restart_identity_is_gated_on_the_flag() {
        let engine = test_engine().await;
        engine.sequences.register(seq("t_id_seq"));
        engine.sequences.nextval("t_id_seq").expect("nextval");
        engine.sequences.nextval("t_id_seq").expect("nextval");

        restart_identity(&engine, "t", false);
        assert_eq!(engine.sequences.nextval("t_id_seq").expect("nextval"), 12);

        restart_identity(&engine, "t", true);
        assert_eq!(engine.sequences.nextval("t_id_seq").expect("nextval"), 10);
    }

    #[tokio::test]
    async fn array_engine_is_refused_naming_drop_array() {
        let engine = test_engine().await;
        let err = truncate_engine(&engine, "grid", EngineType::Array)
            .await
            .expect_err("array truncate is refused");
        assert!(err.to_string().contains("DROP ARRAY"), "{err}");
    }
}
