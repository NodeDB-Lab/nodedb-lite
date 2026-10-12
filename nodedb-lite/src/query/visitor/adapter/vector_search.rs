// SPDX-License-Identifier: Apache-2.0

//! Vector search lowering: resolves the optional `ArrayPrefilter` to a
//! surrogate bitmap, encodes `MetadataFilter`s from SQL filters + payload
//! atoms, then dispatches to `crate::engine::vector::search::run_vector_search`.

use nodedb_array::query::slice::{DimRange, Slice};
use nodedb_array::schema::dim_spec::DimType;
use nodedb_array::types::domain::DomainBound;
use nodedb_sql::VectorSearchVisitArgs;
use nodedb_sql::types::plan::ArrayPrefilter;
use nodedb_sql::types_array::ArrayCoordLiteral;
use nodedb_types::result::QueryResult;

use crate::engine::vector::search::{
    AnnTuning, SearchFilter, VectorSearchRequest, run_vector_search,
};
use crate::error::LiteError;
use crate::query::engine::LiteQueryEngine;
use crate::query::filter_convert::sql_filters_to_metadata;
use crate::query::physical_visitor::execute_surrogate_scan;
use crate::storage::engine::StorageEngine;

use super::visitor::LiteFut;

/// Coerce an `ArrayCoordLiteral` to a `DomainBound` using the declared `DimType`.
fn coerce_literal(lit: &ArrayCoordLiteral, dtype: DimType) -> Result<DomainBound, LiteError> {
    match (lit, dtype) {
        (ArrayCoordLiteral::Int64(v), DimType::Int64 | DimType::TimestampMs) => {
            Ok(DomainBound::Int64(*v))
        }
        (ArrayCoordLiteral::Float64(v), DimType::Float64) => Ok(DomainBound::Float64(*v)),
        (ArrayCoordLiteral::String(v), DimType::String) => Ok(DomainBound::String(v.clone())),
        (ArrayCoordLiteral::Int64(v), DimType::Float64) => Ok(DomainBound::Float64(*v as f64)),
        _ => Err(LiteError::BadRequest {
            detail: format!(
                "array prefilter: literal {:?} incompatible with dim type {:?}",
                lit, dtype
            ),
        }),
    }
}

/// Build a `RoaringBitmap` from an `ArrayPrefilter` by running a surrogate
/// scan against the array engine. Returns `None` when `prefilter` is `None`.
async fn build_prefilter_bitmap<S: StorageEngine>(
    engine: &LiteQueryEngine<S>,
    prefilter: Option<&ArrayPrefilter>,
) -> Result<Option<roaring::RoaringBitmap>, LiteError> {
    let prefilter = match prefilter {
        Some(p) => p,
        None => return Ok(None),
    };

    // Resolve named dim ranges to positional Vec<Option<DimRange>> using the
    // array schema stored in the engine's array_state catalog.
    let slice_msgpack = {
        let state = engine.array_state.lock().await;
        let array_state =
            state
                .arrays
                .get(&prefilter.array_name)
                .ok_or_else(|| LiteError::BadRequest {
                    detail: format!(
                        "array prefilter: array '{}' not found",
                        prefilter.array_name
                    ),
                })?;
        let schema = array_state.schema.clone();
        let ndims = schema.dims.len();
        let mut dim_ranges: Vec<Option<DimRange>> = vec![None; ndims];
        for named in &prefilter.slice.dim_ranges {
            let idx = schema
                .dims
                .iter()
                .position(|d| d.name == named.dim)
                .ok_or_else(|| LiteError::BadRequest {
                    detail: format!(
                        "array prefilter: array '{}' has no dim '{}'",
                        prefilter.array_name, named.dim
                    ),
                })?;
            let dtype = schema.dims[idx].dtype;
            let lo = coerce_literal(&named.lo, dtype)?;
            let hi = coerce_literal(&named.hi, dtype)?;
            dim_ranges[idx] = Some(DimRange::new(lo, hi));
        }
        let slice = Slice::new(dim_ranges);
        zerompk::to_msgpack_vec(&slice).map_err(|e| LiteError::Serialization {
            detail: format!("encode prefilter slice: {e}"),
        })?
    };

    let bitmap = execute_surrogate_scan(
        &engine.array_state,
        &engine.storage,
        &prefilter.array_name,
        &slice_msgpack,
    )
    .await?;
    Ok(Some(bitmap))
}

pub(super) fn lower_vector_search<'a, S: StorageEngine + 'a>(
    engine: &'a LiteQueryEngine<S>,
    args: VectorSearchVisitArgs<'_>,
) -> Result<LiteFut<'a>, LiteError> {
    let VectorSearchVisitArgs {
        collection,
        field,
        query_vector,
        top_k,
        ef_search,
        metric,
        filters,
        array_prefilter,
        ann_options,
        skip_payload_fetch,
        payload_filters,
        pk_prefilter,
    } = args;
    if pk_prefilter.is_some() {
        return Err(LiteError::Unsupported {
            detail: format!(
                "vector search on '{collection}' with a primary-key prefilter (pk_prefilter): \
                 Lite ranks every row of the vector index"
            ),
        });
    }
    let prefilter = array_prefilter.cloned();
    // Complex QExpr predicates from FilterExpr::Expr are not serializable to the
    // vector search pre-filter; only the primitive MetadataFilter is pushed down.
    // Complex predicates are applied post-search by apply_scan_post_processing.
    let rls_filters = match sql_filters_to_metadata(filters, payload_filters)?.meta {
        None => Vec::new(),
        Some(mf) => zerompk::to_msgpack_vec(&mf).map_err(|e| LiteError::Serialization {
            detail: format!("encode MetadataFilter: {e}"),
        })?,
    };
    let collection = collection.to_string();
    let field = field.to_string();
    let query_vector = query_vector.to_vec();
    let ann_options = ann_options.to_runtime();
    Ok(Box::pin(async move {
        let prefilter_bitmap = build_prefilter_bitmap(engine, prefilter.as_ref()).await?;
        let index_key = if field.is_empty() {
            collection.clone()
        } else {
            format!("{collection}:{field}")
        };
        let metadata_filter: Option<nodedb_types::filter::MetadataFilter> =
            if rls_filters.is_empty() {
                None
            } else {
                Some(
                    zerompk::from_msgpack(&rls_filters).map_err(|e| LiteError::Serialization {
                        detail: format!("decode MetadataFilter: {e}"),
                    })?,
                )
            };
        let results = run_vector_search(
            &engine.vector_state,
            &engine.crdt,
            VectorSearchRequest {
                index_key: &index_key,
                collection: &collection,
                query: &query_vector,
                k: top_k,
                filter: SearchFilter {
                    metadata: metadata_filter.as_ref(),
                    exclude_fields: &[],
                    prefilter_bitmap: prefilter_bitmap.as_ref(),
                },
                tuning: AnnTuning {
                    options: Some(&ann_options),
                    metric: Some(metric),
                    ef_search: Some(ef_search),
                },
                skip_payload_fetch,
            },
        )
        .await
        .map_err(|e| LiteError::Query(e.to_string()))?;

        let columns = vec!["id".to_string(), "distance".to_string()];
        let rows: Vec<Vec<nodedb_types::value::Value>> = results
            .into_iter()
            .map(|r| {
                vec![
                    nodedb_types::value::Value::String(r.id),
                    nodedb_types::value::Value::Float(r.distance as f64),
                ]
            })
            .collect();
        Ok(QueryResult {
            columns,
            rows,
            rows_affected: 0,
            command: None,
        })
    }))
}
