// SPDX-License-Identifier: Apache-2.0
//! `PlanVisitor` impl for Lite — supported variants delegate to LiteQueryEngine helpers;
//! adding a new SqlPlan variant is a hard compile error here.

use std::future::Future;
use std::pin::Pin;

use nodedb_physical::PhysicalTaskVisitor;
use nodedb_physical::physical_plan::TextOp;
use nodedb_sql::PlanVisitor;
use nodedb_sql::fts_types::FtsQuery;
use nodedb_sql::temporal::TemporalScope;
use nodedb_sql::types::SqlValue;
use nodedb_sql::types::filter::Filter;
use nodedb_sql::types::plan::VectorAnnOptions;
use nodedb_sql::types::query::{EngineType, Projection, SortKey, WindowSpec};
use nodedb_sql::types_expr::{SqlExpr, SqlPayloadAtom};
use nodedb_types::result::QueryResult;
use nodedb_types::vector_distance::DistanceMetric;

use nodedb_array::query::slice::{DimRange, Slice};
use nodedb_array::schema::dim_spec::DimType;
use nodedb_array::types::domain::DomainBound;
use nodedb_sql::types_array::ArrayCoordLiteral;

use crate::query::filter_convert::sql_filters_to_metadata;
use crate::query::physical_visitor::execute_surrogate_scan;

use crate::error::LiteError;
use crate::query::physical_visitor::LiteDataPlaneVisitor;
use crate::storage::engine::{StorageEngine, StorageEngineSync};

use super::unsupported::impl_unsupported_lite_visitor_methods;
use crate::query::engine::LiteQueryEngine;

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
async fn build_prefilter_bitmap<S: StorageEngine + StorageEngineSync>(
    engine: &LiteQueryEngine<S>,
    prefilter: Option<&nodedb_sql::types::plan::ArrayPrefilter>,
) -> Result<Option<roaring::RoaringBitmap>, LiteError> {
    let prefilter = match prefilter {
        Some(p) => p,
        None => return Ok(None),
    };

    // Resolve named dim ranges to positional Vec<Option<DimRange>> using the
    // array schema stored in the engine's array_state catalog.
    let slice_msgpack = {
        let state = engine
            .array_state
            .lock()
            .map_err(|_| LiteError::LockPoisoned)?;
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
    )?;
    Ok(Some(bitmap))
}

pub(crate) type LiteFut<'a> =
    Pin<Box<dyn Future<Output = Result<QueryResult, LiteError>> + Send + 'a>>;

pub(crate) struct LiteVisitor<'a, S: StorageEngine + StorageEngineSync> {
    pub(crate) engine: &'a LiteQueryEngine<S>,
}

fn unsupported_fut<'a>(name: &'static str) -> LiteFut<'a> {
    Box::pin(async move {
        Err(LiteError::Unsupported {
            detail: format!("Lite executor does not yet implement SqlPlan::{name}"),
        })
    })
}

macro_rules! u {
    ($name:literal) => {
        Ok(unsupported_fut($name))
    };
}

impl<'a, S: StorageEngine + StorageEngineSync + 'a> PlanVisitor for LiteVisitor<'a, S> {
    type Output = LiteFut<'a>;
    type Error = LiteError;

    fn constant_result(
        &mut self,
        columns: &[String],
        values: &[SqlValue],
    ) -> Result<LiteFut<'a>, LiteError> {
        let columns = columns.to_vec();
        let values = values.to_vec();
        let engine = self.engine;
        Ok(Box::pin(async move {
            engine.execute_constant_result(&columns, &values).await
        }))
    }

    fn scan(
        &mut self,
        collection: &str,
        _alias: Option<&str>,
        engine_type: EngineType,
        filters: &[Filter],
        _projection: &[Projection],
        sort_keys: &[SortKey],
        limit: Option<usize>,
        offset: usize,
        distinct: bool,
        window_functions: &[WindowSpec],
        _temporal: &TemporalScope,
    ) -> Result<LiteFut<'a>, LiteError> {
        let collection = collection.to_string();
        let filters = filters.to_vec();
        let sort_keys = sort_keys.to_vec();
        let window_functions = window_functions.to_vec();
        let engine = self.engine;
        Ok(Box::pin(async move {
            let raw = engine.execute_scan(&collection, &engine_type).await?;
            super::scan_post::apply_scan_post_processing(
                raw,
                &filters,
                &sort_keys,
                &window_functions,
                limit,
                offset,
                distinct,
            )
        }))
    }

    fn point_get(
        &mut self,
        collection: &str,
        _alias: Option<&str>,
        engine_type: EngineType,
        _key_column: &str,
        key_value: &SqlValue,
    ) -> Result<LiteFut<'a>, LiteError> {
        let collection = collection.to_string();
        let key_value = key_value.clone();
        let engine = self.engine;
        Ok(Box::pin(async move {
            engine
                .execute_point_get(&collection, &engine_type, &key_value)
                .await
        }))
    }

    fn insert(
        &mut self,
        collection: &str,
        engine_type: EngineType,
        rows: &[Vec<(String, SqlValue)>],
        _column_defaults: &[(String, String)],
        if_absent: bool,
        _column_schema: &[(String, String)],
    ) -> Result<LiteFut<'a>, LiteError> {
        let collection = collection.to_string();
        let rows = rows.to_vec();
        let engine = self.engine;
        Ok(Box::pin(async move {
            engine
                .execute_insert(&collection, &engine_type, &rows, if_absent)
                .await
        }))
    }

    fn upsert(
        &mut self,
        collection: &str,
        engine_type: EngineType,
        rows: &[Vec<(String, SqlValue)>],
        _column_defaults: &[(String, String)],
        _on_conflict_updates: &[(String, SqlExpr)],
        _column_schema: &[(String, String)],
    ) -> Result<LiteFut<'a>, LiteError> {
        let collection = collection.to_string();
        let rows = rows.to_vec();
        let engine = self.engine;
        Ok(Box::pin(async move {
            engine
                .execute_insert(&collection, &engine_type, &rows, true)
                .await
        }))
    }

    fn update(
        &mut self,
        collection: &str,
        engine_type: EngineType,
        assignments: &[(String, SqlExpr)],
        _filters: &[Filter],
        target_keys: &[SqlValue],
        _returning: bool,
    ) -> Result<LiteFut<'a>, LiteError> {
        let collection = collection.to_string();
        let assignments = assignments.to_vec();
        let target_keys = target_keys.to_vec();
        let engine = self.engine;
        Ok(Box::pin(async move {
            engine
                .execute_update(&collection, &engine_type, &assignments, &target_keys)
                .await
        }))
    }

    fn delete(
        &mut self,
        collection: &str,
        engine_type: EngineType,
        _filters: &[Filter],
        target_keys: &[SqlValue],
    ) -> Result<LiteFut<'a>, LiteError> {
        let collection = collection.to_string();
        let target_keys = target_keys.to_vec();
        let engine = self.engine;
        Ok(Box::pin(async move {
            engine
                .execute_delete(&collection, &engine_type, &target_keys)
                .await
        }))
    }

    fn truncate(
        &mut self,
        collection: &str,
        _restart_identity: bool,
    ) -> Result<LiteFut<'a>, LiteError> {
        let collection = collection.to_string();
        let engine = self.engine;
        Ok(Box::pin(async move {
            engine.execute_truncate(&collection).await
        }))
    }

    fn vector_search(
        &mut self,
        collection: &str,
        field: &str,
        query_vector: &[f32],
        top_k: usize,
        ef_search: usize,
        metric: DistanceMetric,
        filters: &[Filter],
        array_prefilter: Option<&nodedb_sql::types::plan::ArrayPrefilter>,
        ann_options: &VectorAnnOptions,
        skip_payload_fetch: bool,
        payload_filters: &[SqlPayloadAtom],
    ) -> Result<LiteFut<'a>, LiteError> {
        let prefilter = array_prefilter.cloned();
        let rls_filters = match sql_filters_to_metadata(filters, payload_filters)? {
            None => Vec::new(),
            Some(mf) => zerompk::to_msgpack_vec(&mf).map_err(|e| LiteError::Serialization {
                detail: format!("encode MetadataFilter: {e}"),
            })?,
        };
        let engine = self.engine;
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
                    Some(zerompk::from_msgpack(&rls_filters).map_err(|e| {
                        LiteError::Serialization {
                            detail: format!("decode MetadataFilter: {e}"),
                        }
                    })?)
                };
            let results = crate::engine::vector::search::run_vector_search(
                &engine.vector_state,
                &engine.crdt,
                &index_key,
                &collection,
                &query_vector,
                top_k,
                metadata_filter.as_ref(),
                &[],
                prefilter_bitmap.as_ref(),
                Some(&ann_options),
                skip_payload_fetch,
                Some(metric),
                Some(ef_search),
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
            })
        }))
    }

    fn text_search(
        &mut self,
        collection: &str,
        query: &FtsQuery,
        top_k: usize,
        filters: &[Filter],
        score_alias: Option<&str>,
    ) -> Result<LiteFut<'a>, LiteError> {
        // Lower FtsQuery to a TextOp and dispatch through LiteDataPlaneVisitor.
        let text_op = match query {
            FtsQuery::Phrase(terms) => {
                // Phrase queries with no analyzed terms produce no results.
                // Mirror Origin's empty-terms early-return.
                if terms.is_empty() {
                    let engine = self.engine;
                    return Ok(Box::pin(async move {
                        let _ = engine;
                        Ok(QueryResult {
                            columns: vec!["id".to_string(), "score".to_string()],
                            rows: vec![],
                            rows_affected: 0,
                        })
                    }));
                }
                TextOp::PhraseSearch {
                    collection: collection.to_string(),
                    terms: terms.clone(),
                    top_k,
                    prefilter: None,
                }
            }
            FtsQuery::Not(_) => {
                return Err(LiteError::BadRequest {
                    detail: "FTS NOT queries are not supported".to_string(),
                });
            }
            other => {
                // Plain, And, Or, Prefix — extract a BM25-compatible plain string.
                let Some(plain) = other.to_plain_string() else {
                    return Err(LiteError::BadRequest {
                        detail: "FTS query cannot be expressed as a plain text search".to_string(),
                    });
                };
                let fuzzy = other.is_fuzzy();
                // Encode filters into rls_filters bytes.
                let rls_filters = if filters.is_empty() {
                    Vec::new()
                } else {
                    let mf = crate::query::filter_convert::sql_filters_to_metadata(filters, &[])
                        .map_err(|e| LiteError::BadRequest {
                            detail: format!("FTS filter encode: {e}"),
                        })?;
                    match mf {
                        None => Vec::new(),
                        Some(mf) => {
                            zerompk::to_msgpack_vec(&mf).map_err(|e| LiteError::Serialization {
                                detail: format!("encode MetadataFilter: {e}"),
                            })?
                        }
                    }
                };
                if score_alias.is_some() {
                    TextOp::BM25ScoreScan {
                        collection: collection.to_string(),
                        query: plain,
                        score_alias: score_alias.unwrap_or("score").to_string(),
                        fuzzy,
                    }
                } else {
                    TextOp::Search {
                        collection: collection.to_string(),
                        query: plain,
                        top_k,
                        fuzzy,
                        prefilter: None,
                        rls_filters,
                    }
                }
            }
        };

        let engine = self.engine;
        let mut phys = LiteDataPlaneVisitor { engine };
        phys.text(&text_op).map(|fut| Box::pin(fut) as LiteFut<'a>)
    }

    impl_unsupported_lite_visitor_methods!();
}
