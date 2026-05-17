// SPDX-License-Identifier: Apache-2.0

//! Text search lowering: converts an `FtsQuery` to a `TextOp` and dispatches
//! through the data-plane visitor. Empty phrase queries short-circuit to an
//! empty `QueryResult`. `FtsQuery::Not` is rejected — Lite does not support
//! standalone NOT FTS queries.

use nodedb_physical::PhysicalTaskVisitor;
use nodedb_physical::physical_plan::TextOp;
use nodedb_sql::fts_types::FtsQuery;
use nodedb_sql::types::filter::Filter;
use nodedb_types::result::QueryResult;

use crate::error::LiteError;
use crate::query::engine::LiteQueryEngine;
use crate::query::filter_convert::sql_filters_to_metadata;
use crate::query::physical_visitor::LiteDataPlaneVisitor;
use crate::storage::engine::{StorageEngine, StorageEngineSync};

use super::visitor::LiteFut;

pub(super) fn lower_text_search<'a, S: StorageEngine + StorageEngineSync + 'a>(
    engine: &'a LiteQueryEngine<S>,
    collection: &str,
    query: &FtsQuery,
    top_k: usize,
    filters: &[Filter],
    score_alias: Option<&str>,
) -> Result<LiteFut<'a>, LiteError> {
    let text_op = match query {
        FtsQuery::Phrase(terms) => {
            // Phrase queries with no analyzed terms produce no results.
            if terms.is_empty() {
                return Ok(Box::pin(async move {
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
            let Some(plain) = other.to_plain_string() else {
                return Err(LiteError::BadRequest {
                    detail: "FTS query cannot be expressed as a plain text search".to_string(),
                });
            };
            let fuzzy = other.is_fuzzy();
            let rls_filters = if filters.is_empty() {
                Vec::new()
            } else {
                // Complex QExpr predicates cannot be serialized to the FTS physical visitor;
                // they are dropped from the pre-filter here. Any such predicates will be applied
                // at post-scan time by apply_scan_post_processing on the caller side.
                let lf = sql_filters_to_metadata(filters, &[])?;
                match lf.meta {
                    None => Vec::new(),
                    Some(mf) => {
                        zerompk::to_msgpack_vec(&mf).map_err(|e| LiteError::Serialization {
                            detail: format!("encode MetadataFilter: {e}"),
                        })?
                    }
                }
            };
            if let Some(alias) = score_alias {
                TextOp::BM25ScoreScan {
                    collection: collection.to_string(),
                    query: plain,
                    score_alias: alias.to_string(),
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

    let mut phys = LiteDataPlaneVisitor { engine };
    phys.text(&text_op).map(|fut| Box::pin(fut) as LiteFut<'a>)
}
