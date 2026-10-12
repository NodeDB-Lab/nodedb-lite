// SPDX-License-Identifier: Apache-2.0

//! Text search lowering: converts a `TextSearchShape` to a `TextOp` and runs
//! it. A `Match` reads the index of the field the SQL names
//! (`text_match(field, q)`). No field reads the whole-document index. A
//! `ScoreScan` (`bm25_score` with no `text_match`) returns every row the
//! filters admit with its score columns.
//!
//! Empty phrase queries short-circuit to an empty `QueryResult`.
//! `FtsQuery::Not` is rejected: Lite does not support standalone NOT FTS
//! queries.

use nodedb_physical::PhysicalTaskVisitor;
use nodedb_physical::physical_plan::{TextOp, TextScoreSpec};
use nodedb_sql::TextSearchVisitArgs;
use nodedb_sql::fts_types::FtsQuery;
use nodedb_sql::types::TextSearchShape;
use nodedb_types::result::QueryResult;

use crate::error::LiteError;
use crate::query::engine::LiteQueryEngine;
use crate::query::physical_visitor::LiteDataPlaneVisitor;
use crate::query::scan_filter_convert::encode_scan_filters;
use crate::storage::engine::StorageEngine;

use super::visitor::LiteFut;

/// An empty `id` + `score` result.
fn empty_hits<'a>() -> LiteFut<'a> {
    Box::pin(async {
        Ok(QueryResult {
            columns: vec!["id".to_string(), "score".to_string()],
            rows: vec![],
            rows_affected: 0,
            command: None,
        })
    })
}

pub(super) fn lower_text_search<'a, S: StorageEngine + 'a>(
    engine: &'a LiteQueryEngine<S>,
    args: TextSearchVisitArgs<'_>,
) -> Result<LiteFut<'a>, LiteError> {
    let TextSearchVisitArgs {
        collection,
        shape,
        filters,
        scores,
    } = args;
    // Lite holds a bare collection name; DatabaseId::DEFAULT keeps it unqualified.
    let collection =
        nodedb_types::QualifiedCollection::new(nodedb_types::DatabaseId::DEFAULT, collection);
    // The residual filters travel as zerompk `Vec<ScanFilter>` bytes and
    // restrict the candidates before ranking.
    let filters = encode_scan_filters(filters)?;
    let scores: Vec<TextScoreSpec> = scores
        .iter()
        .map(|s| TextScoreSpec {
            field: s.field.clone(),
            query: s.query.clone(),
            mode: s.mode,
            fuzzy: s.fuzzy,
            alias: s.alias.clone(),
        })
        .collect();

    let text_op = match shape {
        TextSearchShape::ScoreScan => TextOp::BM25ScoreScan {
            collection,
            filters,
            rls_filters: Vec::new(),
            scores,
            bound: None,
        },
        TextSearchShape::Match {
            field,
            query: FtsQuery::Phrase(terms),
            top_k,
            ..
        } => {
            // Phrase queries with no terms produce no results.
            if terms.is_empty() {
                return Ok(empty_hits());
            }
            TextOp::PhraseSearch {
                collection,
                field: field.clone(),
                terms: terms.clone(),
                top_k: top_k.unwrap_or(usize::MAX),
                prefilter: None,
                filters,
                rls_filters: Vec::new(),
                scores,
            }
        }
        TextSearchShape::Match {
            query: FtsQuery::Not(_),
            ..
        } => {
            return Err(LiteError::BadRequest {
                detail: "FTS NOT queries are not supported".to_string(),
            });
        }
        TextSearchShape::Match {
            field,
            query,
            mode,
            top_k,
        } => {
            let Some(plain) = query.to_plain_string() else {
                return Err(LiteError::BadRequest {
                    detail: "FTS query cannot be expressed as a plain text search".to_string(),
                });
            };
            TextOp::Search {
                collection,
                field: field.clone(),
                query: plain,
                top_k: top_k.unwrap_or(usize::MAX),
                mode: *mode,
                fuzzy: query.is_fuzzy(),
                prefilter: None,
                filters,
                rls_filters: Vec::new(),
                scores,
            }
        }
    };

    let fut = LiteDataPlaneVisitor::new(engine).text(&text_op)?;
    Ok(Box::pin(fut))
}
