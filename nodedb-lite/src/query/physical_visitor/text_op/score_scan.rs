// SPDX-License-Identifier: Apache-2.0

//! `TextOp::BM25ScoreScan`: every admitted row with its score columns.
//!
//! The rows are the stored rows of the collection the filters admit. With
//! no filter, a document a score's index holds is a row too, so indexed
//! rows of every engine appear. Rows are in document-id order unless the
//! bound orders them by a score column.

use std::cmp::Ordering;
use std::collections::BTreeSet;
use std::sync::Arc;

use nodedb_physical::physical_plan::{ScoreScanBound, ScoreScanOrder, TextScoreSpec};
use nodedb_types::result::QueryResult;
use nodedb_types::value::Value;

use crate::error::LiteError;
use crate::query::engine::LiteQueryEngine;
use crate::storage::engine::StorageEngine;

use super::super::adapter::LitePhysicalFut;
use super::rows::{TextRowGate, admitted_ids, collection_ids};
use super::scores::ScoreColumns;

/// `TextOp::BM25ScoreScan` inputs.
pub(super) struct ScoreScanArgs<'o> {
    pub collection: &'o str,
    pub filters: &'o [u8],
    pub rls_filters: &'o [u8],
    pub scores: &'o [TextScoreSpec],
    pub bound: Option<&'o ScoreScanBound>,
}

/// Order two scores under `order`. A missing score is `null`.
fn compare_scores(a: Option<f32>, b: Option<f32>, order: &ScoreScanOrder) -> Ordering {
    let null_side = if order.nulls_first {
        Ordering::Less
    } else {
        Ordering::Greater
    };
    match (a, b) {
        (None, None) => Ordering::Equal,
        (None, Some(_)) => null_side,
        (Some(_), None) => null_side.reverse(),
        (Some(x), Some(y)) => {
            let by_value = x.total_cmp(&y);
            if order.ascending {
                by_value
            } else {
                by_value.reverse()
            }
        }
    }
}

/// Keep the rows `bound` admits. `ids` arrive in document-id order, which
/// breaks ties of the bound's order.
fn apply_bound(
    mut ids: Vec<String>,
    scores: &ScoreColumns,
    bound: &ScoreScanBound,
) -> Result<Vec<String>, LiteError> {
    if let Some(order) = &bound.order {
        let column = scores
            .position(&order.alias)
            .ok_or_else(|| LiteError::BadRequest {
                detail: format!(
                    "score scan orders by '{}', which names no score column of the scan",
                    order.alias
                ),
            })?;
        ids.sort_by(|a, b| compare_scores(scores.score(column, a), scores.score(column, b), order));
    }
    ids.truncate(bound.rows);
    Ok(ids)
}

/// `TextOp::BM25ScoreScan`: every admitted row with one column per score spec.
pub(super) fn bm25_score_scan<'a, S: StorageEngine + 'a>(
    engine: &'a LiteQueryEngine<S>,
    args: ScoreScanArgs<'_>,
) -> Result<LitePhysicalFut<'a>, LiteError> {
    let gate = TextRowGate::new(args.filters, args.rls_filters)?;
    let collection = args.collection.to_owned();
    let specs = args.scores.to_vec();
    let bound = args.bound.cloned();
    let fts_state = Arc::clone(&engine.fts_state);
    let crdt = Arc::clone(&engine.crdt);
    Ok(Box::pin(async move {
        let admitted = admitted_ids(&crdt, &collection, &gate)?;
        let scores = ScoreColumns::compute(&fts_state, &collection, &specs, admitted.as_ref())?;
        let ordered: BTreeSet<String> = match admitted {
            Some(ids) => ids.into_iter().collect(),
            None => collection_ids(&crdt, &collection)
                .into_iter()
                .chain(scores.held_ids().cloned())
                .collect(),
        };
        let mut ids: Vec<String> = ordered.into_iter().collect();
        if let Some(bound) = &bound {
            ids = apply_bound(ids, &scores, bound)?;
        }

        let mut columns = vec!["id".to_string()];
        columns.extend(scores.aliases());
        let rows = ids
            .into_iter()
            .map(|id| {
                let score_values = scores.values(&id);
                let mut row = Vec::with_capacity(1 + score_values.len());
                row.push(Value::String(id));
                row.extend(score_values);
                row
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

#[cfg(test)]
mod tests {
    use super::*;

    fn order(ascending: bool, nulls_first: bool) -> ScoreScanOrder {
        ScoreScanOrder {
            alias: "s".to_string(),
            ascending,
            nulls_first,
        }
    }

    #[test]
    fn descending_puts_the_best_score_first() {
        let o = order(false, false);
        assert_eq!(
            compare_scores(Some(2.0), Some(1.0), &o),
            Ordering::Less,
            "a higher score sorts first when descending"
        );
    }

    #[test]
    fn nulls_follow_the_nulls_first_flag() {
        let last = order(true, false);
        assert_eq!(compare_scores(None, Some(0.0), &last), Ordering::Greater);
        let first = order(true, true);
        assert_eq!(compare_scores(None, Some(0.0), &first), Ordering::Less);
        assert_eq!(compare_scores(None, None, &first), Ordering::Equal);
    }
}
