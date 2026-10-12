// SPDX-License-Identifier: Apache-2.0

//! Text reads: ranked BM25 search and phrase search.
//!
//! The residual filters and RLS filters restrict the candidates before
//! ranking, so `top_k` counts only admitted rows. Each hit carries `id`,
//! `score`, and one column per score spec.

use std::sync::Arc;

use nodedb_physical::physical_plan::TextScoreSpec;
use nodedb_types::result::QueryResult;
use nodedb_types::text_search::TextSearchParams;
use nodedb_types::value::Value;

use crate::engine::fts::{TextSearchRequest, run_text_search};
use crate::error::LiteError;
use crate::nodedb::lock_ext::LockExt;
use crate::query::engine::LiteQueryEngine;
use crate::storage::engine::StorageEngine;

use super::super::adapter::LitePhysicalFut;
use super::rows::{TextRowGate, admitted_ids, is_eligible};
use super::scores::ScoreColumns;

/// `TextOp::Search` inputs.
pub(super) struct SearchArgs<'o> {
    pub collection: &'o str,
    /// Field index the query reads. Empty reads the whole-document index.
    pub field: &'o str,
    pub query: &'o str,
    pub top_k: usize,
    pub params: TextSearchParams,
    pub filters: &'o [u8],
    pub rls_filters: &'o [u8],
    pub scores: &'o [TextScoreSpec],
}

/// `TextOp::PhraseSearch` inputs.
pub(super) struct PhraseArgs<'o> {
    pub collection: &'o str,
    /// Field index the phrase reads. Empty reads the whole-document index.
    pub field: &'o str,
    pub terms: &'o [String],
    pub top_k: usize,
    pub filters: &'o [u8],
    pub rls_filters: &'o [u8],
    pub scores: &'o [TextScoreSpec],
}

/// `id` + `score` + score-column rows, in hit order.
fn hit_result(hits: Vec<(String, f64)>, scores: &ScoreColumns) -> QueryResult {
    let mut columns = vec!["id".to_string(), "score".to_string()];
    columns.extend(scores.aliases());
    let rows = hits
        .into_iter()
        .map(|(id, score)| {
            let score_values = scores.values(&id);
            let mut row = Vec::with_capacity(2 + score_values.len());
            row.push(Value::String(id));
            row.push(Value::Float(score));
            row.extend(score_values);
            row
        })
        .collect();
    QueryResult {
        columns,
        rows,
        rows_affected: 0,
        command: None,
    }
}

/// `TextOp::Search`: ranked BM25 hits among the admitted rows.
pub(super) fn text_search<'a, S: StorageEngine + 'a>(
    engine: &'a LiteQueryEngine<S>,
    args: SearchArgs<'_>,
) -> Result<LitePhysicalFut<'a>, LiteError> {
    let gate = TextRowGate::new(args.filters, args.rls_filters)?;
    let collection = args.collection.to_owned();
    let field = args.field.to_owned();
    let query = args.query.to_owned();
    let top_k = args.top_k;
    let params = args.params;
    let specs = args.scores.to_vec();
    let fts_state = Arc::clone(&engine.fts_state);
    let crdt = Arc::clone(&engine.crdt);
    Ok(Box::pin(async move {
        let admitted = admitted_ids(&crdt, &collection, &gate)?;
        let results = run_text_search(
            &fts_state,
            &crdt,
            TextSearchRequest {
                collection: &collection,
                field: &field,
                query: &query,
                top_k,
                params: &params,
                allowed_ids: admitted.as_ref(),
            },
        )?;
        let scores = ScoreColumns::compute(&fts_state, &collection, &specs, admitted.as_ref())?;
        Ok(hit_result(
            results
                .into_iter()
                .map(|r| (r.id, f64::from(1.0 - r.distance)))
                .collect(),
            &scores,
        ))
    }))
}

/// `TextOp::PhraseSearch`: admitted documents holding the terms
/// consecutively, best first.
pub(super) fn phrase_search<'a, S: StorageEngine + 'a>(
    engine: &'a LiteQueryEngine<S>,
    args: PhraseArgs<'_>,
) -> Result<LitePhysicalFut<'a>, LiteError> {
    let gate = TextRowGate::new(args.filters, args.rls_filters)?;
    let collection = args.collection.to_owned();
    let field = args.field.to_owned();
    let terms = args.terms.to_vec();
    let top_k = args.top_k;
    let specs = args.scores.to_vec();
    let fts_state = Arc::clone(&engine.fts_state);
    let crdt = Arc::clone(&engine.crdt);
    Ok(Box::pin(async move {
        let admitted = admitted_ids(&crdt, &collection, &gate)?;
        let params = TextSearchParams::default();
        // Every phrase match is ranked, so the admitted ones are kept before
        // `top_k` applies.
        let mut hits: Vec<(String, f64)> = fts_state
            .manager
            .lock_or_recover()
            .phrase_search(&collection, &field, &terms, usize::MAX, &params)?
            .into_iter()
            .filter(|r| is_eligible(admitted.as_ref(), &r.doc_id))
            .map(|r| (r.doc_id, f64::from(r.score)))
            .collect();
        hits.truncate(top_k);
        let scores = ScoreColumns::compute(&fts_state, &collection, &specs, admitted.as_ref())?;
        Ok(hit_result(hits, &scores))
    }))
}
