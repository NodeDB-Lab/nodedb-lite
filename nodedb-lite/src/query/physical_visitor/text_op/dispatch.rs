// SPDX-License-Identifier: Apache-2.0

//! `TextOp` variant routing.

use nodedb_physical::physical_plan::TextOp;
use nodedb_types::text_search::TextSearchParams;

use crate::error::LiteError;
use crate::query::engine::LiteQueryEngine;
use crate::storage::engine::StorageEngine;

use super::super::adapter::LitePhysicalFut;
use super::super::text_config::text_set_config;
use super::hybrid::{HybridArgs, HybridTripleArgs, hybrid_search, hybrid_search_triple};
use super::score_scan::{ScoreScanArgs, bm25_score_scan};
use super::search::{PhraseArgs, SearchArgs, phrase_search, text_search};
use super::sync::{fts_delete_doc, fts_index_doc};

/// Dispatch a `TextOp`. Returns a pinned future that resolves to a
/// `QueryResult`.
pub(crate) fn execute_text_op<'a, S: StorageEngine + 'a>(
    engine: &'a LiteQueryEngine<S>,
    op: &TextOp,
) -> Result<LitePhysicalFut<'a>, LiteError> {
    execute_text_op_admitted(engine, None, op)
}

/// Dispatch a `TextOp` under `permit`, when the caller holds one. Each read
/// names its field in the op. No field reads the whole-document index.
pub(crate) fn execute_text_op_admitted<'a, S: StorageEngine + 'a>(
    engine: &'a LiteQueryEngine<S>,
    permit: Option<&'a crate::engine::fts::coordinator::TextMutationPermit>,
    op: &TextOp,
) -> Result<LitePhysicalFut<'a>, LiteError> {
    match op {
        TextOp::Search {
            collection,
            field,
            query,
            top_k,
            mode,
            fuzzy,
            // Lite resolves eligibility from `filters`; it computes no
            // surrogate prefilter.
            prefilter: _,
            filters,
            rls_filters,
            scores,
        } => text_search(
            engine,
            SearchArgs {
                collection: collection.as_str(),
                field: field.as_deref().unwrap_or(""),
                query,
                top_k: *top_k,
                params: TextSearchParams {
                    mode: *mode,
                    fuzzy: *fuzzy,
                },
                filters,
                rls_filters,
                scores,
            },
        ),

        TextOp::BM25ScoreScan {
            collection,
            filters,
            rls_filters,
            scores,
            bound,
        } => bm25_score_scan(
            engine,
            ScoreScanArgs {
                collection: collection.as_str(),
                filters,
                rls_filters,
                scores,
                bound: bound.as_ref(),
            },
        ),

        TextOp::PhraseSearch {
            collection,
            field,
            terms,
            top_k,
            prefilter: _,
            filters,
            rls_filters,
            scores,
        } => phrase_search(
            engine,
            PhraseArgs {
                collection: collection.as_str(),
                field: field.as_deref().unwrap_or(""),
                terms,
                top_k: *top_k,
                filters,
                rls_filters,
                scores,
            },
        ),

        TextOp::HybridSearch {
            collection,
            vector_field,
            query_vector,
            text_field,
            query_text,
            filters,
            top_k,
            ef_search: _,
            mode,
            fuzzy,
            vector_weight,
            filter_bitmap: _,
            rls_filters,
            score_alias,
        } => hybrid_search(
            engine,
            HybridArgs {
                collection: collection.as_str(),
                vector_field,
                text_field: text_field.as_deref().unwrap_or(""),
                query_vector,
                query_text,
                filters,
                top_k: *top_k,
                mode: *mode,
                fuzzy: *fuzzy,
                vector_weight: *vector_weight,
                rls_filters,
                score_alias: score_alias.as_deref(),
            },
        ),

        TextOp::HybridSearchTriple {
            collection,
            vector_field,
            query_vector,
            text_field,
            query_text,
            filters,
            graph_seed_id,
            graph_depth,
            graph_edge_label,
            top_k,
            ef_search: _,
            mode,
            fuzzy,
            rrf_k,
            filter_bitmap: _,
            rls_filters,
            score_alias,
        } => hybrid_search_triple(
            engine,
            HybridTripleArgs {
                collection: collection.as_str(),
                vector_field,
                text_field: text_field.as_deref().unwrap_or(""),
                query_vector,
                query_text,
                filters,
                graph_seed_id,
                graph_depth: *graph_depth,
                graph_edge_label: graph_edge_label.as_deref(),
                top_k: *top_k,
                mode: *mode,
                fuzzy: *fuzzy,
                rrf_k: *rrf_k,
                rls_filters,
                score_alias: score_alias.as_deref(),
            },
        ),

        TextOp::FtsIndexDoc {
            collection,
            surrogate,
            fields,
            provenance: _,
        } => Ok(fts_index_doc(
            engine,
            permit,
            collection.as_str(),
            *surrogate,
            fields,
        )),

        TextOp::FtsDeleteDoc {
            collection,
            surrogate,
            provenance: _,
        } => Ok(fts_delete_doc(
            engine,
            permit,
            collection.as_str(),
            *surrogate,
        )),

        // ── Config write ──────────────────────────────────────────────────────
        TextOp::SetTextConfig {
            collection,
            analyzer_name,
            fuzzy_default,
        } => text_set_config(
            engine,
            permit,
            collection.as_str().to_string(),
            analyzer_name.clone(),
            *fuzzy_default,
        ),
    }
}
