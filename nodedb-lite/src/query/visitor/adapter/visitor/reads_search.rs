// SPDX-License-Identifier: Apache-2.0

//! Search family: vector_search/text_search/multi_vector_search/
//! sparse_search/hybrid_search/hybrid_search_triple.

use nodedb_sql::fts_types::FtsQuery;
use nodedb_sql::types::filter::Filter;
use nodedb_sql::{HybridSearchTripleVisitArgs, HybridSearchVisitArgs, VectorSearchVisitArgs};

use crate::error::LiteError;
use crate::query::engine::LiteQueryEngine;
use crate::query::visitor::adapter::text_search::lower_text_search;
use crate::query::visitor::adapter::vector_search::lower_vector_search;
use crate::query::visitor::search::{
    lower_hybrid_search, lower_hybrid_search_triple, lower_multi_vector_search, lower_sparse_search,
};
use crate::storage::engine::StorageEngine;

use super::LiteFut;

pub(super) fn vector_search<'a, S: StorageEngine + 'a>(
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
    } = args;
    lower_vector_search(
        engine,
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
    )
}

pub(super) fn text_search<'a, S: StorageEngine + 'a>(
    engine: &'a LiteQueryEngine<S>,
    collection: &str,
    query: &FtsQuery,
    top_k: usize,
    filters: &[Filter],
    score_alias: Option<&str>,
) -> Result<LiteFut<'a>, LiteError> {
    lower_text_search(engine, collection, query, top_k, filters, score_alias)
}

pub(super) fn multi_vector_search<'a, S: StorageEngine + 'a>(
    engine: &'a LiteQueryEngine<S>,
    collection: &str,
    query_vector: &[f32],
    top_k: usize,
    ef_search: usize,
) -> Result<LiteFut<'a>, LiteError> {
    lower_multi_vector_search(engine, collection, query_vector, top_k, ef_search)
}

pub(super) fn sparse_search<'a, S: StorageEngine + 'a>(
    engine: &'a LiteQueryEngine<S>,
    collection: &str,
    field: &str,
    query_entries: &[(u32, f32)],
    top_k: usize,
) -> Result<LiteFut<'a>, LiteError> {
    lower_sparse_search(engine, collection, field, query_entries, top_k)
}

pub(super) fn hybrid_search<'a, S: StorageEngine + 'a>(
    engine: &'a LiteQueryEngine<S>,
    args: HybridSearchVisitArgs<'_>,
) -> Result<LiteFut<'a>, LiteError> {
    let HybridSearchVisitArgs {
        collection,
        query_vector,
        query_text,
        top_k,
        ef_search,
        vector_weight,
        fuzzy,
        score_alias,
    } = args;
    lower_hybrid_search(
        engine,
        collection,
        query_vector,
        query_text,
        top_k,
        ef_search,
        vector_weight,
        fuzzy,
        score_alias,
    )
}

pub(super) fn hybrid_search_triple<'a, S: StorageEngine + 'a>(
    engine: &'a LiteQueryEngine<S>,
    args: HybridSearchTripleVisitArgs<'_>,
) -> Result<LiteFut<'a>, LiteError> {
    let HybridSearchTripleVisitArgs {
        collection,
        query_vector,
        query_text,
        graph_seed_id,
        graph_depth,
        graph_edge_label,
        top_k,
        ef_search,
        fuzzy,
        rrf_k,
        score_alias,
    } = args;
    lower_hybrid_search_triple(
        engine,
        collection,
        query_vector,
        query_text,
        graph_seed_id,
        graph_depth,
        graph_edge_label,
        top_k,
        ef_search,
        fuzzy,
        rrf_k,
        score_alias,
    )
}
