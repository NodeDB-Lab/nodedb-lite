// SPDX-License-Identifier: Apache-2.0

//! Free-function FTS search callable from both `NodeDbLite` and
//! `LiteDataPlaneVisitor` without depending on either concrete type.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use nodedb_types::error::NodeDbResult;
use nodedb_types::result::SearchResult;
use nodedb_types::text_search::TextSearchParams;

use crate::engine::crdt::CrdtEngine;
use crate::engine::fts::state::FtsState;
use crate::nodedb::convert::loro_value_to_document;
use crate::nodedb::lock_ext::LockExt;

/// Run a BM25 text query against the in-memory FTS index and hydrate each
/// hit with the document's fields from CRDT storage.
///
/// The FTS score is converted to a `distance` in `[0.0, 1.0]` via
/// `1.0 - min(score / 20.0, 1.0)` so callers can rank text and vector hits
/// on the same axis (lower = better).
pub(crate) fn run_text_search(
    fts_state: &Arc<FtsState>,
    crdt: &Arc<Mutex<CrdtEngine>>,
    collection: &str,
    query: &str,
    top_k: usize,
    params: &TextSearchParams,
) -> NodeDbResult<Vec<SearchResult>> {
    let results = fts_state
        .manager
        .lock_or_recover()
        .search(collection, query, top_k, params);
    let crdt_guard = crdt.lock_or_recover();
    Ok(results
        .into_iter()
        .map(|r| {
            let metadata = if let Some(loro_val) = crdt_guard.read(collection, &r.doc_id) {
                loro_value_to_document(&r.doc_id, &loro_val).fields
            } else {
                HashMap::new()
            };
            SearchResult {
                id: r.doc_id,
                node_id: None,
                distance: 1.0 - (r.score / 20.0).min(1.0),
                metadata,
            }
        })
        .collect())
}
