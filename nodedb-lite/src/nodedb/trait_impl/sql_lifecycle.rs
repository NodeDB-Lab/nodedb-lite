// SPDX-License-Identifier: Apache-2.0

//! SQL execution and text-search helpers for `NodeDbLite`.

use std::collections::HashMap;

use nodedb_types::error::{NodeDbError, NodeDbResult};
use nodedb_types::result::{QueryResult, SearchResult};
use nodedb_types::text_search::TextSearchParams;
use nodedb_types::value::Value;

use crate::nodedb::LockExt;
use crate::nodedb::NodeDbLite;
use crate::nodedb::convert::loro_value_to_document;
use crate::storage::engine::{StorageEngine, StorageEngineSync};

impl<S: StorageEngine + StorageEngineSync> NodeDbLite<S> {
    /// Execute a SQL statement against the embedded query engine.
    ///
    /// `params` is accepted for API parity with Origin's prepared-statement
    /// path but is not yet plumbed through the Lite query engine — values must
    /// currently be embedded literally in `query`.
    pub(super) async fn execute_sql_impl(
        &self,
        query: &str,
        _params: &[Value],
    ) -> NodeDbResult<QueryResult> {
        self.query_engine
            .execute_sql(query)
            .await
            .map_err(NodeDbError::storage)
    }

    /// Run a BM25 text query against the in-memory FTS index for `collection`
    /// and hydrate each hit with the document's fields from CRDT storage.
    ///
    /// The FTS score is converted to a `distance` in `[0.0, 1.0]` via
    /// `1.0 - min(score / 20.0, 1.0)` so callers can rank text and vector hits
    /// on the same axis (lower = better). The `20.0` divisor matches the BM25
    /// score range produced by the bundled analyzer pipeline.
    pub(super) async fn text_search_impl(
        &self,
        collection: &str,
        query: &str,
        top_k: usize,
        params: TextSearchParams,
    ) -> NodeDbResult<Vec<SearchResult>> {
        let results = self
            .fts
            .lock_or_recover()
            .search(collection, query, top_k, &params);

        let crdt = self.crdt.lock_or_recover();
        Ok(results
            .into_iter()
            .map(|r| {
                let metadata = if let Some(loro_val) = crdt.read(collection, &r.doc_id) {
                    let doc = loro_value_to_document(&r.doc_id, &loro_val);
                    doc.fields
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
}
