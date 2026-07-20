// SPDX-License-Identifier: Apache-2.0

//! The single `impl NodeDb for NodeDbLite<S>` block.
//!
//! Each method delegates to a domain-specific inherent helper defined
//! in a sibling module (`vector`, `graph`, `document`, `sql_lifecycle`).
//! This keeps the trait surface in one place while the implementations
//! stay split by concern.

use std::collections::HashSet;

use async_trait::async_trait;

use nodedb_client::NodeDb;
use nodedb_types::document::Document;
use nodedb_types::dropped_collection::DroppedCollection;
use nodedb_types::error::NodeDbResult;
use nodedb_types::filter::{EdgeFilter, MetadataFilter};
use nodedb_types::graph::GraphStats;
use nodedb_types::id::{EdgeId, NodeId};
use nodedb_types::result::{QueryResult, SearchResult, SubGraph};
use nodedb_types::text_search::TextSearchParams;
use nodedb_types::value::Value;

use crate::nodedb::NodeDbLite;
use crate::storage::engine::StorageEngine;

use super::vector::{INTERNAL_FIELDS_BASE, INTERNAL_FIELDS_NAMED};

#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
impl<S: StorageEngine> NodeDb for NodeDbLite<S> {
    // ─── Vector Operations ───────────────────────────────────────────

    async fn vector_search(
        &self,
        collection: &str,
        query: &[f32],
        k: usize,
        filter: Option<&MetadataFilter>,
        allowed_ids: Option<&HashSet<String>>,
    ) -> NodeDbResult<Vec<SearchResult>> {
        self.vector_search_internal(
            collection,
            collection,
            query,
            k,
            filter,
            INTERNAL_FIELDS_BASE,
            allowed_ids,
        )
        .await
    }

    async fn vector_insert(
        &self,
        collection: &str,
        id: &str,
        embedding: &[f32],
        metadata: Option<Document>,
    ) -> NodeDbResult<()> {
        self.vector_insert_impl(collection, id, embedding, metadata)
            .await
    }

    async fn vector_delete(&self, collection: &str, id: &str) -> NodeDbResult<()> {
        self.vector_delete_impl(collection, id).await
    }

    async fn vector_insert_field(
        &self,
        collection: &str,
        field_name: &str,
        id: &str,
        embedding: &[f32],
        metadata: Option<Document>,
    ) -> NodeDbResult<()> {
        self.vector_insert_field_impl(collection, field_name, id, embedding, metadata)
            .await
    }

    async fn vector_search_field(
        &self,
        collection: &str,
        field_name: &str,
        query: &[f32],
        k: usize,
        filter: Option<&MetadataFilter>,
    ) -> NodeDbResult<Vec<SearchResult>> {
        let index_key = if field_name.is_empty() {
            collection.to_string()
        } else {
            format!("{collection}:{field_name}")
        };
        self.vector_search_internal(
            &index_key,
            collection,
            query,
            k,
            filter,
            INTERNAL_FIELDS_NAMED,
            None,
        )
        .await
    }

    // ─── Graph Operations ────────────────────────────────────────────

    async fn graph_traverse(
        &self,
        collection: &str,
        start: &NodeId,
        depth: u8,
        edge_filter: Option<&EdgeFilter>,
    ) -> NodeDbResult<SubGraph> {
        self.graph_traverse_impl(collection, start, depth, edge_filter)
            .await
    }

    async fn graph_insert_edge(
        &self,
        collection: &str,
        from: &NodeId,
        to: &NodeId,
        edge_type: &str,
        properties: Option<Document>,
    ) -> NodeDbResult<EdgeId> {
        self.graph_insert_edge_impl(collection, from, to, edge_type, properties)
            .await
    }

    async fn graph_delete_edge(&self, collection: &str, edge_id: &EdgeId) -> NodeDbResult<()> {
        self.graph_delete_edge_impl(collection, edge_id).await
    }

    async fn graph_stats(
        &self,
        collection: Option<&str>,
        as_of: Option<i64>,
    ) -> NodeDbResult<Vec<GraphStats>> {
        self.graph_stats_impl(collection, as_of).await
    }

    async fn graph_pagerank(
        &self,
        collection: &str,
        personalization: Option<std::collections::HashMap<String, f64>>,
        damping: Option<f64>,
        max_iterations: Option<u32>,
    ) -> NodeDbResult<Vec<(String, f64)>> {
        self.graph_pagerank_impl(collection, personalization, damping, max_iterations)
            .await
    }

    async fn graph_shortest_path(
        &self,
        collection: &str,
        from: &NodeId,
        to: &NodeId,
        max_depth: u8,
        edge_filter: Option<&EdgeFilter>,
    ) -> NodeDbResult<Option<Vec<NodeId>>> {
        self.graph_shortest_path_impl(collection, from, to, max_depth, edge_filter)
            .await
    }

    // ─── CRDT List Operations (Movable List) ───────────────────────────

    async fn list_insert(
        &self,
        collection: &str,
        document_id: &str,
        list_path: &str,
        index: usize,
        fields: &Value,
    ) -> NodeDbResult<()> {
        self.list_insert_impl(collection, document_id, list_path, index, fields)
            .await
    }

    async fn list_delete(
        &self,
        collection: &str,
        document_id: &str,
        list_path: &str,
        index: usize,
    ) -> NodeDbResult<()> {
        self.list_delete_impl(collection, document_id, list_path, index)
            .await
    }

    async fn list_move(
        &self,
        collection: &str,
        document_id: &str,
        list_path: &str,
        from_index: usize,
        to_index: usize,
    ) -> NodeDbResult<()> {
        self.list_move_impl(collection, document_id, list_path, from_index, to_index)
            .await
    }

    // ─── Document Operations ─────────────────────────────────────────

    async fn document_get(&self, collection: &str, id: &str) -> NodeDbResult<Option<Document>> {
        self.document_get_impl(collection, id).await
    }

    async fn document_put(&self, collection: &str, doc: Document) -> NodeDbResult<()> {
        self.document_put_impl(collection, doc).await
    }

    async fn document_delete(&self, collection: &str, id: &str) -> NodeDbResult<()> {
        self.document_delete_impl(collection, id).await
    }

    async fn document_put_with_vector(
        &self,
        doc_collection: &str,
        doc: Document,
        vector_collection: &str,
        id: &str,
        embedding: &[f32],
    ) -> NodeDbResult<()> {
        self.document_put_with_vector_impl(doc_collection, doc, vector_collection, id, embedding)
            .await
    }

    async fn document_get_as_of(
        &self,
        collection: &str,
        id: &str,
        as_of_ms: Option<i64>,
        valid_time_ms: Option<i64>,
    ) -> NodeDbResult<Option<Document>> {
        self.document_get_as_of_impl(collection, id, as_of_ms, valid_time_ms)
            .await
    }

    async fn document_put_with_valid_time(
        &self,
        collection: &str,
        doc: Document,
        valid_from_ms: Option<i64>,
        valid_until_ms: Option<i64>,
    ) -> NodeDbResult<()> {
        self.document_put_with_valid_time_impl(collection, doc, valid_from_ms, valid_until_ms)
            .await
    }

    // ─── SQL and Text Search ─────────────────────────────────────────

    async fn execute_sql(&self, query: &str, params: &[Value]) -> NodeDbResult<QueryResult> {
        self.execute_sql_impl(query, params).await
    }

    async fn text_search(
        &self,
        collection: &str,
        _field: &str,
        query: &str,
        top_k: usize,
        params: TextSearchParams,
        allowed_ids: Option<&HashSet<String>>,
    ) -> NodeDbResult<Vec<SearchResult>> {
        self.text_search_impl(collection, query, top_k, params, allowed_ids)
            .await
    }

    // ─── Collection Lifecycle ─────────────────────────────────────────

    async fn list_dropped_collections(&self) -> NodeDbResult<Vec<DroppedCollection>> {
        // Lite has no soft-delete or retention layer, so the list is
        // always empty. Routing through `execute_sql` would hit the
        // catalog-shaped query the Origin trait default expects, which
        // Lite's executor does not implement.
        Ok(Vec::new())
    }
}
