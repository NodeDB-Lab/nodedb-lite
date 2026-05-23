// SPDX-License-Identifier: Apache-2.0

//! `NodeDbLite` runtime methods: index helpers, engine accessors, and CRDT ops.

use std::sync::{Arc, Mutex};

use nodedb_types::error::{NodeDbError, NodeDbResult};

use crate::engine::strict::StrictEngine;
use crate::memory::{EngineId, MemoryGovernor};
use crate::nodedb::lock_ext::LockExt;
use crate::storage::engine::StorageEngine;

use super::types::NodeDbLite;

impl<S: StorageEngine> NodeDbLite<S> {
    /// Rebuild all text indices from CRDT state.
    ///
    /// Called once on cold start after CRDT snapshot restore.
    /// Scans all collections and indexes all string fields.
    pub(crate) fn rebuild_text_indices(&self) {
        let crdt = self.crdt.lock_or_recover();
        let collections = crdt.collection_names();
        let mut fts = self.fts_state.manager.lock_or_recover();

        for collection in &collections {
            if collection.starts_with("__") {
                continue;
            }
            let ids = crdt.list_ids(collection);
            if ids.is_empty() {
                continue;
            }

            for id in &ids {
                if let Some(loro_val) = crdt.read(collection, id) {
                    let doc = crate::nodedb::convert::loro_value_to_document(id, &loro_val);
                    let text: String = doc
                        .fields
                        .values()
                        .filter_map(|v| match v {
                            nodedb_types::Value::String(s) => Some(s.as_str()),
                            _ => None,
                        })
                        .collect::<Vec<_>>()
                        .join(" ");
                    fts.index_document(collection, id, &text);
                }
            }
        }
    }

    /// Rebuild spatial indices from CRDT state (cold start fallback).
    ///
    /// Scans all collections for geometry-valued fields and indexes them.
    /// Called when checkpoint restore produces empty spatial indices.
    pub(crate) fn rebuild_spatial_indices(&self) {
        let crdt = self.crdt.lock_or_recover();
        let collections = crdt.collection_names();
        let mut spatial = self.spatial.lock_or_recover();

        for collection in &collections {
            if collection.starts_with("__") {
                continue;
            }
            let ids = crdt.list_ids(collection);
            for id in &ids {
                if let Some(loro_val) = crdt.read(collection, id) {
                    let doc = crate::nodedb::convert::loro_value_to_document(id, &loro_val);
                    for (field, value) in &doc.fields {
                        // Geometry fields are stored as GeoJSON strings.
                        if let nodedb_types::Value::String(s) = value
                            && let Ok(geom) =
                                sonic_rs::from_str::<nodedb_types::geometry::Geometry>(s)
                        {
                            spatial.index_document(collection, field, id, &geom);
                        }
                    }
                }
            }
        }
    }

    /// Update the inverted text index after a document write.
    ///
    /// Called by `document_put` to keep the text index in sync.
    /// Concatenates all string fields for full-text indexing.
    pub(crate) fn index_document_text(
        &self,
        collection: &str,
        doc_id: &str,
        fields: &std::collections::HashMap<String, nodedb_types::Value>,
    ) {
        let text: String = fields
            .values()
            .filter_map(|v| match v {
                nodedb_types::Value::String(s) => Some(s.as_str()),
                _ => None,
            })
            .collect::<Vec<_>>()
            .join(" ");

        self.fts_state
            .manager
            .lock_or_recover()
            .index_document(collection, doc_id, &text);

        // Propagate to Origin via sync outbound queue.
        #[cfg(not(target_arch = "wasm32"))]
        if let Some(q) = &self.fts_outbound {
            q.enqueue_index(collection, doc_id, text);
        }
        #[cfg(target_arch = "wasm32")]
        let _ = text;
    }

    /// Remove a document from the text index.
    pub(crate) fn remove_document_text(&self, collection: &str, doc_id: &str) {
        self.fts_state
            .manager
            .lock_or_recover()
            .remove_document(collection, doc_id);

        // Propagate deletion to Origin via sync outbound queue.
        #[cfg(not(target_arch = "wasm32"))]
        if let Some(q) = &self.fts_outbound {
            q.enqueue_delete(collection, doc_id);
        }
    }

    // ── Spatial public API ────────────────────────────────────────────────────

    /// Index a geometry in a collection's spatial index.
    ///
    /// `field` identifies which geometry field is being indexed (allows a
    /// collection to carry multiple spatial fields).  If the document was
    /// previously indexed under the same `(collection, doc_id)`, the old entry
    /// is replaced (upsert semantics).
    pub fn spatial_insert(
        &self,
        collection: &str,
        field: &str,
        doc_id: &str,
        geometry: &nodedb_types::geometry::Geometry,
    ) {
        let mut spatial = self.spatial.lock_or_recover();
        spatial.index_document(collection, field, doc_id, geometry);
        drop(spatial);
        #[cfg(not(target_arch = "wasm32"))]
        if let Some(q) = &self.spatial_outbound {
            q.enqueue_insert(collection, field, doc_id, geometry);
        }
    }

    /// Remove a document's geometry from the spatial index.
    pub fn spatial_delete(&self, collection: &str, field: &str, doc_id: &str) {
        let mut spatial = self.spatial.lock_or_recover();
        spatial.remove_document(collection, field, doc_id);
        drop(spatial);
        #[cfg(not(target_arch = "wasm32"))]
        if let Some(q) = &self.spatial_outbound {
            q.enqueue_delete(collection, field, doc_id);
        }
    }

    /// Bounding-box range search: returns all doc entry IDs whose bbox
    /// intersects the query rectangle.
    pub fn spatial_search_bbox(
        &self,
        collection: &str,
        field: &str,
        query: &nodedb_types::BoundingBox,
    ) -> Vec<nodedb_spatial::rtree::RTreeEntry> {
        let spatial = self.spatial.lock_or_recover();
        spatial
            .search(collection, field, query)
            .into_iter()
            .cloned()
            .collect()
    }

    /// Nearest-neighbor search: returns the `k` closest spatial entries to
    /// the given `(lng, lat)` point.
    pub fn spatial_nearest(
        &self,
        collection: &str,
        field: &str,
        lng: f64,
        lat: f64,
        k: usize,
    ) -> Vec<nodedb_spatial::rtree::NnResult> {
        let spatial = self.spatial.lock_or_recover();
        spatial.nearest(collection, field, lng, lat, k)
    }

    /// Update memory governor with current engine usage.
    pub fn update_memory_stats(&self) {
        if let Ok(indices) = self.vector_state.hnsw_indices.lock() {
            let hnsw_bytes: usize = indices
                .values()
                .map(|idx| idx.len() * (idx.dim() * 4 + 128))
                .sum();
            self.governor.report_usage(EngineId::Hnsw, hnsw_bytes);
        }
        if let Ok(csr_map) = self.csr.lock() {
            let total: usize = csr_map
                .values()
                .map(|idx| idx.estimated_memory_bytes())
                .sum();
            self.governor.report_usage(EngineId::Csr, total);
        }
        if let Ok(crdt) = self.crdt.lock() {
            self.governor
                .report_usage(EngineId::Loro, crdt.estimated_memory_bytes());
        }
    }

    /// List currently loaded HNSW collections.
    pub fn loaded_collections(&self) -> NodeDbResult<Vec<String>> {
        let indices = self.vector_state.hnsw_indices.lock_or_recover();
        Ok(indices.keys().cloned().collect())
    }

    /// Access the memory governor.
    pub fn governor(&self) -> &MemoryGovernor {
        &self.governor
    }

    /// Access the strict document engine (for direct Binary Tuple CRUD).
    pub fn strict_engine(&self) -> &Arc<StrictEngine<S>> {
        &self.strict
    }

    /// Access the columnar analytics engine (for direct segment operations).
    pub fn columnar_engine(&self) -> &Arc<crate::engine::columnar::ColumnarEngine<S>> {
        &self.columnar
    }

    /// Access the HTAP bridge (for materialized view inspection).
    pub fn htap_bridge(&self) -> &Arc<crate::engine::htap::HtapBridge> {
        &self.htap
    }

    /// Access the timeseries engine (continuous aggregates, ingest, flush).
    pub fn timeseries_engine(
        &self,
    ) -> &Arc<Mutex<crate::engine::timeseries::engine::TimeseriesEngine>> {
        &self.timeseries
    }

    // -- Indexed CRUD for strict/columnar collections --

    /// Insert a row into a strict collection and update secondary indexes.
    ///
    /// Combines `StrictEngine.insert()` with `index_row()` for geometry,
    /// vector, and text columns.
    pub async fn strict_insert(
        &self,
        collection: &str,
        values: &[nodedb_types::value::Value],
    ) -> NodeDbResult<()> {
        let schema = self.strict.schema(collection).ok_or_else(|| {
            NodeDbError::storage(format!("strict collection '{collection}' not found"))
        })?;

        // Insert into storage. `StrictEngine` is interior-mutable; await directly.
        self.strict
            .insert(collection, values)
            .await
            .map_err(NodeDbError::storage)?;

        // Build a row_id string from the PK value for index keying.
        let row_id = pk_to_string(&schema.columns, values);

        // Update secondary indexes.
        crate::engine::index_integration::index_row(
            collection,
            &row_id,
            &schema.columns,
            values,
            &self.vector_state.hnsw_indices,
            &self.spatial,
            &self.fts_state.manager,
        );

        // Update secondary B-tree indexes on non-PK columns.
        {
            use crate::engine::strict::secondary_index::SecondaryIndex;
            let mut sec = self.secondary_indices.lock_or_recover();
            for (i, col) in schema.columns.iter().enumerate() {
                if col.primary_key || i >= values.len() {
                    continue;
                }
                let key = format!("{collection}:{}", col.name);
                sec.entry(key)
                    .or_insert_with(|| SecondaryIndex::new(&col.name))
                    .insert(&values[i], &row_id);
            }
        }

        // Replicate to materialized columnar views (HTAP CDC).
        self.htap
            .replicate_insert(collection, values, &self.columnar);

        Ok(())
    }

    /// Delete a row from a strict collection and clean up text indexes.
    pub async fn strict_delete(
        &self,
        collection: &str,
        pk: &nodedb_types::value::Value,
    ) -> NodeDbResult<bool> {
        let schema = self.strict.schema(collection).ok_or_else(|| {
            NodeDbError::storage(format!("strict collection '{collection}' not found"))
        })?;

        let row_id = format!("{pk:?}");

        // Remove text index entries before deleting the row.
        crate::engine::index_integration::deindex_row_text(
            collection,
            &row_id,
            &schema.columns,
            &self.fts_state.manager,
        );

        // Replicate delete to materialized columnar views (HTAP CDC).
        self.htap.replicate_delete(collection, pk, &self.columnar);

        self.strict
            .delete(collection, pk)
            .await
            .map_err(NodeDbError::storage)
    }

    /// Insert a row into a columnar collection and update secondary indexes.
    pub fn columnar_insert(
        &self,
        collection: &str,
        values: &[nodedb_types::value::Value],
    ) -> NodeDbResult<()> {
        let schema = self.columnar.schema(collection).ok_or_else(|| {
            NodeDbError::storage(format!("columnar collection '{collection}' not found"))
        })?;

        self.columnar
            .insert(collection, values)
            .map_err(NodeDbError::storage)?;

        let row_id = pk_to_string(&schema.columns, values);

        crate::engine::index_integration::index_row(
            collection,
            &row_id,
            &schema.columns,
            values,
            &self.vector_state.hnsw_indices,
            &self.spatial,
            &self.fts_state.manager,
        );

        // Spatial profile: compute geohash for Point geometries and store
        // in the text index for prefix-based proximity queries.
        if let Some(profile) = self.columnar.profile(collection)
            && let Some((_idx, geom)) = crate::engine::columnar::spatial_profile::extract_geometry(
                &schema, &profile, values,
            )
            && let Some(hash) = crate::engine::columnar::spatial_profile::compute_geohash(&geom)
        {
            self.fts_state
                .manager
                .lock_or_recover()
                .index_field(collection, "_geohash", &row_id, &hash);
        }
        Ok(())
    }

    /// Apply a CRDT field-level update to a strict collection row.
    ///
    /// Used during sync: a remote delta specifies field changes for a row.
    /// This reads the current tuple, patches the fields, and writes back.
    pub async fn strict_crdt_patch(
        &self,
        collection: &str,
        pk: &nodedb_types::value::Value,
        field_updates: &std::collections::HashMap<String, nodedb_types::value::Value>,
    ) -> NodeDbResult<()> {
        let schema = self.strict.schema(collection).ok_or_else(|| {
            NodeDbError::storage(format!("strict collection '{collection}' not found"))
        })?;

        // Read existing tuple.
        let existing = self
            .strict
            .get(collection, pk)
            .await
            .map_err(NodeDbError::storage)?
            .ok_or_else(|| NodeDbError::storage("row not found for CRDT patch"))?;

        // Re-encode as tuple bytes for the adapter.
        let encoder = nodedb_strict::TupleEncoder::new(&schema);
        let tuple_bytes = encoder
            .encode(&existing)
            .map_err(|e| NodeDbError::storage(e.to_string()))?;

        // Apply the CRDT patch.
        let patched = crate::engine::strict::crdt_adapter::apply_crdt_set(
            &tuple_bytes,
            &schema,
            field_updates,
        )
        .map_err(NodeDbError::storage)?;

        // Decode patched tuple back to values and update.
        let decoder = nodedb_strict::TupleDecoder::new(&schema);
        let new_values = decoder
            .extract_all(&patched)
            .map_err(|e| NodeDbError::storage(e.to_string()))?;

        // Write back via the standard update path.
        self.strict
            .update_by_values(collection, pk, &new_values)
            .await
            .map_err(NodeDbError::storage)?;

        Ok(())
    }

    /// Access pending CRDT deltas (for sync client).
    pub fn pending_crdt_deltas(
        &self,
    ) -> NodeDbResult<Vec<crate::engine::crdt::engine::PendingDelta>> {
        let crdt = self.crdt.lock_or_recover();
        Ok(crdt.pending_deltas().to_vec())
    }

    /// Acknowledge synced deltas (called after Origin ACK).
    pub fn acknowledge_deltas(&self, acked_id: u64) -> NodeDbResult<()> {
        let mut crdt = self.crdt.lock_or_recover();
        crdt.acknowledge(acked_id);
        Ok(())
    }

    /// Import remote deltas from Origin.
    pub fn import_remote_deltas(&self, data: &[u8]) -> NodeDbResult<()> {
        let crdt = self.crdt.lock_or_recover();
        crdt.import_remote(data).map_err(NodeDbError::storage)
    }

    /// Reject a specific delta (rollback optimistic local state).
    pub fn reject_delta(&self, mutation_id: u64) -> NodeDbResult<()> {
        let mut crdt = self.crdt.lock_or_recover();
        crdt.reject_delta(mutation_id);
        Ok(())
    }

    /// Start background sync to Origin.
    ///
    /// Spawns a Tokio task that connects to the Origin WebSocket endpoint,
    /// pushes pending deltas, and receives shape updates. Runs forever
    /// with auto-reconnect.
    ///
    /// Returns immediately — the sync runs in the background.
    #[cfg(not(target_arch = "wasm32"))]
    pub fn start_sync(
        self: &Arc<Self>,
        config: crate::sync::SyncConfig,
    ) -> Arc<crate::sync::SyncClient> {
        let client = Arc::new(crate::sync::SyncClient::new(config, self.peer_id()));
        let delegate: Arc<dyn crate::sync::SyncDelegate> = Arc::clone(self) as _;
        let client_clone = Arc::clone(&client);
        tokio::spawn(async move {
            crate::sync::run_sync_loop(client_clone, delegate).await;
        });
        client
    }

    /// Get the peer ID (from the CRDT engine).
    pub fn peer_id(&self) -> u64 {
        self.crdt.lock().map(|c| c.peer_id()).unwrap_or(0)
    }
}

/// Build a string row ID from PK column values (for index keying).
fn pk_to_string(
    columns: &[nodedb_types::columnar::ColumnDef],
    values: &[nodedb_types::value::Value],
) -> String {
    use nodedb_types::value::Value;
    let mut parts = Vec::new();
    for (i, col) in columns.iter().enumerate() {
        if col.primary_key
            && let Some(val) = values.get(i)
        {
            match val {
                Value::Integer(n) => parts.push(n.to_string()),
                Value::String(s) => parts.push(s.clone()),
                Value::Uuid(s) => parts.push(s.clone()),
                other => parts.push(format!("{other:?}")),
            }
        }
    }
    parts.join(":")
}
