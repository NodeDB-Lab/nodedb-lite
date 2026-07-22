// SPDX-License-Identifier: Apache-2.0

//! Document engine helpers for `NodeDbLite`.
//!
//! Read-path strategy for bitemporal collections (mirrors Origin's choice in
//! `nodedb/src/engine/document/store/engine/get.rs:10-28`):
//!
//! **Option A — switch the read path entirely.**  When a collection is
//! bitemporal, `document_get` reads from `versioned_get_current` (the history
//! table) rather than the CRDT store.  The CRDT store still receives the write
//! via `document_put` so that sync and current-state access both work, but for
//! bitemporal collections the history table is authoritative for reads and
//! `document_delete` appends a tombstone rather than performing a hard delete.

use nodedb_types::document::Document;
use nodedb_types::error::{NodeDbError, NodeDbResult};

use crate::engine::document::history::ops::{
    is_bitemporal, versioned_get_as_of, versioned_get_current, versioned_put, versioned_tombstone,
};
// Note: versioned_get_current is used only for the non-as_of path of document_get.
use crate::engine::document::history::value::DecodedVersion;
use crate::nodedb::LockExt;
use crate::nodedb::NodeDbLite;
use crate::nodedb::convert::{document_to_msgpack, loro_value_to_document, value_to_loro};
use crate::runtime::{monotonic_millis_i64, now_millis_i64};
use crate::storage::engine::StorageEngine;

impl<S: StorageEngine> NodeDbLite<S> {
    /// Read a single document by id.
    ///
    /// For bitemporal collections, delegates to `versioned_get_current` so the
    /// history table is the source of truth (mirrors Origin get.rs:10-28).
    /// For plain collections, reads directly from the CRDT store.
    pub(super) async fn document_get_impl(
        &self,
        collection: &str,
        id: &str,
    ) -> NodeDbResult<Option<Document>> {
        if is_bitemporal(&*self.storage, collection)
            .await
            .map_err(NodeDbError::storage)?
        {
            let version = versioned_get_current(&*self.storage, collection, id)
                .await
                .map_err(NodeDbError::storage)?;
            return Ok(version.map(|v| decoded_version_to_document(id, &v)));
        }

        let crdt = self.crdt.lock_or_recover();
        let Some(value) = crdt.read(collection, id) else {
            return Ok(None);
        };
        Ok(Some(loro_value_to_document(id, &value)))
    }

    /// Upsert a document.
    ///
    /// For bitemporal collections: writes to the CRDT store first (so sync and
    /// current-state CRDT reads continue to work), then appends a versioned
    /// `LIVE` record to the history table with `system_from_ms = now`.
    ///
    /// For plain collections: unchanged CRDT put + FTS indexing.
    pub(super) async fn document_put_impl(
        &self,
        collection: &str,
        doc: Document,
    ) -> NodeDbResult<()> {
        if self.governor.pressure() == crate::memory::PressureLevel::Critical {
            return Err(NodeDbError::storage(
                crate::error::LiteError::Backpressure {
                    detail: "document put rejected: memory governor is at Critical pressure".into(),
                },
            ));
        }

        let doc_id = if doc.id.is_empty() {
            nodedb_types::id_gen::uuid_v7()
        } else {
            doc.id.clone()
        };

        // Always write to the CRDT store (current-state + sync).
        {
            let mut crdt = self.crdt.lock_or_recover();
            let fields: Vec<(&str, loro::LoroValue)> = doc
                .fields
                .iter()
                .map(|(k, v)| (k.as_str(), value_to_loro(v)))
                .collect();
            let mutation_id = crdt
                .upsert(collection, &doc_id, &fields)
                .map_err(NodeDbError::storage)?;
            // Keep local-only documents out of the outbound CRDT delta stream.
            if !self.should_sync_doc(collection, &doc.fields) {
                crdt.drop_pending(mutation_id);
            }
        }

        // For bitemporal collections, also record the versioned history entry.
        if is_bitemporal(&*self.storage, collection)
            .await
            .map_err(NodeDbError::storage)?
        {
            let now_ms = monotonic_millis_i64();
            let body = document_to_msgpack(&doc);
            versioned_put(
                &*self.storage,
                collection,
                &doc_id,
                &body,
                now_ms,
                // system-time (`now_ms`) is monotonic for a unique history key;
                // valid_from must stay true wall-clock so "valid as-of now"
                // queries see the row immediately (no monotonic future-skew).
                Some(now_millis_i64()),
                None,
            )
            .await
            .map_err(NodeDbError::storage)?;
        }

        self.index_document_text(collection, &doc_id, &doc.fields);
        self.index_document_sparse(collection, &doc_id, &doc.fields);

        Ok(())
    }

    /// Upsert a document and insert its embedding vector under one CRDT lock.
    ///
    /// Performs two logical writes (document upsert + vector metadata upsert) via
    /// a single `batch_upsert` call so Loro exports one oplog delta instead of two.
    /// The HNSW insert and sidecar encoding run after the CRDT lock is released.
    ///
    /// `embedding` being empty is a no-op for the vector path; the document write
    /// proceeds normally.
    pub(super) async fn document_put_with_vector_impl(
        &self,
        doc_collection: &str,
        doc: Document,
        vector_collection: &str,
        id: &str,
        embedding: &[f32],
    ) -> NodeDbResult<()> {
        use crate::engine::crdt::engine::CrdtBatchOp;
        use crate::engine::vector::sidecar;
        use crate::engine::vector::state::ensure_hnsw;
        use nodedb_types::vector_dtype::VectorStorageDtype;

        let doc_id = if doc.id.is_empty() {
            nodedb_types::id_gen::uuid_v7()
        } else {
            doc.id.clone()
        };

        // Build field slices for both ops before acquiring the lock.
        let doc_fields: Vec<(&str, loro::LoroValue)> = doc
            .fields
            .iter()
            .map(|(k, v)| (k.as_str(), value_to_loro(v)))
            .collect();

        let vec_meta_field = loro::LoroValue::I64(embedding.len() as i64);
        let vec_fields: Vec<(&str, loro::LoroValue)> = if !embedding.is_empty() {
            vec![("embedding_dim", vec_meta_field)]
        } else {
            vec![]
        };

        let sync_doc = self.should_sync_doc(doc_collection, &doc.fields);

        // One CRDT lock — one batch_upsert — one Loro oplog export.
        {
            let mut crdt = self.crdt.lock_or_recover();
            let mutation_id = if !embedding.is_empty() {
                let ops: &[CrdtBatchOp<'_>] = &[
                    (doc_collection, &doc_id, doc_fields.as_slice()),
                    (vector_collection, id, vec_fields.as_slice()),
                ];
                crdt.batch_upsert(ops).map_err(NodeDbError::storage)?
            } else {
                crdt.upsert(doc_collection, &doc_id, &doc_fields)
                    .map_err(NodeDbError::storage)?
            };
            // Keep local-only documents out of the outbound CRDT delta stream.
            if !sync_doc {
                crdt.drop_pending(mutation_id);
            }
        }

        // For bitemporal collections, record versioned history (outside the CRDT lock).
        if is_bitemporal(&*self.storage, doc_collection)
            .await
            .map_err(NodeDbError::storage)?
        {
            let now_ms = monotonic_millis_i64();
            let body = document_to_msgpack(&doc);
            versioned_put(
                &*self.storage,
                doc_collection,
                &doc_id,
                &body,
                now_ms,
                // See note above: monotonic system-time key, wall-clock valid_from.
                Some(now_millis_i64()),
                None,
            )
            .await
            .map_err(NodeDbError::storage)?;
        }

        self.index_document_text(doc_collection, &doc_id, &doc.fields);
        self.index_document_sparse(doc_collection, &doc_id, &doc.fields);

        // HNSW insert (no CRDT lock needed — vector_state uses its own locks).
        if !embedding.is_empty() {
            let internal_id = {
                let dtype = {
                    let configs = self.vector_state.per_index_config.lock_or_recover();
                    configs
                        .get(vector_collection)
                        .map(|cfg| cfg.storage_dtype)
                        .unwrap_or(VectorStorageDtype::F32)
                };
                let mut indices = self.vector_state.hnsw_indices.lock_or_recover();
                let index = ensure_hnsw(&mut indices, vector_collection, embedding.len(), dtype);
                let id_before = index.len() as u32;
                index
                    .insert(embedding.to_vec())
                    .map_err(NodeDbError::bad_request)?;
                id_before
            };

            {
                let mut id_map = self.vector_state.vector_id_map.lock_or_recover();
                id_map.insert(
                    format!("{vector_collection}:{internal_id}"),
                    (id.to_string(), internal_id),
                );
            }

            match sidecar::ensure_sidecar(&self.vector_state, vector_collection) {
                Ok(true) => {
                    let mut sidecars = self.vector_state.codec_sidecars.lock_or_recover();
                    if let Some(s) = sidecars.get_mut(vector_collection)
                        && let Err(e) = s.encode_and_insert(internal_id, embedding)
                    {
                        tracing::warn!(
                            index_key = vector_collection,
                            id = internal_id,
                            error = %e,
                            "sidecar encode_and_insert failed; row falls back to FP32 rerank"
                        );
                    }
                }
                Ok(false) => {}
                Err(e) => return Err(NodeDbError::bad_request(e.to_string())),
            }

            #[cfg(not(target_arch = "wasm32"))]
            if sync_doc && let Some(q) = &self.vector_outbound {
                crate::sync::reconcile_outbound_enqueue(
                    q.enqueue_insert(
                        vector_collection,
                        id,
                        embedding.to_vec(),
                        embedding.len(),
                        "",
                    )
                    .await,
                    "vector insert (with document)",
                    vector_collection,
                    id,
                )
                .map_err(NodeDbError::storage)?;
            }

            self.update_memory_stats();
        }

        Ok(())
    }

    /// Delete a document.
    ///
    /// For bitemporal collections: appends a Tombstone version to the history
    /// table (preserves history for AS-OF queries) but does NOT hard-delete from
    /// the CRDT store — the LIVE history entry takes precedence for reads via
    /// `document_get` which now routes through `versioned_get_current`.
    ///
    /// For plain collections: hard-delete from CRDT + FTS removal (unchanged).
    pub(super) async fn document_delete_impl(
        &self,
        collection: &str,
        id: &str,
    ) -> NodeDbResult<()> {
        if is_bitemporal(&*self.storage, collection)
            .await
            .map_err(NodeDbError::storage)?
        {
            let now_ms = monotonic_millis_i64();
            // Monotonic system-time key; wall-clock valid_from so the deletion is
            // visible to "valid as-of now" queries immediately (see versioned_put).
            versioned_tombstone(&*self.storage, collection, id, now_ms, Some(now_millis_i64()))
                .await
                .map_err(NodeDbError::storage)?;
            // FTS removal still applies — the document is logically gone now.
            self.remove_document_text(collection, id);
            self.remove_document_sparse(collection, id);
            return Ok(());
        }

        let mut crdt = self.crdt.lock_or_recover();
        crdt.delete(collection, id).map_err(NodeDbError::storage)?;
        drop(crdt);

        self.remove_document_text(collection, id);
        self.remove_document_sparse(collection, id);

        Ok(())
    }

    /// Read a document as-of a system time, optionally filtered by valid_time.
    ///
    /// Only valid on collections created `WITH (bitemporal=true)`. Returns an
    /// error when called on a plain document collection.
    ///
    /// When `as_of_ms` is `None`, delegates to `versioned_get_current` (same
    /// result as `document_get` for bitemporal collections). When `as_of_ms`
    /// is `Some(t)`, returns the version visible at system time `t`.
    pub(super) async fn document_get_as_of_impl(
        &self,
        collection: &str,
        id: &str,
        as_of_ms: Option<i64>,
        valid_time_ms: Option<i64>,
    ) -> NodeDbResult<Option<Document>> {
        if !is_bitemporal(&*self.storage, collection)
            .await
            .map_err(NodeDbError::storage)?
        {
            return Err(NodeDbError::storage(
                "document_get_as_of requires a collection created WITH (bitemporal=true)",
            ));
        }

        // When as_of_ms is None, use i64::MAX as the system time so we
        // always see the most-recent version — but still apply the
        // valid_time_ms filter via versioned_get_as_of.  Using
        // versioned_get_current would skip the valid_time filter.
        let sys_as_of = as_of_ms.unwrap_or(i64::MAX);
        let version = versioned_get_as_of(&*self.storage, collection, id, sys_as_of, valid_time_ms)
            .await
            .map_err(NodeDbError::storage)?;

        Ok(version.map(|v| decoded_version_to_document(id, &v)))
    }

    /// Put a document with explicit valid-time bounds into a bitemporal collection.
    ///
    /// Only valid on collections created `WITH (bitemporal=true)`. Returns an
    /// error when called on a plain document collection.
    pub(super) async fn document_put_with_valid_time_impl(
        &self,
        collection: &str,
        doc: Document,
        valid_from_ms: Option<i64>,
        valid_until_ms: Option<i64>,
    ) -> NodeDbResult<()> {
        if !is_bitemporal(&*self.storage, collection)
            .await
            .map_err(NodeDbError::storage)?
        {
            return Err(NodeDbError::storage(
                "document_put_with_valid_time requires a collection created WITH (bitemporal=true)",
            ));
        }

        let doc_id = if doc.id.is_empty() {
            nodedb_types::id_gen::uuid_v7()
        } else {
            doc.id.clone()
        };

        // Write to CRDT store for current-state access + sync.
        {
            let mut crdt = self.crdt.lock_or_recover();
            let fields: Vec<(&str, loro::LoroValue)> = doc
                .fields
                .iter()
                .map(|(k, v)| (k.as_str(), value_to_loro(v)))
                .collect();
            crdt.upsert(collection, &doc_id, &fields)
                .map_err(NodeDbError::storage)?;
        }

        let now_ms = monotonic_millis_i64();
        let body = document_to_msgpack(&doc);
        versioned_put(
            &*self.storage,
            collection,
            &doc_id,
            &body,
            now_ms,
            // Monotonic system-time key; an unspecified valid_from means "valid
            // from now", which must be true wall-clock (not the monotonic
            // system time) so it never lands ahead of a concurrent
            // valid_until = now (which would invert the window).
            valid_from_ms.or_else(|| Some(now_millis_i64())),
            valid_until_ms,
        )
        .await
        .map_err(NodeDbError::storage)?;

        self.index_document_text(collection, &doc_id, &doc.fields);
        self.index_document_sparse(collection, &doc_id, &doc.fields);

        Ok(())
    }
}

// ---------------------------------------------------------------------------
// Private helpers
// ---------------------------------------------------------------------------

/// Decode a `DecodedVersion` body (msgpack bytes) into a `Document`.
///
/// Uses `nodedb_types::json_msgpack::value_from_msgpack` for decoding,
/// falling back to an empty document on any parse error.
fn decoded_version_to_document(id: &str, version: &DecodedVersion) -> Document {
    use nodedb_types::value::Value;

    let mut doc = Document::new(id);
    if version.body.is_empty() {
        return doc;
    }

    if let Ok(Value::Object(fields)) = nodedb_types::json_msgpack::value_from_msgpack(&version.body)
    {
        for (k, v) in fields {
            doc.set(k, v);
        }
    }

    doc
}
