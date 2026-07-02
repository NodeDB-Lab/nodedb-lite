// SPDX-License-Identifier: Apache-2.0

//! Cold-start rebuild helpers for FTS (text) and Spatial indices.
//!
//! - `rebuild_text_indices` — two-pass: CRDT scan + DocumentHistory scan for
//!   bitemporal collections.
//! - `rebuild_spatial_indices` — single-pass CRDT scan for geometry fields.

use std::collections::HashSet;

use nodedb_types::Namespace;

use crate::engine::document::history::key::coll_prefix;
use crate::engine::document::history::ops::versioned_get_current;
use crate::engine::document::history::value::VersionTag;
use crate::nodedb::lock_ext::LockExt;
use crate::storage::engine::StorageEngine;

use crate::nodedb::core::types::NodeDbLite;

/// Meta key prefix for the document bitemporal flag (mirrors `history::ops`).
const META_DOCUMENT_BITEMPORAL_PREFIX: &str = "document_bitemporal:";

impl<S: StorageEngine> NodeDbLite<S> {
    /// Rebuild all text indices from CRDT state and, for bitemporal collections,
    /// from the authoritative `DocumentHistory` table.
    ///
    /// Called once on cold start after CRDT snapshot restore, when no FTS
    /// checkpoint is present. Two-pass approach:
    ///
    /// **Pass 1 — CRDT scan** (non-bitemporal collections):
    /// Reads every document from the Loro CRDT engine and indexes all string
    /// fields. Bitemporal collections may not have had their CRDT snapshot
    /// flushed before the previous process exited, so Pass 1 alone is
    /// insufficient for them.
    ///
    /// **Pass 2 — DocumentHistory scan** (bitemporal collections only):
    /// Enumerates every collection flagged as bitemporal via `Namespace::Meta`,
    /// prefix-scans `Namespace::DocumentHistory` for unique doc_ids, fetches the
    /// current live version of each, and indexes its string fields. Documents
    /// already indexed in Pass 1 are skipped to avoid duplicate work.
    pub(crate) async fn rebuild_text_indices(&self) {
        // ── Pass 1: CRDT scan (non-bitemporal collections) ───────────────────
        // Collect doc_ids indexed in this pass so Pass 2 can skip duplicates.
        let mut indexed: HashSet<(String, String)> = HashSet::new();

        {
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
                        indexed.insert((collection.clone(), id.clone()));
                    }
                }
            }
        }

        // ── Pass 2: DocumentHistory scan (bitemporal collections) ─────────────
        // Enumerate all collections flagged as bitemporal from Meta.
        let bitemporal_collections = self.list_bitemporal_collections().await;

        for collection in &bitemporal_collections {
            // Collect unique doc_ids from the history key prefix.
            let unique_ids = self.collect_doc_ids_from_history(collection).await;

            for doc_id in &unique_ids {
                // Skip if already indexed from CRDT in Pass 1.
                if indexed.contains(&(collection.clone(), doc_id.clone())) {
                    continue;
                }

                // Fetch the current live version from the history table.
                let version = match versioned_get_current(&*self.storage, collection, doc_id).await
                {
                    Ok(v) => v,
                    Err(e) => {
                        tracing::warn!(
                            collection = %collection,
                            doc_id = %doc_id,
                            error = %e,
                            "FTS rebuild: failed to fetch history version; skipping"
                        );
                        continue;
                    }
                };

                let Some(version) = version else {
                    // Tombstoned or missing — do not index.
                    continue;
                };

                if version.tag != VersionTag::Live {
                    continue;
                }

                // Decode the msgpack body and extract string fields.
                if let Ok(nodedb_types::Value::Object(fields)) =
                    nodedb_types::json_msgpack::value_from_msgpack(&version.body)
                {
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

    /// Return the names of all collections that have the bitemporal flag set.
    ///
    /// Reads `Namespace::Meta` keys prefixed with `document_bitemporal:` and
    /// returns only those whose stored byte equals `0x01` (enabled).
    async fn list_bitemporal_collections(&self) -> Vec<String> {
        let prefix = META_DOCUMENT_BITEMPORAL_PREFIX.as_bytes();
        let entries = match self.storage.scan_prefix(Namespace::Meta, prefix).await {
            Ok(e) => e,
            Err(e) => {
                tracing::warn!(error = %e, "FTS rebuild: failed to scan bitemporal meta keys");
                return Vec::new();
            }
        };

        entries
            .into_iter()
            .filter_map(|(key, value)| {
                // Value byte 0x01 = bitemporal enabled.
                if value.first().copied() != Some(1) {
                    return None;
                }
                // Strip the prefix to recover the collection name.
                let key_str = std::str::from_utf8(&key).ok()?;
                key_str
                    .strip_prefix(META_DOCUMENT_BITEMPORAL_PREFIX)
                    .map(str::to_owned)
            })
            .collect()
    }

    /// Collect unique doc_ids from `Namespace::DocumentHistory` for `collection`.
    ///
    /// Key format: `{collection}:{doc_id}\x00{system_from_ms:020}`. Splits on
    /// the NUL separator to extract `{doc_id}` and deduplicates across versions.
    async fn collect_doc_ids_from_history(&self, collection: &str) -> Vec<String> {
        let prefix = coll_prefix(collection);
        let entries = match self
            .storage
            .scan_prefix(Namespace::DocumentHistory, &prefix)
            .await
        {
            Ok(e) => e,
            Err(e) => {
                tracing::warn!(
                    collection = %collection,
                    error = %e,
                    "FTS rebuild: failed to scan document history; skipping collection"
                );
                return Vec::new();
            }
        };

        let prefix_str = format!("{collection}:");
        let mut seen: HashSet<String> = HashSet::new();
        let mut ids: Vec<String> = Vec::new();

        for (key, _value) in &entries {
            let Ok(key_str) = std::str::from_utf8(key) else {
                continue;
            };
            // key_str = "{collection}:{doc_id}\x00{timestamp}"
            // Split on NUL to get the "{collection}:{doc_id}" part.
            let Some(coll_and_id) = key_str.split('\x00').next() else {
                continue;
            };
            let Some(doc_id) = coll_and_id.strip_prefix(&prefix_str) else {
                continue;
            };
            if seen.insert(doc_id.to_owned()) {
                ids.push(doc_id.to_owned());
            }
        }

        ids
    }
}
