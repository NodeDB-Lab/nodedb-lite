// SPDX-License-Identifier: Apache-2.0

//! Cold-start rebuild helpers for the CSR graph adjacency index.
//!
//! `rebuild_graph_indices` is called from `open_inner` when
//! `restore_csr_indices` returns an empty map — i.e. when no checkpoint was
//! written before the previous process exited. Three passes:
//!
//! - Pass 1 — CRDT edge scan (edges written via the trait API path that store
//!   edge documents in `__edges__{collection}` CRDT collections).
//! - Pass 2 — `Namespace::Graph` KV scan (edges written via the SQL visitor
//!   path, which writes directly under NUL-delimited keys).
//! - Pass 3 — `Namespace::GraphHistory` scan for bitemporal collections (covers
//!   CRDT-path edges whose CRDT snapshot may not have been flushed before exit).

use std::collections::{HashMap, HashSet};

use nodedb_types::Namespace;
use nodedb_types::id::EdgeId;

use crate::engine::graph::history::SYSTEM_TO_CURRENT;
use crate::engine::graph::index::CsrIndex;
use crate::nodedb::lock_ext::LockExt;
use crate::storage::engine::StorageEngine;

use crate::nodedb::core::types::NodeDbLite;

/// Meta key prefix for the graph bitemporal flag (mirrors `engine::graph::history`).
const META_GRAPH_BITEMPORAL_PREFIX: &str = "graph_bitemporal:";

/// Trailer size of each `Namespace::GraphHistory` value:
/// 8-byte big-endian `system_to_ms`.
const GRAPH_HISTORY_TRAILER_LEN: usize = 8;

impl<S: StorageEngine> NodeDbLite<S> {
    /// Rebuild all graph CSR adjacency indices from durable storage on cold start.
    ///
    /// See the module-level doc for the three-pass strategy.
    pub(crate) async fn rebuild_graph_indices(&self) {
        // ── Pass 1: CRDT edge scan ────────────────────────────────────────────
        // Track (collection, src, label, dst) tuples to deduplicate across passes.
        let mut indexed: HashSet<(String, String, String, String)> = HashSet::new();

        {
            let crdt = self.crdt.lock_or_recover();
            let collections = crdt.collection_names();
            let mut csr_map = self.csr.lock_or_recover();

            for crdt_coll in &collections {
                // Edge collections are named `__edges__{collection}`.
                let Some(collection) = crdt_coll.strip_prefix("__edges__") else {
                    continue;
                };
                let ids = crdt.list_ids(crdt_coll);
                if ids.is_empty() {
                    continue;
                }

                let csr = csr_map
                    .entry(collection.to_string())
                    .or_insert_with(CsrIndex::new);

                for id in &ids {
                    if let Some(loro_val) = crdt.read(crdt_coll, id) {
                        let doc = crate::nodedb::convert::loro_value_to_document(id, &loro_val);
                        let src = doc.fields.get("src").and_then(|v| match v {
                            nodedb_types::Value::String(s) => Some(s.clone()),
                            _ => None,
                        });
                        let dst = doc.fields.get("dst").and_then(|v| match v {
                            nodedb_types::Value::String(s) => Some(s.clone()),
                            _ => None,
                        });
                        let label = doc.fields.get("label").and_then(|v| match v {
                            nodedb_types::Value::String(s) => Some(s.clone()),
                            _ => None,
                        });
                        if let (Some(src), Some(dst), Some(label)) = (src, dst, label) {
                            let _ = csr.add_edge(&src, &label, &dst);
                            indexed.insert((collection.to_string(), src, label, dst));
                        }
                    }
                }
            }
        }

        // ── Pass 2: Namespace::Graph KV edge scan ─────────────────────────────
        // Key format: `{collection}\x00{src}\x00{label}\x00{dst}`.
        // Non-edge keys (e.g. legacy `csr:*` checkpoint blobs) contain no NUL
        // bytes and are skipped by the 4-segment check.
        let graph_entries = match self.storage.scan_prefix(Namespace::Graph, b"").await {
            Ok(e) => e,
            Err(e) => {
                tracing::warn!(
                    error = %e,
                    "graph rebuild: failed to scan Namespace::Graph; skipping KV pass"
                );
                Vec::new()
            }
        };

        {
            let mut csr_map = self.csr.lock_or_recover();
            for (key, _value) in &graph_entries {
                // Only process keys that split into exactly 4 NUL-separated segments.
                let parts: Vec<&[u8]> = key.splitn(4, |&b| b == 0).collect();
                if parts.len() != 4 {
                    continue;
                }
                let Ok(collection) = std::str::from_utf8(parts[0]) else {
                    continue;
                };
                let Ok(src) = std::str::from_utf8(parts[1]) else {
                    continue;
                };
                let Ok(label) = std::str::from_utf8(parts[2]) else {
                    continue;
                };
                let Ok(dst) = std::str::from_utf8(parts[3]) else {
                    continue;
                };
                let tuple = (
                    collection.to_string(),
                    src.to_string(),
                    label.to_string(),
                    dst.to_string(),
                );
                if indexed.contains(&tuple) {
                    continue;
                }
                let csr = csr_map
                    .entry(collection.to_string())
                    .or_insert_with(CsrIndex::new);
                let _ = csr.add_edge(src, label, dst);
                indexed.insert(tuple);
            }
        }

        // ── Pass 3: GraphHistory scan (bitemporal collections only) ───────────
        let bitemporal_collections = self.list_bitemporal_graph_collections().await;

        for collection in &bitemporal_collections {
            let prefix = {
                let mut p = collection.as_bytes().to_vec();
                p.push(b':');
                p
            };
            let entries = match self
                .storage
                .scan_prefix(Namespace::GraphHistory, &prefix)
                .await
            {
                Ok(e) => e,
                Err(e) => {
                    tracing::warn!(
                        collection = %collection,
                        error = %e,
                        "graph rebuild: failed to scan GraphHistory; skipping collection"
                    );
                    continue;
                }
            };

            // Collect the maximum `system_to` seen per edge_key.
            // Key format: `{collection}:{edge_key}:{system_from_ms_8be}`.
            // The `system_from_ms` is always 8 raw big-endian bytes appended to
            // the key. We strip `{prefix}` (collection + ':') plus the trailing
            // `:{8 bytes}` from the raw key to isolate `edge_key`.
            let mut edge_latest_system_to: HashMap<String, u64> = HashMap::new();

            for (key, value) in &entries {
                if value.len() < GRAPH_HISTORY_TRAILER_LEN {
                    continue;
                }
                // key = prefix || edge_key_bytes || b':' || system_from_8be
                // Minimum key length: prefix.len() + 1 (separator) + 8 (timestamp).
                if key.len() < prefix.len() + 1 + GRAPH_HISTORY_TRAILER_LEN {
                    continue;
                }
                let edge_key_bytes = &key[prefix.len()..key.len() - 1 - GRAPH_HISTORY_TRAILER_LEN];
                let Ok(edge_key) = std::str::from_utf8(edge_key_bytes) else {
                    continue;
                };

                let start = value.len() - GRAPH_HISTORY_TRAILER_LEN;
                let system_to = u64::from_be_bytes(value[start..].try_into().unwrap_or([0; 8]));

                let entry = edge_latest_system_to
                    .entry(edge_key.to_string())
                    .or_insert(0);
                if system_to > *entry {
                    *entry = system_to;
                }
            }

            // Add live edges (system_to == u64::MAX) whose edge_key uses the
            // EdgeId Display format (CRDT-API path). KV-path edge keys use a
            // different format and are already covered by Pass 2.
            let mut csr_map = self.csr.lock_or_recover();
            let csr = csr_map
                .entry(collection.to_string())
                .or_insert_with(CsrIndex::new);

            for (edge_key, system_to) in &edge_latest_system_to {
                if *system_to != SYSTEM_TO_CURRENT {
                    continue;
                }
                // Parse via EdgeId::from_str; skip keys in other formats.
                let Ok(edge_id) = edge_key.parse::<EdgeId>() else {
                    continue;
                };
                let src = edge_id.src.as_str();
                let label = &edge_id.label;
                let dst = edge_id.dst.as_str();

                let tuple = (
                    collection.to_string(),
                    src.to_string(),
                    label.to_string(),
                    dst.to_string(),
                );
                if indexed.contains(&tuple) {
                    continue;
                }
                let _ = csr.add_edge(src, label, dst);
                indexed.insert(tuple);
            }
        }
    }

    /// Return the names of all graph collections that have the bitemporal flag set.
    ///
    /// Reads `Namespace::Meta` keys prefixed with `graph_bitemporal:` and returns
    /// only those whose stored byte equals `0x01` (enabled).
    async fn list_bitemporal_graph_collections(&self) -> Vec<String> {
        let prefix = META_GRAPH_BITEMPORAL_PREFIX.as_bytes();
        let entries = match self.storage.scan_prefix(Namespace::Meta, prefix).await {
            Ok(e) => e,
            Err(e) => {
                tracing::warn!(
                    error = %e,
                    "graph rebuild: failed to scan bitemporal graph meta keys"
                );
                return Vec::new();
            }
        };

        entries
            .into_iter()
            .filter_map(|(key, value)| {
                if value.first().copied() != Some(1) {
                    return None;
                }
                let key_str = std::str::from_utf8(&key).ok()?;
                key_str
                    .strip_prefix(META_GRAPH_BITEMPORAL_PREFIX)
                    .map(str::to_owned)
            })
            .collect()
    }
}
