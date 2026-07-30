// SPDX-License-Identifier: Apache-2.0

//! Per-collection CSR graph index restore.

use std::collections::HashMap;
use std::sync::Arc;

use nodedb_types::Namespace;
use nodedb_types::error::NodeDbResult;

use crate::engine::graph::index::CsrIndex;
use crate::storage::engine::StorageEngine;

use crate::nodedb::core::types::{META_CSR_COLLECTIONS, NodeDbLite};

impl<S: StorageEngine> NodeDbLite<S> {
    /// Restore per-collection CSR graph indices from storage.
    ///
    /// On native targets with `PagedbStorage`, CSR blobs are read from pagedb
    /// segments (segment-first, then fall back to the legacy B+ tree KV blob
    /// for databases written by older builds).  On WASM, only the B+ tree path
    /// is used.
    pub(in crate::nodedb::core::open) async fn restore_csr_indices(
        storage: &Arc<S>,
    ) -> NodeDbResult<HashMap<String, CsrIndex>> {
        let mut csr_map: HashMap<String, CsrIndex> = HashMap::new();
        let Some(collections_bytes) = storage.get(Namespace::Meta, META_CSR_COLLECTIONS).await?
        else {
            return Ok(csr_map);
        };
        let Ok(names) = zerompk::from_msgpack::<Vec<String>>(&collections_bytes) else {
            return Ok(csr_map);
        };

        // On native targets, prefer the pagedb segment path when available.
        #[cfg(not(target_arch = "wasm32"))]
        let graph_seg_ext = storage.as_graph_segment_ext();

        for name in &names {
            // ── Segment path (native PagedbStorage) ──
            #[cfg(not(target_arch = "wasm32"))]
            if let Some(ext) = graph_seg_ext {
                match ext.open_graph_segment(name).await {
                    Ok(Some(bytes)) => {
                        match CsrIndex::from_checkpoint(&bytes) {
                            Ok(Some(idx)) => {
                                csr_map.insert(name.clone(), idx);
                            }
                            Ok(None) | Err(_) => {
                                tracing::warn!(
                                    collection = %name,
                                    "CSR segment deserialization failed, will rebuild from CRDT"
                                );
                            }
                        }
                        continue;
                    }
                    Ok(None) => {
                        // No segment yet — fall through to legacy B+ tree path below.
                    }
                    Err(e) => {
                        tracing::warn!(
                            collection = %name,
                            error = %e,
                            "CSR segment open failed, falling back to legacy B+ tree path"
                        );
                    }
                }
            }

            // ── Legacy B+ tree path (WASM or pre-migration data) ──
            let key = format!("csr:{name}");
            if let Some(envelope) = storage.get(Namespace::Graph, key.as_bytes()).await? {
                match crate::storage::checksum::unwrap(&envelope) {
                    Some(bytes) => match CsrIndex::from_checkpoint(&bytes) {
                        Ok(Some(idx)) => {
                            csr_map.insert(name.clone(), idx);
                        }
                        Ok(None) | Err(_) => {
                            tracing::warn!(
                                collection = %name,
                                "CSR checkpoint deserialization failed, will rebuild from CRDT"
                            );
                        }
                    },
                    None => {
                        tracing::error!(
                            collection = %name,
                            "CSR checkpoint CRC32C mismatch — discarding. \
                             Will rebuild from CRDT edge documents on next insert."
                        );
                        let _ = storage.delete(Namespace::Graph, key.as_bytes()).await;
                    }
                }
            }
        }
        Ok(csr_map)
    }
}
