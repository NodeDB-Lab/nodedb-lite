// SPDX-License-Identifier: Apache-2.0

//! `NodeDbLite::flush` — persist all in-memory state to storage.

use crate::storage::engine::{StorageEngine, WriteOp};
use nodedb_types::Namespace;
use nodedb_types::error::{NodeDbError, NodeDbResult};

use crate::engine::crdt::CrdtEngine;
use crate::nodedb::lock_ext::LockExt;

use super::types::{
    META_CRDT_DELTAS, META_CRDT_SNAPSHOT, META_CSR_COLLECTIONS, META_HNSW_COLLECTIONS,
    META_LAST_FLUSHED_MID, NodeDbLite,
};

impl<S: StorageEngine> NodeDbLite<S> {
    /// Persist all in-memory state to storage (call before shutdown).
    pub async fn flush(&self) -> NodeDbResult<()> {
        // Drain the buffered KV writes first — they have their own batch-commit
        // path. Without this, `flush()` (and the auto-flush timer) would not
        // persist KV `put`s, contradicting "persist all in-memory state".
        self.kv_flush_inner().await?;

        let mut ops = Vec::new();

        // ── Persist CRDT snapshot (CRC32C wrapped) ──
        {
            let crdt = self.crdt.lock_or_recover();
            let snapshot = crdt.export_snapshot().map_err(NodeDbError::storage)?;
            ops.push(WriteOp::Put {
                ns: Namespace::LoroState,
                key: META_CRDT_SNAPSHOT.to_vec(),
                value: crate::storage::checksum::wrap(&snapshot),
            });

            // Write pending deltas individually (append-only persistence).
            // Each delta is stored under `crdt:delta:{mutation_id:016x}`.
            // Also write the legacy bulk blob for backward compatibility.
            let pending = crdt.pending_deltas();
            let max_mid = pending.iter().map(|d| d.mutation_id).max().unwrap_or(0);

            for delta in pending {
                let key = CrdtEngine::delta_storage_key(delta.mutation_id);
                let value = CrdtEngine::serialize_delta(delta).map_err(NodeDbError::storage)?;
                ops.push(WriteOp::Put {
                    ns: Namespace::Crdt,
                    key,
                    value,
                });
            }

            // Legacy bulk blob (for clients that haven't upgraded to incremental restore).
            let deltas_bulk = crdt
                .serialize_pending_deltas()
                .map_err(NodeDbError::storage)?;
            ops.push(WriteOp::Put {
                ns: Namespace::Crdt,
                key: META_CRDT_DELTAS.to_vec(),
                value: deltas_bulk,
            });

            // Write the last-flushed mutation_id for partial flush safety.
            ops.push(WriteOp::Put {
                ns: Namespace::Meta,
                key: META_LAST_FLUSHED_MID.to_vec(),
                value: max_mid.to_le_bytes().to_vec(),
            });
        }

        // ── Persist per-collection CSR indices ──
        // When the pagedb segment extension is available (native PagedbStorage):
        //   - CSR blob → pagedb segment (written after batch_write)
        //   - B+ tree receives only the collection-name index (META_CSR_COLLECTIONS)
        // Otherwise (WASM or non-pagedb native backends):
        //   - CSR blob → B+ tree (Namespace::Graph, CRC32C wrapped)
        #[cfg(not(target_arch = "wasm32"))]
        let graph_seg_ext = self.storage.as_graph_segment_ext();
        #[cfg_attr(target_arch = "wasm32", allow(unused_variables))]
        let csr_segment_data: Vec<(String, Vec<u8>)> = {
            let csr_map = self.csr.lock_or_recover();
            let names: Vec<String> = csr_map.keys().cloned().collect();
            let names_bytes = zerompk::to_msgpack_vec(&names)
                .map_err(|e| NodeDbError::serialization("msgpack", e))?;
            ops.push(WriteOp::Put {
                ns: Namespace::Meta,
                key: META_CSR_COLLECTIONS.to_vec(),
                value: names_bytes,
            });

            // Mutated only via the native segment-ext path, compiled out on wasm32.
            #[cfg_attr(target_arch = "wasm32", allow(unused_mut))]
            let mut segment_data = Vec::new();
            for (name, index) in csr_map.iter() {
                match index.checkpoint_to_bytes() {
                    Ok(checkpoint) => {
                        #[cfg(not(target_arch = "wasm32"))]
                        {
                            if graph_seg_ext.is_some() {
                                // Pagedb segment path: collect for post-batch write.
                                segment_data.push((name.clone(), checkpoint));
                            } else {
                                // Legacy B+ tree path.
                                let key = format!("csr:{name}");
                                ops.push(WriteOp::Put {
                                    ns: Namespace::Graph,
                                    key: key.into_bytes(),
                                    value: crate::storage::checksum::wrap(&checkpoint),
                                });
                            }
                        }
                        #[cfg(target_arch = "wasm32")]
                        {
                            let key = format!("csr:{name}");
                            ops.push(WriteOp::Put {
                                ns: Namespace::Graph,
                                key: key.into_bytes(),
                                value: crate::storage::checksum::wrap(&checkpoint),
                            });
                        }
                    }
                    Err(e) => {
                        tracing::error!(
                            collection = %name,
                            error = %e,
                            "CSR checkpoint failed for collection; graph state not persisted"
                        );
                    }
                }
            }
            segment_data
        };

        // ── Persist HNSW vector_id_map ──
        // The id_map is a flat HashMap<composite_key, (doc_id, internal_id)>
        // serialized as one MessagePack blob. It must be written before any restart
        // so that vector_search can return real doc_ids (not HNSW integer strings).
        // Vector search with an empty id_map after restart is the bug this fixes.
        // Vectors are flush-only (no per-insert durability path); the id_map
        // follows the same durability contract — flush required.
        {
            let id_map = self.vector_state.vector_id_map.lock_or_recover();
            // Serialize as Vec<(composite_key, doc_id, internal_id)> for stable msgpack encoding.
            let entries: Vec<(&str, &str, u32)> = id_map
                .iter()
                .map(|(k, (doc_id, iid))| (k.as_str(), doc_id.as_str(), *iid))
                .collect();
            match zerompk::to_msgpack_vec(&entries) {
                Ok(bytes) => {
                    ops.push(WriteOp::Put {
                        ns: Namespace::Vector,
                        key: b"hnsw_id_map".to_vec(),
                        value: crate::storage::checksum::wrap(&bytes),
                    });
                }
                Err(e) => {
                    tracing::error!(
                        error = %e,
                        "vector_id_map serialization failed; \
                         vector search after restart will fall back to HNSW integer IDs"
                    );
                }
            }
        }

        // ── Persist HNSW indices ──
        // When the pagedb segment extension is available (native PagedbStorage):
        //   - graph topology blob → B+ tree (graph_checkpoint_to_bytes; empty vector slots)
        //   - vector data → pagedb segment (written after batch_write)
        // Otherwise (WASM or legacy backends):
        //   - full checkpoint blob → B+ tree (checkpoint_to_bytes)
        #[cfg(not(target_arch = "wasm32"))]
        let seg_ext = self.storage.as_vector_segment_ext();
        #[cfg_attr(
            target_arch = "wasm32",
            allow(unused_variables, clippy::type_complexity)
        )]
        #[allow(clippy::type_complexity)]
        let hnsw_segment_data: Vec<(String, usize, Vec<Vec<f32>>, Vec<u64>)> = {
            let indices = self.vector_state.hnsw_indices.lock_or_recover();
            let names: Vec<String> = indices.keys().cloned().collect();
            let names_bytes = zerompk::to_msgpack_vec(&names)
                .map_err(|e| NodeDbError::serialization("msgpack", e))?;
            ops.push(WriteOp::Put {
                ns: Namespace::Meta,
                key: META_HNSW_COLLECTIONS.to_vec(),
                value: names_bytes,
            });

            // Mutated only via the native segment-ext path, compiled out on wasm32.
            #[cfg_attr(target_arch = "wasm32", allow(unused_mut))]
            let mut segment_data = Vec::new();
            for (name, index) in indices.iter() {
                let key = format!("hnsw:{name}");

                #[cfg(not(target_arch = "wasm32"))]
                {
                    if seg_ext.is_some() {
                        // Graph-only blob (vector bytes are empty placeholders).
                        let graph_bytes = index
                            .graph_checkpoint_to_bytes()
                            .map_err(|e| NodeDbError::serialization("hnsw-graph-checkpoint", e))?;
                        ops.push(WriteOp::Put {
                            ns: Namespace::Vector,
                            key: key.into_bytes(),
                            value: crate::storage::checksum::wrap(&graph_bytes),
                        });
                        // Collect vector + surrogate data for segment write after batch_write.
                        let (vectors, surrogates) = index.extract_vectors_and_surrogates();
                        let dim = index.dim();
                        segment_data.push((name.clone(), dim, vectors, surrogates));
                    } else {
                        // Non-pagedb native backend: full checkpoint blob path.
                        let checkpoint = index
                            .checkpoint_to_bytes()
                            .map_err(|e| NodeDbError::serialization("hnsw-checkpoint", e))?;
                        ops.push(WriteOp::Put {
                            ns: Namespace::Vector,
                            key: key.into_bytes(),
                            value: crate::storage::checksum::wrap(&checkpoint),
                        });
                    }
                }
                #[cfg(target_arch = "wasm32")]
                {
                    // WASM: full checkpoint blob path (no segment ops).
                    let checkpoint = index
                        .checkpoint_to_bytes()
                        .map_err(|e| NodeDbError::serialization("hnsw-checkpoint", e))?;
                    ops.push(WriteOp::Put {
                        ns: Namespace::Vector,
                        key: key.into_bytes(),
                        value: crate::storage::checksum::wrap(&checkpoint),
                    });
                }
            }
            segment_data
        };

        self.storage
            .batch_write(&ops)
            .await
            .map_err(NodeDbError::storage)?;

        // ── Write HNSW vector segments to pagedb (native PagedbStorage only) ──
        #[cfg(not(target_arch = "wasm32"))]
        if let Some(ext) = seg_ext {
            for (name, dim, vectors, surrogates) in &hnsw_segment_data {
                if let Err(e) = ext
                    .write_vector_segment(name, *dim, vectors, surrogates)
                    .await
                {
                    tracing::error!(
                        collection = %name,
                        error = %e,
                        "HNSW vector segment write failed; \
                         graph topology is persisted but vectors may be lost on cold restart"
                    );
                }
            }
        }

        // ── Write CSR adjacency segments to pagedb (native PagedbStorage only) ──
        #[cfg(not(target_arch = "wasm32"))]
        if let Some(ext) = graph_seg_ext {
            for (name, checkpoint) in &csr_segment_data {
                if let Err(e) = ext.write_graph_segment(name, checkpoint).await {
                    tracing::error!(
                        collection = %name,
                        error = %e,
                        "CSR adjacency segment write failed; \
                         graph state may be lost on cold restart"
                    );
                }
            }
        }

        // ── Persist spatial indices (separate batch — includes docmap) ────────
        let (spatial_checkpoints, spatial_doc_to_entry, spatial_next_id) =
            self.spatial.lock_or_recover().checkpoint_data();
        crate::engine::spatial::checkpoint::flush_spatial(
            self.storage.as_ref(),
            &spatial_checkpoints,
            &spatial_doc_to_entry,
            spatial_next_id,
        )
        .await?;

        // ── Persist FTS indices (separate batch — potentially large) ──
        // Serialize is synchronous (no I/O); do it inside the lock so we don't
        // need to clone FtsIndex.  The resulting ops + segment blobs are written
        // to storage after the lock is released.
        let (fts_ops, fts_segment_writes) = {
            let fts = self.fts_state.manager.lock_or_recover();
            let (indices, id_to_surrogate, next_surrogate) = fts.checkpoint_data();
            crate::engine::fts::checkpoint::serialize_fts(indices, id_to_surrogate, next_surrogate)
                .map_err(|e| NodeDbError::storage(format!("fts serialize: {e}")))?
        };
        crate::engine::fts::checkpoint::write_serialized_fts(
            self.storage.as_ref(),
            fts_ops,
            fts_segment_writes,
        )
        .await
        .map_err(|e| NodeDbError::storage(format!("fts flush: {e}")))?;

        // ── Persist sparse-vector inverted indices ────────────────────────────
        // Same shape as the FTS block: serialize synchronously under the lock,
        // then perform the storage write after releasing it.
        let sparse_ops = {
            let sparse = self.sparse_state.manager.lock_or_recover();
            crate::engine::sparse_vector::checkpoint::serialize_sparse(sparse.checkpoint_data())
                .map_err(|e| NodeDbError::storage(format!("sparse serialize: {e}")))?
        };
        crate::engine::sparse_vector::checkpoint::write_serialized_sparse(
            self.storage.as_ref(),
            sparse_ops,
        )
        .await
        .map_err(|e| NodeDbError::storage(format!("sparse flush: {e}")))?;

        // ── Spill FTS + spatial staging buffers to durable queues ────────────
        // These queues accumulate sync entries written synchronously by
        // `index_document_text`, `remove_document_text`, `spatial_insert`, and
        // `spatial_delete`. Spilling here (async, ~every second) keeps the
        // staging buffers bounded and ensures entries are durable before the
        // next sync transport drain.
        #[cfg(not(target_arch = "wasm32"))]
        if let Some(q) = &self.fts_outbound
            && let Err(e) = q.flush_staging().await
        {
            tracing::warn!(error = %e, "fts outbound flush_staging failed; \
                    staged entries remain and will be retried on next flush");
        }
        #[cfg(not(target_arch = "wasm32"))]
        if let Some(q) = &self.spatial_outbound
            && let Err(e) = q.flush_staging().await
        {
            tracing::warn!(error = %e, "spatial outbound flush_staging failed; \
                    staged entries remain and will be retried on next flush");
        }

        Ok(())
    }
}
