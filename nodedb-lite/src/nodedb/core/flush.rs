// SPDX-License-Identifier: Apache-2.0

//! `NodeDbLite::flush` — persist all in-memory state to storage.

use crate::storage::engine::{StorageEngine, WriteOp};
use nodedb_types::Namespace;
use nodedb_types::error::{NodeDbError, NodeDbResult};

use crate::engine::crdt::{CrdtEngine, CrdtWriteKind};
use crate::nodedb::lock_ext::LockExt;

use super::types::{
    META_CRDT_DELTAS, META_CSR_COLLECTIONS, META_HNSW_COLLECTIONS, META_LAST_FLUSHED_MID,
    NodeDbLite,
};

impl<S: StorageEngine> NodeDbLite<S> {
    /// Number of full CRDT snapshot exports performed since this handle was
    /// opened.
    ///
    /// A snapshot export costs O(document), so this is the term that decides
    /// both flush latency and how much the file grows per tick. It is a
    /// counter, not a timing, so a caller can assert on it directly: an idle
    /// store must not advance it.
    pub fn crdt_snapshot_export_count(&self) -> u64 {
        self.crdt.lock_or_recover().snapshot_export_count()
    }

    /// Number of unsent-delta queue entries written since this handle was
    /// opened.
    ///
    /// The queue is append-only, so this advances by what was added, not by
    /// the queue's length. An idle store must not advance it at all.
    pub fn crdt_delta_write_count(&self) -> u64 {
        self.crdt.lock_or_recover().pending_delta_write_count()
    }

    /// Persist all in-memory state to storage (call before shutdown).
    pub async fn flush(&self) -> NodeDbResult<()> {
        // One flush at a time: the CRDT update sequence is allocated under the
        // `crdt` guard but committed after it is released, so concurrent
        // flushes would hand out the same numbers. See `flush_lock`.
        let _flush_guard = self.flush_lock.lock().await;

        // Drain the buffered KV writes first — they have their own batch-commit
        // path. Without this, `flush()` (and the auto-flush timer) would not
        // persist KV `put`s, contradicting "persist all in-memory state".
        self.kv_flush_inner().await?;

        let mut ops = Vec::new();

        // Delta entries already on disk. Restore prefers these over the bulk
        // blob, so any one that is no longer pending — acknowledged by Origin,
        // or replaced by a peer-id rotation that re-authored the row — would
        // come back on the next open and be pushed again. They are deleted in
        // the same batch that writes the current set.
        let persisted_delta_keys: Vec<Vec<u8>> = self
            .storage
            .scan_prefix(Namespace::Crdt, b"delta:")
            .await?
            .into_iter()
            .map(|(key, _)| key)
            .collect();

        // ── Persist one CRDT snapshot per collection (CRC32C wrapped) ──
        // Each collection owns its own Loro document, so each gets its own
        // storage entry under `loro_snapshot:<collection>`.
        //
        // A collection whose frontier has not moved is not written at all; one
        // that has moved is written as an update since its last persisted
        // frontier, and only periodically as a fresh snapshot. Exporting a full
        // snapshot per collection per tick cost O(document) regardless of the
        // write rate — unbounded file growth on an otherwise idle store, and an
        // export duty cycle that starved readers once the document outgrew the
        // flush interval.
        let (persisted, written_deltas) = {
            let crdt = self.crdt.lock_or_recover();
            let plan = crdt.plan_persistence().map_err(NodeDbError::storage)?;
            let mut persisted = Vec::with_capacity(plan.len());
            for write in plan {
                persisted.push(write.persisted());
                match write.kind {
                    CrdtWriteKind::Checkpoint { superseded_deltas } => {
                        // In the same batch as the new base, so no restore ever
                        // sees a base with updates in front of it that it
                        // already contains.
                        for seq in 0..superseded_deltas {
                            ops.push(WriteOp::Delete {
                                ns: Namespace::LoroState,
                                key: CrdtEngine::state_delta_key_for(&write.collection, seq),
                            });
                        }
                        ops.push(WriteOp::Put {
                            ns: Namespace::LoroState,
                            key: CrdtEngine::snapshot_key_for(&write.collection),
                            value: crate::storage::checksum::wrap(&write.bytes),
                        });
                    }
                    CrdtWriteKind::Delta { seq } => {
                        ops.push(WriteOp::Put {
                            ns: Namespace::LoroState,
                            key: CrdtEngine::state_delta_key_for(&write.collection, seq),
                            value: crate::storage::checksum::wrap(&write.bytes),
                        });
                    }
                }
            }

            // Write pending deltas individually (append-only persistence).
            // Each delta is stored under `crdt:delta:{mutation_id:016x}`.
            //
            // Only the entries added or edited since the last flush are
            // written. The queue is append-only and each entry owns its key,
            // so rewriting an unchanged one stores bytes identical to the ones
            // already there — and a replica with no Origin to acknowledge its
            // deltas accumulates them without bound, which made that rewrite
            // the whole outbox, once per `auto_flush_ms`.
            let pending = crdt.pending_deltas();
            let max_mid = pending.iter().map(|d| d.mutation_id).max().unwrap_or(0);

            let live_keys: std::collections::HashSet<Vec<u8>> = pending
                .iter()
                .map(|d| CrdtEngine::delta_storage_key(d.mutation_id))
                .collect();
            let mut retired_any = false;
            for key in persisted_delta_keys {
                if !live_keys.contains(&key) {
                    retired_any = true;
                    ops.push(WriteOp::Delete {
                        ns: Namespace::Crdt,
                        key,
                    });
                }
            }

            // The revision each entry was written at travels with it: the
            // acknowledgement below happens after an await, and an entry queued
            // or re-sequenced in that window was never in this batch.
            let mut written_deltas: Vec<(u64, u64)> = Vec::new();
            for (delta, revision) in crdt.pending_deltas_needing_write() {
                let key = CrdtEngine::delta_storage_key(delta.mutation_id);
                let value = CrdtEngine::serialize_delta(delta).map_err(NodeDbError::storage)?;
                written_deltas.push((delta.mutation_id, revision));
                ops.push(WriteOp::Put {
                    ns: Namespace::Crdt,
                    key,
                    value,
                });
            }

            // Legacy bulk blob (for clients that haven't upgraded to incremental
            // restore). It duplicates every entry above, so it is rewritten only
            // when the queue actually changed rather than on every tick.
            let queue_changed = retired_any || crdt.has_unpersisted_deltas();
            if queue_changed {
                let deltas_bulk = crdt
                    .serialize_pending_deltas()
                    .map_err(NodeDbError::storage)?;
                ops.push(WriteOp::Put {
                    ns: Namespace::Crdt,
                    key: META_CRDT_DELTAS.to_vec(),
                    value: deltas_bulk,
                });
            }

            // Write the last-flushed mutation_id for partial flush safety.
            ops.push(WriteOp::Put {
                ns: Namespace::Meta,
                key: META_LAST_FLUSHED_MID.to_vec(),
                value: max_mid.to_le_bytes().to_vec(),
            });

            (persisted, written_deltas)
        };

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
            let mut segment_data: Vec<(String, Vec<u8>)> = Vec::new();
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
        let hnsw_segment_names: Vec<String> = {
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
            let mut segment_names: Vec<String> = Vec::new();
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
                        // The segment payload is sourced from the DURABLE vectors
                        // after this lock is released, NOT from `index` — see
                        // `engine::vector::durable::segment_payload`. Reading it
                        // from the index would serialize empty vectors whenever
                        // the index was restored from a graph-only checkpoint.
                        segment_names.push(name.clone());
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
            segment_names
        };

        self.storage
            .batch_write(&ops)
            .await
            .map_err(NodeDbError::storage)?;

        // The CRDT writes are durable now, so advance the frontiers and the
        // checkpoint accounting. Doing this only after the write means a failed
        // batch leaves every collection outstanding and the next flush retries
        // it.
        {
            let mut crdt = self.crdt.lock_or_recover();
            crdt.mark_persisted(persisted);
            crdt.mark_pending_deltas_persisted(written_deltas);
        }

        // ── Write HNSW vector segments to pagedb (native PagedbStorage only) ──
        #[cfg(not(target_arch = "wasm32"))]
        if let Some(ext) = seg_ext {
            for name in &hnsw_segment_names {
                let payload =
                    match crate::engine::vector::durable::segment_payload(&*self.storage, name)
                        .await
                    {
                        Ok(Some(p)) => p,
                        // No durable vectors: nothing to publish. Leaving the
                        // existing segment untouched is deliberate — replacing it
                        // with an empty one is exactly the corruption this fixes.
                        Ok(None) => continue,
                        Err(e) => {
                            tracing::error!(
                                collection = %name,
                                error = %e,
                                "reading durable vectors for the segment write failed; \
                                 leaving the existing segment in place"
                            );
                            continue;
                        }
                    };
                let (dim, vectors, surrogates) = payload;
                if let Err(e) = ext
                    .write_vector_segment(name, dim, &vectors, &surrogates)
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
