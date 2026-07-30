// SPDX-License-Identifier: Apache-2.0

//! Cold-start restore helpers: CRDT/identity, CSR, HNSW, spatial, sparse-vector, and FTS.

use std::collections::HashMap;
use std::sync::Arc;

use nodedb_types::Namespace;
use nodedb_types::error::{NodeDbError, NodeDbResult};

use crate::engine::crdt::CrdtEngine;
use crate::engine::graph::index::CsrIndex;
use crate::engine::vector::graph::HnswIndex;
use crate::storage::engine::StorageEngine;

/// `"{collection}:{internal_id}"` → (document id, internal id).
type VectorIdMap = HashMap<String, (String, u32)>;

use crate::nodedb::core::types::{
    META_CRDT_DELTAS, META_CSR_COLLECTIONS, META_CSR_LEGACY, META_HNSW_COLLECTIONS,
    META_LAST_FLUSHED_MID, NodeDbLite,
};

impl<S: StorageEngine> NodeDbLite<S> {
    /// Restore Lite identity (lite_id + epoch) and CRDT state.
    ///
    /// Loads or creates the Lite identity, restores per-collection CRDT
    /// snapshots, backfills the registered-collection set and `LatestVersion`
    /// index from persisted bitemporal flags, restores pending deltas, checks
    /// partial-flush safety, and deletes the legacy single-CSR checkpoint if
    /// present.
    pub(super) async fn restore_identity_and_crdt(
        storage: &Arc<S>,
        peer_id: u64,
    ) -> NodeDbResult<(
        CrdtEngine,
        crate::engine::timeseries::identity::LiteIdentity,
    )> {
        // ── Load or create Lite identity (lite_id + epoch) ──
        //
        // This must happen before any outbound sync so the handshake carries a
        // non-empty lite_id and epoch ≥ 1, enabling Origin's idempotent-producer
        // gate. The epoch is incremented on every open, so a new process
        // incarnation fences out writes from the previous one.
        let lite_identity =
            crate::engine::timeseries::identity::LiteIdentity::load_or_create(&**storage)
                .await
                .map_err(|e| {
                    // Preserve corruption typing so a corrupt identity read is
                    // routed to the post-open recovery driver rather than
                    // crash-looping as a generic storage error.
                    let detail = format!("lite identity load failed: {e}");
                    if crate::error::is_corruption(&e) {
                        NodeDbError::segment_corrupted(detail)
                    } else {
                        NodeDbError::storage(detail)
                    }
                })?;

        // ── Restore CRDT state, one Loro document per collection ──
        // Snapshots are stored under `loro_snapshot:<collection>`, so the whole
        // set is recovered with a single prefix scan. A collection whose
        // snapshot fails its CRC32C check is dropped individually — the other
        // collections stay intact instead of the whole engine resetting.
        let mut crdt = CrdtEngine::new(peer_id)
            .map_err(|e| NodeDbError::storage(format!("CRDT init failed: {e}")))?;
        let snapshot_entries = storage
            .scan_prefix(Namespace::LoroState, CrdtEngine::snapshot_key_prefix())
            .await?;
        for (key, envelope) in &snapshot_entries {
            let Some(collection) = CrdtEngine::collection_from_snapshot_key(key) else {
                tracing::error!(
                    "CRDT snapshot key is not a valid `loro_snapshot:<collection>` entry — \
                     skipping; its collection cannot be determined without guessing."
                );
                continue;
            };
            match crate::storage::checksum::unwrap(envelope) {
                Some(snapshot) => {
                    crdt.import_snapshot(collection, &snapshot).map_err(|e| {
                        NodeDbError::storage(format!("CRDT restore of '{collection}' failed: {e}"))
                    })?;
                }
                None => {
                    tracing::error!(
                        collection = %collection,
                        "CRDT snapshot CRC32C mismatch — discarding corrupted snapshot for this \
                         collection. A full re-sync from Origin is needed for it."
                    );
                    // Delete the corrupted snapshot so we don't re-read it.
                    let _ = storage.delete(Namespace::LoroState, key).await;
                }
            }
        }

        // Rebuild the CRDT's registered-collection set from persisted bitemporal
        // flags so that SELECT queries on bitemporal collections work immediately
        // after open, even for collections with no inserted documents yet.
        // Also backfill the LatestVersion index for collections written before
        // the index was introduced — safe on fresh DBs and idempotent otherwise.
        const BITEMPORAL_PREFIX: &[u8] = b"document_bitemporal:";
        let bitemporal_entries = storage
            .scan_prefix(Namespace::Meta, BITEMPORAL_PREFIX)
            .await
            .unwrap_or_default();
        for (key, value) in &bitemporal_entries {
            // Only process collections where the flag byte is 0x01 (enabled).
            if value.first().copied() != Some(1) {
                continue;
            }
            if let Ok(key_str) = std::str::from_utf8(key)
                && let Some(name) = key_str.strip_prefix("document_bitemporal:")
            {
                crdt.register_collection(name);

                if let Err(e) = crate::engine::document::history::ops::backfill_latest_version(
                    storage.as_ref(),
                    name,
                )
                .await
                {
                    tracing::warn!(
                        collection = name,
                        error = %e,
                        "LatestVersion backfill failed — bitemporal reads will \
                         fall back to prefix scan for this collection"
                    );
                }
            }
        }

        // Restore pending deltas — prefer incremental entries over legacy bulk blob.
        let incremental_entries = storage.scan_prefix(Namespace::Crdt, b"delta:").await?;

        if !incremental_entries.is_empty() {
            // Use incremental entries (append-only format).
            crdt.restore_pending_deltas_incremental(&incremental_entries);
        } else if let Some(delta_bytes) = storage.get(Namespace::Crdt, META_CRDT_DELTAS).await? {
            // Fall back to legacy bulk blob.
            crdt.restore_pending_deltas(&delta_bytes);
        }

        // Partial flush safety: check if the last-flushed mutation_id matches.
        if crdt.pending_count() > 0
            && let Some(last_flushed_bytes) =
                storage.get(Namespace::Meta, META_LAST_FLUSHED_MID).await?
            && last_flushed_bytes.len() == 8
        {
            let last_flushed = u64::from_le_bytes(last_flushed_bytes.try_into().unwrap_or([0; 8]));
            let max_pending = crdt
                .pending_deltas()
                .iter()
                .map(|d| d.mutation_id)
                .max()
                .unwrap_or(0);

            if max_pending > 0 && last_flushed > 0 && max_pending != last_flushed {
                tracing::warn!(
                    last_flushed,
                    max_pending,
                    "partial flush detected — pending deltas may be inconsistent. \
                     Clearing pending queue; CRDT state is authoritative."
                );
                crdt.clear_pending_deltas();
            }
        }

        // ── Delete legacy single-CSR checkpoint if present ──
        if storage
            .get(Namespace::Graph, META_CSR_LEGACY)
            .await?
            .is_some()
        {
            let _ = storage.delete(Namespace::Graph, META_CSR_LEGACY).await;
        }

        Ok((crdt, lite_identity))
    }

    /// Restore per-collection CSR graph indices from storage.
    ///
    /// On native targets with `PagedbStorage`, CSR blobs are read from pagedb
    /// segments (segment-first, then fall back to the legacy B+ tree KV blob
    /// for databases written by older builds).  On WASM, only the B+ tree path
    /// is used.
    pub(super) async fn restore_csr_indices(
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

    /// Restore HNSW indices and the vector id_map from storage.
    ///
    /// Returns `(indices, id_map)`. The id_map maps `"{index_key}:{internal_id}"`
    /// to `(doc_id, internal_id)` and is loaded from the blob written by `flush`.
    /// When no id_map blob exists (first open or pre-fix databases), the returned
    /// map is empty and vector search will fall back to HNSW integer IDs until the
    /// next flush.
    pub(super) async fn restore_hnsw_indices(
        storage: &Arc<S>,
    ) -> NodeDbResult<(HashMap<String, HnswIndex>, HashMap<String, (String, u32)>)> {
        let mut hnsw_indices = HashMap::new();
        // Collections whose index was rebuilt from durable vectors: their
        // internal ids are freshly assigned, so the persisted `hnsw_id_map`
        // entries for them are stale and must be replaced, not merged.
        #[cfg(not(target_arch = "wasm32"))]
        let mut rebuilt_id_maps: Vec<(String, VectorIdMap)> = Vec::new();

        // `META_HNSW_COLLECTIONS` is written by `flush`, so on a database that
        // has taken writes but never flushed it is absent — and those are
        // precisely the vectors with no segment to restore from. Discovery
        // therefore starts from the checkpoint list and is UNIONED with the
        // collections that have durable vectors, so a never-flushed database
        // still comes back with its indexes.
        let mut names: Vec<String> = match storage.get(Namespace::Meta, META_HNSW_COLLECTIONS).await
        {
            Ok(Some(bytes)) => zerompk::from_msgpack::<Vec<String>>(&bytes).unwrap_or_default(),
            _ => Vec::new(),
        };
        #[cfg(not(target_arch = "wasm32"))]
        {
            match crate::engine::vector::durable::list_collections(storage.as_ref()).await {
                Ok(durable_names) => {
                    for n in durable_names {
                        if !names.contains(&n) {
                            names.push(n);
                        }
                    }
                }
                Err(e) => {
                    tracing::warn!(
                        error = %e,
                        "listing durable vector collections failed; only checkpointed \
                         collections will be restored"
                    );
                }
            }
        }
        if names.is_empty() {
            return Ok((hnsw_indices, HashMap::new()));
        }

        // On native targets, check if vector segment operations are available.
        // When yes, the graph blob has empty vector placeholders; we load the
        // backing from the pagedb segment and attach it to the restored index.
        #[cfg(not(target_arch = "wasm32"))]
        let seg_ext = storage.as_vector_segment_ext();

        for name in &names {
            let key = format!("hnsw:{name}");
            if let Some(envelope) = storage.get(Namespace::Vector, key.as_bytes()).await? {
                match crate::storage::checksum::unwrap(&envelope) {
                    Some(checkpoint) => match HnswIndex::from_checkpoint(&checkpoint) {
                        // `index` is mutated only by the native segment-backing
                        // attach below, which is compiled out on wasm32.
                        #[cfg_attr(target_arch = "wasm32", allow(unused_mut))]
                        Ok(Some(mut index)) => {
                            // With segment support, `flush` writes a GRAPH-ONLY
                            // checkpoint: the node vector bytes in it are empty
                            // placeholders and the floats live in the
                            // `vec/hnsw/<name>` segment. So an index whose segment
                            // cannot be attached holds no vector data at all, and the
                            // first distance computation panics on a zero-length node
                            // (`dist_to_node: byte-length mismatch`). Keeping such an
                            // index is what turned one unreadable segment into a daemon
                            // that panicked on every query.
                            //
                            // The segment is only a DERIVED index, so this is
                            // recoverable rather than fatal: the authoritative vectors
                            // are the per-document rows written by
                            // `engine::vector::durable`, and the index is rebuilt from
                            // them here. (The CRDT is NOT a source — it carries only
                            // `embedding_dim`, never the floats, which is why the
                            // "rebuild from CRDT" this code used to claim could never
                            // have worked.)
                            #[cfg(not(target_arch = "wasm32"))]
                            let attached = match seg_ext {
                                Some(ext) => match ext.open_vector_segment(name).await {
                                    Ok(Some(backing)) => {
                                        use std::sync::Arc;
                                        index.with_backing(Arc::new(backing));
                                        tracing::debug!(
                                            collection = %name,
                                            "HNSW restored with pagedb vector segment backing"
                                        );
                                        true
                                    }
                                    Ok(None) => false,
                                    Err(e) => {
                                        tracing::warn!(
                                            collection = %name,
                                            error = %e,
                                            "vector segment unreadable; rebuilding HNSW \
                                             from durable vectors"
                                        );
                                        false
                                    }
                                },
                                // No segment support: the checkpoint carries its own
                                // vectors, so it stands on its own.
                                None => true,
                            };
                            // WASM has no segment path; its checkpoint is self-contained.
                            #[cfg(target_arch = "wasm32")]
                            let attached = true;

                            if attached {
                                hnsw_indices.insert(name.clone(), index);
                            } else {
                                #[cfg(not(target_arch = "wasm32"))]
                                rebuild_into(
                                    storage,
                                    name,
                                    Some((index.dim(), index.params().clone())),
                                    &mut hnsw_indices,
                                    &mut rebuilt_id_maps,
                                )
                                .await;
                            }
                        }
                        Ok(None) | Err(_) => {
                            tracing::warn!(
                                collection = %name,
                                "HNSW checkpoint unreadable; rebuilding from durable vectors"
                            );
                            #[cfg(not(target_arch = "wasm32"))]
                            rebuild_into(
                                storage,
                                name,
                                None,
                                &mut hnsw_indices,
                                &mut rebuilt_id_maps,
                            )
                            .await;
                        }
                    },
                    None => {
                        tracing::error!(
                            collection = %name,
                            "HNSW checkpoint CRC32C mismatch — discarding and rebuilding \
                             from durable vectors."
                        );
                        let _ = storage.delete(Namespace::Vector, key.as_bytes()).await;
                        #[cfg(not(target_arch = "wasm32"))]
                        rebuild_into(storage, name, None, &mut hnsw_indices, &mut rebuilt_id_maps)
                            .await;
                    }
                }
            } else {
                // No checkpoint at all — a database that took writes but never
                // flushed. The durable vectors are still on disk, so the index
                // is built straight from them.
                #[cfg(not(target_arch = "wasm32"))]
                rebuild_into(storage, name, None, &mut hnsw_indices, &mut rebuilt_id_maps).await;
            }
        }

        // ── Restore vector_id_map ──
        // The blob is written by `flush` and contains the full flat map.
        // Without this, vector_search returns HNSW integer strings after restart.
        let id_map = match storage
            .get(Namespace::Vector, b"hnsw_id_map")
            .await
            .unwrap_or(None)
        {
            Some(envelope) => match crate::storage::checksum::unwrap(&envelope) {
                Some(bytes) => match zerompk::from_msgpack::<Vec<(String, String, u32)>>(&bytes) {
                    Ok(entries) => entries
                        .into_iter()
                        .map(|(k, doc_id, iid)| (k, (doc_id, iid)))
                        .collect(),
                    Err(e) => {
                        tracing::error!(
                            error = %e,
                            "vector_id_map deserialization failed — \
                             vector search will fall back to HNSW integer IDs until next flush"
                        );
                        HashMap::new()
                    }
                },
                None => {
                    tracing::error!(
                        "vector_id_map CRC32C mismatch — discarding. \
                         Vector search will fall back to HNSW integer IDs until next flush."
                    );
                    let _ = storage.delete(Namespace::Vector, b"hnsw_id_map").await;
                    HashMap::new()
                }
            },
            None => HashMap::new(),
        };

        // Replace stale id-map entries for any rebuilt collection. Its internal
        // ids were reassigned during the rebuild, so keeping the persisted ones
        // would map search hits to the wrong documents.
        #[cfg(not(target_arch = "wasm32"))]
        let id_map = {
            let mut id_map = id_map;
            for (name, rebuilt) in rebuilt_id_maps {
                let prefix = format!("{name}:");
                id_map.retain(|k, _| !k.starts_with(&prefix));
                id_map.extend(rebuilt);
            }
            id_map
        };

        Ok((hnsw_indices, id_map))
    }

    /// Restore spatial indices from storage.
    pub(super) async fn restore_spatial_indices(
        storage: &Arc<S>,
    ) -> crate::engine::spatial::SpatialIndexManager {
        match crate::engine::spatial::checkpoint::restore_spatial(storage.as_ref()).await {
            Ok((checkpoints, doc_to_entry, next_id)) if !checkpoints.is_empty() => {
                let mut mgr = crate::engine::spatial::SpatialIndexManager::new();
                mgr.load_checkpoint(&checkpoints, doc_to_entry, next_id);
                mgr
            }
            Ok(_) => crate::engine::spatial::SpatialIndexManager::new(),
            Err(e) => {
                tracing::error!(
                    error = %e,
                    "spatial checkpoint restore failed — starting with empty index; \
                     will rebuild from CRDT state on cold open"
                );
                crate::engine::spatial::SpatialIndexManager::new()
            }
        }
    }

    /// Restore sparse-vector inverted indices from a persistent checkpoint.
    ///
    /// Returns the restored manager plus whether a checkpoint was found. The
    /// caller uses the flag to decide whether a rebuild from source documents
    /// is needed — an empty manager from a real checkpoint means "no sparse
    /// columns", which needs no rebuild. A restore failure is logged and
    /// reported as "no checkpoint" so the rebuild path repopulates the index
    /// rather than leaving searches silently empty.
    pub(super) async fn restore_sparse_indices(
        storage: &Arc<S>,
    ) -> (crate::engine::sparse_vector::SparseVectorManager, bool) {
        let mut mgr = crate::engine::sparse_vector::SparseVectorManager::new();
        match crate::engine::sparse_vector::checkpoint::restore_sparse(storage.as_ref()).await {
            Ok(Some(indices)) => {
                mgr.load_checkpoint(indices);
                (mgr, true)
            }
            Ok(None) => (mgr, false),
            Err(e) => {
                tracing::error!(
                    error = %e,
                    "sparse vector checkpoint restore failed — rebuilding from source documents"
                );
                (mgr, false)
            }
        }
    }

    /// Restore FTS indices from a persistent checkpoint.
    ///
    /// Returns an empty `FtsCollectionManager` when no checkpoint exists (first
    /// open or after a collection drop).  The caller decides whether to fall
    /// back to `rebuild_text_indices` — see `open_inner`.
    pub(super) async fn restore_fts_indices(
        storage: &Arc<S>,
    ) -> NodeDbResult<crate::engine::fts::FtsCollectionManager> {
        let mut mgr = crate::engine::fts::FtsCollectionManager::new();
        match crate::engine::fts::checkpoint::restore_fts(storage.as_ref()).await {
            Ok((indices, id_to_surrogate, surrogate_to_id, next_surrogate))
                if !indices.is_empty() =>
            {
                mgr.load_checkpoint(indices, id_to_surrogate, surrogate_to_id, next_surrogate);
            }
            Ok(_) => {
                // No checkpoint found — caller will rebuild from CRDT state.
            }
            Err(e) => {
                tracing::error!(
                    error = %e,
                    "FTS checkpoint restore failed — starting with empty index; \
                     will rebuild from CRDT state on cold open"
                );
            }
        }
        Ok(mgr)
    }
}

/// Rebuild `collection` from its durable vectors and record the result.
///
/// Shared by every restore path that ends up without a usable index — no
/// checkpoint, an unreadable checkpoint, or a checkpoint whose vector segment
/// could not be attached — so all of them recover identically instead of each
/// inventing its own fallback.
#[cfg(not(target_arch = "wasm32"))]
async fn rebuild_into<S: StorageEngine>(
    storage: &Arc<S>,
    collection: &str,
    template: Option<(usize, crate::engine::vector::HnswParams)>,
    hnsw_indices: &mut HashMap<String, HnswIndex>,
    rebuilt_id_maps: &mut Vec<(String, VectorIdMap)>,
) {
    match crate::engine::vector::durable::rebuild_index(storage.as_ref(), collection, template)
        .await
    {
        Ok(Some((index, id_map))) => {
            tracing::info!(
                collection,
                vectors = index.len(),
                "HNSW rebuilt from durable vectors"
            );
            hnsw_indices.insert(collection.to_owned(), index);
            rebuilt_id_maps.push((collection.to_owned(), id_map));
        }
        Ok(None) => {
            tracing::debug!(collection, "no durable vectors; HNSW starts empty");
        }
        Err(e) => {
            tracing::error!(
                collection,
                error = %e,
                "rebuilding HNSW from durable vectors FAILED; dense retrieval is \
                 unavailable for this collection until the next write"
            );
        }
    }
}
