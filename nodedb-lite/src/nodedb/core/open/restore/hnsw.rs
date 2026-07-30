// SPDX-License-Identifier: Apache-2.0

//! HNSW index and vector id-map restore.

use std::collections::HashMap;
use std::sync::Arc;

use nodedb_types::Namespace;
use nodedb_types::error::NodeDbResult;

use crate::engine::vector::graph::HnswIndex;
use crate::storage::engine::StorageEngine;

use crate::nodedb::core::types::{META_HNSW_COLLECTIONS, NodeDbLite};

/// `"{collection}:{internal_id}"` → (document id, internal id).
type VectorIdMap = HashMap<String, (String, u32)>;

impl<S: StorageEngine> NodeDbLite<S> {
    /// Restore HNSW indices and the vector id_map from storage.
    ///
    /// Returns `(indices, id_map)`. The id_map maps `"{index_key}:{internal_id}"`
    /// to `(doc_id, internal_id)` and is loaded from the blob written by `flush`.
    /// When no id_map blob exists (first open or pre-fix databases), the returned
    /// map is empty and vector search will fall back to HNSW integer IDs until the
    /// next flush.
    pub(in crate::nodedb::core::open) async fn restore_hnsw_indices(
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
                                        // A segment that READS but cannot serve the
                                        // index's nodes is the dangerous case: the
                                        // graph looks healthy, so every later query
                                        // scores a node with no vector. `with_backing`
                                        // validates and refuses, so this rebuilds too.
                                        match index.with_backing(Arc::new(backing)) {
                                            Ok(_) => {
                                                tracing::debug!(
                                                    collection = %name,
                                                    "HNSW restored with pagedb vector segment backing"
                                                );
                                                true
                                            }
                                            Err(e) => {
                                                tracing::warn!(
                                                    collection = %name,
                                                    error = %e,
                                                    "vector segment cannot serve this index \
                                                     (empty or short payload); rebuilding HNSW \
                                                     from durable vectors"
                                                );
                                                false
                                            }
                                        }
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
