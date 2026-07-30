// SPDX-License-Identifier: Apache-2.0

//! Lazy HNSW index loader: brings a cold index into memory from storage on
//! first search, then attempts to restore or retrain its codec sidecar.

use std::sync::Arc;

use nodedb_types::Namespace;
use nodedb_types::error::NodeDbResult;

use crate::engine::vector::VectorState;
use crate::engine::vector::graph::HnswIndex;
use crate::engine::vector::sidecar;
use crate::nodedb::lock_ext::LockExt;
use crate::storage::engine::StorageEngine;

/// If `index_key` is not already in memory, load its HNSW checkpoint from
/// storage and restore (or retrain) its codec sidecar.
///
/// Called at the start of every search so cold collections are transparently
/// promoted to hot without a full database restart.
pub(super) async fn ensure_index_loaded<S: StorageEngine>(
    vector_state: &Arc<VectorState<S>>,
    index_key: &str,
) -> NodeDbResult<()> {
    let has_it = vector_state
        .hnsw_indices
        .lock_or_recover()
        .contains_key(index_key);

    if has_it {
        return Ok(());
    }

    // A collection already known to be unloadable must not be retried: the work
    // below is proportional to the whole graph, and repeating it per search is
    // what makes one unusable segment pin a core indefinitely.
    if vector_state
        .unloadable
        .lock_or_recover()
        .contains(index_key)
    {
        return Ok(());
    }

    /// Record `index_key` as unloadable so later searches short-circuit.
    macro_rules! give_up {
        () => {{
            vector_state
                .unloadable
                .lock_or_recover()
                .insert(index_key.to_string());
            return Ok(());
        }};
    }

    let key = format!("hnsw:{index_key}");
    let Some(envelope) = vector_state
        .storage
        .get(Namespace::Vector, key.as_bytes())
        .await?
    else {
        return Ok(());
    };

    let Some(checkpoint) = crate::storage::checksum::unwrap(&envelope) else {
        tracing::warn!(
            index_key,
            "HNSW checkpoint CRC32C mismatch on lazy-load; skipping"
        );
        give_up!();
    };

    // `index` is mutated only by the native segment-backing path (wasm32: none).
    #[cfg_attr(target_arch = "wasm32", allow(unused_mut))]
    let Ok(Some(mut index)) = HnswIndex::from_checkpoint(&checkpoint) else {
        tracing::warn!(
            index_key,
            "HNSW checkpoint could not be deserialized on lazy-load; skipping"
        );
        give_up!();
    };

    // On native targets, attach vector segment backing if available.
    //
    // With segment backing the checkpoint is GRAPH-ONLY — its node vector bytes
    // are empty placeholders — so an index whose segment will not attach holds
    // no vector data and panics in the distance kernels
    // (`dist_to_node: byte-length mismatch`) on the first search. There are no
    // "inline vectors" to fall back to on this path, despite what this code
    // used to say. The segment is a derived index, so the recovery is to
    // rebuild from the durable per-document vectors instead.
    #[cfg(not(target_arch = "wasm32"))]
    if let Some(ext) = vector_state.storage.as_vector_segment_ext() {
        let attached = match ext.open_vector_segment(index_key).await {
            Ok(Some(backing)) => {
                use std::sync::Arc;
                // A segment that reads but cannot serve this index's nodes is
                // refused by `with_backing` — otherwise the graph would look
                // healthy and every query would score a vectorless node.
                match index.with_backing(Arc::new(backing)) {
                    Ok(_) => {
                        tracing::debug!(
                            index_key,
                            "lazy-load: attached pagedb vector segment backing"
                        );
                        true
                    }
                    Err(e) => {
                        tracing::warn!(
                            index_key,
                            error = %e,
                            "lazy-load: vector segment cannot serve this index \
                             (empty or short payload); rebuilding from durable vectors"
                        );
                        false
                    }
                }
            }
            Ok(None) => false,
            Err(e) => {
                tracing::warn!(
                    index_key,
                    error = %e,
                    "lazy-load: vector segment unreadable; rebuilding from durable vectors"
                );
                false
            }
        };

        if !attached {
            // Snapshot the shape before awaiting: `HnswIndex` holds a `RefCell`
            // arena, so a borrow held across the await would make this future
            // non-`Send`.
            let shape = (index.dim(), index.params().clone());
            match crate::engine::vector::durable::rebuild_index(
                &*vector_state.storage,
                index_key,
                Some(shape),
            )
            .await
            {
                Ok(Some((rebuilt, id_map))) => {
                    tracing::info!(
                        index_key,
                        vectors = rebuilt.len(),
                        "lazy-load: HNSW rebuilt from durable vectors"
                    );
                    {
                        let mut map = vector_state.vector_id_map.lock_or_recover();
                        let prefix = format!("{index_key}:");
                        map.retain(|k, _| !k.starts_with(&prefix));
                        map.extend(id_map);
                    }
                    index = rebuilt;
                }
                Ok(None) | Err(_) => {
                    // Nothing durable to rebuild from: publishing the
                    // vectorless checkpoint would score nodes that have no
                    // vector, so leave the collection unloaded instead — and
                    // remember that, so the next search does not repeat all of
                    // the work above to reach the same conclusion.
                    tracing::warn!(
                        index_key,
                        "lazy-load: no durable vectors to rebuild from; \
                         leaving collection unloaded rather than publishing a \
                         vectorless index"
                    );
                    give_up!();
                }
            }
        }
    }

    tracing::info!(index_key, "lazy-loaded HNSW collection from storage");
    vector_state
        .hnsw_indices
        .lock_or_recover()
        .insert(index_key.to_string(), index);

    // Try to restore a persisted sidecar. On failure, fall through to
    // ensure_sidecar which retrains from the live HNSW vectors.
    match sidecar::try_restore_sidecar(vector_state, index_key).await {
        Ok(true) => {
            tracing::debug!(index_key, "sidecar restored from storage after lazy-load");
        }
        Ok(false) => {
            if let Err(e) = sidecar::ensure_sidecar(vector_state, index_key) {
                tracing::warn!(
                    index_key,
                    error = %e,
                    "sidecar rebuild after lazy-load failed; \
                     codec rerank will degrade to FP32 for this collection"
                );
            } else {
                tracing::debug!(index_key, "sidecar rebuilt after lazy-load");
            }
        }
        Err(e) => {
            tracing::warn!(
                index_key,
                error = %e,
                "sidecar restore failed; attempting rebuild via ensure_sidecar"
            );
            if let Err(e2) = sidecar::ensure_sidecar(vector_state, index_key) {
                tracing::warn!(
                    index_key,
                    error = %e2,
                    "sidecar rebuild also failed; \
                     codec rerank will degrade to FP32 for this collection"
                );
            }
        }
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::pagedb_storage::PagedbStorageMem;

    /// A collection that cannot be loaded must be attempted ONCE. Before the
    /// negative cache, every search repeated the full load — read the
    /// checkpoint, deserialize the whole graph, open and validate the segment,
    /// scan the durable rows — cached nothing, and concluded the same thing,
    /// which is how one unusable segment pinned a core indefinitely.
    #[tokio::test]
    async fn an_unloadable_collection_is_only_attempted_once() {
        let storage = Arc::new(PagedbStorageMem::open_in_memory().await.unwrap());
        let state = Arc::new(VectorState::new(Arc::clone(&storage), 64));

        // A checkpoint that exists but is not deserializable: the load path gets
        // far enough to give up, which is exactly what must not be repeated.
        storage
            .put(
                Namespace::Vector,
                b"hnsw:broken",
                &crate::storage::checksum::wrap(b"not a valid hnsw checkpoint"),
            )
            .await
            .unwrap();

        ensure_index_loaded(&state, "broken").await.unwrap();
        assert!(
            state.unloadable.lock_or_recover().contains("broken"),
            "an unloadable collection must be recorded so it is not retried"
        );
        assert!(
            !state.hnsw_indices.lock_or_recover().contains_key("broken"),
            "an unusable index must never be published"
        );

        // Second call short-circuits; it must not resurrect or publish anything.
        ensure_index_loaded(&state, "broken").await.unwrap();
        assert!(!state.hnsw_indices.lock_or_recover().contains_key("broken"));
    }

    /// The negative cache must not block a collection that later gains vectors:
    /// an insert creates the index directly, and the load path short-circuits on
    /// `hnsw_indices` before ever consulting `unloadable`.
    #[tokio::test]
    async fn a_later_insert_resolves_an_unloadable_collection() {
        let storage = Arc::new(PagedbStorageMem::open_in_memory().await.unwrap());
        let state = Arc::new(VectorState::new(Arc::clone(&storage), 64));
        state.unloadable.lock_or_recover().insert("late".into());

        {
            let mut indices = state.hnsw_indices.lock_or_recover();
            let idx = crate::engine::vector::state::ensure_hnsw(
                &mut indices,
                "late",
                3,
                nodedb_types::vector_dtype::VectorStorageDtype::F32,
            );
            idx.insert(vec![1.0, 2.0, 3.0]).unwrap();
        }

        ensure_index_loaded(&state, "late").await.unwrap();
        let indices = state.hnsw_indices.lock_or_recover();
        assert_eq!(
            indices.get("late").map(|i| i.len()),
            Some(1),
            "a collection populated by insert must stay usable despite the negative cache"
        );
    }
}
