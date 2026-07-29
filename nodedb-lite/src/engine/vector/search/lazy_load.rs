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
        return Ok(());
    };

    // `index` is mutated only by the native segment-backing path (wasm32: none).
    #[cfg_attr(target_arch = "wasm32", allow(unused_mut))]
    let Ok(Some(mut index)) = HnswIndex::from_checkpoint(&checkpoint) else {
        return Ok(());
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
                index.with_backing(Arc::new(backing));
                tracing::debug!(
                    index_key,
                    "lazy-load: attached pagedb vector segment backing"
                );
                true
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
                    // vectorless checkpoint would panic on first search, so
                    // leave the collection unloaded instead.
                    tracing::warn!(
                        index_key,
                        "lazy-load: no durable vectors to rebuild from; \
                         leaving collection unloaded rather than publishing a \
                         vectorless index"
                    );
                    return Ok(());
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
