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
    let Some(checkpoint) = vector_state
        .storage
        .get(Namespace::Vector, key.as_bytes())
        .await?
    else {
        return Ok(());
    };

    let Ok(Some(index)) = HnswIndex::from_checkpoint(&checkpoint) else {
        return Ok(());
    };

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
