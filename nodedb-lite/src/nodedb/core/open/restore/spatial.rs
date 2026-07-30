// SPDX-License-Identifier: Apache-2.0

//! Spatial index restore.

use std::sync::Arc;

use crate::storage::engine::StorageEngine;

use crate::nodedb::core::types::NodeDbLite;

impl<S: StorageEngine> NodeDbLite<S> {
    /// Restore spatial indices from storage.
    pub(in crate::nodedb::core::open) async fn restore_spatial_indices(
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
}
