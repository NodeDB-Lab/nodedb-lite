// SPDX-License-Identifier: Apache-2.0

//! Full-text search index restore.

use std::sync::Arc;

use nodedb_types::error::NodeDbResult;

use crate::storage::engine::StorageEngine;

use crate::nodedb::core::types::NodeDbLite;

impl<S: StorageEngine> NodeDbLite<S> {
    /// Restore FTS indices from a persistent checkpoint.
    ///
    /// Returns an empty `FtsCollectionManager` when no checkpoint exists (first
    /// open or after a collection drop).  The caller decides whether to fall
    /// back to `rebuild_text_indices` — see `open_inner`.
    pub(in crate::nodedb::core::open) async fn restore_fts_indices(
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
