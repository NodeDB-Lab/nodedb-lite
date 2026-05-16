// SPDX-License-Identifier: Apache-2.0

//! Shared runtime state for FTS on Lite.
//!
//! Held as `Arc<FtsState>` on both `NodeDbLite` (user-facing entry points)
//! and `LiteQueryEngine` (PhysicalPlan executor) so the physical visitor
//! can run text ops without re-architecting the engine boundary.

use std::sync::Mutex;

use super::manager::FtsCollectionManager;

/// Arc-shareable wrapper around the per-collection FTS manager.
///
/// FTS is not storage-parameterised — the manager uses an in-memory backend
/// independent of `S`. No generic parameter is needed here.
pub struct FtsState {
    pub(crate) manager: Mutex<FtsCollectionManager>,
}

impl FtsState {
    /// Create a new, empty `FtsState`.
    pub fn new() -> Self {
        Self {
            manager: Mutex::new(FtsCollectionManager::new()),
        }
    }

    /// Wrap an already-restored `FtsCollectionManager`.
    pub fn from_restored(manager: FtsCollectionManager) -> Self {
        Self {
            manager: Mutex::new(manager),
        }
    }
}

impl Default for FtsState {
    fn default() -> Self {
        Self::new()
    }
}
