// SPDX-License-Identifier: Apache-2.0

//! Shared runtime state for sparse-vector search on Lite.
//!
//! Held as `Arc<SparseVectorState>` on both `NodeDbLite` (user-facing write
//! path) and `LiteQueryEngine` (PhysicalPlan executor) so `VectorOp::Sparse*`
//! sees exactly the index the document writes maintain.

use std::sync::Mutex;

use super::manager::SparseVectorManager;

/// Arc-shareable wrapper around the per-collection sparse index manager.
///
/// The manager is purely in-memory and independent of the storage parameter
/// `S` — persistence happens through the checkpoint module at flush time — so
/// no generic parameter is needed here.
pub struct SparseVectorState {
    pub(crate) manager: Mutex<SparseVectorManager>,
}

impl SparseVectorState {
    /// Create a new, empty `SparseVectorState`.
    pub fn new() -> Self {
        Self {
            manager: Mutex::new(SparseVectorManager::new()),
        }
    }

    /// Wrap an already-restored `SparseVectorManager`.
    pub fn from_restored(manager: SparseVectorManager) -> Self {
        Self {
            manager: Mutex::new(manager),
        }
    }
}

impl Default for SparseVectorState {
    fn default() -> Self {
        Self::new()
    }
}
