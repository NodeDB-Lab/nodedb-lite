// SPDX-License-Identifier: Apache-2.0

//! Shared runtime state for HNSW vector search on Lite.
//!
//! Held as `Arc<VectorState<S>>` on both `NodeDbLite<S>` (user-facing
//! entry points) and `LiteQueryEngine<S>` (PhysicalPlan executor) so
//! the visitor pipeline can run vector ops without re-architecting the
//! engine boundary.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use crate::engine::vector::HnswIndex;
use crate::storage::engine::StorageEngine;

pub struct VectorState<S: StorageEngine> {
    pub(crate) hnsw_indices: Mutex<HashMap<String, HnswIndex>>,
    /// composite_key → (doc_id, vector_id)
    pub(crate) vector_id_map: Mutex<HashMap<String, (String, u32)>>,
    pub(crate) search_ef: usize,
    pub(crate) storage: Arc<S>,
}

impl<S: StorageEngine> VectorState<S> {
    pub fn new(storage: Arc<S>, search_ef: usize) -> Self {
        Self {
            hnsw_indices: Mutex::new(HashMap::new()),
            vector_id_map: Mutex::new(HashMap::new()),
            search_ef,
            storage,
        }
    }

    pub fn from_restored(
        storage: Arc<S>,
        search_ef: usize,
        indices: HashMap<String, HnswIndex>,
    ) -> Self {
        Self {
            hnsw_indices: Mutex::new(indices),
            vector_id_map: Mutex::new(HashMap::new()),
            search_ef,
            storage,
        }
    }
}
