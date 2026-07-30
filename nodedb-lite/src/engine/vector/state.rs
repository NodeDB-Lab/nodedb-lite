// SPDX-License-Identifier: Apache-2.0

//! Shared runtime state for HNSW vector search on Lite.
//!
//! Held as `Arc<VectorState<S>>` on both `NodeDbLite<S>` (user-facing
//! entry points) and `LiteQueryEngine<S>` (PhysicalPlan executor) so
//! the visitor pipeline can run vector ops without re-architecting the
//! engine boundary.

use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Mutex};

use nodedb_types::collection_config::VectorPrimaryConfig;
use nodedb_types::hnsw::HnswParams;
use nodedb_types::vector_dtype::VectorStorageDtype;
use nodedb_vector::rerank::CodecSidecar;

use crate::engine::vector::HnswIndex;
use crate::storage::engine::StorageEngine;

pub struct VectorState<S: StorageEngine> {
    pub(crate) hnsw_indices: Mutex<HashMap<String, HnswIndex>>,
    /// composite_key → (doc_id, vector_id)
    pub(crate) vector_id_map: Mutex<HashMap<String, (String, u32)>>,
    pub(crate) search_ef: usize,
    pub(crate) storage: Arc<S>,
    /// index_key → trained codec sidecar (populated by S2.a.11).
    pub(crate) codec_sidecars: Arc<Mutex<HashMap<String, CodecSidecar>>>,
    /// Per-(index_key) collection config — populated when a collection is
    /// registered via DDL (C2c will wire that). Lookup is best-effort:
    /// callers that don't find an entry default to F32 storage, matching
    /// the previous behavior.
    pub(crate) per_index_config: Arc<Mutex<HashMap<String, VectorPrimaryConfig>>>,
    /// Index keys whose stored checkpoint exists but cannot be turned into a
    /// usable index — unreadable checkpoint, or a segment that cannot serve the
    /// graph with no durable vectors to rebuild from.
    ///
    /// Without this, `ensure_index_loaded` gives up WITHOUT caching anything, so
    /// every later search on that collection repeats the entire cost — read the
    /// checkpoint, deserialize the full graph, open and validate the segment,
    /// scan the durable rows — and still finds nothing. On a collection with
    /// thousands of nodes that turns one unusable segment into an operation that
    /// pins a core indefinitely while reporting no progress.
    ///
    /// This is a NEGATIVE cache for the load path only. It is not consulted once
    /// the collection is present in `hnsw_indices`, so a later insert (which
    /// creates the index through `ensure_hnsw`) resolves the collection normally
    /// without anything here needing to be cleared.
    pub(crate) unloadable: Mutex<HashSet<String>>,
}

/// Get or create the HNSW index for `index_key` with the given dimensionality and
/// storage dtype. When the index already exists the `dtype` argument is ignored —
/// dtype is fixed at index-creation time and cannot be changed in place.
pub(crate) fn ensure_hnsw<'a>(
    indices: &'a mut HashMap<String, HnswIndex>,
    index_key: &str,
    dim: usize,
    dtype: VectorStorageDtype,
) -> &'a mut HnswIndex {
    indices.entry(index_key.to_string()).or_insert_with(|| {
        HnswIndex::new(
            dim,
            HnswParams {
                dtype,
                ..HnswParams::default()
            },
        )
    })
}

impl<S: StorageEngine> VectorState<S> {
    pub fn new(storage: Arc<S>, search_ef: usize) -> Self {
        Self {
            hnsw_indices: Mutex::new(HashMap::new()),
            vector_id_map: Mutex::new(HashMap::new()),
            search_ef,
            storage,
            codec_sidecars: Arc::new(Mutex::new(HashMap::new())),
            per_index_config: Arc::new(Mutex::new(HashMap::new())),
            unloadable: Mutex::new(HashSet::new()),
        }
    }

    pub fn from_restored(
        storage: Arc<S>,
        search_ef: usize,
        indices: HashMap<String, HnswIndex>,
        id_map: HashMap<String, (String, u32)>,
    ) -> Self {
        Self {
            hnsw_indices: Mutex::new(indices),
            vector_id_map: Mutex::new(id_map),
            search_ef,
            storage,
            codec_sidecars: Arc::new(Mutex::new(HashMap::new())),
            per_index_config: Arc::new(Mutex::new(HashMap::new())),
            unloadable: Mutex::new(HashSet::new()),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::pagedb_storage::PagedbStorageMem;

    #[tokio::test]
    async fn per_index_config_starts_empty() {
        let storage = Arc::new(
            PagedbStorageMem::open_in_memory()
                .await
                .expect("in-memory pagedb"),
        );
        let state = VectorState::new(storage, 100);
        let configs = state.per_index_config.lock().expect("lock");
        assert!(
            configs.is_empty(),
            "per_index_config must be empty on construction"
        );
    }

    #[test]
    fn ensure_hnsw_creates_index_with_f32_default() {
        let mut indices: HashMap<String, HnswIndex> = HashMap::new();
        ensure_hnsw(&mut indices, "col", 4, VectorStorageDtype::F32);
        let idx = indices.get("col").expect("index created");
        assert_eq!(idx.params().dtype, VectorStorageDtype::F32);
    }

    #[test]
    fn ensure_hnsw_creates_index_with_bf16() {
        let mut indices: HashMap<String, HnswIndex> = HashMap::new();
        ensure_hnsw(&mut indices, "col", 4, VectorStorageDtype::BF16);
        let idx = indices.get("col").expect("index created");
        assert_eq!(idx.params().dtype, VectorStorageDtype::BF16);
    }

    #[test]
    fn ensure_hnsw_existing_index_ignores_dtype_arg() {
        let mut indices: HashMap<String, HnswIndex> = HashMap::new();
        ensure_hnsw(&mut indices, "col", 4, VectorStorageDtype::F32);
        // Call again with BF16 — dtype is fixed at creation time, must not change.
        ensure_hnsw(&mut indices, "col", 4, VectorStorageDtype::BF16);
        let idx = indices.get("col").expect("index present");
        assert_eq!(
            idx.params().dtype,
            VectorStorageDtype::F32,
            "dtype must remain F32; dtype is fixed at index-creation time"
        );
    }
}
