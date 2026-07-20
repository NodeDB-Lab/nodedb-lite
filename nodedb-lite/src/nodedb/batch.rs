//! Batch operations and memory eviction for NodeDbLite.

use nodedb_types::Namespace;
use nodedb_types::error::{NodeDbError, NodeDbResult};
use nodedb_types::vector_dtype::VectorStorageDtype;

use crate::engine::vector::state::ensure_hnsw;

use super::{LockExt, NodeDbLite};
use crate::storage::engine::StorageEngine;

impl<S: StorageEngine> NodeDbLite<S> {
    /// Batch insert vectors — O(1) CRDT delta export instead of O(N).
    ///
    /// Use this for bulk loading (cold-start hydration, benchmark setup, imports).
    /// Each vector is inserted into HNSW and tracked in the ID map, but only one
    /// Loro delta is generated for the entire batch.
    pub fn batch_vector_insert(
        &self,
        collection: &str,
        vectors: &[(&str, &[f32])],
    ) -> NodeDbResult<()> {
        if vectors.is_empty() {
            return Ok(());
        }

        if self.governor.pressure() == crate::memory::PressureLevel::Critical {
            return Err(NodeDbError::storage(
                crate::error::LiteError::Backpressure {
                    detail: "batch vector insert rejected: memory governor is at Critical pressure"
                        .into(),
                },
            ));
        }

        let dim = vectors[0].1.len();

        {
            let dtype = {
                let configs = self.vector_state.per_index_config.lock_or_recover();
                configs
                    .get(collection)
                    .map(|cfg| cfg.storage_dtype)
                    .unwrap_or(VectorStorageDtype::F32)
            };
            let mut indices = self.vector_state.hnsw_indices.lock_or_recover();
            let index = ensure_hnsw(&mut indices, collection, dim, dtype);
            let mut id_map = self.vector_state.vector_id_map.lock_or_recover();

            for &(id, embedding) in vectors {
                let internal_id = index.len() as u32;
                index
                    .insert(embedding.to_vec())
                    .map_err(NodeDbError::bad_request)?;
                id_map.insert(
                    format!("{collection}:{internal_id}"),
                    (id.to_string(), internal_id),
                );
            }
        }

        {
            let mut crdt = self.crdt.lock_or_recover();

            use crate::engine::crdt::engine::{CrdtBatchOp, CrdtField};

            let fields: Vec<Vec<CrdtField<'_>>> = vectors
                .iter()
                .map(|&(_, emb)| vec![("embedding_dim", loro::LoroValue::I64(emb.len() as i64))])
                .collect();

            let ops: Vec<CrdtBatchOp<'_>> = vectors
                .iter()
                .zip(fields.iter())
                .map(|(&(id, _), f)| (collection, id, f.as_slice()))
                .collect();

            crdt.batch_upsert(&ops).map_err(NodeDbError::storage)?;
        }

        self.update_memory_stats();
        Ok(())
    }

    /// Batch insert graph edges into a named collection — O(1) CRDT delta
    /// export instead of O(N). Edges are isolated to `collection`.
    pub fn batch_graph_insert_edges(
        &self,
        collection: &str,
        edges: &[(&str, &str, &str)],
    ) -> NodeDbResult<()> {
        if edges.is_empty() {
            return Ok(());
        }

        {
            let mut csr_map = self.csr.lock_or_recover();
            let csr = csr_map
                .entry(collection.to_string())
                .or_insert_with(crate::engine::graph::index::CsrIndex::new);
            for &(src, dst, label) in edges {
                let _ = csr.add_edge(src, label, dst);
            }
        }

        {
            let mut crdt = self.crdt.lock_or_recover();

            use crate::engine::crdt::engine::{CrdtBatchOp, CrdtField};
            let edge_coll = format!("__edges__{collection}");

            let ops: Vec<(String, Vec<CrdtField<'_>>)> = edges
                .iter()
                .map(|&(src, dst, label)| {
                    let edge_id = format!("{src}--{label}-->{dst}");
                    let fields: Vec<CrdtField<'_>> = vec![
                        ("src", loro::LoroValue::String(src.into())),
                        ("dst", loro::LoroValue::String(dst.into())),
                        ("label", loro::LoroValue::String(label.into())),
                    ];
                    (edge_id, fields)
                })
                .collect();

            let refs: Vec<CrdtBatchOp<'_>> = ops
                .iter()
                .map(|(id, fields)| (edge_coll.as_str(), id.as_str(), fields.as_slice()))
                .collect();

            crdt.batch_upsert(&refs).map_err(NodeDbError::storage)?;
        }

        self.update_memory_stats();
        Ok(())
    }

    /// Compact all per-collection CSR graph indices (merge buffer into dense arrays).
    pub fn compact_graph(&self) -> NodeDbResult<()> {
        let mut csr_map = self.csr.lock_or_recover();
        for (name, csr) in csr_map.iter_mut() {
            csr.compact().map_err(|e| {
                NodeDbError::storage(format!("graph csr compact failed for '{name}': {e}"))
            })?;
        }
        Ok(())
    }

    /// Evict HNSW collections to reduce memory usage.
    ///
    /// Persists each evicted collection to storage first, then drops
    /// it from memory. Collections are evicted smallest-first.
    pub async fn evict_collections(&self, max_to_evict: usize) -> NodeDbResult<usize> {
        let mut evicted = 0;

        let candidates: Vec<(String, usize)> = {
            let indices = self.vector_state.hnsw_indices.lock_or_recover();
            let mut sorted: Vec<(String, usize)> = indices
                .iter()
                .map(|(name, idx)| (name.clone(), idx.len()))
                .collect();
            sorted.sort_by_key(|(_, size)| *size);
            sorted
        };

        // Check once whether the pagedb segment path is available.
        #[cfg(not(target_arch = "wasm32"))]
        let seg_ext = self.storage.as_vector_segment_ext();

        for (name, _) in candidates.into_iter().take(max_to_evict) {
            // Snapshot checkpoint while holding the lock.
            // On native with segment support: graph-only bytes + extract vectors.
            // Otherwise: full checkpoint blob (WASM and non-pagedb native backends).
            #[cfg(not(target_arch = "wasm32"))]
            let (blob, segment_data) = {
                let indices = self.vector_state.hnsw_indices.lock_or_recover();
                match indices.get(&name) {
                    Some(idx) => {
                        if seg_ext.is_some() {
                            let graph_bytes = idx.graph_checkpoint_to_bytes().map_err(|e| {
                                NodeDbError::serialization("hnsw-graph-checkpoint", e)
                            })?;
                            let (vectors, surrogates) = idx.extract_vectors_and_surrogates();
                            let dim = idx.dim();
                            (graph_bytes, Some((dim, vectors, surrogates)))
                        } else {
                            let blob = idx
                                .checkpoint_to_bytes()
                                .map_err(|e| NodeDbError::serialization("hnsw-checkpoint", e))?;
                            (blob, None)
                        }
                    }
                    None => continue,
                }
            };
            #[cfg(target_arch = "wasm32")]
            let blob = {
                let indices = self.vector_state.hnsw_indices.lock_or_recover();
                match indices.get(&name) {
                    Some(idx) => idx
                        .checkpoint_to_bytes()
                        .map_err(|e| NodeDbError::serialization("hnsw-checkpoint", e))?,
                    None => continue,
                }
            };

            let key = format!("hnsw:{name}");
            self.storage
                .put(
                    Namespace::Vector,
                    key.as_bytes(),
                    &crate::storage::checksum::wrap(&blob),
                )
                .await
                .map_err(NodeDbError::storage)?;

            // Write vector segment on native targets when segment ext is available.
            #[cfg(not(target_arch = "wasm32"))]
            if let (Some((dim, vectors, surrogates)), Some(ext)) = (segment_data, seg_ext)
                && let Err(e) = ext
                    .write_vector_segment(&name, dim, &vectors, &surrogates)
                    .await
            {
                tracing::error!(
                    collection = %name,
                    error = %e,
                    "HNSW vector segment write failed during eviction; \
                     graph blob is persisted but vectors may be lost on cold restart"
                );
            }

            {
                let mut indices = self.vector_state.hnsw_indices.lock_or_recover();
                indices.remove(&name);
            }

            tracing::info!(collection = %name, "HNSW collection evicted from memory");
            evicted += 1;
        }

        self.update_memory_stats();
        Ok(evicted)
    }

    /// Check memory pressure and evict if needed.
    pub async fn check_and_evict(&self) -> NodeDbResult<usize> {
        use crate::memory::PressureLevel;

        self.update_memory_stats();
        match self.governor.pressure() {
            PressureLevel::Critical => self.evict_collections(2).await,
            PressureLevel::Warning => self.evict_collections(1).await,
            PressureLevel::Normal => Ok(0),
        }
    }
}
