//! Batch operations and memory eviction for NodeDbLite.

use nodedb_types::Namespace;
use nodedb_types::error::{NodeDbError, NodeDbResult};

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

        let dim = vectors[0].1.len();

        {
            let mut indices = self.hnsw_indices.lock_or_recover();
            let index = Self::ensure_hnsw(&mut indices, collection, dim);
            let mut id_map = self.vector_id_map.lock_or_recover();

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

    /// Batch insert graph edges — O(1) CRDT delta export instead of O(N).
    pub fn batch_graph_insert_edges(&self, edges: &[(&str, &str, &str)]) -> NodeDbResult<()> {
        if edges.is_empty() {
            return Ok(());
        }

        {
            let mut csr = self.csr.lock_or_recover();
            for &(src, dst, label) in edges {
                csr.add_edge(src, label, dst);
            }
        }

        {
            let mut crdt = self.crdt.lock_or_recover();

            use crate::engine::crdt::engine::{CrdtBatchOp, CrdtField};

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
                .map(|(id, fields)| ("__edges", id.as_str(), fields.as_slice()))
                .collect();

            crdt.batch_upsert(&refs).map_err(NodeDbError::storage)?;
        }

        self.update_memory_stats();
        Ok(())
    }

    /// Compact the CSR graph index (merge buffer into dense arrays).
    pub fn compact_graph(&self) -> NodeDbResult<()> {
        let mut csr = self.csr.lock_or_recover();
        csr.compact();
        Ok(())
    }

    /// Evict HNSW collections to reduce memory usage.
    ///
    /// Persists each evicted collection to storage first, then drops
    /// it from memory. Collections are evicted smallest-first.
    pub async fn evict_collections(&self, max_to_evict: usize) -> NodeDbResult<usize> {
        let mut evicted = 0;

        let candidates: Vec<(String, usize)> = {
            let indices = self.hnsw_indices.lock_or_recover();
            let mut sorted: Vec<(String, usize)> = indices
                .iter()
                .map(|(name, idx)| (name.clone(), idx.len()))
                .collect();
            sorted.sort_by_key(|(_, size)| *size);
            sorted
        };

        for (name, _) in candidates.into_iter().take(max_to_evict) {
            let checkpoint = {
                let indices = self.hnsw_indices.lock_or_recover();
                match indices.get(&name) {
                    Some(idx) => idx.checkpoint_to_bytes(),
                    None => continue,
                }
            };

            let key = format!("hnsw:{name}");
            self.storage
                .put(Namespace::Vector, key.as_bytes(), &checkpoint)
                .await
                .map_err(NodeDbError::storage)?;

            {
                let mut indices = self.hnsw_indices.lock_or_recover();
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
