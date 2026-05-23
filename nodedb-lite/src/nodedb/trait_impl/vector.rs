// SPDX-License-Identifier: Apache-2.0

//! Vector engine helpers for `NodeDbLite`.

use loro::LoroValue;

use nodedb_types::document::Document;
use nodedb_types::error::{NodeDbError, NodeDbResult};
use nodedb_types::filter::MetadataFilter;
use nodedb_types::result::SearchResult;
use nodedb_types::vector_dtype::VectorStorageDtype;

use crate::engine::vector::state::ensure_hnsw;
use crate::nodedb::LockExt;
use crate::nodedb::NodeDbLite;
use crate::nodedb::convert::value_to_loro;
use crate::storage::engine::StorageEngine;

/// Internal fields stripped from search-result metadata for a single-vector collection.
pub(super) const INTERNAL_FIELDS_BASE: &[&str] = &["embedding_dim"];
/// Internal fields stripped from search-result metadata for a named-vector collection
/// (adds `__field` which records which named vector the row belongs to).
pub(super) const INTERNAL_FIELDS_NAMED: &[&str] = &["embedding_dim", "__field"];

impl<S: StorageEngine> NodeDbLite<S> {
    /// Shared vector search implementation.
    pub(super) async fn vector_search_internal(
        &self,
        index_key: &str,
        collection: &str,
        query: &[f32],
        k: usize,
        filter: Option<&MetadataFilter>,
        exclude_fields: &[&str],
    ) -> NodeDbResult<Vec<SearchResult>> {
        crate::engine::vector::search::run_vector_search(
            &self.vector_state,
            &self.crdt,
            index_key,
            collection,
            query,
            k,
            filter,
            exclude_fields,
            None,
            None,
            false,
            None,
            None,
        )
        .await
    }

    /// Insert a single embedding into the collection's default HNSW index and
    /// persist its document fields (including the embedding dimension) to CRDT
    /// storage. Lazily creates the HNSW index on first insert.
    pub(super) async fn vector_insert_impl(
        &self,
        collection: &str,
        id: &str,
        embedding: &[f32],
        metadata: Option<Document>,
    ) -> NodeDbResult<()> {
        let internal_id = {
            let dtype = {
                let configs = self.vector_state.per_index_config.lock_or_recover();
                configs
                    .get(collection)
                    .map(|cfg| cfg.storage_dtype)
                    .unwrap_or(VectorStorageDtype::F32)
            };
            let mut indices = self.vector_state.hnsw_indices.lock_or_recover();
            let index = ensure_hnsw(&mut indices, collection, embedding.len(), dtype);
            let id_before = index.len() as u32;
            index
                .insert(embedding.to_vec())
                .map_err(NodeDbError::bad_request)?;
            id_before
        };

        {
            let mut id_map = self.vector_state.vector_id_map.lock_or_recover();
            id_map.insert(
                format!("{collection}:{internal_id}"),
                (id.to_string(), internal_id),
            );
        }

        // Lazily install a sidecar if the collection config calls for one, then
        // encode the just-inserted vector.  Sidecar install errors surface as
        // BadRequest (e.g. unsupported codec).  Encode failures warn-and-continue
        // so a single bad vector does not abort the insert; affected rows degrade
        // to FP32 rerank at search time.
        match crate::engine::vector::sidecar::ensure_sidecar(&self.vector_state, collection) {
            Ok(true) => {
                let mut sidecars = self.vector_state.codec_sidecars.lock_or_recover();
                if let Some(sidecar) = sidecars.get_mut(collection)
                    && let Err(e) = sidecar.encode_and_insert(internal_id, embedding)
                {
                    tracing::warn!(
                        index_key = collection,
                        id = internal_id,
                        error = %e,
                        "sidecar encode_and_insert failed; row falls back to FP32 rerank"
                    );
                }
            }
            Ok(false) => {}
            Err(e) => return Err(NodeDbError::bad_request(e.to_string())),
        }

        {
            let mut crdt = self.crdt.lock_or_recover();
            let mut fields = vec![("embedding_dim", LoroValue::I64(embedding.len() as i64))];
            if let Some(meta) = &metadata {
                for (k, v) in &meta.fields {
                    fields.push((k.as_str(), value_to_loro(v)));
                }
            }
            crdt.upsert(collection, id, &fields)
                .map_err(NodeDbError::storage)?;
        }

        // Enqueue for sync to Origin (no-op when sync is disabled).
        #[cfg(not(target_arch = "wasm32"))]
        if let Some(q) = &self.vector_outbound {
            q.enqueue_insert(collection, id, embedding.to_vec(), embedding.len(), "");
        }

        self.update_memory_stats();
        Ok(())
    }

    /// Tombstone an embedding in the HNSW index (by external id → internal id
    /// lookup) and delete its CRDT document. The HNSW slot is reclaimed lazily
    /// on later inserts; no compaction is performed here.
    pub(super) async fn vector_delete_impl(&self, collection: &str, id: &str) -> NodeDbResult<()> {
        let internal_id = {
            let id_map = self.vector_state.vector_id_map.lock_or_recover();
            id_map
                .iter()
                .find(|(_, (doc_id, _))| doc_id == id)
                .map(|(_, (_, iid))| *iid)
        };

        if let Some(iid) = internal_id {
            {
                let mut indices = self.vector_state.hnsw_indices.lock_or_recover();
                if let Some(index) = indices.get_mut(collection) {
                    index.delete(iid);
                }
            }

            // Remove the encoded entry from any installed sidecar so it
            // doesn't carry stale data after the HNSW slot is tombstoned.
            {
                let mut sidecars = self.vector_state.codec_sidecars.lock_or_recover();
                if let Some(sidecar) = sidecars.get_mut(collection) {
                    sidecar.remove(iid);
                }
            }

            // Persist the updated sidecar after every delete. Deletes change
            // the sidecar's encoded-vector set in a way that cannot be
            // reconstructed cheaply from HNSW vectors alone (a deleted slot
            // is tombstoned and has no live vector to re-encode). Persisting
            // here ensures restarts don't re-surface deleted entries.
            if let Err(e) =
                crate::engine::vector::sidecar::persist_sidecar(&self.vector_state, collection)
                    .await
            {
                tracing::warn!(
                    error = %e,
                    collection,
                    "sidecar persist after delete failed; in-memory sidecar still valid"
                );
            }
        }

        {
            let mut crdt = self.crdt.lock_or_recover();
            crdt.delete(collection, id).map_err(NodeDbError::storage)?;
        }

        // Enqueue for sync to Origin (no-op when sync is disabled).
        #[cfg(not(target_arch = "wasm32"))]
        if let Some(q) = &self.vector_outbound {
            q.enqueue_delete(collection, id, "");
        }

        Ok(())
    }

    /// Insert an embedding into a named-vector sub-index of a collection.
    ///
    /// Each named field gets its own HNSW index keyed by `"{collection}:{field_name}"`
    /// so a single document can carry multiple independent embeddings. The CRDT row
    /// records the `__field` tag so search results can be re-associated with the
    /// originating field. When `field_name` is empty, this is equivalent to
    /// [`Self::vector_insert_impl`] (no `__field` tag, index keyed by collection).
    pub(super) async fn vector_insert_field_impl(
        &self,
        collection: &str,
        field_name: &str,
        id: &str,
        embedding: &[f32],
        metadata: Option<Document>,
    ) -> NodeDbResult<()> {
        let index_key = if field_name.is_empty() {
            collection.to_string()
        } else {
            format!("{collection}:{field_name}")
        };

        let internal_id = {
            let dtype = {
                let configs = self.vector_state.per_index_config.lock_or_recover();
                configs
                    .get(&index_key)
                    .map(|cfg| cfg.storage_dtype)
                    .unwrap_or(VectorStorageDtype::F32)
            };
            let mut indices = self.vector_state.hnsw_indices.lock_or_recover();
            let index = ensure_hnsw(&mut indices, &index_key, embedding.len(), dtype);
            let id_before = index.len() as u32;
            index
                .insert(embedding.to_vec())
                .map_err(NodeDbError::bad_request)?;
            id_before
        };

        {
            let mut id_map = self.vector_state.vector_id_map.lock_or_recover();
            id_map.insert(
                format!("{index_key}:{internal_id}"),
                (id.to_string(), internal_id),
            );
        }

        // Lazily install a sidecar if the collection config calls for one, then
        // encode the just-inserted vector.  Encode failures warn-and-continue.
        match crate::engine::vector::sidecar::ensure_sidecar(&self.vector_state, &index_key) {
            Ok(true) => {
                let mut sidecars = self.vector_state.codec_sidecars.lock_or_recover();
                if let Some(sidecar) = sidecars.get_mut(&index_key)
                    && let Err(e) = sidecar.encode_and_insert(internal_id, embedding)
                {
                    tracing::warn!(
                        index_key = %index_key,
                        id = internal_id,
                        error = %e,
                        "sidecar encode_and_insert failed; row falls back to FP32 rerank"
                    );
                }
            }
            Ok(false) => {}
            Err(e) => return Err(NodeDbError::bad_request(e.to_string())),
        }

        {
            let mut crdt = self.crdt.lock_or_recover();
            let mut fields = vec![
                (
                    "embedding_dim",
                    loro::LoroValue::I64(embedding.len() as i64),
                ),
                ("__field", loro::LoroValue::String(field_name.into())),
            ];
            if let Some(meta) = &metadata {
                for (k, v) in &meta.fields {
                    fields.push((k.as_str(), value_to_loro(v)));
                }
            }
            crdt.upsert(collection, id, &fields)
                .map_err(NodeDbError::storage)?;
        }

        // Enqueue for sync to Origin (no-op when sync is disabled).
        #[cfg(not(target_arch = "wasm32"))]
        if let Some(q) = &self.vector_outbound {
            q.enqueue_insert(
                collection,
                id,
                embedding.to_vec(),
                embedding.len(),
                field_name,
            );
        }

        self.update_memory_stats();
        Ok(())
    }
}
