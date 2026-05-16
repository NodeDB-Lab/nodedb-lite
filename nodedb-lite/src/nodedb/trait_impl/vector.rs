// SPDX-License-Identifier: Apache-2.0

//! Vector engine helpers for `NodeDbLite`.

use loro::LoroValue;

use nodedb_types::document::Document;
use nodedb_types::error::{NodeDbError, NodeDbResult};
use nodedb_types::filter::MetadataFilter;
use nodedb_types::result::SearchResult;

use crate::nodedb::LockExt;
use crate::nodedb::NodeDbLite;
use crate::nodedb::convert::value_to_loro;
use crate::storage::engine::{StorageEngine, StorageEngineSync};

/// Internal fields stripped from search-result metadata for a single-vector collection.
pub(super) const INTERNAL_FIELDS_BASE: &[&str] = &["embedding_dim"];
/// Internal fields stripped from search-result metadata for a named-vector collection
/// (adds `__field` which records which named vector the row belongs to).
pub(super) const INTERNAL_FIELDS_NAMED: &[&str] = &["embedding_dim", "__field"];

impl<S: StorageEngine + StorageEngineSync> NodeDbLite<S> {
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
            let mut indices = self.vector_state.hnsw_indices.lock_or_recover();
            let index = Self::ensure_hnsw(&mut indices, collection, embedding.len());
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
            let mut indices = self.vector_state.hnsw_indices.lock_or_recover();
            if let Some(index) = indices.get_mut(collection) {
                index.delete(iid);
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
            let mut indices = self.vector_state.hnsw_indices.lock_or_recover();
            let index = Self::ensure_hnsw(&mut indices, &index_key, embedding.len());
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
