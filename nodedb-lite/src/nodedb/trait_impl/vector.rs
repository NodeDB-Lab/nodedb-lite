// SPDX-License-Identifier: Apache-2.0

//! Vector engine helpers for `NodeDbLite`.

use std::collections::HashSet;

use loro::LoroValue;

use nodedb_types::document::Document;
use nodedb_types::error::{NodeDbError, NodeDbResult};
use nodedb_types::filter::MetadataFilter;
use nodedb_types::result::SearchResult;

use crate::engine::vector::nodes::{encode_sidecar, unbind_node, upsert_node};
use crate::engine::vector::resident::{check_insert_widths, lock_resident};
use crate::engine::vector::row::{
    EMBEDDING_DIM_FIELD, VECTOR_FIELD_TAG, VECTOR_ROW_FIELDS, VectorSlot, detach_vector_row,
    is_vector_primary,
};
use crate::engine::vector::search::{
    AnnTuning, SearchFilter, VectorSearchRequest, run_vector_search,
};
use crate::nodedb::LockExt;
use crate::nodedb::NodeDbLite;
use crate::nodedb::convert::value_to_loro;
use crate::storage::engine::StorageEngine;

/// Internal fields stripped from search-result metadata for a single-vector collection.
pub(super) const INTERNAL_FIELDS_BASE: &[&str] = &[EMBEDDING_DIM_FIELD];
/// Internal fields stripped from search-result metadata for a named-vector collection
/// (adds `__field` which records which named vector the row belongs to).
pub(super) const INTERNAL_FIELDS_NAMED: &[&str] = VECTOR_ROW_FIELDS;

/// One search issued through the `NodeDb` trait.
///
/// `index_key` names the HNSW index. `collection` names the CRDT collection
/// that holds the row metadata. `exclude_fields` lists the vector-internal row
/// fields that never reach result metadata.
pub(super) struct TraitVectorSearch<'a> {
    pub index_key: &'a str,
    pub collection: &'a str,
    pub query: &'a [f32],
    pub k: usize,
    pub filter: Option<&'a MetadataFilter>,
    pub exclude_fields: &'a [&'a str],
    pub allowed_ids: Option<&'a HashSet<String>>,
}

impl<S: StorageEngine> NodeDbLite<S> {
    /// Shared vector search implementation.
    ///
    /// When `allowed_ids` is `Some`, translates the set of string doc-IDs to a
    /// `RoaringBitmap` of the nodes they hold in `index_key`'s index and
    /// passes it as the `prefilter_bitmap`, so only documents from the allowed
    /// set are returned. Nodes of any other index never enter the bitmap.
    pub(super) async fn vector_search_internal(
        &self,
        search: TraitVectorSearch<'_>,
    ) -> NodeDbResult<Vec<SearchResult>> {
        let TraitVectorSearch {
            index_key,
            collection,
            query,
            k,
            filter,
            exclude_fields,
            allowed_ids,
        } = search;
        let prefilter = allowed_ids.map(|ids| {
            let id_map = self.vector_state.vector_id_map.lock_or_recover();
            let mut bm = roaring::RoaringBitmap::new();
            if let Some(index_ids) = id_map.index(index_key) {
                bm.extend(ids.iter().filter_map(|doc_id| index_ids.node(doc_id)));
            }
            bm
        });
        run_vector_search(
            &self.vector_state,
            &self.crdt,
            VectorSearchRequest {
                index_key,
                collection,
                query,
                k,
                filter: SearchFilter {
                    metadata: filter,
                    exclude_fields,
                    prefilter_bitmap: prefilter.as_ref(),
                },
                tuning: AnnTuning::default(),
                skip_payload_fetch: false,
            },
        )
        .await
    }

    /// Insert a single embedding into the collection's default HNSW index and
    /// merge its metadata (including the embedding dimension) into the CRDT
    /// row with the same id. Other row fields are kept. Re-inserting an id
    /// replaces its vector. Lazily creates the HNSW index on first insert.
    pub(super) async fn vector_insert_impl(
        &self,
        collection: &str,
        id: &str,
        embedding: &[f32],
        metadata: Option<Document>,
    ) -> NodeDbResult<()> {
        if self.governor.worst_engine_pressure() == nodedb_mem::PressureLevel::Emergency {
            return Err(nodedb_types::error::NodeDbError::storage(
                crate::error::LiteError::Backpressure {
                    detail: "vector insert rejected: memory governor is at Emergency pressure"
                        .into(),
                },
            ));
        }

        // A vector of another width than the collection's index, loaded back
        // if it was evicted, is refused before its durable row is written.
        check_insert_widths(&self.vector_state, collection, [embedding.len()])
            .await
            .map_err(NodeDbError::from)?;

        // Make the vector durable BEFORE it enters the in-memory index. The
        // durable row is the source of truth that the index and the pagedb
        // segment are both derived from, so it must never be the copy that is
        // missing after a crash — and a flush that found no durable row would
        // write no segment at all.
        if !embedding.is_empty() {
            let op = crate::engine::vector::durable::put_op(collection, id, embedding);
            self.storage
                .batch_write(std::slice::from_ref(&op))
                .await
                .map_err(NodeDbError::storage)?;
        }

        // Replaces the node `id` held before, so the id stays one node.
        let internal_id = upsert_node(&self.vector_state, collection, id, embedding)
            .await
            .map_err(NodeDbError::from)?;
        // A sidecar install error is a bad request (e.g. unsupported codec).
        encode_sidecar(&self.vector_state, collection, internal_id, embedding)
            .map_err(|e| NodeDbError::bad_request(e.to_string()))?;

        {
            let mut crdt = self.crdt.lock_or_recover();
            let mut fields = vec![(EMBEDDING_DIM_FIELD, LoroValue::I64(embedding.len() as i64))];
            if let Some(meta) = &metadata {
                for (k, v) in &meta.fields {
                    fields.push((k.as_str(), value_to_loro(v)));
                }
            }
            // A merge: the vector attaches to the row and keeps its fields.
            crdt.set_fields(collection, id, &fields)
                .map_err(NodeDbError::from)?;
        }

        // Enqueue for sync to Origin (no-op when sync is disabled).
        #[cfg(not(target_arch = "wasm32"))]
        if let Some(q) = &self.vector_outbound {
            crate::sync::reconcile_outbound_enqueue(
                q.enqueue_insert(collection, id, embedding.to_vec(), embedding.len(), "")
                    .await,
                "vector insert",
                collection,
                id,
            )
            .map_err(nodedb_types::error::NodeDbError::storage)?;
        }

        self.update_memory_stats();
        Ok(())
    }

    /// Tombstone the base embedding of `id` and detach it from its CRDT row.
    /// See [`Self::vector_delete_slot`].
    pub(super) async fn vector_delete_impl(&self, collection: &str, id: &str) -> NodeDbResult<()> {
        self.vector_delete_slot(collection, "", id).await
    }

    /// Delete the named vector `field_name` of `id` in `collection`, keeping
    /// the row's document fields and any other vector attached to it. An
    /// empty `field_name` names the base vector, as `vector_delete` does.
    pub async fn vector_delete_field(
        &self,
        collection: &str,
        field_name: &str,
        id: &str,
    ) -> NodeDbResult<()> {
        self.vector_delete_slot(collection, field_name, id).await
    }

    /// Tombstone one embedding of `id` in its HNSW index and detach it from
    /// its CRDT row: only the fields that vector owns alone go, and document
    /// fields stay. The whole row goes once no vector remains attached and
    /// the collection is vector-primary or no document field remains. The
    /// tombstoned HNSW node keeps its slot; search skips it, and nothing here
    /// compacts the index.
    async fn vector_delete_slot(
        &self,
        collection: &str,
        field_name: &str,
        id: &str,
    ) -> NodeDbResult<()> {
        let (index_key, slot) = if field_name.is_empty() {
            (collection.to_string(), VectorSlot::Base)
        } else {
            (
                format!("{collection}:{field_name}"),
                VectorSlot::Named(field_name),
            )
        };

        // Drop the durable row FIRST. It is the source of truth the index is
        // rebuilt from, so leaving it behind would resurrect a deleted vector
        // on the next rebuild — the in-memory tombstone below does not survive
        // one. Ordering also matters: if the process dies between the two, a
        // surviving durable row would come back, whereas a removed row simply
        // leaves the tombstoned slot to be rebuilt away.
        crate::engine::vector::durable::remove(&*self.storage, &index_key, id)
            .await
            .map_err(NodeDbError::from)?;

        // The index, loaded back if it was evicted, takes the tombstone:
        // an evicted index that missed it would bring the vector back.
        let (internal_id, remaining) = {
            let mut indices = lock_resident(&self.vector_state, &index_key)
                .await
                .map_err(NodeDbError::from)?;
            let node = unbind_node(
                &self.vector_state,
                indices.get_mut(&index_key),
                &index_key,
                id,
            );
            let remaining = self
                .vector_state
                .vector_id_map
                .lock_or_recover()
                .attached_vectors(collection, id);
            (node, remaining)
        };

        if internal_id.is_some() {
            // Persist the updated sidecar after every delete. Deletes change
            // the sidecar's encoded-vector set in a way that cannot be
            // reconstructed cheaply from HNSW vectors alone (a deleted slot
            // is tombstoned and has no live vector to re-encode). Persisting
            // here ensures restarts don't re-surface deleted entries.
            if let Err(e) =
                crate::engine::vector::sidecar::persist_sidecar(&self.vector_state, &index_key)
                    .await
            {
                tracing::warn!(
                    error = %e,
                    index_key,
                    "sidecar persist after delete failed; in-memory sidecar still valid"
                );
            }
        }

        let vector_primary = is_vector_primary(&*self.storage, collection)
            .await
            .map_err(NodeDbError::from)?;
        {
            let mut crdt = self.crdt.lock_or_recover();
            detach_vector_row(&mut crdt, collection, id, slot, &remaining, vector_primary)
                .map_err(NodeDbError::storage)?;
        }

        // Enqueue for sync to Origin (no-op when sync is disabled).
        #[cfg(not(target_arch = "wasm32"))]
        if let Some(q) = &self.vector_outbound {
            crate::sync::reconcile_outbound_enqueue(
                q.enqueue_delete(collection, id, field_name).await,
                "vector delete",
                collection,
                id,
            )
            .map_err(nodedb_types::error::NodeDbError::storage)?;
        }

        Ok(())
    }

    /// Insert an embedding into a named-vector sub-index of a collection.
    ///
    /// Each named field gets its own HNSW index keyed by `"{collection}:{field_name}"`
    /// so a single document can carry multiple independent embeddings. The CRDT row
    /// records the `__field` tag so search results can be re-associated with the
    /// originating field; the row's other fields are kept. Re-inserting an id
    /// replaces its vector. When `field_name` is empty, this is equivalent to
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

        check_insert_widths(&self.vector_state, &index_key, [embedding.len()])
            .await
            .map_err(NodeDbError::from)?;

        // Durable row first — see `vector_insert_impl`. Keyed by `index_key` so
        // each named-vector sub-index rebuilds from its own rows.
        if !embedding.is_empty() {
            let op = crate::engine::vector::durable::put_op(&index_key, id, embedding);
            self.storage
                .batch_write(std::slice::from_ref(&op))
                .await
                .map_err(NodeDbError::storage)?;
        }

        // Replaces the node `id` held before, so the id stays one node.
        let internal_id = upsert_node(&self.vector_state, &index_key, id, embedding)
            .await
            .map_err(NodeDbError::from)?;
        encode_sidecar(&self.vector_state, &index_key, internal_id, embedding)
            .map_err(|e| NodeDbError::bad_request(e.to_string()))?;

        {
            let mut crdt = self.crdt.lock_or_recover();
            let mut fields = vec![
                (EMBEDDING_DIM_FIELD, LoroValue::I64(embedding.len() as i64)),
                (VECTOR_FIELD_TAG, LoroValue::String(field_name.into())),
            ];
            if let Some(meta) = &metadata {
                for (k, v) in &meta.fields {
                    fields.push((k.as_str(), value_to_loro(v)));
                }
            }
            // A merge: the vector attaches to the row and keeps its fields.
            crdt.set_fields(collection, id, &fields)
                .map_err(NodeDbError::from)?;
        }

        // Enqueue for sync to Origin (no-op when sync is disabled).
        #[cfg(not(target_arch = "wasm32"))]
        if let Some(q) = &self.vector_outbound {
            crate::sync::reconcile_outbound_enqueue(
                q.enqueue_insert(
                    collection,
                    id,
                    embedding.to_vec(),
                    embedding.len(),
                    field_name,
                )
                .await,
                "vector field insert",
                collection,
                id,
            )
            .map_err(nodedb_types::error::NodeDbError::storage)?;
        }

        self.update_memory_stats();
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    //! Vector delete on a collection declared vector-primary: the row exists
    //! for its vector, so it goes with the last vector even when it carries
    //! payload fields.

    use nodedb_client::NodeDb;
    use nodedb_types::collection::CollectionType;
    use nodedb_types::collection_config::{PartitionStrategy, PrimaryEngine};
    use nodedb_types::document::Document;
    use nodedb_types::id::DatabaseId;
    use nodedb_types::sync::wire::CollectionDescriptor;
    use nodedb_types::value::Value;

    use crate::PagedbStorageMem;
    use crate::engine::vector::row::is_vector_primary;
    use crate::nodedb::NodeDbLite;
    use crate::nodedb::collection::CollectionMeta;
    use crate::storage::engine::StorageEngine;

    async fn open_db() -> std::sync::Arc<NodeDbLite<PagedbStorageMem>> {
        let storage = PagedbStorageMem::open_in_memory()
            .await
            .expect("in-memory storage");
        NodeDbLite::open(storage).await.expect("open")
    }

    /// Persist `name`'s collection meta with a vector-primary descriptor, as an
    /// inbound schema announcement does.
    async fn declare_vector_primary(db: &NodeDbLite<PagedbStorageMem>, name: &str) {
        let descriptor = CollectionDescriptor {
            tenant_id: 1,
            database_id: DatabaseId::DEFAULT,
            name: name.into(),
            collection_type: CollectionType::document(),
            bitemporal: false,
            crdt: false,
            fields: Vec::new(),
            primary: PrimaryEngine::Vector,
            vector_primary: None,
            partition_strategy: PartitionStrategy::default(),
            declared_primary_key: None,
            descriptor_version: 1,
        };
        let meta = CollectionMeta {
            name: name.into(),
            collection_type: "document".into(),
            created_at_ms: 0,
            fields: Vec::new(),
            config_json: None,
            descriptor_json: Some(sonic_rs::to_string(&descriptor).expect("descriptor json")),
            bitemporal: false,
            crdt: false,
        };
        db.storage
            .put(
                nodedb_types::Namespace::Meta,
                format!("collection:{name}").as_bytes(),
                &sonic_rs::to_vec(&meta).expect("meta json"),
            )
            .await
            .expect("put meta");
    }

    #[tokio::test]
    async fn vector_delete_on_vector_collection_removes_row() {
        let db = open_db().await;
        declare_vector_primary(&db, "vp").await;
        assert!(
            is_vector_primary(&*db.storage, "vp")
                .await
                .expect("read meta")
        );

        let mut payload = Document::new("v1");
        payload.set("title", Value::String("hello".into()));
        db.vector_insert("vp", "v1", &[1.0, 0.0], Some(payload))
            .await
            .expect("vector_insert");
        db.vector_delete("vp", "v1").await.expect("vector_delete");

        assert!(
            db.document_get("vp", "v1")
                .await
                .expect("document_get")
                .is_none(),
            "a vector-primary row goes with its vector"
        );
    }

    #[tokio::test]
    async fn undeclared_collection_is_not_vector_primary() {
        let db = open_db().await;
        assert!(
            !is_vector_primary(&*db.storage, "plain")
                .await
                .expect("read meta")
        );
    }
}
