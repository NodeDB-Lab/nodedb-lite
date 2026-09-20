// SPDX-License-Identifier: Apache-2.0
//! Vector-primary `DirectTruncate`, and the index clear a document
//! `TRUNCATE` applies to every bucket its collection owns.
//!
//! Every stored row leaves through the same per-row path `DirectDelete`
//! takes: HNSW tombstone, durable vector remove, payload row delete. A
//! durable vector with no payload row behind it goes too, so a reopen
//! cannot rebuild it. The index bucket is then reset: a fresh HNSW under
//! the same dimension and params, an empty id map, no codec sidecar, and
//! no stale checkpoint. The collection's config stays registered.

use std::sync::Arc;

use nodedb_types::Namespace;

use crate::engine::vector::{HnswIndex, VectorState};
use crate::error::LiteError;
use crate::nodedb::LockExt;
use crate::query::engine::LiteQueryEngine;
use crate::query::truncate::truncated;
use crate::storage::engine::StorageEngine;

use super::super::adapter::LitePhysicalFut;
use super::common::{delete_row, index_key, read_all_rows, remove_durable, remove_live_node};

/// Remove every row of the collection's primary index and reset the bucket.
pub(in crate::query::physical_visitor) fn vector_direct_truncate<'a, S>(
    engine: &'a LiteQueryEngine<S>,
    collection: String,
    field: String,
) -> LitePhysicalFut<'a>
where
    S: StorageEngine + 'a,
{
    let key = index_key(&collection, &field);
    let vector_state = Arc::clone(&engine.vector_state);
    let crdt = Arc::clone(&engine.crdt);
    Box::pin(async move {
        for (doc_id, _) in read_all_rows(&crdt, &collection) {
            remove_live_node(&vector_state, &key, &doc_id);
            remove_durable(&vector_state, &key, &doc_id, "DirectTruncate").await?;
            delete_row(&crdt, &collection, &doc_id, "DirectTruncate")?;
        }
        clear_index(&vector_state, &key).await?;
        Ok(truncated())
    })
}

/// Remove every durable vector and live node under `index_key`, then reset
/// the bucket.
pub(crate) async fn clear_index<S: StorageEngine>(
    vector_state: &VectorState<S>,
    index_key: &str,
) -> Result<(), LiteError> {
    let rows =
        crate::engine::vector::durable::load_collection(&*vector_state.storage, index_key).await?;
    for (doc_id, _) in rows {
        remove_live_node(vector_state, index_key, &doc_id);
        remove_durable(vector_state, index_key, &doc_id, "Truncate").await?;
    }
    reset_index(vector_state, index_key).await
}

/// Clear every index bucket `collection` owns: its base key and each
/// `collection:<field>` key that is live or configured. The base key is
/// always cleared: its durable prefix `v:<collection>:` covers the rows of
/// every named bucket too, so no durable row survives for a bucket that is
/// not in memory.
pub(crate) async fn clear_collection_indexes<S: StorageEngine>(
    vector_state: &VectorState<S>,
    collection: &str,
) -> Result<(), LiteError> {
    let owned = |key: &str| {
        key.strip_prefix(collection)
            .is_some_and(|rest| rest.starts_with(':'))
    };
    let mut keys: Vec<String> = vec![collection.to_string()];
    {
        let indices = vector_state.hnsw_indices.lock_or_recover();
        keys.extend(indices.keys().filter(|k| owned(k)).cloned());
    }
    {
        let configs = vector_state.per_index_config.lock_or_recover();
        keys.extend(configs.keys().filter(|k| owned(k)).cloned());
    }
    keys.sort();
    keys.dedup();
    for key in &keys {
        clear_index(vector_state, key).await?;
    }
    Ok(())
}

/// Replace the HNSW bucket with an empty one of the same shape, drop its
/// id-map entries, codec sidecar, and persisted checkpoint.
async fn reset_index<S: StorageEngine>(
    vector_state: &VectorState<S>,
    index_key: &str,
) -> Result<(), LiteError> {
    {
        let mut indices = vector_state.hnsw_indices.lock_or_recover();
        if let Some(index) = indices.get_mut(index_key) {
            *index = HnswIndex::new(index.dim(), index.params().clone());
        }
    }
    let prefix = format!("{index_key}:");
    vector_state
        .vector_id_map
        .lock_or_recover()
        .retain(|k, _| !k.starts_with(&prefix));
    vector_state.unloadable.lock_or_recover().remove(index_key);
    crate::engine::vector::sidecar::remove_sidecar(vector_state, index_key).await?;
    // The checkpoint describes the old graph; the next flush writes the
    // empty index in its place.
    vector_state
        .storage
        .delete(Namespace::Vector, format!("hnsw:{index_key}").as_bytes())
        .await
}

#[cfg(test)]
mod tests {
    use nodedb_physical::physical_plan::VectorDirectWriteIntent;
    use nodedb_types::{Surrogate, VectorQuantization, VectorStorageDtype};

    use super::super::write::{DirectWriteArgs, vector_direct_write};
    use super::*;
    use crate::PagedbStorageMem;
    use crate::query::engine::test_engine;

    const COLLECTION: &str = "vp";
    const FIELD: &str = "vec";

    async fn insert(engine: &LiteQueryEngine<PagedbStorageMem>, id: &str, vector: Vec<f32>) {
        vector_direct_write(
            engine,
            DirectWriteArgs {
                collection: COLLECTION.to_string(),
                field: FIELD.to_string(),
                surrogate: Surrogate::ZERO,
                pk_bytes: id.as_bytes().to_vec(),
                vector,
                payload: Vec::new(),
                quantization: VectorQuantization::None,
                storage_dtype: VectorStorageDtype::F32,
                intent: VectorDirectWriteIntent::Insert,
                on_conflict_updates: Vec::new(),
            },
        )
        .expect("lower")
        .await
        .expect("insert");
    }

    fn live_nodes(engine: &LiteQueryEngine<PagedbStorageMem>) -> usize {
        let indices = engine.vector_state.hnsw_indices.lock_or_recover();
        indices
            .get(&format!("{COLLECTION}:{FIELD}"))
            .map(|idx| idx.live_count())
            .unwrap_or(0)
    }

    #[tokio::test]
    async fn truncate_removes_rows_durable_vectors_and_resets_the_index() {
        let engine = test_engine().await;
        for (i, id) in ["a", "b", "c"].iter().enumerate() {
            insert(&engine, id, vec![i as f32, 1.0]).await;
        }
        assert_eq!(live_nodes(&engine), 3);
        let key = format!("{COLLECTION}:{FIELD}");

        let r = vector_direct_truncate(&engine, COLLECTION.into(), FIELD.into())
            .await
            .expect("truncate");
        assert_eq!(r.rows_affected, 0);
        assert_eq!(r.command.as_deref(), Some("TRUNCATE"));
        assert_eq!(live_nodes(&engine), 0);
        {
            let indices = engine.vector_state.hnsw_indices.lock_or_recover();
            let idx = indices.get(&key).expect("bucket stays registered");
            assert_eq!(idx.len(), 0, "tombstones are gone, not just hidden");
            assert_eq!(idx.dim(), 2, "dimension is kept");
        }
        assert!(read_all_rows(&engine.crdt, COLLECTION).is_empty());
        let durable =
            crate::engine::vector::durable::load_collection(&*engine.vector_state.storage, &key)
                .await
                .expect("load");
        assert!(durable.is_empty(), "durable vectors are removed");
        assert!(
            engine
                .vector_state
                .vector_id_map
                .lock_or_recover()
                .keys()
                .all(|k| !k.starts_with(&format!("{key}:")))
        );

        insert(&engine, "d", vec![9.0, 9.0]).await;
        assert_eq!(live_nodes(&engine), 1);
        assert_eq!(read_all_rows(&engine.crdt, COLLECTION).len(), 1);
    }

    #[tokio::test]
    async fn clear_collection_indexes_covers_base_and_named_buckets_only() {
        use super::super::common::insert_node;
        let engine = test_engine().await;
        let state = &engine.vector_state;
        insert_node(state, "docs", "a", &[1.0, 0.0], "test")
            .await
            .expect("base");
        insert_node(state, "docs:emb", "a", &[0.0, 1.0], "test")
            .await
            .expect("named");
        insert_node(state, "docs2", "z", &[1.0, 1.0], "test")
            .await
            .expect("other collection");

        clear_collection_indexes(state, "docs")
            .await
            .expect("clear");

        {
            let indices = state.hnsw_indices.lock_or_recover();
            assert_eq!(indices.get("docs").map(|i| i.live_count()), Some(0));
            assert_eq!(indices.get("docs:emb").map(|i| i.live_count()), Some(0));
            assert_eq!(
                indices.get("docs2").map(|i| i.live_count()),
                Some(1),
                "a collection sharing the prefix is untouched"
            );
        }
        let durable = crate::engine::vector::durable::list_collections(&*state.storage)
            .await
            .expect("list");
        assert_eq!(durable, vec!["docs2".to_string()]);
    }

    #[tokio::test]
    async fn truncate_of_an_empty_collection_is_the_bare_tag() {
        let engine = test_engine().await;
        let r = vector_direct_truncate(&engine, COLLECTION.into(), FIELD.into())
            .await
            .expect("truncate");
        assert_eq!(r.command.as_deref(), Some("TRUNCATE"));
        assert_eq!(live_nodes(&engine), 0);
    }
}
