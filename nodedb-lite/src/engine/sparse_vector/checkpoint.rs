// SPDX-License-Identifier: Apache-2.0

//! Checkpoint serialization and restoration for [`SparseVectorManager`].
//!
//! Persists every sparse inverted index so a cold open loads them directly —
//! no rebuild pass over source documents is required.
//!
//! ## Key layout under `Namespace::Vector`
//!
//! `nodedb_types::Namespace` has no sparse-specific variant, so the sparse
//! index shares the vector engine's namespace behind a distinct `sparse:`
//! key prefix that cannot collide with the dense HNSW keys.
//!
//! | Key                        | Value                                                 |
//! |----------------------------|-------------------------------------------------------|
//! | `sparse:_indices`          | MessagePack `Vec<String>` — index key list            |
//! | `sparse:{index_key}:docs`  | MessagePack `Vec<(String, Vec<(u32, f32)>)>`          |
//!
//! Documents are stored as their own `(dimension, weight)` entries rather than
//! as posting lists: the inverted index is a pure function of the document
//! vectors, so storing the source form keeps the blob minimal and makes the
//! restored index structurally identical to a freshly built one.
//!
//! [`SparseVectorManager`]: super::manager::SparseVectorManager

use std::collections::HashMap;

use nodedb_types::Namespace;
use nodedb_types::error::{NodeDbError, NodeDbResult};

use crate::storage::engine::{StorageEngine, WriteOp};

use super::index::SparseInvertedIndex;

/// Key holding the MessagePack list of live sparse index keys.
const INDEX_LIST_KEY: &[u8] = b"sparse:_indices";

/// A single document's sparse vector as persisted.
type SerDocument = (String, Vec<(u32, f32)>);

/// Per-index documents key: `sparse:{index_key}:docs`.
fn documents_key(index_key: &str) -> String {
    format!("sparse:{index_key}:docs")
}

/// Serialize sparse index state into write ops.
///
/// Pure and synchronous, so it is safe to call while holding the manager's
/// mutex guard; the caller performs the I/O after releasing the lock.
///
/// Empty indexes are still listed so that a checkpoint taken after every
/// document was deleted restores as empty rather than resurrecting the
/// previous checkpoint's contents.
pub(crate) fn serialize_sparse(
    indices: &HashMap<String, SparseInvertedIndex>,
) -> NodeDbResult<Vec<WriteOp>> {
    let mut ops: Vec<WriteOp> = Vec::with_capacity(indices.len() + 1);

    let mut index_keys: Vec<String> = indices.keys().cloned().collect();
    index_keys.sort();
    let keys_bytes = zerompk::to_msgpack_vec(&index_keys)
        .map_err(|e| NodeDbError::serialization("msgpack", e))?;
    ops.push(WriteOp::Put {
        ns: Namespace::Vector,
        key: INDEX_LIST_KEY.to_vec(),
        value: keys_bytes,
    });

    for (index_key, index) in indices {
        let documents: Vec<SerDocument> = index.documents();
        let bytes = zerompk::to_msgpack_vec(&documents)
            .map_err(|e| NodeDbError::serialization("msgpack", e))?;
        ops.push(WriteOp::Put {
            ns: Namespace::Vector,
            key: documents_key(index_key).into_bytes(),
            value: bytes,
        });
    }

    Ok(ops)
}

/// Write pre-serialized sparse index state to storage.
pub(crate) async fn write_serialized_sparse<S>(storage: &S, ops: Vec<WriteOp>) -> NodeDbResult<()>
where
    S: StorageEngine,
{
    if ops.is_empty() {
        return Ok(());
    }
    storage
        .batch_write(&ops)
        .await
        .map_err(|e| NodeDbError::storage(format!("sparse checkpoint batch_write: {e}")))?;
    Ok(())
}

/// Restore sparse index state from storage on cold open.
///
/// Returns `None` when no checkpoint has ever been written, which the caller
/// distinguishes from a checkpoint that legitimately holds no documents — only
/// the former warrants a rebuild from source documents. A corrupt blob for one
/// index is logged and that index restored empty rather than failing the whole
/// open; the remaining indexes stay usable.
pub(crate) async fn restore_sparse<S>(
    storage: &S,
) -> NodeDbResult<Option<HashMap<String, SparseInvertedIndex>>>
where
    S: StorageEngine,
{
    let Some(keys_bytes) = storage.get(Namespace::Vector, INDEX_LIST_KEY).await? else {
        return Ok(None);
    };
    let Ok(index_keys) = zerompk::from_msgpack::<Vec<String>>(&keys_bytes) else {
        tracing::warn!("sparse checkpoint: index list undecodable — starting fresh");
        return Ok(None);
    };

    let mut indices: HashMap<String, SparseInvertedIndex> =
        HashMap::with_capacity(index_keys.len());

    for index_key in &index_keys {
        let key = documents_key(index_key);
        let Some(bytes) = storage.get(Namespace::Vector, key.as_bytes()).await? else {
            indices.insert(index_key.clone(), SparseInvertedIndex::new());
            continue;
        };
        match zerompk::from_msgpack::<Vec<SerDocument>>(&bytes) {
            Ok(documents) => {
                indices.insert(
                    index_key.clone(),
                    SparseInvertedIndex::from_documents(documents),
                );
            }
            Err(e) => {
                tracing::warn!(
                    index_key,
                    error = %e,
                    "sparse checkpoint: document blob undecodable — index restored empty"
                );
                indices.insert(index_key.clone(), SparseInvertedIndex::new());
            }
        }
    }

    tracing::debug!(index_count = indices.len(), "sparse checkpoint restored");
    Ok(Some(indices))
}

#[cfg(test)]
mod tests {
    use nodedb_types::SparseVector;

    use super::*;
    use crate::PagedbStorageMem;

    fn sv(entries: &[(u32, f32)]) -> SparseVector {
        SparseVector::from_entries(entries.to_vec()).expect("valid sparse vector")
    }

    #[tokio::test]
    async fn round_trip_through_storage() {
        let storage = PagedbStorageMem::open_in_memory()
            .await
            .expect("in-memory pagedb");

        let mut index = SparseInvertedIndex::new();
        index.insert("d1", &sv(&[(1, 0.5), (9, 0.25)]));
        index.insert("d2", &sv(&[(9, 1.0)]));

        let mut indices = HashMap::new();
        indices.insert("docs:emb".to_string(), index);

        let ops = serialize_sparse(&indices).expect("serialize");
        write_serialized_sparse(&storage, ops).await.expect("write");

        let restored = restore_sparse(&storage)
            .await
            .expect("restore")
            .expect("checkpoint present");
        let restored_index = restored.get("docs:emb").expect("index restored");
        assert_eq!(restored_index.doc_count(), 2);

        let hits = restored_index.search(&sv(&[(9, 1.0)]), 10);
        assert_eq!(hits.len(), 2);
        assert_eq!(hits[0].doc_id, "d2");
    }

    #[tokio::test]
    async fn absent_checkpoint_reports_none() {
        let storage = PagedbStorageMem::open_in_memory()
            .await
            .expect("in-memory pagedb");
        let restored = restore_sparse(&storage).await.expect("restore");
        assert!(restored.is_none());
    }
}
