//! Checkpoint serialization and restoration for [`SpatialIndexManager`].
//!
//! Persists the full in-memory spatial state to `Namespace::Spatial` so that a
//! cold open can load the index without rebuilding from CRDT documents.
//!
//! ## Key layout under `Namespace::Spatial`
//!
//! | Key                                    | Value                                               |
//! |----------------------------------------|-----------------------------------------------------|
//! | `spatial:_collections`                 | MessagePack `Vec<(String, String)>` — (collection, field) pairs |
//! | `spatial:{collection}:{field}:rtree`   | CRC32C-wrapped R-tree checkpoint bytes              |
//! | `spatial:{collection}:{field}:docmap`  | MessagePack `Vec<(String, u64)>` — doc_id → entry_id |
//! | `spatial:_next_id`                     | MessagePack `u64` — next entry ID                  |
//!
//! The `docmap` key is what distinguishes this checkpoint from the original
//! inline implementation: persisting the `doc_id → entry_id` mapping means
//! upserts and deletes after a cold open correctly remove stale R-tree entries
//! instead of accumulating duplicates.

use std::collections::HashMap;

use nodedb_types::Namespace;
use nodedb_types::error::{NodeDbError, NodeDbResult};

use crate::storage::engine::{StorageEngine, WriteOp};

/// Flush the full spatial state to storage.
///
/// Persists each R-tree checkpoint (CRC32C-wrapped) plus the `doc_id → entry_id`
/// mapping so that cold opens can restore exact index state.
pub(crate) async fn flush_spatial<S>(
    storage: &S,
    checkpoints: &[(String, String, Vec<u8>)],
    doc_to_entry: &HashMap<(String, String), u64>,
    next_id: u64,
) -> NodeDbResult<()>
where
    S: StorageEngine,
{
    let mut ops: Vec<WriteOp> = Vec::new();

    // ── Collection list ───────────────────────────────────────────────────────
    let index_keys: Vec<(String, String)> = checkpoints
        .iter()
        .map(|(c, f, _)| (c.clone(), f.clone()))
        .collect();
    let keys_bytes = zerompk::to_msgpack_vec(&index_keys)
        .map_err(|e| NodeDbError::serialization("msgpack", e))?;
    ops.push(WriteOp::Put {
        ns: Namespace::Spatial,
        key: b"spatial:_collections".to_vec(),
        value: keys_bytes,
    });

    // ── Next entry ID ─────────────────────────────────────────────────────────
    let next_id_bytes =
        zerompk::to_msgpack_vec(&next_id).map_err(|e| NodeDbError::serialization("msgpack", e))?;
    ops.push(WriteOp::Put {
        ns: Namespace::Spatial,
        key: b"spatial:_next_id".to_vec(),
        value: next_id_bytes,
    });

    // ── Per-index R-tree bytes and doc-map ────────────────────────────────────
    for (collection, field, rtree_bytes) in checkpoints {
        let rtree_key = format!("spatial:{collection}:{field}:rtree");
        ops.push(WriteOp::Put {
            ns: Namespace::Spatial,
            key: rtree_key.into_bytes(),
            value: crate::storage::checksum::wrap(rtree_bytes),
        });

        // Collect doc_to_entry pairs for this (collection, field).
        // The doc_to_entry map is keyed by (collection, doc_id); field comes
        // from the per-index loop, but the manager stores one map across all
        // fields. We persist the entries whose (collection, doc_id) pair
        // matches any doc indexed under this (collection, field).
        //
        // Because `doc_to_entry` uses (collection, doc_id) as key (not field),
        // we persist a flat list of (doc_id, entry_id) per (collection, field).
        // On restore, we reconstruct the map using the same scheme.
        let docmap_key = format!("spatial:{collection}:{field}:docmap");
        let pairs: Vec<(String, u64)> = doc_to_entry
            .iter()
            .filter(|((coll, _doc_id), _)| coll == collection)
            .map(|((_coll, doc_id), &entry_id)| (doc_id.clone(), entry_id))
            .collect();
        let docmap_bytes = zerompk::to_msgpack_vec(&pairs)
            .map_err(|e| NodeDbError::serialization("msgpack", e))?;
        ops.push(WriteOp::Put {
            ns: Namespace::Spatial,
            key: docmap_key.into_bytes(),
            value: docmap_bytes,
        });
    }

    storage
        .batch_write(&ops)
        .await
        .map_err(NodeDbError::storage)?;

    Ok(())
}

/// Restore spatial state from storage on cold open.
///
/// Returns `(checkpoints, doc_to_entry, next_id)`.
/// Returns an empty state if no checkpoint is found.
pub(crate) async fn restore_spatial<S>(
    storage: &S,
) -> NodeDbResult<(
    Vec<(String, String, Vec<u8>)>,
    HashMap<(String, String), u64>,
    u64,
)>
where
    S: StorageEngine,
{
    // ── Read collection list ──────────────────────────────────────────────────
    let Some(keys_bytes) = storage
        .get(Namespace::Spatial, b"spatial:_collections")
        .await?
    else {
        return Ok((Vec::new(), HashMap::new(), 1));
    };

    let Ok(index_keys) = zerompk::from_msgpack::<Vec<(String, String)>>(&keys_bytes) else {
        tracing::warn!("spatial checkpoint: failed to decode collection list — starting fresh");
        return Ok((Vec::new(), HashMap::new(), 1));
    };

    if index_keys.is_empty() {
        return Ok((Vec::new(), HashMap::new(), 1));
    }

    // ── Read next_id ──────────────────────────────────────────────────────────
    let next_id = if let Some(bytes) = storage.get(Namespace::Spatial, b"spatial:_next_id").await? {
        zerompk::from_msgpack::<u64>(&bytes).unwrap_or(1)
    } else {
        1
    };

    // ── Per-index R-tree bytes and doc-map ────────────────────────────────────
    let mut checkpoints: Vec<(String, String, Vec<u8>)> = Vec::new();
    let mut doc_to_entry: HashMap<(String, String), u64> = HashMap::new();

    for (collection, field) in &index_keys {
        let rtree_key = format!("spatial:{collection}:{field}:rtree");
        if let Ok(Some(envelope)) = storage.get(Namespace::Spatial, rtree_key.as_bytes()).await {
            match crate::storage::checksum::unwrap(&envelope) {
                Some(bytes) => {
                    checkpoints.push((collection.clone(), field.clone(), bytes));
                }
                None => {
                    tracing::error!(
                        collection = %collection,
                        field = %field,
                        "spatial R-tree CRC32C mismatch — discarding"
                    );
                    let _ = storage
                        .delete(Namespace::Spatial, rtree_key.as_bytes())
                        .await;
                    continue;
                }
            }
        } else {
            continue;
        }

        // ── Restore doc_id → entry_id mapping ────────────────────────────────
        let docmap_key = format!("spatial:{collection}:{field}:docmap");
        if let Ok(Some(docmap_bytes)) = storage.get(Namespace::Spatial, docmap_key.as_bytes()).await
            && let Ok(pairs) = zerompk::from_msgpack::<Vec<(String, u64)>>(&docmap_bytes)
        {
            for (doc_id, entry_id) in pairs {
                doc_to_entry.insert((collection.clone(), doc_id), entry_id);
            }
        }
    }

    tracing::debug!(
        index_count = checkpoints.len(),
        doc_entry_count = doc_to_entry.len(),
        next_id,
        "spatial checkpoint restored"
    );

    Ok((checkpoints, doc_to_entry, next_id))
}
