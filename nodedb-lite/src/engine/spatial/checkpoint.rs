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
//! | `spatial:{collection}:{field}:docmap`  | MessagePack `Vec<(String, u64)>` — doc_id → entry_id |
//! | `spatial:_next_id`                     | MessagePack `u64` — next entry ID                  |
//!
//! The R-tree blob (`spatial:{collection}:{field}:rtree`) is stored in a pagedb
//! segment when `as_spatial_segment_ext()` is available, or falls back to the
//! `Namespace::Spatial` KV path (e.g. WASM). In both cases the
//! bytes stored are the CRC32C-wrapped R-tree checkpoint produced by
//! `crate::storage::checksum::wrap`.

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

    // ── Per-index doc-map (always on B+ tree) ─────────────────────────────────
    for (collection, field, _rtree_bytes) in checkpoints {
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

    // ── Commit catalog + docmap to B+ tree ────────────────────────────────────
    storage
        .batch_write(&ops)
        .await
        .map_err(NodeDbError::storage)?;

    // ── Per-index R-tree bytes: segment when available, KV fallback ───────────
    #[cfg(not(target_arch = "wasm32"))]
    if let Some(seg) = storage.as_spatial_segment_ext() {
        for (collection, field, rtree_bytes) in checkpoints {
            let wrapped = crate::storage::checksum::wrap(rtree_bytes);
            seg.write_spatial_segment(collection, field, &wrapped)
                .await
                .map_err(NodeDbError::storage)?;
        }
        return Ok(());
    }

    // Legacy KV path (WASM fallback).
    let mut rtree_ops: Vec<WriteOp> = Vec::with_capacity(checkpoints.len());
    for (collection, field, rtree_bytes) in checkpoints {
        let rtree_key = format!("spatial:{collection}:{field}:rtree");
        rtree_ops.push(WriteOp::Put {
            ns: Namespace::Spatial,
            key: rtree_key.into_bytes(),
            value: crate::storage::checksum::wrap(rtree_bytes),
        });
    }
    if !rtree_ops.is_empty() {
        storage
            .batch_write(&rtree_ops)
            .await
            .map_err(NodeDbError::storage)?;
    }

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
        // Try pagedb segment first (non-WASM), then fall back to KV blob.
        let rtree_envelope: Option<Vec<u8>> = {
            #[cfg(not(target_arch = "wasm32"))]
            {
                if let Some(seg) = storage.as_spatial_segment_ext() {
                    match seg.open_spatial_segment(collection, field).await {
                        Ok(Some(boxed)) => Some(boxed.into_vec()),
                        Ok(None) => {
                            // Segment absent — fall through to KV blob.
                            None
                        }
                        Err(e) => {
                            tracing::warn!(
                                collection = %collection,
                                field = %field,
                                error = %e,
                                "spatial segment open failed — falling back to KV blob"
                            );
                            None
                        }
                    }
                } else {
                    None
                }
            }
            #[cfg(target_arch = "wasm32")]
            {
                None
            }
        };

        // If segment was not found (absent or not supported), try KV blob.
        let envelope = if let Some(env) = rtree_envelope {
            env
        } else {
            let rtree_key = format!("spatial:{collection}:{field}:rtree");
            match storage.get(Namespace::Spatial, rtree_key.as_bytes()).await {
                Ok(Some(env)) => env,
                Ok(None) => continue,
                Err(_) => continue,
            }
        };

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
                // Best-effort cleanup of the stale KV blob (segment path has
                // no stale entry to clean in this branch).
                let rtree_key = format!("spatial:{collection}:{field}:rtree");
                let _ = storage
                    .delete(Namespace::Spatial, rtree_key.as_bytes())
                    .await;
                continue;
            }
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
