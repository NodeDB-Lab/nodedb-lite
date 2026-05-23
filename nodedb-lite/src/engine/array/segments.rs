//! Segment read/write helpers for the Lite array engine.
//!
//! On pagedb-backed storage (`as_array_segment_ext()` returns `Some`), tile
//! data is stored in pagedb encrypted segments under `arr/tile/{name}/{id}`.
//! On WASM (where `as_array_segment_ext()` returns `None`), the legacy KV blob path is used:
//! bytes stored in the `Array` namespace under `segment:{name}:{id}`.
//!
//! The on-disk bytes are identical in both paths — the exact output of
//! `nodedb_array::SegmentWriter::finish()`, which includes the header, tile
//! frames, and footer.  `SegmentReader::open` can parse them directly.

use std::sync::Arc;

use nodedb_array::segment::extract_cell_bytes;
use nodedb_array::{SegmentReader, SegmentWriter, SparseTile, TilePayload};
use nodedb_types::Namespace;

use crate::engine::array::manifest::segment_key;
use crate::error::LiteError;
use crate::storage::engine::StorageEngine;

/// Flush a batch of `(TileId, SparseTile)` pairs into a new segment and
/// persist the bytes.
///
/// When `as_array_segment_ext()` is available (pagedb backend), the bytes are
/// stored as an encrypted pagedb segment.  Otherwise they are stored as a KV
/// blob in the `Array` namespace.
///
/// Returns the serialized segment bytes so the caller can record the
/// `byte_len` in the manifest without a second storage read.
pub async fn write_segment<S: StorageEngine>(
    storage: &Arc<S>,
    name: &str,
    seg_id: u64,
    schema_hash: u64,
    tiles: &[(nodedb_array::types::TileId, &SparseTile)],
) -> Result<Vec<u8>, LiteError> {
    let mut writer = SegmentWriter::new(schema_hash);
    for (tile_id, tile) in tiles {
        writer
            .append_sparse(*tile_id, tile)
            .map_err(|e| LiteError::Storage {
                detail: format!("append_sparse: {e}"),
            })?;
    }
    let bytes = writer.finish(None).map_err(|e| LiteError::Storage {
        detail: format!("segment finish: {e}"),
    })?;

    #[cfg(not(target_arch = "wasm32"))]
    if let Some(ext) = storage.as_array_segment_ext() {
        ext.write_array_segment(name, seg_id, &bytes).await?;
        return Ok(bytes);
    }

    let key = segment_key(name, seg_id);
    storage.put(Namespace::Array, &key, &bytes).await?;
    Ok(bytes)
}

/// Load segment bytes for `seg_id`.
///
/// When `as_array_segment_ext()` is available (pagedb backend), the bytes are
/// read from the encrypted pagedb segment.  Otherwise they are read from the
/// KV blob in the `Array` namespace.
pub async fn load_segment<S: StorageEngine>(
    storage: &Arc<S>,
    name: &str,
    seg_id: u64,
) -> Result<Vec<u8>, LiteError> {
    // On pagedb-backed storage, attempt to read from the encrypted segment.
    // Fall through to the KV path if the segment is not found there — this
    // handles data written via the legacy KV path (e.g. pre-migration data
    // or tests that write directly to KV).
    #[cfg(not(target_arch = "wasm32"))]
    if let Some(ext) = storage.as_array_segment_ext() {
        if let Some(bytes) = ext.open_array_segment(name, seg_id).await? {
            return Ok(bytes.into_vec());
        }
        // Not in pagedb — fall through to KV lookup below.
    }

    let key = segment_key(name, seg_id);
    storage
        .get(Namespace::Array, &key)
        .await?
        .ok_or_else(|| LiteError::Storage {
            detail: format!("segment {name}/{seg_id} not found"),
        })
}

/// Open a `SegmentReader` over the given byte slice.
pub fn open_reader(bytes: &[u8]) -> Result<SegmentReader<'_>, LiteError> {
    SegmentReader::open(bytes).map_err(|e| LiteError::Storage {
        detail: format!("SegmentReader::open: {e}"),
    })
}

/// Delete the segment for `seg_id` from storage.
///
/// On pagedb backend, the segment is tombstoned and reaped by the next GC
/// cycle.  On KV backends, the blob is deleted immediately.
pub async fn delete_segment<S: StorageEngine>(
    storage: &Arc<S>,
    name: &str,
    seg_id: u64,
) -> Result<(), LiteError> {
    // On pagedb-backed storage, tombstone the encrypted segment and also
    // remove any legacy KV entry for the same segment (dual cleanup ensures
    // no stale KV blobs after migration).
    #[cfg(not(target_arch = "wasm32"))]
    if let Some(ext) = storage.as_array_segment_ext() {
        ext.delete_array_segment(name, seg_id).await?;
        // Best-effort cleanup of any legacy KV entry.
        let _ = storage
            .delete(Namespace::Array, &segment_key(name, seg_id))
            .await;
        return Ok(());
    }

    storage
        .delete(Namespace::Array, &segment_key(name, seg_id))
        .await
}

/// Collect all tile versions for `hilbert_prefix` at or before `system_as_of`
/// across all segments, filtering to cells at `coord`. Returns raw cell bytes
/// for each `(TileId, cell_bytes)` pair compatible with the ceiling resolver.
///
/// If a coord is not in the tile, `extract_cell_bytes` returns `None` and
/// that tile is silently skipped. The caller must aggregate across segments.
pub async fn iter_cell_versions_across_segments<S: StorageEngine>(
    storage: &Arc<S>,
    name: &str,
    seg_ids: impl Iterator<Item = u64>,
    hilbert_prefix: u64,
    system_as_of: i64,
    coord: &[nodedb_array::types::coord::value::CoordValue],
) -> Result<Vec<(nodedb_array::types::TileId, Vec<u8>)>, LiteError> {
    let mut out = Vec::new();
    for seg_id in seg_ids {
        let bytes = load_segment(storage, name, seg_id).await?;
        let reader = SegmentReader::open(&bytes).map_err(|e| LiteError::Storage {
            detail: format!("open reader seg {seg_id}: {e}"),
        })?;
        let versions = reader
            .iter_tile_versions(hilbert_prefix, system_as_of)
            .map_err(|e| LiteError::Storage {
                detail: format!("iter_tile_versions seg {seg_id}: {e}"),
            })?;
        for res in versions {
            let (tile_id, payload) = res.map_err(|e| LiteError::Storage {
                detail: format!("iter_tile_versions: {e}"),
            })?;
            if let TilePayload::Sparse(tile) = &payload {
                match extract_cell_bytes(tile, coord) {
                    Ok(Some(cell_bytes)) => out.push((tile_id, cell_bytes)),
                    Ok(None) => {}
                    Err(e) => {
                        return Err(LiteError::Storage {
                            detail: format!("extract_cell_bytes: {e}"),
                        });
                    }
                }
            }
        }
    }
    Ok(out)
}

/// Collect all live cells from all segments for the given `hilbert_prefix`
/// at or before `system_as_of` — used by `array_slice`.
pub async fn collect_tile_versions_across_segments<S: StorageEngine>(
    storage: &Arc<S>,
    name: &str,
    seg_ids: &[u64],
    hilbert_prefix: u64,
    system_as_of: i64,
) -> Result<Vec<(nodedb_array::types::TileId, TilePayload)>, LiteError> {
    let mut out = Vec::new();
    for &seg_id in seg_ids {
        let bytes = load_segment(storage, name, seg_id).await?;
        let reader = SegmentReader::open(&bytes).map_err(|e| LiteError::Storage {
            detail: format!("open reader seg {seg_id}: {e}"),
        })?;
        let versions = reader
            .iter_tile_versions(hilbert_prefix, system_as_of)
            .map_err(|e| LiteError::Storage {
                detail: format!("iter_tile_versions seg {seg_id}: {e}"),
            })?;
        for res in versions {
            let (tile_id, payload) = res.map_err(|e| LiteError::Storage {
                detail: format!("decode tile seg {seg_id}: {e}"),
            })?;
            out.push((tile_id, payload));
        }
    }
    Ok(out)
}

/// Compile a new merged segment from the provided `(TileId, SparseTile)` list
/// and persist it, then delete the old segments.
///
/// Used by retention compaction. Returns the new segment ID and byte length.
pub async fn rewrite_segment<S: StorageEngine>(
    storage: &Arc<S>,
    name: &str,
    new_seg_id: u64,
    schema_hash: u64,
    tiles: &[(nodedb_array::types::TileId, SparseTile)],
    old_seg_ids: &[u64],
) -> Result<Vec<u8>, LiteError> {
    let refs: Vec<_> = tiles.iter().map(|(id, tile)| (*id, tile)).collect();
    let bytes = write_segment(storage, name, new_seg_id, schema_hash, &refs).await?;
    for &old_id in old_seg_ids {
        delete_segment(storage, name, old_id).await?;
    }
    Ok(bytes)
}

/// Read all decoded cells matching `hilbert_prefix` at or before `system_as_of`
/// from a single segment's bytes (already loaded). Returns `(TileId, TilePayload)`.
pub fn tile_versions_from_bytes(
    bytes: &[u8],
    hilbert_prefix: u64,
    system_as_of: i64,
) -> Result<Vec<(nodedb_array::types::TileId, TilePayload)>, LiteError> {
    let reader = SegmentReader::open(bytes).map_err(|e| LiteError::Storage {
        detail: format!("SegmentReader::open: {e}"),
    })?;
    let versions = reader
        .iter_tile_versions(hilbert_prefix, system_as_of)
        .map_err(|e| LiteError::Storage {
            detail: format!("iter_tile_versions: {e}"),
        })?;
    versions
        .map(|r| {
            r.map_err(|e| LiteError::Storage {
                detail: format!("tile decode: {e}"),
            })
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::pagedb_storage::PagedbStorageMem;
    use nodedb_array::schema::ArraySchemaBuilder;
    use nodedb_array::schema::attr_spec::{AttrSpec, AttrType};
    use nodedb_array::schema::dim_spec::{DimSpec, DimType};
    use nodedb_array::tile::sparse_tile::SparseTileBuilder;
    use nodedb_array::types::TileId;
    use nodedb_array::types::cell_value::value::CellValue;
    use nodedb_array::types::coord::value::CoordValue;
    use nodedb_array::types::domain::{Domain, DomainBound};
    use std::sync::Arc;

    fn schema() -> nodedb_array::schema::ArraySchema {
        ArraySchemaBuilder::new("g")
            .dim(DimSpec::new(
                "x",
                DimType::Int64,
                Domain::new(DomainBound::Int64(0), DomainBound::Int64(15)),
            ))
            .attr(AttrSpec::new("v", AttrType::Int64, true))
            .tile_extents(vec![4])
            .build()
            .unwrap()
    }

    fn make_tile(s: &nodedb_array::schema::ArraySchema) -> SparseTile {
        let mut b = SparseTileBuilder::new(s);
        b.push(&[CoordValue::Int64(1)], &[CellValue::Int64(42)])
            .unwrap();
        b.build()
    }

    #[tokio::test]
    async fn write_and_load_segment() {
        let s = schema();
        let storage = Arc::new(PagedbStorageMem::open_in_memory().await.unwrap());
        let tile = make_tile(&s);
        let tile_id = TileId::snapshot(0);
        write_segment(&storage, "g", 0, 0xABCD, &[(tile_id, &tile)])
            .await
            .unwrap();

        let bytes = load_segment(&storage, "g", 0).await.unwrap();
        let reader = open_reader(&bytes).unwrap();
        assert_eq!(reader.tile_count(), 1);
    }

    #[tokio::test]
    async fn delete_segment_removes_key() {
        let s = schema();
        let storage = Arc::new(PagedbStorageMem::open_in_memory().await.unwrap());
        let tile = make_tile(&s);
        write_segment(&storage, "g", 0, 0, &[(TileId::snapshot(0), &tile)])
            .await
            .unwrap();
        delete_segment(&storage, "g", 0).await.unwrap();
        assert!(load_segment(&storage, "g", 0).await.is_err());
    }

    /// Verify that `write_segment` dispatches through the pagedb segment path
    /// when the storage backend supports `as_array_segment_ext()`.
    ///
    /// The segment bytes must be absent from the KV namespace (proving the
    /// pagedb path was taken), and must be readable back via `load_segment`.
    #[cfg(not(target_arch = "wasm32"))]
    #[tokio::test]
    async fn pagedb_path_writes_to_segment_not_kv() {
        use nodedb_types::Namespace;

        let s = schema();
        let storage = Arc::new(PagedbStorageMem::open_in_memory().await.unwrap());
        let tile = make_tile(&s);
        let tile_id = TileId::snapshot(0);

        write_segment(&storage, "arr_test", 7, 0xCAFE, &[(tile_id, &tile)])
            .await
            .unwrap();

        // The KV namespace must be empty — no blob was written there.
        let kv_key = crate::engine::array::manifest::segment_key("arr_test", 7);
        let kv_val = storage.get(Namespace::Array, &kv_key).await.unwrap();
        assert!(
            kv_val.is_none(),
            "expected no KV entry on pagedb-backed storage"
        );

        // But load_segment must succeed via the pagedb path.
        let bytes = load_segment(&storage, "arr_test", 7).await.unwrap();
        let reader = open_reader(&bytes).unwrap();
        assert_eq!(reader.tile_count(), 1);
    }

    /// Format bit-identity: bytes written by `write_segment` must parse via
    /// `nodedb_array::SegmentReader::open` — same as the Origin-side reader.
    #[cfg(not(target_arch = "wasm32"))]
    #[tokio::test]
    async fn format_bit_identity_with_origin_reader() {
        let s = schema();
        let storage = Arc::new(PagedbStorageMem::open_in_memory().await.unwrap());
        let tile = make_tile(&s);
        let tile_id = TileId::new(1, 100);

        let written_bytes =
            write_segment(&storage, "identity_test", 0, 0xDEAD, &[(tile_id, &tile)])
                .await
                .unwrap();

        // The bytes returned by write_segment must be parseable by SegmentReader
        // (the same path used by Origin's SegmentHandle).
        let reader = nodedb_array::SegmentReader::open(&written_bytes).unwrap();
        assert_eq!(reader.tile_count(), 1);
        assert_eq!(reader.tiles()[0].tile_id, tile_id);
        assert_eq!(reader.schema_hash(), 0xDEAD);

        // The bytes retrieved via load_segment must also be parseable.
        let loaded = load_segment(&storage, "identity_test", 0).await.unwrap();
        assert_eq!(loaded, written_bytes, "loaded bytes must be bit-identical");
    }
}
