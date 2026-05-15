//! Segment read/write helpers for the Lite array engine.
//!
//! Segments are stored as raw byte blobs in the `Array` namespace under the
//! key `segment:{name}:{id}`. The bytes are the exact output of
//! `nodedb_array::SegmentWriter::finish()`, which includes the header,
//! tile frames, and footer — the reader can round-trip them without any
//! extra envelope.

use std::sync::Arc;

use nodedb_array::segment::extract_cell_bytes;
use nodedb_array::{SegmentReader, SegmentWriter, SparseTile, TilePayload};
use nodedb_types::Namespace;

use crate::engine::array::manifest::segment_key;
use crate::error::LiteError;
use crate::storage::engine::StorageEngineSync;

/// Flush a batch of `(TileId, SparseTile)` pairs into a new segment and
/// persist the bytes under `segment:{name}:{id}`.
///
/// Returns the serialized segment bytes so the caller can record the
/// `byte_len` in the manifest without a second storage read.
pub fn write_segment<S: StorageEngineSync>(
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

    let key = segment_key(name, seg_id);
    storage.put_sync(Namespace::Array, &key, &bytes)?;
    Ok(bytes)
}

/// Load segment bytes for `seg_id` and open a `SegmentReader` over them.
///
/// The returned `Vec<u8>` owns the bytes; the `SegmentReader` borrows from it.
/// The caller receives both so it can keep the bytes alive.
pub fn load_segment<S: StorageEngineSync>(
    storage: &Arc<S>,
    name: &str,
    seg_id: u64,
) -> Result<Vec<u8>, LiteError> {
    let key = segment_key(name, seg_id);
    storage
        .get_sync(Namespace::Array, &key)?
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

/// Delete the segment bytes for `seg_id` from storage.
pub fn delete_segment<S: StorageEngineSync>(
    storage: &Arc<S>,
    name: &str,
    seg_id: u64,
) -> Result<(), LiteError> {
    storage.delete_sync(Namespace::Array, &segment_key(name, seg_id))
}

/// Iterate all tile versions for `hilbert_prefix` at or before
/// `system_as_of` across all segments (oldest-first segment order, then
/// newest-first within each segment). Returns raw cell bytes for each
/// `(TileId, cell_bytes)` pair compatible with the ceiling resolver.
///
/// If a coord is not in the tile, `extract_cell_bytes` returns `None` and
/// that tile is silently skipped. The caller must aggregate across segments.
pub fn iter_cell_versions_across_segments<'a, S: StorageEngineSync>(
    storage: &Arc<S>,
    name: &str,
    seg_ids: impl Iterator<Item = u64> + 'a,
    hilbert_prefix: u64,
    system_as_of: i64,
    coord: &'a [nodedb_array::types::coord::value::CoordValue],
) -> impl Iterator<Item = Result<(nodedb_array::types::TileId, Vec<u8>), LiteError>> + 'a {
    let storage = Arc::clone(storage);
    let name = name.to_owned();
    seg_ids.flat_map(move |seg_id| {
        let bytes = match load_segment(&storage, &name, seg_id) {
            Ok(b) => b,
            Err(e) => return vec![Err(e)].into_iter(),
        };
        // Reader over owned bytes — we collect into a Vec before returning so
        // the bytes lifetime stays in scope.
        let reader = match SegmentReader::open(&bytes) {
            Ok(r) => r,
            Err(e) => {
                return vec![Err(LiteError::Storage {
                    detail: format!("open reader seg {seg_id}: {e}"),
                })]
                .into_iter();
            }
        };
        let versions: Vec<_> = match reader.iter_tile_versions(hilbert_prefix, system_as_of) {
            Ok(it) => it
                .filter_map(|res| {
                    let (tile_id, payload) = match res {
                        Ok(v) => v,
                        Err(e) => {
                            return Some(Err(LiteError::Storage {
                                detail: format!("iter_tile_versions: {e}"),
                            }));
                        }
                    };
                    match &payload {
                        TilePayload::Sparse(tile) => match extract_cell_bytes(tile, coord) {
                            Ok(Some(cell_bytes)) => Some(Ok((tile_id, cell_bytes))),
                            Ok(None) => None,
                            Err(e) => Some(Err(LiteError::Storage {
                                detail: format!("extract_cell_bytes: {e}"),
                            })),
                        },
                        TilePayload::Dense(_) => None,
                    }
                })
                .collect(),
            Err(e) => {
                return vec![Err(LiteError::Storage {
                    detail: format!("iter_tile_versions seg {seg_id}: {e}"),
                })]
                .into_iter();
            }
        };
        versions.into_iter()
    })
}

/// Collect all live cells from all segments for the given `hilbert_prefix`
/// at or before `system_as_of` — used by `array_slice`.
pub fn collect_tile_versions_across_segments<S: StorageEngineSync>(
    storage: &Arc<S>,
    name: &str,
    seg_ids: &[u64],
    hilbert_prefix: u64,
    system_as_of: i64,
) -> Result<Vec<(nodedb_array::types::TileId, TilePayload)>, LiteError> {
    let mut out = Vec::new();
    for &seg_id in seg_ids {
        let bytes = load_segment(storage, name, seg_id)?;
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
pub fn rewrite_segment<S: StorageEngineSync>(
    storage: &Arc<S>,
    name: &str,
    new_seg_id: u64,
    schema_hash: u64,
    tiles: &[(nodedb_array::types::TileId, SparseTile)],
    old_seg_ids: &[u64],
) -> Result<Vec<u8>, LiteError> {
    let refs: Vec<_> = tiles.iter().map(|(id, tile)| (*id, tile)).collect();
    let bytes = write_segment(storage, name, new_seg_id, schema_hash, &refs)?;
    for &old_id in old_seg_ids {
        delete_segment(storage, name, old_id)?;
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
    use crate::storage::redb_storage::RedbStorage;
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

    #[test]
    fn write_and_load_segment() {
        let s = schema();
        let storage = Arc::new(RedbStorage::open_in_memory().unwrap());
        let tile = make_tile(&s);
        let tile_id = TileId::snapshot(0);
        write_segment(&storage, "g", 0, 0xABCD, &[(tile_id, &tile)]).unwrap();

        let bytes = load_segment(&storage, "g", 0).unwrap();
        let reader = open_reader(&bytes).unwrap();
        assert_eq!(reader.tile_count(), 1);
    }

    #[test]
    fn delete_segment_removes_key() {
        let s = schema();
        let storage = Arc::new(RedbStorage::open_in_memory().unwrap());
        let tile = make_tile(&s);
        write_segment(&storage, "g", 0, 0, &[(TileId::snapshot(0), &tile)]).unwrap();
        delete_segment(&storage, "g", 0).unwrap();
        assert!(load_segment(&storage, "g", 0).is_err());
    }
}
