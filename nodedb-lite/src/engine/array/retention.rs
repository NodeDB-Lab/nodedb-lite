//! Synchronous retention compaction for the Lite array engine.
//!
//! Delegates to `nodedb_array::query::retention::merge_for_retention` for the
//! per-`hilbert_prefix` merge logic. Because Lite has no background TPC, this
//! runs inline at the end of `array_put_cell` when `audit_retain_ms` is set.

use std::collections::HashMap;
use std::sync::Arc;

use nodedb_array::query::retention::merge_for_retention;
use nodedb_array::schema::ArraySchema;
use nodedb_array::segment::TileEntry;
use nodedb_array::{SegmentReader, SparseTile, TilePayload};

use crate::engine::array::manifest::{ArrayManifest, SegmentRef, save_manifest};
use crate::engine::array::segments::{delete_segment, write_segment};
use crate::error::LiteError;
use crate::storage::engine::StorageEngineSync;

/// Run idle retention compaction for one array.
///
/// For each segment, groups tile entries by `hilbert_prefix`, calls
/// `merge_for_retention`, and rewrites the segment when any tile-versions
/// were merged or dropped. The manifest is updated atomically after all
/// rewrites succeed.
///
/// Returns the number of segments rewritten.
pub fn run_retention<S: StorageEngineSync>(
    storage: &Arc<S>,
    name: &str,
    manifest: &mut ArrayManifest,
    schema: &ArraySchema,
    schema_hash: u64,
    audit_retain_ms: i64,
    now_ms: i64,
) -> Result<usize, LiteError> {
    let horizon_ms = now_ms.saturating_sub(audit_retain_ms);
    let mut rewritten = 0;

    let mut new_segments: Vec<SegmentRef> = Vec::new();

    let old_segs: Vec<SegmentRef> = manifest.segments.clone();

    for seg_ref in &old_segs {
        let bytes = crate::engine::array::segments::load_segment(storage, name, seg_ref.id)?;
        let reader = SegmentReader::open(&bytes).map_err(|e| LiteError::Storage {
            detail: format!("open reader seg {}: {e}", seg_ref.id),
        })?;

        let tiles = reader.tiles();
        if tiles.is_empty() {
            new_segments.push(seg_ref.clone());
            continue;
        }

        // Check whether any tile is outside the horizon.
        let needs_merge = tiles.iter().any(|e| e.tile_id.system_from_ms < horizon_ms);

        if !needs_merge {
            new_segments.push(seg_ref.clone());
            continue;
        }

        // Group tile entries by hilbert_prefix.
        let mut by_prefix: HashMap<u64, Vec<TileEntry>> = HashMap::new();
        for entry in tiles {
            by_prefix
                .entry(entry.tile_id.hilbert_prefix)
                .or_default()
                .push(entry.clone());
        }

        // Merge per-prefix and collect surviving tiles.
        let mut keep_tiles: Vec<(nodedb_array::types::TileId, SparseTile)> = Vec::new();

        for (&prefix, entries) in &by_prefix {
            let result =
                merge_for_retention(entries, &reader, schema, horizon_ms).map_err(|e| {
                    LiteError::Storage {
                        detail: format!("merge_for_retention: {e}"),
                    }
                })?;

            // Ceiling tile: synthesised live state AS OF horizon for out-of-horizon cells.
            if let Some(ceiling) = result.ceiling_tile {
                // Place the ceiling at `horizon_ms` as a new tile-version.
                let ceil_id = nodedb_array::types::TileId::new(prefix, horizon_ms);
                keep_tiles.push((ceil_id, ceiling));
            }

            // Keep in-horizon tile versions unchanged.
            for tile_id in &result.keep_inhorizon {
                let payload = reader
                    .read_tile_as_of(tile_id.hilbert_prefix, tile_id.system_from_ms, None)
                    .map_err(|e| LiteError::Storage {
                        detail: format!("read_tile_as_of: {e}"),
                    })?;
                if let Some(TilePayload::Sparse(tile)) = payload {
                    keep_tiles.push((*tile_id, tile));
                }
            }
        }

        // Sort tiles by TileId (required by SegmentWriter: strictly ascending).
        keep_tiles.sort_by_key(|(id, _)| *id);
        keep_tiles.dedup_by_key(|(id, _)| *id);

        // Delete old segment.
        delete_segment(storage, name, seg_ref.id)?;

        if keep_tiles.is_empty() {
            // Nothing survived — segment fully purged.
            rewritten += 1;
            continue;
        }

        let new_id = manifest.next_id;
        manifest.next_id += 1;

        let refs: Vec<_> = keep_tiles.iter().map(|(id, tile)| (*id, tile)).collect();
        let new_bytes = write_segment(storage, name, new_id, schema_hash, &refs)?;
        new_segments.push(SegmentRef {
            id: new_id,
            byte_len: new_bytes.len() as u64,
        });
        rewritten += 1;
    }

    manifest.segments = new_segments;
    save_manifest(storage, name, manifest)?;

    Ok(rewritten)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::engine::array::manifest::{ArrayManifest, save_manifest};
    use crate::engine::array::segments::write_segment;
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

    fn schema() -> ArraySchema {
        ArraySchemaBuilder::new("r")
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

    fn make_tile(s: &ArraySchema, x: i64, v: i64) -> SparseTile {
        let mut b = SparseTileBuilder::new(s);
        b.push(&[CoordValue::Int64(x)], &[CellValue::Int64(v)])
            .unwrap();
        b.build()
    }

    #[test]
    fn empty_segment_passes_through() {
        let storage = Arc::new(RedbStorage::open_in_memory().unwrap());
        let s = schema();
        let hash = crate::engine::array::catalog::hash_schema(&s).unwrap();
        let mut manifest = ArrayManifest::new();

        // Empty segment.
        let writer = nodedb_array::SegmentWriter::new(hash);
        let seg_bytes = writer.finish().unwrap();
        let seg_id = manifest.push_segment(seg_bytes.len() as u64);
        storage
            .put_sync(
                nodedb_types::Namespace::Array,
                &crate::engine::array::manifest::segment_key("r", seg_id),
                &seg_bytes,
            )
            .unwrap();
        save_manifest(&storage, "r", &manifest).unwrap();

        let n = run_retention(&storage, "r", &mut manifest, &s, hash, 60_000, 100_000).unwrap();
        assert_eq!(n, 0);
        assert_eq!(manifest.segments.len(), 1);
    }

    #[test]
    fn in_horizon_segment_not_rewritten() {
        let storage = Arc::new(RedbStorage::open_in_memory().unwrap());
        let s = schema();
        let hash = crate::engine::array::catalog::hash_schema(&s).unwrap();
        let mut manifest = ArrayManifest::new();

        // Tile with system_from_ms = 90_000 (inside horizon: now=100_000, retain=60_000 → horizon=40_000).
        let tile = make_tile(&s, 1, 10);
        let seg_id = manifest.push_segment(0);
        write_segment(
            &storage,
            "r",
            seg_id,
            hash,
            &[(TileId::new(0, 90_000), &tile)],
        )
        .unwrap();
        save_manifest(&storage, "r", &manifest).unwrap();

        let n = run_retention(&storage, "r", &mut manifest, &s, hash, 60_000, 100_000).unwrap();
        assert_eq!(n, 0);
    }

    #[test]
    fn out_of_horizon_segment_gets_rewritten() {
        let storage = Arc::new(RedbStorage::open_in_memory().unwrap());
        let s = schema();
        let hash = crate::engine::array::catalog::hash_schema(&s).unwrap();
        let mut manifest = ArrayManifest::new();

        // Two tiles for same coord: out-of-horizon (sys=10) and in-horizon (sys=90_000).
        // horizon = 100_000 - 60_000 = 40_000. sys=10 < 40_000 → outside.
        let tile1 = make_tile(&s, 1, 10);
        let tile2 = make_tile(&s, 1, 20);
        let seg_id = manifest.push_segment(0);
        write_segment(
            &storage,
            "r",
            seg_id,
            hash,
            &[
                (TileId::new(0, 10), &tile1),
                (TileId::new(0, 90_000), &tile2),
            ],
        )
        .unwrap();
        save_manifest(&storage, "r", &manifest).unwrap();

        let n = run_retention(&storage, "r", &mut manifest, &s, hash, 60_000, 100_000).unwrap();
        assert_eq!(n, 1);
    }
}
