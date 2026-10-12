//! Read paths of the Lite array engine: point reads, slices, and the
//! surrogate bitmap scan. Each read merges memtable and segment versions and
//! resolves them through the bitemporal ceiling.

use std::collections::HashMap;
use std::sync::Arc;

use nodedb_array::query::ceiling::{CeilingParams, CeilingResult, ceiling_resolve_cell};
use nodedb_array::query::slice::{DimRange, Slice};
use nodedb_array::schema::ArraySchema;
use nodedb_array::segment::extract_cell_bytes;
use nodedb_array::tile::cell_payload::CellPayload;
use nodedb_array::tile::sparse_tile::SparseTile;
use nodedb_array::types::TileId;
use nodedb_array::types::coord::value::CoordValue;
use nodedb_array::{TilePayload, tile_id_for_cell};

use crate::engine::array::engine::ArrayEngineState;
use crate::engine::array::memtable::ArrayMemtable;
use crate::error::LiteError;
use crate::storage::engine::StorageEngine;

impl ArrayEngineState {
    /// Read the most recent live payload for `coord` at or before `system_as_of`.
    /// Returns `None` for NotFound, Tombstoned, or Erased results.
    pub async fn read_coord<S: StorageEngine>(
        &self,
        storage: &Arc<S>,
        name: &str,
        coord: &[CoordValue],
        system_as_of: i64,
    ) -> Result<Option<CellPayload>, LiteError> {
        let state = self.arrays.get(name).ok_or_else(|| LiteError::BadRequest {
            detail: format!("array '{name}' not found"),
        })?;

        let schema = &state.schema;
        let seg_ids: Vec<u64> = state.manifest.segments.iter().map(|s| s.id).collect();

        let tile_id =
            tile_id_for_cell(schema, coord, system_as_of).map_err(|e| LiteError::Storage {
                detail: format!("tile_id_for_cell: {e}"),
            })?;
        let hilbert_prefix = tile_id.hilbert_prefix;

        let mem_versions = state
            .memtable
            .iter_cell_versions(hilbert_prefix, system_as_of, coord);

        let seg_versions = cell_versions_from_segments(
            storage,
            name,
            &seg_ids,
            hilbert_prefix,
            system_as_of,
            coord,
        )
        .await?;

        let mut all_versions: Vec<(TileId, Vec<u8>)> =
            mem_versions.into_iter().chain(seg_versions).collect();
        // ceiling_resolve_cell requires newest-first ordering.
        all_versions.sort_unstable_by_key(|v| std::cmp::Reverse(v.0.system_from_ms));

        let params = CeilingParams {
            system_as_of,
            valid_at_ms: None,
        };
        let result = ceiling_resolve_cell(
            all_versions.iter().map(|(id, b)| (*id, b.as_slice())),
            coord,
            &params,
        )
        .map_err(|e| LiteError::Storage {
            detail: format!("ceiling_resolve_cell: {e}"),
        })?;

        match result {
            CeilingResult::Live(payload) => Ok(Some(payload)),
            CeilingResult::Tombstoned | CeilingResult::Erased | CeilingResult::NotFound => Ok(None),
        }
    }

    /// Return all live cells whose coordinates fall within `ranges` at or
    /// before `system_as_of`.
    pub async fn slice<S: StorageEngine>(
        &mut self,
        storage: &Arc<S>,
        name: &str,
        ranges: Vec<Option<DimRange>>,
        system_as_of: i64,
    ) -> Result<Vec<CellPayload>, LiteError> {
        let state = self
            .arrays
            .get_mut(name)
            .ok_or_else(|| LiteError::BadRequest {
                detail: format!("array '{name}' not found"),
            })?;

        let schema = state.schema.clone();
        let seg_ids: Vec<u64> = state.manifest.segments.iter().map(|s| s.id).collect();
        let slice_pred = Slice::new(ranges);

        // coord_key → Vec<(TileId, cell_bytes)>.
        let mut coord_versions: HashMap<Vec<u8>, Vec<(TileId, Vec<u8>)>> = HashMap::new();

        // Segments (oldest-first iteration, index from end to get newest-first).
        for &seg_id in &seg_ids {
            let bytes = crate::engine::array::segments::load_segment(storage, name, seg_id).await?;
            let reader = crate::engine::array::segments::open_reader(&bytes)?;
            for idx in (0..reader.tile_count()).rev() {
                let entry_tile_id = reader.tiles()[idx].tile_id;
                if entry_tile_id.system_from_ms > system_as_of {
                    continue;
                }
                let payload = reader.read_tile(idx).map_err(|e| LiteError::Storage {
                    detail: format!("read_tile: {e}"),
                })?;
                if let TilePayload::Sparse(tile) = &payload {
                    collect_tile_into_map(tile, entry_tile_id, &mut coord_versions)?;
                }
            }
        }

        // Memtable.
        let mem_tiles = state
            .memtable
            .drain_all_tiles_read_only(system_as_of, &schema)?;
        for (tile_id, tile) in &mem_tiles {
            collect_tile_into_map(tile, *tile_id, &mut coord_versions)?;
        }

        // Ceiling + slice filter.
        let mut results = Vec::new();
        for (coord_key, mut versions) in coord_versions {
            versions.sort_by_key(|v| std::cmp::Reverse(v.0.system_from_ms));

            let coord: Vec<CoordValue> =
                zerompk::from_msgpack(&coord_key).map_err(|e| LiteError::Serialization {
                    detail: format!("decode coord: {e}"),
                })?;

            let params = CeilingParams {
                system_as_of,
                valid_at_ms: None,
            };
            let result = ceiling_resolve_cell(
                versions.iter().map(|(id, b)| (*id, b.as_slice())),
                &coord,
                &params,
            )
            .map_err(|e| LiteError::Storage {
                detail: format!("ceiling_resolve_cell: {e}"),
            })?;

            if let CeilingResult::Live(payload) = result
                && cell_in_slice(&coord, &slice_pred)
            {
                results.push(payload);
            }
        }

        Ok(results)
    }

    /// Return the set of surrogates for all live cells whose coordinates
    /// fall within `ranges` at or before `system_as_of`.
    ///
    /// This is the cross-engine prefilter primitive: the returned bitmap
    /// gates the HNSW candidate set in the vector search path so only
    /// vector records whose array-cell counterpart matches the slice
    /// predicate are considered.
    pub async fn surrogate_bitmap_scan<S: StorageEngine>(
        &mut self,
        storage: &Arc<S>,
        name: &str,
        ranges: Vec<Option<DimRange>>,
        system_as_of: i64,
    ) -> Result<roaring::RoaringBitmap, LiteError> {
        let cells = self.slice(storage, name, ranges, system_as_of).await?;
        let mut bitmap = roaring::RoaringBitmap::new();
        for payload in cells {
            bitmap.insert(payload.surrogate.as_u32());
        }
        Ok(bitmap)
    }
}

async fn cell_versions_from_segments<S: StorageEngine>(
    storage: &Arc<S>,
    name: &str,
    seg_ids: &[u64],
    hilbert_prefix: u64,
    system_as_of: i64,
    coord: &[CoordValue],
) -> Result<Vec<(TileId, Vec<u8>)>, LiteError> {
    let tile_payloads = crate::engine::array::segments::collect_tile_versions_across_segments(
        storage,
        name,
        seg_ids,
        hilbert_prefix,
        system_as_of,
    )
    .await?;
    let mut out = Vec::new();
    for (tile_id, payload) in tile_payloads {
        if let TilePayload::Sparse(tile) = &payload
            && let Ok(Some(cell_bytes)) = extract_cell_bytes(tile, coord)
        {
            out.push((tile_id, cell_bytes));
        }
    }
    Ok(out)
}

fn collect_tile_into_map(
    tile: &SparseTile,
    tile_id: TileId,
    acc: &mut HashMap<Vec<u8>, Vec<(TileId, Vec<u8>)>>,
) -> Result<(), LiteError> {
    let n = tile.row_count();
    for row in 0..n {
        let coord = tile_row_coord(tile, row)?;
        let key = zerompk::to_msgpack_vec(&coord).map_err(|e| LiteError::Serialization {
            detail: format!("encode coord: {e}"),
        })?;
        if let Ok(Some(cell_bytes)) = extract_cell_bytes(tile, &coord) {
            acc.entry(key).or_default().push((tile_id, cell_bytes));
        }
    }
    Ok(())
}

fn tile_row_coord(tile: &SparseTile, row: usize) -> Result<Vec<CoordValue>, LiteError> {
    let arity = tile.dim_dicts.len();
    let mut coord = Vec::with_capacity(arity);
    for dim_idx in 0..arity {
        let dict = tile
            .dim_dicts
            .get(dim_idx)
            .ok_or_else(|| LiteError::Storage {
                detail: format!("tile_row_coord: dim {dim_idx} missing"),
            })?;
        let entry_idx = *dict.indices.get(row).ok_or_else(|| LiteError::Storage {
            detail: format!("tile_row_coord: row {row} out of range"),
        })? as usize;
        let val = dict
            .values
            .get(entry_idx)
            .ok_or_else(|| LiteError::Storage {
                detail: format!("tile_row_coord: dict entry {entry_idx} missing"),
            })?;
        coord.push(val.clone());
    }
    Ok(coord)
}

fn cell_in_slice(coord: &[CoordValue], slice: &Slice) -> bool {
    use CoordValue::*;
    use nodedb_array::types::domain::DomainBound;
    for (i, range) in slice.dim_ranges.iter().enumerate() {
        let Some(r) = range else { continue };
        let Some(c) = coord.get(i) else { return false };
        let in_range = match (&r.lo, c, &r.hi) {
            (DomainBound::Int64(l), Int64(v), DomainBound::Int64(h)) => l <= v && v <= h,
            (DomainBound::Float64(l), Float64(v), DomainBound::Float64(h)) => l <= v && v <= h,
            (DomainBound::String(l), String(v), DomainBound::String(h)) => l <= v && v <= h,
            _ => false,
        };
        if !in_range {
            return false;
        }
    }
    true
}

impl ArrayMemtable {
    /// Read-only snapshot of all tile-version entries at or before
    /// `system_as_of`, materialised as `SparseTile`s.
    pub fn drain_all_tiles_read_only(
        &self,
        system_as_of: i64,
        schema: &ArraySchema,
    ) -> Result<Vec<(TileId, SparseTile)>, LiteError> {
        // Collect all prefixes in the memtable.
        let mut prefixes: std::collections::HashSet<u64> = std::collections::HashSet::new();
        for id in self.tiles_iter() {
            if id.system_from_ms <= system_as_of {
                prefixes.insert(id.hilbert_prefix);
            }
        }
        let mut result = Vec::new();
        for prefix in prefixes {
            let mut tiles = self.iter_tiles_for_prefix(prefix, system_as_of, schema)?;
            result.append(&mut tiles);
        }
        Ok(result)
    }
}
