//! `ArrayEngineState` — the embedded ND sparse array engine for NodeDB-Lite.
//!
//! Separates in-memory state from storage access. The state struct is not
//! generic over the storage backend; instead, callers pass `&Arc<S>` to each
//! operation that requires storage I/O. This allows `ArrayEngineState` to be
//! stored in a `Mutex` inside `NodeDbLite<S: StorageEngine>` without adding a
//! `StorageEngineSync` bound to the struct.

use std::collections::HashMap;
use std::sync::Arc;

use roaring;

use nodedb_array::query::ceiling::{CeilingParams, CeilingResult, ceiling_resolve_cell};
use nodedb_array::query::slice::{DimRange, Slice};
use nodedb_array::schema::ArraySchema;
use nodedb_array::segment::extract_cell_bytes;
use nodedb_array::tile::cell_payload::CellPayload;
use nodedb_array::tile::sparse_tile::SparseTile;
use nodedb_array::types::TileId;
use nodedb_array::types::coord::value::CoordValue;
use nodedb_array::{TilePayload, tile_id_for_cell};
use nodedb_types::Surrogate;

use crate::engine::array::catalog::{ArrayCatalog, ArrayCatalogEntry, hash_schema};
use crate::engine::array::manifest::{ArrayManifest, SegmentRef, load_manifest, save_manifest};
use crate::engine::array::memtable::{ArrayMemtable, PutCellArgs};
use crate::engine::array::segments::write_segment;
use crate::error::LiteError;
use crate::storage::engine::StorageEngineSync;

const DEFAULT_FLUSH_THRESHOLD: usize = 4096;

/// Per-array runtime state.
pub(crate) struct ArrayState {
    pub schema: ArraySchema,
    pub schema_hash: u64,
    pub manifest: ArrayManifest,
    pub memtable: ArrayMemtable,
    pub audit_retain_ms: Option<i64>,
}

/// Storage-agnostic in-memory state for the array engine.
///
/// All operations that touch persistent storage take an explicit `storage`
/// parameter so this struct can be stored in `Mutex<ArrayEngineState>` inside
/// `NodeDbLite<S: StorageEngine>` without requiring `S: StorageEngineSync` on
/// the struct bound.
pub struct ArrayEngineState {
    pub(crate) arrays: HashMap<String, ArrayState>,
    flush_threshold: usize,
}

impl Default for ArrayEngineState {
    fn default() -> Self {
        Self {
            arrays: HashMap::new(),
            flush_threshold: DEFAULT_FLUSH_THRESHOLD,
        }
    }
}

impl ArrayEngineState {
    pub fn new() -> Self {
        Self::default()
    }

    /// Restore array engine from persistent storage.
    pub fn open<S: StorageEngineSync>(storage: &Arc<S>) -> Result<Self, LiteError> {
        let catalog = ArrayCatalog::open(Arc::clone(storage))?;
        let mut arrays = HashMap::new();

        for name in catalog.names().map(|s| s.to_owned()).collect::<Vec<_>>() {
            let entry = catalog
                .get(&name)
                .ok_or_else(|| LiteError::Storage {
                    detail: format!("array catalog entry '{name}' missing after iteration"),
                })?
                .clone();
            let schema = entry.schema()?;
            let manifest = load_manifest(storage, &name)?;
            arrays.insert(
                name,
                ArrayState {
                    schema,
                    schema_hash: entry.schema_hash,
                    manifest,
                    memtable: ArrayMemtable::new(),
                    audit_retain_ms: entry.audit_retain_ms,
                },
            );
        }

        Ok(Self {
            arrays,
            flush_threshold: DEFAULT_FLUSH_THRESHOLD,
        })
    }

    /// Create a new array. Returns `Err` if an array named `name` already exists.
    pub fn create_array<S: StorageEngineSync>(
        &mut self,
        storage: &Arc<S>,
        name: &str,
        schema: ArraySchema,
    ) -> Result<(), LiteError> {
        if self.arrays.contains_key(name) {
            return Err(LiteError::BadRequest {
                detail: format!("array '{name}' already exists"),
            });
        }
        let schema_bytes =
            zerompk::to_msgpack_vec(&schema).map_err(|e| LiteError::Serialization {
                detail: format!("encode schema: {e}"),
            })?;
        let schema_hash = hash_schema(&schema)?;
        let entry = ArrayCatalogEntry {
            name: name.to_owned(),
            schema_bytes,
            schema_hash,
            audit_retain_ms: None,
            minimum_audit_retain_ms: None,
        };
        let mut catalog = ArrayCatalog::open(Arc::clone(storage))?;
        catalog.insert(entry)?;
        self.arrays.insert(
            name.to_owned(),
            ArrayState {
                schema,
                schema_hash,
                manifest: ArrayManifest::new(),
                memtable: ArrayMemtable::new(),
                audit_retain_ms: None,
            },
        );
        Ok(())
    }

    /// Delete an array and all its data.
    pub fn delete_array<S: StorageEngineSync>(
        &mut self,
        storage: &Arc<S>,
        name: &str,
    ) -> Result<(), LiteError> {
        let state = self
            .arrays
            .remove(name)
            .ok_or_else(|| LiteError::BadRequest {
                detail: format!("array '{name}' not found"),
            })?;
        crate::engine::array::manifest::drop_manifest(storage, name, &state.manifest)?;
        let mut catalog = ArrayCatalog::open(Arc::clone(storage))?;
        catalog.remove(name)?;
        Ok(())
    }

    /// Write a cell into the array.
    #[allow(clippy::too_many_arguments)]
    pub fn put_cell<S: StorageEngineSync>(
        &mut self,
        storage: &Arc<S>,
        name: &str,
        coord: Vec<CoordValue>,
        attrs: Vec<nodedb_array::types::cell_value::value::CellValue>,
        system_from_ms: i64,
        valid_from_ms: i64,
        valid_until_ms: i64,
    ) -> Result<(), LiteError> {
        let state = self
            .arrays
            .get_mut(name)
            .ok_or_else(|| LiteError::BadRequest {
                detail: format!("array '{name}' not found"),
            })?;

        state.memtable.put_cell(
            &state.schema,
            PutCellArgs {
                coord,
                attrs,
                surrogate: Surrogate::ZERO,
                system_from_ms,
                valid_from_ms,
                valid_until_ms,
            },
        )?;

        if state.memtable.stats().cell_count >= self.flush_threshold {
            let name_owned = name.to_owned();
            self.flush_memtable(storage, &name_owned)?;
        }

        Ok(())
    }

    /// Soft-delete a cell by appending a tombstone version.
    pub fn delete_cell(
        &mut self,
        name: &str,
        coord: Vec<CoordValue>,
        system_from_ms: i64,
    ) -> Result<(), LiteError> {
        let state = self
            .arrays
            .get_mut(name)
            .ok_or_else(|| LiteError::BadRequest {
                detail: format!("array '{name}' not found"),
            })?;
        let schema = state.schema.clone();
        state
            .memtable
            .delete_cell(&schema, coord, system_from_ms)
            .map(|_| ())
    }

    /// GDPR erase a cell — appends the `0xFE` erasure sentinel and immediately
    /// flushes so the sentinel is durably stored on disk.
    pub fn gdpr_erase_cell<S: StorageEngineSync>(
        &mut self,
        storage: &Arc<S>,
        name: &str,
        coord: Vec<CoordValue>,
        system_from_ms: i64,
    ) -> Result<(), LiteError> {
        let state = self
            .arrays
            .get_mut(name)
            .ok_or_else(|| LiteError::BadRequest {
                detail: format!("array '{name}' not found"),
            })?;
        let schema = state.schema.clone();
        state.memtable.erase_cell(&schema, coord, system_from_ms)?;
        self.flush_memtable(storage, name)
    }

    /// Read the most recent live payload for `coord` at or before `system_as_of`.
    /// Returns `None` for NotFound, Tombstoned, or Erased results.
    pub fn read_coord<S: StorageEngineSync>(
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
        )?;

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
    pub fn slice<S: StorageEngineSync>(
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
            let bytes = crate::engine::array::segments::load_segment(storage, name, seg_id)?;
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
    pub fn surrogate_bitmap_scan<S: StorageEngineSync>(
        &mut self,
        storage: &Arc<S>,
        name: &str,
        ranges: Vec<Option<DimRange>>,
        system_as_of: i64,
    ) -> Result<roaring::RoaringBitmap, LiteError> {
        let cells = self.slice(storage, name, ranges, system_as_of)?;
        let mut bitmap = roaring::RoaringBitmap::new();
        for payload in cells {
            bitmap.insert(payload.surrogate.as_u32());
        }
        Ok(bitmap)
    }

    /// Flush pending memtable data to a persistent segment.
    pub fn flush<S: StorageEngineSync>(
        &mut self,
        storage: &Arc<S>,
        name: &str,
    ) -> Result<(), LiteError> {
        self.flush_memtable(storage, name)
    }

    fn flush_memtable<S: StorageEngineSync>(
        &mut self,
        storage: &Arc<S>,
        name: &str,
    ) -> Result<(), LiteError> {
        let state = self
            .arrays
            .get_mut(name)
            .ok_or_else(|| LiteError::BadRequest {
                detail: format!("array '{name}' not found"),
            })?;

        if state.memtable.is_empty() {
            return Ok(());
        }

        let schema = state.schema.clone();
        let schema_hash = state.schema_hash;
        let tiles = state.memtable.drain_and_materialise(&schema)?;

        if tiles.is_empty() {
            return Ok(());
        }

        let refs: Vec<_> = tiles.iter().map(|(id, tile)| (*id, tile)).collect();
        let new_id = state.manifest.next_id;
        state.manifest.next_id += 1;
        let bytes = write_segment(storage, name, new_id, schema_hash, &refs)?;
        state.manifest.segments.push(SegmentRef {
            id: new_id,
            byte_len: bytes.len() as u64,
        });
        save_manifest(storage, name, &state.manifest)?;

        // Retention compaction (synchronous, only if configured).
        if let Some(retain_ms) = state.audit_retain_ms {
            let now_ms = now_millis();
            crate::engine::array::retention::run_retention(
                storage,
                name,
                &mut state.manifest,
                &schema,
                schema_hash,
                retain_ms,
                now_ms,
            )?;
        }

        Ok(())
    }
}

// ── Helpers ───────────────────────────────────────────────────────────────────

fn now_millis() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as i64
}

fn cell_versions_from_segments<S: StorageEngineSync>(
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
    )?;
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

// ── Extension to ArrayMemtable for slice ──────────────────────────────────────

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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::redb_storage::RedbStorage;
    use nodedb_array::schema::ArraySchemaBuilder;
    use nodedb_array::schema::attr_spec::{AttrSpec, AttrType};
    use nodedb_array::schema::dim_spec::{DimSpec, DimType};
    use nodedb_array::types::cell_value::value::CellValue;
    use nodedb_array::types::domain::{Domain, DomainBound};
    use nodedb_types::OPEN_UPPER;
    use std::sync::Arc;

    fn schema() -> ArraySchema {
        ArraySchemaBuilder::new("test")
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

    fn storage() -> Arc<RedbStorage> {
        Arc::new(RedbStorage::open_in_memory().unwrap())
    }

    #[test]
    fn create_and_put_and_read() {
        let s = storage();
        let mut engine = ArrayEngineState::open(&s).unwrap();
        engine.create_array(&s, "a", schema()).unwrap();
        engine
            .put_cell(
                &s,
                "a",
                vec![CoordValue::Int64(1)],
                vec![CellValue::Int64(42)],
                100,
                0,
                OPEN_UPPER,
            )
            .unwrap();
        engine.flush(&s, "a").unwrap();
        let result = engine
            .read_coord(&s, "a", &[CoordValue::Int64(1)], 200)
            .unwrap();
        assert!(result.is_some());
        assert_eq!(result.unwrap().attrs[0], CellValue::Int64(42));
    }

    #[test]
    fn delete_returns_none() {
        let s = storage();
        let mut engine = ArrayEngineState::open(&s).unwrap();
        engine.create_array(&s, "b", schema()).unwrap();
        engine
            .put_cell(
                &s,
                "b",
                vec![CoordValue::Int64(2)],
                vec![CellValue::Int64(7)],
                10,
                0,
                OPEN_UPPER,
            )
            .unwrap();
        engine
            .delete_cell("b", vec![CoordValue::Int64(2)], 20)
            .unwrap();
        engine.flush(&s, "b").unwrap();
        let result = engine
            .read_coord(&s, "b", &[CoordValue::Int64(2)], 100)
            .unwrap();
        assert!(result.is_none());
    }

    #[test]
    fn open_restores_catalog() {
        let s = storage();
        {
            let mut engine = ArrayEngineState::open(&s).unwrap();
            engine.create_array(&s, "persist", schema()).unwrap();
        }
        let engine2 = ArrayEngineState::open(&s).unwrap();
        assert!(engine2.arrays.contains_key("persist"));
    }

    #[test]
    fn bitemporal_as_of() {
        let s = storage();
        let mut engine = ArrayEngineState::open(&s).unwrap();
        engine.create_array(&s, "bt", schema()).unwrap();
        engine
            .put_cell(
                &s,
                "bt",
                vec![CoordValue::Int64(0)],
                vec![CellValue::Int64(10)],
                100,
                0,
                OPEN_UPPER,
            )
            .unwrap();
        engine
            .put_cell(
                &s,
                "bt",
                vec![CoordValue::Int64(0)],
                vec![CellValue::Int64(20)],
                200,
                0,
                OPEN_UPPER,
            )
            .unwrap();
        engine.flush(&s, "bt").unwrap();
        let r = engine
            .read_coord(&s, "bt", &[CoordValue::Int64(0)], 150)
            .unwrap()
            .unwrap();
        assert_eq!(r.attrs[0], CellValue::Int64(10));
        let r2 = engine
            .read_coord(&s, "bt", &[CoordValue::Int64(0)], 300)
            .unwrap()
            .unwrap();
        assert_eq!(r2.attrs[0], CellValue::Int64(20));
    }
}
