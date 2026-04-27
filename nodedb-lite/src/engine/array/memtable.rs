//! In-memory write buffer for one array — thin re-implementation of the
//! Origin memtable adapted for Lite's sync context.
//!
//! Cells are bucketed by `TileId` (`hilbert_prefix`, `system_from_ms`).
//! Each bucket is a `HashMap<encoded_coord, payload_bytes>`. Flush drains
//! the buckets through `SparseTileBuilder` to produce `SparseTile`s ready
//! for `SegmentWriter`.

use std::collections::{BTreeMap, HashMap};

use nodedb_array::schema::ArraySchema;
use nodedb_array::tile::cell_payload::{
    CELL_GDPR_ERASURE_SENTINEL, CELL_TOMBSTONE_SENTINEL, CellPayload, is_cell_gdpr_erasure,
    is_cell_sentinel, is_cell_tombstone,
};
use nodedb_array::tile::sparse_tile::{RowKind, SparseRow, SparseTile, SparseTileBuilder};
use nodedb_array::tile::tile_id_for_cell;
use nodedb_array::types::TileId;
use nodedb_array::types::cell_value::value::CellValue;
use nodedb_array::types::coord::value::CoordValue;
use nodedb_types::{OPEN_UPPER, Surrogate};

use crate::error::LiteError;

fn encode_coord(coord: &[CoordValue]) -> Result<Vec<u8>, LiteError> {
    zerompk::to_msgpack_vec(&coord.to_vec()).map_err(|e| LiteError::Serialization {
        detail: format!("encode coord: {e}"),
    })
}

fn decode_coord(key: &[u8]) -> Result<Vec<CoordValue>, LiteError> {
    zerompk::from_msgpack(key).map_err(|e| LiteError::Serialization {
        detail: format!("decode coord: {e}"),
    })
}

/// Raw write buffer for a single tile version.
#[derive(Debug, Default, Clone)]
struct TileBuffer {
    entries: HashMap<Vec<u8>, Vec<u8>>,
}

impl TileBuffer {
    fn entry_count(&self) -> usize {
        self.entries.len()
    }

    fn live_cell_count(&self) -> usize {
        self.entries
            .values()
            .filter(|v| !is_cell_sentinel(v))
            .count()
    }

    fn get(&self, coord: &[CoordValue]) -> Option<&[u8]> {
        let key = encode_coord(coord).ok()?;
        self.entries.get(&key).map(|v| v.as_slice())
    }

    fn materialise(&self, schema: &ArraySchema) -> Result<SparseTile, LiteError> {
        let mut b = SparseTileBuilder::new(schema);
        for (coord_key, bytes) in &self.entries {
            let coord = decode_coord(coord_key)?;
            if is_cell_tombstone(bytes) {
                b.push_row(SparseRow {
                    coord: &coord,
                    attrs: &[],
                    surrogate: Surrogate::ZERO,
                    valid_from_ms: 0,
                    valid_until_ms: OPEN_UPPER,
                    kind: RowKind::Tombstone,
                })
                .map_err(|e| LiteError::Storage {
                    detail: format!("push tombstone row: {e}"),
                })?;
            } else if is_cell_gdpr_erasure(bytes) {
                b.push_row(SparseRow {
                    coord: &coord,
                    attrs: &[],
                    surrogate: Surrogate::ZERO,
                    valid_from_ms: 0,
                    valid_until_ms: OPEN_UPPER,
                    kind: RowKind::GdprErased,
                })
                .map_err(|e| LiteError::Storage {
                    detail: format!("push erasure row: {e}"),
                })?;
            } else {
                let payload = CellPayload::decode(bytes).map_err(|e| LiteError::Storage {
                    detail: format!("decode CellPayload: {e}"),
                })?;
                b.push_row(SparseRow {
                    coord: &coord,
                    attrs: &payload.attrs,
                    surrogate: payload.surrogate,
                    valid_from_ms: payload.valid_from_ms,
                    valid_until_ms: payload.valid_until_ms,
                    kind: RowKind::Live,
                })
                .map_err(|e| LiteError::Storage {
                    detail: format!("push live row: {e}"),
                })?;
            }
        }
        Ok(b.build())
    }
}

/// Statistics about the current memtable state.
#[derive(Debug, Default)]
pub struct MemtableStats {
    pub tile_count: usize,
    pub cell_count: usize,
}

/// Arguments for `ArrayMemtable::put_cell`.
pub struct PutCellArgs {
    pub coord: Vec<CoordValue>,
    pub attrs: Vec<CellValue>,
    pub surrogate: Surrogate,
    pub system_from_ms: i64,
    pub valid_from_ms: i64,
    pub valid_until_ms: i64,
}

/// In-memory write buffer for a single array.
#[derive(Debug, Default)]
pub struct ArrayMemtable {
    tiles: BTreeMap<TileId, TileBuffer>,
}

impl ArrayMemtable {
    pub fn new() -> Self {
        Self::default()
    }

    /// Insert a cell. Creates a new tile-version for the given `system_from_ms`.
    pub fn put_cell(
        &mut self,
        schema: &ArraySchema,
        args: PutCellArgs,
    ) -> Result<TileId, LiteError> {
        let PutCellArgs {
            coord,
            attrs,
            surrogate,
            system_from_ms,
            valid_from_ms,
            valid_until_ms,
        } = args;
        let tile_id =
            tile_id_for_cell(schema, &coord, system_from_ms).map_err(|e| LiteError::Storage {
                detail: format!("tile_id_for_cell: {e}"),
            })?;
        let payload = CellPayload {
            valid_from_ms,
            valid_until_ms,
            attrs,
            surrogate,
        };
        let bytes = payload.encode().map_err(|e| LiteError::Storage {
            detail: format!("encode CellPayload: {e}"),
        })?;
        let key = encode_coord(&coord)?;
        self.tiles
            .entry(tile_id)
            .or_default()
            .entries
            .insert(key, bytes);
        Ok(tile_id)
    }

    /// Append a tombstone version for `(coord, system_from_ms)`.
    pub fn delete_cell(
        &mut self,
        schema: &ArraySchema,
        coord: Vec<CoordValue>,
        system_from_ms: i64,
    ) -> Result<TileId, LiteError> {
        let tile_id =
            tile_id_for_cell(schema, &coord, system_from_ms).map_err(|e| LiteError::Storage {
                detail: format!("tile_id_for_cell: {e}"),
            })?;
        let key = encode_coord(&coord)?;
        self.tiles
            .entry(tile_id)
            .or_default()
            .entries
            .insert(key, CELL_TOMBSTONE_SENTINEL.to_vec());
        Ok(tile_id)
    }

    /// Append a GDPR erasure sentinel for `(coord, system_from_ms)`.
    pub fn erase_cell(
        &mut self,
        schema: &ArraySchema,
        coord: Vec<CoordValue>,
        system_from_ms: i64,
    ) -> Result<TileId, LiteError> {
        let tile_id =
            tile_id_for_cell(schema, &coord, system_from_ms).map_err(|e| LiteError::Storage {
                detail: format!("tile_id_for_cell: {e}"),
            })?;
        let key = encode_coord(&coord)?;
        self.tiles
            .entry(tile_id)
            .or_default()
            .entries
            .insert(key, CELL_GDPR_ERASURE_SENTINEL.to_vec());
        Ok(tile_id)
    }

    /// Total cell count (live cells only, sentinels excluded).
    pub fn stats(&self) -> MemtableStats {
        MemtableStats {
            tile_count: self.tiles.len(),
            cell_count: self.tiles.values().map(|b| b.live_cell_count()).sum(),
        }
    }

    pub fn is_empty(&self) -> bool {
        self.tiles.values().all(|b| b.entry_count() == 0)
    }

    /// Iterate all tile IDs currently stored (used for prefix enumeration).
    pub fn tiles_iter(&self) -> impl Iterator<Item = TileId> + '_ {
        self.tiles.keys().copied()
    }

    /// Drain tiles sorted by `TileId` and materialise each into a `SparseTile`.
    pub fn drain_and_materialise(
        &mut self,
        schema: &ArraySchema,
    ) -> Result<Vec<(TileId, SparseTile)>, LiteError> {
        let tiles: Vec<_> = std::mem::take(&mut self.tiles).into_iter().collect();
        tiles
            .into_iter()
            .map(|(id, buf)| buf.materialise(schema).map(|tile| (id, tile)))
            .collect()
    }

    /// Iterate memtable tile versions for `hilbert_prefix` at or before
    /// `system_as_of`, newest-first. Returns `(TileId, raw_cell_bytes)` for
    /// the requested `coord`.
    pub fn iter_cell_versions(
        &self,
        hilbert_prefix: u64,
        system_as_of: i64,
        coord: &[CoordValue],
    ) -> Vec<(TileId, Vec<u8>)> {
        let lo = TileId::new(hilbert_prefix, i64::MIN);
        let hi = TileId::new(hilbert_prefix, system_as_of);
        self.tiles
            .range(lo..=hi)
            .rev()
            .filter_map(|(id, buf)| buf.get(coord).map(|b| (*id, b.to_vec())))
            .collect()
    }

    /// Iterate all tile versions for `hilbert_prefix` at or before `system_as_of`
    /// returning `(TileId, SparseTile)` — used by slice queries.
    pub fn iter_tiles_for_prefix(
        &self,
        hilbert_prefix: u64,
        system_as_of: i64,
        schema: &ArraySchema,
    ) -> Result<Vec<(TileId, SparseTile)>, LiteError> {
        let lo = TileId::new(hilbert_prefix, i64::MIN);
        let hi = TileId::new(hilbert_prefix, system_as_of);
        self.tiles
            .range(lo..=hi)
            .map(|(id, buf)| buf.materialise(schema).map(|tile| (*id, tile)))
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use nodedb_array::schema::ArraySchemaBuilder;
    use nodedb_array::schema::attr_spec::{AttrSpec, AttrType};
    use nodedb_array::schema::dim_spec::{DimSpec, DimType};
    use nodedb_array::types::domain::{Domain, DomainBound};

    fn schema() -> ArraySchema {
        ArraySchemaBuilder::new("a")
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

    #[test]
    fn put_and_stats() {
        let s = schema();
        let mut m = ArrayMemtable::new();
        m.put_cell(
            &s,
            PutCellArgs {
                coord: vec![CoordValue::Int64(1)],
                attrs: vec![CellValue::Int64(10)],
                surrogate: Surrogate::ZERO,
                system_from_ms: 100,
                valid_from_ms: 0,
                valid_until_ms: OPEN_UPPER,
            },
        )
        .unwrap();
        assert_eq!(m.stats().cell_count, 1);
    }

    #[test]
    fn delete_adds_tombstone_version() {
        let s = schema();
        let mut m = ArrayMemtable::new();
        m.put_cell(
            &s,
            PutCellArgs {
                coord: vec![CoordValue::Int64(1)],
                attrs: vec![CellValue::Int64(1)],
                surrogate: Surrogate::ZERO,
                system_from_ms: 100,
                valid_from_ms: 0,
                valid_until_ms: OPEN_UPPER,
            },
        )
        .unwrap();
        m.delete_cell(&s, vec![CoordValue::Int64(1)], 200).unwrap();
        assert_eq!(m.tiles.len(), 2);
        assert_eq!(m.stats().cell_count, 1);
    }

    #[test]
    fn erase_adds_gdpr_sentinel() {
        let s = schema();
        let mut m = ArrayMemtable::new();
        m.put_cell(
            &s,
            PutCellArgs {
                coord: vec![CoordValue::Int64(2)],
                attrs: vec![CellValue::Int64(99)],
                surrogate: Surrogate::ZERO,
                system_from_ms: 10,
                valid_from_ms: 0,
                valid_until_ms: OPEN_UPPER,
            },
        )
        .unwrap();
        m.erase_cell(&s, vec![CoordValue::Int64(2)], 20).unwrap();
        assert_eq!(m.tiles.len(), 2);
    }

    #[test]
    fn drain_and_materialise_empties() {
        let s = schema();
        let mut m = ArrayMemtable::new();
        m.put_cell(
            &s,
            PutCellArgs {
                coord: vec![CoordValue::Int64(3)],
                attrs: vec![CellValue::Int64(7)],
                surrogate: Surrogate::ZERO,
                system_from_ms: 1,
                valid_from_ms: 0,
                valid_until_ms: OPEN_UPPER,
            },
        )
        .unwrap();
        let tiles = m.drain_and_materialise(&s).unwrap();
        assert_eq!(tiles.len(), 1);
        assert!(m.is_empty());
    }

    #[test]
    fn iter_cell_versions_newest_first() {
        let s = schema();
        let mut m = ArrayMemtable::new();
        let coord = vec![CoordValue::Int64(0)];
        for sys in [100i64, 200, 300] {
            m.put_cell(
                &s,
                PutCellArgs {
                    coord: coord.clone(),
                    attrs: vec![CellValue::Int64(sys)],
                    surrogate: Surrogate::ZERO,
                    system_from_ms: sys,
                    valid_from_ms: 0,
                    valid_until_ms: OPEN_UPPER,
                },
            )
            .unwrap();
        }
        let tile_id = nodedb_array::tile::tile_id_for_cell(&s, &coord, 100).unwrap();
        let versions = m.iter_cell_versions(tile_id.hilbert_prefix, 300, &coord);
        assert_eq!(versions.len(), 3);
        assert!(versions[0].0.system_from_ms >= versions[1].0.system_from_ms);
    }
}
