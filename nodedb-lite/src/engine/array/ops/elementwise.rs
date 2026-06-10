// SPDX-License-Identifier: Apache-2.0

//! `ArrayOp::Elementwise` handler for NodeDB-Lite.
//!
//! Loads tiles from two arrays (left, right), pairs tiles that share the
//! same Hilbert prefix, and calls `nodedb_array::query::elementwise::elementwise`
//! per tile-pair. Unpaired tiles are elementwise-combined with an empty tile so
//! the outer-join null semantics propagate correctly. Results are collected as
//! one row per output cell.

use std::collections::HashMap;
use std::sync::Arc;

use nodedb_array::query::elementwise::{BinaryOp, elementwise};
use nodedb_array::tile::sparse_tile::SparseTile;
use nodedb_array::types::TileId;
use nodedb_array::{SegmentReader, TilePayload};
use nodedb_physical::physical_plan::ArrayBinaryOp;
use nodedb_types::result::QueryResult;
use nodedb_types::value::Value;

use crate::engine::array::engine::ArrayEngineState;
use crate::engine::array::ops::util::cell::cell_value_to_value;
use crate::error::LiteError;
use crate::runtime::now_millis_i64;
use crate::storage::engine::StorageEngine;

fn map_binary_op(op: ArrayBinaryOp) -> BinaryOp {
    match op {
        ArrayBinaryOp::Add => BinaryOp::Add,
        ArrayBinaryOp::Sub => BinaryOp::Sub,
        ArrayBinaryOp::Mul => BinaryOp::Mul,
        ArrayBinaryOp::Div => BinaryOp::Div,
    }
}

/// Collect all sparse tiles from an array's segments + memtable into a map
/// keyed by Hilbert prefix. The system-time cutoff is `system_as_of`.
async fn collect_tiles_for_array<S: StorageEngine>(
    array_state: &Arc<tokio::sync::Mutex<ArrayEngineState>>,
    storage: &Arc<S>,
    name: &str,
    system_as_of: i64,
) -> Result<HashMap<u64, Vec<SparseTile>>, LiteError> {
    let (seg_ids, schema) = {
        let state = array_state.lock().await;
        let arr = state
            .arrays
            .get(name)
            .ok_or_else(|| LiteError::BadRequest {
                detail: format!("array '{name}' not found"),
            })?;
        let seg_ids: Vec<u64> = arr.manifest.segments.iter().map(|s| s.id).collect();
        (seg_ids, arr.schema.clone())
    };

    let mut prefix_tiles: HashMap<u64, Vec<SparseTile>> = HashMap::new();

    for seg_id in &seg_ids {
        let bytes = crate::engine::array::segments::load_segment(storage, name, *seg_id).await?;
        let reader = SegmentReader::open(&bytes).map_err(|e| LiteError::Storage {
            detail: format!("open segment {seg_id}: {e}"),
        })?;
        for idx in 0..reader.tile_count() {
            let entry_tile_id: TileId = reader.tiles()[idx].tile_id;
            if entry_tile_id.system_from_ms > system_as_of {
                continue;
            }
            let payload = reader.read_tile(idx).map_err(|e| LiteError::Storage {
                detail: format!("read_tile seg {seg_id} idx {idx}: {e}"),
            })?;
            if let TilePayload::Sparse(tile) = payload {
                prefix_tiles
                    .entry(entry_tile_id.hilbert_prefix)
                    .or_default()
                    .push(tile);
            }
        }
    }

    // Memtable.
    {
        let mut state = array_state.lock().await;
        let arr = state
            .arrays
            .get_mut(name)
            .ok_or_else(|| LiteError::BadRequest {
                detail: format!("array '{name}' not found"),
            })?;
        let mem_tiles = arr
            .memtable
            .drain_all_tiles_read_only(system_as_of, &schema)
            .map_err(|e| LiteError::Storage {
                detail: format!("memtable drain '{name}': {e}"),
            })?;
        for (tile_id, tile) in mem_tiles {
            prefix_tiles
                .entry(tile_id.hilbert_prefix)
                .or_default()
                .push(tile);
        }
    }

    Ok(prefix_tiles)
}

/// Execute `ArrayOp::Elementwise` for the Lite engine.
pub async fn elementwise_op<S: StorageEngine>(
    array_state: &Arc<tokio::sync::Mutex<ArrayEngineState>>,
    storage: &Arc<S>,
    left_name: &str,
    right_name: &str,
    op: ArrayBinaryOp,
) -> Result<QueryResult, LiteError> {
    let system_as_of = now_millis_i64();
    let binary_op = map_binary_op(op);

    let schema = {
        let state = array_state.lock().await;
        let arr = state
            .arrays
            .get(left_name)
            .ok_or_else(|| LiteError::BadRequest {
                detail: format!("array '{left_name}' not found"),
            })?;
        arr.schema.clone()
    };

    let left_tiles = collect_tiles_for_array(array_state, storage, left_name, system_as_of).await?;
    let right_tiles =
        collect_tiles_for_array(array_state, storage, right_name, system_as_of).await?;

    // Union of all Hilbert prefixes from both sides.
    let mut all_prefixes: std::collections::HashSet<u64> = std::collections::HashSet::new();
    all_prefixes.extend(left_tiles.keys().copied());
    all_prefixes.extend(right_tiles.keys().copied());

    let columns = vec![
        "attrs".to_string(),
        "valid_from_ms".to_string(),
        "valid_until_ms".to_string(),
    ];
    let mut rows: Vec<Vec<Value>> = Vec::new();

    let empty_tile = nodedb_array::tile::sparse_tile::SparseTileBuilder::new(&schema).build();

    for prefix in all_prefixes {
        let left_set = left_tiles.get(&prefix);
        let right_set = right_tiles.get(&prefix);

        // Merge all tiles within the same prefix on each side before combining.
        let left_merged = merge_prefix_tiles(left_set, &schema, binary_op)?;
        let right_merged = merge_prefix_tiles(right_set, &schema, binary_op)?;

        let l = left_merged.as_ref().unwrap_or(&empty_tile);
        let r = right_merged.as_ref().unwrap_or(&empty_tile);

        let out = elementwise(&schema, l, r, binary_op).map_err(|e| LiteError::Storage {
            detail: format!("elementwise prefix {prefix}: {e}"),
        })?;

        let n = out.nnz() as usize;
        let attr_count = out.attr_cols.len();
        for row in 0..n {
            let attrs: Vec<Value> = (0..attr_count)
                .map(|ai| {
                    out.attr_cols
                        .get(ai)
                        .and_then(|col| col.get(row))
                        .map(|cv| cell_value_to_value(cv.clone()))
                        .unwrap_or(Value::Null)
                })
                .collect();
            rows.push(vec![
                Value::Array(attrs),
                Value::Integer(0),
                Value::Integer(i64::MAX),
            ]);
        }
    }

    Ok(QueryResult {
        columns,
        rows,
        rows_affected: 0,
    })
}

/// Merge a slice of tiles on the same side (left or right) into a single
/// tile using the elementwise op (same schema, so merging is self-consistent).
/// Returns `None` when the input is empty or absent.
fn merge_prefix_tiles(
    tiles: Option<&Vec<SparseTile>>,
    schema: &nodedb_array::schema::ArraySchema,
    op: BinaryOp,
) -> Result<Option<SparseTile>, LiteError> {
    let tiles = match tiles {
        Some(t) if !t.is_empty() => t,
        _ => return Ok(None),
    };
    let mut acc = tiles[0].clone();
    for tile in &tiles[1..] {
        acc = elementwise(schema, &acc, tile, op).map_err(|e| LiteError::Storage {
            detail: format!("merge prefix tiles: {e}"),
        })?;
    }
    Ok(Some(acc))
}
