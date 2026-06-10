// SPDX-License-Identifier: Apache-2.0

//! `ArrayOp::Aggregate` handler for NodeDB-Lite.
//!
//! Scans all segments and the memtable, applies `aggregate_attr` (or
//! `group_by_dim`) per tile, then merges partials across tiles via
//! `AggregateResult::merge`. Bitemporal system-time ceiling is honoured:
//! tiles whose `system_from_ms` exceeds the cutoff are skipped, mirroring
//! the same filter applied in `engine.rs::slice`.

use std::collections::HashMap;
use std::sync::Arc;

use nodedb_array::query::aggregate::{
    AggregateResult, GroupAggregate, Reducer, aggregate_attr, group_by_dim,
};
use nodedb_array::tile::sparse_tile::SparseTile;
use nodedb_array::{SegmentReader, TilePayload};
use nodedb_physical::physical_plan::ArrayReducer;
use nodedb_types::result::QueryResult;
use nodedb_types::value::Value;

use crate::engine::array::engine::ArrayEngineState;
use crate::error::LiteError;
use crate::runtime::now_millis_i64;
use crate::storage::engine::StorageEngine;

fn map_reducer(r: ArrayReducer) -> Reducer {
    match r {
        ArrayReducer::Sum => Reducer::Sum,
        ArrayReducer::Count => Reducer::Count,
        ArrayReducer::Min => Reducer::Min,
        ArrayReducer::Max => Reducer::Max,
        ArrayReducer::Mean => Reducer::Mean,
    }
}

fn result_to_value(r: AggregateResult) -> Value {
    match r.finalize() {
        Some(v) => Value::Float(v),
        None => Value::Null,
    }
}

fn coord_value_to_value(cv: nodedb_array::types::coord::value::CoordValue) -> Value {
    use nodedb_array::types::coord::value::CoordValue;
    match cv {
        CoordValue::Int64(i) => Value::Integer(i),
        CoordValue::TimestampMs(i) => Value::Integer(i),
        CoordValue::Float64(f) => Value::Float(f),
        CoordValue::String(s) => Value::String(s),
    }
}

/// Emit one row per group.  Key column name comes from the dim at `dim_idx`.
fn emit_groups(groups: Vec<GroupAggregate>, rows: &mut Vec<Vec<Value>>) {
    for g in groups {
        rows.push(vec![coord_value_to_value(g.key), result_to_value(g.result)]);
    }
}

/// Merge a group map `dst` with groups from `src`.
fn merge_groups(
    dst: &mut HashMap<Vec<u8>, AggregateResult>,
    src: Vec<GroupAggregate>,
) -> Result<(), LiteError> {
    for g in src {
        let key_bytes = zerompk::to_msgpack_vec(&g.key).map_err(|e| LiteError::Serialization {
            detail: format!("encode group key: {e}"),
        })?;
        let entry = dst
            .entry(key_bytes)
            .or_insert(AggregateResult::Empty(match g.result {
                AggregateResult::Sum { .. } => Reducer::Sum,
                AggregateResult::Count { .. } => Reducer::Count,
                AggregateResult::Min { .. } => Reducer::Min,
                AggregateResult::Max { .. } => Reducer::Max,
                AggregateResult::Mean { .. } => Reducer::Mean,
                AggregateResult::Empty(r) => r,
            }));
        *entry = entry.merge(g.result);
    }
    Ok(())
}

/// Accumulate a single tile into the running scalar partial or group map.
fn accumulate_tile(
    tile: &SparseTile,
    attr_idx: usize,
    reducer: Reducer,
    group_dim: Option<usize>,
    scalar: &mut AggregateResult,
    groups: &mut HashMap<Vec<u8>, AggregateResult>,
) -> Result<(), LiteError> {
    match group_dim {
        None => {
            let partial = aggregate_attr(tile, attr_idx, reducer);
            *scalar = scalar.merge(partial);
        }
        Some(dim_idx) => {
            let tile_groups = group_by_dim(tile, dim_idx, attr_idx, reducer);
            merge_groups(groups, tile_groups)?;
        }
    }
    Ok(())
}

/// Execute `ArrayOp::Aggregate` for the Lite engine.
pub async fn aggregate<S: StorageEngine>(
    array_state: &Arc<tokio::sync::Mutex<ArrayEngineState>>,
    storage: &Arc<S>,
    name: &str,
    attr_idx: u32,
    reducer: ArrayReducer,
    group_by_dim_idx: i32,
) -> Result<QueryResult, LiteError> {
    let system_as_of = now_millis_i64();
    let reducer_inner = map_reducer(reducer);
    let group_dim: Option<usize> = if group_by_dim_idx >= 0 {
        Some(group_by_dim_idx as usize)
    } else {
        None
    };

    let (seg_ids, schema, attr_count, dim_count) = {
        let state = array_state.lock().await;
        let arr = state
            .arrays
            .get(name)
            .ok_or_else(|| LiteError::BadRequest {
                detail: format!("array '{name}' not found"),
            })?;
        let seg_ids: Vec<u64> = arr.manifest.segments.iter().map(|s| s.id).collect();
        let attr_count = arr.schema.attrs.len();
        let dim_count = arr.schema.dims.len();
        (seg_ids, arr.schema.clone(), attr_count, dim_count)
    };

    let attr_idx_usize = attr_idx as usize;
    if attr_idx_usize >= attr_count {
        return Err(LiteError::BadRequest {
            detail: format!(
                "aggregate attr index {attr_idx} out of range (array '{name}' has {attr_count} attrs)"
            ),
        });
    }
    if let Some(d) = group_dim
        && d >= dim_count
    {
        return Err(LiteError::BadRequest {
            detail: format!(
                "group_by dim index {d} out of range (array '{name}' has {dim_count} dims)"
            ),
        });
    }

    let mut scalar = AggregateResult::Empty(reducer_inner);
    // Key: msgpack-encoded CoordValue, Value: running partial.
    let mut groups: HashMap<Vec<u8>, AggregateResult> = HashMap::new();

    // Segments.
    for seg_id in &seg_ids {
        let bytes = crate::engine::array::segments::load_segment(storage, name, *seg_id).await?;
        let reader = SegmentReader::open(&bytes).map_err(|e| LiteError::Storage {
            detail: format!("open segment {seg_id}: {e}"),
        })?;
        for idx in 0..reader.tile_count() {
            let entry_tile_id = reader.tiles()[idx].tile_id;
            if entry_tile_id.system_from_ms > system_as_of {
                continue;
            }
            let payload = reader.read_tile(idx).map_err(|e| LiteError::Storage {
                detail: format!("read_tile seg {seg_id} idx {idx}: {e}"),
            })?;
            let TilePayload::Sparse(tile) = payload else {
                continue;
            };
            accumulate_tile(
                &tile,
                attr_idx_usize,
                reducer_inner,
                group_dim,
                &mut scalar,
                &mut groups,
            )?;
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
                detail: format!("memtable drain: {e}"),
            })?;
        for (_tile_id, tile) in &mem_tiles {
            accumulate_tile(
                tile,
                attr_idx_usize,
                reducer_inner,
                group_dim,
                &mut scalar,
                &mut groups,
            )?;
        }
    }

    // Build result.
    match group_dim {
        None => {
            let columns = vec!["value".to_string()];
            let row = vec![result_to_value(scalar)];
            Ok(QueryResult {
                columns,
                rows: vec![row],
                rows_affected: 0,
            })
        }
        Some(_) => {
            let columns = vec!["key".to_string(), "value".to_string()];
            let mut rows: Vec<Vec<Value>> = Vec::new();
            // Decode group keys back to CoordValue for display.
            for (key_bytes, result) in groups {
                let coord_val: nodedb_array::types::coord::value::CoordValue =
                    zerompk::from_msgpack(&key_bytes).map_err(|e| LiteError::Serialization {
                        detail: format!("decode group key: {e}"),
                    })?;
                emit_groups(
                    vec![GroupAggregate {
                        key: coord_val,
                        result,
                    }],
                    &mut rows,
                );
            }
            Ok(QueryResult {
                columns,
                rows,
                rows_affected: 0,
            })
        }
    }
}
