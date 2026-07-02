// SPDX-License-Identifier: Apache-2.0

//! `ArrayOp::Project` handler for NodeDB-Lite.
//!
//! Scans all tiles (memtable + segments) for the named array, applies
//! attribute projection via `nodedb_array::query::project::project_sparse`,
//! and returns one row per live cell. The response mirrors the Slice arm:
//! columns `["attrs", "valid_from_ms", "valid_until_ms"]`.

use std::sync::Arc;

use nodedb_array::query::project::{Projection, project_sparse};
use nodedb_array::query::retention::decode_sparse_rows;
use nodedb_array::tile::sparse_tile::RowKind;
use nodedb_array::{SegmentReader, TilePayload};
use nodedb_types::result::QueryResult;
use nodedb_types::value::Value;

use crate::engine::array::engine::ArrayEngineState;
use crate::engine::array::ops::util::cell::cell_value_to_value;
use crate::error::LiteError;
use crate::runtime::now_millis_i64;
use crate::storage::engine::StorageEngine;

/// Execute `ArrayOp::Project` for the Lite engine.
///
/// Scans all segments and the memtable, projects to the requested attribute
/// indices, and returns every live cell as a row. The attribute order in the
/// response matches `attr_indices` — not the schema order.
pub async fn project<S: StorageEngine>(
    array_state: &Arc<tokio::sync::Mutex<ArrayEngineState>>,
    storage: &Arc<S>,
    name: &str,
    attr_indices: &[u32],
) -> Result<QueryResult, LiteError> {
    let now_ms = now_millis_i64();

    let (seg_ids, schema, schema_attr_count) = {
        let state = array_state.lock().await;
        let arr = state
            .arrays
            .get(name)
            .ok_or_else(|| LiteError::BadRequest {
                detail: format!("array '{name}' not found"),
            })?;
        let seg_ids: Vec<u64> = arr.manifest.segments.iter().map(|s| s.id).collect();
        let attr_count = arr.schema.attrs.len();
        (seg_ids, arr.schema.clone(), attr_count)
    };

    // Validate projection indices against schema attr count.
    for &idx in attr_indices {
        if idx as usize >= schema_attr_count {
            return Err(LiteError::BadRequest {
                detail: format!(
                    "project attr index {idx} out of range (array '{name}' has {schema_attr_count} attrs)"
                ),
            });
        }
    }

    let proj = Projection::new(attr_indices.iter().map(|&i| i as usize).collect());

    let mut rows: Vec<Vec<Value>> = Vec::new();
    let columns = vec![
        "attrs".to_string(),
        "valid_from_ms".to_string(),
        "valid_until_ms".to_string(),
    ];

    // Segments.
    for seg_id in &seg_ids {
        let bytes = crate::engine::array::segments::load_segment(storage, name, *seg_id).await?;
        let reader = SegmentReader::open(&bytes).map_err(|e| LiteError::Storage {
            detail: format!("open segment {seg_id}: {e}"),
        })?;
        for idx in 0..reader.tile_count() {
            let entry_tile_id = reader.tiles()[idx].tile_id;
            if entry_tile_id.system_from_ms > now_ms {
                continue;
            }
            let payload = reader.read_tile(idx).map_err(|e| LiteError::Storage {
                detail: format!("read_tile seg {seg_id} idx {idx}: {e}"),
            })?;
            let sparse = match payload {
                TilePayload::Sparse(s) => s,
                TilePayload::Dense(_) => continue,
            };
            let projected = project_sparse(&sparse, &proj).map_err(|e| LiteError::Storage {
                detail: format!("project_sparse: {e}"),
            })?;
            for row in decode_sparse_rows(&projected).map_err(|e| LiteError::Storage {
                detail: format!("decode_sparse_rows: {e}"),
            })? {
                if row.kind != RowKind::Live {
                    continue;
                }
                let p = match row.payload {
                    Some(p) => p,
                    None => continue,
                };
                let attrs_val =
                    Value::Array(p.attrs.into_iter().map(cell_value_to_value).collect());
                rows.push(vec![
                    attrs_val,
                    Value::Integer(p.valid_from_ms),
                    Value::Integer(p.valid_until_ms),
                ]);
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
            .drain_all_tiles_read_only(now_ms, &schema)
            .map_err(|e| LiteError::Storage {
                detail: format!("memtable drain: {e}"),
            })?;
        for (_tile_id, sparse) in &mem_tiles {
            let projected = project_sparse(sparse, &proj).map_err(|e| LiteError::Storage {
                detail: format!("project_sparse memtable: {e}"),
            })?;
            for row in decode_sparse_rows(&projected).map_err(|e| LiteError::Storage {
                detail: format!("decode_sparse_rows memtable: {e}"),
            })? {
                if row.kind != RowKind::Live {
                    continue;
                }
                let p = match row.payload {
                    Some(p) => p,
                    None => continue,
                };
                let attrs_val =
                    Value::Array(p.attrs.into_iter().map(cell_value_to_value).collect());
                rows.push(vec![
                    attrs_val,
                    Value::Integer(p.valid_from_ms),
                    Value::Integer(p.valid_until_ms),
                ]);
            }
        }
    }

    Ok(QueryResult {
        columns,
        rows,
        rows_affected: 0,
    })
}
