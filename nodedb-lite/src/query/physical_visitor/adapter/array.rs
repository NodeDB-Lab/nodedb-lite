// SPDX-License-Identifier: Apache-2.0
//! ArrayOp dispatch for the Lite physical visitor.

use std::sync::Arc;

use nodedb_array::query::slice::Slice;
use nodedb_array::types::cell_value::value::CellValue;
use nodedb_array::types::coord::value::CoordValue;
use nodedb_physical::physical_plan::ArrayOp;
use nodedb_types::result::QueryResult;
use nodedb_types::value::Value;

use crate::engine::array::ops::util::cell::cell_value_to_value;
use crate::engine::array::ops::util::time::now_ms;
use crate::error::LiteError;
use crate::query::engine::LiteQueryEngine;
use crate::storage::engine::StorageEngine;

use super::{LitePhysicalFut, execute_surrogate_scan};

/// Local mirror of `nodedb::engine::array::wal::ArrayPutCell` for
/// deserializing `ArrayOp::Put.cells_msgpack` without a dependency on the
/// Origin binary crate. Field order and types must match the Origin
/// definition exactly because zerompk encodes structs as positional arrays.
#[derive(serde::Deserialize, zerompk::FromMessagePack)]
struct PutCellWire {
    coord: Vec<CoordValue>,
    attrs: Vec<CellValue>,
    _surrogate: nodedb_types::Surrogate,
    system_from_ms: i64,
    valid_from_ms: i64,
    valid_until_ms: i64,
}

pub(super) fn dispatch<'a, S: StorageEngine + 'a>(
    engine: &'a LiteQueryEngine<S>,
    op: &ArrayOp,
) -> Result<LitePhysicalFut<'a>, LiteError> {
    match op {
        ArrayOp::OpenArray {
            array_id,
            schema_msgpack,
            ..
        } => {
            let name = array_id.name.clone();
            let schema_bytes = schema_msgpack.clone();
            let array_state = Arc::clone(&engine.array_state);
            let storage = Arc::clone(&engine.storage);
            Ok(Box::pin(async move {
                let schema =
                    zerompk::from_msgpack(&schema_bytes).map_err(|e| LiteError::Serialization {
                        detail: format!("decode ArraySchema: {e}"),
                    })?;
                let mut state = array_state.lock().await;
                state.create_array(&storage, &name, schema).await?;
                Ok(QueryResult {
                    columns: vec![],
                    rows: vec![],
                    rows_affected: 1,
                })
            }))
        }

        ArrayOp::Put {
            array_id,
            cells_msgpack,
            ..
        } => {
            let name = array_id.name.clone();
            let cells_bytes = cells_msgpack.clone();
            let array_state = Arc::clone(&engine.array_state);
            let storage = Arc::clone(&engine.storage);
            Ok(Box::pin(async move {
                let cells: Vec<PutCellWire> =
                    zerompk::from_msgpack(&cells_bytes).map_err(|e| LiteError::Serialization {
                        detail: format!("decode Put cells: {e}"),
                    })?;
                let mut state = array_state.lock().await;
                let mut rows_affected: u64 = 0;
                for cell in cells {
                    state
                        .put_cell(
                            &storage,
                            &name,
                            cell.coord,
                            cell.attrs,
                            cell.system_from_ms,
                            cell.valid_from_ms,
                            cell.valid_until_ms,
                        )
                        .await?;
                    rows_affected += 1;
                }
                Ok(QueryResult {
                    columns: vec![],
                    rows: vec![],
                    rows_affected,
                })
            }))
        }

        ArrayOp::Delete {
            array_id,
            coords_msgpack,
            ..
        } => {
            let name = array_id.name.clone();
            let coords_bytes = coords_msgpack.clone();
            let array_state = Arc::clone(&engine.array_state);
            Ok(Box::pin(async move {
                let coords: Vec<Vec<CoordValue>> =
                    zerompk::from_msgpack(&coords_bytes).map_err(|e| LiteError::Serialization {
                        detail: format!("decode Delete coords: {e}"),
                    })?;
                let now = now_ms();
                let mut state = array_state.lock().await;
                let mut rows_affected: u64 = 0;
                for coord in coords {
                    state.delete_cell(&name, coord, now)?;
                    rows_affected += 1;
                }
                Ok(QueryResult {
                    columns: vec![],
                    rows: vec![],
                    rows_affected,
                })
            }))
        }

        ArrayOp::Slice {
            array_id,
            slice_msgpack,
            system_as_of,
            ..
        } => {
            let name = array_id.name.clone();
            let slice_bytes = slice_msgpack.clone();
            let system_as_of = system_as_of.unwrap_or(i64::MAX);
            let array_state = Arc::clone(&engine.array_state);
            let storage = Arc::clone(&engine.storage);
            Ok(Box::pin(async move {
                let slice: Slice =
                    zerompk::from_msgpack(&slice_bytes).map_err(|e| LiteError::Serialization {
                        detail: format!("decode Slice predicate: {e}"),
                    })?;
                let mut state = array_state.lock().await;
                let cells = state
                    .slice(&storage, &name, slice.dim_ranges, system_as_of)
                    .await?;
                let columns = vec![
                    "attrs".to_string(),
                    "valid_from_ms".to_string(),
                    "valid_until_ms".to_string(),
                ];
                let rows: Vec<Vec<Value>> = cells
                    .into_iter()
                    .map(|payload| {
                        let attrs_val = Value::Array(
                            payload.attrs.into_iter().map(cell_value_to_value).collect(),
                        );
                        vec![
                            attrs_val,
                            Value::Integer(payload.valid_from_ms),
                            Value::Integer(payload.valid_until_ms),
                        ]
                    })
                    .collect();
                Ok(QueryResult {
                    columns,
                    rows,
                    rows_affected: 0,
                })
            }))
        }

        ArrayOp::Flush { array_id, .. } => {
            let name = array_id.name.clone();
            let array_state = Arc::clone(&engine.array_state);
            let storage = Arc::clone(&engine.storage);
            Ok(Box::pin(async move {
                let mut state = array_state.lock().await;
                state.flush(&storage, &name).await?;
                Ok(QueryResult {
                    columns: vec![],
                    rows: vec![],
                    rows_affected: 0,
                })
            }))
        }

        ArrayOp::DropArray { array_id } => {
            let name = array_id.name.clone();
            let array_state = Arc::clone(&engine.array_state);
            let storage = Arc::clone(&engine.storage);
            Ok(Box::pin(async move {
                let mut state = array_state.lock().await;
                state.delete_array(&storage, &name).await?;
                Ok(QueryResult {
                    columns: vec![],
                    rows: vec![],
                    rows_affected: 1,
                })
            }))
        }

        ArrayOp::Project {
            array_id,
            attr_indices,
        } => {
            let name = array_id.name.clone();
            let indices = attr_indices.clone();
            let array_state = Arc::clone(&engine.array_state);
            let storage = Arc::clone(&engine.storage);
            Ok(Box::pin(async move {
                crate::engine::array::ops::project::project(&array_state, &storage, &name, &indices)
                    .await
            }))
        }

        ArrayOp::Aggregate {
            array_id,
            attr_idx,
            reducer,
            group_by_dim,
            ..
        } => {
            let name = array_id.name.clone();
            let attr_idx = *attr_idx;
            let reducer = *reducer;
            let group_by_dim = *group_by_dim;
            let array_state = Arc::clone(&engine.array_state);
            let storage = Arc::clone(&engine.storage);
            Ok(Box::pin(async move {
                crate::engine::array::ops::aggregate::aggregate(
                    &array_state,
                    &storage,
                    &name,
                    attr_idx,
                    reducer,
                    group_by_dim,
                )
                .await
            }))
        }

        ArrayOp::Elementwise {
            left, right, op, ..
        } => {
            let left_name = left.name.clone();
            let right_name = right.name.clone();
            let op = *op;
            let array_state = Arc::clone(&engine.array_state);
            let storage = Arc::clone(&engine.storage);
            Ok(Box::pin(async move {
                crate::engine::array::ops::elementwise::elementwise_op(
                    &array_state,
                    &storage,
                    &left_name,
                    &right_name,
                    op,
                )
                .await
            }))
        }

        ArrayOp::Compact {
            array_id,
            audit_retain_ms,
        } => {
            let name = array_id.name.clone();
            let retain_ms = *audit_retain_ms;
            let array_state = Arc::clone(&engine.array_state);
            let storage = Arc::clone(&engine.storage);
            Ok(Box::pin(async move {
                crate::engine::array::ops::compact::compact(
                    &array_state,
                    &storage,
                    &name,
                    retain_ms,
                )
                .await
            }))
        }

        ArrayOp::SurrogateBitmapScan {
            array_id,
            slice_msgpack,
        } => {
            let name = array_id.name.clone();
            let slice_bytes = slice_msgpack.clone();
            let array_state = Arc::clone(&engine.array_state);
            let storage = Arc::clone(&engine.storage);
            Ok(Box::pin(async move {
                let bitmap =
                    execute_surrogate_scan(&array_state, &storage, &name, &slice_bytes).await?;
                let mut bitmap_bytes = Vec::new();
                bitmap
                    .serialize_into(&mut bitmap_bytes)
                    .map_err(|e| LiteError::Serialization {
                        detail: format!("serialize surrogate bitmap: {e}"),
                    })?;
                Ok(QueryResult {
                    columns: vec!["bitmap".to_string()],
                    rows: vec![vec![nodedb_types::value::Value::Bytes(bitmap_bytes)]],
                    rows_affected: 0,
                })
            }))
        }
    }
}
