// SPDX-License-Identifier: Apache-2.0
//! `PhysicalTaskVisitor` impl for Lite. Single place that decides which
//! `PhysicalPlan` variants Lite can execute. Adding a new variant to
//! `nodedb-physical` is a hard compile error here until handled.

use std::future::Future;
use std::pin::Pin;

use std::sync::Arc;

use nodedb_array::query::slice::Slice;
use nodedb_array::types::cell_value::value::CellValue;
use nodedb_array::types::coord::value::CoordValue;

use crate::engine::array::ops::util::cell::cell_value_to_value;
use crate::engine::array::ops::util::time::now_ms;
use nodedb_physical::PhysicalTaskVisitor;
use nodedb_physical::physical_plan::{ArrayOp, VectorOp};
use roaring;

use super::vector_op::execute_vector_op;

use nodedb_physical::physical_plan::TextOp;

use crate::error::LiteError;
use crate::query::engine::LiteQueryEngine;
use crate::storage::engine::{StorageEngine, StorageEngineSync};
use nodedb_types::result::QueryResult;
use nodedb_types::value::Value;

use super::text_op::execute_text_op;

/// Local mirror of `nodedb::engine::array::wal::ArrayPutCell` for
/// deserializing `ArrayOp::Put.cells_msgpack` without a dependency on the
/// Origin binary crate. Field order and types must match the Origin definition
/// exactly because zerompk encodes structs as positional arrays.
#[derive(serde::Deserialize, zerompk::FromMessagePack)]
struct PutCellWire {
    coord: Vec<CoordValue>,
    attrs: Vec<CellValue>,
    _surrogate: nodedb_types::Surrogate,
    system_from_ms: i64,
    valid_from_ms: i64,
    valid_until_ms: i64,
}

use super::unsupported::impl_unsupported_lite_physical_visitor_methods;

/// Decode a msgpack-encoded `Slice` for array `name` and run a surrogate
/// bitmap scan against the array engine, returning the set of surrogates
/// for all live cells that match the slice predicate.
///
/// Callers that need a `RoaringBitmap` in-process (e.g. `vector_search`)
/// call this directly; the `array::SurrogateBitmapScan` arm calls this and
/// wraps the result into a `QueryResult` for dispatch-path callers.
pub(crate) fn execute_surrogate_scan<S: StorageEngine + StorageEngineSync>(
    array_state: &Arc<std::sync::Mutex<crate::engine::array::engine::ArrayEngineState>>,
    storage: &Arc<S>,
    name: &str,
    slice_bytes: &[u8],
) -> Result<roaring::RoaringBitmap, LiteError> {
    let slice: Slice =
        zerompk::from_msgpack(slice_bytes).map_err(|e| LiteError::Serialization {
            detail: format!("decode Slice predicate: {e}"),
        })?;
    let system_as_of = now_ms();
    let mut state = array_state.lock().map_err(|_| LiteError::LockPoisoned)?;
    state.surrogate_bitmap_scan(storage, name, slice.dim_ranges, system_as_of)
}

pub(crate) type LitePhysicalFut<'a> =
    Pin<Box<dyn Future<Output = Result<QueryResult, LiteError>> + Send + 'a>>;

pub(crate) struct LiteDataPlaneVisitor<'a, S: StorageEngine + StorageEngineSync> {
    pub(crate) engine: &'a LiteQueryEngine<S>,
}

fn unsupported_phys_fut<'a>(name: &'static str) -> LitePhysicalFut<'a> {
    Box::pin(async move {
        Err(LiteError::Unsupported {
            detail: format!("Lite executor does not yet implement PhysicalPlan::{name}"),
        })
    })
}

macro_rules! u_phys {
    ($name:literal) => {
        Ok(unsupported_phys_fut($name))
    };
}

impl<'a, S: StorageEngine + StorageEngineSync + 'a> PhysicalTaskVisitor
    for LiteDataPlaneVisitor<'a, S>
{
    type Output = LitePhysicalFut<'a>;
    type Error = LiteError;

    fn vector(&mut self, op: &VectorOp) -> Result<LitePhysicalFut<'a>, LiteError> {
        execute_vector_op(self.engine, op)
    }

    fn array(&mut self, op: &ArrayOp) -> Result<LitePhysicalFut<'a>, LiteError> {
        let engine = self.engine;
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
                    let schema = zerompk::from_msgpack(&schema_bytes).map_err(|e| {
                        LiteError::Serialization {
                            detail: format!("decode ArraySchema: {e}"),
                        }
                    })?;
                    let mut state = array_state.lock().map_err(|_| LiteError::LockPoisoned)?;
                    state.create_array(&storage, &name, schema)?;
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
                        zerompk::from_msgpack(&cells_bytes).map_err(|e| {
                            LiteError::Serialization {
                                detail: format!("decode Put cells: {e}"),
                            }
                        })?;
                    let mut state = array_state.lock().map_err(|_| LiteError::LockPoisoned)?;
                    let mut rows_affected: u64 = 0;
                    for cell in cells {
                        state.put_cell(
                            &storage,
                            &name,
                            cell.coord,
                            cell.attrs,
                            cell.system_from_ms,
                            cell.valid_from_ms,
                            cell.valid_until_ms,
                        )?;
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
                    let coords: Vec<Vec<CoordValue>> = zerompk::from_msgpack(&coords_bytes)
                        .map_err(|e| LiteError::Serialization {
                            detail: format!("decode Delete coords: {e}"),
                        })?;
                    let now = now_ms();
                    let mut state = array_state.lock().map_err(|_| LiteError::LockPoisoned)?;
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
                    let slice: Slice = zerompk::from_msgpack(&slice_bytes).map_err(|e| {
                        LiteError::Serialization {
                            detail: format!("decode Slice predicate: {e}"),
                        }
                    })?;
                    let mut state = array_state.lock().map_err(|_| LiteError::LockPoisoned)?;
                    let cells = state.slice(&storage, &name, slice.dim_ranges, system_as_of)?;
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
                    let mut state = array_state.lock().map_err(|_| LiteError::LockPoisoned)?;
                    state.flush(&storage, &name)?;
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
                    let mut state = array_state.lock().map_err(|_| LiteError::LockPoisoned)?;
                    state.delete_array(&storage, &name)?;
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
                    crate::engine::array::ops::project::project(
                        &array_state,
                        &storage,
                        &name,
                        &indices,
                    )
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
                        execute_surrogate_scan(&array_state, &storage, &name, &slice_bytes)?;
                    let mut bitmap_bytes = Vec::new();
                    bitmap.serialize_into(&mut bitmap_bytes).map_err(|e| {
                        LiteError::Serialization {
                            detail: format!("serialize surrogate bitmap: {e}"),
                        }
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

    fn text(&mut self, op: &TextOp) -> Result<LitePhysicalFut<'a>, LiteError> {
        execute_text_op(self.engine, op)
    }

    impl_unsupported_lite_physical_visitor_methods!();
}
