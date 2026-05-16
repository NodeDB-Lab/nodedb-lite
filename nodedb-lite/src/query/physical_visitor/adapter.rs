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
use nodedb_physical::PhysicalTaskVisitor;
use nodedb_physical::physical_plan::{ArrayOp, VectorOp};
use roaring;

use nodedb_physical::physical_plan::TextOp;

use crate::engine::vector::search::run_vector_search;
use crate::error::LiteError;
use crate::query::engine::LiteQueryEngine;
use crate::storage::engine::{StorageEngine, StorageEngineSync};
use nodedb_types::filter::MetadataFilter;
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

fn cell_value_to_value(cv: CellValue) -> Value {
    match cv {
        CellValue::Int64(i) => Value::Integer(i),
        CellValue::Float64(f) => Value::Float(f),
        CellValue::String(s) => Value::String(s),
        CellValue::Bytes(b) => Value::Bytes(b),
        CellValue::Null => Value::Null,
    }
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
    let system_as_of = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as i64;
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
        match op {
            VectorOp::Search {
                collection,
                field_name,
                query_vector,
                top_k,
                ef_search,
                rls_filters,
                metric,
                skip_payload_fetch,
                ..
            } => {
                let index_key = if field_name.is_empty() {
                    collection.clone()
                } else {
                    format!("{collection}:{field_name}")
                };
                let collection = collection.clone();
                let query = query_vector.clone();
                let k = *top_k;
                let ef = *ef_search;
                let metric = *metric;
                let skip_payload_fetch = *skip_payload_fetch;
                let metadata_filter: Option<MetadataFilter> = if rls_filters.is_empty() {
                    None
                } else {
                    Some(zerompk::from_msgpack(rls_filters).map_err(|e| {
                        LiteError::Serialization {
                            detail: format!("decode MetadataFilter: {e}"),
                        }
                    })?)
                };
                let vector_state = std::sync::Arc::clone(&self.engine.vector_state);
                let crdt = std::sync::Arc::clone(&self.engine.crdt);
                Ok(Box::pin(async move {
                    let results = run_vector_search(
                        &vector_state,
                        &crdt,
                        &index_key,
                        &collection,
                        &query,
                        k,
                        metadata_filter.as_ref(),
                        &[],
                        None,
                        None,
                        skip_payload_fetch,
                        Some(metric),
                        Some(ef),
                    )
                    .await
                    .map_err(|e| LiteError::Query(e.to_string()))?;

                    let columns = vec!["id".to_string(), "distance".to_string()];
                    let rows: Vec<Vec<Value>> = results
                        .into_iter()
                        .map(|r| vec![Value::String(r.id), Value::Float(r.distance as f64)])
                        .collect();
                    Ok(QueryResult {
                        columns,
                        rows,
                        rows_affected: 0,
                    })
                }))
            }
            _ => Ok(Box::pin(async {
                Err(LiteError::Unsupported {
                    detail: "Lite supports VectorOp::Search only".to_string(),
                })
            })),
        }
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
                    let now_ms = std::time::SystemTime::now()
                        .duration_since(std::time::UNIX_EPOCH)
                        .unwrap_or_default()
                        .as_millis() as i64;
                    let mut state = array_state.lock().map_err(|_| LiteError::LockPoisoned)?;
                    let mut rows_affected: u64 = 0;
                    for coord in coords {
                        state.delete_cell(&name, coord, now_ms)?;
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

            ArrayOp::Project { .. } => Ok(Box::pin(async {
                unimplemented!(
                    "Lite array engine does not yet support ArrayOp::Project; \
                     add the `project` method to ArrayEngineState in \
                     nodedb-lite::engine::array::engine"
                )
            })),

            ArrayOp::Aggregate { .. } => Ok(Box::pin(async {
                unimplemented!(
                    "Lite array engine does not yet support ArrayOp::Aggregate; \
                     add the `aggregate` method to ArrayEngineState in \
                     nodedb-lite::engine::array::engine"
                )
            })),

            ArrayOp::Elementwise { .. } => Ok(Box::pin(async {
                unimplemented!(
                    "Lite array engine does not yet support ArrayOp::Elementwise; \
                     add the `elementwise` method to ArrayEngineState in \
                     nodedb-lite::engine::array::engine"
                )
            })),

            ArrayOp::Compact { .. } => Ok(Box::pin(async {
                unimplemented!(
                    "Lite array engine does not yet support ArrayOp::Compact; \
                     add the `compact` method to ArrayEngineState in \
                     nodedb-lite::engine::array::engine"
                )
            })),

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
