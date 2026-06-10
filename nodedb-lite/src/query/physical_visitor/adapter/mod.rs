// SPDX-License-Identifier: Apache-2.0
//! `PhysicalTaskVisitor` impl for Lite. Single place that decides which
//! `PhysicalPlan` variants Lite can execute. Adding a new variant to
//! `nodedb-physical` is a hard compile error here until handled.
//!
//! Per-op-family dispatch lives in the sibling modules (`array`, `document`,
//! `kv`, `crdt`, `meta`); this module wires them into the visitor trait.

use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

use nodedb_array::query::slice::Slice;
use roaring;

use nodedb_physical::PhysicalTaskVisitor;
use nodedb_physical::physical_plan::{
    ArrayOp, ColumnarOp, CrdtOp, DocumentOp, GraphOp, KvOp, MetaOp, QueryOp, SpatialOp, TextOp,
    TimeseriesOp, VectorOp,
};
use nodedb_types::result::QueryResult;

use crate::error::LiteError;
use crate::query::engine::LiteQueryEngine;
use crate::runtime::now_millis_i64;
use crate::storage::engine::StorageEngine;

use super::text_op::execute_text_op;
use super::vector_op::execute_vector_op;

mod array;
mod columnar;
mod crdt;
mod document;
mod graph;
mod kv;
mod meta;
mod query;
mod spatial;
mod timeseries;

// On wasm32 the StorageEngine futures are `!Send` (async_trait(?Send)), so we
// cannot require Send on the physical future type.
#[cfg(not(target_arch = "wasm32"))]
pub(crate) type LitePhysicalFut<'a> =
    Pin<Box<dyn Future<Output = Result<QueryResult, LiteError>> + Send + 'a>>;

#[cfg(target_arch = "wasm32")]
pub(crate) type LitePhysicalFut<'a> =
    Pin<Box<dyn Future<Output = Result<QueryResult, LiteError>> + 'a>>;

pub(crate) struct LiteDataPlaneVisitor<'a, S: StorageEngine> {
    pub(crate) engine: &'a LiteQueryEngine<S>,
}

/// Decode a msgpack-encoded `Slice` for array `name` and run a surrogate
/// bitmap scan against the array engine, returning the set of surrogates
/// for all live cells that match the slice predicate.
pub(crate) async fn execute_surrogate_scan<S: StorageEngine>(
    array_state: &Arc<tokio::sync::Mutex<crate::engine::array::engine::ArrayEngineState>>,
    storage: &Arc<S>,
    name: &str,
    slice_bytes: &[u8],
) -> Result<roaring::RoaringBitmap, LiteError> {
    let slice: Slice =
        zerompk::from_msgpack(slice_bytes).map_err(|e| LiteError::Serialization {
            detail: format!("decode Slice predicate: {e}"),
        })?;
    let system_as_of = now_millis_i64();
    let mut state = array_state.lock().await;
    state
        .surrogate_bitmap_scan(storage, name, slice.dim_ranges, system_as_of)
        .await
}

impl<'a, S: StorageEngine + 'a> PhysicalTaskVisitor for LiteDataPlaneVisitor<'a, S> {
    type Output = LitePhysicalFut<'a>;
    type Error = LiteError;

    fn vector(&mut self, op: &VectorOp) -> Result<LitePhysicalFut<'a>, LiteError> {
        execute_vector_op(self.engine, op)
    }

    fn array(&mut self, op: &ArrayOp) -> Result<LitePhysicalFut<'a>, LiteError> {
        array::dispatch(self.engine, op)
    }

    fn text(&mut self, op: &TextOp) -> Result<LitePhysicalFut<'a>, LiteError> {
        execute_text_op(self.engine, op)
    }

    fn document(&mut self, op: &DocumentOp) -> Result<LitePhysicalFut<'a>, LiteError> {
        document::dispatch(self.engine, op)
    }

    fn kv(&mut self, op: &KvOp) -> Result<LitePhysicalFut<'a>, LiteError> {
        kv::dispatch(self.engine, op)
    }

    fn crdt(&mut self, op: &CrdtOp) -> Result<LitePhysicalFut<'a>, LiteError> {
        crdt::dispatch(self.engine, op)
    }

    fn meta(&mut self, op: &MetaOp) -> Result<LitePhysicalFut<'a>, LiteError> {
        meta::dispatch(self.engine, op)
    }

    fn columnar(&mut self, op: &ColumnarOp) -> Result<LitePhysicalFut<'a>, LiteError> {
        columnar::dispatch(self.engine, op)
    }

    fn timeseries(&mut self, op: &TimeseriesOp) -> Result<LitePhysicalFut<'a>, LiteError> {
        timeseries::dispatch(self.engine, op)
    }

    fn spatial(&mut self, op: &SpatialOp) -> Result<LitePhysicalFut<'a>, LiteError> {
        spatial::dispatch(self.engine, op)
    }

    fn graph(&mut self, op: &GraphOp) -> Result<LitePhysicalFut<'a>, LiteError> {
        graph::dispatch(self.engine, op)
    }

    fn query(&mut self, op: &QueryOp) -> Result<LitePhysicalFut<'a>, LiteError> {
        query::dispatch(self.engine, op)
    }

    fn cluster_array(
        &mut self,
        _op: &nodedb_physical::physical_plan::ClusterArrayOp,
    ) -> Result<LitePhysicalFut<'a>, LiteError> {
        unreachable!(
            "ClusterArray plans are coordinator-only; Lite never sets \
             cluster_enabled so its SQL planner cannot produce this variant"
        )
    }
}
