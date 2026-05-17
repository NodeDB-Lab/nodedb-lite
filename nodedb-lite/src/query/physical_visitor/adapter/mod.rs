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
use nodedb_physical::physical_plan::{ArrayOp, CrdtOp, DocumentOp, KvOp, MetaOp, TextOp, VectorOp};
use nodedb_types::result::QueryResult;

use crate::engine::array::ops::util::time::now_ms;
use crate::error::LiteError;
use crate::query::engine::LiteQueryEngine;
use crate::storage::engine::{StorageEngine, StorageEngineSync};

use super::text_op::execute_text_op;
use super::unsupported::impl_unsupported_lite_physical_visitor_methods;
use super::vector_op::execute_vector_op;

mod array;
mod crdt;
mod document;
mod kv;
mod meta;

pub(crate) type LitePhysicalFut<'a> =
    Pin<Box<dyn Future<Output = Result<QueryResult, LiteError>> + Send + 'a>>;

pub(crate) struct LiteDataPlaneVisitor<'a, S: StorageEngine + StorageEngineSync> {
    pub(crate) engine: &'a LiteQueryEngine<S>,
}

/// Decode a msgpack-encoded `Slice` for array `name` and run a surrogate
/// bitmap scan against the array engine, returning the set of surrogates
/// for all live cells that match the slice predicate.
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

    impl_unsupported_lite_physical_visitor_methods!();
}
