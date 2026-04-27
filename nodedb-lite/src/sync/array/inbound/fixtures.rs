//! Shared test fixtures for the inbound dispatcher.
//!
//! Compiled only under `#[cfg(test)]`. Each per-message-family submodule
//! (`delta`, `snapshot`, `schema`, `reject`) imports these helpers from here
//! rather than duplicating them.

#![cfg(test)]

use std::sync::{Arc, Mutex};

use nodedb_array::schema::array_schema::ArraySchema;
use nodedb_array::schema::attr_spec::{AttrSpec, AttrType};
use nodedb_array::schema::cell_order::{CellOrder, TileOrder};
use nodedb_array::schema::dim_spec::{DimSpec, DimType};
use nodedb_array::sync::hlc::Hlc;
use nodedb_array::sync::op::{ArrayOp, ArrayOpHeader, ArrayOpKind};
use nodedb_array::sync::replica_id::ReplicaId;
use nodedb_array::types::cell_value::value::CellValue;
use nodedb_array::types::coord::value::CoordValue;
use nodedb_array::types::domain::{Domain, DomainBound};

use crate::engine::array::engine::ArrayEngineState;
use crate::storage::redb_storage::RedbStorage;
use crate::sync::array::catchup::CatchupTracker;
use crate::sync::array::op_log_redb::RedbOpLog;
use crate::sync::array::pending::PendingQueue;
use crate::sync::array::replica_state::ReplicaState;
use crate::sync::array::schema_registry::SchemaRegistry;

use super::apply::LiteApplyEngine;
use super::dispatcher::ArrayInbound;

pub(crate) fn simple_schema(name: &str) -> ArraySchema {
    ArraySchema {
        name: name.into(),
        dims: vec![DimSpec::new(
            "x",
            DimType::Int64,
            Domain::new(DomainBound::Int64(0), DomainBound::Int64(99)),
        )],
        attrs: vec![AttrSpec::new("v", AttrType::Float64, true)],
        tile_extents: vec![10],
        cell_order: CellOrder::RowMajor,
        tile_order: TileOrder::RowMajor,
    }
}

pub(crate) fn rep() -> ReplicaId {
    ReplicaId::new(1)
}

pub(crate) fn hlc(ms: u64) -> Hlc {
    Hlc::new(ms, 0, rep()).unwrap()
}

pub(crate) fn put_op(array: &str, ms: u64, schema_ms: u64) -> ArrayOp {
    ArrayOp {
        header: ArrayOpHeader {
            array: array.into(),
            hlc: hlc(ms),
            schema_hlc: hlc(schema_ms),
            valid_from_ms: 0,
            valid_until_ms: -1,
            system_from_ms: ms as i64,
        },
        kind: ArrayOpKind::Put,
        coord: vec![CoordValue::Int64(ms as i64 % 10)],
        attrs: Some(vec![CellValue::Float64(ms as f64)]),
    }
}

pub(crate) type InboundFixture = (
    ArrayInbound<RedbStorage>,
    Arc<SchemaRegistry<RedbStorage>>,
    Arc<PendingQueue<RedbStorage>>,
    Arc<RedbStorage>,
);

/// Build a complete test fixture: storage + all sync sub-components +
/// [`ArrayInbound`].
pub(crate) fn make_inbound() -> InboundFixture {
    let storage = Arc::new(RedbStorage::open_in_memory().unwrap());
    let replica = Arc::new(ReplicaState::load_or_init(&*storage).unwrap());
    let schemas = Arc::new(SchemaRegistry::new(
        Arc::clone(&storage),
        Arc::clone(&replica),
    ));
    let op_log = Arc::new(RedbOpLog::new(Arc::clone(&storage)));
    let pending = Arc::new(PendingQueue::new(Arc::clone(&storage)));
    let array_state = Arc::new(Mutex::new(ArrayEngineState::new()));
    let engine = Arc::new(LiteApplyEngine::new(
        Arc::clone(&storage),
        Arc::clone(&array_state),
        Arc::clone(&schemas),
        Arc::clone(&op_log),
    ));
    let catchup = Arc::new(CatchupTracker::load(Arc::clone(&storage)).unwrap());
    let inbound = ArrayInbound::new(
        engine,
        Arc::clone(&schemas),
        Arc::clone(&replica),
        Arc::clone(&pending),
        op_log,
        catchup,
    );
    (inbound, schemas, pending, storage)
}
