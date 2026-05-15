//! In-process sync harness: one Lite node with shared inbound dispatcher,
//! outbound emitter, and engine state.
//!
//! Bypasses WebSocket transport; exercises wire-message handlers directly
//! against an in-memory redb store.

use std::sync::{Arc, Mutex};

use nodedb_array::schema::array_schema::ArraySchema;
use nodedb_array::sync::hlc::Hlc;
use nodedb_array::sync::op::ArrayOp;
use nodedb_array::sync::op_codec;
use nodedb_array::types::cell_value::value::CellValue;
use nodedb_array::types::coord::value::CoordValue;
use nodedb_lite::engine::array::engine::ArrayEngineState;
use nodedb_lite::storage::redb_storage::RedbStorage;
use nodedb_lite::sync::array::catchup::CatchupTracker;
use nodedb_lite::sync::array::inbound::apply::LiteApplyEngine;
use nodedb_lite::sync::array::inbound::dispatcher::ArrayInbound;
use nodedb_lite::sync::array::inbound::outcome::InboundOutcome;
use nodedb_lite::sync::array::op_log_redb::RedbOpLog;
use nodedb_lite::sync::array::outbound::ArrayOutbound;
use nodedb_lite::sync::array::pending::PendingQueue;
use nodedb_lite::sync::array::replica_state::ReplicaState;
use nodedb_lite::sync::array::schema_registry::SchemaRegistry;
use nodedb_types::sync::wire::array::ArrayDeltaMsg;

use super::schema::simple_schema;

/// Convenience constructor used by outbound-loop tests.
pub fn make_outbound_harness() -> SyncHarness {
    SyncHarness::new_in_memory()
}

pub struct SyncHarness {
    pub inbound: ArrayInbound<RedbStorage>,
    pub outbound: ArrayOutbound<RedbStorage>,
    pub schemas: Arc<SchemaRegistry<RedbStorage>>,
    pub pending: Arc<PendingQueue<RedbStorage>>,
    pub op_log: Arc<RedbOpLog<RedbStorage>>,
    pub storage: Arc<RedbStorage>,
    /// Direct handle to the shared engine state for AS-OF queries in tests.
    pub array_state: Arc<Mutex<ArrayEngineState>>,
    pub catchup: Arc<CatchupTracker<RedbStorage>>,
}

impl SyncHarness {
    /// Create a harness backed by a fresh in-memory redb database.
    pub fn new_in_memory() -> Self {
        let storage = Arc::new(RedbStorage::open_in_memory().expect("open_in_memory"));
        Self::from_storage(storage)
    }

    /// Create a harness backed by the given storage (allows durability tests).
    pub fn from_storage(storage: Arc<RedbStorage>) -> Self {
        let replica = Arc::new(ReplicaState::load_or_init(&*storage).expect("load_or_init"));
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
        let catchup = Arc::new(CatchupTracker::load(Arc::clone(&storage)).expect("catchup load"));

        let inbound = ArrayInbound::new(
            engine,
            Arc::clone(&schemas),
            Arc::clone(&replica),
            Arc::clone(&pending),
            Arc::clone(&op_log),
            Arc::clone(&catchup),
        );

        let outbound = ArrayOutbound::new(
            Arc::clone(&op_log),
            Arc::clone(&pending),
            Arc::clone(&schemas),
            Arc::clone(&replica),
        );

        SyncHarness {
            inbound,
            outbound,
            schemas,
            pending,
            op_log,
            storage,
            array_state,
            catchup,
        }
    }

    /// Register the given schema in the SchemaRegistry AND the engine catalog.
    pub fn create_array(&self, name: &str) {
        let schema = simple_schema(name);
        self.schemas.put_schema(name, &schema).expect("put_schema");
        let mut state = self.array_state.lock().expect("lock");
        state
            .create_array(&self.storage, name, simple_schema(name))
            .expect("create_array");
    }

    /// Register a custom schema.
    pub fn create_array_with_schema(&self, name: &str, schema: ArraySchema) {
        self.schemas.put_schema(name, &schema).expect("put_schema");
        let mut state = self.array_state.lock().expect("lock");
        state
            .create_array(&self.storage, name, schema)
            .expect("create_array");
    }

    /// Schema HLC for the named array (panics if not registered).
    pub fn schema_hlc(&self, name: &str) -> Hlc {
        self.schemas
            .schema_hlc(name)
            .expect("schema not registered")
    }

    /// Deliver a single op to the inbound dispatcher and return the outcome.
    pub fn deliver(&self, op: &ArrayOp) -> InboundOutcome {
        let payload = op_codec::encode_op(op).expect("encode_op");
        let msg = ArrayDeltaMsg {
            array: op.header.array.clone(),
            op_payload: payload,
        };
        self.inbound.handle_delta(&msg).expect("handle_delta")
    }

    /// Read coord AS-OF `as_of_ms` from the local engine state.
    ///
    /// Returns the first attribute value of the live cell, or `None` if the
    /// cell is absent, tombstoned, or erased.
    pub fn read_coord(&self, array: &str, coord_x: i64, as_of_ms: i64) -> Option<CellValue> {
        let state = self.array_state.lock().expect("lock");
        let cell = state
            .read_coord(
                &self.storage,
                array,
                &[CoordValue::Int64(coord_x)],
                as_of_ms,
            )
            .expect("read_coord");
        cell.and_then(|c| c.attrs.into_iter().next())
    }

    /// Flush buffered writes for the named array to storage.
    pub fn flush(&self, array: &str) {
        let mut state = self.array_state.lock().expect("lock");
        state.flush(&self.storage, array).expect("flush");
    }
}
