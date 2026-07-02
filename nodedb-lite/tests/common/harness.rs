//! In-process sync harness: one Lite node with shared inbound dispatcher,
//! outbound emitter, and engine state.
//!
//! Bypasses WebSocket transport; exercises wire-message handlers directly
//! against an in-memory pagedb store.

use std::sync::Arc;

use nodedb_array::schema::array_schema::ArraySchema;
use nodedb_array::sync::hlc::Hlc;
use nodedb_array::sync::op::ArrayOp;
use nodedb_array::sync::op_codec;
use nodedb_array::types::cell_value::value::CellValue;
use nodedb_array::types::coord::value::CoordValue;
use nodedb_lite::PagedbStorageMem;
use nodedb_lite::engine::array::engine::ArrayEngineState;
use nodedb_lite::sync::array::catchup::CatchupTracker;
use nodedb_lite::sync::array::inbound::apply::LiteApplyEngine;
use nodedb_lite::sync::array::inbound::dispatcher::ArrayInbound;
use nodedb_lite::sync::array::inbound::outcome::InboundOutcome;
use nodedb_lite::sync::array::op_log_store::KvOpLogStore;
use nodedb_lite::sync::array::outbound::ArrayOutbound;
use nodedb_lite::sync::array::pending::PendingQueue;
use nodedb_lite::sync::array::replica_state::ReplicaState;
use nodedb_lite::sync::array::schema_registry::SchemaRegistry;
use nodedb_types::sync::wire::array::ArrayDeltaMsg;

use super::schema::simple_schema;

/// Convenience constructor used by outbound-loop tests.
pub async fn make_outbound_harness() -> SyncHarness {
    SyncHarness::new_in_memory().await
}

pub struct SyncHarness {
    pub inbound: ArrayInbound<PagedbStorageMem>,
    pub outbound: ArrayOutbound<PagedbStorageMem>,
    pub schemas: Arc<SchemaRegistry<PagedbStorageMem>>,
    pub pending: Arc<PendingQueue<PagedbStorageMem>>,
    pub op_log: Arc<KvOpLogStore<PagedbStorageMem>>,
    pub storage: Arc<PagedbStorageMem>,
    /// Direct handle to the shared engine state for AS-OF queries in tests.
    pub array_state: Arc<tokio::sync::Mutex<ArrayEngineState>>,
    pub catchup: Arc<CatchupTracker<PagedbStorageMem>>,
}

impl SyncHarness {
    /// Create a harness backed by a fresh in-memory pagedb database.
    pub async fn new_in_memory() -> Self {
        let storage = Arc::new(
            PagedbStorageMem::open_in_memory()
                .await
                .expect("open_in_memory"),
        );
        Self::from_storage(storage).await
    }

    /// Create a harness backed by the given storage (allows durability tests).
    pub async fn from_storage(storage: Arc<PagedbStorageMem>) -> Self {
        let replica = Arc::new(
            ReplicaState::load_or_init(&*storage)
                .await
                .expect("load_or_init"),
        );
        let schemas = Arc::new(SchemaRegistry::new(
            Arc::clone(&storage),
            Arc::clone(&replica),
        ));
        let op_log = Arc::new(KvOpLogStore::new(Arc::clone(&storage)));
        let pending = Arc::new(PendingQueue::new(Arc::clone(&storage)));
        let array_state = Arc::new(tokio::sync::Mutex::new(ArrayEngineState::new()));

        let engine = Arc::new(
            LiteApplyEngine::new(
                Arc::clone(&storage),
                Arc::clone(&array_state),
                Arc::clone(&schemas),
                Arc::clone(&op_log),
            )
            .await,
        );
        let catchup = Arc::new(
            CatchupTracker::load(Arc::clone(&storage))
                .await
                .expect("catchup load"),
        );

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
    pub async fn create_array(&self, name: &str) {
        let schema = simple_schema(name);
        self.schemas
            .put_schema(name, &schema)
            .await
            .expect("put_schema");
        self.array_state
            .lock()
            .await
            .create_array(&self.storage, name, simple_schema(name))
            .await
            .expect("create_array");
    }

    /// Register a custom schema.
    pub async fn create_array_with_schema(&self, name: &str, schema: ArraySchema) {
        self.schemas
            .put_schema(name, &schema)
            .await
            .expect("put_schema");
        self.array_state
            .lock()
            .await
            .create_array(&self.storage, name, schema)
            .await
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
            producer_id: 0,
            epoch: 0,
            seq: 0,
        };
        self.inbound.handle_delta(&msg).expect("handle_delta")
    }

    /// Read coord AS-OF `as_of_ms` from the local engine state.
    ///
    /// Returns the first attribute value of the live cell, or `None` if the
    /// cell is absent, tombstoned, or erased.
    pub async fn read_coord(&self, array: &str, coord_x: i64, as_of_ms: i64) -> Option<CellValue> {
        let state = self.array_state.lock().await;
        let cell = state
            .read_coord(
                &self.storage,
                array,
                &[CoordValue::Int64(coord_x)],
                as_of_ms,
            )
            .await
            .expect("read_coord");
        cell.and_then(|c| c.attrs.into_iter().next())
    }

    /// Flush buffered writes for the named array to storage.
    pub async fn flush(&self, array: &str) {
        self.array_state
            .lock()
            .await
            .flush(&self.storage, array)
            .await
            .expect("flush");
    }
}
