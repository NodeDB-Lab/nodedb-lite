//! Dispatch-table tests for the transport. The push and connect paths are
//! covered by the WebSocket integration tests in `tests/`.

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use nodedb_types::sync::wire::{SyncFrame, SyncMessageType};

use super::delegate::SyncDelegate;
use super::dispatch::dispatch_frame;
use crate::engine::crdt::engine::PendingDelta;
use crate::sync::client::SyncClient;
use crate::sync::outbound::columnar::PendingColumnarBatch;
use crate::sync::outbound::fts::{PendingFtsDelete, PendingFtsIndex};
use crate::sync::outbound::spatial::{PendingSpatialDelete, PendingSpatialInsert};
use crate::sync::outbound::timeseries::PendingTimeseriesBatch;
use crate::sync::outbound::vector::{PendingVectorDelete, PendingVectorInsert};

/// Mock delegate for testing (uses std::sync::Mutex, not tokio's).
struct MockDelegate {
    acked_up_to: AtomicU64,
    rejected: std::sync::Mutex<Vec<u64>>,
    imported: std::sync::Mutex<Vec<Vec<u8>>>,
}

impl MockDelegate {
    fn new() -> Self {
        Self {
            acked_up_to: AtomicU64::new(0),
            rejected: std::sync::Mutex::new(Vec::new()),
            imported: std::sync::Mutex::new(Vec::new()),
        }
    }
}

#[async_trait::async_trait]
impl SyncDelegate for MockDelegate {
    fn pending_deltas(&self) -> Vec<PendingDelta> {
        Vec::new()
    }
    fn acknowledge(&self, mutation_id: u64) {
        self.acked_up_to.store(mutation_id, Ordering::Relaxed);
    }
    fn reject(&self, mutation_id: u64) {
        self.rejected.lock().unwrap().push(mutation_id);
    }
    fn reject_with_policy(
        &self,
        mutation_id: u64,
        _hint: &nodedb_types::sync::compensation::CompensationHint,
    ) {
        self.rejected.lock().unwrap().push(mutation_id);
    }
    fn import_remote(&self, data: &[u8]) {
        self.imported.lock().unwrap().push(data.to_vec());
    }
    async fn import_definition(&self, _msg: &nodedb_types::sync::wire::DefinitionSyncMsg) {}
    fn handle_array_delta(
        &self,
        _msg: &nodedb_types::sync::wire::ArrayDeltaMsg,
    ) -> Option<nodedb_types::sync::wire::ArrayAckMsg> {
        None
    }
    fn handle_array_delta_batch(
        &self,
        _msg: &nodedb_types::sync::wire::ArrayDeltaBatchMsg,
    ) -> Option<nodedb_types::sync::wire::ArrayAckMsg> {
        None
    }
    fn handle_array_reject(&self, _msg: &nodedb_types::sync::wire::ArrayRejectMsg) {}

    fn pending_columnar_batches(&self) -> Vec<PendingColumnarBatch> {
        Vec::new()
    }
    fn acknowledge_columnar_batch(&self, _batch_id: u64) {}
    fn reject_columnar_batch(&self, _batch: PendingColumnarBatch) {}

    fn pending_vector_inserts(&self) -> Vec<PendingVectorInsert> {
        Vec::new()
    }
    fn acknowledge_vector_insert(&self, _batch_id: u64) {}
    fn reject_vector_insert(&self, _entry: PendingVectorInsert) {}

    fn pending_vector_deletes(&self) -> Vec<PendingVectorDelete> {
        Vec::new()
    }
    fn acknowledge_vector_delete(&self, _batch_id: u64) {}
    fn reject_vector_delete(&self, _entry: PendingVectorDelete) {}

    fn pending_fts_indexes(&self) -> Vec<PendingFtsIndex> {
        Vec::new()
    }
    fn acknowledge_fts_index(&self, _batch_id: u64) {}
    fn reject_fts_index(&self, _entry: PendingFtsIndex) {}

    fn pending_fts_deletes(&self) -> Vec<PendingFtsDelete> {
        Vec::new()
    }
    fn acknowledge_fts_delete(&self, _batch_id: u64) {}
    fn reject_fts_delete(&self, _entry: PendingFtsDelete) {}

    fn pending_spatial_inserts(&self) -> Vec<PendingSpatialInsert> {
        Vec::new()
    }
    fn acknowledge_spatial_insert(&self, _batch_id: u64) {}
    fn reject_spatial_insert(&self, _entry: PendingSpatialInsert) {}

    fn pending_spatial_deletes(&self) -> Vec<PendingSpatialDelete> {
        Vec::new()
    }
    fn acknowledge_spatial_delete(&self, _batch_id: u64) {}
    fn reject_spatial_delete(&self, _entry: PendingSpatialDelete) {}

    fn pending_timeseries_batches(&self) -> Vec<PendingTimeseriesBatch> {
        Vec::new()
    }
    fn acknowledge_timeseries_collection(&self, _collection: &str) {}
    fn reject_timeseries_batch(&self, _batch: PendingTimeseriesBatch) {}
}

fn make_client() -> Arc<SyncClient> {
    Arc::new(SyncClient::new(
        crate::sync::client::SyncConfig::new("wss://localhost/sync", "jwt"),
        1,
    ))
}

#[tokio::test]
async fn dispatch_delta_ack() {
    let client = make_client();
    let mock = Arc::new(MockDelegate::new());
    let delegate: Arc<dyn SyncDelegate> = Arc::clone(&mock) as _;

    let ack = nodedb_types::sync::wire::DeltaAckMsg {
        mutation_id: 42,
        lsn: 100,
        clock_skew_warning_ms: None,
    };
    let frame = SyncFrame::try_encode(SyncMessageType::DeltaAck, &ack).expect("test frame encode");

    dispatch_frame(&client, &delegate, &frame).await;
    assert_eq!(mock.acked_up_to.load(Ordering::Relaxed), 42);
}

#[tokio::test]
async fn dispatch_delta_reject() {
    let client = make_client();
    let mock = Arc::new(MockDelegate::new());
    let delegate: Arc<dyn SyncDelegate> = Arc::clone(&mock) as _;

    let reject = nodedb_types::sync::wire::DeltaRejectMsg {
        mutation_id: 7,
        reason: "unique violation".into(),
        compensation: None,
    };
    let frame =
        SyncFrame::try_encode(SyncMessageType::DeltaReject, &reject).expect("test frame encode");

    dispatch_frame(&client, &delegate, &frame).await;
    assert_eq!(*mock.rejected.lock().unwrap(), vec![7]);
}

#[tokio::test]
async fn dispatch_shape_delta_imports() {
    let client = make_client();
    let mock = Arc::new(MockDelegate::new());
    let delegate: Arc<dyn SyncDelegate> = Arc::clone(&mock) as _;

    {
        let mut shapes = client.shapes().lock().await;
        shapes.subscribe(nodedb_types::sync::shape::ShapeDefinition {
            shape_id: "s1".into(),
            tenant_id: 1,
            shape_type: nodedb_types::sync::shape::ShapeType::Document {
                collection: "orders".into(),
                predicate: Vec::new(),
            },
            description: "test".into(),
            field_filter: vec![],
        });
    }

    let delta = nodedb_types::sync::wire::ShapeDeltaMsg {
        shape_id: "s1".into(),
        collection: "orders".into(),
        document_id: "o1".into(),
        operation: "INSERT".into(),
        delta: vec![1, 2, 3],
        lsn: 50,
    };
    let frame =
        SyncFrame::try_encode(SyncMessageType::ShapeDelta, &delta).expect("test frame encode");

    dispatch_frame(&client, &delegate, &frame).await;

    {
        let imported = mock.imported.lock().unwrap();
        assert_eq!(imported.len(), 1);
        assert_eq!(imported[0], vec![1, 2, 3]);
    }

    let shapes = client.shapes().lock().await;
    assert_eq!(shapes.get("s1").unwrap().last_lsn, 50);
}

#[tokio::test]
async fn dispatch_clock_sync() {
    let client = make_client();
    let mock = Arc::new(MockDelegate::new());
    let delegate: Arc<dyn SyncDelegate> = Arc::clone(&mock) as _;

    let clock_msg = nodedb_types::sync::wire::VectorClockSyncMsg {
        clocks: {
            let mut m = std::collections::HashMap::new();
            m.insert("0000000000000001".to_string(), 99u64);
            m
        },
        sender_id: 0,
    };
    let frame = SyncFrame::try_encode(SyncMessageType::VectorClockSync, &clock_msg)
        .expect("test frame encode");

    dispatch_frame(&client, &delegate, &frame).await;

    let clock = client.clock().lock().await;
    assert_eq!(clock.get(1), 99);
}
