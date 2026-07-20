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
    imported_schemas: std::sync::Mutex<Vec<String>>,
    pending: std::sync::Mutex<Vec<PendingDelta>>,
    collection_metas: std::sync::Mutex<
        std::collections::HashMap<String, crate::nodedb::collection::CollectionMeta>,
    >,
}

impl MockDelegate {
    fn new() -> Self {
        Self {
            acked_up_to: AtomicU64::new(0),
            rejected: std::sync::Mutex::new(Vec::new()),
            imported: std::sync::Mutex::new(Vec::new()),
            imported_schemas: std::sync::Mutex::new(Vec::new()),
            pending: std::sync::Mutex::new(Vec::new()),
            collection_metas: std::sync::Mutex::new(std::collections::HashMap::new()),
        }
    }
}

#[async_trait::async_trait]
impl SyncDelegate for MockDelegate {
    fn pending_deltas(&self) -> Vec<PendingDelta> {
        self.pending.lock().unwrap().clone()
    }
    fn acknowledge(&self, mutation_id: u64) {
        self.acked_up_to.store(mutation_id, Ordering::Relaxed);
    }
    async fn set_pending_delta_seq(&self, _mutation_id: u64, _seq: u64) {}
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
    async fn import_collection_schema(
        &self,
        msg: &nodedb_types::sync::wire::CollectionSchemaSyncMsg,
    ) {
        self.imported_schemas
            .lock()
            .unwrap()
            .push(msg.descriptor.name.clone());
    }
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

    async fn pending_columnar_batches(&self) -> Vec<(Vec<u8>, PendingColumnarBatch)> {
        Vec::new()
    }
    async fn mark_columnar_batch_in_flight(&self, _batch_id: u64, _durable_key: Vec<u8>) {}
    async fn ack_columnar_batch_in_flight(&self, _batch_id: u64) {}
    async fn acknowledge_columnar_batch(&self, _durable_key: Vec<u8>) {}

    async fn pending_vector_inserts(&self) -> Vec<(Vec<u8>, PendingVectorInsert)> {
        Vec::new()
    }
    async fn mark_vector_insert_in_flight(&self, _batch_id: u64, _durable_key: Vec<u8>) {}
    async fn ack_vector_insert_in_flight(&self, _batch_id: u64) {}
    async fn acknowledge_vector_insert(&self, _durable_key: Vec<u8>) {}

    async fn pending_vector_deletes(&self) -> Vec<(Vec<u8>, PendingVectorDelete)> {
        Vec::new()
    }
    async fn mark_vector_delete_in_flight(&self, _batch_id: u64, _durable_key: Vec<u8>) {}
    async fn ack_vector_delete_in_flight(&self, _batch_id: u64) {}
    async fn acknowledge_vector_delete(&self, _durable_key: Vec<u8>) {}

    async fn pending_fts_indexes(&self) -> Vec<(Vec<u8>, PendingFtsIndex)> {
        Vec::new()
    }
    async fn mark_fts_index_in_flight(&self, _batch_id: u64, _durable_key: Vec<u8>) {}
    async fn ack_fts_index_in_flight(&self, _batch_id: u64) {}
    async fn acknowledge_fts_index(&self, _durable_key: Vec<u8>) {}

    async fn pending_fts_deletes(&self) -> Vec<(Vec<u8>, PendingFtsDelete)> {
        Vec::new()
    }
    async fn mark_fts_delete_in_flight(&self, _batch_id: u64, _durable_key: Vec<u8>) {}
    async fn ack_fts_delete_in_flight(&self, _batch_id: u64) {}
    async fn acknowledge_fts_delete(&self, _durable_key: Vec<u8>) {}

    async fn pending_spatial_inserts(&self) -> Vec<(Vec<u8>, PendingSpatialInsert)> {
        Vec::new()
    }
    async fn mark_spatial_insert_in_flight(&self, _batch_id: u64, _durable_key: Vec<u8>) {}
    async fn ack_spatial_insert_in_flight(&self, _batch_id: u64) {}
    async fn acknowledge_spatial_insert(&self, _durable_key: Vec<u8>) {}

    async fn pending_spatial_deletes(&self) -> Vec<(Vec<u8>, PendingSpatialDelete)> {
        Vec::new()
    }
    async fn mark_spatial_delete_in_flight(&self, _batch_id: u64, _durable_key: Vec<u8>) {}
    async fn ack_spatial_delete_in_flight(&self, _batch_id: u64) {}
    async fn acknowledge_spatial_delete(&self, _durable_key: Vec<u8>) {}

    async fn pending_timeseries_batches(&self) -> Vec<(Vec<u8>, PendingTimeseriesBatch)> {
        Vec::new()
    }
    async fn mark_timeseries_batch_in_flight(&self, _stream_seq: u64, _durable_key: Vec<u8>) {}
    async fn ack_timeseries_batches_through_seq(&self, _applied_seq: u64) {}
    async fn acknowledge_timeseries_batch(&self, _durable_key: Vec<u8>) {}
    async fn clear_engine_in_flight(&self) {}

    async fn persist_producer_state(&self, _producer_id: u64, _accepted_epoch: u64) {}
    async fn load_producer_state(&self) -> (u64, u64) {
        (0, 0)
    }
    async fn next_stream_seq(&self, _stream_id: u64) -> u64 {
        0
    }
    async fn record_stream_ack(&self, _stream_id: u64, _applied_seq: u64) {}

    async fn get_collection_meta(
        &self,
        name: &str,
    ) -> Option<crate::nodedb::collection::CollectionMeta> {
        self.collection_metas.lock().unwrap().get(name).cloned()
    }

    async fn persist_columnar_seq(
        &self,
        _key: &[u8],
        _batch: &PendingColumnarBatch,
    ) -> Result<(), crate::error::LiteError> {
        Ok(())
    }
    async fn persist_timeseries_seq(
        &self,
        _key: &[u8],
        _batch: &PendingTimeseriesBatch,
    ) -> Result<(), crate::error::LiteError> {
        Ok(())
    }
    async fn persist_vector_insert_seq(
        &self,
        _key: &[u8],
        _insert: &PendingVectorInsert,
    ) -> Result<(), crate::error::LiteError> {
        Ok(())
    }
    async fn persist_vector_delete_seq(
        &self,
        _key: &[u8],
        _delete: &PendingVectorDelete,
    ) -> Result<(), crate::error::LiteError> {
        Ok(())
    }
    async fn persist_fts_index_seq(
        &self,
        _key: &[u8],
        _entry: &PendingFtsIndex,
    ) -> Result<(), crate::error::LiteError> {
        Ok(())
    }
    async fn persist_fts_delete_seq(
        &self,
        _key: &[u8],
        _entry: &PendingFtsDelete,
    ) -> Result<(), crate::error::LiteError> {
        Ok(())
    }
    async fn persist_spatial_insert_seq(
        &self,
        _key: &[u8],
        _insert: &PendingSpatialInsert,
    ) -> Result<(), crate::error::LiteError> {
        Ok(())
    }
    async fn persist_spatial_delete_seq(
        &self,
        _key: &[u8],
        _delete: &PendingSpatialDelete,
    ) -> Result<(), crate::error::LiteError> {
        Ok(())
    }
}

impl MockDelegate {
    fn set_pending(&self, deltas: Vec<PendingDelta>) {
        *self.pending.lock().unwrap() = deltas;
    }

    fn set_collection_meta(&self, name: &str, meta: crate::nodedb::collection::CollectionMeta) {
        self.collection_metas
            .lock()
            .unwrap()
            .insert(name.to_string(), meta);
    }
}

/// A `Sink<Message>` that captures every frame sent through it, for
/// asserting on wire-frame ordering without a real WebSocket.
#[derive(Default)]
struct CapturingSink {
    frames: std::sync::Mutex<Vec<tokio_tungstenite::tungstenite::Message>>,
}

impl futures::Sink<tokio_tungstenite::tungstenite::Message> for CapturingSink {
    type Error = std::convert::Infallible;

    fn poll_ready(
        self: std::pin::Pin<&mut Self>,
        _cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), Self::Error>> {
        std::task::Poll::Ready(Ok(()))
    }

    fn start_send(
        self: std::pin::Pin<&mut Self>,
        item: tokio_tungstenite::tungstenite::Message,
    ) -> Result<(), Self::Error> {
        self.frames.lock().unwrap().push(item);
        Ok(())
    }

    fn poll_flush(
        self: std::pin::Pin<&mut Self>,
        _cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), Self::Error>> {
        std::task::Poll::Ready(Ok(()))
    }

    fn poll_close(
        self: std::pin::Pin<&mut Self>,
        _cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), Self::Error>> {
        std::task::Poll::Ready(Ok(()))
    }
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
        applied_seq: 0,
        status: nodedb_types::sync::wire::AckStatus::Applied,
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

#[tokio::test]
async fn dispatch_collection_schema() {
    let client = make_client();
    let mock = Arc::new(MockDelegate::new());
    let delegate: Arc<dyn SyncDelegate> = Arc::clone(&mock) as _;

    let msg = nodedb_types::sync::wire::CollectionSchemaSyncMsg {
        descriptor: nodedb_types::sync::wire::CollectionDescriptor {
            tenant_id: 1,
            database_id: nodedb_types::id::DatabaseId::new(1),
            name: "users".into(),
            collection_type: nodedb_types::collection::CollectionType::document(),
            bitemporal: false,
            crdt: false,
            fields: Vec::new(),
            primary: nodedb_types::PrimaryEngine::Document,
            vector_primary: None,
            partition_strategy: nodedb_types::PartitionStrategy::default(),
            declared_primary_key: None,
            descriptor_version: 1,
        },
        creation_hlc: nodedb_types::hlc::Hlc::new(1, 0),
    };
    let frame =
        SyncFrame::try_encode(SyncMessageType::CollectionSchema, &msg).expect("test frame encode");

    dispatch_frame(&client, &delegate, &frame).await;

    assert_eq!(
        *mock.imported_schemas.lock().unwrap(),
        vec!["users".to_string()]
    );
}

/// A `CollectionSchema` (0x13) frame for a collection must be sent before
/// the first `DeltaPush` frame for that collection, and a second push tick
/// must NOT re-announce it (per-session dedup via `announced_collections`).
#[tokio::test]
async fn collection_schema_announced_before_first_delta_and_deduped() {
    let client = make_client();
    let mock = Arc::new(MockDelegate::new());
    let delegate: Arc<dyn SyncDelegate> = Arc::clone(&mock) as _;

    mock.set_collection_meta(
        "widgets",
        crate::nodedb::collection::CollectionMeta {
            name: "widgets".to_string(),
            collection_type: "document".to_string(),
            created_at_ms: 0,
            fields: Vec::new(),
            config_json: None,
            descriptor_json: None,
            bitemporal: false,
            crdt: false,
        },
    );
    mock.set_pending(vec![PendingDelta {
        mutation_id: 1,
        collection: "widgets".to_string(),
        document_id: "d1".to_string(),
        delta_bytes: vec![9, 9, 9],
        seq: 0,
    }]);

    let sink = Arc::new(tokio::sync::Mutex::new(CapturingSink::default()));

    assert!(
        !super::push::control::push_collection_schemas(&client, &delegate, &sink)
            .await
            .is_break()
    );
    assert!(
        !super::push::control::push_crdt_deltas(&client, &delegate, &sink)
            .await
            .is_break()
    );

    {
        let guard = sink.lock().await;
        let frames = guard.frames.lock().unwrap();
        assert_eq!(
            frames.len(),
            2,
            "expected one schema frame + one delta frame"
        );
        let schema_frame = SyncFrame::from_bytes(frames[0].clone().into_data().as_ref())
            .expect("schema frame decodes");
        assert_eq!(schema_frame.msg_type, SyncMessageType::CollectionSchema);
        let delta_frame = SyncFrame::from_bytes(frames[1].clone().into_data().as_ref())
            .expect("delta frame decodes");
        assert_eq!(delta_frame.msg_type, SyncMessageType::DeltaPush);
    }

    // Second push cycle: same collection still has a pending delta (it
    // wasn't acked), but must NOT be re-announced this session.
    assert!(
        !super::push::control::push_collection_schemas(&client, &delegate, &sink)
            .await
            .is_break()
    );

    let guard = sink.lock().await;
    let frames = guard.frames.lock().unwrap();
    let schema_count = frames
        .iter()
        .filter(|f| {
            SyncFrame::from_bytes((*f).clone().into_data().as_ref())
                .map(|frame| frame.msg_type == SyncMessageType::CollectionSchema)
                .unwrap_or(false)
        })
        .count();
    assert_eq!(
        schema_count, 1,
        "collection must be announced only once per session"
    );
}
