// SPDX-License-Identifier: Apache-2.0

//! Transport routing, end to end over a real WebSocket.
//!
//! Each test stands up a mock Origin on loopback, runs the public
//! `run_sync_loop` against it, and asserts that a frame put on the wire
//! reaches the right `SyncDelegate` callback — or, for the outbound
//! direction, that the client puts the right frames on the wire in the right
//! order. Nothing here reaches into the transport's private dispatch
//! functions, so the handshake, receive loop, and push loop are all exercised
//! as a client actually uses them.

use std::sync::Arc;
use std::time::Duration;

use nodedb_lite::engine::crdt::engine::PendingDelta;
use nodedb_lite::nodedb::CollectionMeta;
use nodedb_lite::sync::{SyncClient, SyncConfig, SyncDelegate, run_sync_loop};
use nodedb_types::sync::wire::SyncMessageType;

mod common;

use common::mock_delegate::MockDelegate;
use common::ws_origin::{MockOrigin, await_until, collect_frames_for, send_frame};

/// Window for observing outbound traffic. The push loop ticks every 100ms, so
/// this covers several ticks — enough for a missing per-session dedup to show
/// up as a repeated announce.
const PUSH_OBSERVATION_WINDOW: Duration = Duration::from_millis(600);

/// Aborts the sync loop when the test ends, so a client that is mid-reconnect
/// does not outlive its mock Origin.
struct LoopGuard(tokio::task::JoinHandle<()>);

impl Drop for LoopGuard {
    fn drop(&mut self) {
        self.0.abort();
    }
}

fn make_client(origin: &MockOrigin) -> Arc<SyncClient> {
    Arc::new(SyncClient::new(
        SyncConfig::new(origin.url(), "test.jwt.token"),
        1,
    ))
}

fn spawn_loop(client: &Arc<SyncClient>, mock: &Arc<MockDelegate>) -> LoopGuard {
    let delegate: Arc<dyn SyncDelegate> = Arc::clone(mock) as _;
    LoopGuard(tokio::spawn(run_sync_loop(Arc::clone(client), delegate)))
}

fn delta_ack(
    mutation_id: u64,
    status: nodedb_types::sync::wire::AckStatus,
) -> nodedb_types::sync::wire::DeltaAckMsg {
    nodedb_types::sync::wire::DeltaAckMsg {
        mutation_id,
        lsn: 100,
        clock_skew_warning_ms: None,
        applied_seq: 0,
        status,
    }
}

fn delta_reject(mutation_id: u64) -> nodedb_types::sync::wire::DeltaRejectMsg {
    nodedb_types::sync::wire::DeltaRejectMsg {
        mutation_id,
        reason: "unique violation".into(),
        compensation: None,
    }
}

#[tokio::test]
async fn dispatch_delta_ack() {
    let origin = MockOrigin::bind().await;
    let client = make_client(&origin);
    let mock = Arc::new(MockDelegate::new());
    let _guard = spawn_loop(&client, &mock);

    let mut ws = origin.accept_handshaked().await;
    send_frame(
        &mut ws,
        SyncMessageType::DeltaAck,
        &delta_ack(42, nodedb_types::sync::wire::AckStatus::Applied),
    )
    .await;

    let recorded = mock.as_ref();
    await_until(
        move || async move { recorded.acked_up_to() == 42 },
        "an Applied DeltaAck to retire mutation 42",
    )
    .await;
}

#[tokio::test]
async fn dispatch_delta_reject() {
    let origin = MockOrigin::bind().await;
    let client = make_client(&origin);
    let mock = Arc::new(MockDelegate::new());
    let _guard = spawn_loop(&client, &mock);

    let mut ws = origin.accept_handshaked().await;
    send_frame(&mut ws, SyncMessageType::DeltaReject, &delta_reject(7)).await;

    let recorded = mock.as_ref();
    await_until(
        move || async move { recorded.rejected() == vec![7] },
        "a DeltaReject to reach the delegate",
    )
    .await;
}

/// A `Gap` ack means Origin did NOT apply the delta — it expected a different
/// sequence number and skipped this frame entirely. Acknowledging it discards
/// the pending delta, so the write is lost with only a warning logged.
///
/// A non-applied ack must not retire the delta from the pending queue.
#[tokio::test]
async fn gap_ack_does_not_retire_the_unapplied_delta() {
    let origin = MockOrigin::bind().await;
    let client = make_client(&origin);
    let mock = Arc::new(MockDelegate::new());
    let _guard = spawn_loop(&client, &mock);

    let mut ws = origin.accept_handshaked().await;
    send_frame(
        &mut ws,
        SyncMessageType::DeltaAck,
        &delta_ack(
            42,
            nodedb_types::sync::wire::AckStatus::Gap { expected: 41 },
        ),
    )
    .await;
    // Frames are dispatched in arrival order, so once this later frame's
    // effect is visible the Gap ack has certainly been handled — the absence
    // of an acknowledge is then a real absence, not a race.
    send_frame(&mut ws, SyncMessageType::DeltaReject, &delta_reject(99)).await;

    let recorded = mock.as_ref();
    await_until(
        move || async move { recorded.rejected() == vec![99] },
        "the frame following the Gap ack to be dispatched",
    )
    .await;

    assert_eq!(
        mock.acked_up_to(),
        0,
        "a Gap ack retired a delta Origin never applied; the write is lost"
    );
}

/// A `Fenced` ack means this producer's epoch was rejected — the delta was not
/// applied and the client must re-establish its producer identity, not throw
/// the write away.
#[tokio::test]
async fn fenced_ack_does_not_retire_the_unapplied_delta() {
    let origin = MockOrigin::bind().await;
    let client = make_client(&origin);
    let mock = Arc::new(MockDelegate::new());
    let _guard = spawn_loop(&client, &mock);

    let mut ws = origin.accept_handshaked().await;
    send_frame(
        &mut ws,
        SyncMessageType::DeltaAck,
        &delta_ack(7, nodedb_types::sync::wire::AckStatus::Fenced),
    )
    .await;

    // Fencing flips the client's own flag, which is the observable proof the
    // ack was dispatched before the acked-up-to assertion below.
    let fenced = client.as_ref();
    await_until(
        move || async move { fenced.is_fenced() },
        "the Fenced ack to fence the producer",
    )
    .await;

    assert_eq!(
        mock.acked_up_to(),
        0,
        "a Fenced ack retired a delta Origin never applied; the write is lost"
    );
}

#[tokio::test]
async fn dispatch_shape_delta_imports() {
    let origin = MockOrigin::bind().await;
    let client = make_client(&origin);
    let mock = Arc::new(MockDelegate::new());

    // Subscribed before the loop starts so the subscription is already in the
    // handshake the mock Origin answers.
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

    let _guard = spawn_loop(&client, &mock);

    let mut ws = origin.accept_handshaked().await;
    let delta = nodedb_types::sync::wire::ShapeDeltaMsg {
        shape_id: "s1".into(),
        collection: "orders".into(),
        document_id: "o1".into(),
        operation: "INSERT".into(),
        delta: vec![1, 2, 3],
        lsn: 50,
    };
    send_frame(&mut ws, SyncMessageType::ShapeDelta, &delta).await;

    let recorded = mock.as_ref();
    await_until(
        move || async move { recorded.imported().len() == 1 },
        "a ShapeDelta to be imported",
    )
    .await;

    assert_eq!(mock.imported()[0], ("orders".to_string(), vec![1, 2, 3]));

    let shapes = client.shapes().lock().await;
    assert_eq!(
        shapes
            .get("s1")
            .expect("subscription still present")
            .last_lsn,
        50
    );
}

#[tokio::test]
async fn dispatch_clock_sync() {
    let origin = MockOrigin::bind().await;
    let client = make_client(&origin);
    let mock = Arc::new(MockDelegate::new());
    let _guard = spawn_loop(&client, &mock);

    let mut ws = origin.accept_handshaked().await;
    let clock_msg = nodedb_types::sync::wire::VectorClockSyncMsg {
        clocks: {
            let mut m = std::collections::HashMap::new();
            m.insert("0000000000000001".to_string(), 99u64);
            m
        },
        sender_id: 0,
    };
    send_frame(&mut ws, SyncMessageType::VectorClockSync, &clock_msg).await;

    let synced = client.as_ref();
    await_until(
        move || async move { synced.clock().lock().await.get(1) == 99 },
        "a VectorClockSync to advance the local clock",
    )
    .await;
}

#[tokio::test]
async fn dispatch_collection_schema() {
    let origin = MockOrigin::bind().await;
    let client = make_client(&origin);
    let mock = Arc::new(MockDelegate::new());
    let _guard = spawn_loop(&client, &mock);

    let mut ws = origin.accept_handshaked().await;
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
    send_frame(&mut ws, SyncMessageType::CollectionSchema, &msg).await;

    let recorded = mock.as_ref();
    await_until(
        move || async move { recorded.imported_schemas() == vec!["users".to_string()] },
        "a CollectionSchema to be imported",
    )
    .await;
}

/// Origin sends server-originated writes as `RowPush`, carrying a row
/// post-image rather than Loro update bytes. Before this path existed the
/// frame hit the dispatcher's catch-all and was discarded as "unexpected
/// frame type from Origin", so nothing written on the server ever reached the
/// device.
#[tokio::test]
async fn dispatch_row_push_applies_the_row() {
    let origin = MockOrigin::bind().await;
    let client = make_client(&origin);
    let mock = Arc::new(MockDelegate::new());
    let _guard = spawn_loop(&client, &mock);

    let mut ws = origin.accept_handshaked().await;
    let msg = nodedb_types::sync::wire::RowPushMsg {
        collection: "orders".into(),
        document_id: "o-1".into(),
        payload: vec![0x80], // empty msgpack map
        op: nodedb_types::sync::wire::RowOp::Upsert,
        lsn: 7,
        peer_id: 1,
        sequence: 3,
    };
    send_frame(&mut ws, SyncMessageType::RowPush, &msg).await;

    let recorded = mock.as_ref();
    await_until(
        move || async move { !recorded.applied_rows().is_empty() },
        "a RowPush to be applied",
    )
    .await;

    assert_eq!(
        mock.applied_rows(),
        vec![(
            "orders".to_string(),
            "o-1".to_string(),
            nodedb_types::sync::wire::RowOp::Upsert
        )],
        "a RowPush frame must be applied, not discarded as an unknown type"
    );
}

/// A delete carries an empty payload and must be applied as a removal, not
/// inferred from the payload being empty.
#[tokio::test]
async fn dispatch_row_push_carries_delete_explicitly() {
    let origin = MockOrigin::bind().await;
    let client = make_client(&origin);
    let mock = Arc::new(MockDelegate::new());
    let _guard = spawn_loop(&client, &mock);

    let mut ws = origin.accept_handshaked().await;
    let msg = nodedb_types::sync::wire::RowPushMsg {
        collection: "orders".into(),
        document_id: "o-2".into(),
        payload: Vec::new(),
        op: nodedb_types::sync::wire::RowOp::Delete,
        lsn: 8,
        peer_id: 1,
        sequence: 4,
    };
    send_frame(&mut ws, SyncMessageType::RowPush, &msg).await;

    let recorded = mock.as_ref();
    await_until(
        move || async move { !recorded.applied_rows().is_empty() },
        "a RowPush delete to be applied",
    )
    .await;

    let applied = mock.applied_rows();
    assert_eq!(applied.len(), 1);
    assert_eq!(applied[0].2, nodedb_types::sync::wire::RowOp::Delete);
}

/// A `CollectionSchema` (0x13) frame for a collection must be sent before the
/// first `DeltaPush` frame for that collection, and later push ticks must NOT
/// re-announce it (per-session dedup via `announced_collections`).
#[tokio::test]
async fn collection_schema_announced_before_first_delta_and_deduped() {
    let origin = MockOrigin::bind().await;
    let client = make_client(&origin);
    let mock = Arc::new(MockDelegate::new());

    mock.set_collection_meta(
        "widgets",
        CollectionMeta {
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
    // Never acknowledged, so the collection still has pending work on every
    // subsequent tick — the announce would repeat if it were not deduped.
    mock.set_pending(vec![PendingDelta {
        mutation_id: 1,
        collection: "widgets".to_string(),
        document_id: "d1".to_string(),
        delta_bytes: vec![9, 9, 9],
        seq: 0,
    }]);

    let _guard = spawn_loop(&client, &mock);

    let mut ws = origin.accept_handshaked().await;
    let frames = collect_frames_for(&mut ws, PUSH_OBSERVATION_WINDOW).await;

    assert!(
        frames.len() >= 2,
        "expected at least a schema frame followed by a delta frame, got {}",
        frames.len()
    );
    assert_eq!(frames[0].msg_type, SyncMessageType::CollectionSchema);
    assert_eq!(frames[1].msg_type, SyncMessageType::DeltaPush);

    let schema_count = frames
        .iter()
        .filter(|f| f.msg_type == SyncMessageType::CollectionSchema)
        .count();
    assert_eq!(
        schema_count, 1,
        "collection must be announced only once per session"
    );
}
