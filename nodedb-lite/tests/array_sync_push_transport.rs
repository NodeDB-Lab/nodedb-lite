// SPDX-License-Identifier: Apache-2.0

//! Array push over a real WebSocket against a mock Origin.
//!
//! Each test runs the public `run_sync_loop` against a loopback mock Origin
//! that plays Origin's side of the push protocol:
//!
//! 1. Before an array's first delta in a session, the client sends the
//!    array's `ArraySchema`.
//! 2. The client sends one `ArrayDelta` per queued op, in HLC order, then a
//!    `PingPong` ping carrying the window's barrier token.
//! 3. Origin sends no frame for an applied delta and one `ArrayReject` for a
//!    refused one. It answers the ping with a pong after every earlier frame.
//! 4. On the pong the client retires every op Origin did not refuse.
//!
//! Most tests drive a real `NodeDbLite`, so ops come from `array_put_cell`
//! and the durable pending queue. The mock-delegate tests pin down frame
//! order across arrays.

#![cfg(feature = "sync")]

use std::sync::Arc;
use std::time::Duration;

use nodedb_array::sync::hlc::Hlc;
use nodedb_array::sync::op_codec;
use nodedb_array::types::cell_value::value::CellValue;
use nodedb_array::types::coord::value::CoordValue;
use nodedb_lite::sync::array::PendingArrayOp;
use nodedb_lite::sync::{SyncClient, SyncConfig, SyncDelegate, run_sync_loop};
use nodedb_lite::{NodeDbLite, PagedbStorageMem};
use nodedb_types::sync::wire::{
    ArrayDeltaMsg, ArrayRejectMsg, ArrayRejectReason, ArraySchemaSyncMsg, PingPongMsg,
    SyncMessageType,
};

pub mod common;

use common::array_wire::open_lite_with_array;
use common::clock::{hlc, replica};
use common::mock_delegate::MockDelegate;
use common::ops::put_op;
use common::ws_origin::{
    MockOrigin, OriginSocket, await_until, collect_frames_for, next_frame, send_frame,
};

/// Window for negative assertions on outbound traffic. The push loop ticks
/// every 100ms, so this covers several ticks.
const PUSH_OBSERVATION_WINDOW: Duration = Duration::from_millis(600);

/// Set on every barrier token. `ping_loop` pings carry wall-clock
/// milliseconds, which never set it.
const BARRIER_TOKEN_TAG: u64 = 1 << 63;

const ARRAY: &str = "grid";

/// Aborts the sync loop when the test ends.
struct LoopGuard(tokio::task::JoinHandle<()>);

impl Drop for LoopGuard {
    fn drop(&mut self) {
        self.0.abort();
    }
}

fn make_client(origin: &MockOrigin) -> Arc<SyncClient> {
    let mut config = SyncConfig::new(origin.url(), "test.jwt.token");
    config.min_backoff = Duration::from_millis(50);
    Arc::new(SyncClient::new(config))
}

fn spawn_loop<D: SyncDelegate>(client: &Arc<SyncClient>, delegate: Arc<D>) -> LoopGuard {
    let delegate: Arc<dyn SyncDelegate> = delegate;
    LoopGuard(tokio::spawn(run_sync_loop(Arc::clone(client), delegate)))
}

/// One array push window as Origin receives it.
struct Window {
    schemas: Vec<ArraySchemaSyncMsg>,
    deltas: Vec<ArrayDeltaMsg>,
    token: u64,
}

/// Read frames up to and including the next barrier ping, skipping catch-up
/// requests and keepalive pings. Asserts every schema precedes every delta.
async fn read_window(ws: &mut OriginSocket) -> Window {
    let mut schemas = Vec::new();
    let mut deltas = Vec::new();
    loop {
        let frame = next_frame(ws)
            .await
            .expect("client closed before finishing its array window");
        match frame.msg_type {
            SyncMessageType::ArraySchema => {
                assert!(deltas.is_empty(), "ArraySchema must precede every delta");
                schemas.push(frame.decode_body().expect("decode ArraySchema"));
            }
            SyncMessageType::ArrayDelta => {
                deltas.push(frame.decode_body().expect("decode ArrayDelta"));
            }
            SyncMessageType::PingPong => {
                let ping: PingPongMsg = frame.decode_body().expect("decode PingPong");
                if !ping.is_pong && ping.timestamp_ms & BARRIER_TOKEN_TAG != 0 {
                    return Window {
                        schemas,
                        deltas,
                        token: ping.timestamp_ms,
                    };
                }
            }
            _ => {}
        }
    }
}

/// Answer a barrier ping the way Origin does: echo its token in a pong.
async fn answer_barrier(ws: &mut OriginSocket, token: u64) {
    let pong = PingPongMsg {
        timestamp_ms: token,
        is_pong: true,
    };
    send_frame(ws, SyncMessageType::PingPong, &pong).await;
}

async fn send_reject(ws: &mut OriginSocket, delta: &ArrayDeltaMsg, reason: ArrayRejectReason) {
    let reject = ArrayRejectMsg {
        array: delta.array.clone(),
        op_hlc_bytes: op_hlc(delta).to_bytes(),
        reason,
        detail: "refused by the mock Origin".into(),
    };
    send_frame(ws, SyncMessageType::ArrayReject, &reject).await;
}

fn op_hlc(delta: &ArrayDeltaMsg) -> Hlc {
    op_codec::decode_op(&delta.op_payload)
        .expect("decode op payload")
        .header
        .hlc
}

fn hlcs(deltas: &[ArrayDeltaMsg]) -> Vec<Hlc> {
    deltas.iter().map(op_hlc).collect()
}

fn seqs(deltas: &[ArrayDeltaMsg]) -> Vec<u64> {
    deltas.iter().map(|d| d.seq).collect()
}

async fn put(lite: &NodeDbLite<PagedbStorageMem>, x: i64) {
    lite.array_put_cell(
        ARRAY,
        vec![CoordValue::Int64(x)],
        vec![CellValue::Float64(x as f64)],
        x * 1_000,
        0,
        i64::MAX,
    )
    .await
    .expect("array_put_cell");
}

async fn queued(lite: &NodeDbLite<PagedbStorageMem>) -> Vec<PendingArrayOp> {
    SyncDelegate::pending_array_ops(lite, None, usize::MAX)
        .await
        .expect("read the array pending queue")
}

async fn await_queue_len(lite: &NodeDbLite<PagedbStorageMem>, len: usize) {
    await_until(
        move || async move { queued(lite).await.len() == len },
        "the array pending queue to settle",
    )
    .await;
}

fn assert_no_push_frames(frames: &[nodedb_types::sync::wire::SyncFrame]) {
    for frame in frames {
        assert!(
            !matches!(
                frame.msg_type,
                SyncMessageType::ArraySchema | SyncMessageType::ArrayDelta
            ),
            "unexpected {:?}",
            frame.msg_type
        );
        if frame.msg_type == SyncMessageType::PingPong {
            let ping: PingPongMsg = frame.decode_body().expect("decode PingPong");
            assert_eq!(
                ping.timestamp_ms & BARRIER_TOKEN_TAG,
                0,
                "no barrier ping without deltas"
            );
        }
    }
}

// ── Real replica ────────────────────────────────────────────────────────────

#[tokio::test(flavor = "multi_thread")]
async fn nothing_is_sent_when_the_array_queue_is_empty() {
    let origin = MockOrigin::bind().await;
    let client = make_client(&origin);
    let lite = open_lite_with_array(ARRAY).await;
    let _guard = spawn_loop(&client, lite);

    let mut ws = origin.accept_handshaked().await;
    let frames = collect_frames_for(&mut ws, PUSH_OBSERVATION_WINDOW).await;
    assert_no_push_frames(&frames);
}

#[tokio::test(flavor = "multi_thread")]
async fn schema_is_announced_before_the_first_delta() {
    let origin = MockOrigin::bind().await;
    let client = make_client(&origin);
    let lite = open_lite_with_array(ARRAY).await;
    put(&lite, 1).await;
    let _guard = spawn_loop(&client, Arc::clone(&lite));

    let mut ws = origin.accept_handshaked().await;
    let window = read_window(&mut ws).await;
    assert_eq!(
        window.schemas.len(),
        1,
        "one announce per array per session"
    );
    let schema = &window.schemas[0];
    assert_eq!(schema.array, ARRAY);
    let schema_hlc = lite.array_schema_hlc(ARRAY).expect("schema_hlc");
    assert_eq!(schema.schema_hlc_bytes, schema_hlc.to_bytes());
    assert!(!schema.snapshot_payload.is_empty());

    let op = op_codec::decode_op(&window.deltas[0].op_payload).expect("decode op");
    assert_eq!(
        op.header.schema_hlc, schema_hlc,
        "the op names the announced schema"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn deltas_drain_in_order_and_retire_on_the_barrier_pong() {
    let origin = MockOrigin::bind().await;
    let client = make_client(&origin);
    let lite = open_lite_with_array(ARRAY).await;
    for x in 1..=3 {
        put(&lite, x).await;
    }
    let expected: Vec<Hlc> = queued(&lite).await.iter().map(|op| op.hlc).collect();
    let _guard = spawn_loop(&client, Arc::clone(&lite));

    let mut ws = origin.accept_handshaked().await;
    let window = read_window(&mut ws).await;
    assert_eq!(hlcs(&window.deltas), expected, "deltas go out in HLC order");
    for delta in &window.deltas {
        assert_eq!(delta.array, ARRAY);
        assert_eq!(
            (delta.producer_id, delta.epoch),
            (1, 1),
            "stamped with the handshake identity"
        );
    }
    let sent_seqs = seqs(&window.deltas);
    assert!(
        sent_seqs.windows(2).all(|w| w[0] < w[1]) && sent_seqs[0] > 0,
        "each op gets a fresh stream seq: {sent_seqs:?}"
    );
    assert_eq!(
        queued(&lite).await.len(),
        3,
        "nothing retires before the pong"
    );

    answer_barrier(&mut ws, window.token).await;
    await_queue_len(&lite, 0).await;
    let frames = collect_frames_for(&mut ws, PUSH_OBSERVATION_WINDOW).await;
    assert_no_push_frames(&frames);
}

#[tokio::test(flavor = "multi_thread")]
async fn array_unknown_reject_resends_the_schema_then_the_op() {
    let origin = MockOrigin::bind().await;
    let client = make_client(&origin);
    let lite = open_lite_with_array(ARRAY).await;
    put(&lite, 1).await;
    let _guard = spawn_loop(&client, Arc::clone(&lite));

    let mut ws = origin.accept_handshaked().await;
    let first = read_window(&mut ws).await;
    send_reject(&mut ws, &first.deltas[0], ArrayRejectReason::ArrayUnknown).await;
    answer_barrier(&mut ws, first.token).await;

    let retry = read_window(&mut ws).await;
    assert_eq!(retry.schemas.len(), 1, "the schema goes out again");
    assert_eq!(hlcs(&retry.deltas), hlcs(&first.deltas), "then the same op");
    assert_eq!(
        seqs(&retry.deltas),
        seqs(&first.deltas),
        "with the same seq"
    );
    assert_eq!(queued(&lite).await.len(), 1, "the refused op stayed queued");

    answer_barrier(&mut ws, retry.token).await;
    await_queue_len(&lite, 0).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn unacked_ops_are_resent_after_reconnect() {
    let origin = MockOrigin::bind().await;
    let client = make_client(&origin);
    let lite = open_lite_with_array(ARRAY).await;
    put(&lite, 1).await;
    put(&lite, 2).await;
    let _guard = spawn_loop(&client, Arc::clone(&lite));

    // The session ends before Origin answers the barrier.
    let mut ws = origin.accept_handshaked().await;
    let first = read_window(&mut ws).await;
    let _ = ws.close(None).await;

    let mut ws = origin.accept_handshaked().await;
    let resent = read_window(&mut ws).await;
    assert_eq!(resent.schemas.len(), 1, "a new session announces again");
    assert_eq!(hlcs(&resent.deltas), hlcs(&first.deltas));
    assert_eq!(
        seqs(&resent.deltas),
        seqs(&first.deltas),
        "a re-send reuses the persisted seq"
    );

    answer_barrier(&mut ws, resent.token).await;
    await_queue_len(&lite, 0).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn engine_rejected_op_is_held_until_the_next_session() {
    let origin = MockOrigin::bind().await;
    let client = make_client(&origin);
    let lite = open_lite_with_array(ARRAY).await;
    put(&lite, 1).await;
    put(&lite, 2).await;
    let _guard = spawn_loop(&client, Arc::clone(&lite));

    let mut ws = origin.accept_handshaked().await;
    let first = read_window(&mut ws).await;
    let refused = op_hlc(&first.deltas[0]);
    send_reject(&mut ws, &first.deltas[0], ArrayRejectReason::EngineRejected).await;
    answer_barrier(&mut ws, first.token).await;

    await_queue_len(&lite, 1).await;
    assert_eq!(
        queued(&lite).await[0].hlc,
        refused,
        "only the refused op stays"
    );
    let frames = collect_frames_for(&mut ws, PUSH_OBSERVATION_WINDOW).await;
    assert_no_push_frames(&frames);
    let _ = ws.close(None).await;

    let mut ws = origin.accept_handshaked().await;
    let retry = read_window(&mut ws).await;
    assert_eq!(hlcs(&retry.deltas), vec![refused]);
    answer_barrier(&mut ws, retry.token).await;
    await_queue_len(&lite, 0).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn shape_invalid_reject_drops_the_op() {
    let origin = MockOrigin::bind().await;
    let client = make_client(&origin);
    let lite = open_lite_with_array(ARRAY).await;
    put(&lite, 1).await;
    let _guard = spawn_loop(&client, Arc::clone(&lite));

    let mut ws = origin.accept_handshaked().await;
    let window = read_window(&mut ws).await;
    send_reject(&mut ws, &window.deltas[0], ArrayRejectReason::ShapeInvalid).await;
    answer_barrier(&mut ws, window.token).await;

    await_queue_len(&lite, 0).await;
    let frames = collect_frames_for(&mut ws, PUSH_OBSERVATION_WINDOW).await;
    assert_no_push_frames(&frames);
}

// ── Recording delegate ──────────────────────────────────────────────────────

fn mock_op(array: &str, ms: u64) -> PendingArrayOp {
    let op = put_op(array, 1, 1.0, ms, hlc(1, replica(7)), replica(7));
    PendingArrayOp {
        array: array.into(),
        hlc: op.header.hlc,
        op_payload: op_codec::encode_op(&op).expect("encode op"),
        seq: 0,
    }
}

fn mock_schema(array: &str) -> ArraySchemaSyncMsg {
    ArraySchemaSyncMsg {
        array: array.into(),
        replica_id: 7,
        schema_hlc_bytes: hlc(1, replica(7)).to_bytes(),
        snapshot_payload: vec![1, 2, 3],
    }
}

#[tokio::test]
async fn nothing_is_sent_when_the_delegate_queue_is_empty() {
    let origin = MockOrigin::bind().await;
    let client = make_client(&origin);
    let mock = Arc::new(MockDelegate::new());
    mock.array.set_schema(mock_schema("a"));
    let _guard = spawn_loop(&client, Arc::clone(&mock));

    let mut ws = origin.accept_handshaked().await;
    let frames = collect_frames_for(&mut ws, PUSH_OBSERVATION_WINDOW).await;
    assert_no_push_frames(&frames);
    assert!(mock.array.retired().is_empty());
}

#[tokio::test]
async fn every_array_schema_precedes_the_window_deltas() {
    let origin = MockOrigin::bind().await;
    let client = make_client(&origin);
    let mock = Arc::new(MockDelegate::new());
    mock.array.set_schema(mock_schema("a"));
    mock.array.set_schema(mock_schema("b"));
    let ops = vec![mock_op("a", 10), mock_op("b", 20), mock_op("a", 30)];
    let expected: Vec<Hlc> = ops.iter().map(|op| op.hlc).collect();
    mock.array.enqueue(ops);
    let _guard = spawn_loop(&client, Arc::clone(&mock));

    let mut ws = origin.accept_handshaked().await;
    let window = read_window(&mut ws).await;
    let announced: Vec<&str> = window.schemas.iter().map(|s| s.array.as_str()).collect();
    assert_eq!(announced, vec!["a", "b"], "in first-appearance order");
    assert_eq!(hlcs(&window.deltas), expected);

    answer_barrier(&mut ws, window.token).await;
    let reader = Arc::clone(&mock);
    await_until(
        move || {
            let reader = Arc::clone(&reader);
            async move { reader.array.queued().is_empty() }
        },
        "the window to retire",
    )
    .await;
    assert_eq!(mock.array.retired(), expected);
}
