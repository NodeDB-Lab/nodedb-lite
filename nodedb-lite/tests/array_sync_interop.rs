//! Array sync against a real Origin over WebSocket.
//!
//! - Catch-up: a Lite replica that missed array ops while disconnected
//!   converges after it reconnects, through `ArrayCatchupRequest` and the
//!   `ArrayDeltaBatch` stream Origin's catch-up server answers with.
//! - Put round trip: a cell written on one Lite replica reaches Origin
//!   through the push loop (`ArraySchema`, then `ArrayDelta` frames closed
//!   by a barrier ping), and a second replica through Origin's catch-up
//!   stream.
//!
//! The catch-up test seeds Origin through a raw sync session that sends the
//! frames a writing replica sends: the array's schema snapshot, then its ops.
//! Each test spawns its own Origin with a fresh data directory, and skips
//! when the Origin binary is unavailable.

#![cfg(feature = "sync")]

pub mod common;

use std::sync::Arc;
use std::time::Duration;

use futures::{SinkExt, StreamExt};
use nodedb_array::sync::hlc::Hlc;
use nodedb_array::sync::op::ArrayOp;
use nodedb_array::types::cell_value::value::CellValue;
use nodedb_array::types::coord::value::CoordValue;
use nodedb_lite::sync::{SyncClient, SyncConfig, SyncDelegate, SyncState, run_sync_loop};
use nodedb_lite::{NodeDbLite, PagedbStorageMem};
use nodedb_types::sync::wire::array::{
    ArrayCatchupRequestMsg, ArrayDeltaBatchMsg, ArrayRejectMsg, ArraySchemaSyncMsg,
};
use nodedb_types::sync::wire::{SyncFrame, SyncMessageType};
use tokio_tungstenite::tungstenite::Message;

use common::array_wire::{delta_batch, open_lite_with_array, read_value, remote_put};
use common::origin::{ORIGIN_WS, OriginServer, OriginWs, connect_and_handshake};

/// Upper bound on Origin applying, persisting, and streaming ops.
const SYNC_DEADLINE: Duration = Duration::from_secs(10);

const ARRAY: &str = "interop_grid";

fn skip() {
    eprintln!("SKIP: Origin binary unavailable (set NODEDB_BIN or run via `cargo nextest`)");
}

async fn send<T: zerompk::ToMessagePack>(ws: &mut OriginWs, msg_type: SyncMessageType, msg: &T) {
    let bytes = SyncFrame::try_encode(msg_type, msg)
        .expect("encode frame")
        .to_bytes();
    ws.send(Message::Binary(bytes.into()))
        .await
        .expect("send frame to Origin");
}

/// Ask Origin for every op of `ARRAY` on the seeding session and wait until
/// the streamed batches carry `expected` ops. Returns once Origin's op log
/// holds them, so a replica's catch-up request observes all of them.
async fn await_origin_op_log(ws: &mut OriginWs, expected: usize) {
    let deadline = tokio::time::Instant::now() + SYNC_DEADLINE;
    loop {
        let request = ArrayCatchupRequestMsg {
            array: ARRAY.into(),
            from_hlc_bytes: Hlc::ZERO.to_bytes(),
        };
        send(ws, SyncMessageType::ArrayCatchupRequest, &request).await;

        let mut streamed = 0;
        let round_end = tokio::time::Instant::now() + Duration::from_millis(500);
        while let Ok(next) = tokio::time::timeout_at(round_end, ws.next()).await {
            let msg = next
                .expect("Origin closed the seeding session")
                .expect("read from Origin");
            let Some(frame) = SyncFrame::from_bytes(msg.into_data().as_ref()) else {
                continue;
            };
            match frame.msg_type {
                SyncMessageType::ArrayDeltaBatch => {
                    let batch: ArrayDeltaBatchMsg = frame.decode_body().expect("decode batch");
                    streamed += batch.op_payloads.len();
                }
                SyncMessageType::ArrayReject => {
                    let reject: ArrayRejectMsg = frame.decode_body().expect("decode reject");
                    panic!(
                        "Origin refused a seeded frame: {:?}: {}",
                        reject.reason, reject.detail
                    );
                }
                _ => {}
            }
        }
        if streamed >= expected {
            return;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "Origin streamed {streamed} of {expected} seeded ops within {SYNC_DEADLINE:?}"
        );
    }
}

/// Run the sync loop for `lite` until it is connected. The returned guard
/// aborts the loop, which closes the session.
async fn start_sync(lite: &Arc<NodeDbLite<PagedbStorageMem>>) -> AbortOnDrop {
    let mut config = SyncConfig::new(ORIGIN_WS, "");
    config.min_backoff = Duration::from_millis(50);
    let client = Arc::new(SyncClient::new(config));
    let delegate: Arc<dyn SyncDelegate> = Arc::clone(lite) as _;
    let handle = tokio::spawn(run_sync_loop(Arc::clone(&client), delegate));
    let guard = AbortOnDrop(handle);

    let deadline = tokio::time::Instant::now() + SYNC_DEADLINE;
    while client.state().await != SyncState::Connected {
        assert!(
            tokio::time::Instant::now() < deadline,
            "sync connection did not establish within {SYNC_DEADLINE:?}"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    guard
}

struct AbortOnDrop(tokio::task::JoinHandle<()>);

impl Drop for AbortOnDrop {
    fn drop(&mut self) {
        self.0.abort();
    }
}

/// Poll until coords `1..=last` hold their seeded values.
async fn await_converged(lite: &NodeDbLite<PagedbStorageMem>, last: i64) {
    let deadline = tokio::time::Instant::now() + SYNC_DEADLINE;
    loop {
        let mut converged = true;
        for x in 1..=last {
            if read_value(lite, ARRAY, x).await != Some(CellValue::Float64(x as f64 * 10.0)) {
                converged = false;
                break;
            }
        }
        if converged {
            return;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "replica did not converge on coords 1..={last} within {SYNC_DEADLINE:?}"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

fn seeded_ops(range: std::ops::RangeInclusive<i64>, schema_hlc: Hlc) -> Vec<ArrayOp> {
    range
        .map(|x| remote_put(ARRAY, x, x as u64 * 1_000, schema_hlc))
        .collect()
}

#[tokio::test(flavor = "multi_thread")]
async fn replica_that_missed_ops_while_disconnected_converges_after_reconnect() {
    let Some(origin) = OriginServer::try_spawn() else {
        return skip();
    };

    // The writer's schema snapshot defines the array on Origin. Its ops carry
    // the writer's schema HLC.
    let writer = common::SyncHarness::new_in_memory().await;
    writer.create_array(ARRAY).await;
    let schema_hlc = writer.schema_hlc(ARRAY);
    let snapshot_payload = writer
        .schemas
        .export_snapshot(ARRAY)
        .expect("export schema snapshot")
        .expect("schema registered");

    let mut seed = connect_and_handshake(origin.ws_url).await;
    send(
        &mut seed,
        SyncMessageType::ArraySchema,
        &ArraySchemaSyncMsg {
            array: ARRAY.into(),
            replica_id: common::array_wire::REMOTE_REPLICA,
            schema_hlc_bytes: schema_hlc.to_bytes(),
            snapshot_payload,
        },
    )
    .await;
    send(
        &mut seed,
        SyncMessageType::ArrayDeltaBatch,
        &delta_batch(ARRAY, &seeded_ops(1..=3, schema_hlc)),
    )
    .await;
    await_origin_op_log(&mut seed, 3).await;

    // The replica defines the same array after the writer, so its local
    // schema HLC is not older than the one the seeded ops carry.
    let lite = open_lite_with_array(ARRAY).await;

    // First session: the catch-up request from `Hlc::ZERO` streams ops 1..=3.
    let session = start_sync(&lite).await;
    await_converged(&lite, 3).await;
    drop(session);

    // Ops 4 and 5 reach Origin while the replica is offline.
    send(
        &mut seed,
        SyncMessageType::ArrayDeltaBatch,
        &delta_batch(ARRAY, &seeded_ops(4..=5, schema_hlc)),
    )
    .await;
    await_origin_op_log(&mut seed, 5).await;

    // Second session: the catch-up request resumes at the last applied op.
    let _session = start_sync(&lite).await;
    await_converged(&lite, 5).await;
}

/// Poll until `lite`'s array pending queue is empty. The push loop retires
/// an op only after Origin's pong proves Origin applied it.
async fn await_pushed(lite: &NodeDbLite<PagedbStorageMem>) {
    let deadline = tokio::time::Instant::now() + SYNC_DEADLINE;
    loop {
        let queued = SyncDelegate::pending_array_ops(lite, None, usize::MAX)
            .await
            .expect("read the array pending queue");
        if queued.is_empty() {
            return;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "{} array ops still queued after {SYNC_DEADLINE:?}",
            queued.len()
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn array_interop_put_roundtrip() {
    let Some(_origin) = OriginServer::try_spawn() else {
        return skip();
    };

    // Origin has never seen the array: the writer's schema announce defines it.
    let writer = open_lite_with_array(ARRAY).await;
    for x in 1..=3 {
        writer
            .array_put_cell(
                ARRAY,
                vec![CoordValue::Int64(x)],
                vec![CellValue::Float64(x as f64 * 10.0)],
                x * 1_000,
                0,
                i64::MAX,
            )
            .await
            .expect("array_put_cell");
    }
    // The reader defines the same array after the writer, so its local schema
    // HLC is not older than the one the writer's ops carry.
    let reader = open_lite_with_array(ARRAY).await;

    let _writer_session = start_sync(&writer).await;
    await_pushed(&writer).await;

    // The reader's catch-up request streams the writer's ops from Origin.
    let _reader_session = start_sync(&reader).await;
    await_converged(&reader, 3).await;
}
