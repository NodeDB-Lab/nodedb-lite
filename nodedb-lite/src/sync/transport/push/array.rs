//! Array op push.
//!
//! Each tick with no window open:
//!
//! 1. Reads up to [`ARRAY_PUSH_WINDOW`] queued ops after the session cursor,
//!    skipping ops held until the next session.
//! 2. Sends `ArraySchema` for each array not yet announced in this session,
//!    before any of that array's deltas. Origin commits the schema before it
//!    reads the next frame, so the deltas that follow find the array.
//! 3. Sends one `ArrayDelta` per op, stamped with the producer id, the
//!    accepted epoch, and the op's stream seq. The seq is assigned and
//!    persisted on first send, so a re-send carries the same seq.
//! 4. Sends a `PingPong` ping carrying the window's barrier token.
//!
//! The pong closes the window (see `dispatch_array`). Ops leave the durable
//! queue only then. A send error ends the session, and the next session
//! re-sends every unconfirmed op.
//!
//! `ArrayDeltaBatch` is not used on this path. Origin answers a batch with at
//! most one reject and applies its ops without a seq. A second refused op in
//! a batch draws no reject, so the pong confirms it and the write is lost.

use std::ops::ControlFlow;
use std::sync::Arc;
use std::time::Instant;

use futures::SinkExt;
use tokio::sync::Mutex;
use tokio_tungstenite::tungstenite::Message;

use nodedb_array::sync::hlc::Hlc;
use nodedb_types::sync::wire::{
    ArrayDeltaMsg, EngineKind, PingPongMsg, SyncFrame, SyncMessageType, stream_id_for,
};

use super::send::send_binary;
use crate::sync::array::PendingArrayOp;
use crate::sync::client::{ARRAY_PUSH_WINDOW, AnnounceStep, SyncClient, TickPlan};
use crate::sync::delegate::SyncDelegate;

pub(super) async fn push<S>(
    client: &Arc<SyncClient>,
    delegate: &Arc<dyn SyncDelegate>,
    sink: &Arc<Mutex<S>>,
) -> ControlFlow<()>
where
    S: SinkExt<Message> + Unpin,
    S::Error: std::fmt::Display,
{
    let after = match client.array_push().lock().await.begin_tick(Instant::now()) {
        TickPlan::Wait => return ControlFlow::Continue(()),
        TickPlan::Drain { after } => after,
    };
    let scanned = match delegate.pending_array_ops(after, ARRAY_PUSH_WINDOW).await {
        Ok(ops) => ops,
        Err(e) => {
            tracing::warn!(error = %e, "array push: reading the pending queue failed; retrying next tick");
            return ControlFlow::Continue(());
        }
    };

    let ops = select_sendable(client, scanned).await;
    let ready = announce_schemas(client, delegate, sink, &ops).await?;

    let producer_id = client.producer_id().await;
    let epoch = client.accepted_epoch().await;
    for mut op in ops {
        if !ready.contains(&op.array) {
            continue;
        }
        if op.seq == 0 {
            op.seq = delegate
                .next_stream_seq(stream_id_for(EngineKind::Array, &op.array))
                .await;
            if let Err(e) = delegate.persist_array_op_seq(&op).await {
                tracing::warn!(
                    array = %op.array,
                    error = %e,
                    "array push: persisting the stream seq failed; the op waits for a later tick"
                );
                client.array_push().lock().await.request_rewind();
                continue;
            }
        }
        send_delta(client, delegate, sink, op, producer_id, epoch).await?;
    }

    let mut window = client.array_push().lock().await;
    if !window.has_in_flight() {
        window.finish_idle_tick();
        return ControlFlow::Continue(());
    }
    let token = window.open_barrier(Instant::now());
    drop(window);
    let ping = PingPongMsg {
        timestamp_ms: token,
        is_pong: false,
    };
    let Some(frame) = SyncFrame::try_encode(SyncMessageType::PingPong, &ping) else {
        tracing::error!("array push: barrier ping encode failed; the window's ops are re-sent");
        client.array_push().lock().await.abandon_window();
        return ControlFlow::Continue(());
    };
    if let Err(e) = send_binary(sink, frame).await {
        tracing::warn!(error = %e, "array push: barrier ping send failed; ops re-sent after reconnect");
        return ControlFlow::Break(());
    }
    ControlFlow::Continue(())
}

/// Drop ops held until the next session and move the cursor past every
/// scanned op.
async fn select_sendable(client: &SyncClient, scanned: Vec<PendingArrayOp>) -> Vec<PendingArrayOp> {
    let mut window = client.array_push().lock().await;
    let mut ops = Vec::with_capacity(scanned.len());
    for op in scanned {
        window.advance(op.hlc);
        if !window.is_held(&op) {
            ops.push(op);
        }
    }
    ops
}

/// Announce the schema of every array in `ops` that needs it, in the order
/// the arrays first appear. Returns the arrays whose ops can go out now.
async fn announce_schemas<S>(
    client: &SyncClient,
    delegate: &Arc<dyn SyncDelegate>,
    sink: &Arc<Mutex<S>>,
    ops: &[PendingArrayOp],
) -> ControlFlow<(), Vec<String>>
where
    S: SinkExt<Message> + Unpin,
    S::Error: std::fmt::Display,
{
    let mut seen: Vec<&str> = Vec::new();
    for op in ops {
        if !seen.contains(&op.array.as_str()) {
            seen.push(&op.array);
        }
    }

    let mut ready = Vec::with_capacity(seen.len());
    for array in seen {
        let step = client.array_push().lock().await.announce_step(array);
        match step {
            AnnounceStep::Ready => ready.push(array.to_owned()),
            AnnounceStep::GiveUp => {}
            AnnounceStep::Send => {
                if announce(client, delegate, sink, array).await? {
                    ready.push(array.to_owned());
                }
            }
        }
    }
    ControlFlow::Continue(ready)
}

/// Send `array`'s `ArraySchema`. Returns `false` when its ops must wait.
async fn announce<S>(
    client: &SyncClient,
    delegate: &Arc<dyn SyncDelegate>,
    sink: &Arc<Mutex<S>>,
    array: &str,
) -> ControlFlow<(), bool>
where
    S: SinkExt<Message> + Unpin,
    S::Error: std::fmt::Display,
{
    let msg = match delegate.array_schema_announce(array) {
        Ok(Some(msg)) => msg,
        Ok(None) => {
            tracing::error!(
                array,
                "array push: queued ops name an array with no local schema; ops stay queued \
                 until the next session"
            );
            client.array_push().lock().await.park_array(array);
            return ControlFlow::Continue(false);
        }
        Err(e) => {
            tracing::warn!(array, error = %e, "array push: reading the schema failed; retrying later");
            client.array_push().lock().await.request_rewind();
            return ControlFlow::Continue(false);
        }
    };
    let Some(frame) = SyncFrame::try_encode(SyncMessageType::ArraySchema, &msg) else {
        tracing::error!(
            array,
            "array push: ArraySchema encode failed; ops stay queued until the next session"
        );
        client.array_push().lock().await.park_array(array);
        return ControlFlow::Continue(false);
    };
    client
        .array_push()
        .lock()
        .await
        .record_announced(array, Hlc::from_bytes(&msg.schema_hlc_bytes));
    if let Err(e) = send_binary(sink, frame).await {
        tracing::warn!(array, error = %e, "array push: ArraySchema send failed");
        return ControlFlow::Break(());
    }
    tracing::debug!(array, "announced ArraySchema to Origin");
    ControlFlow::Continue(true)
}

/// Send one op as an `ArrayDelta`.
async fn send_delta<S>(
    client: &SyncClient,
    delegate: &Arc<dyn SyncDelegate>,
    sink: &Arc<Mutex<S>>,
    op: PendingArrayOp,
    producer_id: u64,
    epoch: u64,
) -> ControlFlow<()>
where
    S: SinkExt<Message> + Unpin,
    S::Error: std::fmt::Display,
{
    let hlc = op.hlc;
    let msg = ArrayDeltaMsg {
        array: op.array,
        op_payload: op.op_payload,
        producer_id,
        epoch,
        seq: op.seq,
    };
    let Some(frame) = SyncFrame::try_encode(SyncMessageType::ArrayDelta, &msg) else {
        // An unencodable op kept in the queue fails again on every rewind.
        // It is retired, as every engine queue retires an entry it cannot
        // encode.
        tracing::error!(
            array = %msg.array,
            op_hlc_physical_ms = hlc.physical_ms,
            "array push: ArrayDelta encode failed; retiring the op — Origin does not get it"
        );
        if let Err(e) = delegate.retire_array_ops(&[hlc]).await {
            tracing::error!(error = %e, "array push: retiring the unencodable op failed");
        }
        return ControlFlow::Continue(());
    };
    client.array_push().lock().await.record_sent(hlc);
    if let Err(e) = send_binary(sink, frame).await {
        tracing::warn!(
            array = %msg.array,
            error = %e,
            "array push: ArrayDelta send failed; the op stays queued for re-send on reconnect"
        );
        return ControlFlow::Break(());
    }
    tracing::debug!(array = %msg.array, seq = msg.seq, "sent ArrayDelta to Origin");
    ControlFlow::Continue(())
}
