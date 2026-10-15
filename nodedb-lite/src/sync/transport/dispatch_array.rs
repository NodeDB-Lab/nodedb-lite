//! `ArrayReject` and `PingPong` handling for the array push path.
//!
//! Both frames arrive on the ordered receive loop. Every reject for a window
//! is therefore applied before the pong that closes the window.

use std::sync::Arc;

use nodedb_types::sync::wire::{ArrayAdvisoryRefusedMsg, ArrayRejectMsg, PingPongMsg, SyncFrame};

use crate::sync::client::SyncClient;
use crate::sync::delegate::SyncDelegate;

/// Apply an `ArrayReject` to the push window, then to the delegate.
///
/// The window records which in-flight op Origin refused and what follows: a
/// schema re-send, a park until the next session, or nothing. The delegate
/// removes a permanently refused op from the pending queue and returns a
/// catch-up request for a `RetentionFloor` reject, which the push loop sends
/// on its next tick.
pub(super) async fn handle_array_reject(
    client: &Arc<SyncClient>,
    delegate: &Arc<dyn SyncDelegate>,
    frame: &SyncFrame,
) {
    let Some(msg) = frame.decode_body::<ArrayRejectMsg>() else {
        tracing::warn!("ArrayReject: failed to decode frame body");
        return;
    };
    client.array_push().lock().await.record_reject(&msg);
    if let Some(request) = delegate.handle_array_reject(&msg).await {
        client.queue_array_catchup(request).await;
    }
}

/// Log an `ArrayAdvisoryRefused`: Origin refused an `ArrayAck` or an
/// `ArrayCatchupRequest`.
///
/// The refusal names no local op, so the push window and the pending queue
/// stay unchanged. A refused ack only delays Origin's GC frontier. A refused
/// catch-up request is sent again by the next session's catch-up round.
pub(super) fn handle_array_advisory_refused(frame: &SyncFrame) {
    let Some(msg) = frame.decode_body::<ArrayAdvisoryRefusedMsg>() else {
        tracing::warn!("ArrayAdvisoryRefused: failed to decode frame body");
        return;
    };
    tracing::warn!(
        array = %msg.array,
        request = ?msg.request,
        reason = ?msg.reason,
        class = ?msg.reason.class(),
        detail = %msg.detail,
        "Origin refused an advisory array request"
    );
}

/// Close the open array window when a pong carries its barrier token, and
/// retire every op in it Origin did not refuse.
///
/// A ping from Origin needs no answer: `ping_loop` keeps the link alive. A
/// pong to a `ping_loop` ping carries a wall-clock timestamp, which never
/// matches a barrier token.
pub(super) async fn handle_ping_pong(
    client: &Arc<SyncClient>,
    delegate: &Arc<dyn SyncDelegate>,
    frame: &SyncFrame,
) {
    let Some(msg) = frame.decode_body::<PingPongMsg>() else {
        tracing::warn!("PingPong: failed to decode frame body");
        return;
    };
    if !msg.is_pong {
        tracing::trace!("received ping from Origin");
        return;
    }
    let Some(outcome) = client
        .array_push()
        .lock()
        .await
        .complete_barrier(msg.timestamp_ms)
    else {
        return;
    };
    if outcome.parked_unattributed > 0 {
        tracing::error!(
            ops = outcome.parked_unattributed,
            "array push: Origin refused a frame without naming the op; the window's ops \
             stay queued until the next session"
        );
    }
    if outcome.confirmed.is_empty() {
        return;
    }
    match delegate.retire_array_ops(&outcome.confirmed).await {
        Ok(()) => tracing::debug!(
            ops = outcome.confirmed.len(),
            "array push: Origin applied the window; ops retired"
        ),
        Err(e) => tracing::warn!(
            error = %e,
            "array push: retiring applied ops failed; they stay queued and the next session \
             re-sends them, which Origin deduplicates by HLC"
        ),
    }
}
