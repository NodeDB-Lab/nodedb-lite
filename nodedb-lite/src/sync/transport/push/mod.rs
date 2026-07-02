//! Outbound push loops — each tick drains every engine's outbound queue and
//! writes wire frames to the WebSocket sink.
//!
//! `delta_push_loop` is the single tick coordinator: each tick it walks every
//! engine queue (columnar, vector, fts, spatial, timeseries) plus the CRDT
//! delta queue and any latched control messages (resync, ArrayAck, token
//! refresh). Per-engine push helpers live in sibling modules; the shared
//! send / encode primitives live in `send`.

mod columnar;
// `control` is visible within the `transport` module so `transport::tests` can
// drive `push_collection_schemas` / `push_crdt_deltas` directly for the
// schema-before-delta ordering test; not part of the public API.
pub(in crate::sync::transport) mod control;
mod fts;
mod send;
mod spatial;
mod timeseries;
mod vector;

use std::sync::Arc;
use std::time::Duration;

use futures::SinkExt;
use tokio::sync::Mutex;
use tokio_tungstenite::tungstenite::Message;

use nodedb_types::sync::wire::SyncMessageType;

use self::send::{encode_and_send, send_binary};
use super::delegate::SyncDelegate;
use crate::sync::client::{SyncClient, SyncState};

/// Maximum age of an unACK'd in-flight entry before it is evicted by the stale-
/// cleanup pass. Entries older than this are treated as losses: flow control
/// applies AIMD multiplicative decrease and the `stale_timeouts` metric is
/// incremented. The recovery path is the normal push loop retrying from the
/// pending queue.
const STALE_IN_FLIGHT_TIMEOUT: Duration = Duration::from_secs(30);

/// Periodically push pending deltas (and every other outbound queue) to Origin.
pub(super) async fn delta_push_loop<S>(
    client: &Arc<SyncClient>,
    delegate: &Arc<dyn SyncDelegate>,
    sink: &Arc<Mutex<S>>,
) where
    S: SinkExt<Message> + Unpin,
    S::Error: std::fmt::Display,
{
    let mut interval = tokio::time::interval(Duration::from_millis(100));

    loop {
        interval.tick().await;

        if client.state().await != SyncState::Connected {
            continue;
        }

        // If Origin has fenced this producer, stop all outbound push until
        // the sync loop reconnects (which clears the flag). Reconnect alone
        // does not bump the epoch — epoch is only minted on db-open via
        // LiteIdentity. A persistently fenced producer requires a process
        // restart to obtain a new epoch.
        if client.is_fenced() {
            tracing::error!(
                "push loop halted: producer is fenced by Origin; \
                 waiting for reconnect (process restart required for new epoch)"
            );
            return;
        }

        if control::push_control_messages(client, sink)
            .await
            .is_break()
        {
            return;
        }
        if columnar::push(client, delegate, sink).await.is_break() {
            return;
        }
        if vector::push(client, delegate, sink).await.is_break() {
            return;
        }
        if fts::push(client, delegate, sink).await.is_break() {
            return;
        }
        if spatial::push(client, delegate, sink).await.is_break() {
            return;
        }
        if timeseries::push(client, delegate, sink).await.is_break() {
            return;
        }
        if control::push_collection_schemas(client, delegate, sink)
            .await
            .is_break()
        {
            return;
        }
        if control::push_crdt_deltas(client, delegate, sink)
            .await
            .is_break()
        {
            return;
        }
    }
}

/// Periodically send ping frames for keepalive and check token refresh.
pub(super) async fn ping_loop<S>(client: &Arc<SyncClient>, sink: &Arc<Mutex<S>>)
where
    S: SinkExt<Message> + Unpin,
    S::Error: std::fmt::Display,
{
    let mut interval = tokio::time::interval(client.config().ping_interval);

    loop {
        interval.tick().await;

        if client.state().await != SyncState::Connected {
            continue;
        }

        // Stale in-flight cleanup: evict any unACK'd entries that have exceeded
        // the deadline and apply AIMD multiplicative decrease. This unblocks the
        // push pipeline when a DeltaAck is silently dropped (e.g. malformed frame).
        {
            let mut flow = client.flow().lock().await;
            flow.cleanup_stale_and_record(STALE_IN_FLIGHT_TIMEOUT, client.metrics());
        }

        // Proactive token refresh: check if the token is approaching expiry.
        if client.should_refresh_token().await
            && let Some(refresh_msg) = client.initiate_token_refresh().await
            && encode_and_send(
                sink,
                SyncMessageType::TokenRefresh,
                &refresh_msg,
                "TokenRefresh",
            )
            .await
            .is_break()
        {
            return;
        }

        let Some(frame) = client.build_ping() else {
            tracing::error!("failed to encode ping frame");
            return;
        };
        if let Err(e) = send_binary(sink, frame).await {
            tracing::warn!(error = %e, "ping send failed");
            return;
        }
    }
}
