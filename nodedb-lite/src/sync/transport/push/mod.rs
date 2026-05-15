//! Outbound push loops — each tick drains every engine's outbound queue and
//! writes wire frames to the WebSocket sink.
//!
//! `delta_push_loop` is the single tick coordinator: each tick it walks every
//! engine queue (columnar, vector, fts, spatial, timeseries) plus the CRDT
//! delta queue and any latched control messages (resync, ArrayAck, token
//! refresh). Per-engine push helpers live in sibling modules; the shared
//! send / encode primitives live in `send`.

mod columnar;
mod control;
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
