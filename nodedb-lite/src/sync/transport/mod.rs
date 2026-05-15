//! WebSocket transport — the runtime side of Lite ↔ Origin sync.
//!
//! Public surface is intentionally tiny: callers spawn [`run_sync_loop`]
//! once, after constructing a [`SyncDelegate`] that bridges the running
//! `NodeDbLite` to the transport's read/write callbacks. Everything else
//! (handshake, dispatch, per-engine push, ping keepalive) is private.
//!
//! Module map:
//!
//! - [`delegate`] — the `SyncDelegate` trait
//! - `connect`   — single-attempt connect + handshake
//! - `dispatch`  — inbound frame receive loop and message dispatch table
//! - `push`      — outbound delta + per-engine push loops, plus ping keepalive

pub mod delegate;

mod connect;
mod dispatch;
mod push;

#[cfg(test)]
mod tests;

use std::sync::Arc;

pub use delegate::SyncDelegate;

use crate::sync::client::{SyncClient, SyncState};

/// Run the sync loop — connects, handshakes, pushes/receives, reconnects.
///
/// Runs forever (until the task is cancelled). On disconnect it sleeps for
/// [`SyncClient::backoff_duration`] and retries; on a clean close the
/// backoff resets to zero.
pub async fn run_sync_loop(client: Arc<SyncClient>, delegate: Arc<dyn SyncDelegate>) {
    let mut attempt: u32 = 0;

    loop {
        client.set_state(SyncState::Connecting).await;
        tracing::info!(url = %client.config().url, attempt, "connecting to Origin");

        match connect::connect_and_run(&client, &delegate).await {
            Ok(()) => {
                tracing::info!("sync connection closed cleanly");
                attempt = 0;
            }
            Err(e) => {
                tracing::warn!(error = %e, attempt, "sync connection failed");
            }
        }

        client.set_state(SyncState::Reconnecting).await;
        let backoff = client.backoff_duration(attempt);
        tracing::info!(
            backoff_ms = backoff.as_millis(),
            "reconnecting after backoff"
        );
        tokio::time::sleep(backoff).await;
        attempt = attempt.saturating_add(1);
    }
}
