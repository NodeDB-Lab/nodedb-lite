//! The reconnecting sync loop: connect, handshake, push and receive, then
//! back off and reconnect.

use std::sync::Arc;

use crate::sync::client::{SyncClient, SyncState};
use crate::sync::delegate::SyncDelegate;

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

        match super::connect::connect_and_run(&client, &delegate).await {
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
