//! Keepalive, backoff, and flow-control-facing accessors.

use std::time::Duration;

use nodedb_types::sync::wire::{PingPongMsg, SyncFrame, SyncMessageType};

use super::config::SyncState;
use super::state::SyncClient;
use crate::sync::flow_control::SyncMetricsSnapshot;

impl SyncClient {
    /// Build a ping frame.
    pub fn build_ping(&self) -> Option<SyncFrame> {
        let ping = PingPongMsg {
            timestamp_ms: crate::runtime::now_millis(),
            is_pong: false,
        };
        SyncFrame::try_encode(SyncMessageType::PingPong, &ping)
    }

    /// Calculate backoff duration for reconnection attempt N.
    pub fn backoff_duration(&self, attempt: u32) -> Duration {
        let base = self.config.min_backoff.as_millis() as u64;
        let max = self.config.max_backoff.as_millis() as u64;
        let delay = (base * 2u64.saturating_pow(attempt)).min(max);
        Duration::from_millis(delay)
    }

    /// Update pending queue stats in the flow controller.
    /// Called from the push loop after reading pending deltas.
    pub async fn update_pending_stats(&self, count: usize, bytes: usize) {
        let mut flow = self.flow.lock().await;
        flow.update_pending(count, bytes);
    }

    /// Check if the pending queue is at capacity (flow control).
    pub async fn is_queue_full(&self) -> bool {
        let flow = self.flow.lock().await;
        flow.is_queue_full()
    }

    /// Get a snapshot of sync metrics for monitoring/health.
    pub async fn sync_metrics(&self) -> SyncMetricsSnapshot {
        let state = *self.state.lock().await;
        let state_str = match state {
            SyncState::Disconnected => "disconnected",
            SyncState::Connecting => "connecting",
            SyncState::Connected => "connected",
            SyncState::Reconnecting => "reconnecting",
        };
        let flow = self.flow.lock().await;
        flow.snapshot(state_str, &self.metrics)
    }

    /// Reset flow controller on reconnect.
    pub async fn reset_flow_control(&self) {
        let mut flow = self.flow.lock().await;
        flow.reset();
        self.metrics.record_reconnect();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sync::client::SyncConfig;

    fn make_config() -> SyncConfig {
        SyncConfig::new("wss://localhost:9090/sync", "test.jwt.token")
    }

    #[test]
    fn backoff_exponential_with_cap() {
        let client = SyncClient::new(make_config(), 1);
        assert_eq!(client.backoff_duration(0), Duration::from_secs(1));
        assert_eq!(client.backoff_duration(1), Duration::from_secs(2));
        assert_eq!(client.backoff_duration(2), Duration::from_secs(4));
        assert_eq!(client.backoff_duration(3), Duration::from_secs(8));
        assert_eq!(client.backoff_duration(10), Duration::from_secs(60));
    }

    #[test]
    fn ping_frame_is_valid() {
        let client = SyncClient::new(make_config(), 1);
        let frame = client.build_ping().expect("ping encode");
        assert_eq!(frame.msg_type, SyncMessageType::PingPong);
        assert!(!frame.body.is_empty());
    }

    #[tokio::test]
    async fn sync_metrics_snapshot() {
        let client = SyncClient::new(make_config(), 1);
        let snap = client.sync_metrics().await;
        assert_eq!(snap.state, "disconnected");
        assert_eq!(snap.pending_count, 0);
        assert_eq!(snap.deltas_pushed, 0);
    }
}
