//! `SyncClient` struct, constructors, and simple accessors.

use std::sync::Arc;

use tokio::sync::Mutex;

use nodedb_types::sync::wire::ResyncRequestMsg;

use super::config::{SyncConfig, SyncState};
use crate::sync::clock::VectorClock;
use crate::sync::compensation::{CompensationHandler, CompensationRegistry};
use crate::sync::flow_control::{FlowControlConfig, FlowController, SyncMetrics};
use crate::sync::shapes::ShapeManager;

/// Sync client — manages the WebSocket connection to Origin.
///
/// The client runs as a background Tokio task. It:
/// 1. Connects to Origin via WebSocket
/// 2. Sends handshake with JWT + vector clock + shape subscriptions
/// 3. Pushes accumulated CRDT deltas
/// 4. Receives shape snapshots and incremental deltas
/// 5. Handles rejections via CompensationRegistry
/// 6. Auto-reconnects with exponential backoff on disconnect
pub struct SyncClient {
    pub(super) config: SyncConfig,
    pub(super) state: Arc<Mutex<SyncState>>,
    pub(super) clock: Arc<Mutex<VectorClock>>,
    pub(super) shapes: Arc<Mutex<ShapeManager>>,
    pub(super) compensation: Arc<CompensationRegistry>,
    /// Session ID assigned by Origin after handshake.
    pub(super) session_id: Arc<Mutex<Option<String>>>,
    /// Peer ID of this Lite client (for CRDT identity).
    pub(super) peer_id: u64,
    /// Lite instance identity (UUID v7) for fork detection.
    pub(super) lite_id: Option<String>,
    /// Monotonic epoch counter for fork detection.
    pub(super) epoch: Option<u64>,
    /// Sequence tracker: per-shape, the last LSN received from Origin.
    pub(super) last_seen_lsn: Arc<Mutex<std::collections::HashMap<String, u64>>>,
    /// Whether a re-sync request has been sent for this connection.
    pub(super) resync_requested: Arc<Mutex<bool>>,
    /// Pending re-sync request to send to Origin.
    pub(super) pending_resync: Arc<Mutex<Option<ResyncRequestMsg>>>,
    /// Flow controller: in-flight window, adaptive batch sizing, queue bounds.
    pub(super) flow: Arc<Mutex<FlowController>>,
    /// Sync metrics: atomic counters for monitoring.
    pub(super) metrics: Arc<SyncMetrics>,
    /// Timestamp (epoch ms) when the current JWT was set (for proactive refresh).
    pub(super) token_set_at_ms: Arc<Mutex<u64>>,
    /// Whether a token refresh is currently in-flight.
    pub(super) token_refresh_pending: Arc<Mutex<bool>>,
    /// Whether delta push is paused due to auth failure (awaiting refresh).
    pub(super) push_paused_for_auth: Arc<Mutex<bool>>,
}

impl SyncClient {
    /// Create a new sync client (does not connect yet).
    pub fn new(config: SyncConfig, peer_id: u64) -> Self {
        Self::with_flow_control(config, peer_id, FlowControlConfig::default())
    }

    /// Create a new sync client with custom flow control config.
    pub fn with_flow_control(
        config: SyncConfig,
        peer_id: u64,
        flow_config: FlowControlConfig,
    ) -> Self {
        Self {
            config,
            state: Arc::new(Mutex::new(SyncState::Disconnected)),
            clock: Arc::new(Mutex::new(VectorClock::new())),
            shapes: Arc::new(Mutex::new(ShapeManager::new())),
            compensation: Arc::new(CompensationRegistry::new()),
            session_id: Arc::new(Mutex::new(None)),
            peer_id,
            lite_id: None,
            epoch: None,
            last_seen_lsn: Arc::new(Mutex::new(std::collections::HashMap::new())),
            resync_requested: Arc::new(Mutex::new(false)),
            pending_resync: Arc::new(Mutex::new(None)),
            flow: Arc::new(Mutex::new(FlowController::new(flow_config))),
            metrics: Arc::new(SyncMetrics::new()),
            token_set_at_ms: Arc::new(Mutex::new(crate::runtime::now_millis())),
            token_refresh_pending: Arc::new(Mutex::new(false)),
            push_paused_for_auth: Arc::new(Mutex::new(false)),
        }
    }

    /// Set the Lite identity for fork detection (called after LiteIdentity::load_or_create).
    pub fn set_identity(&mut self, lite_id: String, epoch: u64) {
        self.lite_id = Some(lite_id);
        self.epoch = Some(epoch);
    }

    /// Current connection state.
    pub async fn state(&self) -> SyncState {
        *self.state.lock().await
    }

    /// Set the connection state.
    pub async fn set_state(&self, new_state: SyncState) {
        *self.state.lock().await = new_state;
    }

    /// Register a compensation handler.
    pub fn set_compensation_handler(&self, handler: Arc<dyn CompensationHandler>) {
        self.compensation.set_handler(handler);
    }

    /// Access the shape manager (for subscribing/unsubscribing).
    pub fn shapes(&self) -> &Arc<Mutex<ShapeManager>> {
        &self.shapes
    }

    /// Access the vector clock.
    pub fn clock(&self) -> &Arc<Mutex<VectorClock>> {
        &self.clock
    }

    /// Access the compensation registry.
    pub fn compensation(&self) -> &Arc<CompensationRegistry> {
        &self.compensation
    }

    /// Access config.
    pub fn config(&self) -> &SyncConfig {
        &self.config
    }

    /// Peer ID.
    pub fn peer_id(&self) -> u64 {
        self.peer_id
    }

    /// Access the flow controller.
    pub fn flow(&self) -> &Arc<Mutex<FlowController>> {
        &self.flow
    }

    /// Access the sync metrics.
    pub fn metrics(&self) -> &Arc<SyncMetrics> {
        &self.metrics
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_config() -> SyncConfig {
        SyncConfig::new("wss://localhost:9090/sync", "test.jwt.token")
    }

    #[tokio::test]
    async fn initial_state_is_disconnected() {
        let client = SyncClient::new(make_config(), 1);
        assert_eq!(client.state().await, SyncState::Disconnected);
    }
}
