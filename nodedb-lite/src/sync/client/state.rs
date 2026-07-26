//! `SyncClient` struct, constructors, and simple accessors.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use tokio::sync::Mutex;

use nodedb_types::sync::wire::{ArrayAckMsg, ResyncRequestMsg};

/// Pending array acks keyed by array name.
///
/// Holding one entry per array name (the highest-HLC ack seen for that array)
/// is sufficient to advance Origin's GC frontier: Origin only needs to know the
/// highest durable HLC per replica per array, not every intermediate ack.
type PendingArrayAcks = std::collections::HashMap<String, ArrayAckMsg>;

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
    /// Epoch-ms timestamp of the last token refresh attempt (successful or not).
    /// Used with `token_refresh_backoff_ms` to enforce a minimum retry interval.
    pub(super) token_last_attempt_ms: Arc<Mutex<u64>>,
    /// Current backoff delay (ms) before the next refresh attempt is allowed.
    /// Doubles on each consecutive failure (exponential), capped at 5 minutes.
    pub(super) token_refresh_backoff_ms: Arc<Mutex<u64>>,
    /// Pending array acks to send on the next push-loop tick, keyed by array name.
    ///
    /// Set by `dispatch_frame` when an `ArrayDelta` or `ArrayDeltaBatch` is
    /// successfully applied. Each entry holds the highest-HLC ack seen for that
    /// array since the last drain. The push loop drains all entries and transmits
    /// them to Origin to advance the GC frontier.
    pub(super) pending_array_ack: Arc<Mutex<PendingArrayAcks>>,
    /// Producer ID assigned by Origin in `HandshakeAckMsg`.
    ///
    /// Used to stamp outbound frames so Origin can route acks back to this
    /// producer. `None` until the first successful handshake.
    pub(super) producer_id: Arc<Mutex<Option<u64>>>,
    /// Accepted epoch echoed by Origin in `HandshakeAckMsg`.
    ///
    /// Confirms Origin accepted the epoch sent in our handshake. `None` until
    /// the first successful handshake.
    pub(super) accepted_epoch: Arc<Mutex<Option<u64>>>,
    /// Set to `true` when Origin returns `AckStatus::Fenced` on any frame.
    ///
    /// Means the producer epoch is stale and Origin has a newer epoch on record.
    /// The sync loop must disconnect and reconnect; on reconnect the handshake
    /// will present the persisted epoch (from storage) which Origin already
    /// accepted. If LiteIdentity bumps epoch only on db-open (not reconnect),
    /// the epoch stays the same across reconnects and will still be fenced.
    /// In that case the operator must restart the db process to mint a new epoch.
    pub(crate) fenced: Arc<AtomicBool>,
    /// Collection names already announced (via `CollectionSchema`, opcode
    /// `0x13`) to Origin during the current session.
    ///
    /// Cleared whenever `session_id` is (re)set on handshake so each new
    /// session re-announces every collection with pending deltas, mirroring
    /// Origin's per-session announced set in `session_handler/announce.rs`.
    pub(super) announced_collections: Arc<Mutex<std::collections::HashSet<String>>>,
    /// Highest `RowPushMsg.sequence` applied so far, keyed by
    /// `(peer_id, collection)`.
    ///
    /// Client → server writes are protected by Origin's `sync_admit` gate
    /// (producer/epoch/seq), but nothing symmetric exists for server →
    /// client `RowPush` frames. Without this, a re-delivered or
    /// out-of-order frame is applied blindly: a duplicate delete-then-push
    /// re-delivery can resurrect a row a later write already removed, and
    /// an out-of-order pair can leave a stale post-image as the winner.
    /// In-memory and per-connection is the right scope: Origin's fan-out
    /// is a live mpsc channel with no replay across reconnects, so there is
    /// nothing durable to reconcile against after a reconnect anyway.
    ///
    /// Nested by peer_id then collection (rather than a flat `(u64, String)`
    /// key) so the common case — checking the mark for a collection that is
    /// already tracked — looks it up by `&str` without allocating; a `String`
    /// is only allocated the first time a given collection is seen.
    pub(super) row_push_watermark:
        Arc<Mutex<std::collections::HashMap<u64, std::collections::HashMap<String, u64>>>>,
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
            pending_array_ack: Arc::new(Mutex::new(PendingArrayAcks::new())),
            producer_id: Arc::new(Mutex::new(None)),
            accepted_epoch: Arc::new(Mutex::new(None)),
            fenced: Arc::new(AtomicBool::new(false)),
            token_last_attempt_ms: Arc::new(Mutex::new(0)),
            token_refresh_backoff_ms: Arc::new(Mutex::new(
                crate::sync::client::token::TOKEN_REFRESH_MIN_BACKOFF_MS,
            )),
            announced_collections: Arc::new(Mutex::new(std::collections::HashSet::new())),
            row_push_watermark: Arc::new(Mutex::new(std::collections::HashMap::new())),
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

    /// Producer ID assigned by Origin, or 0 if the handshake has not yet completed.
    pub async fn producer_id(&self) -> u64 {
        self.producer_id.lock().await.unwrap_or_default()
    }

    /// Accepted epoch echoed by Origin, or 0 if the handshake has not yet completed.
    pub async fn accepted_epoch(&self) -> u64 {
        self.accepted_epoch.lock().await.unwrap_or_default()
    }

    /// Store the server-assigned producer ID.
    pub(super) async fn set_producer_id(&self, id: u64) {
        *self.producer_id.lock().await = Some(id);
    }

    /// Store the accepted epoch echoed by Origin.
    pub(super) async fn set_accepted_epoch(&self, epoch: u64) {
        *self.accepted_epoch.lock().await = Some(epoch);
    }

    /// Load producer state (producer_id + accepted_epoch) from previously
    /// persisted values. Called on reconnect so the client knows its identity
    /// before the next handshake.
    pub async fn load_producer_state(&self, producer_id: u64, accepted_epoch: u64) {
        *self.producer_id.lock().await = Some(producer_id);
        *self.accepted_epoch.lock().await = Some(accepted_epoch);
    }

    /// Whether Origin fenced this producer.
    ///
    /// When `true`, the sync loop should disconnect and reconnect. The epoch
    /// is only bumped on db-open (via `LiteIdentity`), so reconnecting
    /// alone does not change the epoch. A fenced producer requires the
    /// operator to restart the process to mint a fresh epoch.
    pub fn is_fenced(&self) -> bool {
        self.fenced.load(Ordering::Acquire)
    }

    /// Mark this producer as fenced by Origin.
    ///
    /// Also unsets the `push_paused_for_auth` flag so the disconnect path is
    /// not confused with an auth-pause: fencing is a permanent producer-epoch
    /// rejection, not a token issue.
    pub fn set_fenced(&self) {
        self.fenced.store(true, Ordering::Release);
        tracing::error!(
            "producer epoch fenced by Origin — this producer's epoch is stale; \
             process restart required to mint a new epoch"
        );
    }

    /// Clear the fenced flag. Called on reconnect so the client can attempt
    /// re-registration; if Origin still fences it the flag is set again.
    pub fn clear_fenced(&self) {
        self.fenced.store(false, Ordering::Release);
    }

    /// Access the per-session set of collections already announced via
    /// `CollectionSchema` (opcode `0x13`).
    pub(crate) fn announced_collections(&self) -> &Arc<Mutex<std::collections::HashSet<String>>> {
        &self.announced_collections
    }

    /// Returns true when this row push should be applied.
    ///
    /// Every client → server engine path is protected by Origin's
    /// `sync_admit` gate (producer/epoch/seq); there is no equivalent for
    /// server → client `RowPush` frames. Without this gate, a re-delivered
    /// frame is applied unconditionally: a duplicate delivery of a stale
    /// post-image can resurrect a row a later, un-replayed write already
    /// deleted, and an out-of-order pair can leave the older post-image as
    /// the final state.
    ///
    /// `sequence == 0` is the unsequenced sentinel — it mirrors how Origin
    /// treats `producer_id == 0` as unidentified in `sync_gate.rs`, marking
    /// DDL-managed system rows (retention policies, alerts) that were never
    /// assigned a per-collection sequence. Such a frame carries no ordering
    /// information to dedup against, so it is always applied and never
    /// allowed to move the watermark (doing so would let a later
    /// legitimately-sequenced frame for the same collection be skipped as
    /// "stale").
    pub async fn admit_row_push(&self, peer_id: u64, collection: &str, sequence: u64) -> bool {
        if sequence == 0 {
            return true;
        }

        let mut watermark = self.row_push_watermark.lock().await;
        let per_collection = watermark.entry(peer_id).or_default();
        let mark = per_collection.get(collection).copied().unwrap_or(0);

        if sequence <= mark {
            tracing::debug!(
                peer_id,
                collection,
                sequence,
                watermark = mark,
                "RowPush: sequence at or below high-water mark, skipping duplicate/stale frame"
            );
            return false;
        }

        match per_collection.get_mut(collection) {
            Some(mark) => *mark = sequence,
            None => {
                per_collection.insert(collection.to_string(), sequence);
            }
        }
        true
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
