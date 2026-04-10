//! Flow control, adaptive batching, and sync metrics.
//!
//! Manages the delta push pipeline with:
//! - **ACK-based flow control**: in-flight window limits concurrent unACK'd deltas
//! - **Adaptive batch sizing**: AIMD algorithm adjusts batch size based on observed RTT
//! - **Bounded pending queue**: configurable limits on pending delta count and bytes
//! - **Sync metrics**: structured, observable sync state for monitoring
//!
//! All state is behind `tokio::sync::Mutex` for use from the async sync transport.

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Instant;

use serde::Serialize;

/// Flow control configuration.
#[derive(Debug, Clone)]
pub struct FlowControlConfig {
    /// Maximum in-flight (unACK'd) deltas before pausing pushes.
    /// Default: 64.
    pub max_in_flight: usize,
    /// Minimum batch size (floor for AIMD decrease).
    /// Default: 10.
    pub min_batch_size: usize,
    /// Maximum batch size (ceiling for AIMD increase).
    /// Default: 500.
    pub max_batch_size: usize,
    /// Initial batch size before RTT data is available.
    /// Default: 50.
    pub initial_batch_size: usize,
    /// Maximum pending deltas in queue before rejecting writes.
    /// Default: 10_000.
    pub max_pending_count: usize,
    /// Maximum pending bytes in queue before rejecting writes.
    /// Default: 50 MB.
    pub max_pending_bytes: usize,
}

impl Default for FlowControlConfig {
    fn default() -> Self {
        Self {
            max_in_flight: 64,
            min_batch_size: 10,
            max_batch_size: 500,
            initial_batch_size: 50,
            max_pending_count: 10_000,
            max_pending_bytes: 50 * 1024 * 1024,
        }
    }
}

/// Observable sync metrics — all atomic for lock-free reads.
///
/// Serialize to JSON for health endpoints and monitoring.
#[derive(Debug, Serialize)]
pub struct SyncMetricsSnapshot {
    /// Current sync connection state.
    pub state: &'static str,
    /// Number of pending (unsent + unACK'd) deltas.
    pub pending_count: u64,
    /// Total bytes of pending deltas.
    pub pending_bytes: u64,
    /// Total deltas pushed to Origin (lifetime).
    pub deltas_pushed: u64,
    /// Total deltas received from Origin (lifetime).
    pub deltas_received: u64,
    /// Total deltas rejected by Origin (lifetime).
    pub deltas_rejected: u64,
    /// Exponential moving average RTT in milliseconds.
    pub avg_rtt_ms: u64,
    /// Current in-flight (pushed but not yet ACK'd) count.
    pub in_flight: u64,
    /// Total reconnect attempts (lifetime).
    pub reconnect_count: u64,
    /// Timestamp (millis) of last successful sync activity.
    pub last_sync_ts: u64,
    /// Total CRC32C checksum failures detected (lifetime).
    pub checksum_failures: u64,
    /// Current adaptive batch size.
    pub current_batch_size: u64,
    /// Total conflict-related rejections (lifetime).
    pub conflicts_total: u64,
}

/// Sync metrics — atomic counters for lock-free concurrent access.
pub struct SyncMetrics {
    pub deltas_pushed: AtomicU64,
    pub deltas_received: AtomicU64,
    pub deltas_rejected: AtomicU64,
    pub reconnect_count: AtomicU64,
    pub last_sync_ts: AtomicU64,
    pub checksum_failures: AtomicU64,
    /// Total conflict-related rejections (UNIQUE, FK, schema violations).
    pub conflicts_total: AtomicU64,
    /// Per-collection conflict counts. Protected by std::sync::Mutex for HashMap.
    conflicts_by_collection: std::sync::Mutex<HashMap<String, u64>>,
}

impl SyncMetrics {
    pub fn new() -> Self {
        Self {
            deltas_pushed: AtomicU64::new(0),
            deltas_received: AtomicU64::new(0),
            deltas_rejected: AtomicU64::new(0),
            reconnect_count: AtomicU64::new(0),
            last_sync_ts: AtomicU64::new(0),
            checksum_failures: AtomicU64::new(0),
            conflicts_total: AtomicU64::new(0),
            conflicts_by_collection: std::sync::Mutex::new(HashMap::new()),
        }
    }

    pub fn record_push(&self, count: u64) {
        self.deltas_pushed.fetch_add(count, Ordering::Relaxed);
        self.last_sync_ts
            .store(crate::runtime::now_millis(), Ordering::Relaxed);
    }

    pub fn record_received(&self) {
        self.deltas_received.fetch_add(1, Ordering::Relaxed);
        self.last_sync_ts
            .store(crate::runtime::now_millis(), Ordering::Relaxed);
    }

    pub fn record_reject(&self) {
        self.deltas_rejected.fetch_add(1, Ordering::Relaxed);
    }

    pub fn record_reconnect(&self) {
        self.reconnect_count.fetch_add(1, Ordering::Relaxed);
    }

    pub fn record_checksum_failure(&self) {
        self.checksum_failures.fetch_add(1, Ordering::Relaxed);
    }

    /// Record a conflict (constraint-related rejection) for a collection.
    pub fn record_conflict(&self, collection: &str) {
        self.conflicts_total.fetch_add(1, Ordering::Relaxed);
        if let Ok(mut map) = self.conflicts_by_collection.lock() {
            *map.entry(collection.to_string()).or_insert(0) += 1;
        }
    }

    /// Get per-collection conflict counts snapshot.
    pub fn conflicts_by_collection(&self) -> HashMap<String, u64> {
        self.conflicts_by_collection
            .lock()
            .map(|m| m.clone())
            .unwrap_or_default()
    }
}

impl Default for SyncMetrics {
    fn default() -> Self {
        Self::new()
    }
}

/// Flow controller — manages the delta push pipeline.
///
/// Tracks in-flight deltas, computes RTT for adaptive batch sizing,
/// and enforces pending queue bounds.
pub struct FlowController {
    config: FlowControlConfig,

    /// In-flight tracking: mutation_id → send timestamp.
    in_flight: HashMap<u64, Instant>,

    /// Exponential moving average of RTT in milliseconds.
    /// Uses α = 0.125 (TCP-style EWMA).
    ema_rtt_ms: f64,

    /// Current adaptive batch size.
    current_batch_size: usize,

    /// Count of consecutive ACK successes (for AIMD additive increase).
    consecutive_acks: usize,

    /// Current pending delta count (tracked externally, updated here).
    pending_count: usize,

    /// Current pending delta bytes.
    pending_bytes: usize,
}

impl FlowController {
    pub fn new(config: FlowControlConfig) -> Self {
        let initial_batch = config.initial_batch_size;
        Self {
            config,
            in_flight: HashMap::new(),
            ema_rtt_ms: 0.0,
            current_batch_size: initial_batch,
            consecutive_acks: 0,
            pending_count: 0,
            pending_bytes: 0,
        }
    }

    /// Check if the push pipeline can accept more deltas (flow control window).
    pub fn can_push(&self) -> bool {
        self.in_flight.len() < self.config.max_in_flight
    }

    /// How many deltas should be pushed in the next batch.
    /// Returns 0 if the flow control window is full.
    pub fn next_batch_size(&self) -> usize {
        let window_remaining = self
            .config
            .max_in_flight
            .saturating_sub(self.in_flight.len());
        self.current_batch_size.min(window_remaining)
    }

    /// Record that deltas were pushed (track in-flight).
    pub fn record_push(&mut self, mutation_ids: &[u64]) {
        let now = Instant::now();
        for &mid in mutation_ids {
            self.in_flight.insert(mid, now);
        }
    }

    /// Record an ACK from Origin — update RTT, adjust batch size (AIMD).
    ///
    /// Returns the measured RTT in milliseconds for this ACK.
    pub fn record_ack(&mut self, mutation_id: u64) -> Option<u64> {
        let send_time = self.in_flight.remove(&mutation_id)?;
        let rtt_ms = send_time.elapsed().as_millis() as f64;

        // EWMA update: α = 0.125 (TCP-style smoothing).
        if self.ema_rtt_ms == 0.0 {
            self.ema_rtt_ms = rtt_ms;
        } else {
            self.ema_rtt_ms = 0.875 * self.ema_rtt_ms + 0.125 * rtt_ms;
        }

        // AIMD additive increase: every 8 consecutive ACKs, increase batch by 1.
        self.consecutive_acks += 1;
        if self.consecutive_acks >= 8 {
            self.consecutive_acks = 0;
            self.current_batch_size = (self.current_batch_size + 1).min(self.config.max_batch_size);
        }

        Some(rtt_ms as u64)
    }

    /// Record a rejection from Origin — halve the batch size (AIMD multiplicative decrease).
    pub fn record_reject(&mut self, mutation_id: u64) {
        self.in_flight.remove(&mutation_id);
        self.consecutive_acks = 0;
        // Multiplicative decrease: halve the batch size.
        self.current_batch_size = (self.current_batch_size / 2).max(self.config.min_batch_size);
    }

    /// Update pending queue stats (called when deltas are added/removed).
    pub fn update_pending(&mut self, count: usize, bytes: usize) {
        self.pending_count = count;
        self.pending_bytes = bytes;
    }

    /// Check if the pending queue is at capacity.
    pub fn is_queue_full(&self) -> bool {
        self.pending_count >= self.config.max_pending_count
            || self.pending_bytes >= self.config.max_pending_bytes
    }

    /// Current in-flight count.
    pub fn in_flight_count(&self) -> usize {
        self.in_flight.len()
    }

    /// Current EMA RTT in milliseconds.
    pub fn avg_rtt_ms(&self) -> u64 {
        self.ema_rtt_ms as u64
    }

    /// Current adaptive batch size.
    pub fn current_batch_size(&self) -> usize {
        self.current_batch_size
    }

    /// Pending count.
    pub fn pending_count(&self) -> usize {
        self.pending_count
    }

    /// Pending bytes.
    pub fn pending_bytes(&self) -> usize {
        self.pending_bytes
    }

    /// Clean up in-flight entries older than a timeout (stale ACKs).
    /// Returns the number of timed-out entries cleaned.
    pub fn cleanup_stale(&mut self, timeout: std::time::Duration) -> usize {
        let now = Instant::now();
        let before = self.in_flight.len();
        self.in_flight
            .retain(|_, sent_at| now.duration_since(*sent_at) < timeout);
        let cleaned = before - self.in_flight.len();
        if cleaned > 0 {
            // Treat stale in-flight as losses → multiplicative decrease.
            self.consecutive_acks = 0;
            self.current_batch_size = (self.current_batch_size / 2).max(self.config.min_batch_size);
        }
        cleaned
    }

    /// Build a snapshot of all sync metrics (for health API / monitoring).
    pub fn snapshot(&self, state: &'static str, metrics: &SyncMetrics) -> SyncMetricsSnapshot {
        SyncMetricsSnapshot {
            state,
            pending_count: self.pending_count as u64,
            pending_bytes: self.pending_bytes as u64,
            deltas_pushed: metrics.deltas_pushed.load(Ordering::Relaxed),
            deltas_received: metrics.deltas_received.load(Ordering::Relaxed),
            deltas_rejected: metrics.deltas_rejected.load(Ordering::Relaxed),
            avg_rtt_ms: self.avg_rtt_ms(),
            in_flight: self.in_flight.len() as u64,
            reconnect_count: metrics.reconnect_count.load(Ordering::Relaxed),
            last_sync_ts: metrics.last_sync_ts.load(Ordering::Relaxed),
            checksum_failures: metrics.checksum_failures.load(Ordering::Relaxed),
            current_batch_size: self.current_batch_size as u64,
            conflicts_total: metrics.conflicts_total.load(Ordering::Relaxed),
        }
    }

    /// Reset flow control state on reconnect.
    pub fn reset(&mut self) {
        self.in_flight.clear();
        self.consecutive_acks = 0;
        // Keep the current batch size — don't reset the learned value.
        // The RTT will naturally adapt to the new connection.
    }
}

impl Default for FlowController {
    fn default() -> Self {
        Self::new(FlowControlConfig::default())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_config_values() {
        let cfg = FlowControlConfig::default();
        assert_eq!(cfg.max_in_flight, 64);
        assert_eq!(cfg.min_batch_size, 10);
        assert_eq!(cfg.max_batch_size, 500);
        assert_eq!(cfg.initial_batch_size, 50);
        assert_eq!(cfg.max_pending_count, 10_000);
        assert_eq!(cfg.max_pending_bytes, 50 * 1024 * 1024);
    }

    #[test]
    fn flow_control_window() {
        let mut fc = FlowController::new(FlowControlConfig {
            max_in_flight: 3,
            ..Default::default()
        });

        assert!(fc.can_push());
        assert_eq!(fc.next_batch_size(), 3); // Window = 3, batch = min(50, 3) = 3.

        fc.record_push(&[1, 2, 3]);
        assert!(!fc.can_push());
        assert_eq!(fc.next_batch_size(), 0);
        assert_eq!(fc.in_flight_count(), 3);

        // ACK one — window opens.
        fc.record_ack(2);
        assert!(fc.can_push());
        assert_eq!(fc.next_batch_size(), 1);
    }

    #[test]
    fn aimd_additive_increase() {
        let mut fc = FlowController::new(FlowControlConfig {
            initial_batch_size: 10,
            max_batch_size: 500,
            max_in_flight: 1000,
            ..Default::default()
        });

        assert_eq!(fc.current_batch_size(), 10);

        // 8 consecutive ACKs → batch size increases by 1.
        for i in 0..8 {
            fc.record_push(&[i]);
            fc.record_ack(i);
        }
        assert_eq!(fc.current_batch_size(), 11);

        // 8 more → 12.
        for i in 8..16 {
            fc.record_push(&[i]);
            fc.record_ack(i);
        }
        assert_eq!(fc.current_batch_size(), 12);
    }

    #[test]
    fn aimd_multiplicative_decrease() {
        let mut fc = FlowController::new(FlowControlConfig {
            initial_batch_size: 100,
            min_batch_size: 10,
            max_in_flight: 1000,
            ..Default::default()
        });

        assert_eq!(fc.current_batch_size(), 100);

        // Rejection → halve.
        fc.record_push(&[1]);
        fc.record_reject(1);
        assert_eq!(fc.current_batch_size(), 50);

        // Another rejection → halve again.
        fc.record_push(&[2]);
        fc.record_reject(2);
        assert_eq!(fc.current_batch_size(), 25);

        // Keep halving until floor.
        fc.record_push(&[3]);
        fc.record_reject(3);
        assert_eq!(fc.current_batch_size(), 12);

        fc.record_push(&[4]);
        fc.record_reject(4);
        assert_eq!(fc.current_batch_size(), 10); // Floor.

        fc.record_push(&[5]);
        fc.record_reject(5);
        assert_eq!(fc.current_batch_size(), 10); // Can't go below floor.
    }

    #[test]
    fn rtt_ewma() {
        let mut fc = FlowController::default();
        fc.record_push(&[1]);
        // Can't precisely test RTT since Instant::now() is real time,
        // but we can verify the structure works.
        let rtt = fc.record_ack(1);
        assert!(rtt.is_some());
        assert!(fc.avg_rtt_ms() < 100); // Should be near-instant in test.
    }

    #[test]
    fn bounded_pending_queue_count() {
        let mut fc = FlowController::new(FlowControlConfig {
            max_pending_count: 100,
            max_pending_bytes: 1_000_000,
            ..Default::default()
        });

        fc.update_pending(99, 500);
        assert!(!fc.is_queue_full());

        fc.update_pending(100, 500);
        assert!(fc.is_queue_full());
    }

    #[test]
    fn bounded_pending_queue_bytes() {
        let mut fc = FlowController::new(FlowControlConfig {
            max_pending_count: 100_000,
            max_pending_bytes: 1000,
            ..Default::default()
        });

        fc.update_pending(5, 999);
        assert!(!fc.is_queue_full());

        fc.update_pending(5, 1000);
        assert!(fc.is_queue_full());
    }

    #[test]
    fn cleanup_stale_in_flight() {
        let mut fc = FlowController::new(FlowControlConfig {
            initial_batch_size: 100,
            min_batch_size: 10,
            max_in_flight: 1000,
            ..Default::default()
        });

        fc.record_push(&[1, 2, 3]);
        assert_eq!(fc.in_flight_count(), 3);

        // Cleanup with a very long timeout — nothing should be cleaned.
        let cleaned = fc.cleanup_stale(std::time::Duration::from_secs(3600));
        assert_eq!(cleaned, 0);
        assert_eq!(fc.in_flight_count(), 3);

        // Cleanup with zero timeout — everything should be cleaned.
        let cleaned = fc.cleanup_stale(std::time::Duration::ZERO);
        assert_eq!(cleaned, 3);
        assert_eq!(fc.in_flight_count(), 0);
        // Stale cleanup triggers multiplicative decrease.
        assert_eq!(fc.current_batch_size(), 50);
    }

    #[test]
    fn reset_clears_in_flight() {
        let mut fc = FlowController::default();
        fc.record_push(&[1, 2, 3]);
        assert_eq!(fc.in_flight_count(), 3);

        fc.reset();
        assert_eq!(fc.in_flight_count(), 0);
        // Batch size preserved across reset (learned value).
        assert_eq!(fc.current_batch_size(), 50);
    }

    #[test]
    fn metrics_snapshot() {
        let mut fc = FlowController::default();
        let metrics = SyncMetrics::new();
        metrics.record_push(5);
        metrics.record_received();
        metrics.record_reject();
        metrics.record_reconnect();

        fc.update_pending(42, 8192);

        let snap = fc.snapshot("connected", &metrics);
        assert_eq!(snap.state, "connected");
        assert_eq!(snap.pending_count, 42);
        assert_eq!(snap.pending_bytes, 8192);
        assert_eq!(snap.deltas_pushed, 5);
        assert_eq!(snap.deltas_received, 1);
        assert_eq!(snap.deltas_rejected, 1);
        assert_eq!(snap.reconnect_count, 1);
        assert_eq!(snap.current_batch_size, 50);
    }

    #[test]
    fn ack_unknown_mutation_returns_none() {
        let mut fc = FlowController::default();
        assert!(fc.record_ack(999).is_none());
    }

    #[test]
    fn batch_size_capped_at_max() {
        let mut fc = FlowController::new(FlowControlConfig {
            initial_batch_size: 499,
            max_batch_size: 500,
            max_in_flight: 10_000,
            ..Default::default()
        });

        // 8 ACKs → increase by 1 → 500 (max).
        for i in 0..8 {
            fc.record_push(&[i]);
            fc.record_ack(i);
        }
        assert_eq!(fc.current_batch_size(), 500);

        // 8 more → still 500 (capped).
        for i in 8..16 {
            fc.record_push(&[i]);
            fc.record_ack(i);
        }
        assert_eq!(fc.current_batch_size(), 500);
    }
}
