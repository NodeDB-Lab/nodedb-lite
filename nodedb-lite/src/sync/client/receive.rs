//! Receive-path handlers: shape snapshot/delta, clock sync, sequence gap detection, resync.

use nodedb_types::sync::wire::{
    ResyncReason, ResyncRequestMsg, ShapeDeltaMsg, ShapeSnapshotMsg, VectorClockSyncMsg,
};

use super::state::SyncClient;

impl SyncClient {
    /// Process a ShapeSnapshot from Origin.
    pub async fn handle_shape_snapshot(&self, msg: &ShapeSnapshotMsg) {
        let mut shapes = self.shapes.lock().await;
        shapes.mark_snapshot_loaded(&msg.shape_id, msg.snapshot_lsn);
        tracing::info!(
            shape_id = %msg.shape_id,
            lsn = msg.snapshot_lsn,
            doc_count = msg.doc_count,
            "shape snapshot received"
        );
    }

    /// Process a ShapeDelta from Origin.
    pub async fn handle_shape_delta(&self, msg: &ShapeDeltaMsg) {
        let mut shapes = self.shapes.lock().await;
        shapes.advance_lsn(&msg.shape_id, msg.lsn);
        tracing::debug!(
            shape_id = %msg.shape_id,
            collection = %msg.collection,
            doc_id = %msg.document_id,
            lsn = msg.lsn,
            "shape delta received"
        );
    }

    /// Process a VectorClockSync from Origin.
    pub async fn handle_clock_sync(&self, msg: &VectorClockSyncMsg) {
        let mut clock = self.clock.lock().await;
        for (peer_hex, &counter) in &msg.clocks {
            if let Ok(peer_id) = u64::from_str_radix(peer_hex, 16) {
                clock.advance(peer_id, counter);
            }
        }
    }

    /// Check an incoming ShapeDelta for sequence gaps.
    ///
    /// For each shape, we track the last LSN received. If the incoming LSN
    /// is not contiguous (gap > 1), this indicates missing deltas in the stream.
    /// Returns `Some(ResyncRequestMsg)` if a gap is detected, `None` otherwise.
    pub async fn check_sequence_gap(&self, shape_id: &str, lsn: u64) -> Option<ResyncRequestMsg> {
        if *self.resync_requested.lock().await {
            return None;
        }

        let mut tracker = self.last_seen_lsn.lock().await;
        if let Some(&last_lsn) = tracker.get(shape_id)
            && lsn > last_lsn + 1
        {
            tracing::warn!(
                shape_id,
                expected = last_lsn + 1,
                received = lsn,
                "sequence gap detected in incoming delta stream"
            );
            tracker.insert(shape_id.to_string(), lsn);

            *self.resync_requested.lock().await = true;

            return Some(ResyncRequestMsg {
                reason: ResyncReason::SequenceGap {
                    expected: last_lsn + 1,
                    received: lsn,
                },
                from_mutation_id: last_lsn + 1,
                collection: String::new(),
            });
        }
        tracker.insert(shape_id.to_string(), lsn);
        None
    }

    /// Reset sequence tracking state on reconnect.
    pub async fn reset_sequence_tracking(&self) {
        self.last_seen_lsn.lock().await.clear();
        *self.resync_requested.lock().await = false;
        *self.pending_resync.lock().await = None;
    }

    /// Store a pending re-sync request (set by gap detection in receive loop).
    pub async fn set_pending_resync(&self, msg: ResyncRequestMsg) {
        *self.pending_resync.lock().await = Some(msg);
    }

    /// Take the pending re-sync request (consumed by delta push loop).
    pub async fn take_pending_resync(&self) -> Option<ResyncRequestMsg> {
        self.pending_resync.lock().await.take()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sync::client::SyncConfig;

    fn make_config() -> SyncConfig {
        SyncConfig::new("wss://localhost:9090/sync", "test.jwt.token")
    }

    #[tokio::test]
    async fn shape_snapshot_updates_manager() {
        let client = SyncClient::new(make_config(), 1);
        {
            let mut shapes = client.shapes().lock().await;
            shapes.subscribe(nodedb_types::sync::shape::ShapeDefinition {
                shape_id: "s1".into(),
                tenant_id: 1,
                shape_type: nodedb_types::sync::shape::ShapeType::Vector {
                    collection: "vecs".into(),
                    field_name: None,
                },
                description: "test".into(),
                field_filter: vec![],
            });
        }

        client
            .handle_shape_snapshot(&ShapeSnapshotMsg {
                shape_id: "s1".into(),
                data: Vec::new(),
                snapshot_lsn: 100,
                doc_count: 50,
            })
            .await;

        let shapes = client.shapes().lock().await;
        let sub = shapes.get("s1").unwrap();
        assert!(sub.snapshot_loaded);
        assert_eq!(sub.last_lsn, 100);
    }

    #[tokio::test]
    async fn sequence_gap_detection_no_gap() {
        let client = SyncClient::new(make_config(), 1);
        assert!(client.check_sequence_gap("s1", 1).await.is_none());
        assert!(client.check_sequence_gap("s1", 2).await.is_none());
        assert!(client.check_sequence_gap("s1", 3).await.is_none());
    }

    #[tokio::test]
    async fn sequence_gap_detection_with_gap() {
        let client = SyncClient::new(make_config(), 1);
        assert!(client.check_sequence_gap("s1", 1).await.is_none());
        let resync = client.check_sequence_gap("s1", 5).await;
        assert!(resync.is_some());
        let msg = resync.unwrap();
        assert_eq!(msg.from_mutation_id, 2);
        assert!(matches!(
            msg.reason,
            ResyncReason::SequenceGap {
                expected: 2,
                received: 5
            }
        ));
    }

    #[tokio::test]
    async fn sequence_gap_only_one_resync_per_connection() {
        let client = SyncClient::new(make_config(), 1);
        assert!(client.check_sequence_gap("s1", 1).await.is_none());
        assert!(client.check_sequence_gap("s1", 10).await.is_some());
        assert!(client.check_sequence_gap("s1", 20).await.is_none());
    }

    #[tokio::test]
    async fn reset_sequence_tracking_clears_state() {
        let client = SyncClient::new(make_config(), 1);
        assert!(client.check_sequence_gap("s1", 1).await.is_none());
        assert!(client.check_sequence_gap("s1", 10).await.is_some());

        client.reset_sequence_tracking().await;

        assert!(client.check_sequence_gap("s1", 1).await.is_none());
        assert!(client.check_sequence_gap("s1", 5).await.is_some());
    }
}
