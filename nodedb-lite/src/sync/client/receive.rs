//! Receive-path handlers: shape snapshot/delta, clock sync, sequence gap detection, resync.

use nodedb_types::sync::wire::{
    ArrayAckMsg, ResyncReason, ResyncRequestMsg, ShapeDeltaMsg, ShapeSnapshotMsg,
    VectorClockSyncMsg,
};

use super::state::SyncClient;

impl SyncClient {
    /// Process a ShapeSnapshot from Origin.
    ///
    /// Marks the shape as snapshot-loaded and re-bases the sequence tracker
    /// so that gap detection restarts from the snapshot LSN rather than any
    /// stale watermark from before the resync. Also clears the resync gate so
    /// a future gap on this or another shape can trigger a new request.
    pub async fn handle_shape_snapshot(&self, msg: &ShapeSnapshotMsg) {
        let mut shapes = self.shapes.lock().await;
        shapes.mark_snapshot_loaded(&msg.shape_id, msg.snapshot_lsn);
        drop(shapes);

        // Re-base the per-shape LSN tracker at the snapshot watermark so the
        // gap detector does not immediately fire again after the resync.
        self.last_seen_lsn
            .lock()
            .await
            .insert(msg.shape_id.clone(), msg.snapshot_lsn);

        // Clear the resync gate so future gaps can trigger new requests.
        *self.resync_requested.lock().await = false;
        *self.pending_resync.lock().await = None;

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
                shape_id: shape_id.to_string(),
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

    /// Merge an `ArrayAck` into the pending-ack map.
    ///
    /// Per-array, only the ack with the highest HLC is kept — `ack_hlc_bytes` is
    /// stored in the same 18-byte big-endian layout used by `nodedb_array::sync::Hlc`,
    /// so byte-wise comparison gives the correct temporal ordering. This means no
    /// ack is silently lost: if two acks arrive for the same array before the push
    /// loop drains them, the one with the higher frontier is retained, which is
    /// exactly what Origin needs to advance its GC cursor.
    pub async fn set_pending_array_ack(&self, msg: ArrayAckMsg) {
        let mut map = self.pending_array_ack.lock().await;
        let entry = map.entry(msg.array.clone()).or_insert_with(|| msg.clone());
        if msg.ack_hlc_bytes > entry.ack_hlc_bytes {
            *entry = msg;
        }
    }

    /// Drain all pending `ArrayAck`s (consumed by the push loop).
    ///
    /// Returns all per-array acks accumulated since the last drain.  The map is
    /// cleared so new acks can accumulate for the next tick.
    pub async fn drain_pending_array_acks(&self) -> Vec<ArrayAckMsg> {
        let mut map = self.pending_array_ack.lock().await;
        if map.is_empty() {
            return Vec::new();
        }
        let drained: Vec<ArrayAckMsg> = map.drain().map(|(_, v)| v).collect();
        drained
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
    async fn array_ack_merge_keeps_highest_hlc() {
        let client = SyncClient::new(make_config(), 1);

        // Lower HLC bytes first.
        let lower = ArrayAckMsg {
            array: "arr1".into(),
            replica_id: 1,
            ack_hlc_bytes: [0x00; 18],
            applied_seq: 0,
            status: nodedb_types::sync::wire::AckStatus::Applied,
        };
        let higher = ArrayAckMsg {
            array: "arr1".into(),
            replica_id: 1,
            ack_hlc_bytes: [0xFF; 18],
            applied_seq: 0,
            status: nodedb_types::sync::wire::AckStatus::Applied,
        };

        client.set_pending_array_ack(lower).await;
        client.set_pending_array_ack(higher.clone()).await;

        let drained = client.drain_pending_array_acks().await;
        assert_eq!(drained.len(), 1);
        assert_eq!(drained[0].ack_hlc_bytes, higher.ack_hlc_bytes);
    }

    #[tokio::test]
    async fn array_ack_merge_keeps_higher_over_lower() {
        let client = SyncClient::new(make_config(), 1);

        // Insert higher first, then lower — should still keep higher.
        let mut higher_bytes = [0x00u8; 18];
        higher_bytes[0] = 0x10;
        let mut lower_bytes = [0x00u8; 18];
        lower_bytes[0] = 0x01;

        client
            .set_pending_array_ack(ArrayAckMsg {
                array: "arr2".into(),
                replica_id: 1,
                ack_hlc_bytes: higher_bytes,
                applied_seq: 0,
                status: nodedb_types::sync::wire::AckStatus::Applied,
            })
            .await;
        client
            .set_pending_array_ack(ArrayAckMsg {
                array: "arr2".into(),
                replica_id: 1,
                ack_hlc_bytes: lower_bytes,
                applied_seq: 0,
                status: nodedb_types::sync::wire::AckStatus::Applied,
            })
            .await;

        let drained = client.drain_pending_array_acks().await;
        assert_eq!(drained.len(), 1);
        assert_eq!(drained[0].ack_hlc_bytes, higher_bytes);
    }

    #[tokio::test]
    async fn array_ack_merge_separate_arrays_both_drained() {
        let client = SyncClient::new(make_config(), 1);

        client
            .set_pending_array_ack(ArrayAckMsg {
                array: "arr_a".into(),
                replica_id: 1,
                ack_hlc_bytes: [0x01; 18],
                applied_seq: 0,
                status: nodedb_types::sync::wire::AckStatus::Applied,
            })
            .await;
        client
            .set_pending_array_ack(ArrayAckMsg {
                array: "arr_b".into(),
                replica_id: 1,
                ack_hlc_bytes: [0x02; 18],
                applied_seq: 0,
                status: nodedb_types::sync::wire::AckStatus::Applied,
            })
            .await;

        let mut drained = client.drain_pending_array_acks().await;
        drained.sort_by(|a, b| a.array.cmp(&b.array));
        assert_eq!(drained.len(), 2);
        assert_eq!(drained[0].array, "arr_a");
        assert_eq!(drained[1].array, "arr_b");
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
