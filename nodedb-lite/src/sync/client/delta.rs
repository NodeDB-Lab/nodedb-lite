//! Delta push / ack / reject.

use nodedb_types::sync::wire::{DeltaAckMsg, DeltaPushMsg, DeltaRejectMsg};

use super::state::SyncClient;
use crate::engine::crdt::engine::PendingDelta;
use crate::sync::compensation::CompensationEvent;

impl SyncClient {
    /// Build DeltaPush messages from pending deltas.
    ///
    /// Respects the flow control window: returns at most `next_batch_size()`
    /// deltas. Each message includes a CRC32C checksum of the delta payload
    /// for integrity verification at Origin.
    pub async fn build_delta_pushes(&self, pending: &[PendingDelta]) -> Vec<DeltaPushMsg> {
        let flow = self.flow.lock().await;
        let batch_limit = flow.next_batch_size();
        drop(flow);

        if batch_limit == 0 {
            return Vec::new();
        }

        let device_valid_time_ms = crate::runtime::now_millis() as i64;

        pending
            .iter()
            .take(batch_limit)
            .map(|delta| DeltaPushMsg {
                collection: delta.collection.clone(),
                document_id: delta.document_id.clone(),
                checksum: crc32c::crc32c(&delta.delta_bytes),
                delta: delta.delta_bytes.clone(),
                peer_id: self.peer_id,
                mutation_id: delta.mutation_id,
                device_valid_time_ms: Some(device_valid_time_ms),
                // producer_id, epoch, and seq are overwritten with real producer/epoch/stable-seq in push_crdt_deltas.
                producer_id: 0,
                epoch: 0,
                seq: 0,
            })
            .collect()
    }

    /// Record that deltas were pushed (update flow control in-flight tracking).
    pub async fn record_push(&self, mutation_ids: &[u64]) {
        let mut flow = self.flow.lock().await;
        flow.record_push(mutation_ids);
        self.metrics.record_push(mutation_ids.len() as u64);
    }

    /// Process a DeltaAck from Origin.
    pub async fn handle_delta_ack(&self, ack: &DeltaAckMsg) {
        let mut clock = self.clock.lock().await;
        clock.advance(0, ack.lsn); // peer 0 = Origin convention.
        drop(clock);

        let mut flow = self.flow.lock().await;
        if let Some(rtt_ms) = flow.record_ack(ack.mutation_id) {
            tracing::debug!(
                mutation_id = ack.mutation_id,
                lsn = ack.lsn,
                rtt_ms,
                batch_size = flow.current_batch_size(),
                "delta acknowledged"
            );
        } else {
            tracing::debug!(
                mutation_id = ack.mutation_id,
                lsn = ack.lsn,
                "delta acknowledged (no in-flight entry)"
            );
        }

        if let Some(skew_ms) = ack.clock_skew_warning_ms {
            tracing::warn!(
                mutation_id = ack.mutation_id,
                skew_ms,
                "Origin reports device clock skew exceeds tolerance"
            );
            self.metrics.record_clock_skew_warning();
        }
    }

    /// Process a DeltaReject from Origin.
    pub async fn handle_delta_reject(&self, reject: &DeltaRejectMsg) {
        tracing::warn!(
            mutation_id = reject.mutation_id,
            reason = %reject.reason,
            "delta rejected by Origin"
        );

        {
            let mut flow = self.flow.lock().await;
            flow.record_reject(reject.mutation_id);
        }
        self.metrics.record_reject();

        if let Some(hint) = &reject.compensation {
            use nodedb_types::sync::compensation::CompensationHint;
            let is_conflict = matches!(
                hint,
                CompensationHint::UniqueViolation { .. }
                    | CompensationHint::ForeignKeyMissing { .. }
                    | CompensationHint::SchemaViolation { .. }
            );
            if is_conflict {
                self.metrics.record_conflict(&reject.reason);
            }

            self.compensation.dispatch(CompensationEvent {
                mutation_id: reject.mutation_id,
                collection: String::new(),
                document_id: String::new(),
                hint: hint.clone(),
            });
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sync::client::SyncConfig;
    use std::sync::Arc;

    fn make_config() -> SyncConfig {
        SyncConfig::new("wss://localhost:9090/sync", "test.jwt.token")
    }

    #[tokio::test]
    async fn build_delta_pushes() {
        let client = SyncClient::new(make_config(), 42);
        let pending = vec![
            PendingDelta {
                mutation_id: 1,
                collection: "orders".into(),
                document_id: "o1".into(),
                delta_bytes: vec![1, 2, 3],
                seq: 0,
            },
            PendingDelta {
                mutation_id: 2,
                collection: "users".into(),
                document_id: "u1".into(),
                delta_bytes: vec![4, 5, 6],
                seq: 0,
            },
        ];

        let msgs = client.build_delta_pushes(&pending).await;
        assert_eq!(msgs.len(), 2);
        assert_eq!(msgs[0].peer_id, 42);
        assert_eq!(msgs[0].mutation_id, 1);
        assert_eq!(msgs[1].collection, "users");
        assert!(msgs[0].device_valid_time_ms.is_some());
        assert!(msgs[0].device_valid_time_ms.unwrap() > 0);
    }

    #[tokio::test]
    async fn handle_delta_ack_advances_clock() {
        let client = SyncClient::new(make_config(), 1);
        client
            .handle_delta_ack(&DeltaAckMsg {
                mutation_id: 1,
                lsn: 42,
                clock_skew_warning_ms: None,
                applied_seq: 0,
                status: nodedb_types::sync::wire::AckStatus::Applied,
            })
            .await;

        let clock = client.clock().lock().await;
        assert_eq!(clock.get(0), 42); // peer 0 = Origin.
    }

    #[tokio::test]
    async fn handle_delta_reject_dispatches_compensation() {
        let client = SyncClient::new(make_config(), 1);

        let count = Arc::new(std::sync::atomic::AtomicU32::new(0));
        let count_clone = count.clone();
        client.set_compensation_handler(Arc::new(move |_: CompensationEvent| {
            count_clone.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        }));

        client
            .handle_delta_reject(&DeltaRejectMsg {
                mutation_id: 1,
                reason: "unique violation".into(),
                compensation: Some(
                    nodedb_types::sync::compensation::CompensationHint::UniqueViolation {
                        field: "email".into(),
                        conflicting_value: "a@b.com".into(),
                    },
                ),
            })
            .await;

        assert_eq!(count.load(std::sync::atomic::Ordering::Relaxed), 1);
    }

    #[tokio::test]
    async fn delta_push_includes_crc32c() {
        let client = SyncClient::new(make_config(), 42);
        let delta_bytes = vec![1, 2, 3, 4, 5];
        let expected_crc = crc32c::crc32c(&delta_bytes);
        let pending = vec![PendingDelta {
            mutation_id: 1,
            collection: "test".into(),
            document_id: "d1".into(),
            delta_bytes,
            seq: 0,
        }];
        let msgs = client.build_delta_pushes(&pending).await;
        assert_eq!(msgs[0].checksum, expected_crc);
        assert_ne!(msgs[0].checksum, 0);
    }

    #[tokio::test]
    async fn flow_control_pauses_when_window_full() {
        let client = SyncClient::with_flow_control(
            make_config(),
            1,
            crate::sync::flow_control::FlowControlConfig {
                max_in_flight: 2,
                initial_batch_size: 10,
                ..Default::default()
            },
        );
        let pending = vec![
            PendingDelta {
                mutation_id: 1,
                collection: "a".into(),
                document_id: "d1".into(),
                delta_bytes: vec![1],
                seq: 0,
            },
            PendingDelta {
                mutation_id: 2,
                collection: "a".into(),
                document_id: "d2".into(),
                delta_bytes: vec![2],
                seq: 0,
            },
            PendingDelta {
                mutation_id: 3,
                collection: "a".into(),
                document_id: "d3".into(),
                delta_bytes: vec![3],
                seq: 0,
            },
        ];

        let msgs = client.build_delta_pushes(&pending).await;
        assert_eq!(msgs.len(), 2);

        client.record_push(&[1, 2]).await;

        let msgs = client.build_delta_pushes(&pending).await;
        assert_eq!(msgs.len(), 0);

        client
            .handle_delta_ack(&DeltaAckMsg {
                mutation_id: 1,
                lsn: 10,
                clock_skew_warning_ms: None,
                applied_seq: 0,
                status: nodedb_types::sync::wire::AckStatus::Applied,
            })
            .await;
        let msgs = client.build_delta_pushes(&pending).await;
        assert_eq!(msgs.len(), 1);
    }
}
