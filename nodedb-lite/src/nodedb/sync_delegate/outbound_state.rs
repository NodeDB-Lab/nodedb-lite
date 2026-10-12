// SPDX-License-Identifier: Apache-2.0

//! Free functions behind the outbound bookkeeping `SyncDelegate` methods:
//! the reconnect in-flight reset and the per-stream seq frontier.
//!
//! Called from the thin delegation methods in `delegate_impl.rs`.

use crate::nodedb::core::NodeDbLite;
use crate::storage::engine::StorageEngine;

pub(super) async fn clear_engine_in_flight_impl<S: StorageEngine>(db: &NodeDbLite<S>) {
    if let Some(q) = &db.columnar_outbound {
        q.clear_in_flight().await;
    }
    if let Some(q) = &db.timeseries_outbound {
        q.clear_in_flight().await;
    }
    if let Some(q) = &db.vector_outbound {
        q.clear_in_flight().await;
    }
    if let Some(q) = &db.fts_outbound {
        q.clear_in_flight().await;
    }
    if let Some(q) = &db.spatial_outbound {
        q.clear_in_flight().await;
    }
    if let Some(q) = &db.kv_outbound {
        q.clear_in_flight().await;
    }
}

pub(super) async fn next_stream_seq_impl<S: StorageEngine>(
    db: &NodeDbLite<S>,
    stream_id: u64,
) -> u64 {
    match db.stream_seq.next_seq(stream_id).await {
        Ok(seq) => seq,
        Err(e) => {
            tracing::warn!(
                stream_id,
                error = %e,
                "SyncDelegate::next_stream_seq: persist failed; using sentinel 0"
            );
            0
        }
    }
}

pub(super) async fn record_stream_ack_impl<S: StorageEngine>(
    db: &NodeDbLite<S>,
    stream_id: u64,
    applied_seq: u64,
) {
    if let Err(e) = db.stream_seq.record_ack(stream_id, applied_seq).await {
        tracing::warn!(
            stream_id,
            applied_seq,
            error = %e,
            "SyncDelegate::record_stream_ack: persist failed; ignoring"
        );
    }
}
