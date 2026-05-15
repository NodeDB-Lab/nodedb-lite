//! Timeseries push: drains pending row batches, encodes them as
//! Gorilla-compressed `(ts_block, val_block)` pairs, and ships
//! `TimeseriesPush` frames to Origin.

use std::ops::ControlFlow;
use std::sync::Arc;

use futures::SinkExt;
use tokio::sync::Mutex;
use tokio_tungstenite::tungstenite::Message;

use nodedb_types::sync::wire::{SyncFrame, SyncMessageType};

use super::send::send_binary;
use crate::sync::client::SyncClient;
use crate::sync::outbound::timeseries::PendingTimeseriesBatch;
use crate::sync::transport::delegate::SyncDelegate;

pub(super) async fn push<S>(
    client: &Arc<SyncClient>,
    delegate: &Arc<dyn SyncDelegate>,
    sink: &Arc<Mutex<S>>,
) -> ControlFlow<()>
where
    S: SinkExt<Message> + Unpin,
    S::Error: std::fmt::Display,
{
    let lite_id = format!("{}", client.peer_id());

    for batch in delegate.pending_timeseries_batches() {
        let Some(msg) = encode_batch(&lite_id, &batch) else {
            // Empty batch — drop pending entries for this collection so the
            // queue does not loop on a zero-row batch.
            delegate.acknowledge_timeseries_collection(&batch.collection);
            continue;
        };

        let Some(frame) = SyncFrame::try_encode(SyncMessageType::TimeseriesPush, &msg) else {
            tracing::error!(
                collection = %batch.collection, batch_id = batch.batch_id,
                "failed to encode TimeseriesPush frame; dropping batch"
            );
            continue;
        };
        if let Err(e) = send_binary(sink, frame).await {
            tracing::warn!(
                collection = %batch.collection, batch_id = batch.batch_id, error = %e,
                "TimeseriesPush send failed; re-queuing batch"
            );
            delegate.reject_timeseries_batch(batch);
            return ControlFlow::Break(());
        }
        tracing::debug!(
            collection = %batch.collection, batch_id = batch.batch_id,
            samples = msg.sample_count,
            "sent TimeseriesPush to Origin"
        );
    }
    ControlFlow::Continue(())
}

/// Pack a single timeseries batch into a `TimeseriesPushMsg` with
/// Gorilla-encoded timestamp and value blocks. Returns `None` if no rows
/// produced a usable `(timestamp, value)` pair.
fn encode_batch(
    lite_id: &str,
    batch: &PendingTimeseriesBatch,
) -> Option<nodedb_types::sync::wire::TimeseriesPushMsg> {
    // Time column = first column whose name contains "time"; fall back to col 0.
    let time_col_idx = batch
        .column_names
        .iter()
        .position(|n| n.to_lowercase().contains("time"))
        .unwrap_or(0);
    // Value column = first numeric column that isn't the time col; fall back to col 1.
    let val_col_idx = batch
        .column_names
        .iter()
        .enumerate()
        .position(|(i, _)| i != time_col_idx)
        .unwrap_or(1);

    let mut ts_enc = nodedb_codec::GorillaEncoder::new();
    let mut val_enc = nodedb_codec::GorillaEncoder::new();
    let mut min_ts = i64::MAX;
    let mut max_ts = i64::MIN;
    let mut sample_count: u64 = 0;

    for row in &batch.rows {
        let ts_ms: i64 = match row.get(time_col_idx) {
            Some(nodedb_types::value::Value::Integer(i)) => *i / 1000, // micros → ms
            Some(nodedb_types::value::Value::NaiveDateTime(dt)) => dt.unix_millis(),
            _ => continue,
        };
        let val: f64 = match row.get(val_col_idx) {
            Some(nodedb_types::value::Value::Float(f)) => *f,
            Some(nodedb_types::value::Value::Integer(i)) => *i as f64,
            _ => 0.0,
        };

        ts_enc.encode(ts_ms, 0.0);
        val_enc.encode(sample_count as i64, val);
        min_ts = min_ts.min(ts_ms);
        max_ts = max_ts.max(ts_ms);
        sample_count += 1;
    }

    if sample_count == 0 {
        return None;
    }

    Some(nodedb_types::sync::wire::TimeseriesPushMsg {
        lite_id: lite_id.to_string(),
        collection: batch.collection.clone(),
        ts_block: ts_enc.finish(),
        val_block: val_enc.finish(),
        series_block: Vec::new(),
        sample_count,
        min_ts,
        max_ts,
        watermarks: std::collections::HashMap::new(),
    })
}
