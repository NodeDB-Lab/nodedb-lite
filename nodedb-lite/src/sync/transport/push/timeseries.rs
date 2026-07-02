//! Timeseries push: drains pending row batches, encodes them as
//! Gorilla-compressed `(ts_block, val_block)` pairs, and ships
//! `TimeseriesPush` frames to Origin.

use std::ops::ControlFlow;
use std::sync::Arc;

use futures::SinkExt;
use tokio::sync::Mutex;
use tokio_tungstenite::tungstenite::Message;

use nodedb_types::sync::wire::{EngineKind, SyncFrame, SyncMessageType, stream_id_for};

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
    let producer_id = client.producer_id().await;
    let epoch = client.accepted_epoch().await;

    for (durable_key, mut batch) in delegate.pending_timeseries_batches().await {
        let stream_id = stream_id_for(EngineKind::Timeseries, &batch.collection);
        if batch.seq == 0 {
            batch.seq = delegate.next_stream_seq(stream_id).await;
            if let Err(e) = delegate.persist_timeseries_seq(&durable_key, &batch).await {
                tracing::warn!(error = %e, "failed to persist stream seq into durable entry; retaining for retry");
                continue;
            }
        }
        let seq = batch.seq;
        let Some(msg) = encode_batch(&lite_id, &batch, producer_id, epoch, seq) else {
            // Empty batch — delete from durable storage so the queue doesn't loop.
            delegate.acknowledge_timeseries_batch(durable_key).await;
            continue;
        };

        let Some(frame) = SyncFrame::try_encode(SyncMessageType::TimeseriesPush, &msg) else {
            tracing::error!(
                collection = %batch.collection, batch_id = batch.batch_id,
                "failed to encode TimeseriesPush frame; dropping batch"
            );
            delegate.acknowledge_timeseries_batch(durable_key).await;
            continue;
        };
        if let Err(e) = send_binary(sink, frame).await {
            tracing::warn!(
                collection = %batch.collection, batch_id = batch.batch_id, error = %e,
                "TimeseriesPush send failed; durable entry retained for re-send on reconnect"
            );
            return ControlFlow::Break(());
        }
        tracing::debug!(
            collection = %batch.collection, batch_id = batch.batch_id,
            samples = msg.sample_count,
            "sent TimeseriesPush to Origin; awaiting ack before deleting durable entry"
        );
        // Mark in-flight keyed by seq (TimeseriesAckMsg echoes applied_seq, not batch_id).
        delegate
            .mark_timeseries_batch_in_flight(seq, durable_key)
            .await;
    }
    ControlFlow::Continue(())
}

/// Pack a single timeseries batch into a `TimeseriesPushMsg` with
/// Gorilla-encoded timestamp and value blocks. Returns `None` if no rows
/// produced a usable `(timestamp, value)` pair.
fn encode_batch(
    lite_id: &str,
    batch: &PendingTimeseriesBatch,
    producer_id: u64,
    epoch: u64,
    seq: u64,
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
        producer_id,
        epoch,
        seq,
    })
}
