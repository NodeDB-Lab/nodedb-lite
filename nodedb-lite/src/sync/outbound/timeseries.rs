//! Timeseries insert outbound queue for Lite sync.
//!
//! When a timeseries-profile columnar collection is written on Lite, rows are
//! enqueued here rather than in `ColumnarOutbound`. The sync transport drains
//! this queue and sends `TimeseriesPush` (0x40) wire frames to Origin.
//!
//! Each pending batch holds a collection name and a list of raw rows (one
//! `Vec<Value>` per row in schema column order). The transport converts rows
//! to Gorilla-encoded ts/val blocks when building the wire message, using the
//! first `TIMESTAMP` column as the time key and the first `FLOAT64` column as
//! the metric value.

use nodedb_types::value::Value;

use super::queue::{BatchIdGen, PendingQueue};

/// One pending row batch for a timeseries collection.
#[derive(Debug, Clone)]
pub struct PendingTimeseriesBatch {
    /// Monotonic batch ID for ACK correlation.
    pub batch_id: u64,
    /// Collection name.
    pub collection: String,
    /// Column names in schema order (mirrors `ColumnarSchema::columns`).
    pub column_names: Vec<String>,
    /// Rows in schema column order.
    pub rows: Vec<Vec<Value>>,
}

#[derive(Debug, Default)]
pub struct TimeseriesOutbound {
    queue: PendingQueue<PendingTimeseriesBatch>,
    ids: BatchIdGen,
}

impl TimeseriesOutbound {
    pub const fn new() -> Self {
        Self {
            queue: PendingQueue::new(),
            ids: BatchIdGen::new(),
        }
    }

    /// Enqueue a single row, coalescing into the open batch for this
    /// collection if one exists.
    pub fn enqueue_row(&self, collection: &str, column_names: Vec<String>, row: Vec<Value>) {
        let mut row_slot = Some(row);
        let appended = self.queue.with_first_mut(
            |b| b.collection == collection,
            |b| {
                if let Some(r) = row_slot.take() {
                    b.rows.push(r);
                }
            },
        );
        if appended.is_some() {
            return;
        }
        let row = row_slot.expect("row preserved when no open batch matched");
        self.queue.push(PendingTimeseriesBatch {
            batch_id: self.ids.next(),
            collection: collection.to_string(),
            column_names,
            rows: vec![row],
        });
    }

    pub fn drain_pending(&self) -> Vec<PendingTimeseriesBatch> {
        self.queue.drain()
    }

    pub fn acknowledge_batch(&self, batch_id: u64) {
        self.queue.retain(|b| b.batch_id != batch_id);
    }

    /// Drop every pending batch for a collection — used when Origin returns a
    /// collection-level `TimeseriesAck` covering all in-flight batches.
    pub fn acknowledge_collection(&self, collection: &str) {
        self.queue.retain(|b| b.collection != collection);
    }

    pub fn requeue_batch(&self, batch: PendingTimeseriesBatch) {
        self.queue.requeue(batch);
    }

    pub fn pending_count(&self) -> usize {
        self.queue.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn enqueue_and_drain() {
        let q = TimeseriesOutbound::new();
        q.enqueue_row(
            "metrics",
            vec!["time".into(), "value".into()],
            vec![Value::Integer(1000), Value::Float(1.0)],
        );
        q.enqueue_row(
            "metrics",
            vec!["time".into(), "value".into()],
            vec![Value::Integer(2000), Value::Float(2.0)],
        );

        let batches = q.drain_pending();
        assert_eq!(batches.len(), 1);
        assert_eq!(batches[0].rows.len(), 2);
        assert!(q.drain_pending().is_empty());
    }

    #[test]
    fn acknowledge_removes_batch() {
        let q = TimeseriesOutbound::new();
        q.enqueue_row(
            "m",
            vec!["time".into(), "value".into()],
            vec![Value::Integer(1000), Value::Float(1.0)],
        );
        let batches = q.drain_pending();
        let id = batches[0].batch_id;
        q.acknowledge_batch(id);
        assert!(q.drain_pending().is_empty());
    }

    #[test]
    fn requeue_retries_on_next_drain() {
        let q = TimeseriesOutbound::new();
        q.enqueue_row(
            "m",
            vec!["time".into(), "value".into()],
            vec![Value::Integer(1000), Value::Float(1.0)],
        );
        let batches = q.drain_pending();
        q.requeue_batch(batches.into_iter().next().unwrap());
        let retried = q.drain_pending();
        assert_eq!(retried.len(), 1);
    }
}
