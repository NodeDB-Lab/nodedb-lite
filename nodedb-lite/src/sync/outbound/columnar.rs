//! Columnar insert outbound queue for Lite sync.
//!
//! When `ColumnarEngine::insert` is called on Lite, it enqueues rows here.
//! The sync transport drains this queue and sends `ColumnarInsert` wire
//! frames to Origin. Each batch gets a monotonic `batch_id` for ACK
//! correlation.
//!
//! The queue is in-memory only (no durable persistence). If Lite restarts
//! before a batch is ACKed, the rows are already in the local `ColumnarEngine`
//! segments; full catch-up replay is future work. PREVIEW targets
//! live-session replication (device never goes offline between insert and
//! sync).

use nodedb_types::value::Value;

use super::queue::{BatchIdGen, PendingQueue};

/// A single pending batch of columnar rows awaiting sync to Origin.
#[derive(Debug, Clone)]
pub struct PendingColumnarBatch {
    /// Monotonic batch ID (per-collection, Lite-assigned).
    pub batch_id: u64,
    /// Collection name.
    pub collection: String,
    /// Rows in schema column order, one `Vec<Value>` per row.
    pub rows: Vec<Vec<Value>>,
    /// MessagePack-serialized `ColumnarSchema` hint. May be empty.
    pub schema_bytes: Vec<u8>,
}

/// Thread-safe outbound queue for columnar inserts.
///
/// Held by `NodeDbLite` and shared with `ColumnarEngine` via `Arc`.
#[derive(Debug, Default)]
pub struct ColumnarOutbound {
    queue: PendingQueue<PendingColumnarBatch>,
    ids: BatchIdGen,
}

impl ColumnarOutbound {
    pub const fn new() -> Self {
        Self {
            queue: PendingQueue::new(),
            ids: BatchIdGen::new(),
        }
    }

    /// Enqueue a single row for a collection.
    ///
    /// Rows for the same collection are coalesced into a single batch if a
    /// pending batch for that collection already exists; otherwise a new
    /// batch is created with a fresh `batch_id`.
    pub fn enqueue_row(&self, collection: &str, row: Vec<Value>, schema_bytes: Vec<u8>) {
        // `with_first_mut` consumes `row` only on the matched path, so we use
        // an `Option` shuttle to recover ownership when no open batch exists.
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
        self.queue.push(PendingColumnarBatch {
            batch_id: self.ids.next(),
            collection: collection.to_string(),
            rows: vec![row],
            schema_bytes,
        });
    }

    /// Drain all pending batches for sending.
    pub fn drain_pending(&self) -> Vec<PendingColumnarBatch> {
        self.queue.drain()
    }

    /// Remove the batch with the given `batch_id` (ACK path; no-op if absent).
    pub fn acknowledge_batch(&self, batch_id: u64) {
        self.queue.retain(|b| b.batch_id != batch_id);
    }

    /// Re-queue a rejected batch at the head for retry on the next drain.
    pub fn requeue_batch(&self, batch: PendingColumnarBatch) {
        self.queue.requeue(batch);
    }

    /// Number of pending batches (diagnostics).
    pub fn pending_count(&self) -> usize {
        self.queue.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn enqueue_and_drain() {
        let q = ColumnarOutbound::new();
        q.enqueue_row("metrics", vec![Value::Integer(1)], Vec::new());
        q.enqueue_row("metrics", vec![Value::Integer(2)], Vec::new());

        let batches = q.drain_pending();
        assert_eq!(batches.len(), 1);
        assert_eq!(batches[0].collection, "metrics");
        assert_eq!(batches[0].rows.len(), 2);
        assert!(q.drain_pending().is_empty());
    }

    #[test]
    fn separate_collections_separate_batches() {
        let q = ColumnarOutbound::new();
        q.enqueue_row("a", vec![Value::Integer(1)], Vec::new());
        q.enqueue_row("b", vec![Value::Integer(2)], Vec::new());

        let batches = q.drain_pending();
        assert_eq!(batches.len(), 2);
    }

    #[test]
    fn acknowledge_removes_batch() {
        let q = ColumnarOutbound::new();
        q.enqueue_row("m", vec![Value::Integer(1)], Vec::new());
        let batches = q.drain_pending();
        let id = batches[0].batch_id;
        q.acknowledge_batch(id);
        assert!(q.drain_pending().is_empty());
    }

    #[test]
    fn requeue_retries_on_next_drain() {
        let q = ColumnarOutbound::new();
        q.enqueue_row("m", vec![Value::Integer(1)], Vec::new());
        let batches = q.drain_pending();
        q.requeue_batch(batches.into_iter().next().unwrap());

        let retried = q.drain_pending();
        assert_eq!(retried.len(), 1);
        assert_eq!(retried[0].rows.len(), 1);
    }
}
