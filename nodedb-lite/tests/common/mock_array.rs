//! Array state behind `MockDelegate`: the catch-up round, recorded snapshot
//! frames, and the push-side queue of local ops.
//!
//! Every container is a `std::sync::Mutex` so assertions can read it from
//! outside an async context.

use std::collections::HashMap;
use std::sync::Mutex;
use std::sync::atomic::{AtomicU64, Ordering};

use nodedb_array::sync::hlc::Hlc;
use nodedb_lite::sync::array::PendingArrayOp;
use nodedb_types::sync::wire::{
    AckStatus, ArrayAckMsg, ArrayCatchupRequestMsg, ArrayRejectMsg, ArrayRejectReason,
    ArraySchemaSyncMsg, ArraySnapshotChunkMsg, ArraySnapshotMsg,
};

#[derive(Default)]
pub struct MockArrayState {
    catchups: Mutex<Vec<ArrayCatchupRequestMsg>>,
    catchup_rounds: AtomicU64,
    snapshot_frames: Mutex<Vec<String>>,
    pending: Mutex<Vec<PendingArrayOp>>,
    retired: Mutex<Vec<Hlc>>,
    schemas: Mutex<HashMap<String, ArraySchemaSyncMsg>>,
}

impl MockArrayState {
    /// Requests `begin_array_catchup` returns on every session.
    pub fn set_catchups(&self, requests: Vec<ArrayCatchupRequestMsg>) {
        *self.catchups.lock().expect("catchups lock") = requests;
    }

    /// How many times `begin_array_catchup` was invoked.
    pub fn catchup_rounds(&self) -> u64 {
        self.catchup_rounds.load(Ordering::Relaxed)
    }

    /// Snapshot frames routed to the delegate, in order: `header:{array}` and
    /// `chunk:{array}:{index}`.
    pub fn snapshot_frames(&self) -> Vec<String> {
        self.snapshot_frames
            .lock()
            .expect("snapshot_frames lock")
            .clone()
    }

    /// Queue local ops for the push loop. Kept sorted by HLC, as the durable
    /// queue is.
    pub fn enqueue(&self, ops: Vec<PendingArrayOp>) {
        let mut pending = self.pending.lock().expect("pending lock");
        pending.extend(ops);
        pending.sort_by_key(|a| a.hlc);
    }

    /// Register the `ArraySchema` body announced for `msg.array`.
    pub fn set_schema(&self, msg: ArraySchemaSyncMsg) {
        self.schemas
            .lock()
            .expect("schemas lock")
            .insert(msg.array.clone(), msg);
    }

    /// Ops still queued, in HLC order.
    pub fn queued(&self) -> Vec<PendingArrayOp> {
        self.pending.lock().expect("pending lock").clone()
    }

    /// HLCs passed to `retire_array_ops`, in order.
    pub fn retired(&self) -> Vec<Hlc> {
        self.retired.lock().expect("retired lock").clone()
    }

    pub fn begin_catchup(&self) -> Vec<ArrayCatchupRequestMsg> {
        self.catchup_rounds.fetch_add(1, Ordering::Relaxed);
        self.catchups.lock().expect("catchups lock").clone()
    }

    /// Mirror the real delegate: a `ShapeInvalid` reject drops the op, a
    /// `RetentionFloor` reject asks for a snapshot, every other reason keeps
    /// the op queued.
    pub fn reject(&self, msg: &ArrayRejectMsg) -> Option<ArrayCatchupRequestMsg> {
        match msg.reason {
            ArrayRejectReason::ShapeInvalid => {
                let hlc = Hlc::from_bytes(&msg.op_hlc_bytes);
                self.pending
                    .lock()
                    .expect("pending lock")
                    .retain(|op| op.hlc != hlc);
                None
            }
            ArrayRejectReason::RetentionFloor => Some(ArrayCatchupRequestMsg {
                array: msg.array.clone(),
                from_hlc_bytes: [0u8; 18],
            }),
            _ => None,
        }
    }

    pub fn record_header(&self, msg: &ArraySnapshotMsg) {
        self.snapshot_frames
            .lock()
            .expect("snapshot_frames lock")
            .push(format!("header:{}", msg.array));
    }

    /// Record a chunk. The last chunk yields the ack for the snapshot HLC.
    pub fn record_chunk(&self, msg: &ArraySnapshotChunkMsg) -> Option<ArrayAckMsg> {
        self.snapshot_frames
            .lock()
            .expect("snapshot_frames lock")
            .push(format!("chunk:{}:{}", msg.array, msg.chunk_index));
        (msg.chunk_index + 1 == msg.total_chunks).then(|| ArrayAckMsg {
            array: msg.array.clone(),
            replica_id: 1,
            ack_hlc_bytes: msg.snapshot_hlc_bytes,
            applied_seq: 0,
            status: AckStatus::Applied,
        })
    }

    pub fn pending_after(&self, after: Option<Hlc>, limit: usize) -> Vec<PendingArrayOp> {
        self.pending
            .lock()
            .expect("pending lock")
            .iter()
            .filter(|op| after.is_none_or(|cursor| op.hlc > cursor))
            .take(limit)
            .cloned()
            .collect()
    }

    pub fn persist_seq(&self, op: &PendingArrayOp) {
        if let Some(queued) = self
            .pending
            .lock()
            .expect("pending lock")
            .iter_mut()
            .find(|queued| queued.hlc == op.hlc)
        {
            queued.seq = op.seq;
        }
    }

    pub fn retire(&self, hlcs: &[Hlc]) {
        self.pending
            .lock()
            .expect("pending lock")
            .retain(|op| !hlcs.contains(&op.hlc));
        self.retired
            .lock()
            .expect("retired lock")
            .extend_from_slice(hlcs);
    }

    pub fn schema(&self, array: &str) -> Option<ArraySchemaSyncMsg> {
        self.schemas
            .lock()
            .expect("schemas lock")
            .get(array)
            .cloned()
    }
}
