//! Free functions behind the array-related `SyncDelegate` methods.
//!
//! These are called from the thin delegation methods in `delegate_impl.rs` to
//! keep the `impl SyncDelegate` block concise.
//!
//! Every inbound apply path records what it applied in the array's
//! [`CatchupTracker`](crate::sync::array::CatchupTracker). The next session's
//! `ArrayCatchupRequest` starts at that HLC, so Origin re-sends only the ops
//! this replica has not applied.
//!
//! The push-side functions read and retire the durable array pending queue
//! and build the `ArraySchema` announce the push loop sends before an array's
//! first delta in each session.

use nodedb_array::sync::hlc::Hlc;
use nodedb_array::sync::op_codec;
use nodedb_types::sync::wire::{
    AckStatus, ArrayAckMsg, ArrayCatchupRequestMsg, ArrayDeltaBatchMsg, ArrayDeltaMsg,
    ArrayRejectMsg, ArrayRejectReason, ArraySchemaSyncMsg, ArraySnapshotChunkMsg, ArraySnapshotMsg,
};

use crate::error::LiteError;
use crate::nodedb::core::NodeDbLite;
use crate::storage::engine::StorageEngine;
use crate::sync::array::PendingArrayOp;
use crate::sync::array::inbound::outcome::InboundOutcome;

/// The ack Origin's GC frontier advances on: `hlc` is applied on `array`.
fn applied_ack<S: StorageEngine>(db: &NodeDbLite<S>, array: &str, hlc: Hlc) -> ArrayAckMsg {
    ArrayAckMsg {
        array: array.to_owned(),
        replica_id: db.array_inbound.replica_id(),
        ack_hlc_bytes: hlc.to_bytes(),
        applied_seq: 0,
        status: AckStatus::Applied,
    }
}

/// Advance the catch-up high-water mark for `array` to `hlc`.
///
/// A storage error is logged and not propagated: the op is applied, and the
/// stale mark only makes the next catch-up re-send ops that apply as
/// idempotent.
async fn record_applied<S: StorageEngine>(db: &NodeDbLite<S>, array: &str, hlc: Hlc) {
    if let Err(e) = db.array_inbound.catchup_tracker().record(array, hlc).await {
        tracing::warn!(
            array = %array,
            error = %e,
            "SyncDelegate: failed to record the array catch-up high-water mark"
        );
    }
}

/// Persist the catch-up flag for `array`, so the next session asks for a
/// snapshot. A storage error is logged.
async fn flag_for_snapshot<S: StorageEngine>(db: &NodeDbLite<S>, array: &str) {
    if let Err(e) = db
        .array_inbound
        .catchup_tracker()
        .record_reject_retention_floor(array)
        .await
    {
        tracing::warn!(
            array = %array,
            error = %e,
            "SyncDelegate: failed to persist the array catch-up flag"
        );
    }
}

pub(super) async fn handle_array_delta_impl<S: StorageEngine>(
    db: &NodeDbLite<S>,
    msg: &ArrayDeltaMsg,
) -> Option<ArrayAckMsg> {
    let op = match op_codec::decode_op(&msg.op_payload) {
        Ok(op) => op,
        Err(e) => {
            tracing::warn!(
                array = %msg.array,
                error = %e,
                "SyncDelegate::handle_array_delta: decode failed"
            );
            return None;
        }
    };
    let op_hlc = op.header.hlc;

    match db.array_inbound.handle_delta(msg) {
        Ok(InboundOutcome::Applied) => {
            record_applied(db, &msg.array, op_hlc).await;
            Some(applied_ack(db, &msg.array, op_hlc))
        }
        Ok(InboundOutcome::Idempotent) => {
            record_applied(db, &msg.array, op_hlc).await;
            None
        }
        Ok(_) => None,
        Err(e) => {
            tracing::warn!(
                array = %msg.array,
                error = %e,
                "SyncDelegate::handle_array_delta: apply failed"
            );
            None
        }
    }
}

pub(super) async fn handle_array_delta_batch_impl<S: StorageEngine>(
    db: &NodeDbLite<S>,
    msg: &ArrayDeltaBatchMsg,
) -> Option<ArrayAckMsg> {
    let mut hlcs = Vec::with_capacity(msg.op_payloads.len());
    for payload in &msg.op_payloads {
        match op_codec::decode_op(payload) {
            Ok(op) => hlcs.push(op.header.hlc),
            Err(e) => {
                // `handle_delta_batch` fails on the same payload, so the
                // whole batch is refused rather than applied around a gap.
                tracing::warn!(
                    array = %msg.array,
                    error = %e,
                    "SyncDelegate::handle_array_delta_batch: decode failed; batch not applied"
                );
                return None;
            }
        }
    }

    let outcomes = match db.array_inbound.handle_delta_batch(msg) {
        Ok(outcomes) => outcomes,
        Err(e) => {
            tracing::warn!(
                array = %msg.array,
                error = %e,
                "SyncDelegate::handle_array_delta_batch: apply failed"
            );
            return None;
        }
    };

    let mut latest_applied: Option<Hlc> = None;
    let mut contiguous_seen: Option<Hlc> = None;
    let mut gap = false;
    for (outcome, hlc) in outcomes.iter().zip(hlcs.iter().copied()) {
        match outcome {
            InboundOutcome::Applied => {
                latest_applied = latest_applied.max(Some(hlc));
                if !gap {
                    contiguous_seen = contiguous_seen.max(Some(hlc));
                }
            }
            InboundOutcome::Idempotent => {
                if !gap {
                    contiguous_seen = contiguous_seen.max(Some(hlc));
                }
            }
            _ => gap = true,
        }
    }

    if let Some(hlc) = contiguous_seen {
        record_applied(db, &msg.array, hlc).await;
    }
    latest_applied.map(|hlc| applied_ack(db, &msg.array, hlc))
}

pub(super) async fn handle_array_reject_impl<S: StorageEngine>(
    db: &NodeDbLite<S>,
    msg: &ArrayRejectMsg,
) -> Option<ArrayCatchupRequestMsg> {
    if let Err(e) = db.array_inbound.handle_reject(msg).await {
        tracing::warn!(
            array = %msg.array,
            error = %e,
            "SyncDelegate::handle_array_reject: failed"
        );
    }
    (msg.reason == ArrayRejectReason::RetentionFloor)
        .then(|| db.array_inbound.catchup_tracker().build_request(&msg.array))
}

pub(super) async fn handle_array_snapshot_impl<S: StorageEngine>(
    db: &NodeDbLite<S>,
    msg: &ArraySnapshotMsg,
) {
    if let Err(e) = db.array_inbound.handle_snapshot_header(msg) {
        tracing::warn!(
            array = %msg.array,
            error = %e,
            "SyncDelegate::handle_array_snapshot: header refused"
        );
        flag_for_snapshot(db, &msg.array).await;
    }
}

pub(super) async fn handle_array_snapshot_chunk_impl<S: StorageEngine>(
    db: &NodeDbLite<S>,
    msg: &ArraySnapshotChunkMsg,
) -> Option<ArrayAckMsg> {
    match db.array_inbound.handle_snapshot_chunk(msg) {
        Ok(InboundOutcome::SnapshotApplied { ops_applied }) => {
            let snapshot_hlc = Hlc::from_bytes(&msg.snapshot_hlc_bytes);
            record_applied(db, &msg.array, snapshot_hlc).await;
            if let Err(e) = db
                .array_inbound
                .catchup_tracker()
                .clear_catchup_needed(&msg.array)
                .await
            {
                tracing::warn!(
                    array = %msg.array,
                    error = %e,
                    "SyncDelegate: failed to clear the array catch-up flag"
                );
            }
            tracing::debug!(array = %msg.array, ops_applied, "array snapshot applied");
            Some(applied_ack(db, &msg.array, snapshot_hlc))
        }
        Ok(_) => None,
        Err(e) => {
            tracing::warn!(
                array = %msg.array,
                error = %e,
                "SyncDelegate::handle_array_snapshot_chunk: snapshot refused"
            );
            flag_for_snapshot(db, &msg.array).await;
            None
        }
    }
}

pub(super) fn begin_array_catchup_impl<S: StorageEngine>(
    db: &NodeDbLite<S>,
) -> Vec<ArrayCatchupRequestMsg> {
    db.array_inbound.begin_catchup_round().unwrap_or_else(|e| {
        tracing::warn!(
            error = %e,
            "SyncDelegate::begin_array_catchup: failed; no catch-up this session"
        );
        Vec::new()
    })
}

pub(super) async fn pending_array_ops_impl<S: StorageEngine>(
    db: &NodeDbLite<S>,
    after: Option<Hlc>,
    limit: usize,
) -> Result<Vec<PendingArrayOp>, LiteError> {
    db.array_outbound.pending().scan_after(after, limit).await
}

pub(super) async fn persist_array_op_seq_impl<S: StorageEngine>(
    db: &NodeDbLite<S>,
    op: &PendingArrayOp,
) -> Result<(), LiteError> {
    if db.array_outbound.pending().set_seq(op.hlc, op.seq).await? {
        return Ok(());
    }
    Err(LiteError::Storage {
        detail: format!(
            "array '{}' op at {}ms left the pending queue before its seq was persisted; it is not sent",
            op.array, op.hlc.physical_ms
        ),
    })
}

pub(super) async fn retire_array_ops_impl<S: StorageEngine>(
    db: &NodeDbLite<S>,
    hlcs: &[Hlc],
) -> Result<(), LiteError> {
    db.array_outbound.pending().retire(hlcs).await.map(|_| ())
}

pub(super) fn array_schema_announce_impl<S: StorageEngine>(
    db: &NodeDbLite<S>,
    array: &str,
) -> Result<Option<ArraySchemaSyncMsg>, LiteError> {
    let announce =
        db.array_schemas
            .export_versioned(array)?
            .map(|(schema_hlc, snapshot_payload)| ArraySchemaSyncMsg {
                array: array.to_owned(),
                replica_id: db.array_inbound.replica_id(),
                schema_hlc_bytes: schema_hlc.to_bytes(),
                snapshot_payload,
            });
    Ok(announce)
}
