//! Free functions behind the array-related `SyncDelegate` methods.
//!
//! These are called from the thin delegation methods in `delegate_impl.rs` to
//! keep the `impl SyncDelegate` block concise.

use crate::nodedb::core::NodeDbLite;
use crate::storage::engine::StorageEngine;

pub(super) fn handle_array_delta_impl<S: StorageEngine>(
    db: &NodeDbLite<S>,
    msg: &nodedb_types::sync::wire::ArrayDeltaMsg,
) -> Option<nodedb_types::sync::wire::ArrayAckMsg> {
    use crate::sync::array::inbound::outcome::InboundOutcome;
    use nodedb_array::sync::op_codec;

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
    let replica_id = db.array_inbound.replica_id();

    match db.array_inbound.handle_delta(msg) {
        Ok(InboundOutcome::Applied) => Some(nodedb_types::sync::wire::ArrayAckMsg {
            array: msg.array.clone(),
            replica_id,
            ack_hlc_bytes: op_hlc.to_bytes(),
            applied_seq: 0,
            status: nodedb_types::sync::wire::AckStatus::Applied,
        }),
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

pub(super) fn handle_array_delta_batch_impl<S: StorageEngine>(
    db: &NodeDbLite<S>,
    msg: &nodedb_types::sync::wire::ArrayDeltaBatchMsg,
) -> Option<nodedb_types::sync::wire::ArrayAckMsg> {
    use crate::sync::array::inbound::outcome::InboundOutcome;
    use nodedb_array::sync::op_codec;

    let ops: Vec<_> = msg
        .op_payloads
        .iter()
        .filter_map(|payload| match op_codec::decode_op(payload) {
            Ok(op) => Some(op),
            Err(e) => {
                tracing::warn!(
                    array = %msg.array,
                    error = %e,
                    "SyncDelegate::handle_array_delta_batch: decode failed; skipping op"
                );
                None
            }
        })
        .collect();

    let replica_id = db.array_inbound.replica_id();

    match db.array_inbound.handle_delta_batch(msg) {
        Ok(outcomes) => {
            let mut latest_hlc = None;
            for (outcome, op) in outcomes.iter().zip(ops.iter()) {
                if *outcome == InboundOutcome::Applied {
                    let hlc = op.header.hlc;
                    match latest_hlc {
                        None => latest_hlc = Some(hlc),
                        Some(prev) if hlc > prev => latest_hlc = Some(hlc),
                        _ => {}
                    }
                }
            }
            latest_hlc.map(|hlc| nodedb_types::sync::wire::ArrayAckMsg {
                array: msg.array.clone(),
                replica_id,
                ack_hlc_bytes: hlc.to_bytes(),
                applied_seq: 0,
                status: nodedb_types::sync::wire::AckStatus::Applied,
            })
        }
        Err(e) => {
            tracing::warn!(
                array = %msg.array,
                error = %e,
                "SyncDelegate::handle_array_delta_batch: apply failed"
            );
            None
        }
    }
}

pub(super) fn handle_array_reject_impl<S: StorageEngine>(
    db: &NodeDbLite<S>,
    msg: &nodedb_types::sync::wire::ArrayRejectMsg,
) {
    let inbound = std::sync::Arc::clone(&db.array_inbound);
    let msg_owned = msg.clone();
    tokio::spawn(async move {
        if let Err(e) = inbound.handle_reject(&msg_owned).await {
            tracing::warn!(
                array = %msg_owned.array,
                error = %e,
                "SyncDelegate::handle_array_reject: failed"
            );
        }
    });
}
