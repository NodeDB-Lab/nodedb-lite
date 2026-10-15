//! Reject-message handler: Origin refused an op this replica emitted.
//!
//! [`reject_disposition`] decides the op's fate. Only a `Drop` disposition
//! removes the op from the pending queue. The push loop handles schema
//! re-sends and parking. Every reject is logged.

use nodedb_array::sync::hlc::Hlc;
use nodedb_types::sync::wire::array::ArrayRejectMsg;

use crate::error::LiteError;
use crate::storage::engine::StorageEngine;
use crate::sync::array::reject_policy::{RejectDisposition, reject_disposition};

use super::dispatcher::ArrayInbound;
use super::outcome::InboundOutcome;

impl<S: StorageEngine> ArrayInbound<S> {
    /// Process a reject message from Origin.
    ///
    /// - `Drop`: removes the op from the pending queue and logs the loss at
    ///   error level.
    /// - `CatchUp`: persists the array's catch-up flag, so the next catch-up
    ///   request starts at `Hlc::ZERO`.
    /// - `ResendSchema` and `Park`: keep the op queued and log the reject.
    ///
    /// Returns [`InboundOutcome::RejectAcknowledged`] in every case, including
    /// a `Drop` for an op no longer queued.
    pub async fn handle_reject(&self, msg: &ArrayRejectMsg) -> Result<InboundOutcome, LiteError> {
        let op_hlc = Hlc::from_bytes(&msg.op_hlc_bytes);
        match reject_disposition(msg.reason) {
            RejectDisposition::Drop => {
                let was_present = self.pending.remove(op_hlc).await?;
                tracing::error!(
                    array = %msg.array,
                    reason = ?msg.reason,
                    detail = %msg.detail,
                    op_hlc_physical_ms = op_hlc.physical_ms,
                    was_in_pending = was_present,
                    "array op permanently refused by Origin; op dropped, the write never reaches Origin"
                );
            }
            RejectDisposition::CatchUp => {
                tracing::warn!(
                    array = %msg.array,
                    detail = %msg.detail,
                    "array catch-up cursor is below Origin's retention floor; requesting a snapshot"
                );
                if let Err(e) = self.catchup.record_reject_retention_floor(&msg.array).await {
                    tracing::warn!(
                        array = %msg.array,
                        error = %e,
                        "array_inbound: failed to persist catchup_needed flag"
                    );
                }
            }
            disposition @ (RejectDisposition::ResendSchema | RejectDisposition::Park) => {
                tracing::warn!(
                    array = %msg.array,
                    reason = ?msg.reason,
                    detail = %msg.detail,
                    op_hlc_physical_ms = op_hlc.physical_ms,
                    ?disposition,
                    "array op refused by Origin; op kept for retry"
                );
            }
        }

        Ok(InboundOutcome::RejectAcknowledged)
    }
}

#[cfg(test)]
mod tests {
    use nodedb_types::sync::wire::array::{ArrayRejectMsg, ArrayRejectReason};

    use super::super::fixtures::{hlc, make_inbound, put_op};
    use super::super::outcome::InboundOutcome;

    fn reject(reason: ArrayRejectReason) -> ArrayRejectMsg {
        ArrayRejectMsg {
            array: "any".into(),
            op_hlc_bytes: hlc(10).to_bytes(),
            reason,
            detail: "refused".into(),
        }
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn shape_invalid_reject_drops_op_from_pending() {
        let (inbound, _schemas, pending, _storage) = make_inbound().await;
        pending.enqueue(&put_op("any", 10, 1)).await.unwrap();

        let outcome = inbound
            .handle_reject(&reject(ArrayRejectReason::ShapeInvalid))
            .await
            .unwrap();
        assert_eq!(outcome, InboundOutcome::RejectAcknowledged);
        assert_eq!(pending.len().await.unwrap(), 0, "op must be removed");
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn retryable_rejects_keep_op_in_pending() {
        let (inbound, _schemas, pending, _storage) = make_inbound().await;
        pending.enqueue(&put_op("any", 10, 1)).await.unwrap();

        for reason in [
            ArrayRejectReason::ArrayUnknown,
            ArrayRejectReason::SchemaTooNew,
            ArrayRejectReason::EngineUnavailable,
            ArrayRejectReason::SnapshotCapacity,
            ArrayRejectReason::SnapshotBufferLost,
            ArrayRejectReason::Unauthenticated,
            ArrayRejectReason::RetentionFloor,
        ] {
            let outcome = inbound.handle_reject(&reject(reason)).await.unwrap();
            assert_eq!(outcome, InboundOutcome::RejectAcknowledged);
            assert_eq!(
                pending.len().await.unwrap(),
                1,
                "{reason:?} must keep the op queued"
            );
        }
    }
}
