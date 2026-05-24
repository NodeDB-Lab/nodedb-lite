//! Reject-message handler — Origin telling us a previously-emitted op was
//! refused; we drop it from the pending queue.

use nodedb_array::sync::hlc::Hlc;
use nodedb_types::sync::wire::array::{ArrayRejectMsg, ArrayRejectReason};

use crate::error::LiteError;
use crate::storage::engine::StorageEngine;

use super::dispatcher::ArrayInbound;
use super::outcome::InboundOutcome;

impl<S: StorageEngine> ArrayInbound<S> {
    /// Process a reject message from Origin.
    ///
    /// Removes the rejected op from the local pending queue and logs the
    /// rejection via `tracing::warn!`. When the reason is
    /// [`ArrayRejectReason::RetentionFloor`], marks the array as needing a
    /// full catch-up on the next connect. Returns
    /// [`InboundOutcome::RejectAcknowledged`] regardless of whether the op was
    /// found in the queue (it may have already been removed by a concurrent
    /// ack).
    pub async fn handle_reject(&self, msg: &ArrayRejectMsg) -> Result<InboundOutcome, LiteError> {
        let op_hlc = Hlc::from_bytes(&msg.op_hlc_bytes);
        let was_present = self.pending.remove(op_hlc).await?;

        tracing::warn!(
            array = %msg.array,
            reason = ?msg.reason,
            detail = %msg.detail,
            op_hlc_physical_ms = op_hlc.physical_ms,
            was_in_pending = was_present,
            "array op rejected by Origin"
        );

        if msg.reason == ArrayRejectReason::RetentionFloor
            && let Err(e) = self.catchup.record_reject_retention_floor(&msg.array).await
        {
            tracing::warn!(
                array = %msg.array,
                error = %e,
                "array_inbound: failed to persist catchup_needed flag"
            );
        }

        Ok(InboundOutcome::RejectAcknowledged)
    }
}

#[cfg(test)]
mod tests {
    use nodedb_types::sync::wire::array::{ArrayRejectMsg, ArrayRejectReason};

    use super::super::fixtures::{hlc, make_inbound, put_op};
    use super::super::outcome::InboundOutcome;

    #[tokio::test(flavor = "multi_thread")]
    async fn handle_reject_drops_op_from_pending() {
        let (inbound, _schemas, pending, _storage) = make_inbound().await;

        // Pre-populate pending with an op at HLC(10).
        let op_hlc = hlc(10);
        let op = put_op("any", 10, 1);
        pending.enqueue(&op).await.unwrap();
        assert_eq!(pending.len().await.unwrap(), 1);

        let msg = ArrayRejectMsg {
            array: "any".into(),
            op_hlc_bytes: op_hlc.to_bytes(),
            reason: ArrayRejectReason::ArrayUnknown,
            detail: "array not found on Origin".into(),
        };
        let outcome = inbound.handle_reject(&msg).await.unwrap();
        assert_eq!(outcome, InboundOutcome::RejectAcknowledged);
        assert_eq!(
            pending.len().await.unwrap(),
            0,
            "op must be removed from pending"
        );
    }
}
