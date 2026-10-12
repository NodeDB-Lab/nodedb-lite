//! `KvPushAck` dispatch: Origin's answer to one `KvPush`.
//!
//! - `Applied` / `Duplicate`: the write is on Origin. The stream frontier
//!   advances and the write is retired.
//! - `Accepted`: provisional. The write stays in flight.
//! - `Fenced`: this producer is fenced, and the push halts.
//! - `Gap`: Origin expects an earlier seq. Every in-flight write is re-sent
//!   from the oldest, and Origin drops the ones it already applied.
//! - `Rejected`: Origin refused the write for good. It is logged at ERROR
//!   with Origin's reason and retired, the way every engine queue handles a
//!   terminal refusal. The write stays in this replica's KV store.

use std::sync::Arc;

use nodedb_types::sync::wire::{AckStatus, EngineKind, KvPushAckMsg, SyncFrame, stream_id_for};

use crate::sync::client::SyncClient;
use crate::sync::delegate::SyncDelegate;

pub(super) async fn handle_kv_push_ack(
    client: &Arc<SyncClient>,
    delegate: &Arc<dyn SyncDelegate>,
    frame: &SyncFrame,
) {
    let Some(ack) = frame.decode_body::<KvPushAckMsg>() else {
        tracing::warn!(
            frame_len = frame.body.len(),
            "KvPushAck frame body failed to decode; the write stays in flight until reconnect"
        );
        return;
    };
    apply_kv_push_ack(client, delegate, &ack).await;
}

/// Apply one decoded `KvPushAck`.
pub(super) async fn apply_kv_push_ack(
    client: &Arc<SyncClient>,
    delegate: &Arc<dyn SyncDelegate>,
    ack: &KvPushAckMsg,
) {
    tracing::debug!(
        collection = %ack.collection,
        batch_id = ack.batch_id,
        status = ?ack.status,
        "KvPushAck received from Origin"
    );
    match &ack.status {
        AckStatus::Applied | AckStatus::Duplicate => {
            let stream_id = stream_id_for(EngineKind::Kv, &ack.collection);
            delegate.record_stream_ack(stream_id, ack.applied_seq).await;
            retire(delegate, ack).await;
        }
        AckStatus::Accepted => {
            // Provisional admission, not a final outcome: the write stays
            // in flight until Origin reports whether it applied.
        }
        AckStatus::Fenced => {
            tracing::error!(
                collection = %ack.collection,
                batch_id = ack.batch_id,
                "KvPushAck: producer fenced by Origin; halting push"
            );
            client.set_fenced();
        }
        AckStatus::Gap { expected } => {
            tracing::warn!(
                collection = %ack.collection,
                batch_id = ack.batch_id,
                expected,
                applied_seq = ack.applied_seq,
                "KvPushAck: sequence gap detected by Origin; re-sending un-acked writes"
            );
            delegate.clear_engine_in_flight().await;
        }
        AckStatus::Rejected { reason } => {
            tracing::error!(
                collection = %ack.collection,
                key = %String::from_utf8_lossy(&ack.key),
                batch_id = ack.batch_id,
                reason = %reason,
                "KvPushAck: Origin permanently rejected this KV write; \
                 retiring it — Origin does not hold this write and it will not retry"
            );
            retire(delegate, ack).await;
        }
    }
}

async fn retire(delegate: &Arc<dyn SyncDelegate>, ack: &KvPushAckMsg) {
    if let Err(e) = delegate.retire_kv_write(ack.batch_id).await {
        // The durable entry survives, so the write is re-sent after the next
        // reconnect and Origin answers it as a duplicate.
        tracing::error!(
            collection = %ack.collection,
            batch_id = ack.batch_id,
            error = %e,
            "KvPushAck: retiring the acknowledged write failed; it is re-sent on reconnect"
        );
    }
}
