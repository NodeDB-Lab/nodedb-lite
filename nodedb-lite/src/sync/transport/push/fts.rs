//! FTS index / delete push.

use std::ops::ControlFlow;
use std::sync::Arc;

use futures::SinkExt;
use tokio::sync::Mutex;
use tokio_tungstenite::tungstenite::Message;

use nodedb_types::sync::wire::{EngineKind, SyncFrame, SyncMessageType, stream_id_for};

use super::send::send_binary;
use crate::sync::client::SyncClient;
use crate::sync::outbound::fts::PendingFtsIndex;
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
    // The instance's own identity, not its Loro peer id: Origin dedups these
    // entries by `lite_id`, and a peer id both names a different thing and
    // changes under a collision rotation, which would split one producer's
    // stream in two.
    let lite_id = delegate.sync_identity().lite_id;
    let producer_id = client.producer_id().await;
    let epoch = client.accepted_epoch().await;

    let entries = match pending_batch(delegate.pending_fts_indexes().await, "index") {
        ControlFlow::Continue(entries) => entries,
        ControlFlow::Break(()) => return ControlFlow::Break(()),
    };
    for (durable_key, mut entry) in entries {
        // Announce the collection's schema before its first data frame so a
        // lite-only collection is registered on Origin before its FTS docs land.
        if super::control::ensure_collection_announced(client, delegate, sink, &entry.collection)
            .await
            .is_break()
        {
            return ControlFlow::Break(());
        }
        let stream_id = stream_id_for(EngineKind::Fts, &entry.collection);
        if entry.seq == 0 {
            entry.seq = delegate.next_stream_seq(stream_id).await;
            if let Err(e) = delegate.persist_fts_index_seq(&durable_key, &entry).await {
                tracing::warn!(error = %e, "failed to persist stream seq into durable entry; retaining for retry");
                continue;
            }
        }
        let seq = entry.seq;
        let batch_id = entry.batch_id;
        let msg = index_msg(&lite_id, producer_id, epoch, seq, entry);
        let Some(frame) = SyncFrame::try_encode(SyncMessageType::FtsIndex, &msg) else {
            tracing::error!(
                collection = %msg.collection, doc_id = %msg.doc_id, batch_id,
                "failed to encode FtsIndex frame; dropping entry"
            );
            delegate.acknowledge_fts_index(durable_key).await;
            continue;
        };
        if let Err(e) = send_binary(sink, frame).await {
            tracing::warn!(
                collection = %msg.collection, doc_id = %msg.doc_id, batch_id, error = %e,
                "FtsIndex send failed; durable entry retained for re-send on reconnect"
            );
            return ControlFlow::Break(());
        }
        // Mark in-flight: durable entry survives until Origin ack.
        delegate
            .mark_fts_index_in_flight(batch_id, durable_key)
            .await;
        tracing::debug!(
            collection = %msg.collection, doc_id = %msg.doc_id, batch_id,
            "sent FtsIndex to Origin; awaiting ack before deleting durable entry"
        );
    }

    let entries = match pending_batch(delegate.pending_fts_deletes().await, "delete") {
        ControlFlow::Continue(entries) => entries,
        ControlFlow::Break(()) => return ControlFlow::Break(()),
    };
    for (durable_key, mut entry) in entries {
        if super::control::ensure_collection_announced(client, delegate, sink, &entry.collection)
            .await
            .is_break()
        {
            return ControlFlow::Break(());
        }
        let stream_id = stream_id_for(EngineKind::Fts, &entry.collection);
        if entry.seq == 0 {
            entry.seq = delegate.next_stream_seq(stream_id).await;
            if let Err(e) = delegate.persist_fts_delete_seq(&durable_key, &entry).await {
                tracing::warn!(error = %e, "failed to persist stream seq into durable entry; retaining for retry");
                continue;
            }
        }
        let seq = entry.seq;
        let msg = nodedb_types::sync::wire::FtsDeleteMsg {
            lite_id: lite_id.clone(),
            collection: entry.collection,
            doc_id: entry.doc_id,
            batch_id: entry.batch_id,
            producer_id,
            epoch,
            seq,
        };
        let Some(frame) = SyncFrame::try_encode(SyncMessageType::FtsDelete, &msg) else {
            tracing::error!(
                collection = %msg.collection, doc_id = %msg.doc_id, batch_id = entry.batch_id,
                "failed to encode FtsDelete frame; dropping entry"
            );
            delegate.acknowledge_fts_delete(durable_key).await;
            continue;
        };
        if let Err(e) = send_binary(sink, frame).await {
            tracing::warn!(
                collection = %msg.collection, doc_id = %msg.doc_id, batch_id = entry.batch_id, error = %e,
                "FtsDelete send failed; durable entry retained for re-send on reconnect"
            );
            return ControlFlow::Break(());
        }
        // Mark in-flight: durable entry survives until Origin ack.
        delegate
            .mark_fts_delete_in_flight(entry.batch_id, durable_key)
            .await;
        tracing::debug!(
            collection = %msg.collection, doc_id = %msg.doc_id, batch_id = entry.batch_id,
            "sent FtsDelete to Origin; awaiting ack before deleting durable entry"
        );
    }
    ControlFlow::Continue(())
}

fn index_msg(
    lite_id: &str,
    producer_id: u64,
    epoch: u64,
    seq: u64,
    entry: PendingFtsIndex,
) -> nodedb_types::sync::wire::FtsIndexMsg {
    nodedb_types::sync::wire::FtsIndexMsg {
        lite_id: lite_id.to_owned(),
        collection: entry.collection,
        doc_id: entry.doc_id,
        fields: entry.fields,
        batch_id: entry.batch_id,
        producer_id,
        epoch,
        seq,
    }
}

fn pending_batch<T>(
    result: Result<Vec<T>, crate::error::LiteError>,
    operation: &str,
) -> ControlFlow<(), Vec<T>> {
    match result {
        Ok(entries) => ControlFlow::Continue(entries),
        Err(error) => {
            tracing::warn!(error = ?error, operation, "FTS pending drain error: retain entries and retry");
            ControlFlow::Break(())
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{PendingFtsIndex, index_msg, pending_batch};
    use crate::error::LiteError;
    use std::ops::ControlFlow;

    #[test]
    fn pending_drain_errors_stop_push_for_retry() {
        for operation in ["index", "delete"] {
            let error = LiteError::Backpressure {
                detail: "outbound capacity exhausted".into(),
            };
            assert!(matches!(
                pending_batch::<u8>(Err(error), operation),
                ControlFlow::Break(())
            ));
            let error = LiteError::Corrupted {
                detail: "invalid durable entry".into(),
            };
            assert!(matches!(
                pending_batch::<u8>(Err(error), operation),
                ControlFlow::Break(())
            ));
        }
    }

    #[test]
    fn index_message_carries_per_field_text() {
        let entry = PendingFtsIndex {
            batch_id: 9,
            collection: "docs".into(),
            doc_id: "d1".into(),
            fields: vec![
                ("body".into(), "alpha beta".into()),
                ("title".into(), "gamma".into()),
            ],
            seq: 4,
        };
        let msg = index_msg("lite-1", 7, 2, 4, entry);
        assert_eq!(
            msg.fields,
            vec![
                ("body".to_string(), "alpha beta".to_string()),
                ("title".to_string(), "gamma".to_string()),
            ]
        );
        assert_eq!(msg.collection, "docs");
        assert_eq!(msg.doc_id, "d1");
        assert_eq!(msg.batch_id, 9);
        assert_eq!(msg.seq, 4);
    }

    #[test]
    fn successful_pending_batch_preserves_entries() {
        assert_eq!(
            pending_batch(Ok(vec![1, 2]), "index"),
            ControlFlow::Continue(vec![1, 2])
        );
        assert_eq!(
            pending_batch::<u8>(Ok(vec![]), "delete"),
            ControlFlow::Continue(vec![])
        );
    }
}
