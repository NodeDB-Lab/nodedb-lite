//! [`ArrayAckSender`] — periodic ack sender from Lite to Origin.
//!
//! A Tokio task (spawned on sync-session start, shut down on disconnect)
//! that runs every `interval` seconds and sends `ArrayAckMsg` for each known
//! array to Origin. Origin merges these into its `ArrayAckRegistry` to advance
//! the GC frontier.
//!
//! # What gets acked
//!
//! For each registered array the ack carries `last_applied_hlc` — the highest
//! HLC successfully applied by `LiteApplyEngine`. This is the correct value
//! because it represents "I have all ops up to this HLC" from Origin's
//! perspective.
//!
//! # Transport
//!
//! Frames are sent via a `tokio::sync::mpsc::Sender<Vec<u8>>` that the
//! session's WebSocket write loop drains. This matches the pattern used by
//! other Lite sync subsystems.

use std::sync::Arc;
use std::time::Duration;

use nodedb_array::sync::replica_id::ReplicaId;
use nodedb_types::sync::wire::SyncMessageType;
use nodedb_types::sync::wire::array::ArrayAckMsg;
use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use tracing::warn;

use crate::storage::engine::StorageEngine;

use super::inbound::apply::LiteApplyEngine;
use super::schema_registry::SchemaRegistry;

/// Default ack interval: 30 seconds.
pub const DEFAULT_ACK_INTERVAL: Duration = Duration::from_secs(30);

/// Periodic array ack sender task.
///
/// Spawned on session connect and cancelled on disconnect. The returned
/// `JoinHandle` should be stored by the session and aborted (via
/// `handle.abort()`) on session teardown.
pub fn spawn<S: StorageEngine + 'static>(
    schemas: Arc<SchemaRegistry<S>>,
    engine: Arc<LiteApplyEngine<S>>,
    replica_id: ReplicaId,
    tx: mpsc::Sender<Vec<u8>>,
    interval: Duration,
    mut stop: tokio::sync::oneshot::Receiver<()>,
) -> JoinHandle<()> {
    tokio::spawn(async move {
        let mut ticker = tokio::time::interval(interval);
        // Skip the immediate first tick so we don't ack before applying anything.
        ticker.tick().await;

        loop {
            tokio::select! {
                _ = ticker.tick() => {
                    send_acks(&schemas, &engine, replica_id, &tx).await;
                }
                _ = &mut stop => {
                    return;
                }
            }
        }
    })
}

/// Build and send `ArrayAckMsg` frames for all known arrays.
async fn send_acks<S: StorageEngine>(
    schemas: &SchemaRegistry<S>,
    engine: &LiteApplyEngine<S>,
    replica_id: ReplicaId,
    tx: &mpsc::Sender<Vec<u8>>,
) {
    let arrays = schemas.list_arrays();
    for array in &arrays {
        let Some(ack_hlc) = engine.last_applied_hlc(array) else {
            // No ops applied for this array yet — nothing to ack.
            continue;
        };

        let msg = ArrayAckMsg {
            array: array.clone(),
            replica_id: replica_id.as_u64(),
            ack_hlc_bytes: ack_hlc.to_bytes(),
        };

        let frame = match nodedb_types::sync::wire::SyncFrame::try_encode(
            SyncMessageType::ArrayAck,
            &msg,
        ) {
            Some(f) => f.to_bytes(),
            None => {
                warn!(
                    array = %array,
                    "ack_sender: SyncFrame encode failed for ArrayAck"
                );
                continue;
            }
        };

        if tx.send(frame).await.is_err() {
            // Transport closed — session is shutting down.
            return;
        }
    }
}
