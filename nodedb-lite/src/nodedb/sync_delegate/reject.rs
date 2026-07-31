//! Reacting to Origin's refusal of a pushed delta.

use nodedb_types::sync::compensation::CompensationHint;

use crate::nodedb::core::NodeDbLite;
use crate::nodedb::lock_ext::LockExt;
use crate::storage::engine::StorageEngine;

/// The constraint Origin names when a delta's Loro peer id belongs to another
/// replica. It is not an application constraint: no policy can resolve it,
/// because nothing about the row is wrong.
const PEER_ID_COLLISION: &str = "peer_id_collision";

/// Whether a refusal says this replica's producer identity is unusable rather
/// than that its data is.
pub(super) fn is_peer_id_collision(hint: &CompensationHint) -> bool {
    matches!(hint, CompensationHint::Custom { constraint, .. } if constraint == PEER_ID_COLLISION)
}

pub(super) fn handle_reject_with_policy_impl<S: StorageEngine>(
    db: &NodeDbLite<S>,
    mutation_id: u64,
    hint: &CompensationHint,
) {
    // A peer-id collision never reaches the policy path. Policy resolution
    // treats an unrecognised refusal as a constraint the row violated: it
    // deletes the local row and drops the delta. Here the row is valid and
    // Origin never saw it — only the identity it travelled under was refused —
    // so that resolution would destroy the write and leave the replica pushing
    // the next one under the same refused id. Recovery is handled by
    // `rotate_peer_id`, which re-queues this delta with the rest.
    if is_peer_id_collision(hint) {
        tracing::warn!(
            mutation_id,
            "Origin refused this delta's Loro peer id as another replica's — \
             rotating the local peer id and resyncing"
        );
        return;
    }

    let mut crdt = db.crdt.lock_or_recover();
    match crdt.reject_delta_with_policy(mutation_id, hint) {
        Some(nodedb_crdt::PolicyResolution::AutoResolved(action)) => {
            tracing::info!(
                mutation_id,
                action = ?action,
                "SyncDelegate: delta auto-resolved by policy"
            );
        }
        Some(nodedb_crdt::PolicyResolution::Deferred {
            retry_after_ms,
            attempt,
            ..
        }) => {
            tracing::info!(
                mutation_id,
                retry_after_ms,
                attempt,
                "SyncDelegate: delta deferred for retry"
            );
        }
        Some(nodedb_crdt::PolicyResolution::Escalate { .. }) => {
            tracing::warn!(mutation_id, "SyncDelegate: delta escalated to DLQ (policy)");
        }
        Some(nodedb_crdt::PolicyResolution::WebhookRequired { webhook_url, .. }) => {
            tracing::warn!(
                mutation_id,
                webhook_url,
                "SyncDelegate: delta requires webhook (not supported on Lite)"
            );
            let _ = crdt.reject_delta(mutation_id);
        }
        None => {
            tracing::debug!(
                mutation_id,
                "SyncDelegate: reject_with_policy — delta not found"
            );
        }
    }
}
