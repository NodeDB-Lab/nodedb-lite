// SPDX-License-Identifier: Apache-2.0

//! Recovering this replica's producer identity when Origin refuses it.

use nodedb_types::error::{NodeDbError, NodeDbResult};

use crate::nodedb::core::types::NodeDbLite;
use crate::nodedb::lock_ext::LockExt;
use crate::storage::engine::StorageEngine;

impl<S: StorageEngine> NodeDbLite<S> {
    /// The Loro peer id every local operation is authored under.
    pub fn peer_id(&self) -> u64 {
        self.identity.lock_or_recover().peer_id
    }

    /// This instance's durable producer identity, as sent in a sync handshake.
    pub(crate) fn sync_identity(&self) -> crate::identity::LiteIdentity {
        self.identity.lock_or_recover().clone()
    }

    /// Adopt a new Loro peer id and re-author every document under it.
    ///
    /// Called when Origin refuses a delta because its peer id belongs to
    /// another replica. Continuing under the refused id is not an option: the
    /// server refuses every subsequent write, and were it not to, the CRDT
    /// merge would discard them as replays of the owning replica's operations.
    ///
    /// The new id is persisted before the documents adopt it, so a crash
    /// mid-rotation cannot leave the store authoring under an id its own
    /// identity record no longer claims. The rebuilt documents are then
    /// flushed, because they exist only in memory until they are — and a
    /// restart that reloaded the pre-rotation snapshots would resurrect the
    /// operations the rotation exists to abandon.
    ///
    /// Every row is queued for re-push: the rebuilt documents share no history
    /// with what Origin holds, so the rotation is a resync, not a resume.
    pub async fn rotate_peer_id(&self) -> NodeDbResult<u64> {
        let _change = self.identity_change.lock().await;

        // The identity is mutated on a copy so the store-wide guard is never
        // held across the persist; `identity_change` is what keeps two
        // rotations from interleaving on the same starting value.
        let mut identity = self.identity.lock_or_recover().clone();
        let new_peer_id = identity
            .rotate_peer_id(&*self.storage)
            .await
            .map_err(|e| NodeDbError::storage(format!("peer-id rotation failed: {e}")))?;
        *self.identity.lock_or_recover() = identity;

        {
            let mut crdt = self.crdt.lock_or_recover();
            crdt.rotate_peer_id(new_peer_id)
                .map_err(|e| NodeDbError::storage(format!("peer-id rotation failed: {e}")))?;
        }

        self.flush().await?;

        tracing::warn!(
            peer_id = new_peer_id,
            "adopted a new Loro peer id and queued every row for resync"
        );
        Ok(new_peer_id)
    }

    /// Replace the whole producer identity and re-author every document.
    ///
    /// Called when Origin reports this instance as forked: the history it
    /// holds under this `lite_id` diverged from the one here, so the instance
    /// cannot resume the producer stream. A new `lite_id` and epoch make it a
    /// distinct producer, and the peer id goes with them — keeping it would
    /// collide with the history the fork was detected against, which is the
    /// same refusal one step later.
    pub async fn regenerate_identity(&self) -> NodeDbResult<()> {
        let _change = self.identity_change.lock().await;

        let mut identity = self.identity.lock_or_recover().clone();
        identity
            .regenerate(&*self.storage)
            .await
            .map_err(|e| NodeDbError::storage(format!("identity regeneration failed: {e}")))?;
        let (lite_id, peer_id) = (identity.lite_id.clone(), identity.peer_id);
        *self.identity.lock_or_recover() = identity;

        {
            let mut crdt = self.crdt.lock_or_recover();
            crdt.rotate_peer_id(peer_id)
                .map_err(|e| NodeDbError::storage(format!("identity regeneration failed: {e}")))?;
        }

        self.flush().await?;

        tracing::warn!(
            %lite_id,
            peer_id,
            "fork reported by Origin — adopted a new producer identity and queued every row \
             for resync"
        );
        Ok(())
    }
}
