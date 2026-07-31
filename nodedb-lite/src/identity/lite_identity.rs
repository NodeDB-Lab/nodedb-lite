// SPDX-License-Identifier: BUSL-1.1

//! Lite instance identity: UUID v7 + monotonic epoch + Loro peer id.
//!
//! - `lite_id`: UUID v7 generated on first `open()`, persisted in KV metadata
//! - `epoch`: monotonic counter incremented on every `open()`
//! - `peer_id`: this replica's Loro producer identity
//!
//! All three are properties of *the store*, not of the caller that opened it.
//! An application hands every one of its installs the same constants — a
//! build-time id, a restored template, a hard-coded literal — so an identity
//! taken from the caller is shared by every install, and a peer id shared by
//! two live replicas has the CRDT merge discard one of them. Minting on first
//! open and persisting alongside the data it authors is what makes the
//! identity unique per replica and what makes rotating it stick: a rotation
//! that lived only in memory would be forgotten on restart and the replica
//! would resume writing under the id Origin already refused.
//!
//! Fork detection: Origin rejects sync if `epoch <= last_seen_epoch[lite_id]`.

use crate::error::LiteError;
use crate::identity::peer_id::{is_valid_peer_id, mint_peer_id};
use crate::storage::engine::StorageEngine;

/// KV store metadata keys.
const LITE_ID_KEY: &[u8] = b"meta:lite_id";
const EPOCH_KEY: &[u8] = b"meta:epoch";
const PEER_ID_KEY: &[u8] = b"meta:peer_id";

/// Persistent Lite instance identity.
#[derive(Debug, Clone)]
pub struct LiteIdentity {
    /// UUID v7 string (time-ordered, cryptographically random tail).
    pub lite_id: String,
    /// Monotonic epoch counter (incremented on every open).
    pub epoch: u64,
    /// This replica's Loro peer id — the producer of every operation it
    /// authors. Rotated (via [`Self::rotate_peer_id`]) when Origin refuses it
    /// as owned by another replica.
    pub peer_id: u64,
}

impl LiteIdentity {
    /// Load or create identity from storage.
    ///
    /// On first call (no identity in KV store): generates UUID v7, mints a
    /// peer id, sets epoch=1. On subsequent calls: reads the existing id and
    /// peer id, increments the epoch.
    ///
    /// A persisted peer id that is not a usable Loro peer id — zero-length,
    /// truncated, or written before the store carried one — is re-minted, not
    /// passed through: Loro reads `0` as "unset" and would author operations
    /// under an identity no replica owns.
    pub async fn load_or_create<S: StorageEngine>(storage: &S) -> Result<Self, LiteError> {
        let ns = nodedb_types::Namespace::Meta;

        // Load or generate lite_id.
        let lite_id = match storage.get(ns, LITE_ID_KEY).await? {
            Some(bytes) => {
                String::from_utf8(bytes).unwrap_or_else(|_| nodedb_types::id_gen::uuid_v7())
            }
            None => {
                let id = nodedb_types::id_gen::uuid_v7();
                storage.put(ns, LITE_ID_KEY, id.as_bytes()).await?;
                id
            }
        };

        // Load or mint the Loro peer id.
        let stored_peer_id = match storage.get(ns, PEER_ID_KEY).await? {
            Some(bytes) if bytes.len() == 8 => {
                let id = u64::from_le_bytes(bytes.try_into().unwrap_or([0; 8]));
                is_valid_peer_id(id).then_some(id)
            }
            _ => None,
        };
        let peer_id = match stored_peer_id {
            Some(id) => id,
            None => {
                let id = mint_peer_id();
                storage.put(ns, PEER_ID_KEY, &id.to_le_bytes()).await?;
                id
            }
        };

        // Load, increment, and persist epoch.
        let epoch = match storage.get(ns, EPOCH_KEY).await? {
            Some(bytes) if bytes.len() == 8 => {
                let prev = u64::from_le_bytes(bytes.try_into().unwrap_or([0; 8]));
                prev + 1
            }
            _ => 1,
        };
        storage.put(ns, EPOCH_KEY, &epoch.to_le_bytes()).await?;

        Ok(Self {
            lite_id,
            epoch,
            peer_id,
        })
    }

    /// Regenerate the whole identity (called on fork detection).
    ///
    /// Generates a new UUID v7, resets epoch to 1, mints a new peer id, and
    /// persists all three. The peer id goes with the rest: a fork means this
    /// store's history diverged from the one Origin holds under the old
    /// identity, so continuing to author operations under the old peer id
    /// collides with the very history the fork was detected against.
    pub async fn regenerate<S: StorageEngine>(&mut self, storage: &S) -> Result<(), LiteError> {
        let ns = nodedb_types::Namespace::Meta;
        self.lite_id = nodedb_types::id_gen::uuid_v7();
        self.epoch = 1;
        storage
            .put(ns, LITE_ID_KEY, self.lite_id.as_bytes())
            .await?;
        storage
            .put(ns, EPOCH_KEY, &self.epoch.to_le_bytes())
            .await?;
        self.rotate_peer_id(storage).await?;
        Ok(())
    }

    /// Mint and persist a new peer id, leaving `lite_id` and `epoch` alone.
    ///
    /// This is the response to Origin refusing a delta whose peer id another
    /// replica owns: the producer identity is still this store's, only the
    /// Loro peer id it authors under has to change.
    ///
    /// Returns the new peer id. It is persisted before returning so a crash
    /// between the rotation and the next write cannot resurrect the refused
    /// id — the caller re-authors its documents against the value this
    /// returns, and that re-authoring must never outlive the record of it.
    pub async fn rotate_peer_id<S: StorageEngine>(
        &mut self,
        storage: &S,
    ) -> Result<u64, LiteError> {
        let ns = nodedb_types::Namespace::Meta;
        let previous = self.peer_id;
        let mut next = mint_peer_id();
        // Astronomically unlikely, but a rotation that returns the refused id
        // is a rotation that did nothing, and the caller cannot tell.
        while next == previous {
            next = mint_peer_id();
        }
        storage.put(ns, PEER_ID_KEY, &next.to_le_bytes()).await?;
        self.peer_id = next;
        Ok(next)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::identity::peer_id::is_valid_peer_id;
    use crate::storage::pagedb_storage::PagedbStorageMem;

    #[tokio::test]
    async fn first_open_creates_identity() {
        let storage = PagedbStorageMem::open_in_memory().await.unwrap();
        let identity = LiteIdentity::load_or_create(&storage).await.unwrap();
        assert!(!identity.lite_id.is_empty());
        assert_eq!(identity.epoch, 1);
        assert!(is_valid_peer_id(identity.peer_id));
    }

    #[tokio::test]
    async fn second_open_increments_epoch() {
        let storage = PagedbStorageMem::open_in_memory().await.unwrap();
        let id1 = LiteIdentity::load_or_create(&storage).await.unwrap();
        let id2 = LiteIdentity::load_or_create(&storage).await.unwrap();
        assert_eq!(id1.lite_id, id2.lite_id); // Same ID.
        assert_eq!(id2.epoch, 2); // Epoch incremented.
    }

    #[tokio::test]
    async fn peer_id_is_stable_across_opens() {
        let storage = PagedbStorageMem::open_in_memory().await.unwrap();
        let id1 = LiteIdentity::load_or_create(&storage).await.unwrap();
        let id2 = LiteIdentity::load_or_create(&storage).await.unwrap();
        assert_eq!(
            id1.peer_id, id2.peer_id,
            "reopening must not change who authored the store's existing operations"
        );
    }

    #[tokio::test]
    async fn separate_stores_mint_separate_peer_ids() {
        let a = PagedbStorageMem::open_in_memory().await.unwrap();
        let b = PagedbStorageMem::open_in_memory().await.unwrap();
        let id_a = LiteIdentity::load_or_create(&a).await.unwrap();
        let id_b = LiteIdentity::load_or_create(&b).await.unwrap();
        assert_ne!(id_a.peer_id, id_b.peer_id);
        assert_ne!(id_a.lite_id, id_b.lite_id);
    }

    #[tokio::test]
    async fn rotate_peer_id_persists_and_leaves_the_producer_identity() {
        let storage = PagedbStorageMem::open_in_memory().await.unwrap();
        let mut identity = LiteIdentity::load_or_create(&storage).await.unwrap();
        let (lite_id, epoch, before) = (identity.lite_id.clone(), identity.epoch, identity.peer_id);

        let rotated = identity.rotate_peer_id(&storage).await.unwrap();

        assert_ne!(rotated, before);
        assert_eq!(identity.peer_id, rotated);
        assert_eq!(identity.lite_id, lite_id, "rotation is not a new producer");
        assert_eq!(identity.epoch, epoch);

        let reloaded = LiteIdentity::load_or_create(&storage).await.unwrap();
        assert_eq!(
            reloaded.peer_id, rotated,
            "a rotation that is forgotten on restart resumes the refused id"
        );
    }

    #[tokio::test]
    async fn regenerate_changes_id_and_peer_id() {
        let storage = PagedbStorageMem::open_in_memory().await.unwrap();
        let mut id = LiteIdentity::load_or_create(&storage).await.unwrap();
        let original_id = id.lite_id.clone();
        let original_peer = id.peer_id;
        id.regenerate(&storage).await.unwrap();
        assert_ne!(id.lite_id, original_id);
        assert_ne!(
            id.peer_id, original_peer,
            "a forked store must stop authoring under the forked peer id"
        );
        assert_eq!(id.epoch, 1);
    }

    #[tokio::test]
    async fn invalid_stored_peer_id_is_reminted() {
        let storage = PagedbStorageMem::open_in_memory().await.unwrap();
        storage
            .put(
                nodedb_types::Namespace::Meta,
                PEER_ID_KEY,
                &0u64.to_le_bytes(),
            )
            .await
            .unwrap();

        let identity = LiteIdentity::load_or_create(&storage).await.unwrap();

        assert!(
            is_valid_peer_id(identity.peer_id),
            "Loro reads 0 as unset; it must never reach the document"
        );
    }
}
