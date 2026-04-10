//! Lite instance identity: UUID v7 + monotonic epoch for fork detection.
//!
//! - `lite_id`: UUID v7 generated on first `open()`, persisted in redb metadata
//! - `epoch`: monotonic u64 counter incremented on every `open()`
//! - Fork detection: Origin rejects sync if `epoch <= last_seen_epoch[lite_id]`

use crate::error::LiteError;
use crate::storage::engine::StorageEngine;

/// redb metadata keys.
const LITE_ID_KEY: &[u8] = b"meta:lite_id";
const EPOCH_KEY: &[u8] = b"meta:epoch";

/// Persistent Lite instance identity.
#[derive(Debug, Clone)]
pub struct LiteIdentity {
    /// UUID v7 string (time-ordered, cryptographically random tail).
    pub lite_id: String,
    /// Monotonic epoch counter (incremented on every open).
    pub epoch: u64,
}

impl LiteIdentity {
    /// Load or create identity from storage.
    ///
    /// On first call (no identity in redb): generates UUID v7, sets epoch=1.
    /// On subsequent calls: reads existing ID, increments epoch.
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

        // Load, increment, and persist epoch.
        let epoch = match storage.get(ns, EPOCH_KEY).await? {
            Some(bytes) if bytes.len() == 8 => {
                let prev = u64::from_le_bytes(bytes.try_into().unwrap_or([0; 8]));
                prev + 1
            }
            _ => 1,
        };
        storage.put(ns, EPOCH_KEY, &epoch.to_le_bytes()).await?;

        Ok(Self { lite_id, epoch })
    }

    /// Regenerate identity (called after fork detection).
    ///
    /// Generates a new UUID v7, resets epoch to 1, persists both.
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
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::redb_storage::RedbStorage;

    #[tokio::test]
    async fn first_open_creates_identity() {
        let storage = RedbStorage::open_in_memory().unwrap();
        let identity = LiteIdentity::load_or_create(&storage).await.unwrap();
        assert!(!identity.lite_id.is_empty());
        assert_eq!(identity.epoch, 1);
    }

    #[tokio::test]
    async fn second_open_increments_epoch() {
        let storage = RedbStorage::open_in_memory().unwrap();
        let id1 = LiteIdentity::load_or_create(&storage).await.unwrap();
        let id2 = LiteIdentity::load_or_create(&storage).await.unwrap();
        assert_eq!(id1.lite_id, id2.lite_id); // Same ID.
        assert_eq!(id2.epoch, 2); // Epoch incremented.
    }

    #[tokio::test]
    async fn regenerate_changes_id() {
        let storage = RedbStorage::open_in_memory().unwrap();
        let mut id = LiteIdentity::load_or_create(&storage).await.unwrap();
        let original_id = id.lite_id.clone();
        id.regenerate(&storage).await.unwrap();
        assert_ne!(id.lite_id, original_id);
        assert_eq!(id.epoch, 1);
    }
}
