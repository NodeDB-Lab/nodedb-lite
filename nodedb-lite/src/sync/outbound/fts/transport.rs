// SPDX-License-Identifier: Apache-2.0

//! Durable drains, in-flight records, acknowledgments, and seq updates.

use super::{FtsOutbound, PendingFtsDelete, PendingFtsIndex};
use crate::{error::LiteError, storage::engine::StorageEngine};
use std::collections::HashSet;

impl<S: StorageEngine> FtsOutbound<S> {
    /// Drain index entries in per-kind FIFO order, excluding staged and in-flight keys.
    pub async fn drain_indexes(
        &self,
        limit: usize,
    ) -> Result<Vec<(Vec<u8>, PendingFtsIndex)>, LiteError> {
        let _flush = self.flush_mutex.lock().await;
        match self.spill_locked().await {
            Ok(()) | Err(LiteError::Backpressure { .. }) => {}
            Err(error) => return Err(error),
        }
        let reserved = self.staging_indexes.reserved_keys();
        let in_flight = self.in_flight_indexes.lock().await;
        let excluded: HashSet<&[u8]> = reserved
            .iter()
            .map(|key| key.as_slice())
            .chain(in_flight.values().map(Vec::as_slice))
            .collect();
        let pairs = self.durable_indexes.drain_batch(limit).await?;
        let mut out = Vec::with_capacity(pairs.len());
        for (key, payload) in pairs {
            if excluded.contains(key.as_slice()) {
                continue;
            }
            let entry =
                zerompk::from_msgpack(&payload).map_err(|error| LiteError::Serialization {
                    detail: format!("fts index outbound decode: {error}"),
                })?;
            out.push((key, entry));
        }
        Ok(out)
    }

    /// Drain delete entries in per-kind FIFO order, excluding staged and in-flight keys.
    pub async fn drain_deletes(
        &self,
        limit: usize,
    ) -> Result<Vec<(Vec<u8>, PendingFtsDelete)>, LiteError> {
        let _flush = self.flush_mutex.lock().await;
        match self.spill_locked().await {
            Ok(()) | Err(LiteError::Backpressure { .. }) => {}
            Err(error) => return Err(error),
        }
        let reserved = self.staging_deletes.reserved_keys();
        let in_flight = self.in_flight_deletes_map.lock().await;
        let excluded: HashSet<&[u8]> = reserved
            .iter()
            .map(|key| key.as_slice())
            .chain(in_flight.values().map(Vec::as_slice))
            .collect();
        let pairs = self.durable_deletes.drain_batch(limit).await?;
        let mut out = Vec::with_capacity(pairs.len());
        for (key, payload) in pairs {
            if excluded.contains(key.as_slice()) {
                continue;
            }
            let entry =
                zerompk::from_msgpack(&payload).map_err(|error| LiteError::Serialization {
                    detail: format!("fts delete outbound decode: {error}"),
                })?;
            out.push((key, entry));
        }
        Ok(out)
    }
    /// Record that an index entry has been sent to Origin and is awaiting its ack.
    pub async fn mark_index_in_flight(&self, batch_id: u64, durable_key: Vec<u8>) {
        self.in_flight_indexes
            .lock()
            .await
            .insert(batch_id, durable_key);
    }

    /// Remove the in-flight record for an index entry and return its durable key.
    pub async fn ack_index_in_flight(&self, batch_id: u64) -> Option<Vec<u8>> {
        self.in_flight_indexes.lock().await.remove(&batch_id)
    }

    /// Record that a delete entry has been sent to Origin and is awaiting its ack.
    pub async fn mark_delete_in_flight(&self, batch_id: u64, durable_key: Vec<u8>) {
        self.in_flight_deletes_map
            .lock()
            .await
            .insert(batch_id, durable_key);
    }

    /// Remove the in-flight record for a delete entry and return its durable key.
    pub async fn ack_delete_in_flight(&self, batch_id: u64) -> Option<Vec<u8>> {
        self.in_flight_deletes_map.lock().await.remove(&batch_id)
    }

    /// Clear all in-flight records on reconnect so entries are re-drained.
    pub async fn clear_in_flight(&self) {
        self.in_flight_indexes.lock().await.clear();
        self.in_flight_deletes_map.lock().await.clear();
    }

    /// Delete the durable index entries identified by `keys` (Origin ack path).
    pub async fn ack_index_keys(&self, keys: &[Vec<u8>]) -> Result<(), LiteError> {
        self.durable_indexes.ack_keys(keys).await
    }

    /// Delete the durable delete entries identified by `keys` (Origin ack path).
    pub async fn ack_delete_keys(&self, keys: &[Vec<u8>]) -> Result<(), LiteError> {
        self.durable_deletes.ack_keys(keys).await
    }

    /// Update the durable index payload for `key` with the new seq.
    pub async fn update_index_entry(
        &self,
        key: &[u8],
        entry: &PendingFtsIndex,
    ) -> Result<(), LiteError> {
        let payload = zerompk::to_msgpack_vec(entry).map_err(|e| LiteError::Serialization {
            detail: format!("fts index outbound update encode: {e}"),
        })?;
        self.durable_indexes.update_entry(key, &payload).await
    }

    /// Update the durable delete payload for `key` with the new seq.
    pub async fn update_delete_entry(
        &self,
        key: &[u8],
        entry: &PendingFtsDelete,
    ) -> Result<(), LiteError> {
        let payload = zerompk::to_msgpack_vec(entry).map_err(|e| LiteError::Serialization {
            detail: format!("fts delete outbound update encode: {e}"),
        })?;
        self.durable_deletes.update_entry(key, &payload).await
    }

    /// Number of pending index entries in durable storage.
    pub async fn pending_index_count(&self) -> Result<u64, LiteError> {
        self.durable_indexes.len().await
    }

    /// Number of pending delete entries in durable storage.
    pub async fn pending_delete_count(&self) -> Result<u64, LiteError> {
        self.durable_deletes.len().await
    }
}

#[cfg(test)]
mod tests {
    use super::super::injected::{InterruptedStorage, Interruption, PutRule};
    use super::*;
    use crate::nodedb::lock_ext::LockExt;
    use nodedb_types::Namespace;
    use std::sync::Arc;

    #[tokio::test]
    async fn staged_ambiguous_delete_is_excluded_while_index_queue_is_full() {
        let storage = Arc::new(InterruptedStorage::new().await);
        let queue = FtsOutbound::open_with_cap(Arc::clone(&storage), 1)
            .await
            .unwrap();
        queue
            .stage_index(
                "docs",
                "first",
                vec![("body".to_string(), "first".to_string())],
            )
            .unwrap();
        queue
            .stage_index(
                "docs",
                "blocked",
                vec![("body".to_string(), "blocked".to_string())],
            )
            .unwrap();
        queue.stage_delete("docs", "removed").unwrap();
        *storage.rule.lock_or_recover() = Some(PutRule {
            namespace: Namespace::FtsDeletePending,
            skip: 0,
            interruption: Interruption::BackpressureAfter,
        });
        assert!(matches!(
            queue.flush_staging().await,
            Err(LiteError::Backpressure { .. })
        ));
        assert_eq!(queue.pending_delete_count().await.unwrap(), 1);
        assert_eq!(queue.staging_deletes.len(), 1);
        let key = queue.staging_deletes.reserved_keys()[0];
        *storage.get_backpressure.lock_or_recover() = Some(Namespace::FtsDeletePending);
        assert!(queue.drain_deletes(10).await.unwrap().is_empty());
        assert_eq!(queue.staging_deletes.reserved_keys(), vec![key]);
        let rows = queue.drain_deletes(10).await.unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].0, key);
        assert_eq!(rows[0].1.doc_id, "removed");
    }

    #[tokio::test]
    async fn storage_errors_take_priority_over_other_kind_backpressure() {
        let storage = Arc::new(InterruptedStorage::new().await);
        let queue = FtsOutbound::open_with_cap(Arc::clone(&storage), 1)
            .await
            .unwrap();
        queue
            .stage_index(
                "docs",
                "first",
                vec![("body".to_string(), "first".to_string())],
            )
            .unwrap();
        queue
            .stage_index(
                "docs",
                "blocked",
                vec![("body".to_string(), "blocked".to_string())],
            )
            .unwrap();
        queue.stage_delete("docs", "removed").unwrap();
        *storage.rule.lock_or_recover() = Some(PutRule {
            namespace: Namespace::FtsDeletePending,
            skip: 0,
            interruption: Interruption::ErrorBefore,
        });
        assert!(matches!(
            queue.drain_indexes(10).await,
            Err(LiteError::Storage { .. })
        ));
        assert_eq!(queue.staging_indexes.len(), 1);
        assert_eq!(queue.staging_deletes.len(), 1);
        queue.flush_staging().await.unwrap_err();
        *storage.scan_error.lock_or_recover() = Some(Namespace::FtsIndexPending);
        assert!(matches!(
            queue.drain_indexes(10).await,
            Err(LiteError::Storage { .. })
        ));
        *storage.scan_error.lock_or_recover() = Some(Namespace::FtsDeletePending);
        assert!(matches!(
            queue.drain_deletes(10).await,
            Err(LiteError::Storage { .. })
        ));
    }
    async fn make_queue() -> FtsOutbound<crate::storage::pagedb_storage::PagedbStorageMem> {
        let storage = Arc::new(
            crate::storage::pagedb_storage::PagedbStorageMem::open_in_memory()
                .await
                .unwrap(),
        );
        FtsOutbound::open(storage).await.unwrap()
    }

    #[tokio::test]
    async fn ack_index_keys_removes_entries() {
        let q = make_queue().await;
        q.stage_index("docs", "d1", vec![("body".to_string(), "text".to_string())])
            .unwrap();
        q.stage_index(
            "docs",
            "d2",
            vec![("body".to_string(), "text2".to_string())],
        )
        .unwrap();
        q.flush_staging().await.unwrap();

        let pairs = q.drain_indexes(1).await.unwrap();
        assert_eq!(pairs.len(), 1);
        let keys: Vec<Vec<u8>> = pairs.into_iter().map(|(k, _)| k).collect();
        q.ack_index_keys(&keys).await.unwrap();

        assert_eq!(q.pending_index_count().await.unwrap(), 1);
    }

    #[tokio::test]
    async fn ack_delete_keys_removes_entries() {
        let q = make_queue().await;
        q.stage_delete("docs", "d1").unwrap();
        q.stage_delete("docs", "d2").unwrap();
        q.flush_staging().await.unwrap();

        let pairs = q.drain_deletes(1).await.unwrap();
        assert_eq!(pairs.len(), 1);
        let keys: Vec<Vec<u8>> = pairs.into_iter().map(|(k, _)| k).collect();
        q.ack_delete_keys(&keys).await.unwrap();

        assert_eq!(q.pending_delete_count().await.unwrap(), 1);
    }
}
