// SPDX-License-Identifier: Apache-2.0

//! Stable front keys survive storage errors and cancellation.

use super::{FtsOutbound, staging::Staging};
use crate::{
    error::LiteError, storage::engine::StorageEngine,
    sync::outbound::durable_queue::DurableOutboundQueue,
};

impl<S: StorageEngine> FtsOutbound<S> {
    /// Spill both kinds. Staged ownership persists through every await.
    pub async fn flush_staging(&self) -> Result<(), LiteError> {
        let _flush = self.flush_mutex.lock().await;
        self.spill_locked().await
    }

    pub(super) async fn spill_locked(&self) -> Result<(), LiteError> {
        let indexes = spill(&self.staging_indexes, &self.durable_indexes).await;
        let deletes = spill(&self.staging_deletes, &self.durable_deletes).await;
        match (indexes, deletes) {
            (Err(error), _) if !matches!(error, LiteError::Backpressure { .. }) => Err(error),
            (_, Err(error)) if !matches!(error, LiteError::Backpressure { .. }) => Err(error),
            (Err(error), _) | (_, Err(error)) => Err(error),
            _ => Ok(()),
        }
    }
}

async fn spill<S: StorageEngine, T: zerompk::ToMessagePack>(
    staging: &Staging<T>,
    durable: &DurableOutboundQueue<S>,
) -> Result<(), LiteError> {
    while let Some((key, entry)) = staging.front_reserved(durable)? {
        let payload =
            zerompk::to_msgpack_vec(&*entry).map_err(|error| LiteError::Serialization {
                detail: format!("fts outbound encode: {error}"),
            })?;
        durable.persist_reserved(&key, &payload).await?;
        staging.remove_front(key)?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::pagedb_storage::PagedbStorageMem;
    use std::sync::Arc;

    async fn make_queue() -> FtsOutbound<PagedbStorageMem> {
        let storage = Arc::new(PagedbStorageMem::open_in_memory().await.unwrap());
        FtsOutbound::open(storage).await.unwrap()
    }

    #[tokio::test]
    async fn stage_and_flush_indexes() {
        let q = make_queue().await;
        q.stage_index(
            "docs",
            "d1",
            vec![("body".to_string(), "hello world".to_string())],
        )
        .unwrap();
        q.stage_index(
            "docs",
            "d2",
            vec![("body".to_string(), "rust rocks".to_string())],
        )
        .unwrap();

        // Before flush, durable queue is empty.
        assert_eq!(q.pending_index_count().await.unwrap(), 0);

        q.flush_staging().await.unwrap();

        let pairs = q.drain_indexes(usize::MAX).await.unwrap();
        assert_eq!(pairs.len(), 2);
        assert_eq!(pairs[0].1.doc_id, "d1");
        assert_eq!(pairs[1].1.doc_id, "d2");
    }

    #[tokio::test]
    async fn stage_and_flush_deletes() {
        let q = make_queue().await;
        q.stage_delete("docs", "d1").unwrap();

        q.flush_staging().await.unwrap();

        let pairs = q.drain_deletes(usize::MAX).await.unwrap();
        assert_eq!(pairs.len(), 1);
        assert_eq!(pairs[0].1.doc_id, "d1");
    }

    #[tokio::test]
    async fn durable_cap_returns_backpressure() {
        let storage = Arc::new(PagedbStorageMem::open_in_memory().await.unwrap());
        let q = FtsOutbound::open_with_cap(storage, 2).await.unwrap();
        q.stage_index("docs", "a", vec![("body".to_string(), "foo".to_string())])
            .unwrap();
        q.stage_index("docs", "b", vec![("body".to_string(), "bar".to_string())])
            .unwrap();
        q.stage_index("docs", "c", vec![("body".to_string(), "baz".to_string())])
            .unwrap();
        // Durable capacity admits a and b. The next entry remains staged.
        let err = q.flush_staging().await.unwrap_err();
        assert!(matches!(err, LiteError::Backpressure { .. }));
        // The excluded entry remains staged.
        assert_eq!(q.staging_indexes.len(), 1);
    }

    #[tokio::test]
    async fn survives_reload() {
        let storage = Arc::new(PagedbStorageMem::open_in_memory().await.unwrap());
        {
            let q = FtsOutbound::open(Arc::clone(&storage)).await.unwrap();
            q.stage_index("docs", "v1", vec![("body".to_string(), "text".to_string())])
                .unwrap();
            q.flush_staging().await.unwrap();
        }
        let q = FtsOutbound::open(Arc::clone(&storage)).await.unwrap();
        assert_eq!(q.pending_index_count().await.unwrap(), 1);
        q.stage_index(
            "docs",
            "v2",
            vec![("body".to_string(), "text2".to_string())],
        )
        .unwrap();
        q.flush_staging().await.unwrap();
        assert_eq!(q.pending_index_count().await.unwrap(), 2);
    }
    #[tokio::test]
    async fn midpoint_storage_rejection_preserves_persisted_prefix_and_staged_front_tail() {
        use super::super::injected::{InterruptedStorage, Interruption, PutRule};
        use crate::nodedb::lock_ext::LockExt;
        use nodedb_types::Namespace;
        let storage = Arc::new(InterruptedStorage::new().await);
        let queue = FtsOutbound::open(Arc::clone(&storage)).await.unwrap();
        for id in ["prefix", "front", "tail"] {
            queue
                .stage_index("docs", id, vec![("body".to_string(), id.to_string())])
                .unwrap();
        }
        *storage.rule.lock_or_recover() = Some(PutRule {
            namespace: Namespace::FtsIndexPending,
            skip: 1,
            interruption: Interruption::ErrorBefore,
        });
        assert!(matches!(
            queue.flush_staging().await,
            Err(LiteError::Storage { .. })
        ));
        assert_eq!(queue.pending_index_count().await.unwrap(), 1);
        assert_eq!(queue.staging_indexes.len(), 2);
        let front_key = queue.staging_indexes.reserved_keys()[0];
        let rows = queue.drain_indexes(10).await.unwrap();
        assert_eq!(
            rows.iter()
                .map(|(_, entry)| entry.doc_id.as_str())
                .collect::<Vec<_>>(),
            vec!["prefix", "front", "tail"]
        );
        assert_eq!(rows[1].0, front_key);
    }

    #[cfg(not(target_arch = "wasm32"))]
    #[tokio::test]
    async fn cancellation_before_and_after_commit_preserves_front_key_and_tail() {
        use super::super::injected::{InterruptedStorage, Interruption, PutRule};
        use crate::nodedb::lock_ext::LockExt;
        use nodedb_types::Namespace;
        for after in [false, true] {
            let storage = Arc::new(InterruptedStorage::new().await);
            let queue = Arc::new(FtsOutbound::open(Arc::clone(&storage)).await.unwrap());
            for id in ["front", "tail"] {
                queue
                    .stage_index("docs", id, vec![("body".to_string(), id.to_string())])
                    .unwrap();
            }
            *storage.rule.lock_or_recover() = Some(PutRule {
                namespace: Namespace::FtsIndexPending,
                skip: 0,
                interruption: if after {
                    Interruption::PauseAfter
                } else {
                    Interruption::PauseBefore
                },
            });
            let task = {
                let queue = Arc::clone(&queue);
                tokio::spawn(async move { queue.flush_staging().await })
            };
            storage.entered.notified().await;
            let key = queue.staging_indexes.reserved_keys()[0];
            assert_eq!(queue.staging_indexes.len(), 2);
            assert_eq!(queue.pending_index_count().await.unwrap(), u64::from(after));
            task.abort();
            assert!(task.await.unwrap_err().is_cancelled());
            queue
                .stage_index("docs", "new", vec![("body".to_string(), "new".to_string())])
                .unwrap();
            queue.flush_staging().await.unwrap();
            let rows = queue.drain_indexes(10).await.unwrap();
            assert_eq!(rows.len(), 3);
            assert_eq!(rows[0].0, key);
            assert_eq!(
                rows.iter()
                    .map(|(_, entry)| entry.doc_id.as_str())
                    .collect::<Vec<_>>(),
                vec!["front", "tail", "new"]
            );
        }
    }

    #[tokio::test]
    async fn ambiguous_commit_retry_preserves_nonzero_seq_at_capacity() {
        use super::super::injected::{InterruptedStorage, Interruption, PutRule};
        use crate::nodedb::lock_ext::LockExt;
        use nodedb_types::Namespace;
        let storage = Arc::new(InterruptedStorage::new().await);
        let queue = FtsOutbound::open_with_cap(Arc::clone(&storage), 1)
            .await
            .unwrap();
        queue
            .stage_index(
                "docs",
                "front",
                vec![("body".to_string(), "alpha".to_string())],
            )
            .unwrap();
        *storage.rule.lock_or_recover() = Some(PutRule {
            namespace: Namespace::FtsIndexPending,
            skip: 0,
            interruption: Interruption::ErrorAfter,
        });
        assert!(matches!(
            queue.flush_staging().await,
            Err(LiteError::Storage { .. })
        ));
        let key = queue.staging_indexes.reserved_keys()[0];
        let bytes = storage
            .inner
            .get(Namespace::FtsIndexPending, &key)
            .await
            .unwrap()
            .unwrap();
        let mut entry: super::super::PendingFtsIndex = zerompk::from_msgpack(&bytes).unwrap();
        entry.seq = 77;
        queue.update_index_entry(&key, &entry).await.unwrap();
        queue.flush_staging().await.unwrap();
        assert_eq!(queue.staging_indexes.len(), 0);
        let rows = queue.drain_indexes(10).await.unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].0, key);
        assert_eq!(rows[0].1.seq, 77);
    }

    #[tokio::test]
    async fn index_backpressure_does_not_block_delete_spill_or_ack_retry() {
        let storage = Arc::new(PagedbStorageMem::open_in_memory().await.unwrap());
        let queue = FtsOutbound::open_with_cap(storage, 1).await.unwrap();
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
                "next",
                vec![("body".to_string(), "next".to_string())],
            )
            .unwrap();
        queue.stage_delete("docs", "removed").unwrap();
        assert!(matches!(
            queue.flush_staging().await,
            Err(LiteError::Backpressure { .. })
        ));
        assert_eq!(queue.pending_delete_count().await.unwrap(), 1);
        let rows = queue.drain_indexes(1).await.unwrap();
        assert_eq!(rows.len(), 1);
        queue.ack_index_keys(&[rows[0].0.clone()]).await.unwrap();
        queue.flush_staging().await.unwrap();
        let rows = queue.drain_indexes(1).await.unwrap();
        assert_eq!(rows[0].1.doc_id, "next");
        assert_eq!(queue.staging_indexes.len(), 0);
    }
}
