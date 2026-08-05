// SPDX-License-Identifier: Apache-2.0

//! Opening a Lite database from a filesystem path.
//!
//! Corruption can surface at two points: while opening the backing `Db`, and
//! later during identity load or CRDT/index restore, where it arrives typed as
//! [`ErrorDetails::SegmentCorrupted`]. Both are the same fault to the caller,
//! so both obey the same
//! [`CorruptionPolicy`](crate::storage::corruption::CorruptionPolicy) — either
//! the open fails and the store is left alone, or the caller has opted into
//! discarding it and the whole open sequence is re-driven once against a fresh
//! store.

use std::path::Path;
use std::sync::Arc;

use nodedb_types::error::{ErrorDetails, NodeDbResult};

use crate::config::LiteConfig;
use crate::storage::encryption::Encryption;
use crate::storage::pagedb_storage::PagedbStorageDefault;

use crate::nodedb::core::types::NodeDbLite;

impl NodeDbLite<PagedbStorageDefault> {
    /// Open (or create) a Lite database at `path`.
    ///
    /// A store that cannot be read is reported and left untouched — the
    /// [`FailClosed`](crate::storage::corruption::CorruptionPolicy::FailClosed) default. Use
    /// [`open_at_path_with_config`](Self::open_at_path_with_config) with
    /// `corruption_policy` set to choose otherwise.
    pub async fn open_at_path(
        path: impl AsRef<Path>,
        encryption: Encryption,
    ) -> NodeDbResult<Arc<Self>> {
        Self::open_at_path_with_config(path, encryption, LiteConfig::default()).await
    }

    /// Like [`open_at_path`](Self::open_at_path), but with an explicit
    /// [`LiteConfig`].
    ///
    /// `config.corruption_policy` governs both corruption points. Under
    /// [`DiscardStoreAndRecreate`](crate::storage::corruption::CorruptionPolicy::DiscardStoreAndRecreate) a post-open corruption
    /// renames the store aside and retries the open exactly once; the retry is
    /// unguarded, so a second corruption surfaces to the caller rather than
    /// looping.
    pub async fn open_at_path_with_config(
        path: impl AsRef<Path>,
        encryption: Encryption,
        config: LiteConfig,
    ) -> NodeDbResult<Arc<Self>> {
        Self::open_at_path_with_config_and_page_size(path, encryption, config, 4096).await
    }

    /// Like [`open_at_path_with_config`](Self::open_at_path_with_config), but
    /// chooses the durable PageDB page size when creating the store.
    ///
    /// The same size must be supplied on every reopen of that store.
    pub async fn open_at_path_with_config_and_page_size(
        path: impl AsRef<Path>,
        encryption: Encryption,
        config: LiteConfig,
        page_size: usize,
    ) -> NodeDbResult<Arc<Self>> {
        let path = path.as_ref();
        let policy = config.corruption_policy;
        let storage = PagedbStorageDefault::open_with_policy_and_page_size(
            path,
            encryption.clone(),
            policy,
            page_size,
        )
        .await?;
        match Self::open_with_config(storage, config.clone()).await {
            Ok(db) => Ok(db),
            Err(e)
                if policy.may_discard()
                    && matches!(e.details(), ErrorDetails::SegmentCorrupted { .. }) =>
            {
                tracing::error!(
                    path = %path.display(),
                    error = %e,
                    "post-open corruption detected after storage opened cleanly — the caller \
                     opted into discarding the store, renaming it aside and retrying once"
                );
                let storage = PagedbStorageDefault::discard_and_recreate_with_page_size(
                    path,
                    &encryption,
                    page_size,
                )
                .await?;
                Self::open_with_config(storage, config).await
            }
            Err(e) => Err(e),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicBool, Ordering};

    use async_trait::async_trait;
    use nodedb_types::Namespace;
    use nodedb_types::error::ErrorDetails;

    use crate::error::LiteError;
    use crate::nodedb::core::types::NodeDbLite;
    use crate::storage::engine::{CompactionOutcome, KvPair, StorageEngine, WriteOp};
    use crate::storage::pagedb_storage::PagedbStorageMem;

    /// Wraps an in-memory engine and injects a single corruption on the first
    /// read of the Lite identity key (`meta:lite_id`). This proves the
    /// corruption signal travels typed all the way from a post-open storage
    /// read to `NodeDbError::details() == ErrorDetails::SegmentCorrupted`.
    struct CorruptOnFirstIdentityRead {
        inner: PagedbStorageMem,
        tripped: AtomicBool,
    }

    #[async_trait]
    impl StorageEngine for CorruptOnFirstIdentityRead {
        async fn get(&self, ns: Namespace, key: &[u8]) -> Result<Option<Vec<u8>>, LiteError> {
            if ns == Namespace::Meta
                && key == b"meta:lite_id"
                && !self.tripped.swap(true, Ordering::SeqCst)
            {
                return Err(LiteError::Corrupted {
                    detail: "injected identity-read corruption".into(),
                });
            }
            self.inner.get(ns, key).await
        }

        async fn put(&self, ns: Namespace, key: &[u8], value: &[u8]) -> Result<(), LiteError> {
            self.inner.put(ns, key, value).await
        }

        async fn delete(&self, ns: Namespace, key: &[u8]) -> Result<(), LiteError> {
            self.inner.delete(ns, key).await
        }

        async fn scan_prefix(
            &self,
            ns: Namespace,
            prefix: &[u8],
        ) -> Result<Vec<KvPair>, LiteError> {
            self.inner.scan_prefix(ns, prefix).await
        }

        async fn batch_write(&self, ops: &[WriteOp]) -> Result<(), LiteError> {
            self.inner.batch_write(ops).await
        }

        async fn count(&self, ns: Namespace) -> Result<u64, LiteError> {
            self.inner.count(ns).await
        }

        async fn compact(&self) -> Result<CompactionOutcome, LiteError> {
            self.inner.compact().await
        }

        async fn scan_range(
            &self,
            ns: Namespace,
            start: &[u8],
            limit: usize,
        ) -> Result<Vec<KvPair>, LiteError> {
            self.inner.scan_range(ns, start, limit).await
        }

        async fn scan_range_bounded(
            &self,
            ns: Namespace,
            start: Option<&[u8]>,
            end: Option<&[u8]>,
            limit: Option<usize>,
        ) -> Result<Vec<KvPair>, LiteError> {
            self.inner.scan_range_bounded(ns, start, end, limit).await
        }
    }

    #[tokio::test]
    async fn post_open_corruption_surfaces_typed() {
        let inner = PagedbStorageMem::open_in_memory()
            .await
            .expect("in-memory storage opens");
        let wrapped = CorruptOnFirstIdentityRead {
            inner,
            tripped: AtomicBool::new(false),
        };

        // `NodeDbLite` isn't `Debug`, so match rather than `expect_err`.
        let err = match NodeDbLite::open(wrapped).await {
            Ok(_) => panic!("first identity read is corrupted, so open must fail"),
            Err(e) => e,
        };

        assert!(
            matches!(err.details(), ErrorDetails::SegmentCorrupted { .. }),
            "corruption must propagate as SegmentCorrupted, got: {:?}",
            err.details()
        );
    }
}
