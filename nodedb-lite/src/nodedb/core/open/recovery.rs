// SPDX-License-Identifier: Apache-2.0

//! Post-open store-recovery driver.
//!
//! [`PagedbStorage::open`](crate::storage::pagedb_storage::PagedbStorage) only
//! self-heals corruption raised while opening the backing `Db`. Corruption
//! surfaced *after* open — during identity load or CRDT/index restore — is
//! typed as [`ErrorDetails::SegmentCorrupted`] and reaches here, where the
//! whole open sequence is re-driven once against a freshly recreated store.

use std::path::Path;

use nodedb_types::error::{ErrorDetails, NodeDbResult};

use crate::storage::encryption::Encryption;
use crate::storage::pagedb_storage::PagedbStorageDefault;

use crate::nodedb::core::types::NodeDbLite;

impl NodeDbLite<PagedbStorageDefault> {
    /// Open (or create) a Lite database at `path`, self-healing a corrupt store.
    ///
    /// Opens the pagedb-backed storage and runs the full cold-start restore. If
    /// the restore surfaces a corruption-class error
    /// ([`ErrorDetails::SegmentCorrupted`]), the corrupt store is renamed aside
    /// and recreated fresh, then the open is retried exactly once. The retry is
    /// unguarded: a second corruption surfaces to the caller rather than
    /// looping.
    pub async fn open_at_path(
        path: impl AsRef<Path>,
        peer_id: u64,
        encryption: Encryption,
    ) -> NodeDbResult<Self> {
        let path = path.as_ref();
        let storage = PagedbStorageDefault::open(path, encryption.clone()).await?;
        match Self::open(storage, peer_id).await {
            Ok(db) => Ok(db),
            Err(e) if matches!(e.details(), ErrorDetails::SegmentCorrupted { .. }) => {
                tracing::error!(
                    path = %path.display(),
                    error = %e,
                    "post-open corruption detected after storage opened cleanly — \
                     renaming corrupt store aside and recovering (one retry)"
                );
                let storage = PagedbStorageDefault::recover_corrupt(path, &encryption).await?;
                Self::open(storage, peer_id).await
            }
            Err(e) => Err(e),
        }
    }

    /// Like [`open_at_path`](Self::open_at_path), but with an explicit
    /// [`LiteConfig`](crate::config::LiteConfig).
    pub async fn open_at_path_with_config(
        path: impl AsRef<Path>,
        peer_id: u64,
        encryption: Encryption,
        config: crate::config::LiteConfig,
    ) -> NodeDbResult<Self> {
        let path = path.as_ref();
        let storage = PagedbStorageDefault::open(path, encryption.clone()).await?;
        match Self::open_with_config(storage, peer_id, config.clone()).await {
            Ok(db) => Ok(db),
            Err(e) if matches!(e.details(), ErrorDetails::SegmentCorrupted { .. }) => {
                tracing::error!(
                    path = %path.display(),
                    error = %e,
                    "post-open corruption detected after storage opened cleanly — \
                     renaming corrupt store aside and recovering (one retry)"
                );
                let storage = PagedbStorageDefault::recover_corrupt(path, &encryption).await?;
                Self::open_with_config(storage, peer_id, config).await
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
        let err = match NodeDbLite::open(wrapped, 1).await {
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
