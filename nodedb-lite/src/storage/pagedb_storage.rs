//! pagedb-backed `StorageEngine` implementation.
//!
//! Uses pagedb's B+ tree API for all sorted/sparse state that flows through
//! the `StorageEngine` trait. One `Db<V>` per `PagedbStorage`; namespacing is
//! achieved by prefixing every key with a single namespace byte, identical to
//! the redb-era encoding in `redb_storage.rs`.
//!
//! Two VFS variants are exposed via the generic parameter `V`:
//! - `PagedbStorage::<DefaultVfs>::open(path)` — native, platform async I/O.
//! - `PagedbStorage::<MemVfs>::open_in_memory()` — for tests and ephemeral use.
//!
//! Type aliases `PagedbStorageDefault` and `PagedbStorageMem` are provided for
//! ergonomics; callers rarely need to spell the generic.

use std::path::Path;
use std::sync::Arc;

use async_trait::async_trait;
use pagedb::errors::PagedbError;
use pagedb::options::{OpenOptions, RetainPolicy};
use pagedb::vfs::memory::MemVfs;
use pagedb::vfs::traits::Vfs;
use pagedb::{Db, RealmId};

use crate::error::LiteError;
use crate::storage::engine::{KvPair, StorageEngine, WriteOp};
use nodedb_types::Namespace;

// ─── VFS aliases ─────────────────────────────────────────────────────────────

#[cfg(not(target_arch = "wasm32"))]
use pagedb::vfs::DefaultVfs;

/// `PagedbStorage` backed by the native platform VFS (io_uring on Linux, etc.).
#[cfg(not(target_arch = "wasm32"))]
pub type PagedbStorageDefault = PagedbStorage<DefaultVfs>;

/// `PagedbStorage` backed by an in-memory VFS (tests / ephemeral use).
pub type PagedbStorageMem = PagedbStorage<MemVfs>;

// ─── Error mapping ───────────────────────────────────────────────────────────

/// Map `PagedbError` → `LiteError`.
///
/// `PagedbError::NotFound` is **not** mapped here — callers that expect a
/// missing-key result should convert the `Ok(None)` / empty-vec at the call
/// site rather than going through the error path.
///
/// `PagedbError::Quota` is mapped to `LiteError::Storage` for now. A dedicated
/// `LiteError::Quota` variant should be added in a follow-up (see
/// `resource/PAGEDB_GAPS.md` item #9) so that quota pressure is distinguishable
/// at the application layer without string-matching. This is documented deferral
/// — not a silent lump — so that the gap doc captures the intent.
impl From<PagedbError> for LiteError {
    fn from(e: PagedbError) -> Self {
        match e {
            PagedbError::Corruption(_) => LiteError::Storage {
                detail: format!("pagedb corruption: {e}"),
            },
            PagedbError::Quota { .. } => LiteError::Storage {
                detail: format!("pagedb quota exceeded: {e}"),
            },
            other => LiteError::Storage {
                detail: other.to_string(),
            },
        }
    }
}

/// Returns `true` when the error is a corruption-class error that should
/// trigger the rename-and-recreate recovery path in `PagedbStorage::open`.
fn is_corruption(e: &PagedbError) -> bool {
    matches!(e, PagedbError::Corruption(_) | PagedbError::ChecksumFailure)
}

// ─── Key helpers ─────────────────────────────────────────────────────────────

/// Build a composite key: `[namespace_byte, ...key_bytes]`.
///
/// Mirrors `RedbStorage::make_key`. The namespace byte is always the first
/// byte; B+ tree order is preserved because all keys within a namespace share
/// the same leading byte and are sorted lexicographically among themselves.
fn prefix_key(ns: Namespace, key: &[u8]) -> Vec<u8> {
    let mut k = Vec::with_capacity(1 + key.len());
    k.push(ns as u8);
    k.extend_from_slice(key);
    k
}

/// Strip the namespace prefix byte from a composite key returned by pagedb.
///
/// Returns an empty slice if `composite` has length ≤ 1 (defensive).
fn strip_prefix(composite: &[u8]) -> &[u8] {
    if composite.len() > 1 {
        &composite[1..]
    } else {
        &[]
    }
}

/// Exclusive end marker for namespace `n`: the first key that is strictly
/// greater than any key in namespace `n`.
///
/// For `n < 0xFF` this is `[n+1]` (one-byte boundary). `n == 0xFF` is not
/// assigned to any `Namespace` variant today and would require a two-byte
/// sentinel (`[0xFF, 0x00, ...]`). We assert this is unreachable to surface
/// any future `Namespace` addition that would violate the assumption.
fn ns_end(ns: Namespace) -> Vec<u8> {
    let b = ns as u8;
    assert!(
        b < 0xFF,
        "Namespace byte 0xFF would overflow the single-byte end-marker; \
         add a two-byte sentinel before assigning Namespace values in the 0xFF range"
    );
    vec![b + 1]
}

// ─── OpenOptions defaults for Lite ───────────────────────────────────────────

/// Build the `OpenOptions` used for all `PagedbStorage` instances.
///
/// `RetainPolicy::Disabled` is selected per `resource/PAGEDB_GAPS.md` item
/// #11: Lite does not need point-in-time reads; skipping commit-history
/// tracking shaves latency from every `WriteTxn::commit`.
fn lite_open_options() -> OpenOptions {
    OpenOptions::default().with_commit_history_retain(RetainPolicy::Disabled)
}

// ─── PagedbStorage ───────────────────────────────────────────────────────────

/// pagedb-backed KV storage.
///
/// The inner `Db<V>` lives behind `Arc` for cheap cloning across async methods.
/// No outer `Mutex` is needed: `Db::begin_write` already acquires an internal
/// async mutex (single-writer serialization is enforced by pagedb itself — see
/// `resource/PAGEDB_GAPS.md` item #8).
pub struct PagedbStorage<V: Vfs + Clone> {
    db: Arc<Db<V>>,
}

// Manual Clone so we don't require `V: Clone` on the struct level — the
// `Arc` clone is cheap and does not clone the underlying `Db`.
impl<V: Vfs + Clone> Clone for PagedbStorage<V> {
    fn clone(&self) -> Self {
        Self {
            db: Arc::clone(&self.db),
        }
    }
}

// ─── Native-only constructors ─────────────────────────────────────────────────

#[cfg(not(target_arch = "wasm32"))]
impl PagedbStorage<DefaultVfs> {
    /// Open or create a database at `path` using the platform-native async VFS.
    ///
    /// On corruption (`PagedbError::Corruption` / `ChecksumFailure`), the
    /// directory is renamed to `{path}.corrupt.{unix_secs}` and a fresh
    /// database is created — matching the recovery contract in `RedbStorage`.
    /// Data recovery happens via re-sync from Origin.
    ///
    /// # KEK placeholder
    ///
    /// The key-encryption key (`kek`) is currently hardcoded to `[0u8; 32]`.
    /// This is a **known gap** — see `resource/PAGEDB_GAPS.md` item #13.
    /// Do NOT use in production without replacing this with a proper KEK derived
    /// from user credentials or a hardware-backed key store.
    pub async fn open(path: impl AsRef<Path>) -> Result<Self, LiteError> {
        let path = path.as_ref();

        // TODO(PAGEDB_GAPS #13): replace with proper KEK before any production use.
        let kek = [0u8; 32];
        let realm = RealmId::new([0u8; 16]);

        let vfs = pagedb::vfs::open_default(path).map_err(LiteError::from)?;

        match Db::open(vfs, kek, 4096, realm, lite_open_options()).await {
            Ok(db) => Ok(Self { db: Arc::new(db) }),
            Err(e) if is_corruption(&e) && path.exists() => {
                let timestamp = std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap_or_default()
                    .as_secs();
                let corrupt_path = path.with_extension(format!("corrupt.{timestamp}"));

                tracing::error!(
                    path = %path.display(),
                    corrupt_backup = %corrupt_path.display(),
                    error = %e,
                    "pagedb database corrupted — renaming to backup and creating a fresh \
                     database. A full re-sync from Origin is required to recover data."
                );

                if let Err(rename_err) = std::fs::rename(path, &corrupt_path) {
                    tracing::error!(error = %rename_err, "failed to rename corrupted pagedb directory");
                    return Err(LiteError::Storage {
                        detail: format!(
                            "pagedb corrupted and rename failed: open={e}, rename={rename_err}"
                        ),
                    });
                }

                let vfs2 = pagedb::vfs::open_default(path).map_err(LiteError::from)?;
                let db = Db::open(vfs2, kek, 4096, realm, lite_open_options())
                    .await
                    .map_err(|e2| LiteError::Storage {
                        detail: format!(
                            "pagedb corrupted, backup saved to {}, fresh create failed: {e2}",
                            corrupt_path.display()
                        ),
                    })?;
                Ok(Self { db: Arc::new(db) })
            }
            Err(e) => Err(LiteError::from(e)),
        }
    }
}

impl PagedbStorage<MemVfs> {
    /// Create an in-memory database (for testing and WASM without persistence).
    ///
    /// # KEK placeholder
    ///
    /// Same placeholder KEK as `open` — see `resource/PAGEDB_GAPS.md` item #13.
    pub async fn open_in_memory() -> Result<Self, LiteError> {
        // TODO(PAGEDB_GAPS #13): replace with proper KEK before any production use.
        let kek = [0u8; 32];
        let realm = RealmId::new([0u8; 16]);
        let vfs = MemVfs::new();
        let db = Db::open(vfs, kek, 4096, realm, lite_open_options())
            .await
            .map_err(LiteError::from)?;
        Ok(Self { db: Arc::new(db) })
    }
}

// ─── StorageEngine impl — native ─────────────────────────────────────────────

#[cfg(not(target_arch = "wasm32"))]
#[async_trait]
impl<V: Vfs + Clone + Send + Sync + 'static> StorageEngine for PagedbStorage<V>
where
    <V as Vfs>::LockHandle: Sync,
{
    async fn get(&self, ns: Namespace, key: &[u8]) -> Result<Option<Vec<u8>>, LiteError> {
        let composite = prefix_key(ns, key);
        let txn = self.db.begin_read().await.map_err(LiteError::from)?;
        txn.get(&composite).await.map_err(LiteError::from)
    }

    async fn put(&self, ns: Namespace, key: &[u8], value: &[u8]) -> Result<(), LiteError> {
        let composite = prefix_key(ns, key);
        let mut txn = self.db.begin_write().await.map_err(LiteError::from)?;
        txn.put(&composite, value).await.map_err(LiteError::from)?;
        txn.commit().await.map(|_| ()).map_err(LiteError::from)
    }

    async fn delete(&self, ns: Namespace, key: &[u8]) -> Result<(), LiteError> {
        let composite = prefix_key(ns, key);
        let mut txn = self.db.begin_write().await.map_err(LiteError::from)?;
        txn.delete(&composite).await.map_err(LiteError::from)?;
        txn.commit().await.map(|_| ()).map_err(LiteError::from)
    }

    async fn scan_prefix(&self, ns: Namespace, prefix: &[u8]) -> Result<Vec<KvPair>, LiteError> {
        let ns_prefix = prefix_key(ns, prefix);
        let txn = self.db.begin_read().await.map_err(LiteError::from)?;
        let raw = txn.scan_prefix(&ns_prefix).await.map_err(LiteError::from)?;
        Ok(raw
            .into_iter()
            .map(|(k, v)| (strip_prefix(&k).to_vec(), v))
            .collect())
    }

    async fn batch_write(&self, ops: &[WriteOp]) -> Result<(), LiteError> {
        if ops.is_empty() {
            return Ok(());
        }

        let mut txn = self.db.begin_write().await.map_err(LiteError::from)?;

        // Detect duplicate keys (a key that appears in both a Put and a Delete,
        // or appears multiple times). When duplicates exist we fall through to
        // sequential per-op application to preserve original-order semantics.
        // Uniqueness check: if all keys are distinct we can use the fast batch path.
        let all_keys: Vec<Vec<u8>> = ops
            .iter()
            .map(|op| match op {
                WriteOp::Put { ns, key, .. } => prefix_key(*ns, key),
                WriteOp::Delete { ns, key } => prefix_key(*ns, key),
            })
            .collect();
        let unique_count = {
            let mut dedup = all_keys.clone();
            dedup.sort_unstable();
            dedup.dedup();
            dedup.len()
        };

        if unique_count < all_keys.len() {
            // Duplicate keys present — apply in order to preserve last-write semantics.
            for op in ops {
                match op {
                    WriteOp::Put { ns, key, value } => {
                        let composite = prefix_key(*ns, key);
                        txn.put(&composite, value).await.map_err(LiteError::from)?;
                    }
                    WriteOp::Delete { ns, key } => {
                        let composite = prefix_key(*ns, key);
                        txn.delete(&composite).await.map_err(LiteError::from)?;
                    }
                }
            }
        } else {
            // All keys distinct — partition into sorted puts + sorted deletes,
            // then call the batch APIs within the same WriteTxn (both commit atomically).
            let mut puts: Vec<(Vec<u8>, Vec<u8>)> = Vec::new();
            let mut deletes: Vec<Vec<u8>> = Vec::new();

            for op in ops {
                match op {
                    WriteOp::Put { ns, key, value } => {
                        puts.push((prefix_key(*ns, key), value.clone()));
                    }
                    WriteOp::Delete { ns, key } => {
                        deletes.push(prefix_key(*ns, key));
                    }
                }
            }

            puts.sort_unstable_by(|(a, _), (b, _)| a.cmp(b));
            deletes.sort_unstable();

            if !puts.is_empty() {
                txn.put_batch(puts).await.map_err(LiteError::from)?;
            }
            if !deletes.is_empty() {
                txn.delete_batch(deletes).await.map_err(LiteError::from)?;
            }
        }

        txn.commit().await.map(|_| ()).map_err(LiteError::from)
    }

    async fn count(&self, ns: Namespace) -> Result<u64, LiteError> {
        // No count primitive in pagedb B+ tree — scan the prefix and count.
        // See resource/PAGEDB_GAPS.md item #3.
        let ns_prefix = vec![ns as u8];
        let txn = self.db.begin_read().await.map_err(LiteError::from)?;
        let raw = txn.scan_prefix(&ns_prefix).await.map_err(LiteError::from)?;
        Ok(raw.len() as u64)
    }

    async fn scan_range(
        &self,
        ns: Namespace,
        start: &[u8],
        limit: usize,
    ) -> Result<Vec<KvPair>, LiteError> {
        let start_key = prefix_key(ns, start);
        let end_key = ns_end(ns);
        let txn = self.db.begin_read().await.map_err(LiteError::from)?;
        let raw = txn
            .scan(&start_key, &end_key)
            .await
            .map_err(LiteError::from)?;
        Ok(raw
            .into_iter()
            .take(limit)
            .map(|(k, v)| (strip_prefix(&k).to_vec(), v))
            .collect())
    }

    async fn scan_range_bounded(
        &self,
        ns: Namespace,
        start: Option<&[u8]>,
        end: Option<&[u8]>,
        limit: Option<usize>,
    ) -> Result<Vec<KvPair>, LiteError> {
        let start_key = match start {
            Some(s) => prefix_key(ns, s),
            None => vec![ns as u8],
        };
        let end_key = match end {
            Some(e) => prefix_key(ns, e),
            None => ns_end(ns),
        };
        let txn = self.db.begin_read().await.map_err(LiteError::from)?;
        let raw = txn
            .scan(&start_key, &end_key)
            .await
            .map_err(LiteError::from)?;
        let effective_limit = limit.unwrap_or(usize::MAX);
        Ok(raw
            .into_iter()
            .take(effective_limit)
            .map(|(k, v)| (strip_prefix(&k).to_vec(), v))
            .collect())
    }
}

// ─── StorageEngine impl — WASM ────────────────────────────────────────────────
//
// Stage 4 will add the OPFS-backed constructor and VFS. For now the trait impl
// compiles on WASM for any `V: Vfs + Clone` — the `?Send` bound is required
// because WASM is single-threaded. Native code uses the `Send + Sync` impl above.

#[cfg(target_arch = "wasm32")]
#[async_trait(?Send)]
impl<V: Vfs + Clone + 'static> StorageEngine for PagedbStorage<V> {
    async fn get(&self, ns: Namespace, key: &[u8]) -> Result<Option<Vec<u8>>, LiteError> {
        let composite = prefix_key(ns, key);
        let txn = self.db.begin_read().await.map_err(LiteError::from)?;
        txn.get(&composite).await.map_err(LiteError::from)
    }

    async fn put(&self, ns: Namespace, key: &[u8], value: &[u8]) -> Result<(), LiteError> {
        let composite = prefix_key(ns, key);
        let mut txn = self.db.begin_write().await.map_err(LiteError::from)?;
        txn.put(&composite, value).await.map_err(LiteError::from)?;
        txn.commit().await.map(|_| ()).map_err(LiteError::from)
    }

    async fn delete(&self, ns: Namespace, key: &[u8]) -> Result<(), LiteError> {
        let composite = prefix_key(ns, key);
        let mut txn = self.db.begin_write().await.map_err(LiteError::from)?;
        txn.delete(&composite).await.map_err(LiteError::from)?;
        txn.commit().await.map(|_| ()).map_err(LiteError::from)
    }

    async fn scan_prefix(&self, ns: Namespace, prefix: &[u8]) -> Result<Vec<KvPair>, LiteError> {
        let ns_prefix = prefix_key(ns, prefix);
        let txn = self.db.begin_read().await.map_err(LiteError::from)?;
        let raw = txn.scan_prefix(&ns_prefix).await.map_err(LiteError::from)?;
        Ok(raw
            .into_iter()
            .map(|(k, v)| (strip_prefix(&k).to_vec(), v))
            .collect())
    }

    async fn batch_write(&self, ops: &[WriteOp]) -> Result<(), LiteError> {
        if ops.is_empty() {
            return Ok(());
        }

        let mut txn = self.db.begin_write().await.map_err(LiteError::from)?;

        let all_keys: Vec<Vec<u8>> = ops
            .iter()
            .map(|op| match op {
                WriteOp::Put { ns, key, .. } => prefix_key(*ns, key),
                WriteOp::Delete { ns, key } => prefix_key(*ns, key),
            })
            .collect();
        let unique_count = {
            let mut dedup = all_keys.clone();
            dedup.sort_unstable();
            dedup.dedup();
            dedup.len()
        };

        if unique_count < all_keys.len() {
            for op in ops {
                match op {
                    WriteOp::Put { ns, key, value } => {
                        let composite = prefix_key(*ns, key);
                        txn.put(&composite, value).await.map_err(LiteError::from)?;
                    }
                    WriteOp::Delete { ns, key } => {
                        let composite = prefix_key(*ns, key);
                        txn.delete(&composite).await.map_err(LiteError::from)?;
                    }
                }
            }
        } else {
            let mut puts: Vec<(Vec<u8>, Vec<u8>)> = Vec::new();
            let mut deletes: Vec<Vec<u8>> = Vec::new();

            for op in ops {
                match op {
                    WriteOp::Put { ns, key, value } => {
                        puts.push((prefix_key(*ns, key), value.clone()));
                    }
                    WriteOp::Delete { ns, key } => {
                        deletes.push(prefix_key(*ns, key));
                    }
                }
            }

            puts.sort_unstable_by(|(a, _), (b, _)| a.cmp(b));
            deletes.sort_unstable();

            if !puts.is_empty() {
                txn.put_batch(puts).await.map_err(LiteError::from)?;
            }
            if !deletes.is_empty() {
                txn.delete_batch(deletes).await.map_err(LiteError::from)?;
            }
        }

        txn.commit().await.map(|_| ()).map_err(LiteError::from)
    }

    async fn count(&self, ns: Namespace) -> Result<u64, LiteError> {
        let ns_prefix = vec![ns as u8];
        let txn = self.db.begin_read().await.map_err(LiteError::from)?;
        let raw = txn.scan_prefix(&ns_prefix).await.map_err(LiteError::from)?;
        Ok(raw.len() as u64)
    }

    async fn scan_range(
        &self,
        ns: Namespace,
        start: &[u8],
        limit: usize,
    ) -> Result<Vec<KvPair>, LiteError> {
        let start_key = prefix_key(ns, start);
        let end_key = ns_end(ns);
        let txn = self.db.begin_read().await.map_err(LiteError::from)?;
        let raw = txn
            .scan(&start_key, &end_key)
            .await
            .map_err(LiteError::from)?;
        Ok(raw
            .into_iter()
            .take(limit)
            .map(|(k, v)| (strip_prefix(&k).to_vec(), v))
            .collect())
    }

    async fn scan_range_bounded(
        &self,
        ns: Namespace,
        start: Option<&[u8]>,
        end: Option<&[u8]>,
        limit: Option<usize>,
    ) -> Result<Vec<KvPair>, LiteError> {
        let start_key = match start {
            Some(s) => prefix_key(ns, s),
            None => vec![ns as u8],
        };
        let end_key = match end {
            Some(e) => prefix_key(ns, e),
            None => ns_end(ns),
        };
        let txn = self.db.begin_read().await.map_err(LiteError::from)?;
        let raw = txn
            .scan(&start_key, &end_key)
            .await
            .map_err(LiteError::from)?;
        let effective_limit = limit.unwrap_or(usize::MAX);
        Ok(raw
            .into_iter()
            .take(effective_limit)
            .map(|(k, v)| (strip_prefix(&k).to_vec(), v))
            .collect())
    }
}

// ─── Tests ───────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    async fn make_storage() -> PagedbStorage<MemVfs> {
        PagedbStorage::open_in_memory().await.unwrap()
    }

    #[tokio::test]
    async fn put_get_roundtrip() {
        let s = make_storage().await;
        s.put(Namespace::Vector, b"v1", b"hello").await.unwrap();
        let val = s.get(Namespace::Vector, b"v1").await.unwrap();
        assert_eq!(val.as_deref(), Some(b"hello".as_slice()));
    }

    #[tokio::test]
    async fn get_missing_returns_none() {
        let s = make_storage().await;
        let val = s.get(Namespace::Vector, b"nope").await.unwrap();
        assert!(val.is_none());
    }

    #[tokio::test]
    async fn put_overwrites() {
        let s = make_storage().await;
        s.put(Namespace::Graph, b"k", b"first").await.unwrap();
        s.put(Namespace::Graph, b"k", b"second").await.unwrap();
        let val = s.get(Namespace::Graph, b"k").await.unwrap();
        assert_eq!(val.as_deref(), Some(b"second".as_slice()));
    }

    #[tokio::test]
    async fn delete_removes_key() {
        let s = make_storage().await;
        s.put(Namespace::Crdt, b"k", b"val").await.unwrap();
        s.delete(Namespace::Crdt, b"k").await.unwrap();
        assert!(s.get(Namespace::Crdt, b"k").await.unwrap().is_none());
    }

    #[tokio::test]
    async fn delete_nonexistent_is_noop() {
        let s = make_storage().await;
        s.delete(Namespace::Meta, b"ghost").await.unwrap();
    }

    #[tokio::test]
    async fn namespaces_are_isolated() {
        let s = make_storage().await;
        s.put(Namespace::Vector, b"k", b"vec").await.unwrap();
        s.put(Namespace::Graph, b"k", b"graph").await.unwrap();

        assert_eq!(
            s.get(Namespace::Vector, b"k").await.unwrap().as_deref(),
            Some(b"vec".as_slice())
        );
        assert_eq!(
            s.get(Namespace::Graph, b"k").await.unwrap().as_deref(),
            Some(b"graph".as_slice())
        );
    }

    #[tokio::test]
    async fn scan_prefix_basic() {
        let s = make_storage().await;
        s.put(Namespace::Vector, b"vec:001", b"a").await.unwrap();
        s.put(Namespace::Vector, b"vec:002", b"b").await.unwrap();
        s.put(Namespace::Vector, b"vec:003", b"c").await.unwrap();
        s.put(Namespace::Vector, b"other:001", b"d").await.unwrap();

        let results = s.scan_prefix(Namespace::Vector, b"vec:").await.unwrap();
        assert_eq!(results.len(), 3);
        assert_eq!(results[0].0, b"vec:001");
        assert_eq!(results[1].0, b"vec:002");
        assert_eq!(results[2].0, b"vec:003");
    }

    #[tokio::test]
    async fn scan_prefix_empty_returns_all() {
        let s = make_storage().await;
        s.put(Namespace::Meta, b"a", b"1").await.unwrap();
        s.put(Namespace::Meta, b"b", b"2").await.unwrap();
        s.put(Namespace::Vector, b"c", b"3").await.unwrap();

        let results = s.scan_prefix(Namespace::Meta, b"").await.unwrap();
        assert_eq!(results.len(), 2);
    }

    #[tokio::test]
    async fn scan_prefix_no_match() {
        let s = make_storage().await;
        s.put(Namespace::Graph, b"edge:1", b"data").await.unwrap();
        let results = s.scan_prefix(Namespace::Graph, b"node:").await.unwrap();
        assert!(results.is_empty());
    }

    #[tokio::test]
    async fn batch_write_atomic() {
        let s = make_storage().await;
        s.put(Namespace::Crdt, b"to_delete", b"old").await.unwrap();

        s.batch_write(&[
            WriteOp::Put {
                ns: Namespace::Crdt,
                key: b"new1".to_vec(),
                value: b"val1".to_vec(),
            },
            WriteOp::Put {
                ns: Namespace::Crdt,
                key: b"new2".to_vec(),
                value: b"val2".to_vec(),
            },
            WriteOp::Delete {
                ns: Namespace::Crdt,
                key: b"to_delete".to_vec(),
            },
        ])
        .await
        .unwrap();

        assert!(s.get(Namespace::Crdt, b"new1").await.unwrap().is_some());
        assert!(s.get(Namespace::Crdt, b"new2").await.unwrap().is_some());
        assert!(
            s.get(Namespace::Crdt, b"to_delete")
                .await
                .unwrap()
                .is_none()
        );
    }

    /// Same-key put-then-delete in a batch: the delete must win.
    #[tokio::test]
    async fn batch_write_same_key_put_then_delete() {
        let s = make_storage().await;
        s.batch_write(&[
            WriteOp::Put {
                ns: Namespace::Meta,
                key: b"clash".to_vec(),
                value: b"written".to_vec(),
            },
            WriteOp::Delete {
                ns: Namespace::Meta,
                key: b"clash".to_vec(),
            },
        ])
        .await
        .unwrap();
        // Delete came after Put in the ops slice, so the key must be absent.
        assert!(s.get(Namespace::Meta, b"clash").await.unwrap().is_none());
    }

    /// Same-key delete-then-put in a batch: the put must win.
    #[tokio::test]
    async fn batch_write_same_key_delete_then_put() {
        let s = make_storage().await;
        s.put(Namespace::Meta, b"exists", b"old").await.unwrap();
        s.batch_write(&[
            WriteOp::Delete {
                ns: Namespace::Meta,
                key: b"exists".to_vec(),
            },
            WriteOp::Put {
                ns: Namespace::Meta,
                key: b"exists".to_vec(),
                value: b"new".to_vec(),
            },
        ])
        .await
        .unwrap();
        // Put came after Delete, so the key must be present with the new value.
        assert_eq!(
            s.get(Namespace::Meta, b"exists").await.unwrap().as_deref(),
            Some(b"new".as_slice())
        );
    }

    #[tokio::test]
    async fn batch_write_empty_is_noop() {
        let s = make_storage().await;
        s.batch_write(&[]).await.unwrap();
    }

    #[tokio::test]
    async fn count_entries() {
        let s = make_storage().await;
        assert_eq!(s.count(Namespace::Vector).await.unwrap(), 0);

        s.put(Namespace::Vector, b"v1", b"a").await.unwrap();
        s.put(Namespace::Vector, b"v2", b"b").await.unwrap();
        s.put(Namespace::Graph, b"g1", b"c").await.unwrap();

        assert_eq!(s.count(Namespace::Vector).await.unwrap(), 2);
        assert_eq!(s.count(Namespace::Graph).await.unwrap(), 1);
        assert_eq!(s.count(Namespace::Crdt).await.unwrap(), 0);
    }

    #[tokio::test]
    async fn large_value_roundtrip() {
        let s = make_storage().await;
        let large = vec![0xABu8; 1_000_000];
        s.put(Namespace::Vector, b"hnsw:layer0", &large)
            .await
            .unwrap();
        let val = s.get(Namespace::Vector, b"hnsw:layer0").await.unwrap();
        assert_eq!(val.unwrap().len(), 1_000_000);
    }

    #[tokio::test]
    async fn scan_range_with_limit() {
        let s = make_storage().await;
        for i in 0u8..10 {
            s.put(Namespace::Vector, &[i], &[i * 2]).await.unwrap();
        }
        let results = s.scan_range(Namespace::Vector, &[0], 3).await.unwrap();
        assert_eq!(results.len(), 3);
        assert_eq!(results[0].0, &[0u8]);
        assert_eq!(results[1].0, &[1u8]);
        assert_eq!(results[2].0, &[2u8]);
    }

    #[tokio::test]
    async fn scan_range_bounded_with_start_and_end() {
        let s = make_storage().await;
        for i in 0u8..10 {
            s.put(Namespace::Graph, &[i], &[i]).await.unwrap();
        }
        // Keys [2, 3, 4] — start inclusive, end exclusive.
        let results = s
            .scan_range_bounded(Namespace::Graph, Some(&[2]), Some(&[5]), None)
            .await
            .unwrap();
        assert_eq!(results.len(), 3);
        assert_eq!(results[0].0, &[2u8]);
        assert_eq!(results[1].0, &[3u8]);
        assert_eq!(results[2].0, &[4u8]);
    }

    /// Keys in namespace N must not appear in a scan of namespace N+1, and
    /// vice versa. Verifies the single-byte prefix boundary.
    #[tokio::test]
    async fn scan_range_bounded_namespace_isolation() {
        let s = make_storage().await;

        // Write keys into two consecutive namespaces.
        for i in 0u8..5 {
            s.put(Namespace::Vector, &[i], b"vec").await.unwrap();
        }
        for i in 0u8..5 {
            s.put(Namespace::Graph, &[i], b"graph").await.unwrap();
        }

        // Full unbounded scan of Vector must return only Vector entries.
        let vec_results = s
            .scan_range_bounded(Namespace::Vector, None, None, None)
            .await
            .unwrap();
        assert_eq!(
            vec_results.len(),
            5,
            "Vector scan leaked into another namespace"
        );
        assert!(vec_results.iter().all(|(_, v)| v == b"vec"));

        // Full unbounded scan of Graph must return only Graph entries.
        let graph_results = s
            .scan_range_bounded(Namespace::Graph, None, None, None)
            .await
            .unwrap();
        assert_eq!(
            graph_results.len(),
            5,
            "Graph scan leaked into another namespace"
        );
        assert!(graph_results.iter().all(|(_, v)| v == b"graph"));
    }
}
