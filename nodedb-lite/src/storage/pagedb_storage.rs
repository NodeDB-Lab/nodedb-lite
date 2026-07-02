//! pagedb-backed `StorageEngine` implementation.
//!
//! Uses pagedb's B+ tree API for all sorted/sparse state that flows through
//! the `StorageEngine` trait. One `Db<V>` per `PagedbStorage`; namespacing is
//! achieved by prefixing every key with a single namespace byte, identical to
//! the original key-encoding convention (namespace byte first).
//!
//! Two VFS variants are exposed via the generic parameter `V`:
//! - `PagedbStorage::<DefaultVfs>::open(path)` — native, platform async I/O.
//! - `PagedbStorage::<MemVfs>::open_in_memory()` — for tests and ephemeral use.
//!
//! Type aliases `PagedbStorageDefault` and `PagedbStorageMem` are provided for
//! ergonomics; callers rarely need to spell the generic.

// `Path` and the corruption-recovery helpers are only used by the native
// `open()` rename-and-recreate path, which is compiled out on wasm32 (OPFS).
#[cfg(not(target_arch = "wasm32"))]
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

/// `PagedbStorage` backed by the browser OPFS VFS (persistent, wasm32 only).
///
/// Constructed via [`PagedbStorage::open_opfs`].
#[cfg(target_arch = "wasm32")]
pub type PagedbStorageOpfs = PagedbStorage<pagedb::vfs::opfs::OpfsVfs>;

// ─── Error mapping ───────────────────────────────────────────────────────────

/// Map `PagedbError` → `LiteError`.
///
/// `PagedbError::NotFound` is **not** mapped here — callers that expect a
/// missing-key result should convert the `Ok(None)` / empty-vec at the call
/// site rather than going through the error path.
///
/// `PagedbError::Quota` is mapped to `LiteError::Storage` for now. A dedicated
/// `LiteError::Quota` variant should be added so that quota pressure is
/// distinguishable at the application layer without string-matching.
impl From<PagedbError> for LiteError {
    fn from(e: PagedbError) -> Self {
        match e {
            PagedbError::Corruption(_) => LiteError::Storage {
                detail: format!("pagedb corruption: {e}"),
            },
            PagedbError::Quota { .. } => LiteError::Storage {
                detail: format!("pagedb quota exceeded: {e}"),
            },
            // `Unsupported` is returned by the OPFS VFS shim when the `opfs`
            // feature is absent, and also by `OpfsVfs::new` if the worker
            // spawn fails. Surface it as `WorkerFailed` so callers can
            // distinguish it from generic I/O failures without string-matching.
            PagedbError::Unsupported => LiteError::WorkerFailed {
                detail: "pagedb OPFS VFS returned Unsupported — ensure the opfs feature is \
                         enabled and the worker URL is correct"
                    .to_string(),
            },
            other => LiteError::Storage {
                detail: other.to_string(),
            },
        }
    }
}

/// Returns `true` when the error is a corruption-class error that should
/// trigger the rename-and-recreate recovery path in `PagedbStorage::open`.
///
/// Only the native `open()` uses this; OPFS has no rename, so it is compiled
/// out on wasm32.
#[cfg(not(target_arch = "wasm32"))]
fn is_corruption(e: &PagedbError) -> bool {
    matches!(e, PagedbError::Corruption(_) | PagedbError::ChecksumFailure)
}

// ─── Key helpers ─────────────────────────────────────────────────────────────

/// Build a composite key: `[namespace_byte, ...key_bytes]`.
///
/// Prepends the namespace byte. The namespace byte is always the first
/// byte; B+ tree order is preserved because all keys within a namespace share
/// the same leading byte and are sorted lexicographically among themselves.
pub(crate) fn prefix_key(ns: Namespace, key: &[u8]) -> Vec<u8> {
    let mut k = Vec::with_capacity(1 + key.len());
    k.push(ns as u8);
    k.extend_from_slice(key);
    k
}

/// Inline-stack composite key for hot read paths. Avoids heap allocation
/// when the prefixed key fits in 64 bytes (typical Lite KV keys are
/// `{ns_byte}{collection}\0{user_key}` ~ a few dozen bytes).
pub(crate) enum KeyBuf {
    Stack { data: [u8; 64], len: usize },
    Heap(Vec<u8>),
}

impl KeyBuf {
    #[inline]
    pub(crate) fn new(ns: Namespace, key: &[u8]) -> Self {
        let total = 1 + key.len();
        if total <= 64 {
            let mut data = [0u8; 64];
            data[0] = ns as u8;
            data[1..total].copy_from_slice(key);
            KeyBuf::Stack { data, len: total }
        } else {
            let mut v = Vec::with_capacity(total);
            v.push(ns as u8);
            v.extend_from_slice(key);
            KeyBuf::Heap(v)
        }
    }

    #[inline]
    pub(crate) fn as_slice(&self) -> &[u8] {
        match self {
            KeyBuf::Stack { data, len } => &data[..*len],
            KeyBuf::Heap(v) => v.as_slice(),
        }
    }
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
/// `RetainPolicy::Disabled` is selected because Lite does not need
/// point-in-time reads; skipping commit-history tracking shaves latency
/// from every `WriteTxn::commit`.
fn lite_open_options() -> OpenOptions {
    OpenOptions::default().with_commit_history_retain(RetainPolicy::Disabled)
}

// ─── PagedbStorage ───────────────────────────────────────────────────────────

/// pagedb-backed KV storage.
///
/// The inner `Db<V>` lives behind `Arc` for cheap cloning across async methods.
/// No outer `Mutex` is needed: `Db::begin_write` already acquires an internal
/// async mutex (single-writer serialization is enforced by pagedb itself).
pub struct PagedbStorage<V: Vfs + Clone> {
    pub(crate) db: Arc<Db<V>>,
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
    /// `encryption` controls how the 32-byte pagedb page-encryption key is
    /// obtained:
    ///
    /// - [`Encryption::Plaintext`] — no encryption; the all-zero key is used.
    ///   Must be chosen consciously.
    /// - [`Encryption::Passphrase`] — derives the key via Argon2id using a
    ///   random 16-byte salt. The salt is persisted in a plaintext sidecar file
    ///   at `<path>.salt` (created on first open, mode 0o600 on Unix) so that
    ///   the same passphrase reproduces the same key on every reopen.
    /// - [`Encryption::RawKey`] — uses the supplied 32-byte key directly; the
    ///   caller is responsible for key management and no sidecar is written.
    ///
    /// On corruption (`PagedbError::Corruption` / `ChecksumFailure`), the
    /// directory is renamed to `{path}.corrupt.{unix_secs}` and a fresh
    /// database is created using the same `encryption`. Data recovery happens
    /// via re-sync from Origin.
    pub async fn open(
        path: impl AsRef<Path>,
        encryption: crate::storage::encryption::Encryption,
    ) -> Result<Self, LiteError> {
        let path = path.as_ref();
        let kek = crate::storage::encryption::resolve_kek_native(&encryption, path)?;
        let realm = RealmId::new([0u8; 16]);

        let vfs = pagedb::vfs::open_default(path).map_err(LiteError::from)?;

        match Db::open(vfs, kek, 4096, realm, lite_open_options()).await {
            Ok(db) => Ok(Self { db: Arc::new(db) }),
            Err(e) if is_corruption(&e) && path.exists() => {
                let timestamp = crate::runtime::now_secs();
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
    /// In-memory storage is volatile (data lives only for the process lifetime),
    /// so no at-rest encryption is applied; the pagedb KEK is all-zero.
    pub async fn open_in_memory() -> Result<Self, LiteError> {
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
    <V as Vfs>::File: Sync,
{
    async fn get(&self, ns: Namespace, key: &[u8]) -> Result<Option<Vec<u8>>, LiteError> {
        let composite = KeyBuf::new(ns, key);
        let txn = self.db.begin_read().await.map_err(LiteError::from)?;
        txn.get(composite.as_slice()).await.map_err(LiteError::from)
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

    fn as_vector_segment_ext(
        &self,
    ) -> Option<&dyn crate::storage::vector_segment_ext::VectorSegmentExt> {
        Some(self)
    }

    fn as_array_segment_ext(
        &self,
    ) -> Option<&dyn crate::storage::array_segment_ext::ArraySegmentExt> {
        Some(self)
    }

    fn as_fts_segment_ext(&self) -> Option<&dyn crate::storage::fts_segment_ext::FtsSegmentExt> {
        Some(self)
    }

    fn as_columnar_segment_ext(
        &self,
    ) -> Option<&dyn crate::storage::columnar_segment_ext::ColumnarSegmentExt> {
        Some(self)
    }

    fn as_graph_segment_ext(
        &self,
    ) -> Option<&dyn crate::storage::graph_segment_ext::GraphSegmentExt> {
        Some(self)
    }

    fn as_spatial_segment_ext(
        &self,
    ) -> Option<&dyn crate::storage::spatial_segment_ext::SpatialSegmentExt> {
        Some(self)
    }
}

// ─── WASM-only OPFS constructor ───────────────────────────────────────────────

/// Validate an OPFS database name before it is used as the VFS root directory.
///
/// The name becomes a single OPFS directory segment, so it must be non-empty,
/// free of path separators and NUL, and must not be a relative-traversal
/// segment. Rejecting here yields a clear error instead of an opaque worker
/// failure (OPFS `getDirectoryHandle` rejects `.`/`..` with a `TypeError`).
#[cfg(target_arch = "wasm32")]
fn validate_opfs_db_name(name: &str) -> Result<(), LiteError> {
    if name.is_empty()
        || name == "."
        || name == ".."
        || name.contains('/')
        || name.contains('\\')
        || name.contains('\0')
    {
        return Err(LiteError::BadRequest {
            detail: format!(
                "invalid OPFS database name {name:?}: must be a non-empty single path \
                 segment without '/', '\\', or NUL and not '.' or '..'"
            ),
        });
    }
    Ok(())
}

#[cfg(target_arch = "wasm32")]
impl PagedbStorage<pagedb::vfs::opfs::OpfsVfs> {
    /// Open or create a persistent database backed by the browser's Origin
    /// Private File System (OPFS).
    ///
    /// `db_name` selects an OPFS sub-directory that scopes every file this
    /// database touches (`main.db`, segments, locks, the salt sidecar). Distinct
    /// names are fully isolated databases in the shared OPFS origin; reopening
    /// with the same name reattaches the same database. It must be a single path
    /// segment — non-empty, no `/`, `\`, or NUL, and not `.`/`..`.
    ///
    /// `worker_url` is the URL of the JS bootstrap script that calls
    /// `run_opfs_worker()` inside a dedicated Web Worker. The embedder
    /// (nodedb-lite-wasm) must export that function and serve the bootstrap
    /// script at a URL the browser can load.
    ///
    /// `encryption` controls how the 32-byte pagedb page-encryption key is
    /// obtained:
    ///
    /// - [`Encryption::Plaintext`] — no encryption; the all-zero key is used.
    ///   Must be chosen consciously; OPFS storage is not encrypted by the
    ///   browser itself, so a passphrase is strongly recommended.
    /// - [`Encryption::Passphrase`] — derives the key via Argon2id. A random
    ///   16-byte salt is persisted in an OPFS sidecar file at
    ///   `__nodedb_salt` (in the same OPFS origin sandbox as the database)
    ///   so the same passphrase reproduces the same key on every reopen.
    /// - [`Encryption::RawKey`] — uses the supplied 32-byte key directly;
    ///   the caller is responsible for key management and no sidecar is
    ///   written.
    ///
    /// # Corruption recovery
    ///
    /// OPFS does not support `std::fs::rename`, so the
    /// rename-and-recreate recovery path used by the native `open()` is not
    /// available here. On a corruption error the call fails immediately with
    /// `LiteError::WorkerFailed`. Recovery is the caller's responsibility
    /// (e.g. delete the OPFS directory and re-sync from Origin).
    pub async fn open_opfs(
        db_name: &str,
        worker_url: &str,
        encryption: crate::storage::encryption::Encryption,
    ) -> Result<Self, LiteError> {
        validate_opfs_db_name(db_name)?;

        let realm = RealmId::new([0u8; 16]);

        let vfs = pagedb::vfs::opfs::OpfsVfs::with_root(worker_url, db_name).map_err(|e| {
            LiteError::WorkerFailed {
                detail: format!("failed to spawn OPFS worker at '{worker_url}': {e}"),
            }
        })?;

        // Resolve the KEK using a clone of the VFS so the original can be
        // forwarded into Db::open below. OpfsVfs::clone is cheap (Arc clone).
        let kek = crate::storage::encryption::resolve_kek_opfs(&encryption, &vfs.clone()).await?;

        let db = Db::open(vfs, kek, 4096, realm, lite_open_options())
            .await
            .map_err(|e| match e {
                pagedb::errors::PagedbError::Corruption(_)
                | pagedb::errors::PagedbError::ChecksumFailure => LiteError::WorkerFailed {
                    detail: format!(
                        "OPFS database is corrupted — delete the OPFS directory and \
                         re-sync from Origin to recover. Original error: {e}"
                    ),
                },
                other => LiteError::from(other),
            })?;

        Ok(Self { db: Arc::new(db) })
    }
}

// ─── StorageEngine impl — WASM ────────────────────────────────────────────────
//
// The trait impl compiles on WASM for any `V: Vfs + Clone` — the `?Send`
// bound is required because WASM is single-threaded. Native code uses the
// `Send + Sync` impl above.

#[cfg(target_arch = "wasm32")]
#[async_trait(?Send)]
impl<V: Vfs + Clone + 'static> StorageEngine for PagedbStorage<V> {
    async fn get(&self, ns: Namespace, key: &[u8]) -> Result<Option<Vec<u8>>, LiteError> {
        let composite = KeyBuf::new(ns, key);
        let txn = self.db.begin_read().await.map_err(LiteError::from)?;
        txn.get(composite.as_slice()).await.map_err(LiteError::from)
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

// ─── VectorSegmentExt impl ────────────────────────────────────────────────────

#[cfg(not(target_arch = "wasm32"))]
#[async_trait]
impl<V: Vfs + Clone + Send + Sync + 'static> crate::storage::vector_segment_ext::VectorSegmentExt
    for PagedbStorage<V>
where
    <V as Vfs>::LockHandle: Sync,
    <V as Vfs>::File: Sync,
{
    async fn write_vector_segment(
        &self,
        collection_name: &str,
        dim: usize,
        vectors: &[Vec<f32>],
        surrogate_ids: &[u64],
    ) -> Result<(), LiteError> {
        use crate::engine::vector::pagedb_backing::build_ndvs_bytes;
        use pagedb::{RealmId, SegmentKind};

        let ndvs = build_ndvs_bytes(dim, vectors, surrogate_ids)?;

        // Chunk the NDVS bytes into page-sized pieces.
        // Page body capacity = 4096 - ENVELOPE_OVERHEAD (40).
        const PAGE_BODY_CAP: usize = 4096 - 40;
        let chunks: Vec<&[u8]> = ndvs.chunks(PAGE_BODY_CAP).collect();

        let realm = RealmId::new([0u8; 16]);
        let segment_name = format!("vec/hnsw/{collection_name}");

        let mut writer = self
            .db
            .create_segment(realm, SegmentKind::Unspecified)
            .await
            .map_err(LiteError::from)?;
        writer
            .append_extent(&chunks)
            .await
            .map_err(LiteError::from)?;
        let meta = writer.seal().await.map_err(LiteError::from)?;

        let mut txn = self.db.begin_write().await.map_err(LiteError::from)?;

        // Replace if already linked (atomic swap).
        let already_exists = txn.link_segment(&segment_name, &meta).await;
        match already_exists {
            Ok(()) => {}
            Err(pagedb::errors::PagedbError::AlreadyLinked) => {
                // Use replace_segment to atomically swap old → new.
                txn.replace_segment(&segment_name, &meta)
                    .await
                    .map_err(LiteError::from)?;
            }
            Err(e) => return Err(LiteError::from(e)),
        }

        txn.commit().await.map(|_| ()).map_err(LiteError::from)
    }

    async fn open_vector_segment(
        &self,
        collection_name: &str,
    ) -> Result<Option<crate::engine::vector::pagedb_backing::PagedbBacking>, LiteError> {
        use crate::engine::vector::pagedb_backing::PagedbBacking;

        let segment_name = format!("vec/hnsw/{collection_name}");
        let txn = self.db.begin_read().await.map_err(LiteError::from)?;

        let reader = match txn.open_segment(&segment_name).await {
            Ok(r) => r,
            Err(pagedb::errors::PagedbError::NotFound) => return Ok(None),
            Err(e) => return Err(LiteError::from(e)),
        };

        let backing = PagedbBacking::open(reader).await?;
        Ok(Some(backing))
    }

    async fn delete_vector_segment(&self, collection_name: &str) -> Result<(), LiteError> {
        let segment_name = format!("vec/hnsw/{collection_name}");
        let mut txn = self.db.begin_write().await.map_err(LiteError::from)?;
        match txn.unlink_segment(&segment_name).await {
            Ok(()) => {}
            Err(pagedb::errors::PagedbError::NotLinked) => return Ok(()), // already gone
            Err(e) => return Err(LiteError::from(e)),
        }
        txn.commit().await.map(|_| ()).map_err(LiteError::from)
    }
}

// ─── ArraySegmentExt impl ─────────────────────────────────────────────────────

#[cfg(not(target_arch = "wasm32"))]
#[async_trait]
impl<V: Vfs + Clone + Send + Sync + 'static> crate::storage::array_segment_ext::ArraySegmentExt
    for PagedbStorage<V>
where
    <V as Vfs>::LockHandle: Sync,
    <V as Vfs>::File: Sync,
{
    async fn write_array_segment(
        &self,
        array_name: &str,
        seg_id: u64,
        bytes: &[u8],
    ) -> Result<(), LiteError> {
        use pagedb::{RealmId, SegmentKind};

        // Prepend an 8-byte little-endian length so reads can recover the
        // exact byte count after pagedb pads the last page to a full page size.
        let byte_len = bytes.len() as u64;
        let mut payload = Vec::with_capacity(8 + bytes.len());
        payload.extend_from_slice(&byte_len.to_le_bytes());
        payload.extend_from_slice(bytes);

        // Chunk the length-prefixed payload into pagedb page-body-sized pieces.
        // Page body capacity = 4096 - ENVELOPE_OVERHEAD (40).
        const PAGE_BODY_CAP: usize = 4096 - 40;
        let chunks: Vec<&[u8]> = payload.chunks(PAGE_BODY_CAP).collect();

        let realm = RealmId::new([0u8; 16]);
        let segment_name = format!("arr/tile/{array_name}/{seg_id}");

        let mut writer = self
            .db
            .create_segment(realm, SegmentKind::Unspecified)
            .await
            .map_err(LiteError::from)?;
        writer
            .append_extent(&chunks)
            .await
            .map_err(LiteError::from)?;
        let meta = writer.seal().await.map_err(LiteError::from)?;

        let mut txn = self.db.begin_write().await.map_err(LiteError::from)?;

        // Atomically replace if a segment already exists under this name.
        let link_result = txn.link_segment(&segment_name, &meta).await;
        match link_result {
            Ok(()) => {}
            Err(pagedb::errors::PagedbError::AlreadyLinked) => {
                txn.replace_segment(&segment_name, &meta)
                    .await
                    .map_err(LiteError::from)?;
            }
            Err(e) => return Err(LiteError::from(e)),
        }

        txn.commit().await.map(|_| ()).map_err(LiteError::from)
    }

    async fn open_array_segment(
        &self,
        array_name: &str,
        seg_id: u64,
    ) -> Result<Option<Box<[u8]>>, LiteError> {
        let segment_name = format!("arr/tile/{array_name}/{seg_id}");
        let txn = self.db.begin_read().await.map_err(LiteError::from)?;

        let reader = match txn.open_segment(&segment_name).await {
            Ok(r) => r,
            Err(pagedb::errors::PagedbError::NotFound) => return Ok(None),
            Err(e) => return Err(LiteError::from(e)),
        };

        // Read all data pages and concatenate into a flat byte buffer.
        let meta_page_count = reader.meta().page_count;
        let index_pages = u64::from(reader.index_page_count());
        let data_page_count = meta_page_count
            .checked_sub(2 + index_pages)
            .ok_or_else(|| LiteError::Storage {
                detail: format!(
                    "array segment page_count={meta_page_count} too small \
                     (index_pages={index_pages})"
                ),
            })?;

        if data_page_count == 0 {
            return Ok(Some(Box::default()));
        }

        let count_u32 = u32::try_from(data_page_count).map_err(|_| LiteError::Storage {
            detail: format!("array segment has too many data pages: {data_page_count}"),
        })?;

        let pages = reader
            .read_range(1, count_u32)
            .await
            .map_err(|e| LiteError::Storage {
                detail: format!("pagedb array segment read_range failed: {e}"),
            })?;

        let total: usize = pages.iter().map(|p| p.len()).sum();
        let mut flat = Vec::with_capacity(total);
        for page in pages {
            flat.extend_from_slice(&page);
        }

        // Strip the 8-byte length prefix written by `write_array_segment`.
        if flat.len() < 8 {
            return Err(LiteError::Storage {
                detail: format!(
                    "array segment {array_name}/{seg_id} too small to contain length prefix: \
                     {} bytes",
                    flat.len()
                ),
            });
        }
        let byte_len = u64::from_le_bytes(flat[..8].try_into().expect("8-byte slice")) as usize;
        let end = 8_usize
            .checked_add(byte_len)
            .ok_or_else(|| LiteError::Storage {
                detail: format!(
                    "array segment {array_name}/{seg_id} length prefix overflows: {byte_len}"
                ),
            })?;
        if end > flat.len() {
            return Err(LiteError::Storage {
                detail: format!(
                    "array segment {array_name}/{seg_id} declared byte_len={byte_len} \
                     exceeds available data ({} bytes after prefix)",
                    flat.len() - 8
                ),
            });
        }
        let segment_bytes = flat[8..end].to_vec();

        Ok(Some(segment_bytes.into_boxed_slice()))
    }

    async fn delete_array_segment(&self, array_name: &str, seg_id: u64) -> Result<(), LiteError> {
        let segment_name = format!("arr/tile/{array_name}/{seg_id}");
        let mut txn = self.db.begin_write().await.map_err(LiteError::from)?;
        match txn.unlink_segment(&segment_name).await {
            Ok(()) => {}
            Err(pagedb::errors::PagedbError::NotLinked) => return Ok(()), // already gone
            Err(e) => return Err(LiteError::from(e)),
        }
        txn.commit().await.map(|_| ()).map_err(LiteError::from)
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
