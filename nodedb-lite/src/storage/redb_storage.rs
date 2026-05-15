//! redb-backed `StorageEngine` implementation.
//!
//! Pure Rust, ACID, no C dependencies. Works on native, WASM, and WASI.
//! Uses a single redb table with `(namespace, key)` → `value` mapping.
//!
//! redb is a B-tree KV store designed for embedded use — exactly what
//! Lite needs. No SQL parsing, no WAL journal mode, no spawn_blocking.

use std::path::Path;
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use redb::{Database, TableDefinition};

use crate::error::LiteError;
use crate::storage::engine::{StorageEngine, WriteOp};
use nodedb_types::Namespace;

/// Table definition: `(namespace_u8, key_bytes)` → `value_bytes`.
///
/// redb requires a fixed table name at compile time. We use a single table
/// and encode the namespace as the first byte of the key.
const TABLE: TableDefinition<&[u8], &[u8]> = TableDefinition::new("kv");

/// redb-backed KV storage.
///
/// The inner `Database` lives behind `Arc<Mutex<_>>` so async methods can
/// `spawn_blocking` with a cheap clone of the handle on native targets and
/// call the same helpers synchronously on WASM (which has no blocking pool).
pub struct RedbStorage {
    db: Arc<Mutex<Database>>,
}

impl RedbStorage {
    /// Open or create a database at the given path.
    ///
    /// If the database file is corrupted (redb returns a `DatabaseError`),
    /// the corrupted file is renamed to `{path}.corrupt.{timestamp}` and
    /// a fresh database is created. This ensures the application can always
    /// start — data recovery happens via full re-sync from Origin.
    pub fn open(path: impl AsRef<Path>) -> Result<Self, LiteError> {
        let path = path.as_ref();
        match Database::create(path) {
            Ok(db) => Ok(Self {
                db: Arc::new(Mutex::new(db)),
            }),
            Err(e) => {
                if path.exists() {
                    let timestamp = std::time::SystemTime::now()
                        .duration_since(std::time::UNIX_EPOCH)
                        .unwrap_or_default()
                        .as_secs();
                    let corrupt_path = path.with_extension(format!("corrupt.{timestamp}"));

                    tracing::error!(
                        path = %path.display(),
                        corrupt_backup = %corrupt_path.display(),
                        error = %e,
                        "redb database corrupted — renaming to backup and creating fresh database. \
                         A full re-sync from Origin is needed to recover data."
                    );

                    if let Err(rename_err) = std::fs::rename(path, &corrupt_path) {
                        tracing::error!(
                            error = %rename_err,
                            "failed to rename corrupted database file"
                        );
                        return Err(LiteError::Storage {
                            detail: format!(
                                "redb corrupted and rename failed: open={e}, rename={rename_err}"
                            ),
                        });
                    }

                    let db = Database::create(path).map_err(|e2| LiteError::Storage {
                        detail: format!(
                            "redb corrupted, backup saved to {}, fresh create failed: {e2}",
                            corrupt_path.display()
                        ),
                    })?;
                    Ok(Self {
                        db: Arc::new(Mutex::new(db)),
                    })
                } else {
                    Err(LiteError::Storage {
                        detail: format!("redb open failed: {e}"),
                    })
                }
            }
        }
    }

    /// Create an in-memory database (for testing and WASM without persistence).
    pub fn open_in_memory() -> Result<Self, LiteError> {
        let backend = redb::backends::InMemoryBackend::new();
        let db = Database::builder()
            .create_with_backend(backend)
            .map_err(|e| LiteError::Storage {
                detail: format!("redb in-memory create failed: {e}"),
            })?;
        Ok(Self {
            db: Arc::new(Mutex::new(db)),
        })
    }

    /// Wrap a pre-built `Database` (e.g., one created with a custom `StorageBackend`).
    ///
    /// Used by the WASM crate to pass in an OPFS-backed database.
    pub fn from_database(db: Database) -> Self {
        Self {
            db: Arc::new(Mutex::new(db)),
        }
    }

    /// Total user-data bytes resident in the database (sum of key + value
    /// lengths across all tables). Excludes B-tree metadata and free pages.
    ///
    /// Works for both file-backed and in-memory databases. Used by the
    /// benchmark suite to compute compression ratios — both the array
    /// engine and the document engine push their encoded payloads through
    /// the same redb mutator, so this number is the apples-to-apples
    /// "bytes the engine actually wrote" measurement.
    pub fn db_size_bytes(&self) -> Result<u64, LiteError> {
        let db = self.db.lock().map_err(|_| LiteError::LockPoisoned)?;
        // `stats()` lives on `WriteTransaction` in redb 2.x. We never commit
        // — the transaction is dropped (rolled back) immediately after the
        // read.
        let txn = db.begin_write().map_err(|e| LiteError::Storage {
            detail: format!("size write txn failed: {e}"),
        })?;
        let stats = txn.stats().map_err(|e| LiteError::Storage {
            detail: format!("stats failed: {e}"),
        })?;
        drop(txn);
        Ok(stats.stored_bytes())
    }

    /// Build the composite key: `[namespace_u8, ...key_bytes]`.
    fn make_key(ns: Namespace, key: &[u8]) -> Vec<u8> {
        let mut k = Vec::with_capacity(1 + key.len());
        k.push(ns as u8);
        k.extend_from_slice(key);
        k
    }

    /// Extract the user key from a composite key (strip the namespace byte).
    fn strip_ns(composite: &[u8]) -> &[u8] {
        if composite.len() > 1 {
            &composite[1..]
        } else {
            &[]
        }
    }

    // ─── Sync helpers (the only place that touches redb) ──────────────────
    //
    // Take `&Mutex<Database>` so async methods can clone the outer `Arc` and
    // move it into a `spawn_blocking` closure without referencing `self`.

    fn get_inner(
        db: &Mutex<Database>,
        ns: Namespace,
        key: &[u8],
    ) -> Result<Option<Vec<u8>>, LiteError> {
        let composite = Self::make_key(ns, key);
        let db = db.lock().map_err(|_| LiteError::LockPoisoned)?;

        let txn = db.begin_read().map_err(|e| LiteError::Storage {
            detail: format!("read txn failed: {e}"),
        })?;
        let table = match txn.open_table(TABLE) {
            Ok(t) => t,
            Err(redb::TableError::TableDoesNotExist(_)) => return Ok(None),
            Err(e) => {
                return Err(LiteError::Storage {
                    detail: format!("open table failed: {e}"),
                });
            }
        };

        match table.get(composite.as_slice()) {
            Ok(Some(val)) => Ok(Some(val.value().to_vec())),
            Ok(None) => Ok(None),
            Err(e) => Err(LiteError::Storage {
                detail: format!("get failed: {e}"),
            }),
        }
    }

    fn put_inner(
        db: &Mutex<Database>,
        ns: Namespace,
        key: &[u8],
        value: &[u8],
    ) -> Result<(), LiteError> {
        let composite = Self::make_key(ns, key);
        let db = db.lock().map_err(|_| LiteError::LockPoisoned)?;

        let txn = db.begin_write().map_err(|e| LiteError::Storage {
            detail: format!("write txn failed: {e}"),
        })?;
        {
            let mut table = txn.open_table(TABLE).map_err(|e| LiteError::Storage {
                detail: format!("open table failed: {e}"),
            })?;
            table
                .insert(composite.as_slice(), value)
                .map_err(|e| LiteError::Storage {
                    detail: format!("insert failed: {e}"),
                })?;
        }
        txn.commit().map_err(|e| LiteError::Storage {
            detail: format!("commit failed: {e}"),
        })?;
        Ok(())
    }

    fn delete_inner(db: &Mutex<Database>, ns: Namespace, key: &[u8]) -> Result<(), LiteError> {
        let composite = Self::make_key(ns, key);
        let db = db.lock().map_err(|_| LiteError::LockPoisoned)?;

        let txn = db.begin_write().map_err(|e| LiteError::Storage {
            detail: format!("write txn failed: {e}"),
        })?;
        {
            let mut table = txn.open_table(TABLE).map_err(|e| LiteError::Storage {
                detail: format!("open table failed: {e}"),
            })?;
            table
                .remove(composite.as_slice())
                .map_err(|e| LiteError::Storage {
                    detail: format!("remove failed: {e}"),
                })?;
        }
        txn.commit().map_err(|e| LiteError::Storage {
            detail: format!("commit failed: {e}"),
        })?;
        Ok(())
    }

    fn batch_write_inner(db: &Mutex<Database>, ops: &[WriteOp]) -> Result<(), LiteError> {
        if ops.is_empty() {
            return Ok(());
        }

        let db = db.lock().map_err(|_| LiteError::LockPoisoned)?;

        let txn = db.begin_write().map_err(|e| LiteError::Storage {
            detail: format!("write txn failed: {e}"),
        })?;
        {
            let mut table = txn.open_table(TABLE).map_err(|e| LiteError::Storage {
                detail: format!("open table failed: {e}"),
            })?;

            for op in ops {
                match op {
                    WriteOp::Put { ns, key, value } => {
                        let composite = Self::make_key(*ns, key);
                        table
                            .insert(composite.as_slice(), value.as_slice())
                            .map_err(|e| LiteError::Storage {
                                detail: format!("batch insert failed: {e}"),
                            })?;
                    }
                    WriteOp::Delete { ns, key } => {
                        let composite = Self::make_key(*ns, key);
                        table
                            .remove(composite.as_slice())
                            .map_err(|e| LiteError::Storage {
                                detail: format!("batch remove failed: {e}"),
                            })?;
                    }
                }
            }
        }
        txn.commit().map_err(|e| LiteError::Storage {
            detail: format!("batch commit failed: {e}"),
        })?;
        Ok(())
    }

    fn scan_prefix_inner(
        db: &Mutex<Database>,
        ns: Namespace,
        prefix: &[u8],
    ) -> Result<Vec<super::engine::KvPair>, LiteError> {
        let ns_byte = ns as u8;
        let mut start_key = Vec::with_capacity(1 + prefix.len());
        start_key.push(ns_byte);
        start_key.extend_from_slice(prefix);

        let db = db.lock().map_err(|_| LiteError::LockPoisoned)?;

        let txn = db.begin_read().map_err(|e| LiteError::Storage {
            detail: format!("read txn failed: {e}"),
        })?;
        let table = match txn.open_table(TABLE) {
            Ok(t) => t,
            Err(redb::TableError::TableDoesNotExist(_)) => return Ok(Vec::new()),
            Err(e) => {
                return Err(LiteError::Storage {
                    detail: format!("open table failed: {e}"),
                });
            }
        };

        let mut results = Vec::new();

        let range = table
            .range(start_key.as_slice()..)
            .map_err(|e| LiteError::Storage {
                detail: format!("range scan failed: {e}"),
            })?;

        for entry in range {
            let entry = entry.map_err(|e| LiteError::Storage {
                detail: format!("range iteration failed: {e}"),
            })?;
            let k = entry.0.value();

            if k[0] != ns_byte {
                break;
            }

            let user_key = Self::strip_ns(k);

            if !prefix.is_empty() && !user_key.starts_with(prefix) {
                break;
            }

            results.push((user_key.to_vec(), entry.1.value().to_vec()));
        }

        Ok(results)
    }

    fn scan_range_bounded_inner(
        db: &Mutex<Database>,
        ns: Namespace,
        start: Option<&[u8]>,
        end: Option<&[u8]>,
        limit: Option<usize>,
    ) -> Result<Vec<super::engine::KvPair>, LiteError> {
        let ns_byte = ns as u8;

        let start_key: Vec<u8> = match start {
            Some(s) => {
                let mut k = Vec::with_capacity(1 + s.len());
                k.push(ns_byte);
                k.extend_from_slice(s);
                k
            }
            None => vec![ns_byte],
        };

        // Build end key. None means the next namespace byte (open end for this ns).
        let end_key: Vec<u8> = match end {
            Some(e) => {
                let mut k = Vec::with_capacity(1 + e.len());
                k.push(ns_byte);
                k.extend_from_slice(e);
                k
            }
            None => vec![ns_byte + 1],
        };

        let cap = limit.unwrap_or(usize::MAX).min(1024);
        let db = db.lock().map_err(|_| LiteError::LockPoisoned)?;
        let txn = db.begin_read().map_err(|e| LiteError::Storage {
            detail: format!("read txn failed: {e}"),
        })?;
        let table = match txn.open_table(TABLE) {
            Ok(t) => t,
            Err(redb::TableError::TableDoesNotExist(_)) => return Ok(Vec::new()),
            Err(e) => {
                return Err(LiteError::Storage {
                    detail: format!("open table failed: {e}"),
                });
            }
        };

        let range = table
            .range(start_key.as_slice()..end_key.as_slice())
            .map_err(|e| LiteError::Storage {
                detail: format!("bounded range scan failed: {e}"),
            })?;

        let effective_limit = limit.unwrap_or(usize::MAX);
        let mut results = Vec::with_capacity(cap);
        for entry in range {
            if results.len() >= effective_limit {
                break;
            }
            let entry = entry.map_err(|e| LiteError::Storage {
                detail: format!("range iteration failed: {e}"),
            })?;
            let k = entry.0.value();
            if k[0] != ns_byte {
                break;
            }
            results.push((Self::strip_ns(k).to_vec(), entry.1.value().to_vec()));
        }

        Ok(results)
    }

    fn count_inner(db: &Mutex<Database>, ns: Namespace) -> Result<u64, LiteError> {
        let ns_byte = ns as u8;
        let start = vec![ns_byte];
        let end = vec![ns_byte + 1];

        let db = db.lock().map_err(|_| LiteError::LockPoisoned)?;

        let txn = db.begin_read().map_err(|e| LiteError::Storage {
            detail: format!("read txn failed: {e}"),
        })?;
        let table = match txn.open_table(TABLE) {
            Ok(t) => t,
            Err(redb::TableError::TableDoesNotExist(_)) => return Ok(0),
            Err(e) => {
                return Err(LiteError::Storage {
                    detail: format!("open table failed: {e}"),
                });
            }
        };

        let range =
            table
                .range(start.as_slice()..end.as_slice())
                .map_err(|e| LiteError::Storage {
                    detail: format!("count range failed: {e}"),
                })?;

        let mut count = 0u64;
        for entry in range {
            let _ = entry.map_err(|e| LiteError::Storage {
                detail: format!("count iteration failed: {e}"),
            })?;
            count += 1;
        }

        Ok(count)
    }

    fn scan_range_inner(
        db: &Mutex<Database>,
        ns: Namespace,
        start: &[u8],
        limit: usize,
    ) -> Result<Vec<super::engine::KvPair>, LiteError> {
        let ns_byte = ns as u8;
        let mut start_key = Vec::with_capacity(1 + start.len());
        start_key.push(ns_byte);
        start_key.extend_from_slice(start);

        let db = db.lock().map_err(|_| LiteError::LockPoisoned)?;
        let txn = db.begin_read().map_err(|e| LiteError::Storage {
            detail: format!("read txn failed: {e}"),
        })?;
        let table = match txn.open_table(TABLE) {
            Ok(t) => t,
            Err(redb::TableError::TableDoesNotExist(_)) => return Ok(Vec::new()),
            Err(e) => {
                return Err(LiteError::Storage {
                    detail: format!("open table failed: {e}"),
                });
            }
        };

        // Cap pre-allocation at 1024 entries to avoid unbounded buffer growth
        // when callers pass a very large `limit` for an open-ended scan.
        let mut results = Vec::with_capacity(limit.min(1024));
        let range = table
            .range(start_key.as_slice()..)
            .map_err(|e| LiteError::Storage {
                detail: format!("range scan failed: {e}"),
            })?;

        for entry in range {
            if results.len() >= limit {
                break;
            }
            let entry = entry.map_err(|e| LiteError::Storage {
                detail: format!("range iteration failed: {e}"),
            })?;
            let k = entry.0.value();
            if k[0] != ns_byte {
                break;
            }
            results.push((Self::strip_ns(k).to_vec(), entry.1.value().to_vec()));
        }

        Ok(results)
    }
}

// ─── Native: dispatch every async method through `spawn_blocking` ────────
//
// `spawn_blocking` is mandatory. The redb calls are synchronous and can take
// non-trivial time (especially `begin_write` + `commit`). Calling them on a
// `current_thread` Tokio runtime would block the only worker; calling them
// on `multi_thread` would still hog a worker thread. Off-loading to the
// blocking pool keeps the async runtime responsive on every flavor.

#[cfg(not(target_arch = "wasm32"))]
fn join_err(e: tokio::task::JoinError) -> LiteError {
    LiteError::JoinError {
        detail: e.to_string(),
    }
}

#[cfg(not(target_arch = "wasm32"))]
#[async_trait]
impl StorageEngine for RedbStorage {
    async fn get(&self, ns: Namespace, key: &[u8]) -> Result<Option<Vec<u8>>, LiteError> {
        let db = Arc::clone(&self.db);
        let key = key.to_vec();
        tokio::task::spawn_blocking(move || Self::get_inner(&db, ns, &key))
            .await
            .map_err(join_err)?
    }

    async fn put(&self, ns: Namespace, key: &[u8], value: &[u8]) -> Result<(), LiteError> {
        let db = Arc::clone(&self.db);
        let key = key.to_vec();
        let value = value.to_vec();
        tokio::task::spawn_blocking(move || Self::put_inner(&db, ns, &key, &value))
            .await
            .map_err(join_err)?
    }

    async fn delete(&self, ns: Namespace, key: &[u8]) -> Result<(), LiteError> {
        let db = Arc::clone(&self.db);
        let key = key.to_vec();
        tokio::task::spawn_blocking(move || Self::delete_inner(&db, ns, &key))
            .await
            .map_err(join_err)?
    }

    async fn scan_prefix(
        &self,
        ns: Namespace,
        prefix: &[u8],
    ) -> Result<Vec<super::engine::KvPair>, LiteError> {
        let db = Arc::clone(&self.db);
        let prefix = prefix.to_vec();
        tokio::task::spawn_blocking(move || Self::scan_prefix_inner(&db, ns, &prefix))
            .await
            .map_err(join_err)?
    }

    async fn batch_write(&self, ops: &[WriteOp]) -> Result<(), LiteError> {
        let db = Arc::clone(&self.db);
        let ops = ops.to_vec();
        tokio::task::spawn_blocking(move || Self::batch_write_inner(&db, &ops))
            .await
            .map_err(join_err)?
    }

    async fn count(&self, ns: Namespace) -> Result<u64, LiteError> {
        let db = Arc::clone(&self.db);
        tokio::task::spawn_blocking(move || Self::count_inner(&db, ns))
            .await
            .map_err(join_err)?
    }
}

// ─── WASM: no blocking pool, call helpers directly ───────────────────────

#[cfg(target_arch = "wasm32")]
#[async_trait(?Send)]
impl StorageEngine for RedbStorage {
    async fn get(&self, ns: Namespace, key: &[u8]) -> Result<Option<Vec<u8>>, LiteError> {
        Self::get_inner(&self.db, ns, key)
    }

    async fn put(&self, ns: Namespace, key: &[u8], value: &[u8]) -> Result<(), LiteError> {
        Self::put_inner(&self.db, ns, key, value)
    }

    async fn delete(&self, ns: Namespace, key: &[u8]) -> Result<(), LiteError> {
        Self::delete_inner(&self.db, ns, key)
    }

    async fn scan_prefix(
        &self,
        ns: Namespace,
        prefix: &[u8],
    ) -> Result<Vec<super::engine::KvPair>, LiteError> {
        Self::scan_prefix_inner(&self.db, ns, prefix)
    }

    async fn batch_write(&self, ops: &[WriteOp]) -> Result<(), LiteError> {
        Self::batch_write_inner(&self.db, ops)
    }

    async fn count(&self, ns: Namespace) -> Result<u64, LiteError> {
        Self::count_inner(&self.db, ns)
    }
}

impl crate::storage::engine::StorageEngineSync for RedbStorage {
    fn batch_write_sync(&self, ops: &[WriteOp]) -> Result<(), LiteError> {
        Self::batch_write_inner(&self.db, ops)
    }

    fn get_sync(&self, ns: Namespace, key: &[u8]) -> Result<Option<Vec<u8>>, LiteError> {
        Self::get_inner(&self.db, ns, key)
    }

    fn put_sync(&self, ns: Namespace, key: &[u8], value: &[u8]) -> Result<(), LiteError> {
        Self::put_inner(&self.db, ns, key, value)
    }

    fn delete_sync(&self, ns: Namespace, key: &[u8]) -> Result<(), LiteError> {
        Self::delete_inner(&self.db, ns, key)
    }

    fn scan_range_sync(
        &self,
        ns: Namespace,
        start: &[u8],
        limit: usize,
    ) -> Result<Vec<super::engine::KvPair>, LiteError> {
        Self::scan_range_inner(&self.db, ns, start, limit)
    }

    fn scan_range_bounded_sync(
        &self,
        ns: Namespace,
        start: Option<&[u8]>,
        end: Option<&[u8]>,
        limit: Option<usize>,
    ) -> Result<Vec<super::engine::KvPair>, LiteError> {
        Self::scan_range_bounded_inner(&self.db, ns, start, end, limit)
    }

    fn count_sync(&self, ns: Namespace) -> Result<u64, LiteError> {
        Self::count_inner(&self.db, ns)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc as StdArc;

    fn make_storage() -> RedbStorage {
        RedbStorage::open_in_memory().unwrap()
    }

    #[tokio::test]
    async fn put_get_roundtrip() {
        let s = make_storage();
        s.put(Namespace::Vector, b"v1", b"hello").await.unwrap();
        let val = s.get(Namespace::Vector, b"v1").await.unwrap();
        assert_eq!(val.as_deref(), Some(b"hello".as_slice()));
    }

    #[tokio::test]
    async fn get_missing_returns_none() {
        let s = make_storage();
        let val = s.get(Namespace::Vector, b"nope").await.unwrap();
        assert!(val.is_none());
    }

    #[tokio::test]
    async fn put_overwrites() {
        let s = make_storage();
        s.put(Namespace::Graph, b"k", b"first").await.unwrap();
        s.put(Namespace::Graph, b"k", b"second").await.unwrap();
        let val = s.get(Namespace::Graph, b"k").await.unwrap();
        assert_eq!(val.as_deref(), Some(b"second".as_slice()));
    }

    #[tokio::test]
    async fn delete_removes_key() {
        let s = make_storage();
        s.put(Namespace::Crdt, b"k", b"val").await.unwrap();
        s.delete(Namespace::Crdt, b"k").await.unwrap();
        assert!(s.get(Namespace::Crdt, b"k").await.unwrap().is_none());
    }

    #[tokio::test]
    async fn delete_nonexistent_is_noop() {
        let s = make_storage();
        s.delete(Namespace::Meta, b"ghost").await.unwrap();
    }

    #[tokio::test]
    async fn namespaces_are_isolated() {
        let s = make_storage();
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
        let s = make_storage();
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
        let s = make_storage();
        s.put(Namespace::Meta, b"a", b"1").await.unwrap();
        s.put(Namespace::Meta, b"b", b"2").await.unwrap();
        s.put(Namespace::Vector, b"c", b"3").await.unwrap();

        let results = s.scan_prefix(Namespace::Meta, b"").await.unwrap();
        assert_eq!(results.len(), 2);
    }

    #[tokio::test]
    async fn scan_prefix_no_match() {
        let s = make_storage();
        s.put(Namespace::Graph, b"edge:1", b"data").await.unwrap();
        let results = s.scan_prefix(Namespace::Graph, b"node:").await.unwrap();
        assert!(results.is_empty());
    }

    #[tokio::test]
    async fn batch_write_atomic() {
        let s = make_storage();
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

    #[tokio::test]
    async fn batch_write_empty_is_noop() {
        let s = make_storage();
        s.batch_write(&[]).await.unwrap();
    }

    #[tokio::test]
    async fn count_entries() {
        let s = make_storage();
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
        let s = make_storage();
        let large = vec![0xABu8; 1_000_000];
        s.put(Namespace::Vector, b"hnsw:layer0", &large)
            .await
            .unwrap();
        let val = s.get(Namespace::Vector, b"hnsw:layer0").await.unwrap();
        assert_eq!(val.unwrap().len(), 1_000_000);
    }

    #[tokio::test]
    async fn binary_keys_work() {
        let s = make_storage();
        let key = vec![0x00, 0x01, 0xFF, 0xFE];
        s.put(Namespace::LoroState, &key, b"binary_key_val")
            .await
            .unwrap();
        let val = s.get(Namespace::LoroState, &key).await.unwrap();
        assert_eq!(val.as_deref(), Some(b"binary_key_val".as_slice()));
    }

    #[tokio::test]
    async fn open_file_based() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("test.redb");

        {
            let s = RedbStorage::open(&path).unwrap();
            s.put(Namespace::Meta, b"key", b"persistent").await.unwrap();
        }
        {
            let s = RedbStorage::open(&path).unwrap();
            let val = s.get(Namespace::Meta, b"key").await.unwrap();
            assert_eq!(val.as_deref(), Some(b"persistent".as_slice()));
        }
    }

    /// 100 concurrent `get` calls from a `current_thread` Tokio runtime must
    /// complete within 1s. Before the spawn_blocking refactor these would
    /// serialize on the redb mutex while holding the only worker thread,
    /// making throughput collapse and the test stall on slower machines.
    #[test]
    fn concurrent_gets_on_current_thread_runtime() {
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();

        rt.block_on(async {
            let s = StdArc::new(make_storage());
            // Pre-populate.
            for i in 0..100u32 {
                let key = format!("k{i}");
                s.put(Namespace::Vector, key.as_bytes(), b"v")
                    .await
                    .unwrap();
            }

            let start = std::time::Instant::now();
            let mut handles = Vec::with_capacity(100);
            for i in 0..100u32 {
                let s = StdArc::clone(&s);
                handles.push(tokio::spawn(async move {
                    let key = format!("k{i}");
                    s.get(Namespace::Vector, key.as_bytes()).await.unwrap()
                }));
            }
            for h in handles {
                let val = h.await.unwrap();
                assert_eq!(val.as_deref(), Some(b"v".as_slice()));
            }
            let elapsed = start.elapsed();
            assert!(
                elapsed < std::time::Duration::from_secs(1),
                "100 concurrent gets took {elapsed:?}, expected < 1s"
            );
        });
    }
}
