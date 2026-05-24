//! KV collection operations for Lite.
//!
//! Two modes based on `sync_enabled`:
//!
//! - **sync off**: direct KV store via `Namespace::Kv`. No Loro, no CRDT,
//!   no delta tracking. Same performance class as SQLite.
//!
//! - **sync on**: writes go to the KV store (source of truth) AND Loro CRDT
//!   (for delta tracking). Reads always come from the KV store. Sync log
//!   entries are generated for LWW replication to Origin.
//!
//! Writes are buffered in memory and flushed as a single KV transaction
//! on `kv_flush()` or when the buffer exceeds `KV_FLUSH_THRESHOLD`. An
//! in-memory overlay lets reads see uncommitted writes without hitting the
//! KV store.
//!
//! ## Value encoding
//!
//! Every value stored in the KV store is prefixed by an 8-byte little-endian
//! u64 representing the expiry deadline in milliseconds since the Unix epoch.
//! A value of `0` means no expiry. This prefix is transparent to callers —
//! all public methods encode/decode it automatically.

use std::time::{SystemTime, UNIX_EPOCH};

use nodedb_types::Namespace;
use nodedb_types::error::{NodeDbError, NodeDbResult};

use super::super::{LockExt, NodeDbLite};
use crate::storage::engine::{StorageEngine, WriteOp};

/// Prefix for KV collection names in the CRDT namespace.
const KV_CRDT_PREFIX: &str = "_kv_";

/// Flush the write buffer when it reaches this many operations.
const KV_FLUSH_THRESHOLD: usize = 1024;

/// Size of the deadline prefix in bytes (u64 LE).
const DEADLINE_PREFIX_LEN: usize = 8;

/// Build the composite KV key: `{collection}\0{key}`.
fn kv_key(collection: &str, key: &[u8]) -> Vec<u8> {
    let mut k = Vec::with_capacity(collection.len() + 1 + key.len());
    k.extend_from_slice(collection.as_bytes());
    k.push(0);
    k.extend_from_slice(key);
    k
}

/// Extract `(collection, key_bytes)` from a composite KV key.
fn split_kv_key(composite: &[u8]) -> Option<(&str, &[u8])> {
    let sep = composite.iter().position(|&b| b == 0)?;
    let coll = std::str::from_utf8(&composite[..sep]).ok()?;
    let key = &composite[sep + 1..];
    Some((coll, key))
}

/// Encode a value with a deadline prefix.
///
/// `deadline_ms = 0` encodes as "no expiry".
fn encode_value(deadline_ms: u64, value: &[u8]) -> Vec<u8> {
    let mut encoded = Vec::with_capacity(DEADLINE_PREFIX_LEN + value.len());
    encoded.extend_from_slice(&deadline_ms.to_le_bytes());
    encoded.extend_from_slice(value);
    encoded
}

/// Decode a stored value into `(deadline_ms, user_bytes)`.
///
/// Returns `None` if the stored bytes are too short (corrupt entry).
fn decode_value(stored: &[u8]) -> Option<(u64, &[u8])> {
    if stored.len() < DEADLINE_PREFIX_LEN {
        return None;
    }
    let deadline = u64::from_le_bytes(stored[..DEADLINE_PREFIX_LEN].try_into().ok()?);
    Some((deadline, &stored[DEADLINE_PREFIX_LEN..]))
}

/// Return the current time in milliseconds since the Unix epoch.
fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

/// Return `true` if the deadline has passed (key is expired).
///
/// A deadline of `0` means no expiry and is never considered expired.
fn is_expired(deadline_ms: u64) -> bool {
    deadline_ms != 0 && now_ms() >= deadline_ms
}

impl<S: StorageEngine> NodeDbLite<S> {
    /// KV PUT: store a key-value pair with no expiry.
    ///
    /// Buffered in memory — call `kv_flush()` to commit, or let
    /// the auto-flush threshold handle it.
    pub async fn kv_put(&self, collection: &str, key: &str, value: &[u8]) -> NodeDbResult<()> {
        self.kv_put_with_deadline(collection, key, value, 0).await
    }

    /// KV PUT WITH TTL: store a key-value pair that expires after `ttl_ms` ms.
    ///
    /// After `ttl_ms` milliseconds, `kv_get` will return `None` for this key
    /// and lazy-delete it. The deadline survives a database reopen.
    pub async fn kv_put_with_ttl(
        &self,
        collection: &str,
        key: &str,
        value: &[u8],
        ttl_ms: u64,
    ) -> NodeDbResult<()> {
        let deadline = now_ms().saturating_add(ttl_ms);
        self.kv_put_with_deadline(collection, key, value, deadline)
            .await
    }

    /// Internal: write a key with an explicit deadline (0 = no expiry).
    async fn kv_put_with_deadline(
        &self,
        collection: &str,
        key: &str,
        value: &[u8],
        deadline_ms: u64,
    ) -> NodeDbResult<()> {
        let rkey = kv_key(collection, key.as_bytes());
        let encoded = encode_value(deadline_ms, value);

        let mut buf = self.kv_write_buf.lock_or_recover();
        buf.overlay.insert(rkey.clone(), Some(encoded.clone()));
        buf.ops.push(WriteOp::Put {
            ns: Namespace::Kv,
            key: rkey.clone(),
            value: encoded,
        });
        let should_flush = buf.ops.len() >= KV_FLUSH_THRESHOLD;
        self.kv_overlay_len
            .store(buf.overlay.len(), std::sync::atomic::Ordering::Release);
        drop(buf);

        // Invalidate any cached value for this key so subsequent reads go to storage.
        self.kv_cache.lock_or_recover().pop(&rkey);

        if should_flush {
            self.kv_flush_inner().await?;
        }

        // Sync path: also update Loro for delta generation.
        if self.sync_enabled {
            let crdt_collection = format!("{KV_CRDT_PREFIX}{collection}");
            let mut crdt = self.crdt.lock_or_recover();
            let fields: Vec<(&str, loro::LoroValue)> =
                vec![("value", loro::LoroValue::Binary(value.to_vec().into()))];
            crdt.upsert_deferred(&crdt_collection, key, &fields)
                .map_err(NodeDbError::storage)?;
        }

        Ok(())
    }

    /// KV GET: retrieve a value by key.
    ///
    /// Returns `None` for missing or expired keys. Expired keys are lazily
    /// deleted from storage on read.
    ///
    /// Checks the in-memory write buffer first (for uncommitted writes),
    /// then falls through to the KV store.
    pub async fn kv_get(&self, collection: &str, key: &str) -> NodeDbResult<Option<Vec<u8>>> {
        let rkey = kv_key(collection, key.as_bytes());

        // Fast path: when the overlay is empty (common case in read-heavy
        // workloads between flushes), skip the mutex acquire entirely and
        // go straight to storage. The single-writer design + Release stores
        // on overlay mutation make this safe: observing `len == 0` means
        // the writer either hasn't started or has completed; either way the
        // overlay holds no relevant entry.
        if self
            .kv_overlay_len
            .load(std::sync::atomic::Ordering::Acquire)
            > 0
        {
            let buf = self.kv_write_buf.lock_or_recover();
            if let Some(entry) = buf.overlay.get(&rkey) {
                let result = match entry {
                    Some(stored) => decode_value(stored)
                        .and_then(|(deadline, user_bytes)| {
                            if is_expired(deadline) {
                                None
                            } else {
                                Some(user_bytes.to_vec())
                            }
                        })
                        .map(Some)
                        .unwrap_or(None),
                    None => None,
                };
                return Ok(result);
            }
            drop(buf);
        }

        // Cache check: look up the composite key before hitting storage.
        {
            let mut cache = self.kv_cache.lock_or_recover();
            if let Some(encoded) = cache.get(&rkey) {
                match decode_value(encoded) {
                    Some((deadline, user_bytes)) if !is_expired(deadline) => {
                        return Ok(Some(user_bytes.to_vec()));
                    }
                    _ => {
                        // Expired or corrupt — evict and fall through to storage.
                        cache.pop(&rkey);
                    }
                }
            }
        }

        // Fall through to storage.
        let stored = self
            .storage
            .get(Namespace::Kv, &rkey)
            .await
            .map_err(NodeDbError::storage)?;

        match stored {
            None => Ok(None),
            Some(raw) => {
                let decoded = decode_value(&raw);
                match decoded {
                    None => Ok(None),
                    Some((deadline, user_bytes)) => {
                        if is_expired(deadline) {
                            // Lazy expiration: schedule a delete.
                            self.kv_lazy_delete(rkey).await?;
                            Ok(None)
                        } else {
                            let result = user_bytes.to_vec();
                            // Populate cache with the raw encoded bytes before returning.
                            self.kv_cache.lock_or_recover().put(rkey, raw);
                            Ok(Some(result))
                        }
                    }
                }
            }
        }
    }

    /// Internal: queue a lazy delete for an expired key.
    async fn kv_lazy_delete(&self, rkey: Vec<u8>) -> NodeDbResult<()> {
        let mut buf = self.kv_write_buf.lock_or_recover();
        buf.overlay.insert(rkey.clone(), None);
        buf.ops.push(WriteOp::Delete {
            ns: Namespace::Kv,
            key: rkey.clone(),
        });
        let should_flush = buf.ops.len() >= KV_FLUSH_THRESHOLD;
        self.kv_overlay_len
            .store(buf.overlay.len(), std::sync::atomic::Ordering::Release);
        drop(buf);
        // Evict the expired entry so future reads don't serve stale data.
        self.kv_cache.lock_or_recover().pop(&rkey);
        if should_flush {
            self.kv_flush_inner().await?;
        }
        Ok(())
    }

    /// KV DELETE: remove a key.
    pub async fn kv_delete(&self, collection: &str, key: &str) -> NodeDbResult<bool> {
        let rkey = kv_key(collection, key.as_bytes());

        let mut buf = self.kv_write_buf.lock_or_recover();
        buf.overlay.insert(rkey.clone(), None);
        buf.ops.push(WriteOp::Delete {
            ns: Namespace::Kv,
            key: rkey.clone(),
        });
        let should_flush = buf.ops.len() >= KV_FLUSH_THRESHOLD;
        self.kv_overlay_len
            .store(buf.overlay.len(), std::sync::atomic::Ordering::Release);
        drop(buf);

        // Invalidate the cache so subsequent reads don't return stale data.
        self.kv_cache.lock_or_recover().pop(&rkey);

        if should_flush {
            self.kv_flush_inner().await?;
        }

        if self.sync_enabled {
            let crdt_collection = format!("{KV_CRDT_PREFIX}{collection}");
            let mut crdt = self.crdt.lock_or_recover();
            crdt.delete_deferred(&crdt_collection, key)
                .map_err(NodeDbError::storage)?;
        }

        Ok(true)
    }

    /// KV RANGE SCAN: ordered key scan with optional bounds and limit.
    ///
    /// Returns `(key, value)` pairs where `start <= key < end`, ordered by
    /// key in lexicographic byte order. Expired keys are skipped and lazily
    /// deleted.
    ///
    /// - `start = None` means scan from the beginning of the collection.
    /// - `end = None` means scan to the end of the collection.
    /// - `limit = None` means no cap on results.
    ///
    /// Flushes the write buffer before scanning so the KV store reflects all pending
    /// writes.
    pub async fn kv_range_scan(
        &self,
        collection: &str,
        start: Option<&[u8]>,
        end: Option<&[u8]>,
        limit: Option<usize>,
    ) -> NodeDbResult<Vec<(Vec<u8>, Vec<u8>)>> {
        self.kv_flush_inner().await?;

        let col_prefix_end = {
            let mut p = collection.as_bytes().to_vec();
            p.push(0);
            p
        };

        // Build absolute start key (collection\0[user_start]).
        let start_key: Option<Vec<u8>> = Some(match start {
            Some(s) => {
                let mut k = col_prefix_end.clone();
                k.extend_from_slice(s);
                k
            }
            None => col_prefix_end.clone(),
        });

        // Build absolute end key (collection\0[user_end]).
        let end_key: Option<Vec<u8>> = end.map(|e| {
            let mut k = col_prefix_end.clone();
            k.extend_from_slice(e);
            k
        });

        let entries = self
            .storage
            .scan_range_bounded(
                Namespace::Kv,
                start_key.as_deref(),
                end_key.as_deref(),
                limit.map(|l| l + 32), // over-fetch slightly to account for skipped expired keys
            )
            .await
            .map_err(NodeDbError::storage)?;

        let mut results: Vec<(Vec<u8>, Vec<u8>)> =
            Vec::with_capacity(limit.unwrap_or(entries.len()).min(entries.len()));
        let mut expired_keys: Vec<Vec<u8>> = Vec::new();

        for (composite_key, raw_value) in entries {
            if let Some(limit) = limit
                && results.len() >= limit
            {
                break;
            }
            let Some((coll, user_key_bytes)) = split_kv_key(&composite_key) else {
                continue;
            };
            if coll != collection {
                break;
            }
            let Some((deadline, user_bytes)) = decode_value(&raw_value) else {
                continue;
            };
            if is_expired(deadline) {
                expired_keys.push(kv_key(collection, user_key_bytes));
                continue;
            }
            results.push((user_key_bytes.to_vec(), user_bytes.to_vec()));
        }

        // Lazy-delete expired keys discovered during scan.
        if !expired_keys.is_empty() {
            let mut buf = self.kv_write_buf.lock_or_recover();
            for rkey in &expired_keys {
                buf.overlay.insert(rkey.clone(), None);
                buf.ops.push(WriteOp::Delete {
                    ns: Namespace::Kv,
                    key: rkey.clone(),
                });
            }
            let should_flush = buf.ops.len() >= KV_FLUSH_THRESHOLD;
            self.kv_overlay_len
                .store(buf.overlay.len(), std::sync::atomic::Ordering::Release);
            drop(buf);
            // Evict expired keys from the cache.
            let mut cache = self.kv_cache.lock_or_recover();
            for rkey in &expired_keys {
                cache.pop(rkey);
            }
            drop(cache);
            if should_flush {
                self.kv_flush_inner().await?;
            }
        }

        Ok(results)
    }

    /// KV COMPACT EXPIRED: eagerly remove all expired keys in a collection.
    ///
    /// Flushes the write buffer, then scans all keys in the collection and
    /// deletes any whose TTL deadline has passed. Returns the count of keys
    /// removed.
    pub async fn kv_compact_expired(&self, collection: &str) -> NodeDbResult<usize> {
        self.kv_flush_inner().await?;

        let col_prefix = {
            let mut p = collection.as_bytes().to_vec();
            p.push(0);
            p
        };

        let entries = self
            .storage
            .scan_range_bounded(Namespace::Kv, Some(&col_prefix), None, None)
            .await
            .map_err(NodeDbError::storage)?;

        let now = now_ms();
        let mut delete_ops: Vec<WriteOp> = Vec::new();

        for (composite_key, raw_value) in entries {
            let Some((coll, _user_key_bytes)) = split_kv_key(&composite_key) else {
                continue;
            };
            if coll != collection {
                break;
            }
            if let Some((deadline, _)) = decode_value(&raw_value)
                && deadline != 0
                && now >= deadline
            {
                // composite_key is the user-key (namespace byte
                // already stripped by scan_range_bounded). WriteOp
                // re-prepends the namespace byte via make_key internally.
                delete_ops.push(WriteOp::Delete {
                    ns: Namespace::Kv,
                    key: composite_key,
                });
            }
        }

        let count = delete_ops.len();
        if count > 0 {
            self.storage
                .batch_write(&delete_ops)
                .await
                .map_err(NodeDbError::storage)?;
        }

        Ok(count)
    }

    /// KV SCAN: iterate keys in sorted order starting from `cursor`.
    ///
    /// Returns up to `count` key-value pairs where key >= cursor (inclusive).
    /// Pass an empty cursor to start from the beginning of the collection.
    ///
    /// Flushes the write buffer first to ensure the KV store has all data, then
    /// uses the storage's B-tree range scan — O(log N + count).
    pub async fn kv_scan(
        &self,
        collection: &str,
        cursor: &str,
        count: usize,
    ) -> NodeDbResult<Vec<(String, Vec<u8>)>> {
        // Flush pending writes so storage is up to date.
        self.kv_flush_inner().await?;

        let start = kv_key(collection, cursor.as_bytes());
        let entries = self
            .storage
            .scan_range(Namespace::Kv, &start, count)
            .await
            .map_err(NodeDbError::storage)?;

        let mut results = Vec::with_capacity(entries.len());
        for (composite_key, raw_value) in entries {
            let Some((coll, key_bytes)) = split_kv_key(&composite_key) else {
                continue;
            };
            if coll != collection {
                break;
            }
            let Some((deadline, user_bytes)) = decode_value(&raw_value) else {
                continue;
            };
            if is_expired(deadline) {
                continue;
            }
            if let Ok(key_str) = std::str::from_utf8(key_bytes) {
                results.push((key_str.to_string(), user_bytes.to_vec()));
            }
        }

        Ok(results)
    }

    /// Flush buffered KV writes to storage as a single transaction.
    ///
    /// Also flushes deferred CRDT deltas when sync is enabled.
    pub async fn kv_flush(&self) -> NodeDbResult<usize> {
        let count = self.kv_flush_inner().await?;

        if self.sync_enabled {
            let mut crdt = self.crdt.lock_or_recover();
            crdt.flush_deltas().map_err(NodeDbError::storage)?;
        }

        Ok(count)
    }

    /// Internal: flush write buffer to storage without touching CRDT.
    async fn kv_flush_inner(&self) -> NodeDbResult<usize> {
        let mut buf = self.kv_write_buf.lock_or_recover();
        if buf.ops.is_empty() {
            return Ok(0);
        }

        let ops = std::mem::take(&mut buf.ops);
        buf.overlay.clear();
        self.kv_overlay_len
            .store(0, std::sync::atomic::Ordering::Release);
        drop(buf);

        let count = ops.len();
        self.storage
            .batch_write(&ops)
            .await
            .map_err(NodeDbError::storage)?;

        Ok(count)
    }

    /// List all keys in a KV collection.
    pub async fn kv_keys(&self, collection: &str) -> NodeDbResult<Vec<String>> {
        // Flush pending writes first.
        self.kv_flush_inner().await?;

        let prefix = kv_key(collection, b"");
        let entries = self
            .storage
            .scan_range(Namespace::Kv, &prefix, usize::MAX)
            .await
            .map_err(NodeDbError::storage)?;

        let mut keys = Vec::with_capacity(entries.len());
        for (composite_key, raw_value) in entries {
            let Some((coll, key_bytes)) = split_kv_key(&composite_key) else {
                continue;
            };
            if coll != collection {
                break;
            }
            // Skip expired keys.
            if let Some((deadline, _)) = decode_value(&raw_value) {
                if is_expired(deadline) {
                    continue;
                }
            } else {
                continue;
            }
            if let Ok(key_str) = std::str::from_utf8(key_bytes) {
                keys.push(key_str.to_string());
            }
        }
        Ok(keys)
    }

    /// KV INCREMENT: atomic counter increment via CRDT counter semantics.
    ///
    /// Always uses Loro (counters need CRDT merge for correctness).
    pub fn kv_increment(&self, collection: &str, key: &str, delta: i64) -> NodeDbResult<i64> {
        let crdt_collection = format!("{KV_CRDT_PREFIX}{collection}");
        let mut crdt = self.crdt.lock_or_recover();

        let current = match crdt.read(&crdt_collection, key) {
            Some(loro::LoroValue::Map(map)) => {
                if let Some(loro::LoroValue::I64(v)) = map.get("counter") {
                    *v
                } else {
                    0
                }
            }
            _ => 0,
        };

        let new_value = current + delta;
        let fields: Vec<(&str, loro::LoroValue)> =
            vec![("counter", loro::LoroValue::I64(new_value))];
        crdt.upsert(&crdt_collection, key, &fields)
            .map_err(NodeDbError::storage)?;

        Ok(new_value)
    }

    /// Set conflict policy for a KV collection.
    pub fn kv_set_conflict_policy(
        &self,
        collection: &str,
        policy: nodedb_crdt::CollectionPolicy,
    ) -> NodeDbResult<()> {
        let crdt_collection = format!("{KV_CRDT_PREFIX}{collection}");
        let mut crdt = self.crdt.lock_or_recover();
        crdt.set_policy(&crdt_collection, policy);
        Ok(())
    }

    /// Subscribe to a subset of KV keys matching a pattern.
    pub async fn kv_subscribe_shape(
        &self,
        collection: &str,
        key_pattern: &str,
    ) -> NodeDbResult<Vec<String>> {
        let all_keys = self.kv_keys(collection).await?;
        let matched: Vec<String> = all_keys
            .into_iter()
            .filter(|k| glob_matches(key_pattern, k))
            .collect();
        Ok(matched)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::LiteConfig;
    use crate::storage::pagedb_storage::PagedbStorageMem;

    async fn open_db() -> NodeDbLite<PagedbStorageMem> {
        let storage = PagedbStorageMem::open_in_memory()
            .await
            .expect("open in-memory storage");
        NodeDbLite::open(storage, 1).await.expect("open NodeDbLite")
    }

    async fn open_db_with_cache_capacity(cap: usize) -> NodeDbLite<PagedbStorageMem> {
        let storage = PagedbStorageMem::open_in_memory()
            .await
            .expect("open in-memory storage");
        let config = LiteConfig {
            kv_cache_capacity: cap,
            ..LiteConfig::default()
        };
        NodeDbLite::open_with_config(storage, 1, config)
            .await
            .expect("open NodeDbLite with config")
    }

    /// Two consecutive gets on the same key: both return the same value.
    /// The second read is served from the in-process cache.
    #[tokio::test]
    async fn cache_hits_on_repeated_get() {
        let db = open_db().await;
        db.kv_put("col", "key", b"hello").await.unwrap();
        // Prime the cache.
        db.kv_flush().await.unwrap();
        let v1 = db.kv_get("col", "key").await.unwrap();
        assert_eq!(v1.as_deref(), Some(b"hello".as_ref()));
        // Second get — served from cache.
        let v2 = db.kv_get("col", "key").await.unwrap();
        assert_eq!(v2.as_deref(), Some(b"hello".as_ref()));
        // Verify the cache actually holds the entry.
        assert_eq!(db.kv_cache.lock_or_recover().len(), 1);
    }

    /// After a put-get-put sequence the second get must return the new value,
    /// not the stale cached one.
    #[tokio::test]
    async fn cache_invalidated_on_put() {
        let db = open_db().await;
        db.kv_put("col", "key", b"v1").await.unwrap();
        db.kv_flush().await.unwrap();
        let _ = db.kv_get("col", "key").await.unwrap(); // populate cache
        db.kv_put("col", "key", b"v2").await.unwrap();
        db.kv_flush().await.unwrap();
        let v = db.kv_get("col", "key").await.unwrap();
        assert_eq!(v.as_deref(), Some(b"v2".as_ref()));
    }

    /// After a put-get-delete sequence a subsequent get must return None.
    #[tokio::test]
    async fn cache_invalidated_on_delete() {
        let db = open_db().await;
        db.kv_put("col", "key", b"v").await.unwrap();
        db.kv_flush().await.unwrap();
        let _ = db.kv_get("col", "key").await.unwrap(); // populate cache
        db.kv_delete("col", "key").await.unwrap();
        db.kv_flush().await.unwrap();
        let v = db.kv_get("col", "key").await.unwrap();
        assert!(v.is_none(), "deleted key must not be returned from cache");
    }

    /// A key written with a very short TTL must not be served from cache after expiry.
    #[tokio::test]
    async fn expired_cached_value_evicted() {
        let db = open_db().await;
        // 1 ms TTL — expired almost immediately.
        db.kv_put_with_ttl("col", "key", b"v", 1).await.unwrap();
        db.kv_flush().await.unwrap();
        let _ = db.kv_get("col", "key").await.unwrap(); // may or may not cache
        // Sleep long enough that the deadline has definitely passed.
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        let v = db.kv_get("col", "key").await.unwrap();
        assert!(v.is_none(), "expired key must return None");
        // Cache must not hold the evicted entry.
        assert_eq!(
            db.kv_cache.lock_or_recover().len(),
            0,
            "expired entry must be evicted from cache"
        );
    }

    /// The cache must not grow beyond the configured capacity.
    #[tokio::test]
    async fn cache_capacity_eviction() {
        const CAP: usize = 5;
        let db = open_db_with_cache_capacity(CAP).await;
        let col = "cap_test";

        // Write N+10 keys and flush so they land in storage.
        for i in 0..(CAP + 10) {
            db.kv_put(col, &i.to_string(), b"x").await.unwrap();
        }
        db.kv_flush().await.unwrap();

        // Read all keys — each miss populates the cache.
        for i in 0..(CAP + 10) {
            let _ = db.kv_get(col, &i.to_string()).await.unwrap();
        }

        let cache_len = db.kv_cache.lock_or_recover().len();
        assert!(
            cache_len <= CAP,
            "cache must not exceed capacity {CAP}, got {cache_len}"
        );
    }
}

/// Simple glob matching for shape subscriptions.
fn glob_matches(pattern: &str, input: &str) -> bool {
    let pat = pattern.as_bytes();
    let inp = input.as_bytes();
    let mut pi = 0;
    let mut ii = 0;
    let mut star_pi = usize::MAX;
    let mut star_ii = 0;

    while ii < inp.len() {
        if pi < pat.len() && (pat[pi] == b'?' || pat[pi] == inp[ii]) {
            pi += 1;
            ii += 1;
        } else if pi < pat.len() && pat[pi] == b'*' {
            star_pi = pi;
            star_ii = ii;
            pi += 1;
        } else if star_pi != usize::MAX {
            pi = star_pi + 1;
            star_ii += 1;
            ii = star_ii;
        } else {
            return false;
        }
    }

    while pi < pat.len() && pat[pi] == b'*' {
        pi += 1;
    }

    pi == pat.len()
}
