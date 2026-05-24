//! Catchup-request helper: tracks the last-seen inbound HLC per array and
//! builds [`ArrayCatchupRequestMsg`] messages for the transport layer.
//!
//! # Persistence
//!
//! Each per-array `last_seen_hlc` is persisted under `Namespace::Meta` with
//! key `b"array.last_seen_hlc:{name}"`. [`CatchupTracker::load`] restores all
//! entries on startup so the first catchup request after a restart asks only
//! for ops the device has never seen.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use nodedb_array::sync::hlc::Hlc;
use nodedb_types::Namespace;
use nodedb_types::sync::wire::array::ArrayCatchupRequestMsg;

use crate::error::LiteError;
use crate::storage::engine::StorageEngine;

/// Storage key prefix for per-array last-seen HLC entries.
const LAST_SEEN_PREFIX: &str = "array.last_seen_hlc:";

/// Storage key prefix for per-array catchup-needed flags.
const CATCHUP_NEEDED_PREFIX: &str = "array.catchup_needed:";

/// Storage key for a named array's last-seen HLC.
fn last_seen_key(array: &str) -> Vec<u8> {
    format!("{LAST_SEEN_PREFIX}{array}").into_bytes()
}

/// Storage key for a named array's catchup-needed flag.
fn catchup_needed_key(array: &str) -> Vec<u8> {
    format!("{CATCHUP_NEEDED_PREFIX}{array}").into_bytes()
}

/// Tracks the last successfully applied inbound HLC per array.
///
/// Used by the transport layer to populate catchup-request messages after
/// reconnect or when Origin's log GC horizon advances past the local log.
///
/// Thread-safe via an internal [`Mutex`].
pub struct CatchupTracker<S: StorageEngine> {
    storage: Arc<S>,
    state: Mutex<HashMap<String, Hlc>>,
    /// Arrays that need a full catchup on next connect.
    /// Set when Origin sends `ArrayRejectMsg::RetentionFloor`.
    catchup_needed: Mutex<std::collections::HashSet<String>>,
}

impl<S: StorageEngine> CatchupTracker<S> {
    /// Load all persisted `last_seen_hlc` entries from `Namespace::Meta`.
    ///
    /// Scans keys starting with `b"array.last_seen_hlc:"` and parses each
    /// 18-byte value as an [`Hlc`]. Returns an empty tracker if no entries
    /// are stored yet (first run or after a full wipe).
    pub async fn load(storage: Arc<S>) -> Result<Self, LiteError> {
        let prefix = LAST_SEEN_PREFIX.as_bytes();
        let pairs = storage
            .scan_range(Namespace::Meta, prefix, usize::MAX)
            .await?;

        let mut state = HashMap::new();
        for (key, value) in pairs {
            if !key.starts_with(prefix) {
                break;
            }
            let name_bytes = &key[prefix.len()..];
            let name = std::str::from_utf8(name_bytes).map_err(|e| LiteError::Storage {
                detail: format!("catchup_tracker: non-UTF8 array name in storage: {e}"),
            })?;

            let hlc_arr: [u8; 18] = value.try_into().map_err(|v: Vec<u8>| LiteError::Storage {
                detail: format!(
                    "catchup_tracker: last_seen_hlc wrong length ({}) for '{name}'",
                    v.len()
                ),
            })?;
            state.insert(name.to_owned(), Hlc::from_bytes(&hlc_arr));
        }

        // Load catchup-needed flags.
        let needed_prefix = CATCHUP_NEEDED_PREFIX.as_bytes();
        let needed_pairs = storage
            .scan_range(Namespace::Meta, needed_prefix, usize::MAX)
            .await?;
        let mut catchup_needed = std::collections::HashSet::new();
        for (key, _) in needed_pairs {
            if !key.starts_with(needed_prefix) {
                break;
            }
            let name_bytes = &key[needed_prefix.len()..];
            if let Ok(name) = std::str::from_utf8(name_bytes) {
                catchup_needed.insert(name.to_owned());
            }
        }

        Ok(Self {
            storage,
            state: Mutex::new(state),
            catchup_needed: Mutex::new(catchup_needed),
        })
    }

    /// Record that `hlc` has been successfully applied for `array`.
    ///
    /// Updates both the in-memory map and the durable storage entry.
    /// Only persists if `hlc` is strictly greater than the current last-seen
    /// value (monotonic advancement).
    #[allow(clippy::await_holding_lock)]
    pub async fn record(&self, array: &str, hlc: Hlc) -> Result<(), LiteError> {
        let mut state = self.state.lock().map_err(|_| LiteError::LockPoisoned)?;
        let current = state.get(array).copied().unwrap_or(Hlc::ZERO);
        if hlc <= current {
            return Ok(());
        }
        state.insert(array.to_owned(), hlc);
        drop(state);
        self.storage
            .put(Namespace::Meta, &last_seen_key(array), &hlc.to_bytes())
            .await
    }

    /// Return the last-seen HLC for `array`, or [`Hlc::ZERO`] if unknown.
    ///
    /// [`Hlc::ZERO`] in a catchup request tells Origin to replay all history
    /// for the array from the beginning.
    pub fn last_seen(&self, array: &str) -> Hlc {
        self.state
            .lock()
            .ok()
            .and_then(|s| s.get(array).copied())
            .unwrap_or(Hlc::ZERO)
    }

    /// Build an [`ArrayCatchupRequestMsg`] for `array` using the stored
    /// last-seen HLC as the starting point.
    pub fn build_request(&self, array: &str) -> ArrayCatchupRequestMsg {
        ArrayCatchupRequestMsg {
            array: array.to_owned(),
            from_hlc_bytes: self.last_seen(array).to_bytes(),
        }
    }

    /// Returns `true` if `array` should request a full catch-up on connect.
    ///
    /// True when:
    /// - No `last_seen_hlc` is recorded (first connect), OR
    /// - `record_reject_retention_floor` was called (Origin GC advanced past our log).
    pub fn should_request_catchup(&self, array: &str, _current_local_hlc: Hlc) -> bool {
        let needs_catchup = self
            .catchup_needed
            .lock()
            .ok()
            .map(|s| s.contains(array))
            .unwrap_or(false);
        if needs_catchup {
            return true;
        }
        // First connect: no last-seen HLC recorded.
        let state = self.state.lock().ok();
        state.map(|s| !s.contains_key(array)).unwrap_or(false)
    }

    /// Mark `array` as needing a full catch-up on the next connect.
    ///
    /// Called when Origin sends `ArrayRejectMsg::RetentionFloor`.
    /// Persisted under `Namespace::Meta` `"array.catchup_needed:{array}"`.
    pub async fn record_reject_retention_floor(&self, array: &str) -> Result<(), LiteError> {
        if let Ok(mut needed) = self.catchup_needed.lock() {
            needed.insert(array.to_owned());
        }
        // Persist flag (value = 1 byte sentinel).
        self.storage
            .put(Namespace::Meta, &catchup_needed_key(array), &[1u8])
            .await
    }

    /// Clear the catchup-needed flag for `array` after a successful catch-up.
    ///
    /// Called after the snapshot stream has been fully applied.
    pub async fn clear_catchup_needed(&self, array: &str) -> Result<(), LiteError> {
        if let Ok(mut needed) = self.catchup_needed.lock() {
            needed.remove(array);
        }
        // Remove the persisted flag.
        self.storage
            .delete(Namespace::Meta, &catchup_needed_key(array))
            .await
    }

    /// Return all arrays that are marked as needing catch-up.
    pub fn arrays_needing_catchup(&self) -> Vec<String> {
        self.catchup_needed
            .lock()
            .ok()
            .map(|s| s.iter().cloned().collect())
            .unwrap_or_default()
    }
}

// ─── Tests ────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::pagedb_storage::{PagedbStorageDefault, PagedbStorageMem};
    use nodedb_array::sync::replica_id::ReplicaId;

    fn rep() -> ReplicaId {
        ReplicaId::new(1)
    }

    fn hlc(ms: u64) -> Hlc {
        Hlc::new(ms, 0, rep()).unwrap()
    }

    async fn make_tracker() -> CatchupTracker<PagedbStorageMem> {
        let storage = Arc::new(PagedbStorageMem::open_in_memory().await.unwrap());
        CatchupTracker::load(storage).await.unwrap()
    }

    #[tokio::test]
    async fn load_returns_zero_when_empty() {
        let tracker = make_tracker().await;
        assert_eq!(tracker.last_seen("any_array"), Hlc::ZERO);
    }

    #[tokio::test]
    async fn record_persists_across_load() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("catchup_test.pagedb");

        let target_hlc = hlc(42_000);

        {
            let storage = Arc::new(PagedbStorageDefault::open(&path).await.unwrap());
            let tracker = CatchupTracker::load(Arc::clone(&storage)).await.unwrap();
            tracker.record("arr", target_hlc).await.unwrap();
            assert_eq!(tracker.last_seen("arr"), target_hlc);
        }

        {
            let storage = Arc::new(PagedbStorageDefault::open(&path).await.unwrap());
            let tracker = CatchupTracker::load(storage).await.unwrap();
            assert_eq!(
                tracker.last_seen("arr"),
                target_hlc,
                "last_seen must survive storage restart"
            );
        }
    }

    #[tokio::test]
    async fn record_is_monotonic_only() {
        let tracker = make_tracker().await;
        let h1 = hlc(100);
        let h2 = hlc(200);

        tracker.record("x", h2).await.unwrap();
        // Recording a smaller HLC must not regress the stored value.
        tracker.record("x", h1).await.unwrap();
        assert_eq!(tracker.last_seen("x"), h2);
    }

    #[tokio::test]
    async fn build_request_carries_last_seen() {
        let tracker = make_tracker().await;
        let h = hlc(9_999);
        tracker.record("feed", h).await.unwrap();

        let req = tracker.build_request("feed");
        assert_eq!(req.array, "feed");
        assert_eq!(req.from_hlc_bytes, h.to_bytes());
    }

    #[tokio::test]
    async fn build_request_zero_when_unknown() {
        let tracker = make_tracker().await;
        let req = tracker.build_request("unknown");
        assert_eq!(req.from_hlc_bytes, Hlc::ZERO.to_bytes());
    }
}
