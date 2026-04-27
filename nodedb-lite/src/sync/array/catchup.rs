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
use crate::storage::engine::StorageEngineSync;

/// Storage key prefix for per-array last-seen HLC entries.
const LAST_SEEN_PREFIX: &str = "array.last_seen_hlc:";

/// Storage key for a named array's last-seen HLC.
fn last_seen_key(array: &str) -> Vec<u8> {
    format!("{LAST_SEEN_PREFIX}{array}").into_bytes()
}

/// Tracks the last successfully applied inbound HLC per array.
///
/// Used by the transport layer (future phases) to populate catchup-request
/// messages after reconnect or when Origin's log GC horizon advances past the
/// local log.
///
/// Thread-safe via an internal [`Mutex`].
pub struct CatchupTracker<S: StorageEngineSync> {
    storage: Arc<S>,
    state: Mutex<HashMap<String, Hlc>>,
}

impl<S: StorageEngineSync> CatchupTracker<S> {
    /// Load all persisted `last_seen_hlc` entries from `Namespace::Meta`.
    ///
    /// Scans keys starting with `b"array.last_seen_hlc:"` and parses each
    /// 18-byte value as an [`Hlc`]. Returns an empty tracker if no entries
    /// are stored yet (first run or after a full wipe).
    pub fn load(storage: Arc<S>) -> Result<Self, LiteError> {
        let prefix = LAST_SEEN_PREFIX.as_bytes();
        let pairs = storage.scan_range_sync(Namespace::Meta, prefix, usize::MAX)?;

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

        Ok(Self {
            storage,
            state: Mutex::new(state),
        })
    }

    /// Record that `hlc` has been successfully applied for `array`.
    ///
    /// Updates both the in-memory map and the durable storage entry.
    /// Only persists if `hlc` is strictly greater than the current last-seen
    /// value (monotonic advancement).
    pub fn record(&self, array: &str, hlc: Hlc) -> Result<(), LiteError> {
        let mut state = self.state.lock().map_err(|_| LiteError::LockPoisoned)?;
        let current = state.get(array).copied().unwrap_or(Hlc::ZERO);
        if hlc <= current {
            return Ok(());
        }
        state.insert(array.to_owned(), hlc);
        drop(state);
        self.storage
            .put_sync(Namespace::Meta, &last_seen_key(array), &hlc.to_bytes())
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
}

// ─── Tests ────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::redb_storage::RedbStorage;
    use nodedb_array::sync::replica_id::ReplicaId;

    fn rep() -> ReplicaId {
        ReplicaId::new(1)
    }

    fn hlc(ms: u64) -> Hlc {
        Hlc::new(ms, 0, rep()).unwrap()
    }

    fn make_tracker() -> CatchupTracker<RedbStorage> {
        let storage = Arc::new(RedbStorage::open_in_memory().unwrap());
        CatchupTracker::load(storage).unwrap()
    }

    #[test]
    fn load_returns_zero_when_empty() {
        let tracker = make_tracker();
        assert_eq!(tracker.last_seen("any_array"), Hlc::ZERO);
    }

    #[test]
    fn record_persists_across_load() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("catchup_test.redb");

        let target_hlc = hlc(42_000);

        {
            let storage = Arc::new(RedbStorage::open(&path).unwrap());
            let tracker = CatchupTracker::load(Arc::clone(&storage)).unwrap();
            tracker.record("arr", target_hlc).unwrap();
            assert_eq!(tracker.last_seen("arr"), target_hlc);
        }

        {
            let storage = Arc::new(RedbStorage::open(&path).unwrap());
            let tracker = CatchupTracker::load(storage).unwrap();
            assert_eq!(
                tracker.last_seen("arr"),
                target_hlc,
                "last_seen must survive storage restart"
            );
        }
    }

    #[test]
    fn record_is_monotonic_only() {
        let tracker = make_tracker();
        let h1 = hlc(100);
        let h2 = hlc(200);

        tracker.record("x", h2).unwrap();
        // Recording a smaller HLC must not regress the stored value.
        tracker.record("x", h1).unwrap();
        assert_eq!(tracker.last_seen("x"), h2);
    }

    #[test]
    fn build_request_carries_last_seen() {
        let tracker = make_tracker();
        let h = hlc(9_999);
        tracker.record("feed", h).unwrap();

        let req = tracker.build_request("feed");
        assert_eq!(req.array, "feed");
        assert_eq!(req.from_hlc_bytes, h.to_bytes());
    }

    #[test]
    fn build_request_zero_when_unknown() {
        let tracker = make_tracker();
        let req = tracker.build_request("unknown");
        assert_eq!(req.from_hlc_bytes, Hlc::ZERO.to_bytes());
    }
}
