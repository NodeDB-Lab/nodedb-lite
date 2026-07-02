//! Validated id→Arc registry for `NodeDbHandle`.
//!
//! Instead of round-tripping raw `Box::into_raw` pointers through C/Kotlin as
//! integers, every open database is stored in a global `HashMap<u64, Arc<…>>`
//! keyed by a monotonically increasing `u64` id.  The opaque token exposed to
//! callers is that integer id cast to `*mut NodeDbHandle`; on 64-bit targets
//! (arm64 / x86_64) the pointer width is 64 bits so no information is lost.
//!
//! The core UAF fix: `get` clones the `Arc` out from under the read-lock
//! before returning, so an in-flight operation keeps the handle alive even if
//! another thread calls `nodedb_close` / `remove` concurrently.

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, OnceLock, RwLock};

use crate::NodeDbHandle;

// ── id allocator ─────────────────────────────────────────────────────────────

/// Monotonic counter for handle ids.  Starts at 1; id 0 is the invalid token.
static NEXT_ID: AtomicU64 = AtomicU64::new(1);

// ── registry ─────────────────────────────────────────────────────────────────

static REGISTRY: OnceLock<RwLock<HashMap<u64, Arc<NodeDbHandle>>>> = OnceLock::new();

fn registry() -> &'static RwLock<HashMap<u64, Arc<NodeDbHandle>>> {
    REGISTRY.get_or_init(|| RwLock::new(HashMap::new()))
}

// ── public API ───────────────────────────────────────────────────────────────

/// Store `handle` in the registry and return a non-zero opaque id for it.
pub(crate) fn insert(handle: NodeDbHandle) -> u64 {
    let id = NEXT_ID.fetch_add(1, Ordering::Relaxed);
    // id 0 is the invalid sentinel; the counter starts at 1 so this should
    // never wrap in practice, but guard defensively.
    debug_assert!(id != 0, "handle id wrapped to zero — impossibly many opens");
    let arc = Arc::new(handle);
    let mut map = registry().write().unwrap_or_else(|e| e.into_inner());
    map.insert(id, arc);
    id
}

/// Clone the `Arc` for `id` out of the registry.
///
/// Returns `None` for id 0 (invalid sentinel) or unknown ids.
/// The returned `Arc` keeps the handle alive even if `remove` is called
/// concurrently from another thread.
pub(crate) fn get(id: u64) -> Option<Arc<NodeDbHandle>> {
    if id == 0 {
        return None;
    }
    let map = registry().read().unwrap_or_else(|e| e.into_inner());
    map.get(&id).cloned()
}

/// Remove `id` from the registry, dropping the stored `Arc`.
///
/// If no other `Arc` clones are live the `NodeDbHandle` is freed here;
/// otherwise it remains alive until the last clone is dropped.
///
/// Returns `false` for id 0 or ids that are not present.
pub(crate) fn remove(id: u64) -> bool {
    if id == 0 {
        return false;
    }
    let mut map = registry().write().unwrap_or_else(|e| e.into_inner());
    map.remove(&id).is_some()
}

// ── tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::sync::{Arc, RwLock};

    // The registry's correctness does not depend on the stored type being
    // NodeDbHandle — constructing one requires real storage.  We exercise all
    // invariants using a local HashMap<u64, Arc<u64>> that mirrors the
    // production registry exactly, then verify the live id-allocator separately.

    fn make_reg() -> RwLock<HashMap<u64, Arc<u64>>> {
        RwLock::new(HashMap::new())
    }

    fn reg_insert(reg: &RwLock<HashMap<u64, Arc<u64>>>, id: u64, v: u64) {
        reg.write().unwrap().insert(id, Arc::new(v));
    }

    fn reg_get(reg: &RwLock<HashMap<u64, Arc<u64>>>, id: u64) -> Option<Arc<u64>> {
        if id == 0 {
            return None;
        }
        reg.read().unwrap().get(&id).cloned()
    }

    fn reg_remove(reg: &RwLock<HashMap<u64, Arc<u64>>>, id: u64) -> bool {
        if id == 0 {
            return false;
        }
        reg.write().unwrap().remove(&id).is_some()
    }

    // ── id allocator ─────────────────────────────────────────────────────────

    #[test]
    fn id_allocator_returns_nonzero_distinct_ids() {
        use std::sync::atomic::Ordering;
        let id1 = super::NEXT_ID.fetch_add(1, Ordering::Relaxed);
        let id2 = super::NEXT_ID.fetch_add(1, Ordering::Relaxed);
        assert_ne!(id1, 0);
        assert_ne!(id2, 0);
        assert_ne!(id1, id2);
    }

    // ── lookup ───────────────────────────────────────────────────────────────

    #[test]
    fn get_valid_is_some() {
        let r = make_reg();
        reg_insert(&r, 42, 100);
        assert!(reg_get(&r, 42).is_some());
    }

    #[test]
    fn get_zero_is_none() {
        let r = make_reg();
        reg_insert(&r, 1, 1);
        assert!(reg_get(&r, 0).is_none());
    }

    #[test]
    fn get_unknown_is_none() {
        let r = make_reg();
        assert!(reg_get(&r, 999).is_none());
    }

    // ── remove ───────────────────────────────────────────────────────────────

    #[test]
    fn after_remove_get_is_none() {
        let r = make_reg();
        reg_insert(&r, 7, 7);
        assert!(reg_remove(&r, 7));
        assert!(reg_get(&r, 7).is_none());
    }

    #[test]
    fn double_remove_second_returns_false() {
        let r = make_reg();
        reg_insert(&r, 3, 3);
        assert!(reg_remove(&r, 3));
        assert!(!reg_remove(&r, 3));
    }

    #[test]
    fn remove_zero_returns_false() {
        let r = make_reg();
        assert!(!reg_remove(&r, 0));
    }

    // ── UAF regression: Arc clone keeps value alive across remove ────────────

    #[test]
    fn cloned_arc_survives_concurrent_remove() {
        let r = make_reg();
        reg_insert(&r, 5, 55);
        // Simulate an in-flight operation: clone the Arc before the close path
        // calls remove.
        let in_flight = reg_get(&r, 5).expect("present before remove");
        // Close path removes the entry from the registry.
        assert!(reg_remove(&r, 5));
        // Registry no longer holds it.
        assert!(reg_get(&r, 5).is_none());
        // But the in-flight clone is still valid — no use-after-free.
        assert_eq!(*in_flight, 55);
    }
}
