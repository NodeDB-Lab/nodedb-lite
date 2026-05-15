//! Generic primitives shared by every per-engine outbound sync queue.
//!
//! Each `*Outbound` (columnar, vector, fts, spatial, timeseries) shares the
//! same shape: a mutex-protected `Vec<Pending…>` plus a monotonic batch-ID
//! counter for ACK correlation. Before this module was extracted that pattern
//! was copy-pasted across five files (~1.1k lines of near-identical
//! drain/ack/requeue plumbing). It now lives here exactly once.
//!
//! Engine-specific code only writes:
//!   * the `Pending…` struct (a row payload + a `batch_id` field)
//!   * the public-facing wrapper that calls `enqueue` / `drain` / `retain` /
//!     `requeue` and exposes engine-specific `acknowledge_*` semantics
//!
//! The mutex guards are deliberately recovered with `let Ok(mut g) = lock`:
//! a poisoned outbound queue means another thread already crashed mid-sync,
//! and silently dropping further sync work is preferable to propagating the
//! poison and tearing down the whole runtime — the writes are already durable
//! in the local engine.

use std::sync::Mutex;
use std::sync::atomic::{AtomicU64, Ordering};

/// Monotonic batch-ID generator shared between sibling queues (e.g. the
/// insert and delete queues of a single engine) so every in-flight ACK ID is
/// globally unique inside one outbound.
#[derive(Debug)]
pub struct BatchIdGen {
    next: AtomicU64,
}

impl BatchIdGen {
    pub const fn new() -> Self {
        Self {
            next: AtomicU64::new(1),
        }
    }

    /// Allocate the next batch ID. Always non-zero and strictly increasing
    /// within the lifetime of this generator.
    pub fn next(&self) -> u64 {
        self.next.fetch_add(1, Ordering::Relaxed)
    }
}

impl Default for BatchIdGen {
    fn default() -> Self {
        Self::new()
    }
}

/// Mutex-wrapped pending queue. `T` is the engine-specific pending entry
/// (e.g. `PendingVectorInsert`).
#[derive(Debug)]
pub struct PendingQueue<T> {
    items: Mutex<Vec<T>>,
}

impl<T> Default for PendingQueue<T> {
    fn default() -> Self {
        Self::new()
    }
}

impl<T> PendingQueue<T> {
    pub const fn new() -> Self {
        Self {
            items: Mutex::new(Vec::new()),
        }
    }

    /// Append a new pending entry to the tail of the queue.
    pub fn push(&self, item: T) {
        if let Ok(mut g) = self.items.lock() {
            g.push(item);
        }
    }

    /// Take every pending entry and reset the queue. Callers retain the
    /// returned vec until they receive ACKs.
    pub fn drain(&self) -> Vec<T> {
        match self.items.lock() {
            Ok(mut g) => std::mem::take(&mut *g),
            Err(_) => Vec::new(),
        }
    }

    /// Re-queue an entry at the head so it is retried before fresh writes on
    /// the next drain cycle. Used on transport reject.
    pub fn requeue(&self, item: T) {
        if let Ok(mut g) = self.items.lock() {
            g.insert(0, item);
        }
    }

    /// Drop entries matching `predicate` (used by ACK paths that key off
    /// `batch_id` or collection name).
    pub fn retain<F>(&self, predicate: F)
    where
        F: FnMut(&T) -> bool,
    {
        if let Ok(mut g) = self.items.lock() {
            g.retain(predicate);
        }
    }

    /// Run `f` on the first entry that matches `predicate`, returning its
    /// result. Used by engines (columnar, timeseries) that coalesce rows for
    /// the same collection into one open batch.
    ///
    /// Returns `None` if no entry matches (or the lock is poisoned).
    pub fn with_first_mut<P, F, R>(&self, mut predicate: P, f: F) -> Option<R>
    where
        P: FnMut(&T) -> bool,
        F: FnOnce(&mut T) -> R,
    {
        let mut g = self.items.lock().ok()?;
        let entry = g.iter_mut().find(|t| predicate(t))?;
        Some(f(entry))
    }

    /// Number of pending entries (best-effort; returns 0 on lock poison).
    pub fn len(&self) -> usize {
        self.items.lock().map(|g| g.len()).unwrap_or(0)
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn batch_id_gen_is_monotonic_and_nonzero() {
        let g = BatchIdGen::new();
        let a = g.next();
        let b = g.next();
        let c = g.next();
        assert!(a > 0 && b > a && c > b);
    }

    #[test]
    fn push_drain_round_trip() {
        let q = PendingQueue::<u32>::new();
        q.push(1);
        q.push(2);
        let out = q.drain();
        assert_eq!(out, vec![1, 2]);
        assert!(q.drain().is_empty());
    }

    #[test]
    fn requeue_inserts_at_head() {
        let q = PendingQueue::<u32>::new();
        q.push(1);
        q.requeue(99);
        assert_eq!(q.drain(), vec![99, 1]);
    }

    #[test]
    fn retain_removes_matches() {
        let q = PendingQueue::<u32>::new();
        for i in 0..5 {
            q.push(i);
        }
        q.retain(|x| *x % 2 == 0);
        assert_eq!(q.drain(), vec![0, 2, 4]);
    }

    #[test]
    fn with_first_mut_targets_match_only() {
        let q = PendingQueue::<(u32, u32)>::new();
        q.push((1, 10));
        q.push((2, 20));
        let touched = q.with_first_mut(|x| x.0 == 2, |x| x.1 += 5);
        assert_eq!(touched, Some(()));
        assert_eq!(q.drain(), vec![(1, 10), (2, 25)]);
    }
}
