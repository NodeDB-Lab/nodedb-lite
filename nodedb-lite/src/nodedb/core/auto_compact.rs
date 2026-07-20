// SPDX-License-Identifier: Apache-2.0

//! `NodeDbLite::start_auto_compact` — background storage-compaction task.

use std::sync::{Arc, Weak};
use std::time::Duration;

use crate::storage::engine::StorageEngine;

use super::types::NodeDbLite;

impl<S: StorageEngine> NodeDbLite<S> {
    /// Start a background task that calls the global `compact()` every
    /// `interval_ms` milliseconds, reclaiming dead pages and truncating the
    /// backing file so on-disk growth stays bounded.
    ///
    /// # When to use
    ///
    /// Compaction is opt-in and unnecessary for most workloads. Enable it when
    /// writing one commit per entry, where the pagedb deferred-free list would
    /// otherwise accumulate and the file would grow without bound. For engines
    /// with nothing to compact (in-memory / test stores) the call is a cheap
    /// no-op every tick.
    ///
    /// # Cost vs. auto-flush
    ///
    /// This is heavier than `start_auto_flush`: compaction repacks the B+ trees
    /// and truncates the file, and it no-ops while a reader pins the reclaimable
    /// page range. Use a much larger interval than the flush interval — minutes,
    /// not seconds.
    ///
    /// # Usage
    ///
    /// Call this once after wrapping the database in `Arc`:
    ///
    /// ```ignore
    /// let db = Arc::new(NodeDbLite::open(storage, peer_id).await?);
    /// db.start_auto_compact(300_000); // compact every 5 minutes
    /// ```
    ///
    /// Direct library users (not using the FFI or WASM wrappers) must call this
    /// themselves — the embedded `open*` constructors return `Self`, not
    /// `Arc<Self>`, so the task cannot be spawned internally.
    ///
    /// # Task lifecycle
    ///
    /// The spawned task holds a `Weak` reference to the database. When the
    /// `Arc<NodeDbLite>` is dropped, the `Weak` upgrade fails and the task exits
    /// cleanly — no task leak.
    ///
    /// # Disabling
    ///
    /// Pass `interval_ms = 0` to skip spawning entirely (auto-compaction
    /// disabled — the default). Compaction can still be triggered manually via
    /// `compact()`.
    pub fn start_auto_compact(self: &Arc<Self>, interval_ms: u64) {
        if interval_ms == 0 {
            return;
        }

        let weak: Weak<Self> = Arc::downgrade(self);
        let period = Duration::from_millis(interval_ms);

        crate::runtime::spawn(async move {
            let mut ticker = crate::runtime::interval(period);
            // Consume the first tick so the initial period elapses before the
            // first compaction (matches Tokio's immediate-first-tick semantics
            // on native; on WASM the first tick already waits one period).
            ticker.tick().await;

            loop {
                ticker.tick().await;

                let db = match weak.upgrade() {
                    Some(db) => db,
                    None => break,
                };

                match db.compact().await {
                    Ok(outcome) => {
                        if outcome.reclaimed_pages > 0 || outcome.file_bytes_freed > 0 {
                            tracing::debug!(
                                reclaimed_pages = outcome.reclaimed_pages,
                                segments_repacked = outcome.segments_repacked,
                                file_bytes_freed = outcome.file_bytes_freed,
                                "auto-compact reclaimed space"
                            );
                        }
                    }
                    Err(e) => {
                        tracing::warn!(error = %e, "auto-compact failed");
                    }
                }

                // Drop the strong Arc before the next tick so the loop does not
                // keep the database alive between ticks.
                drop(db);
            }
        });
    }
}
