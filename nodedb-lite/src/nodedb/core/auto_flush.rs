// SPDX-License-Identifier: Apache-2.0

//! `NodeDbLite::start_auto_flush` — durable background flush task.

use std::sync::{Arc, Weak};
use std::time::Duration;

use crate::storage::engine::StorageEngine;

use super::types::NodeDbLite;

impl<S: StorageEngine> NodeDbLite<S> {
    /// Start a background task that calls the global `flush()` every
    /// `interval_ms` milliseconds, bounding the data-loss window uniformly
    /// across all engines (KV buffer, vector id-map, CRDT deltas, CSR graph,
    /// spatial, FTS).
    ///
    /// # Durability contract
    ///
    /// `await`-ing a write operation (e.g. `kv_put`, `vector_insert`) returning
    /// `Ok` does NOT guarantee on-disk durability. Durability is bounded by
    /// `interval_ms`. For guaranteed durability, call `flush()` explicitly after
    /// writes.
    ///
    /// # Usage
    ///
    /// The `open*` constructors already start this task from
    /// [`LiteConfig::auto_flush_ms`](crate::config::LiteConfig::auto_flush_ms),
    /// so calling it is only needed to change the interval afterwards:
    ///
    /// ```ignore
    /// let db = NodeDbLite::open(storage).await?;
    /// db.start_auto_flush(5_000); // slow the flusher down to five seconds
    /// ```
    ///
    /// Each call spawns an additional task rather than replacing the running
    /// one, so opening with `auto_flush_ms: 0` is the way to take full manual
    /// control of when flushes happen.
    ///
    /// # Task lifecycle
    ///
    /// The spawned task holds a `Weak` reference to the database. When the
    /// `Arc<NodeDbLite>` is dropped, the `Weak` upgrade fails and the task
    /// exits cleanly — no task leak.
    ///
    /// # Disabling
    ///
    /// Pass `interval_ms = 0` to skip spawning entirely (auto-flush disabled).
    pub fn start_auto_flush(self: &Arc<Self>, interval_ms: u64) {
        if interval_ms == 0 {
            return;
        }

        let weak: Weak<Self> = Arc::downgrade(self);
        let period = Duration::from_millis(interval_ms);

        crate::runtime::spawn(async move {
            let mut ticker = crate::runtime::interval(period);
            // Consume the first tick so the initial period elapses before the
            // first flush (matches Tokio's immediate-first-tick semantics on
            // native; on WASM the first tick already waits one period).
            ticker.tick().await;

            loop {
                ticker.tick().await;

                let db = match weak.upgrade() {
                    Some(db) => db,
                    None => break,
                };

                if let Err(e) = db.flush().await {
                    tracing::warn!(error = %e, "auto-flush failed");
                }

                // Drop the strong Arc before the next tick so the loop does
                // not keep the database alive between ticks.
                drop(db);
            }
        });
    }
}
