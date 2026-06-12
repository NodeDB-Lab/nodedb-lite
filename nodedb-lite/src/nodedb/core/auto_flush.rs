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
    /// Call this once after wrapping the database in `Arc`:
    ///
    /// ```ignore
    /// let db = Arc::new(NodeDbLite::open(storage, peer_id).await?);
    /// db.start_auto_flush(1_000); // flush every second
    /// ```
    ///
    /// Direct library users (not using the FFI or WASM wrappers) must call
    /// this themselves — the embedded `open*` constructors return `Self`, not
    /// `Arc<Self>`, so the task cannot be spawned internally.
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
