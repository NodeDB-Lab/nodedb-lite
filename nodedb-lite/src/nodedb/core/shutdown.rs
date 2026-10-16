// SPDX-License-Identifier: Apache-2.0

//! `NodeDbLite::shutdown` — stop every background task this database started.

use crate::storage::engine::StorageEngine;

use super::types::NodeDbLite;

impl<S: StorageEngine> NodeDbLite<S> {
    /// Stop every background task this database started, and wait for them.
    ///
    /// Call this before dropping the async runtime the database was opened on.
    /// Auto-flush, auto-compact and the sync loop each outlive the call that
    /// started them; a runtime torn down while one is still polling takes the
    /// task down mid-poll, which on a native runtime has crashed the process.
    ///
    /// Call it also before reopening the same store in-process. A background
    /// flush in flight holds the database, and so the store, open past the
    /// drop of the last handle, and the reopen fails with `AlreadyOpen`.
    ///
    /// Each task is signalled first and leaves its loop at a point it chose.
    /// One that ignores the signal past
    /// [`TASK_STOP_TIMEOUT`](crate::tasks::TASK_STOP_TIMEOUT) is aborted.
    /// Admitted search DDL completes before this returns.
    ///
    /// Idempotent, and safe on a database that started no tasks. It does not
    /// flush: what is in memory at shutdown is still in memory, so call
    /// [`flush`](Self::flush) first when the data must be durable.
    pub async fn shutdown(&self) {
        self.tasks.shutdown().await;
        let _permit = self.fts_state.admit_exclusive().await;
    }
}
