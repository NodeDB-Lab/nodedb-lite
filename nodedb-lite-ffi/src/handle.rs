// SPDX-License-Identifier: Apache-2.0

//! The opaque database handle and the temp directory backing `:memory:`.

use std::sync::Arc;

use nodedb_lite::{NodeDbLite, PagedbStorageDefault};

/// Minimal RAII temp-directory wrapper used for the `:memory:` path.
///
/// Deleted on drop. No external crate dependency required.
pub(crate) struct OwnedTempDir(pub(crate) std::path::PathBuf);

impl Drop for OwnedTempDir {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

impl OwnedTempDir {
    /// Create a unique temporary directory under `std::env::temp_dir()`.
    pub(crate) fn new() -> Option<Self> {
        let mut path = std::env::temp_dir();
        // Use process-id + a monotonic counter for uniqueness.
        let pid = std::process::id();
        static COUNTER: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
        let n = COUNTER.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        path.push(format!("nodedb-lite-ffi-{pid}-{n}"));
        if std::fs::create_dir_all(&path).is_ok() {
            Some(Self(path))
        } else {
            None
        }
    }
}

/// Opaque handle to a NodeDB-Lite database.
///
/// Created by `nodedb_open`, freed by `nodedb_close`.
///
/// The `Arc<NodeDbLite>` is what the constructor returned, so the background
/// flush and compaction tasks live exactly as long as this handle does.
///
/// `_tmpdir` is `Some` when the database was opened with the `:memory:` path.
/// The directory is deleted when the handle is dropped.
pub struct NodeDbHandle {
    pub(crate) db: Arc<NodeDbLite<PagedbStorageDefault>>,
    pub(crate) rt: tokio::runtime::Runtime,
    pub(crate) _tmpdir: Option<OwnedTempDir>,
}
