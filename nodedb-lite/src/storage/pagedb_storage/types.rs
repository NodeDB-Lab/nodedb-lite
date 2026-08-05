// SPDX-License-Identifier: Apache-2.0

//! The `PagedbStorage` handle, its VFS aliases, and the shared `OpenOptions`.

use std::sync::Arc;

use pagedb::Db;
use pagedb::options::{OpenOptions, RetainPolicy};
use pagedb::vfs::Vfs;
use pagedb::vfs::memory::MemVfs;

#[cfg(not(target_arch = "wasm32"))]
use pagedb::vfs::DefaultVfs;

/// `PagedbStorage` backed by the native platform VFS (io_uring on Linux, etc.).
#[cfg(not(target_arch = "wasm32"))]
pub type PagedbStorageDefault = PagedbStorage<DefaultVfs>;

/// `PagedbStorage` backed by an in-memory VFS (tests / ephemeral use).
pub type PagedbStorageMem = PagedbStorage<MemVfs>;

/// `PagedbStorage` backed by the browser OPFS VFS (persistent, wasm32 only).
///
/// Constructed via [`PagedbStorage::open_opfs`](PagedbStorage).
#[cfg(target_arch = "wasm32")]
pub type PagedbStorageOpfs = PagedbStorage<pagedb::vfs::opfs::OpfsVfs>;

/// Build the `OpenOptions` used for all `PagedbStorage` instances.
///
/// `RetainPolicy::Disabled` is selected because Lite does not need
/// point-in-time reads; skipping commit-history tracking shaves latency
/// from every `WriteTxn::commit`.
pub(crate) fn lite_open_options() -> OpenOptions {
    OpenOptions::default().with_commit_history_retain(RetainPolicy::Disabled)
}

/// pagedb-backed KV storage.
///
/// The inner `Db<V>` lives behind `Arc` for cheap cloning across async methods.
/// No outer `Mutex` is needed: `Db::begin_write` already acquires an internal
/// async mutex (single-writer serialization is enforced by pagedb itself).
pub struct PagedbStorage<V: Vfs + Clone> {
    pub(crate) db: Arc<Db<V>>,
    pub(crate) page_size: usize,
}

impl<V: Vfs + Clone> PagedbStorage<V> {
    pub(crate) fn page_body_capacity(&self) -> usize {
        self.page_size - 40
    }
}

// Manual Clone so we don't require `V: Clone` on the struct level — the
// `Arc` clone is cheap and does not clone the underlying `Db`.
impl<V: Vfs + Clone> Clone for PagedbStorage<V> {
    fn clone(&self) -> Self {
        Self {
            db: Arc::clone(&self.db),
            page_size: self.page_size,
        }
    }
}
