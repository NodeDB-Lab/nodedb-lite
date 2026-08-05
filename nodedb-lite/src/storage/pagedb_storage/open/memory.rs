// SPDX-License-Identifier: Apache-2.0

//! In-memory constructor (tests and WASM without persistence).

use std::sync::Arc;

use pagedb::vfs::memory::MemVfs;
use pagedb::{Db, RealmId};

use crate::error::LiteError;
use crate::storage::pagedb_storage::types::{PagedbStorage, lite_open_options};

impl PagedbStorage<MemVfs> {
    /// Create an in-memory database (for testing and WASM without persistence).
    ///
    /// In-memory storage is volatile (data lives only for the process lifetime),
    /// so no at-rest encryption is applied; the pagedb KEK is all-zero.
    pub async fn open_in_memory() -> Result<Self, LiteError> {
        let kek = [0u8; 32];
        let realm = RealmId::new([0u8; 16]);
        let vfs = MemVfs::new();
        let db = Db::open(vfs, kek, 4096, realm, lite_open_options())
            .await
            .map_err(LiteError::from)?;
        Ok(Self {
            db: Arc::new(db),
            page_size: 4096,
        })
    }
}
