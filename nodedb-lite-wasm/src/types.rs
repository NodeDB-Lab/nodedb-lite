// SPDX-License-Identifier: Apache-2.0

//! The JS-facing database type and the storage-backend dispatch it wraps.

use std::sync::Arc;

use wasm_bindgen::prelude::*;

use nodedb_lite::NodeDbLite;
use nodedb_lite::storage::pagedb_storage::PagedbStorageMem;

#[cfg(all(target_arch = "wasm32", feature = "opfs"))]
use nodedb_lite::PagedbStorageOpfs;

/// Holds either an in-memory or an OPFS-backed `NodeDbLite` instance.
///
/// The two concrete storage types are different Rust types, so we unify them
/// behind this enum and dispatch each method to the appropriate arm.
///
/// `Arc` is what the `open*` constructors hand back: the background flush and
/// compaction tasks hold a `Weak` to it, so they exit cleanly when the JS
/// object is GC'd.
pub(crate) enum NodeDbLiteWasmInner {
    InMemory(Arc<NodeDbLite<PagedbStorageMem>>),
    #[cfg(all(target_arch = "wasm32", feature = "opfs"))]
    Persistent(Arc<NodeDbLite<PagedbStorageOpfs>>),
}

// These macros are used across every method module.  Declaring them here and
// re-exporting from the crate root makes them available without any `use` path
// gymnastics in the submodules.
macro_rules! dispatch {
    ($self:ident, $inner:ident, $body:expr) => {
        match &$self.inner {
            crate::types::NodeDbLiteWasmInner::InMemory($inner) => $body,
            #[cfg(all(target_arch = "wasm32", feature = "opfs"))]
            crate::types::NodeDbLiteWasmInner::Persistent($inner) => $body,
        }
    };
}
pub(crate) use dispatch;

/// NodeDB-Lite instance for browser/WASM environments.
///
/// Wraps either an in-memory or an OPFS-backed database. Construct via the
/// static factory methods: `openInMemory`, `open`, `openWithConfig`,
/// `openPersistent`, or `openPersistentWithConfig`.
#[wasm_bindgen]
pub struct NodeDbLiteWasm {
    pub(crate) inner: NodeDbLiteWasmInner,
}
