// SPDX-License-Identifier: Apache-2.0

//! pagedb-backed `StorageEngine` implementation.

pub(crate) mod engine;
pub(crate) mod errors;
pub(crate) mod keys;
pub(crate) mod open;
pub(crate) mod segment_ext;
pub(crate) mod types;

// Re-exported for the segment-backed storage modules, which are native-only;
// the engine impls reach into `keys` directly.
#[cfg(not(target_arch = "wasm32"))]
pub(crate) use keys::prefix_key;

#[cfg(not(target_arch = "wasm32"))]
pub use types::PagedbStorageDefault;
#[cfg(target_arch = "wasm32")]
pub use types::PagedbStorageOpfs;
pub use types::{PagedbStorage, PagedbStorageMem};
