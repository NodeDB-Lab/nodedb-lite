//! C FFI bindings for NodeDB-Lite.
#![cfg(not(target_arch = "wasm32"))]
//!
//! Exposes the `NodeDb` trait as C-callable functions for Swift (iOS)
//! and Kotlin/JNI (Android) interop.
//!
//! Memory model:
//! - `nodedb_open` creates a handle; `nodedb_close` frees it.
//! - String parameters (`*const c_char`) are borrowed — caller owns the memory.
//! - Returned strings/buffers are Rust-allocated — caller must free via `nodedb_free_*`.
//! - Error codes: 0 = success, -1 = null pointer, -2 = invalid UTF-8, -3 = operation failed.

pub mod ffi_array;
pub mod ffi_document;
pub mod ffi_graph;
pub mod ffi_vector;
pub(crate) mod handle;
pub(crate) mod handle_registry;
pub mod ids;
pub mod jni_bridge;
pub mod memory;
pub mod open;
pub(crate) mod status;
pub mod sync;
pub(crate) mod util;

pub use ffi_array::*;
pub use ffi_document::*;
pub use ffi_graph::*;
pub use ffi_vector::*;
pub use handle::NodeDbHandle;
pub use ids::*;
pub use memory::*;
pub use open::*;
pub use status::{
    NODEDB_ERR_FAILED, NODEDB_ERR_NOT_FOUND, NODEDB_ERR_NULL, NODEDB_ERR_UTF8, NODEDB_OK,
};
pub use sync::*;

pub(crate) use util::{ffi_guard, handle_ref, ptr_to_str, write_c_string};
