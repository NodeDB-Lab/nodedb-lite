//! C FFI bindings for NodeDB-Lite.
//!
//! Exposes the `NodeDb` trait as C-callable functions for Swift (iOS)
//! and Kotlin/JNI (Android) interop.
//!
//! Memory model:
//! - `nodedb_open` creates a handle; `nodedb_close` frees it.
//! - String parameters (`*const c_char`) are borrowed — caller owns the memory.
//! - Returned strings/buffers are Rust-allocated — caller must free via `nodedb_free_*`.
//! - Error codes: 0 = success, -1 = null pointer, -2 = invalid UTF-8, -3 = operation failed.

pub mod ffi_document;
pub mod ffi_graph;
pub mod ffi_vector;
pub mod jni_bridge;

pub use ffi_document::*;
pub use ffi_graph::*;
pub use ffi_vector::*;

use std::ffi::{CStr, CString};
use std::os::raw::c_char;
use std::sync::Arc;

use nodedb_lite::{LiteConfig, NodeDbLite, RedbStorage};

/// Error codes returned by FFI functions.
pub const NODEDB_OK: i32 = 0;
pub const NODEDB_ERR_NULL: i32 = -1;
pub const NODEDB_ERR_UTF8: i32 = -2;
pub const NODEDB_ERR_FAILED: i32 = -3;
pub const NODEDB_ERR_NOT_FOUND: i32 = -4;

/// Opaque handle to a NodeDB-Lite database.
///
/// Created by `nodedb_open`, freed by `nodedb_close`.
pub struct NodeDbHandle {
    pub(crate) db: Arc<NodeDbLite<RedbStorage>>,
    pub(crate) rt: tokio::runtime::Runtime,
}

/// Open or create a NodeDB-Lite database at the given path.
///
/// Returns an opaque handle on success, NULL on failure.
/// The caller must call `nodedb_close` to free the handle.
///
/// # Safety
/// `path` must be a valid null-terminated UTF-8 string.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn nodedb_open(path: *const c_char, peer_id: u64) -> *mut NodeDbHandle {
    let path = match ptr_to_str(path) {
        Some(s) => s,
        None => return std::ptr::null_mut(),
    };

    let rt = match tokio::runtime::Builder::new_multi_thread()
        .worker_threads(1)
        .enable_all()
        .build()
    {
        Ok(rt) => rt,
        Err(_) => return std::ptr::null_mut(),
    };

    let storage = if path == ":memory:" {
        match RedbStorage::open_in_memory() {
            Ok(s) => s,
            Err(_) => return std::ptr::null_mut(),
        }
    } else {
        match RedbStorage::open(path) {
            Ok(s) => s,
            Err(_) => return std::ptr::null_mut(),
        }
    };

    let db = match rt.block_on(NodeDbLite::open(storage, peer_id)) {
        Ok(db) => Arc::new(db),
        Err(_) => return std::ptr::null_mut(),
    };

    Box::into_raw(Box::new(NodeDbHandle { db, rt }))
}

/// Open or create a NodeDB-Lite database with an explicit memory budget.
///
/// # Safety
/// `path` must be a valid null-terminated UTF-8 string.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn nodedb_open_with_config(
    path: *const c_char,
    peer_id: u64,
    memory_mb: u64,
) -> *mut NodeDbHandle {
    let path = match ptr_to_str(path) {
        Some(s) => s,
        None => return std::ptr::null_mut(),
    };

    let rt = match tokio::runtime::Builder::new_multi_thread()
        .worker_threads(1)
        .enable_all()
        .build()
    {
        Ok(rt) => rt,
        Err(_) => return std::ptr::null_mut(),
    };

    let storage = if path == ":memory:" {
        match RedbStorage::open_in_memory() {
            Ok(s) => s,
            Err(_) => return std::ptr::null_mut(),
        }
    } else {
        match RedbStorage::open(path) {
            Ok(s) => s,
            Err(_) => return std::ptr::null_mut(),
        }
    };

    let config = if memory_mb == 0 {
        LiteConfig::default()
    } else {
        LiteConfig {
            memory_budget: (memory_mb as usize).saturating_mul(1024 * 1024),
            ..LiteConfig::default()
        }
    };

    let db = match rt.block_on(NodeDbLite::open_with_config(storage, peer_id, config)) {
        Ok(db) => Arc::new(db),
        Err(_) => return std::ptr::null_mut(),
    };

    Box::into_raw(Box::new(NodeDbHandle { db, rt }))
}

/// Close a NodeDB-Lite database and free the handle.
///
/// # Safety
/// `handle` must be a valid pointer returned by `nodedb_open`, or NULL (no-op).
#[unsafe(no_mangle)]
pub unsafe extern "C" fn nodedb_close(handle: *mut NodeDbHandle) {
    if !handle.is_null() {
        drop(unsafe { Box::from_raw(handle) });
    }
}

/// Flush all in-memory state to disk.
///
/// # Safety
/// `handle` must be a valid pointer returned by `nodedb_open`.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn nodedb_flush(handle: *mut NodeDbHandle) -> i32 {
    let Some(h) = handle_ref(handle) else {
        return NODEDB_ERR_NULL;
    };
    match h.rt.block_on(h.db.flush()) {
        Ok(()) => NODEDB_OK,
        Err(_) => NODEDB_ERR_FAILED,
    }
}

// ─── CRDT Sync ─────────────────────────────────────────────────────

/// Start background CRDT sync to an Origin server.
///
/// Connects via WebSocket to the given URL, authenticates with the JWT token,
/// and continuously pushes pending deltas / receives shape updates.
/// Runs forever in the background with auto-reconnect.
///
/// Returns `NODEDB_OK` on successful launch (sync runs asynchronously).
///
/// # Safety
/// `url` and `jwt_token` must be valid null-terminated UTF-8 strings.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn nodedb_start_sync(
    handle: *mut NodeDbHandle,
    url: *const c_char,
    jwt_token: *const c_char,
) -> i32 {
    let Some(h) = handle_ref(handle) else {
        return NODEDB_ERR_NULL;
    };
    let Some(url_str) = ptr_to_str(url) else {
        return NODEDB_ERR_UTF8;
    };
    let Some(jwt_str) = ptr_to_str(jwt_token) else {
        return NODEDB_ERR_UTF8;
    };

    let config = nodedb_lite::sync::SyncConfig {
        url: url_str.to_string(),
        jwt_token: jwt_str.to_string(),
        client_version: format!("nodedb-lite-ffi/{}", env!("CARGO_PKG_VERSION")),
        min_backoff: std::time::Duration::from_secs(1),
        max_backoff: std::time::Duration::from_secs(60),
        ping_interval: std::time::Duration::from_secs(30),
        max_batch_size: 100,
        token_provider: None,
        token_lifetime_secs: 0,
    };

    // start_sync requires a tokio runtime context for spawning the background task.
    let _guard = h.rt.enter();
    let _sync_client = h.db.start_sync(config);

    NODEDB_OK
}

// ─── ID Generation ──────────────────────────────────────────────────

/// Generate a UUIDv7 (time-sortable, recommended for primary keys).
///
/// # Safety
/// `out` must be a valid pointer to a `*mut c_char`.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn nodedb_generate_id(out: *mut *mut c_char) -> i32 {
    if out.is_null() {
        return NODEDB_ERR_NULL;
    }
    let id = nodedb_types::id_gen::uuid_v7();
    match CString::new(id) {
        Ok(cs) => {
            unsafe { *out = cs.into_raw() };
            NODEDB_OK
        }
        Err(_) => NODEDB_ERR_FAILED,
    }
}

/// Generate an ID of the specified type.
///
/// Supported types: "uuidv7", "uuidv4", "ulid", "cuid2", "nanoid".
///
/// # Safety
/// `id_type` must be a valid null-terminated UTF-8 string. `out` must be a valid pointer.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn nodedb_generate_id_typed(
    id_type: *const c_char,
    out: *mut *mut c_char,
) -> i32 {
    if out.is_null() {
        return NODEDB_ERR_NULL;
    }
    let Some(id_type_str) = ptr_to_str(id_type) else {
        return NODEDB_ERR_UTF8;
    };
    let id = match nodedb_types::id_gen::generate_by_type(id_type_str) {
        Some(id) => id,
        None => return NODEDB_ERR_FAILED,
    };
    match CString::new(id) {
        Ok(cs) => {
            unsafe { *out = cs.into_raw() };
            NODEDB_OK
        }
        Err(_) => NODEDB_ERR_FAILED,
    }
}

// ─── Memory Management ──────────────────────────────────────────────

/// Free a string returned by nodedb_* functions.
///
/// # Safety
/// `ptr` must be a string previously returned by a nodedb function, or NULL.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn nodedb_free_string(ptr: *mut c_char) {
    if !ptr.is_null() {
        drop(unsafe { CString::from_raw(ptr) });
    }
}

// ─── Internal Helpers ────────────────────────────────────────────────

/// # Safety
/// `ptr` must be a valid null-terminated C string, or null.
pub(crate) fn ptr_to_str<'a>(ptr: *const c_char) -> Option<&'a str> {
    if ptr.is_null() {
        return None;
    }
    unsafe { CStr::from_ptr(ptr) }.to_str().ok()
}

/// # Safety
/// `handle` must be a valid `NodeDbHandle` pointer, or null.
pub(crate) fn handle_ref<'a>(handle: *mut NodeDbHandle) -> Option<&'a NodeDbHandle> {
    if handle.is_null() {
        None
    } else {
        Some(unsafe { &*handle })
    }
}

/// Marshal a JSON string into a C output pointer.
///
/// On success, writes the CString to `*out` and returns `NODEDB_OK`.
/// On failure (interior null byte), returns `NODEDB_ERR_FAILED`.
///
/// # Safety
/// `out` must be a valid, non-null `*mut *mut c_char`.
pub(crate) unsafe fn write_c_string(out: *mut *mut c_char, s: String) -> i32 {
    match CString::new(s) {
        Ok(cs) => {
            unsafe { *out = cs.into_raw() };
            NODEDB_OK
        }
        Err(_) => NODEDB_ERR_FAILED,
    }
}
