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
pub(crate) mod handle_registry;
pub mod jni_bridge;

/// Run `f`, catching any panic so it never unwinds across the FFI boundary
/// (which is UB). On panic, returns `default`.
pub(crate) fn ffi_guard<T>(default: T, f: impl FnOnce() -> T) -> T {
    match std::panic::catch_unwind(std::panic::AssertUnwindSafe(f)) {
        Ok(v) => v,
        Err(_) => default,
    }
}

pub use ffi_array::*;
pub use ffi_document::*;
pub use ffi_graph::*;
pub use ffi_vector::*;

use std::ffi::{CStr, CString};
use std::os::raw::c_char;
use std::sync::Arc;

use nodedb_lite::{Encryption, LiteConfig, NodeDbLite, PagedbStorageDefault};

/// Error codes returned by FFI functions.
pub const NODEDB_OK: i32 = 0;
pub const NODEDB_ERR_NULL: i32 = -1;
pub const NODEDB_ERR_UTF8: i32 = -2;
pub const NODEDB_ERR_FAILED: i32 = -3;
pub const NODEDB_ERR_NOT_FOUND: i32 = -4;

/// Minimal RAII temp-directory wrapper used for the `:memory:` path.
///
/// Deleted on drop. No external crate dependency required.
struct OwnedTempDir(std::path::PathBuf);

impl Drop for OwnedTempDir {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

impl OwnedTempDir {
    /// Create a unique temporary directory under `std::env::temp_dir()`.
    fn new() -> Option<Self> {
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
/// `_tmpdir` is `Some` when the database was opened with the `:memory:` path.
/// The directory is deleted when the handle is dropped.
pub struct NodeDbHandle {
    pub(crate) db: Arc<NodeDbLite<PagedbStorageDefault>>,
    pub(crate) rt: tokio::runtime::Runtime,
    _tmpdir: Option<OwnedTempDir>,
}

/// Open or create a NodeDB-Lite database at the given path.
///
/// Returns an opaque handle on success, NULL on failure.
/// The caller must call `nodedb_close` to free the handle.
///
/// # Safety
/// - `path` must be a valid null-terminated UTF-8 string.
/// - `passphrase` must be NULL or a valid null-terminated UTF-8 string.
///
/// Encryption convention:
/// - `passphrase` is NULL and `path` is `":memory:"` → `Encryption::Plaintext` (volatile data, safe).
/// - `passphrase` is NULL and `path` is a real path → returns NULL (silent plaintext persistent
///   storage is refused; pass an empty string to opt out explicitly).
/// - `passphrase` is `""` (empty string) → `Encryption::Plaintext` (explicit conscious opt-out).
/// - `passphrase` is a non-empty string → `Encryption::passphrase(passphrase)`.
/// - `passphrase` is non-NULL but invalid UTF-8 → returns NULL.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn nodedb_open(
    path: *const c_char,
    peer_id: u64,
    passphrase: *const c_char,
) -> *mut NodeDbHandle {
    ffi_guard(std::ptr::null_mut(), || {
        let path = match ptr_to_str(path) {
            Some(s) => s,
            None => return std::ptr::null_mut(),
        };

        let is_memory = path == ":memory:";
        let enc = match resolve_encryption(passphrase, is_memory) {
            Some(e) => e,
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

        let (storage, tmpdir) = if is_memory {
            let tmp = match OwnedTempDir::new() {
                Some(t) => t,
                None => return std::ptr::null_mut(),
            };
            let s = match rt.block_on(PagedbStorageDefault::open(&tmp.0, enc)) {
                Ok(s) => s,
                Err(_) => return std::ptr::null_mut(),
            };
            (s, Some(tmp))
        } else {
            let s = match rt.block_on(PagedbStorageDefault::open(path, enc)) {
                Ok(s) => s,
                Err(_) => return std::ptr::null_mut(),
            };
            (s, None)
        };

        let db = match rt.block_on(NodeDbLite::open(storage, peer_id)) {
            Ok(db) => Arc::new(db),
            Err(_) => return std::ptr::null_mut(),
        };

        let defaults = LiteConfig::default();
        let auto_flush_ms = defaults.auto_flush_ms;
        let auto_compact_ms = defaults.auto_compact_ms;
        let _guard = rt.enter();
        db.start_auto_flush(auto_flush_ms);
        db.start_auto_compact(auto_compact_ms);

        handle_registry::insert(NodeDbHandle {
            db,
            rt,
            _tmpdir: tmpdir,
        }) as *mut NodeDbHandle
    })
}

/// Open or create a NodeDB-Lite database with an explicit memory budget.
///
/// # Safety
/// - `path` must be a valid null-terminated UTF-8 string.
/// - `passphrase` must be NULL or a valid null-terminated UTF-8 string.
///
/// See `nodedb_open` for the passphrase/encryption convention.
/// `memory_mb` of 0 uses the default memory budget.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn nodedb_open_with_config(
    path: *const c_char,
    peer_id: u64,
    memory_mb: u64,
    passphrase: *const c_char,
) -> *mut NodeDbHandle {
    ffi_guard(std::ptr::null_mut(), || {
        let path = match ptr_to_str(path) {
            Some(s) => s,
            None => return std::ptr::null_mut(),
        };

        let is_memory = path == ":memory:";
        let enc = match resolve_encryption(passphrase, is_memory) {
            Some(e) => e,
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

        let (storage, tmpdir) = if is_memory {
            let tmp = match OwnedTempDir::new() {
                Some(t) => t,
                None => return std::ptr::null_mut(),
            };
            let s = match rt.block_on(PagedbStorageDefault::open(&tmp.0, enc)) {
                Ok(s) => s,
                Err(_) => return std::ptr::null_mut(),
            };
            (s, Some(tmp))
        } else {
            let s = match rt.block_on(PagedbStorageDefault::open(path, enc)) {
                Ok(s) => s,
                Err(_) => return std::ptr::null_mut(),
            };
            (s, None)
        };

        let config = if memory_mb == 0 {
            LiteConfig::default()
        } else {
            LiteConfig {
                memory_budget: (memory_mb as usize).saturating_mul(1024 * 1024),
                ..LiteConfig::default()
            }
        };

        let auto_flush_ms = config.auto_flush_ms;
        let auto_compact_ms = config.auto_compact_ms;
        let db = match rt.block_on(NodeDbLite::open_with_config(storage, peer_id, config)) {
            Ok(db) => Arc::new(db),
            Err(_) => return std::ptr::null_mut(),
        };

        let _guard = rt.enter();
        db.start_auto_flush(auto_flush_ms);
        db.start_auto_compact(auto_compact_ms);

        handle_registry::insert(NodeDbHandle {
            db,
            rt,
            _tmpdir: tmpdir,
        }) as *mut NodeDbHandle
    })
}

/// Close a NodeDB-Lite database and free the handle.
///
/// # Safety
/// `handle` must be a token returned by `nodedb_open`, or NULL/0 (no-op).
/// The token is a `u64` id packed into a pointer-width integer; it is never
/// dereferenced as a raw pointer.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn nodedb_close(handle: *mut NodeDbHandle) {
    ffi_guard((), || {
        // handle is an opaque id token, not a real pointer — never dereference it.
        handle_registry::remove(handle as u64);
    })
}

/// Flush all in-memory state to disk.
///
/// # Safety
/// `handle` must be a valid pointer returned by `nodedb_open`.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn nodedb_flush(handle: *mut NodeDbHandle) -> i32 {
    ffi_guard(NODEDB_ERR_FAILED, || {
        let Some(h) = handle_ref(handle) else {
            return NODEDB_ERR_NULL;
        };
        match h.rt.block_on(h.db.flush()) {
            Ok(()) => NODEDB_OK,
            Err(_) => NODEDB_ERR_FAILED,
        }
    })
}

/// Compact the backing store, reclaiming dead pages and truncating the file to
/// bound on-disk growth.
///
/// The three `out_*` pointers receive the compaction outcome; any of them may
/// be NULL to ignore that field. On error they are left untouched.
///
/// Returns `NODEDB_OK` on success, `NODEDB_ERR_NULL` if `handle` is NULL, or
/// `NODEDB_ERR_FAILED` on a compaction error.
///
/// # Safety
/// `handle` must be a valid pointer returned by `nodedb_open`. Each non-NULL
/// `out_*` pointer must be writable and correctly aligned.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn nodedb_compact(
    handle: *mut NodeDbHandle,
    out_reclaimed_pages: *mut u64,
    out_segments_repacked: *mut u32,
    out_file_bytes_freed: *mut u64,
) -> i32 {
    ffi_guard(NODEDB_ERR_FAILED, || {
        let Some(h) = handle_ref(handle) else {
            return NODEDB_ERR_NULL;
        };
        match h.rt.block_on(h.db.compact()) {
            Ok(outcome) => {
                if !out_reclaimed_pages.is_null() {
                    unsafe { *out_reclaimed_pages = outcome.reclaimed_pages };
                }
                if !out_segments_repacked.is_null() {
                    unsafe { *out_segments_repacked = outcome.segments_repacked };
                }
                if !out_file_bytes_freed.is_null() {
                    unsafe { *out_file_bytes_freed = outcome.file_bytes_freed };
                }
                NODEDB_OK
            }
            Err(_) => NODEDB_ERR_FAILED,
        }
    })
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
    ffi_guard(NODEDB_ERR_FAILED, || {
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
    })
}

// ─── ID Generation ──────────────────────────────────────────────────

/// Generate a UUIDv7 (time-sortable, recommended for primary keys).
///
/// # Safety
/// `out` must be a valid pointer to a `*mut c_char`.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn nodedb_generate_id(out: *mut *mut c_char) -> i32 {
    ffi_guard(NODEDB_ERR_FAILED, || {
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
    })
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
    ffi_guard(NODEDB_ERR_FAILED, || {
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
    })
}

// ─── Memory Management ──────────────────────────────────────────────

/// Free a string returned by nodedb_* functions.
///
/// # Safety
/// `ptr` must be a string previously returned by a nodedb function, or NULL.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn nodedb_free_string(ptr: *mut c_char) {
    ffi_guard((), || {
        if !ptr.is_null() {
            drop(unsafe { CString::from_raw(ptr) });
        }
    })
}

/// Free a byte buffer returned by nodedb_* functions (e.g. `ndb_array_slice`).
///
/// `len` must be the exact length originally written to `*out_len`.
///
/// # Safety
/// `ptr` must be a buffer previously returned by a nodedb function, or NULL.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn nodedb_free_buf(ptr: *mut u8, len: usize) {
    ffi_guard((), || {
        if !ptr.is_null() && len > 0 {
            drop(unsafe { Box::from_raw(std::ptr::slice_from_raw_parts_mut(ptr, len)) });
        }
    })
}

// ─── Internal Helpers ────────────────────────────────────────────────

/// Resolve a `passphrase` C string pointer into an [`Encryption`] variant.
///
/// Returns `None` to signal that the caller should return a null handle (refused open).
///
/// Convention:
/// - `passphrase` NULL + `is_memory` true  → `Encryption::Plaintext` (volatile, always allowed).
/// - `passphrase` NULL + `is_memory` false → `None` (persistent plaintext refused; use `""` to
///   opt out explicitly).
/// - `passphrase` `""` (empty string)      → `Encryption::Plaintext` (explicit conscious opt-out).
/// - `passphrase` non-empty string         → `Encryption::passphrase(s)`.
/// - `passphrase` non-NULL + invalid UTF-8 → `None`.
///
/// # Safety
/// `passphrase` must be NULL or a valid null-terminated C string.
pub(crate) fn resolve_encryption(passphrase: *const c_char, is_memory: bool) -> Option<Encryption> {
    if passphrase.is_null() {
        if is_memory {
            return Some(Encryption::Plaintext);
        } else {
            return None;
        }
    }
    let s = ptr_to_str(passphrase)?;
    if s.is_empty() {
        Some(Encryption::Plaintext)
    } else {
        Some(Encryption::passphrase(s))
    }
}

/// # Safety
/// `ptr` must be a valid null-terminated C string, or null.
pub(crate) fn ptr_to_str<'a>(ptr: *const c_char) -> Option<&'a str> {
    if ptr.is_null() {
        return None;
    }
    unsafe { CStr::from_ptr(ptr) }.to_str().ok()
}

/// Look up the handle for an opaque token returned by `nodedb_open`.
///
/// Returns a cloned `Arc` so the handle stays alive for the duration of the
/// call even if another thread concurrently calls `nodedb_close`.  Token 0
/// (NULL) and unknown ids both return `None`.
///
/// Note: the token is a `u64` id packed into the pointer-width type used by
/// the C ABI.  On all supported 64-bit targets (arm64, x86_64) no bits are
/// truncated.  The pointer is never dereferenced.
pub(crate) fn handle_ref(handle: *mut NodeDbHandle) -> Option<std::sync::Arc<NodeDbHandle>> {
    handle_registry::get(handle as u64)
}

/// Marshal a JSON string into a C output pointer.
///
/// On success, writes the CString to `*out` and returns `NODEDB_OK`.
/// On failure (interior null byte), returns `NODEDB_ERR_FAILED`.
///
/// # Safety
/// `out` must be a valid, non-null `*mut *mut c_char`.
pub(crate) unsafe fn write_c_string(out: *mut *mut c_char, s: String) -> i32 {
    if out.is_null() {
        return NODEDB_ERR_NULL;
    }
    match CString::new(s) {
        Ok(cs) => {
            unsafe { *out = cs.into_raw() };
            NODEDB_OK
        }
        Err(_) => NODEDB_ERR_FAILED,
    }
}

#[cfg(test)]
mod tests {
    use super::ffi_guard;

    #[test]
    fn ffi_guard_returns_value_on_success() {
        let result = ffi_guard(42i32, || 7i32);
        assert_eq!(result, 7);
    }

    #[test]
    fn ffi_guard_returns_default_on_panic() {
        let result = ffi_guard(-3i32, || -> i32 { panic!("intentional panic in test") });
        assert_eq!(result, -3);
    }

    #[test]
    fn ffi_guard_unit_does_not_propagate_panic() {
        // Must not unwind out of the test — the panic is caught.
        ffi_guard((), || panic!("intentional panic in unit test"));
    }
}
