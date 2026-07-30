// SPDX-License-Identifier: Apache-2.0

//! Database lifetime: open, close, flush, compact.

use std::os::raw::c_char;

use nodedb_lite::{LiteConfig, NodeDbLite};

use crate::handle::{NodeDbHandle, OwnedTempDir};
use crate::handle_registry;
use crate::status::{NODEDB_ERR_FAILED, NODEDB_ERR_NULL, NODEDB_OK};
use crate::util::{ffi_guard, handle_ref, ptr_to_str, resolve_encryption};

/// Open or create a NodeDB-Lite database at the given path.
///
/// Returns an opaque handle on success, NULL on failure.
/// The caller must call `nodedb_close` to free the handle.
///
/// Background flush and compaction start with the database, at the intervals
/// the resolved configuration specifies.
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

        let (db, tmpdir) = if is_memory {
            let tmp = match OwnedTempDir::new() {
                Some(t) => t,
                None => return std::ptr::null_mut(),
            };
            let db = match rt.block_on(NodeDbLite::open_at_path(&tmp.0, peer_id, enc)) {
                Ok(db) => db,
                Err(_) => return std::ptr::null_mut(),
            };
            (db, Some(tmp))
        } else {
            let db = match rt.block_on(NodeDbLite::open_at_path(path, peer_id, enc)) {
                Ok(db) => db,
                Err(_) => return std::ptr::null_mut(),
            };
            (db, None)
        };

        // Auto-flush and auto-compact are started by `open_at_path` from the
        // configuration it resolves, inside the `block_on` runtime context.

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

        let config = if memory_mb == 0 {
            LiteConfig::default()
        } else {
            LiteConfig {
                memory_budget: (memory_mb as usize).saturating_mul(1024 * 1024),
                ..LiteConfig::default()
            }
        };

        let (db, tmpdir) = if is_memory {
            let tmp = match OwnedTempDir::new() {
                Some(t) => t,
                None => return std::ptr::null_mut(),
            };
            let db = match rt.block_on(NodeDbLite::open_at_path_with_config(
                &tmp.0, peer_id, enc, config,
            )) {
                Ok(db) => db,
                Err(_) => return std::ptr::null_mut(),
            };
            (db, Some(tmp))
        } else {
            let db = match rt.block_on(NodeDbLite::open_at_path_with_config(
                path, peer_id, enc, config,
            )) {
                Ok(db) => db,
                Err(_) => return std::ptr::null_mut(),
            };
            (db, None)
        };

        // The background maintenance tasks come from `config` itself, spawned
        // by the constructor inside the `block_on` runtime context.

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
