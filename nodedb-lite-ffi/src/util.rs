// SPDX-License-Identifier: Apache-2.0

//! Shared helpers: panic containment, pointer marshalling, handle lookup.

use std::ffi::{CStr, CString};
use std::os::raw::c_char;

use nodedb_lite::Encryption;

use crate::handle::NodeDbHandle;
use crate::handle_registry;
use crate::status::{NODEDB_ERR_FAILED, NODEDB_ERR_NULL, NODEDB_OK};

/// Run `f`, catching any panic so it never unwinds across the FFI boundary
/// (which is UB). On panic, returns `default`.
pub(crate) fn ffi_guard<T>(default: T, f: impl FnOnce() -> T) -> T {
    match std::panic::catch_unwind(std::panic::AssertUnwindSafe(f)) {
        Ok(v) => v,
        Err(_) => default,
    }
}

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
