// SPDX-License-Identifier: Apache-2.0

//! Deallocation of buffers handed to the caller.

use std::ffi::CString;
use std::os::raw::c_char;

use crate::util::ffi_guard;

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
