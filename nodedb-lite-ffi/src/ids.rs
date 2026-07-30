// SPDX-License-Identifier: Apache-2.0

//! Identifier generation entry points.

use std::ffi::CString;
use std::os::raw::c_char;

use crate::status::{NODEDB_ERR_FAILED, NODEDB_ERR_NULL, NODEDB_ERR_UTF8, NODEDB_OK};
use crate::util::{ffi_guard, ptr_to_str};

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
