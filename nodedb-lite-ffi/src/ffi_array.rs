//! Array engine FFI functions.
//!
//! Complex argument types (schema, coordinates, ranges, payloads) are
//! exchanged as MessagePack-encoded byte slices. The caller serialises
//! with zerompk (or any compatible msgpack encoder) and the output
//! buffers must be freed via `nodedb_free_buf`.

use std::os::raw::c_char;

use crate::{
    NODEDB_ERR_FAILED, NODEDB_ERR_NULL, NODEDB_ERR_UTF8, NODEDB_OK, NodeDbHandle, ffi_guard,
    handle_ref, ptr_to_str,
};

// ─── helpers ─────────────────────────────────────────────────────────────────

/// Write a msgpack-encoded value into a caller-supplied `(*mut *mut u8, *mut usize)` pair.
///
/// # Safety
/// `out_buf` and `out_len` must be valid non-null pointers.
unsafe fn write_msgpack_out(bytes: Vec<u8>, out_buf: *mut *mut u8, out_len: *mut usize) -> i32 {
    let boxed = bytes.into_boxed_slice();
    let len = boxed.len();
    let ptr = Box::into_raw(boxed) as *mut u8;
    unsafe {
        *out_buf = ptr;
        *out_len = len;
    }
    NODEDB_OK
}

/// Decode a msgpack byte slice from a raw C pointer + length pair.
///
/// Returns `None` if the pointer is null, len is zero, or deserialization fails.
///
/// # Safety contract (callers must uphold)
/// When `ptr` is non-null and `len > 0`, `ptr` must be non-null, properly aligned
/// for `u8`, and valid for exactly `len` bytes for the entire duration of this call.
/// No runtime length validation beyond the null/zero check is possible; passing a
/// mismatched `len` or a dangling pointer is immediate undefined behaviour.
fn decode_msgpack<T>(ptr: *const u8, len: usize) -> Option<T>
where
    T: for<'a> zerompk::FromMessagePack<'a>,
{
    if ptr.is_null() || len == 0 {
        return None;
    }
    // Safety: caller guarantees ptr is valid for len bytes.
    let slice = unsafe { std::slice::from_raw_parts(ptr, len) };
    zerompk::from_msgpack(slice).ok()
}

// ─── public FFI ──────────────────────────────────────────────────────────────

/// Create a new ND sparse array.
///
/// `schema_msgpack` — zerompk-encoded `ArraySchema`.
/// `dims_msgpack`   — reserved for future use; pass NULL / 0 today.
///
/// Returns `NODEDB_OK` on success, `NODEDB_ERR_*` on failure.
///
/// # Safety
/// All pointer parameters must be valid. `name` must be a null-terminated UTF-8 string.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn ndb_array_create(
    handle: *mut NodeDbHandle,
    name: *const c_char,
    schema_msgpack: *const u8,
    schema_len: usize,
) -> i32 {
    ffi_guard(NODEDB_ERR_FAILED, || {
        let Some(h) = handle_ref(handle) else {
            return NODEDB_ERR_NULL;
        };
        let Some(name_str) = ptr_to_str(name) else {
            return NODEDB_ERR_UTF8;
        };
        let Some(schema) =
            decode_msgpack::<nodedb_array::schema::ArraySchema>(schema_msgpack, schema_len)
        else {
            return NODEDB_ERR_FAILED;
        };

        match h.rt.block_on(h.db.create_array(name_str, schema)) {
            Ok(()) => NODEDB_OK,
            Err(_) => NODEDB_ERR_FAILED,
        }
    })
}

/// Write a cell into array `name` at `coord`.
///
/// `coord_msgpack`   — zerompk-encoded `Vec<CoordValue>`.
/// `payload_msgpack` — zerompk-encoded `Vec<CellValue>` (attribute values).
/// `valid_from_ms`   — valid-time lower bound (milliseconds since epoch).
/// `valid_until_ms`  — valid-time upper bound (`i64::MAX` = open-ended).
///
/// # Safety
/// All pointer parameters must be valid. `name` must be a null-terminated UTF-8 string.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn ndb_array_put_cell(
    handle: *mut NodeDbHandle,
    name: *const c_char,
    coord_msgpack: *const u8,
    coord_len: usize,
    payload_msgpack: *const u8,
    payload_len: usize,
    valid_from_ms: i64,
    valid_until_ms: i64,
) -> i32 {
    ffi_guard(NODEDB_ERR_FAILED, || {
        let Some(h) = handle_ref(handle) else {
            return NODEDB_ERR_NULL;
        };
        let Some(name_str) = ptr_to_str(name) else {
            return NODEDB_ERR_UTF8;
        };
        let Some(coord) = decode_msgpack::<Vec<nodedb_array::types::coord::value::CoordValue>>(
            coord_msgpack,
            coord_len,
        ) else {
            return NODEDB_ERR_FAILED;
        };
        let Some(attrs) = decode_msgpack::<Vec<nodedb_array::types::cell_value::value::CellValue>>(
            payload_msgpack,
            payload_len,
        ) else {
            return NODEDB_ERR_FAILED;
        };

        let system_from_ms = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_millis() as i64)
            .unwrap_or(0);

        match h.rt.block_on(h.db.array_put_cell(
            name_str,
            coord,
            attrs,
            system_from_ms,
            valid_from_ms,
            valid_until_ms,
        )) {
            Ok(()) => NODEDB_OK,
            Err(_) => NODEDB_ERR_FAILED,
        }
    })
}

/// Slice query: return all live cells whose coordinates fall within `ranges`.
///
/// `ranges_msgpack` — zerompk-encoded `Vec<Option<DimRange>>`.
/// `as_of_system_ms` — system-time AS-OF (ignored when `has_as_of == 0`).
/// `has_as_of`       — set to 1 to use `as_of_system_ms`, 0 for current live snapshot.
///
/// On success `*out_buf` points to a zerompk-encoded `Vec<CellPayload>`.
/// Caller must free with `nodedb_free_buf`.
///
/// # Safety
/// All pointer parameters must be valid. `name` must be a null-terminated UTF-8 string.
/// `out_buf` and `out_len` must not be null.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn ndb_array_slice(
    handle: *mut NodeDbHandle,
    name: *const c_char,
    ranges_msgpack: *const u8,
    ranges_len: usize,
    as_of_system_ms: i64,
    has_as_of: u8,
    out_buf: *mut *mut u8,
    out_len: *mut usize,
) -> i32 {
    ffi_guard(NODEDB_ERR_FAILED, || {
        let Some(h) = handle_ref(handle) else {
            return NODEDB_ERR_NULL;
        };
        let Some(name_str) = ptr_to_str(name) else {
            return NODEDB_ERR_UTF8;
        };
        if out_buf.is_null() || out_len.is_null() {
            return NODEDB_ERR_NULL;
        }
        let Some(ranges) = decode_msgpack::<Vec<Option<nodedb_array::query::slice::DimRange>>>(
            ranges_msgpack,
            ranges_len,
        ) else {
            return NODEDB_ERR_FAILED;
        };

        let as_of = if has_as_of != 0 {
            Some(as_of_system_ms)
        } else {
            None
        };

        match h.rt.block_on(h.db.array_slice(name_str, ranges, as_of)) {
            Ok(cells) => {
                let encoded = match zerompk::to_msgpack_vec(&cells) {
                    Ok(b) => b,
                    Err(_) => return NODEDB_ERR_FAILED,
                };
                unsafe { write_msgpack_out(encoded, out_buf, out_len) }
            }
            Err(_) => NODEDB_ERR_FAILED,
        }
    })
}

/// Read the most recent live payload for `coord` at or before `as_of_system_ms`.
///
/// `coord_msgpack`   — zerompk-encoded `Vec<CoordValue>`.
/// `has_as_of`       — set to 1 to use `as_of_system_ms`, 0 for current live snapshot.
///
/// On success and if a cell exists, `*out_buf` points to a zerompk-encoded
/// `CellPayload` (length > 0). If the cell does not exist, `*out_len` is 0
/// and `*out_buf` is NULL.
/// Caller must free with `nodedb_free_buf` when `*out_len > 0`.
///
/// # Safety
/// All pointer parameters must be valid. `name` must be a null-terminated UTF-8 string.
/// `out_buf` and `out_len` must not be null.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn ndb_array_read_coord(
    handle: *mut NodeDbHandle,
    name: *const c_char,
    coord_msgpack: *const u8,
    coord_len: usize,
    as_of_system_ms: i64,
    has_as_of: u8,
    out_buf: *mut *mut u8,
    out_len: *mut usize,
) -> i32 {
    ffi_guard(NODEDB_ERR_FAILED, || {
        let Some(h) = handle_ref(handle) else {
            return NODEDB_ERR_NULL;
        };
        let Some(name_str) = ptr_to_str(name) else {
            return NODEDB_ERR_UTF8;
        };
        if out_buf.is_null() || out_len.is_null() {
            return NODEDB_ERR_NULL;
        }
        let Some(coord) = decode_msgpack::<Vec<nodedb_array::types::coord::value::CoordValue>>(
            coord_msgpack,
            coord_len,
        ) else {
            return NODEDB_ERR_FAILED;
        };

        let as_of = if has_as_of != 0 {
            Some(as_of_system_ms)
        } else {
            None
        };

        match h
            .rt
            .block_on(h.db.array_read_coord(name_str, &coord, as_of))
        {
            Ok(Some(cell)) => {
                let encoded = match zerompk::to_msgpack_vec(&cell) {
                    Ok(b) => b,
                    Err(_) => return NODEDB_ERR_FAILED,
                };
                unsafe { write_msgpack_out(encoded, out_buf, out_len) }
            }
            Ok(None) => {
                unsafe {
                    *out_buf = std::ptr::null_mut();
                    *out_len = 0;
                }
                NODEDB_OK
            }
            Err(_) => NODEDB_ERR_FAILED,
        }
    })
}

/// Soft-delete a cell by writing a tombstone at the current system time.
///
/// `coord_msgpack` — zerompk-encoded `Vec<CoordValue>`.
///
/// # Safety
/// All pointer parameters must be valid. `name` must be a null-terminated UTF-8 string.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn ndb_array_delete_cell(
    handle: *mut NodeDbHandle,
    name: *const c_char,
    coord_msgpack: *const u8,
    coord_len: usize,
) -> i32 {
    ffi_guard(NODEDB_ERR_FAILED, || {
        let Some(h) = handle_ref(handle) else {
            return NODEDB_ERR_NULL;
        };
        let Some(name_str) = ptr_to_str(name) else {
            return NODEDB_ERR_UTF8;
        };
        let Some(coord) = decode_msgpack::<Vec<nodedb_array::types::coord::value::CoordValue>>(
            coord_msgpack,
            coord_len,
        ) else {
            return NODEDB_ERR_FAILED;
        };

        let system_from_ms = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_millis() as i64)
            .unwrap_or(0);

        match h
            .rt
            .block_on(h.db.array_delete_cell(name_str, coord, system_from_ms))
        {
            Ok(()) => NODEDB_OK,
            Err(_) => NODEDB_ERR_FAILED,
        }
    })
}

/// GDPR erasure: permanently remove cell content at `coord`.
///
/// `coord_msgpack` — zerompk-encoded `Vec<CoordValue>`.
///
/// After this call `ndb_array_read_coord` returns empty for the coordinate
/// at any system time >= the erasure system timestamp.
///
/// # Safety
/// All pointer parameters must be valid. `name` must be a null-terminated UTF-8 string.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn ndb_array_gdpr_erase_cell(
    handle: *mut NodeDbHandle,
    name: *const c_char,
    coord_msgpack: *const u8,
    coord_len: usize,
) -> i32 {
    ffi_guard(NODEDB_ERR_FAILED, || {
        let Some(h) = handle_ref(handle) else {
            return NODEDB_ERR_NULL;
        };
        let Some(name_str) = ptr_to_str(name) else {
            return NODEDB_ERR_UTF8;
        };
        let Some(coord) = decode_msgpack::<Vec<nodedb_array::types::coord::value::CoordValue>>(
            coord_msgpack,
            coord_len,
        ) else {
            return NODEDB_ERR_FAILED;
        };

        let system_from_ms = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_millis() as i64)
            .unwrap_or(0);

        match h
            .rt
            .block_on(h.db.array_gdpr_erase_cell(name_str, coord, system_from_ms))
        {
            Ok(()) => NODEDB_OK,
            Err(_) => NODEDB_ERR_FAILED,
        }
    })
}
