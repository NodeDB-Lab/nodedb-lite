//! JNI entry points for the array engine.
//!
//! Byte arrays carry zerompk-encoded payloads. The Kotlin layer
//! serialises / deserialises using the same zerompk codec.
//!
//! ## JNI local-reference ownership
//! `JByteArray::into_raw()` (and `JString::into_raw()`) returns a JNI local
//! reference as the native method's return value. Returning that raw handle
//! directly from the `extern "system"` fn is the correct, safe pattern — the
//! JVM takes ownership on return. These raw handles must NOT be stored for use
//! after the method returns; storing them as global refs would leak memory.

use jni::JNIEnv;
use jni::objects::{JByteArray, JObject, JString};
use jni::sys::{jbyteArray, jint, jlong};

use super::super::{NODEDB_ERR_FAILED, NODEDB_OK, ffi_guard};
use super::core::get_handle;

fn jbytearray_to_vec(env: &mut JNIEnv, arr: &JByteArray) -> Option<Vec<u8>> {
    let len = match env.get_array_length(arr) {
        Ok(l) => l as usize,
        Err(_) => {
            let _ = env.exception_clear();
            return None;
        }
    };
    let mut buf = vec![0i8; len];
    match env.get_byte_array_region(arr, 0, &mut buf) {
        Ok(()) => {}
        Err(_) => {
            let _ = env.exception_clear();
            return None;
        }
    }
    Some(buf.into_iter().map(|b| b as u8).collect())
}

/// Create an ND sparse array.
///
/// `schema_msgpack` — zerompk-encoded `ArraySchema`.
#[unsafe(no_mangle)]
pub extern "system" fn Java_com_nodedb_lite_NodeDbLite_nativeArrayCreate(
    mut env: JNIEnv,
    _obj: JObject,
    handle: jlong,
    name: JString,
    schema_msgpack: JByteArray,
) -> jint {
    ffi_guard(NODEDB_ERR_FAILED, || {
        let Some(h) = get_handle(handle) else {
            return NODEDB_ERR_FAILED;
        };
        let name_str: String = match env.get_string(&name) {
            Ok(s) => s.into(),
            Err(_) => {
                let _ = env.exception_clear();
                return NODEDB_ERR_FAILED;
            }
        };
        let Some(schema_bytes) = jbytearray_to_vec(&mut env, &schema_msgpack) else {
            return NODEDB_ERR_FAILED;
        };
        let schema = match zerompk::from_msgpack::<nodedb_array::schema::ArraySchema>(&schema_bytes)
        {
            Ok(s) => s,
            Err(_) => return NODEDB_ERR_FAILED,
        };

        match h.rt.block_on(h.db.create_array(&name_str, schema)) {
            Ok(()) => NODEDB_OK,
            Err(_) => NODEDB_ERR_FAILED,
        }
    })
}

/// Write a cell into array `name` at `coord`.
///
/// `coord_msgpack`   — zerompk-encoded `Vec<CoordValue>`.
/// `payload_msgpack` — zerompk-encoded `Vec<CellValue>`.
#[unsafe(no_mangle)]
pub extern "system" fn Java_com_nodedb_lite_NodeDbLite_nativeArrayPutCell(
    mut env: JNIEnv,
    _obj: JObject,
    handle: jlong,
    name: JString,
    coord_msgpack: JByteArray,
    payload_msgpack: JByteArray,
    valid_from_ms: jlong,
    valid_until_ms: jlong,
) -> jint {
    ffi_guard(NODEDB_ERR_FAILED, || {
        let Some(h) = get_handle(handle) else {
            return NODEDB_ERR_FAILED;
        };
        let name_str: String = match env.get_string(&name) {
            Ok(s) => s.into(),
            Err(_) => {
                let _ = env.exception_clear();
                return NODEDB_ERR_FAILED;
            }
        };
        let Some(coord_bytes) = jbytearray_to_vec(&mut env, &coord_msgpack) else {
            return NODEDB_ERR_FAILED;
        };
        let Some(payload_bytes) = jbytearray_to_vec(&mut env, &payload_msgpack) else {
            return NODEDB_ERR_FAILED;
        };
        let coord = match zerompk::from_msgpack::<Vec<nodedb_array::types::coord::value::CoordValue>>(
            &coord_bytes,
        ) {
            Ok(c) => c,
            Err(_) => return NODEDB_ERR_FAILED,
        };
        let attrs = match zerompk::from_msgpack::<
            Vec<nodedb_array::types::cell_value::value::CellValue>,
        >(&payload_bytes)
        {
            Ok(a) => a,
            Err(_) => return NODEDB_ERR_FAILED,
        };

        let system_from_ms = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_millis() as i64)
            .unwrap_or(0);

        match h.rt.block_on(h.db.array_put_cell(
            &name_str,
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

/// Slice query returning zerompk-encoded `Vec<CellPayload>` as a byte array.
///
/// `ranges_msgpack` — zerompk-encoded `Vec<Option<DimRange>>`.
/// `as_of_ms`       — system-time AS-OF; pass `i64::MAX` for current live snapshot.
#[unsafe(no_mangle)]
pub extern "system" fn Java_com_nodedb_lite_NodeDbLite_nativeArraySlice(
    mut env: JNIEnv,
    _obj: JObject,
    handle: jlong,
    name: JString,
    ranges_msgpack: JByteArray,
    as_of_ms: jlong,
) -> jbyteArray {
    ffi_guard(std::ptr::null_mut(), || {
        let h = match get_handle(handle) {
            Some(h) => h,
            None => return std::ptr::null_mut(),
        };
        let name_str: String = match env.get_string(&name) {
            Ok(s) => s.into(),
            Err(_) => {
                let _ = env.exception_clear();
                return std::ptr::null_mut();
            }
        };
        let Some(ranges_bytes) = jbytearray_to_vec(&mut env, &ranges_msgpack) else {
            return std::ptr::null_mut();
        };
        let ranges = match zerompk::from_msgpack::<Vec<Option<nodedb_array::query::slice::DimRange>>>(
            &ranges_bytes,
        ) {
            Ok(r) => r,
            Err(_) => return std::ptr::null_mut(),
        };

        let as_of = if as_of_ms == i64::MAX {
            None
        } else {
            Some(as_of_ms)
        };

        let cells = match h.rt.block_on(h.db.array_slice(&name_str, ranges, as_of)) {
            Ok(c) => c,
            Err(_) => return std::ptr::null_mut(),
        };
        let encoded = match zerompk::to_msgpack_vec(&cells) {
            Ok(b) => b,
            Err(_) => return std::ptr::null_mut(),
        };
        let signed: Vec<i8> = encoded.into_iter().map(|b| b as i8).collect();
        match env.new_byte_array(signed.len() as i32) {
            Ok(arr) => {
                if env.set_byte_array_region(&arr, 0, &signed).is_err() {
                    let _ = env.exception_clear();
                    return std::ptr::null_mut();
                }
                arr.into_raw()
            }
            Err(_) => {
                let _ = env.exception_clear();
                std::ptr::null_mut()
            }
        }
    })
}

/// Read a single cell payload as zerompk-encoded `CellPayload`, or null if not found.
///
/// `coord_msgpack` — zerompk-encoded `Vec<CoordValue>`.
/// `as_of_ms`      — system-time AS-OF; pass `i64::MAX` for current live snapshot.
#[unsafe(no_mangle)]
pub extern "system" fn Java_com_nodedb_lite_NodeDbLite_nativeArrayReadCoord(
    mut env: JNIEnv,
    _obj: JObject,
    handle: jlong,
    name: JString,
    coord_msgpack: JByteArray,
    as_of_ms: jlong,
) -> jbyteArray {
    ffi_guard(std::ptr::null_mut(), || {
        let h = match get_handle(handle) {
            Some(h) => h,
            None => return std::ptr::null_mut(),
        };
        let name_str: String = match env.get_string(&name) {
            Ok(s) => s.into(),
            Err(_) => {
                let _ = env.exception_clear();
                return std::ptr::null_mut();
            }
        };
        let Some(coord_bytes) = jbytearray_to_vec(&mut env, &coord_msgpack) else {
            return std::ptr::null_mut();
        };
        let coord = match zerompk::from_msgpack::<Vec<nodedb_array::types::coord::value::CoordValue>>(
            &coord_bytes,
        ) {
            Ok(c) => c,
            Err(_) => return std::ptr::null_mut(),
        };

        let as_of = if as_of_ms == i64::MAX {
            None
        } else {
            Some(as_of_ms)
        };

        let cell = match h
            .rt
            .block_on(h.db.array_read_coord(&name_str, &coord, as_of))
        {
            Ok(c) => c,
            Err(_) => return std::ptr::null_mut(),
        };
        let Some(payload) = cell else {
            return std::ptr::null_mut();
        };
        let encoded = match zerompk::to_msgpack_vec(&payload) {
            Ok(b) => b,
            Err(_) => return std::ptr::null_mut(),
        };
        let signed: Vec<i8> = encoded.into_iter().map(|b| b as i8).collect();
        match env.new_byte_array(signed.len() as i32) {
            Ok(arr) => {
                if env.set_byte_array_region(&arr, 0, &signed).is_err() {
                    let _ = env.exception_clear();
                    return std::ptr::null_mut();
                }
                arr.into_raw()
            }
            Err(_) => {
                let _ = env.exception_clear();
                std::ptr::null_mut()
            }
        }
    })
}

/// Soft-delete a cell (tombstone at current system time).
///
/// `coord_msgpack` — zerompk-encoded `Vec<CoordValue>`.
#[unsafe(no_mangle)]
pub extern "system" fn Java_com_nodedb_lite_NodeDbLite_nativeArrayDeleteCell(
    mut env: JNIEnv,
    _obj: JObject,
    handle: jlong,
    name: JString,
    coord_msgpack: JByteArray,
) -> jint {
    ffi_guard(NODEDB_ERR_FAILED, || {
        let Some(h) = get_handle(handle) else {
            return NODEDB_ERR_FAILED;
        };
        let name_str: String = match env.get_string(&name) {
            Ok(s) => s.into(),
            Err(_) => {
                let _ = env.exception_clear();
                return NODEDB_ERR_FAILED;
            }
        };
        let Some(coord_bytes) = jbytearray_to_vec(&mut env, &coord_msgpack) else {
            return NODEDB_ERR_FAILED;
        };
        let coord = match zerompk::from_msgpack::<Vec<nodedb_array::types::coord::value::CoordValue>>(
            &coord_bytes,
        ) {
            Ok(c) => c,
            Err(_) => return NODEDB_ERR_FAILED,
        };

        let system_from_ms = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_millis() as i64)
            .unwrap_or(0);

        match h
            .rt
            .block_on(h.db.array_delete_cell(&name_str, coord, system_from_ms))
        {
            Ok(()) => NODEDB_OK,
            Err(_) => NODEDB_ERR_FAILED,
        }
    })
}

/// GDPR erasure: permanently remove cell content.
///
/// `coord_msgpack` — zerompk-encoded `Vec<CoordValue>`.
#[unsafe(no_mangle)]
pub extern "system" fn Java_com_nodedb_lite_NodeDbLite_nativeArrayGdprEraseCell(
    mut env: JNIEnv,
    _obj: JObject,
    handle: jlong,
    name: JString,
    coord_msgpack: JByteArray,
) -> jint {
    ffi_guard(NODEDB_ERR_FAILED, || {
        let Some(h) = get_handle(handle) else {
            return NODEDB_ERR_FAILED;
        };
        let name_str: String = match env.get_string(&name) {
            Ok(s) => s.into(),
            Err(_) => {
                let _ = env.exception_clear();
                return NODEDB_ERR_FAILED;
            }
        };
        let Some(coord_bytes) = jbytearray_to_vec(&mut env, &coord_msgpack) else {
            return NODEDB_ERR_FAILED;
        };
        let coord = match zerompk::from_msgpack::<Vec<nodedb_array::types::coord::value::CoordValue>>(
            &coord_bytes,
        ) {
            Ok(c) => c,
            Err(_) => return NODEDB_ERR_FAILED,
        };

        let system_from_ms = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_millis() as i64)
            .unwrap_or(0);

        match h
            .rt
            .block_on(h.db.array_gdpr_erase_cell(&name_str, coord, system_from_ms))
        {
            Ok(()) => NODEDB_OK,
            Err(_) => NODEDB_ERR_FAILED,
        }
    })
}
