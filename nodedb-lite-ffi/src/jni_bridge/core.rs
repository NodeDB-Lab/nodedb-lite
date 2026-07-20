//! JNI bridge — Kotlin/Android native method implementations.
//!
//! Uses jni 0.21 API (stable, widely used in Android Rust projects).
//!
//! ## JNI local-reference ownership
//! `JString::into_raw()` (and similar `JObject`-wrapping types) returns a JNI
//! local reference as the native method's return value. Returning that raw
//! handle directly from the `extern "system"` fn is the correct, safe pattern
//! — the JVM takes ownership on return. These raw handles must NOT be stored
//! for use after the method returns; promoting them to global refs would leak.

use jni::JNIEnv;
use jni::objects::JFloatArray;
use jni::objects::{JClass, JObject, JString};
use jni::sys::{jint, jlong, jstring};

use std::sync::Arc;

use super::super::{NODEDB_ERR_FAILED, NODEDB_OK, NodeDbHandle, ffi_guard};

/// Look up the handle for an opaque token returned by `nativeOpen`.
///
/// Returns a cloned `Arc` — the handle stays alive for the duration of the
/// call even if `nativeClose` is called concurrently from another thread.
/// Token 0 and unknown tokens both return `None`.
pub(super) fn get_handle(ptr: jlong) -> Option<Arc<NodeDbHandle>> {
    crate::handle_registry::get(ptr as u64)
}

#[unsafe(no_mangle)]
pub extern "system" fn Java_com_nodedb_lite_NodeDbLite_00024Companion_nativeOpen(
    mut env: JNIEnv,
    _class: JClass,
    path: JString,
    peer_id: jlong,
    passphrase: JString,
) -> jlong {
    ffi_guard(0, || {
        let path: String = match env.get_string(&path) {
            Ok(s) => s.into(),
            Err(_) => {
                let _ = env.exception_clear();
                return 0;
            }
        };
        let path_c = match std::ffi::CString::new(path) {
            Ok(c) => c,
            Err(_) => return 0,
        };

        // `passphrase` is a nullable JString. Convert to an Option<CString> so we can pass
        // a raw pointer (NULL when the JVM passed null) to the C convention in nodedb_open.
        let passphrase_cstring: Option<std::ffi::CString> = if passphrase.is_null() {
            None
        } else {
            let s: String = match env.get_string(&passphrase) {
                Ok(s) => s.into(),
                Err(_) => {
                    let _ = env.exception_clear();
                    return 0;
                }
            };
            match std::ffi::CString::new(s) {
                Ok(c) => Some(c),
                Err(_) => return 0,
            }
        };
        let passphrase_ptr = passphrase_cstring
            .as_ref()
            .map_or(std::ptr::null(), |c| c.as_ptr());

        let handle =
            unsafe { super::super::nodedb_open(path_c.as_ptr(), peer_id as u64, passphrase_ptr) };
        handle as jlong
    })
}

#[unsafe(no_mangle)]
pub extern "system" fn Java_com_nodedb_lite_NodeDbLite_nativeClose(
    _env: JNIEnv,
    _obj: JObject,
    handle: jlong,
) {
    ffi_guard((), || {
        if handle != 0 {
            unsafe { super::super::nodedb_close(handle as *mut NodeDbHandle) };
        }
    })
}

#[unsafe(no_mangle)]
pub extern "system" fn Java_com_nodedb_lite_NodeDbLite_nativeFlush(
    _env: JNIEnv,
    _obj: JObject,
    handle: jlong,
) -> jint {
    ffi_guard(NODEDB_ERR_FAILED, || {
        let Some(h) = get_handle(handle) else {
            return NODEDB_ERR_FAILED;
        };
        match h.rt.block_on(h.db.flush()) {
            Ok(()) => NODEDB_OK,
            Err(_) => NODEDB_ERR_FAILED,
        }
    })
}

/// Compact the backing store, returning the number of bytes truncated from the
/// backing file, or `-1` on error (a null handle or a compaction failure).
#[unsafe(no_mangle)]
pub extern "system" fn Java_com_nodedb_lite_NodeDbLite_nativeCompact(
    _env: JNIEnv,
    _obj: JObject,
    handle: jlong,
) -> jlong {
    ffi_guard(-1, || {
        let Some(h) = get_handle(handle) else {
            return -1;
        };
        match h.rt.block_on(h.db.compact()) {
            Ok(outcome) => i64::try_from(outcome.file_bytes_freed).unwrap_or(i64::MAX),
            Err(_) => -1,
        }
    })
}

#[unsafe(no_mangle)]
pub extern "system" fn Java_com_nodedb_lite_NodeDbLite_nativeVectorInsert(
    mut env: JNIEnv,
    _obj: JObject,
    handle: jlong,
    collection: JString,
    id: JString,
    embedding: JFloatArray,
    _dim: jint,
) -> jint {
    ffi_guard(NODEDB_ERR_FAILED, || {
        let Some(h) = get_handle(handle) else {
            return NODEDB_ERR_FAILED;
        };
        let collection: String = match env.get_string(&collection) {
            Ok(s) => s.into(),
            Err(_) => {
                let _ = env.exception_clear();
                return NODEDB_ERR_FAILED;
            }
        };
        let id: String = match env.get_string(&id) {
            Ok(s) => s.into(),
            Err(_) => {
                let _ = env.exception_clear();
                return NODEDB_ERR_FAILED;
            }
        };

        let len = match env.get_array_length(&embedding) {
            Ok(l) => l as usize,
            Err(_) => {
                let _ = env.exception_clear();
                return NODEDB_ERR_FAILED;
            }
        };
        let mut buf = vec![0.0f32; len];
        if env.get_float_array_region(&embedding, 0, &mut buf).is_err() {
            let _ = env.exception_clear();
            return NODEDB_ERR_FAILED;
        }

        use nodedb_client::NodeDb;
        match h
            .rt
            .block_on(h.db.vector_insert(&collection, &id, &buf, None))
        {
            Ok(()) => NODEDB_OK,
            Err(_) => NODEDB_ERR_FAILED,
        }
    })
}

#[unsafe(no_mangle)]
pub extern "system" fn Java_com_nodedb_lite_NodeDbLite_nativeVectorSearch(
    mut env: JNIEnv,
    _obj: JObject,
    handle: jlong,
    collection: JString,
    query: JFloatArray,
    _dim: jint,
    k: jint,
) -> jstring {
    ffi_guard(std::ptr::null_mut(), || {
        let h = match get_handle(handle) {
            Some(h) => h,
            None => return std::ptr::null_mut(),
        };
        let collection: String = match env.get_string(&collection) {
            Ok(s) => s.into(),
            Err(_) => {
                let _ = env.exception_clear();
                return std::ptr::null_mut();
            }
        };
        let len = match env.get_array_length(&query) {
            Ok(l) => l as usize,
            Err(_) => {
                let _ = env.exception_clear();
                return std::ptr::null_mut();
            }
        };
        let mut buf = vec![0.0f32; len];
        if env.get_float_array_region(&query, 0, &mut buf).is_err() {
            let _ = env.exception_clear();
            return std::ptr::null_mut();
        }

        use nodedb_client::NodeDb;
        let results =
            match h
                .rt
                .block_on(h.db.vector_search(&collection, &buf, k as usize, None, None))
            {
                Ok(r) => r,
                Err(_) => return std::ptr::null_mut(),
            };

        let json: Vec<serde_json::Value> = results
            .iter()
            .map(|r| serde_json::json!({"id": r.id, "distance": r.distance}))
            .collect();
        let json_str = serde_json::to_string(&json).unwrap_or_else(|_| "[]".into());

        match env.new_string(&json_str) {
            Ok(s) => s.into_raw(),
            Err(_) => {
                let _ = env.exception_clear();
                std::ptr::null_mut()
            }
        }
    })
}

#[unsafe(no_mangle)]
pub extern "system" fn Java_com_nodedb_lite_NodeDbLite_nativeVectorDelete(
    mut env: JNIEnv,
    _obj: JObject,
    handle: jlong,
    collection: JString,
    id: JString,
) -> jint {
    ffi_guard(NODEDB_ERR_FAILED, || {
        let Some(h) = get_handle(handle) else {
            return NODEDB_ERR_FAILED;
        };
        let collection: String = match env.get_string(&collection) {
            Ok(s) => s.into(),
            Err(_) => {
                let _ = env.exception_clear();
                return NODEDB_ERR_FAILED;
            }
        };
        let id: String = match env.get_string(&id) {
            Ok(s) => s.into(),
            Err(_) => {
                let _ = env.exception_clear();
                return NODEDB_ERR_FAILED;
            }
        };
        use nodedb_client::NodeDb;
        match h.rt.block_on(h.db.vector_delete(&collection, &id)) {
            Ok(()) => NODEDB_OK,
            Err(_) => NODEDB_ERR_FAILED,
        }
    })
}

#[unsafe(no_mangle)]
pub extern "system" fn Java_com_nodedb_lite_NodeDbLite_nativeGraphInsertEdge(
    mut env: JNIEnv,
    _obj: JObject,
    handle: jlong,
    collection: JString,
    from: JString,
    to: JString,
    edge_type: JString,
) -> jint {
    ffi_guard(NODEDB_ERR_FAILED, || {
        let Some(h) = get_handle(handle) else {
            return NODEDB_ERR_FAILED;
        };
        let collection: String = match env.get_string(&collection) {
            Ok(s) => s.into(),
            Err(_) => {
                let _ = env.exception_clear();
                return NODEDB_ERR_FAILED;
            }
        };
        let from: String = match env.get_string(&from) {
            Ok(s) => s.into(),
            Err(_) => {
                let _ = env.exception_clear();
                return NODEDB_ERR_FAILED;
            }
        };
        let to: String = match env.get_string(&to) {
            Ok(s) => s.into(),
            Err(_) => {
                let _ = env.exception_clear();
                return NODEDB_ERR_FAILED;
            }
        };
        let edge_type: String = match env.get_string(&edge_type) {
            Ok(s) => s.into(),
            Err(_) => {
                let _ = env.exception_clear();
                return NODEDB_ERR_FAILED;
            }
        };

        use nodedb_client::NodeDb;
        let from_id = match nodedb_types::id::NodeId::try_new(from) {
            Ok(id) => id,
            Err(_) => return NODEDB_ERR_FAILED,
        };
        let to_id = match nodedb_types::id::NodeId::try_new(to) {
            Ok(id) => id,
            Err(_) => return NODEDB_ERR_FAILED,
        };
        match h
            .rt
            .block_on(h.db.graph_insert_edge(&collection, &from_id, &to_id, &edge_type, None))
        {
            Ok(_) => NODEDB_OK,
            Err(_) => NODEDB_ERR_FAILED,
        }
    })
}

#[unsafe(no_mangle)]
pub extern "system" fn Java_com_nodedb_lite_NodeDbLite_nativeGraphTraverse(
    mut env: JNIEnv,
    _obj: JObject,
    handle: jlong,
    collection: JString,
    start: JString,
    depth: jint,
) -> jstring {
    ffi_guard(std::ptr::null_mut(), || {
        let h = match get_handle(handle) {
            Some(h) => h,
            None => return std::ptr::null_mut(),
        };
        let collection: String = match env.get_string(&collection) {
            Ok(s) => s.into(),
            Err(_) => {
                let _ = env.exception_clear();
                return std::ptr::null_mut();
            }
        };
        let start: String = match env.get_string(&start) {
            Ok(s) => s.into(),
            Err(_) => {
                let _ = env.exception_clear();
                return std::ptr::null_mut();
            }
        };

        use nodedb_client::NodeDb;
        let start_id = match nodedb_types::id::NodeId::try_new(start) {
            Ok(id) => id,
            Err(_) => return std::ptr::null_mut(),
        };
        let subgraph =
            match h
                .rt
                .block_on(h.db.graph_traverse(&collection, &start_id, depth as u8, None))
            {
                Ok(sg) => sg,
                Err(_) => return std::ptr::null_mut(),
            };

        let json = serde_json::json!({
            "nodes": subgraph.nodes.iter().map(|n| serde_json::json!({"id": n.id.as_str(), "depth": n.depth})).collect::<Vec<_>>(),
            "edges": subgraph.edges.iter().map(|e| serde_json::json!({"from": e.from.as_str(), "to": e.to.as_str(), "label": e.label})).collect::<Vec<_>>(),
        });
        let json_str = serde_json::to_string(&json).unwrap_or_else(|_| "{}".into());
        match env.new_string(&json_str) {
            Ok(s) => s.into_raw(),
            Err(_) => {
                let _ = env.exception_clear();
                std::ptr::null_mut()
            }
        }
    })
}
