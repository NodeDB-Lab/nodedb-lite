//! JNI entry points for the document engine and ID generation.
//!
//! ## JNI local-reference ownership
//! `JString::into_raw()` returns a JNI local reference as the native method's
//! return value. Returning that raw handle directly from the `extern "system"`
//! fn is the correct, safe pattern — the JVM takes ownership on return. These
//! raw handles must NOT be stored for use after the method returns; promoting
//! them to global refs would leak.

use jni::JNIEnv;
use jni::objects::{JClass, JObject, JString};
use jni::sys::{jint, jlong, jstring};

use super::super::{NODEDB_ERR_FAILED, NODEDB_OK, ffi_guard};
use super::core::get_handle;

#[unsafe(no_mangle)]
pub extern "system" fn Java_com_nodedb_lite_NodeDbLite_nativeDocumentGet(
    mut env: JNIEnv,
    _obj: JObject,
    handle: jlong,
    collection: JString,
    id: JString,
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
        let id: String = match env.get_string(&id) {
            Ok(s) => s.into(),
            Err(_) => {
                let _ = env.exception_clear();
                return std::ptr::null_mut();
            }
        };

        use nodedb_client::NodeDb;
        match h.rt.block_on(h.db.document_get(&collection, &id)) {
            Ok(Some(doc)) => {
                let json_str = sonic_rs::to_string(&doc).unwrap_or_else(|_| "{}".into());
                match env.new_string(&json_str) {
                    Ok(s) => s.into_raw(),
                    Err(_) => {
                        let _ = env.exception_clear();
                        std::ptr::null_mut()
                    }
                }
            }
            _ => std::ptr::null_mut(),
        }
    })
}

#[unsafe(no_mangle)]
pub extern "system" fn Java_com_nodedb_lite_NodeDbLite_nativeDocumentPut(
    mut env: JNIEnv,
    _obj: JObject,
    handle: jlong,
    collection: JString,
    json_body: JString,
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
        let json_str: String = match env.get_string(&json_body) {
            Ok(s) => s.into(),
            Err(_) => {
                let _ = env.exception_clear();
                return NODEDB_ERR_FAILED;
            }
        };

        let mut doc: nodedb_types::Document = match sonic_rs::from_str(&json_str) {
            Ok(d) => d,
            Err(_) => return NODEDB_ERR_FAILED,
        };

        if doc.id.is_empty() {
            doc.id = nodedb_types::id_gen::uuid_v7();
        }

        use nodedb_client::NodeDb;
        match h.rt.block_on(h.db.document_put(&collection, doc)) {
            Ok(()) => NODEDB_OK,
            Err(_) => NODEDB_ERR_FAILED,
        }
    })
}

#[unsafe(no_mangle)]
pub extern "system" fn Java_com_nodedb_lite_NodeDbLite_nativeDocumentDelete(
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
        match h.rt.block_on(h.db.document_delete(&collection, &id)) {
            Ok(()) => NODEDB_OK,
            Err(_) => NODEDB_ERR_FAILED,
        }
    })
}

/// Generate a UUIDv7 (time-sortable, recommended for primary keys).
#[unsafe(no_mangle)]
pub extern "system" fn Java_com_nodedb_lite_NodeDbLite_00024Companion_nativeGenerateId(
    env: JNIEnv,
    _class: JClass,
) -> jstring {
    ffi_guard(std::ptr::null_mut(), || {
        let id = nodedb_types::id_gen::uuid_v7();
        match env.new_string(&id) {
            Ok(s) => s.into_raw(),
            Err(_) => {
                let _ = env.exception_clear();
                std::ptr::null_mut()
            }
        }
    })
}

/// Generate an ID of the specified type.
///
/// Supported types: "uuidv7", "uuidv4", "ulid", "cuid2", "nanoid".
#[unsafe(no_mangle)]
pub extern "system" fn Java_com_nodedb_lite_NodeDbLite_00024Companion_nativeGenerateIdTyped(
    mut env: JNIEnv,
    _class: JClass,
    id_type: JString,
) -> jstring {
    ffi_guard(std::ptr::null_mut(), || {
        let id_type_str: String = match env.get_string(&id_type) {
            Ok(s) => s.into(),
            Err(_) => {
                let _ = env.exception_clear();
                return std::ptr::null_mut();
            }
        };
        let id = match nodedb_types::id_gen::generate_by_type(&id_type_str) {
            Some(id) => id,
            None => return std::ptr::null_mut(),
        };
        match env.new_string(&id) {
            Ok(s) => s.into_raw(),
            Err(_) => {
                let _ = env.exception_clear();
                std::ptr::null_mut()
            }
        }
    })
}
