//! Document engine FFI functions.

use std::os::raw::c_char;

use nodedb_client::NodeDb;

use crate::{
    NODEDB_ERR_FAILED, NODEDB_ERR_NOT_FOUND, NODEDB_ERR_NULL, NODEDB_ERR_UTF8, NODEDB_OK,
    NodeDbHandle, handle_ref, ptr_to_str, write_c_string,
};

/// Get a document by ID. Result written as JSON to `out_json`.
///
/// Returns `NODEDB_ERR_NOT_FOUND` if the document doesn't exist.
/// `*out_json` is only written on success (`NODEDB_OK`). The caller must
/// free the returned string via `nodedb_free_string`.
///
/// # Safety
/// All pointer parameters must be valid. `out_json` must not be null.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn nodedb_document_get(
    handle: *mut NodeDbHandle,
    collection: *const c_char,
    id: *const c_char,
    out_json: *mut *mut c_char,
) -> i32 {
    let Some(h) = handle_ref(handle) else {
        return NODEDB_ERR_NULL;
    };
    let Some(collection) = ptr_to_str(collection) else {
        return NODEDB_ERR_UTF8;
    };
    let Some(id) = ptr_to_str(id) else {
        return NODEDB_ERR_UTF8;
    };
    if out_json.is_null() {
        return NODEDB_ERR_NULL;
    }

    match h.rt.block_on(h.db.document_get(collection, id)) {
        Ok(Some(doc)) => {
            let json_str = sonic_rs::to_string(&doc).unwrap_or_else(|_| "{}".into());
            unsafe { write_c_string(out_json, json_str) }
        }
        Ok(None) => NODEDB_ERR_NOT_FOUND,
        Err(_) => NODEDB_ERR_FAILED,
    }
}

/// Put (insert or update) a document. Body is a JSON string.
///
/// If the JSON has no `"id"` field or it is empty, a UUIDv7 is auto-generated.
/// On success, if `out_id` is non-null, the assigned document ID is written to
/// `*out_id`. The caller must free it via `nodedb_free_string`.
///
/// # Safety
/// All pointer parameters must be valid. `out_id` may be null (ID not returned).
#[unsafe(no_mangle)]
pub unsafe extern "C" fn nodedb_document_put(
    handle: *mut NodeDbHandle,
    collection: *const c_char,
    json_body: *const c_char,
    out_id: *mut *mut c_char,
) -> i32 {
    let Some(h) = handle_ref(handle) else {
        return NODEDB_ERR_NULL;
    };
    let Some(collection) = ptr_to_str(collection) else {
        return NODEDB_ERR_UTF8;
    };
    let Some(json_str) = ptr_to_str(json_body) else {
        return NODEDB_ERR_UTF8;
    };

    let mut doc: nodedb_types::Document = match sonic_rs::from_str(json_str) {
        Ok(d) => d,
        Err(_) => return NODEDB_ERR_FAILED,
    };

    if doc.id.is_empty() {
        doc.id = nodedb_types::id_gen::uuid_v7();
    }

    match h.rt.block_on(h.db.document_put(collection, doc.clone())) {
        Ok(()) => {
            if !out_id.is_null() {
                // write_c_string failure is non-fatal here: the put succeeded.
                let _ = unsafe { write_c_string(out_id, doc.id) };
            }
            NODEDB_OK
        }
        Err(_) => NODEDB_ERR_FAILED,
    }
}

/// Delete a document by ID.
///
/// # Safety
/// All pointer parameters must be valid.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn nodedb_document_delete(
    handle: *mut NodeDbHandle,
    collection: *const c_char,
    id: *const c_char,
) -> i32 {
    let Some(h) = handle_ref(handle) else {
        return NODEDB_ERR_NULL;
    };
    let Some(collection) = ptr_to_str(collection) else {
        return NODEDB_ERR_UTF8;
    };
    let Some(id) = ptr_to_str(id) else {
        return NODEDB_ERR_UTF8;
    };
    match h.rt.block_on(h.db.document_delete(collection, id)) {
        Ok(()) => NODEDB_OK,
        Err(_) => NODEDB_ERR_FAILED,
    }
}

/// Full-text search (BM25). Results written as JSON array to `out_json`.
///
/// `*out_json` is only written on success. The caller must free via `nodedb_free_string`.
///
/// # Safety
/// All pointer parameters must be valid. `out_json` must not be null.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn nodedb_text_search(
    handle: *mut NodeDbHandle,
    collection: *const c_char,
    query: *const c_char,
    top_k: usize,
    out_json: *mut *mut c_char,
) -> i32 {
    let Some(h) = handle_ref(handle) else {
        return NODEDB_ERR_NULL;
    };
    let Some(collection) = ptr_to_str(collection) else {
        return NODEDB_ERR_UTF8;
    };
    let Some(query_str) = ptr_to_str(query) else {
        return NODEDB_ERR_UTF8;
    };
    if out_json.is_null() {
        return NODEDB_ERR_NULL;
    }

    match h.rt.block_on(h.db.text_search(
        collection,
        query_str,
        top_k,
        nodedb_types::TextSearchParams::default(),
    )) {
        Ok(results) => {
            let json_items: Vec<serde_json::Value> = results
                .iter()
                .map(|r| serde_json::json!({"id": r.id, "distance": r.distance}))
                .collect();
            let json_str = serde_json::to_string(&json_items).unwrap_or_else(|_| "[]".into());
            unsafe { write_c_string(out_json, json_str) }
        }
        Err(_) => NODEDB_ERR_FAILED,
    }
}

/// Execute a SQL query. Results written as JSON to `out_json`.
///
/// `*out_json` is only written on success. The caller must free via `nodedb_free_string`.
///
/// # Safety
/// All pointer parameters must be valid. `out_json` must not be null.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn nodedb_execute_sql(
    handle: *mut NodeDbHandle,
    sql: *const c_char,
    out_json: *mut *mut c_char,
) -> i32 {
    let Some(h) = handle_ref(handle) else {
        return NODEDB_ERR_NULL;
    };
    let Some(sql_str) = ptr_to_str(sql) else {
        return NODEDB_ERR_UTF8;
    };
    if out_json.is_null() {
        return NODEDB_ERR_NULL;
    }

    match h.rt.block_on(h.db.execute_sql(sql_str, &[])) {
        Ok(result) => {
            let json = serde_json::json!({
                "columns": result.columns,
                "rows": result.rows,
                "rows_affected": result.rows_affected,
            });
            let json_str = serde_json::to_string(&json).unwrap_or_else(|_| "{}".into());
            unsafe { write_c_string(out_json, json_str) }
        }
        Err(_) => NODEDB_ERR_FAILED,
    }
}
