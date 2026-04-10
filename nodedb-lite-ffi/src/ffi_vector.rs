//! Vector engine FFI functions.

use std::os::raw::c_char;

use nodedb_client::NodeDb;

use crate::{
    NODEDB_ERR_FAILED, NODEDB_ERR_NULL, NODEDB_ERR_UTF8, NODEDB_OK, NodeDbHandle, handle_ref,
    ptr_to_str, write_c_string,
};

/// Insert a vector into a collection.
///
/// # Safety
/// All pointer parameters must be valid. `embedding` must point to `dim` floats.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn nodedb_vector_insert(
    handle: *mut NodeDbHandle,
    collection: *const c_char,
    id: *const c_char,
    embedding: *const f32,
    dim: usize,
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
    if embedding.is_null() || dim == 0 {
        return NODEDB_ERR_NULL;
    }
    let emb = unsafe { std::slice::from_raw_parts(embedding, dim) };

    match h.rt.block_on(h.db.vector_insert(collection, id, emb, None)) {
        Ok(()) => NODEDB_OK,
        Err(_) => NODEDB_ERR_FAILED,
    }
}

/// Search for the k nearest vectors. Results are written as JSON to `out_json`.
///
/// `*out_json` is only written on success. The caller must free via `nodedb_free_string`.
///
/// # Safety
/// `query` must point to `dim` valid floats. `out_json` must not be null.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn nodedb_vector_search(
    handle: *mut NodeDbHandle,
    collection: *const c_char,
    query: *const f32,
    dim: usize,
    k: usize,
    out_json: *mut *mut c_char,
) -> i32 {
    let Some(h) = handle_ref(handle) else {
        return NODEDB_ERR_NULL;
    };
    let Some(collection) = ptr_to_str(collection) else {
        return NODEDB_ERR_UTF8;
    };
    if query.is_null() || dim == 0 || out_json.is_null() {
        return NODEDB_ERR_NULL;
    }
    let q = unsafe { std::slice::from_raw_parts(query, dim) };

    match h.rt.block_on(h.db.vector_search(collection, q, k, None)) {
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

/// Delete a vector by ID.
///
/// # Safety
/// All pointer parameters must be valid.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn nodedb_vector_delete(
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
    match h.rt.block_on(h.db.vector_delete(collection, id)) {
        Ok(()) => NODEDB_OK,
        Err(_) => NODEDB_ERR_FAILED,
    }
}
