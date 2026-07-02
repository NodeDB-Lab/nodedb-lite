//! Graph engine FFI functions.

use std::os::raw::c_char;
use std::str::FromStr as _;

use nodedb_client::NodeDb;

use crate::{
    NODEDB_ERR_FAILED, NODEDB_ERR_NULL, NODEDB_ERR_UTF8, NODEDB_OK, NodeDbHandle, ffi_guard,
    handle_ref, ptr_to_str, write_c_string,
};

/// Insert a directed graph edge into `collection`.
///
/// # Safety
/// All pointer parameters must be valid null-terminated UTF-8.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn nodedb_graph_insert_edge(
    handle: *mut NodeDbHandle,
    collection: *const c_char,
    from: *const c_char,
    to: *const c_char,
    edge_type: *const c_char,
) -> i32 {
    ffi_guard(NODEDB_ERR_FAILED, || {
        let Some(h) = handle_ref(handle) else {
            return NODEDB_ERR_NULL;
        };
        let Some(collection) = ptr_to_str(collection) else {
            return NODEDB_ERR_UTF8;
        };
        let Some(from) = ptr_to_str(from) else {
            return NODEDB_ERR_UTF8;
        };
        let Some(to) = ptr_to_str(to) else {
            return NODEDB_ERR_UTF8;
        };
        let Some(edge_type) = ptr_to_str(edge_type) else {
            return NODEDB_ERR_UTF8;
        };

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
            .block_on(h.db.graph_insert_edge(collection, &from_id, &to_id, edge_type, None))
        {
            Ok(_) => NODEDB_OK,
            Err(_) => NODEDB_ERR_FAILED,
        }
    })
}

/// Delete a graph edge by ID from `collection`.
///
/// Edge ID format: length-prefixed form as returned by `graph_insert_edge`
/// Display (`"{src_len}:{src}|{label_len}:{label}|{dst_len}:{dst}|{seq}"`).
///
/// # Safety
/// All pointer parameters must be valid null-terminated UTF-8.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn nodedb_graph_delete_edge(
    handle: *mut NodeDbHandle,
    collection: *const c_char,
    edge_id: *const c_char,
) -> i32 {
    ffi_guard(NODEDB_ERR_FAILED, || {
        let Some(h) = handle_ref(handle) else {
            return NODEDB_ERR_NULL;
        };
        let Some(collection) = ptr_to_str(collection) else {
            return NODEDB_ERR_UTF8;
        };
        let Some(edge_id_str) = ptr_to_str(edge_id) else {
            return NODEDB_ERR_UTF8;
        };

        let eid = match nodedb_types::id::EdgeId::from_str(edge_id_str) {
            Ok(id) => id,
            Err(_) => return NODEDB_ERR_FAILED,
        };
        match h.rt.block_on(h.db.graph_delete_edge(collection, &eid)) {
            Ok(()) => NODEDB_OK,
            Err(_) => NODEDB_ERR_FAILED,
        }
    })
}

/// Traverse the graph from a start node in `collection`. Results written as JSON to `out_json`.
///
/// `*out_json` is only written on success. The caller must free via `nodedb_free_string`.
///
/// # Safety
/// All pointer parameters must be valid UTF-8. `out_json` must not be null.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn nodedb_graph_traverse(
    handle: *mut NodeDbHandle,
    collection: *const c_char,
    start: *const c_char,
    depth: u8,
    out_json: *mut *mut c_char,
) -> i32 {
    ffi_guard(NODEDB_ERR_FAILED, || {
        let Some(h) = handle_ref(handle) else {
            return NODEDB_ERR_NULL;
        };
        let Some(collection) = ptr_to_str(collection) else {
            return NODEDB_ERR_UTF8;
        };
        let Some(start) = ptr_to_str(start) else {
            return NODEDB_ERR_UTF8;
        };
        if out_json.is_null() {
            return NODEDB_ERR_NULL;
        }

        let start_id = match nodedb_types::id::NodeId::try_new(start) {
            Ok(id) => id,
            Err(_) => return NODEDB_ERR_FAILED,
        };

        match h
            .rt
            .block_on(h.db.graph_traverse(collection, &start_id, depth, None))
        {
            Ok(subgraph) => {
                let json = serde_json::json!({
                    "nodes": subgraph.nodes.iter().map(|n| serde_json::json!({
                        "id": n.id.as_str(),
                        "depth": n.depth,
                    })).collect::<Vec<_>>(),
                    "edges": subgraph.edges.iter().map(|e| serde_json::json!({
                        "from": e.from.as_str(),
                        "to": e.to.as_str(),
                        "label": e.label,
                    })).collect::<Vec<_>>(),
                });
                let json_str = serde_json::to_string(&json).unwrap_or_else(|_| "{}".into());
                unsafe { write_c_string(out_json, json_str) }
            }
            Err(_) => NODEDB_ERR_FAILED,
        }
    })
}

/// Find the shortest path between two nodes in `collection`. Results written as JSON to `out_json`.
///
/// Returns `NODEDB_OK` with a JSON array of node IDs, or `"null"` if no path exists.
/// `*out_json` is only written on success. The caller must free via `nodedb_free_string`.
///
/// # Safety
/// All pointer parameters must be valid. `out_json` must not be null.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn nodedb_graph_shortest_path(
    handle: *mut NodeDbHandle,
    collection: *const c_char,
    from: *const c_char,
    to: *const c_char,
    max_depth: u8,
    out_json: *mut *mut c_char,
) -> i32 {
    ffi_guard(NODEDB_ERR_FAILED, || {
        let Some(h) = handle_ref(handle) else {
            return NODEDB_ERR_NULL;
        };
        let Some(collection) = ptr_to_str(collection) else {
            return NODEDB_ERR_UTF8;
        };
        let Some(from) = ptr_to_str(from) else {
            return NODEDB_ERR_UTF8;
        };
        let Some(to) = ptr_to_str(to) else {
            return NODEDB_ERR_UTF8;
        };
        if out_json.is_null() {
            return NODEDB_ERR_NULL;
        }

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
            .block_on(h.db.graph_shortest_path(collection, &from_id, &to_id, max_depth, None))
        {
            Ok(Some(path)) => {
                let node_ids: Vec<&str> = path.iter().map(|n| n.as_str()).collect();
                let json_str = sonic_rs::to_string(&node_ids).unwrap_or_else(|_| "[]".into());
                unsafe { write_c_string(out_json, json_str) }
            }
            Ok(None) => unsafe { write_c_string(out_json, "null".to_string()) },
            Err(_) => NODEDB_ERR_FAILED,
        }
    })
}
