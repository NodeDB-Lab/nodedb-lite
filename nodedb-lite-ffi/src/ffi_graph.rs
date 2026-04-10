//! Graph engine FFI functions.

use std::os::raw::c_char;

use nodedb_client::NodeDb;

use crate::{
    NODEDB_ERR_FAILED, NODEDB_ERR_NULL, NODEDB_ERR_UTF8, NODEDB_OK, NodeDbHandle, handle_ref,
    ptr_to_str, write_c_string,
};

/// Insert a directed graph edge.
///
/// # Safety
/// All pointer parameters must be valid null-terminated UTF-8.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn nodedb_graph_insert_edge(
    handle: *mut NodeDbHandle,
    from: *const c_char,
    to: *const c_char,
    edge_type: *const c_char,
) -> i32 {
    let Some(h) = handle_ref(handle) else {
        return NODEDB_ERR_NULL;
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

    let from_id = nodedb_types::id::NodeId::new(from);
    let to_id = nodedb_types::id::NodeId::new(to);

    match h
        .rt
        .block_on(h.db.graph_insert_edge(&from_id, &to_id, edge_type, None))
    {
        Ok(_) => NODEDB_OK,
        Err(_) => NODEDB_ERR_FAILED,
    }
}

/// Delete a graph edge by ID.
///
/// Edge ID format: "src--label-->dst" (as returned by graph_insert_edge).
///
/// # Safety
/// `edge_id` must be valid null-terminated UTF-8.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn nodedb_graph_delete_edge(
    handle: *mut NodeDbHandle,
    edge_id: *const c_char,
) -> i32 {
    let Some(h) = handle_ref(handle) else {
        return NODEDB_ERR_NULL;
    };
    let Some(edge_id_str) = ptr_to_str(edge_id) else {
        return NODEDB_ERR_UTF8;
    };

    let eid = nodedb_types::id::EdgeId::new(edge_id_str);
    match h.rt.block_on(h.db.graph_delete_edge(&eid)) {
        Ok(()) => NODEDB_OK,
        Err(_) => NODEDB_ERR_FAILED,
    }
}

/// Traverse the graph from a start node. Results written as JSON to `out_json`.
///
/// `*out_json` is only written on success. The caller must free via `nodedb_free_string`.
///
/// # Safety
/// `start` must be valid UTF-8. `out_json` must not be null.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn nodedb_graph_traverse(
    handle: *mut NodeDbHandle,
    start: *const c_char,
    depth: u8,
    out_json: *mut *mut c_char,
) -> i32 {
    let Some(h) = handle_ref(handle) else {
        return NODEDB_ERR_NULL;
    };
    let Some(start) = ptr_to_str(start) else {
        return NODEDB_ERR_UTF8;
    };
    if out_json.is_null() {
        return NODEDB_ERR_NULL;
    }

    let start_id = nodedb_types::id::NodeId::new(start);

    match h.rt.block_on(h.db.graph_traverse(&start_id, depth, None)) {
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
}

/// Find the shortest path between two nodes. Results written as JSON to `out_json`.
///
/// Returns `NODEDB_OK` with a JSON array of node IDs, or `"null"` if no path exists.
/// `*out_json` is only written on success. The caller must free via `nodedb_free_string`.
///
/// # Safety
/// All pointer parameters must be valid. `out_json` must not be null.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn nodedb_graph_shortest_path(
    handle: *mut NodeDbHandle,
    from: *const c_char,
    to: *const c_char,
    max_depth: u8,
    out_json: *mut *mut c_char,
) -> i32 {
    let Some(h) = handle_ref(handle) else {
        return NODEDB_ERR_NULL;
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

    let from_id = nodedb_types::id::NodeId::new(from);
    let to_id = nodedb_types::id::NodeId::new(to);

    match h
        .rt
        .block_on(h.db.graph_shortest_path(&from_id, &to_id, max_depth, None))
    {
        Ok(Some(path)) => {
            let node_ids: Vec<&str> = path.iter().map(|n| n.as_str()).collect();
            let json_str = sonic_rs::to_string(&node_ids).unwrap_or_else(|_| "[]".into());
            unsafe { write_c_string(out_json, json_str) }
        }
        Ok(None) => unsafe { write_c_string(out_json, "null".to_string()) },
        Err(_) => NODEDB_ERR_FAILED,
    }
}
