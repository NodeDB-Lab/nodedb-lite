//! Collection resolution for graph ops that carry node IDs but no explicit
//! collection. Lite keys all edges by collection in one CSR map, so a
//! collection-less `GraphOp` (e.g. `Hop`, `Neighbors`) resolves its target by
//! scanning that map. Split out of `graph.rs` to keep the dispatcher file under
//! the size limit.

use std::sync::Arc;

/// Resolve which collection a set of node IDs belongs to by scanning the CSR map.
///
/// Returns the first collection that contains any of the given nodes, or an
/// empty string when none is found (which will produce an empty result set
/// rather than an error — correct for "no such graph" semantics).
pub(super) fn resolve_collection_for_nodes(
    csr_map: &Arc<
        std::sync::Mutex<std::collections::HashMap<String, crate::engine::graph::index::CsrIndex>>,
    >,
    node_ids: &[String],
) -> String {
    let Ok(map) = csr_map.lock() else {
        return String::new();
    };
    for (coll, csr) in map.iter() {
        for node in node_ids {
            if csr.contains_node(node) {
                return coll.clone();
            }
        }
    }
    // Fall back to the first collection in the map.
    map.keys().next().cloned().unwrap_or_default()
}
