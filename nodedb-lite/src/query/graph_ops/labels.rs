// SPDX-License-Identifier: Apache-2.0

//! SetNodeLabels and RemoveNodeLabels handlers.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use nodedb_types::result::QueryResult;

use crate::engine::graph::index::CsrIndex;
use crate::error::LiteError;

/// Handle `GraphOp::SetNodeLabels`.
pub fn set_node_labels(
    csr_map: &Arc<Mutex<HashMap<String, CsrIndex>>>,
    collection: &str,
    node_id: &str,
    labels: &[String],
) -> Result<QueryResult, LiteError> {
    let mut map = csr_map.lock().map_err(|_| LiteError::LockPoisoned)?;
    let csr = map
        .entry(collection.to_string())
        .or_insert_with(CsrIndex::new);

    for label in labels {
        csr.add_node_label(node_id, label)
            .map_err(|e| LiteError::Storage {
                detail: e.to_string(),
            })?;
    }

    Ok(QueryResult {
        columns: Vec::new(),
        rows: Vec::new(),
        rows_affected: labels.len() as u64,
    })
}

/// Handle `GraphOp::RemoveNodeLabels`.
pub fn remove_node_labels(
    csr_map: &Arc<Mutex<HashMap<String, CsrIndex>>>,
    collection: &str,
    node_id: &str,
    labels: &[String],
) -> Result<QueryResult, LiteError> {
    let mut map = csr_map.lock().map_err(|_| LiteError::LockPoisoned)?;
    if let Some(csr) = map.get_mut(collection) {
        for label in labels {
            csr.remove_node_label(node_id, label);
        }
    }

    Ok(QueryResult {
        columns: Vec::new(),
        rows: Vec::new(),
        rows_affected: labels.len() as u64,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_csr_map_with_node() -> Arc<Mutex<HashMap<String, CsrIndex>>> {
        let mut csr = CsrIndex::new();
        csr.add_edge("alice", "KNOWS", "bob").unwrap();
        let mut map = HashMap::new();
        map.insert("social".to_string(), csr);
        Arc::new(Mutex::new(map))
    }

    #[test]
    fn test_set_node_labels() {
        let m = make_csr_map_with_node();
        let labels = vec!["Person".to_string(), "Employee".to_string()];
        let r = set_node_labels(&m, "social", "alice", &labels).unwrap();
        assert_eq!(r.rows_affected, 2);

        // Verify via direct CSR lookup.
        let map = m.lock().unwrap();
        let csr = map.get("social").unwrap();
        let node_raw = csr.node_id_raw("alice").unwrap();
        let node_labels = csr.node_labels(node_raw);
        assert!(node_labels.contains(&"Person"));
        assert!(node_labels.contains(&"Employee"));
    }

    #[test]
    fn test_remove_node_labels() {
        let m = make_csr_map_with_node();

        // First set some labels.
        set_node_labels(
            &m,
            "social",
            "alice",
            &["Person".to_string(), "Admin".to_string()],
        )
        .unwrap();

        // Now remove one.
        let r = remove_node_labels(&m, "social", "alice", &["Admin".to_string()]).unwrap();
        assert_eq!(r.rows_affected, 1);

        let map = m.lock().unwrap();
        let csr = map.get("social").unwrap();
        let node_raw = csr.node_id_raw("alice").unwrap();
        let node_labels = csr.node_labels(node_raw);
        assert!(node_labels.contains(&"Person"));
        assert!(!node_labels.contains(&"Admin"));
    }
}
