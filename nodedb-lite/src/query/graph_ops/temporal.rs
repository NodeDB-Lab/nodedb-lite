// SPDX-License-Identifier: Apache-2.0

//! TemporalNeighbors and TemporalAlgorithm handlers.
//!
//! Both variants operate on bitemporal edge history written by
//! `engine::graph::history`. The history key layout is:
//!
//!   `{collection}:{edge_key}:{system_from_ms_8be}`
//!
//! The value is `{props_msgpack}{system_to_ms_8be}` where
//! `system_to_ms == i64::MAX` (stored as u64::MAX big-endian) means
//! "still current".

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use nodedb_graph::params::{AlgoParams, GraphAlgorithm};
use nodedb_types::Namespace;
use nodedb_types::result::QueryResult;
use nodedb_types::value::Value;

use crate::engine::graph::index::CsrIndex;
use crate::error::LiteError;
use crate::storage::engine::{StorageEngine, StorageEngineSync};

use super::algorithms;

const TRAILER_LEN: usize = 8;

/// Decode the `system_to_ms` trailer from a raw history value.
fn decode_system_to(value: &[u8]) -> Option<i64> {
    if value.len() < TRAILER_LEN {
        return None;
    }
    let bytes: [u8; 8] = value[value.len() - TRAILER_LEN..].try_into().ok()?;
    Some(u64::from_be_bytes(bytes) as i64)
}

/// Parse an edge key of the form `{src}->{dst}:{label}` into components.
fn parse_edge_key(edge_key: &str) -> Option<(&str, &str, &str)> {
    // Format: "{src}->{dst}:{label}"
    let arrow_pos = edge_key.find("->")?;
    let src = &edge_key[..arrow_pos];
    let rest = &edge_key[arrow_pos + 2..];
    let colon_pos = rest.rfind(':')?;
    let dst = &rest[..colon_pos];
    let label = &rest[colon_pos + 1..];
    Some((src, dst, label))
}

/// Build a snapshot CSR from history at `system_as_of_ms`.
///
/// Reads all edge history for `collection` and materialises the set of
/// edges whose `system_from <= as_of < system_to`.
async fn build_temporal_snapshot<S: StorageEngine + StorageEngineSync>(
    storage: &Arc<S>,
    collection: &str,
    system_as_of_ms: i64,
) -> Result<CsrIndex, LiteError> {
    let prefix = {
        let mut p = collection.as_bytes().to_vec();
        p.push(b':');
        p
    };

    let entries = storage
        .scan_prefix(Namespace::GraphHistory, &prefix)
        .await?;

    let mut snapshot = CsrIndex::new();

    // Group by edge key (strip the collection prefix and trailing 8-byte timestamp).
    // Key layout: `{collection}:{edge_key}:{system_from_8be}`.
    // We need to find the most recent version of each edge that is visible at as_of.

    struct EdgeVersion {
        system_from: i64,
        #[allow(dead_code)]
        system_to: i64,
        src: String,
        dst: String,
        label: String,
    }

    let mut edge_map: HashMap<String, EdgeVersion> = HashMap::new();

    for (raw_key, raw_val) in &entries {
        // Raw key is bytes. Skip the collection prefix (len + 1 for ':').
        let key_after_prefix = &raw_key[collection.len() + 1..];
        if key_after_prefix.len() < TRAILER_LEN + 1 {
            continue;
        }
        let edge_key_bytes = &key_after_prefix[..key_after_prefix.len() - TRAILER_LEN];
        let from_bytes: [u8; 8] = key_after_prefix[key_after_prefix.len() - TRAILER_LEN..]
            .try_into()
            .unwrap_or([0; 8]);
        let system_from = u64::from_be_bytes(from_bytes) as i64;

        if system_from > system_as_of_ms {
            continue; // This version was written after as_of.
        }

        let system_to = decode_system_to(raw_val).unwrap_or(i64::MAX);

        // Check visibility window: system_from <= as_of AND as_of < system_to.
        if system_as_of_ms >= system_to {
            continue;
        }

        let edge_key_str = String::from_utf8_lossy(edge_key_bytes).into_owned();

        // Keep only the most recent version visible at as_of.
        let keep = edge_map
            .get(&edge_key_str)
            .is_none_or(|ev| system_from > ev.system_from);

        if keep && let Some((src, dst, label)) = parse_edge_key(&edge_key_str) {
            let ev = EdgeVersion {
                system_from,
                system_to,
                src: src.to_string(),
                dst: dst.to_string(),
                label: label.to_string(),
            };
            edge_map.insert(edge_key_str, ev);
        }
    }

    for ev in edge_map.values() {
        let _ = snapshot.add_edge(&ev.src, &ev.label, &ev.dst);
    }

    Ok(snapshot)
}

/// Handle `GraphOp::TemporalNeighbors`.
#[allow(clippy::too_many_arguments)]
pub async fn temporal_neighbors<S: StorageEngine + StorageEngineSync>(
    storage: &Arc<S>,
    csr_map: &Arc<Mutex<HashMap<String, CsrIndex>>>,
    collection: &str,
    node_id: &str,
    edge_label: Option<&str>,
    direction: nodedb_graph::Direction,
    system_as_of_ms: Option<i64>,
    valid_at_ms: Option<i64>,
) -> Result<QueryResult, LiteError> {
    // When no as_of is provided, fall back to current-state neighbors.
    if system_as_of_ms.is_none() {
        return super::traversal::neighbors(csr_map, collection, node_id, edge_label, direction);
    }

    let as_of = system_as_of_ms.unwrap();
    let snapshot = build_temporal_snapshot(storage, collection, as_of).await?;

    let nbrs = snapshot.neighbors(node_id, edge_label, direction);
    let columns = vec!["label".to_string(), "neighbor".to_string()];
    let rows = nbrs
        .into_iter()
        .map(|(lbl, nb)| vec![Value::String(lbl), Value::String(nb)])
        .collect();

    let _ = valid_at_ms; // valid-time filtering requires valid_from/valid_to columns in props;
    // properties are stored as raw msgpack in this Lite implementation.
    // When valid-time columns are added to props, filter here.

    Ok(QueryResult {
        columns,
        rows,
        rows_affected: 0,
    })
}

/// Handle `GraphOp::TemporalAlgorithm`.
pub async fn temporal_algorithm<S: StorageEngine + StorageEngineSync>(
    storage: &Arc<S>,
    csr_map: &Arc<Mutex<HashMap<String, CsrIndex>>>,
    algorithm: GraphAlgorithm,
    params: &AlgoParams,
    system_as_of_ms: Option<i64>,
) -> Result<QueryResult, LiteError> {
    // When no as_of is provided, run against current-state CSR.
    if system_as_of_ms.is_none() {
        return algorithms::run_algo(csr_map, algorithm, params);
    }

    let as_of = system_as_of_ms.unwrap();
    let snapshot = build_temporal_snapshot(storage, &params.collection, as_of).await?;

    // Wrap snapshot in a temporary map so run_algo can borrow it.
    let mut tmp_map = HashMap::new();
    tmp_map.insert(params.collection.clone(), snapshot);
    let tmp_arc = Arc::new(Mutex::new(tmp_map));
    algorithms::run_algo(&tmp_arc, algorithm, params)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_edge_key_roundtrip() {
        let (src, dst, label) = parse_edge_key("alice->bob:KNOWS").unwrap();
        assert_eq!(src, "alice");
        assert_eq!(dst, "bob");
        assert_eq!(label, "KNOWS");
    }

    #[test]
    fn parse_edge_key_no_arrow() {
        assert!(parse_edge_key("alice_bob_KNOWS").is_none());
    }

    #[test]
    fn decode_system_to_max() {
        // History stores i64::MAX cast to u64 as the "still current" sentinel.
        let sentinel = i64::MAX as u64;
        let mut val = vec![0u8; 8];
        val.extend_from_slice(&sentinel.to_be_bytes());
        assert_eq!(decode_system_to(&val), Some(i64::MAX));
    }
}
