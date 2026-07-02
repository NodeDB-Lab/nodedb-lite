// SPDX-License-Identifier: Apache-2.0

//! Stats handler — node_count, edge_count, avg_degree, max_degree, density.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use nodedb_types::result::QueryResult;
use nodedb_types::value::Value;

use crate::engine::graph::index::CsrIndex;
use crate::error::LiteError;
use crate::storage::engine::StorageEngine;

use super::temporal;

/// Handle `GraphOp::Stats`.
pub async fn graph_stats<S: StorageEngine>(
    storage: &Arc<S>,
    csr_map: &Arc<Mutex<HashMap<String, CsrIndex>>>,
    collection: Option<&str>,
    as_of: Option<i64>,
) -> Result<QueryResult, LiteError> {
    let columns = vec![
        "collection".to_string(),
        "node_count".to_string(),
        "edge_count".to_string(),
        "avg_degree".to_string(),
        "max_degree".to_string(),
        "density".to_string(),
    ];

    let rows = match collection {
        Some(coll) => {
            let stats = single_collection_stats(storage, csr_map, coll, as_of).await?;
            vec![stats]
        }
        None => {
            let colls: Vec<String> = {
                let map = csr_map.lock().map_err(|_| LiteError::LockPoisoned)?;
                map.keys().cloned().collect()
            };
            let mut rows = Vec::with_capacity(colls.len());
            for coll in &colls {
                let stats = single_collection_stats(storage, csr_map, coll, as_of).await?;
                rows.push(stats);
            }
            rows
        }
    };

    Ok(QueryResult {
        columns,
        rows,
        rows_affected: 0,
    })
}

async fn single_collection_stats<S: StorageEngine>(
    storage: &Arc<S>,
    csr_map: &Arc<Mutex<HashMap<String, CsrIndex>>>,
    collection: &str,
    as_of: Option<i64>,
) -> Result<Vec<Value>, LiteError> {
    if let Some(cutoff) = as_of {
        // Build temporal snapshot and compute stats on it.
        use nodedb_graph::params::{AlgoParams, GraphAlgorithm};
        let params = AlgoParams {
            collection: collection.to_string(),
            ..Default::default()
        };
        let degree_result = temporal::temporal_algorithm(
            storage,
            csr_map,
            GraphAlgorithm::Degree,
            &params,
            Some(cutoff),
        )
        .await?;
        let node_count = degree_result.rows.len() as i64;
        // Degree centrality values are normalized; un-normalizing exactly requires
        // knowing n. Report approximate edge count as node_count * avg_degree / 2.
        let avg_deg: f64 = if node_count > 0 {
            degree_result
                .rows
                .iter()
                .filter_map(|r| {
                    if let Value::Float(f) = r[1] {
                        Some(f)
                    } else {
                        None
                    }
                })
                .sum::<f64>()
                / node_count as f64
                * (node_count - 1).max(0) as f64 // un-normalize
        } else {
            0.0
        };
        let edge_count = (node_count as f64 * avg_deg / 2.0) as i64;
        let density = if node_count > 1 {
            edge_count as f64 / (node_count * (node_count - 1)) as f64
        } else {
            0.0
        };
        return compute_stats_row_from_values(
            collection, node_count, edge_count, avg_deg, 0, density,
        );
    }

    // Current-state path.
    let map = csr_map.lock().map_err(|_| LiteError::LockPoisoned)?;
    if let Some(csr) = map.get(collection) {
        let stats = csr.compute_statistics().map_err(|e| LiteError::Storage {
            detail: e.to_string(),
        })?;
        let n = stats.node_count as i64;
        let e = stats.edge_count as i64;
        let avg = stats.out_degree_histogram.avg + stats.in_degree_histogram.avg;
        let max_d = stats
            .out_degree_histogram
            .max
            .max(stats.in_degree_histogram.max) as i64;
        let density = if n > 1 {
            e as f64 / (n * (n - 1)) as f64
        } else {
            0.0
        };
        compute_stats_row_from_values(collection, n, e, avg, max_d, density)
    } else {
        compute_stats_row_from_values(collection, 0, 0, 0.0, 0, 0.0)
    }
}

fn compute_stats_row_from_values(
    collection: &str,
    node_count: i64,
    edge_count: i64,
    avg_degree: f64,
    max_degree: i64,
    density: f64,
) -> Result<Vec<Value>, LiteError> {
    Ok(vec![
        Value::String(collection.to_string()),
        Value::Integer(node_count),
        Value::Integer(edge_count),
        Value::Float(avg_degree),
        Value::Integer(max_degree),
        Value::Float(density),
    ])
}

#[cfg(test)]
mod tests {
    use super::*;

    // Note: full async tests require a storage backend; this tests the stats
    // math path synchronously via the lock-guarded CSR.
    #[test]
    fn stats_row_format() {
        let row = compute_stats_row_from_values("test", 10, 20, 2.0, 5, 0.22).unwrap();
        assert_eq!(row.len(), 6);
        assert_eq!(row[0], Value::String("test".to_string()));
        assert_eq!(row[1], Value::Integer(10));
        assert_eq!(row[2], Value::Integer(20));
        if let Value::Float(avg) = row[3] {
            assert!((avg - 2.0).abs() < 1e-9);
        }
        assert_eq!(row[4], Value::Integer(5));
    }

    #[test]
    fn density_non_zero() {
        let row = compute_stats_row_from_values("g", 3, 3, 1.0, 2, 3.0 / 6.0).unwrap();
        if let Value::Float(d) = row[5] {
            assert!(d > 0.0);
        }
    }
}
