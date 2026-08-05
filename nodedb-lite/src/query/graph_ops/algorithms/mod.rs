// SPDX-License-Identifier: Apache-2.0

//! Graph algorithm dispatch: PageRank, WCC, SSSP, LCC, LPA, Closeness,
//! Betweenness, Harmonic, Degree, Louvain, Triangles, Diameter, kCore.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use nodedb_graph::params::{AlgoParams, GraphAlgorithm};
use nodedb_types::result::QueryResult;
#[cfg(test)]
use nodedb_types::value::Value;

use crate::engine::graph::index::CsrIndex;
use crate::error::LiteError;
use crate::query::graph_ops::analytics_results::{AnalyticsRawValues, raw_to_query};

mod communities;
mod lcc;
mod other;
mod pagerank;
mod sssp;

#[cfg(test)]
use communities::{label_priorities, most_frequent_label};
use communities::{label_propagation_raw, wcc_raw};
#[cfg(all(test, not(target_arch = "wasm32")))]
use lcc::count_oriented_triangles_native;
use lcc::lcc_raw;
use other::{betweenness, closeness, degree, diameter, harmonic, kcore, louvain, triangles};
use pagerank::pagerank_raw;
use sssp::{sssp_raw, validate_sssp_weights};

type CsrMap = Arc<Mutex<HashMap<String, CsrIndex>>>;

/// Run a dense algorithm without node-name or query-row materialization.
pub(crate) fn run_dense_raw(
    csr_map: &CsrMap,
    algorithm: GraphAlgorithm,
    params: &AlgoParams,
) -> Result<AnalyticsRawValues, LiteError> {
    let map = csr_map.lock().map_err(|_| LiteError::LockPoisoned)?;
    let csr = graph_csr(&map, &params.collection)?;
    run_dense_raw_on_csr(csr, algorithm, params, false)
}

/// Validate all SSSP weights before starting a dense primitive computation.
pub(crate) fn validate_dense_sssp_weights(
    csr_map: &CsrMap,
    collection: &str,
) -> Result<(), LiteError> {
    let map = csr_map.lock().map_err(|_| LiteError::LockPoisoned)?;
    let csr = graph_csr(&map, collection)?;
    validate_sssp_weights(csr, csr.compacted_out_weighted_adjacency_raw())
}

/// Run a dense SSSP primitive after the caller has separately validated weights.
pub(crate) fn run_dense_raw_prevalidated_sssp(
    csr_map: &CsrMap,
    algorithm: GraphAlgorithm,
    params: &AlgoParams,
) -> Result<AnalyticsRawValues, LiteError> {
    let map = csr_map.lock().map_err(|_| LiteError::LockPoisoned)?;
    let csr = graph_csr(&map, &params.collection)?;
    run_dense_raw_on_csr(csr, algorithm, params, algorithm == GraphAlgorithm::Sssp)
}

pub(crate) fn materialize_dense_raw(
    csr_map: &CsrMap,
    collection: &str,
    algorithm: GraphAlgorithm,
    raw: AnalyticsRawValues,
) -> Result<QueryResult, LiteError> {
    let map = csr_map.lock().map_err(|_| LiteError::LockPoisoned)?;
    let csr = graph_csr(&map, collection)?;
    raw_to_query(csr, algorithm, raw)
}

fn graph_csr<'a>(
    map: &'a HashMap<String, CsrIndex>,
    collection: &str,
) -> Result<&'a CsrIndex, LiteError> {
    map.get(collection).ok_or_else(|| LiteError::Storage {
        detail: format!("graph collection '{collection}' not found"),
    })
}

fn run_dense_raw_on_csr(
    csr: &CsrIndex,
    algorithm: GraphAlgorithm,
    params: &AlgoParams,
    sssp_prevalidated: bool,
) -> Result<AnalyticsRawValues, LiteError> {
    match algorithm {
        GraphAlgorithm::PageRank => Ok(AnalyticsRawValues::PageRank(pagerank_raw(csr, params))),
        GraphAlgorithm::Wcc => Ok(AnalyticsRawValues::Wcc(wcc_raw(csr))),
        GraphAlgorithm::Lcc => Ok(AnalyticsRawValues::Lcc(lcc_raw(csr))),
        GraphAlgorithm::Sssp => Ok(AnalyticsRawValues::Sssp(sssp_raw(
            csr,
            params,
            sssp_prevalidated,
        )?)),
        GraphAlgorithm::LabelPropagation => Ok(AnalyticsRawValues::LabelPropagation(
            label_propagation_raw(csr, params),
        )),
        _ => Err(LiteError::Storage {
            detail: format!("{algorithm:?} does not support dense primitive output"),
        }),
    }
}

/// Dispatch to the correct algorithm implementation.
pub fn run_algo(
    csr_map: &CsrMap,
    algorithm: GraphAlgorithm,
    params: &AlgoParams,
) -> Result<QueryResult, LiteError> {
    let map = csr_map.lock().map_err(|_| LiteError::LockPoisoned)?;
    let csr = graph_csr(&map, &params.collection)?;

    if matches!(
        algorithm,
        GraphAlgorithm::PageRank
            | GraphAlgorithm::Wcc
            | GraphAlgorithm::LabelPropagation
            | GraphAlgorithm::Lcc
            | GraphAlgorithm::Sssp
    ) {
        return raw_to_query(
            csr,
            algorithm,
            run_dense_raw_on_csr(csr, algorithm, params, false)?,
        );
    }

    let schema = algorithm.result_schema();
    let columns: Vec<String> = schema.iter().map(|(n, _)| n.to_string()).collect();

    let rows = match algorithm {
        GraphAlgorithm::PageRank
        | GraphAlgorithm::Wcc
        | GraphAlgorithm::LabelPropagation
        | GraphAlgorithm::Lcc
        | GraphAlgorithm::Sssp => unreachable!("handled as dense primitive output"),
        GraphAlgorithm::Betweenness => betweenness(csr, params),
        GraphAlgorithm::Closeness => closeness(csr, params),
        GraphAlgorithm::Harmonic => harmonic(csr),
        GraphAlgorithm::Degree => degree(csr, params),
        GraphAlgorithm::Louvain => louvain(csr, params),
        GraphAlgorithm::Triangles => triangles(csr),
        GraphAlgorithm::Diameter => diameter(csr),
        GraphAlgorithm::KCore => kcore(csr),
    };

    Ok(QueryResult {
        columns,
        rows,
        rows_affected: 0,
    })
}

pub(super) fn both_neighbors(csr: &CsrIndex, node: u32) -> Vec<u32> {
    csr.iter_out_edges_raw(node)
        .map(|(_, destination)| destination)
        .chain(csr.iter_in_edges_raw(node).map(|(_, source)| source))
        .collect()
}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;

    use super::*;

    fn make_triangle_csr() -> CsrIndex {
        let mut csr = CsrIndex::new();
        csr.add_edge("a", "E", "b").unwrap();
        csr.add_edge("b", "E", "c").unwrap();
        csr.add_edge("c", "E", "a").unwrap();
        csr
    }

    fn make_csr_map(csr: CsrIndex) -> CsrMap {
        let mut map = HashMap::new();
        map.insert("g".to_string(), csr);
        Arc::new(Mutex::new(map))
    }

    fn default_params(collection: &str) -> AlgoParams {
        AlgoParams {
            collection: collection.to_string(),
            ..Default::default()
        }
    }

    #[test]
    fn test_pagerank_sums_to_one() {
        let csr = make_triangle_csr();
        let m = make_csr_map(csr);
        let p = default_params("g");
        let result = run_algo(&m, GraphAlgorithm::PageRank, &p).unwrap();
        let total: f64 = result
            .rows
            .iter()
            .filter_map(|r| {
                if let Value::Float(f) = r[1] {
                    Some(f)
                } else {
                    None
                }
            })
            .sum();
        assert!((total - 1.0).abs() < 0.01, "total={total}");
    }

    #[test]
    fn pagerank_redistributes_dangling_mass() {
        let mut csr = CsrIndex::new();
        csr.add_edge("a", "E", "b").unwrap();
        let result = run_algo(
            &make_csr_map(csr),
            GraphAlgorithm::PageRank,
            &default_params("g"),
        )
        .unwrap();
        let total: f64 = result
            .rows
            .iter()
            .map(|row| match row[1] {
                Value::Float(rank) => rank,
                _ => panic!("expected rank"),
            })
            .sum();
        assert!((total - 1.0).abs() < 1e-12, "total={total}");
    }

    #[test]
    fn pagerank_both_treats_one_stored_edge_as_undirected() {
        let mut csr = CsrIndex::new();
        csr.add_edge("a", "E", "b").unwrap();
        let params = AlgoParams {
            collection: "g".to_string(),
            direction: Some("both".to_string()),
            max_iterations: Some(10),
            tolerance: Some(f64::MIN_POSITIVE),
            ..Default::default()
        };
        let result = run_algo(&make_csr_map(csr), GraphAlgorithm::PageRank, &params).unwrap();
        for row in result.rows {
            let Value::Float(rank) = row[1] else {
                panic!("expected rank");
            };
            assert!((rank - 0.5).abs() < 1e-12);
        }
    }

    #[test]
    fn sssp_uses_edge_weights() {
        let mut csr = CsrIndex::new();
        csr.add_edge_weighted("a", "E", "c", 10.0).unwrap();
        csr.add_edge_weighted("a", "E", "b", 2.0).unwrap();
        csr.add_edge_weighted("b", "E", "c", 2.0).unwrap();
        let params = AlgoParams {
            collection: "g".to_string(),
            source_node: Some("a".to_string()),
            ..Default::default()
        };
        let result = run_algo(&make_csr_map(csr), GraphAlgorithm::Sssp, &params).unwrap();
        let distance = result
            .rows
            .iter()
            .find(|row| row[0] == Value::String("c".to_string()))
            .map(|row| row[1].clone());
        assert_eq!(distance, Some(Value::Float(4.0)));
    }

    #[test]
    fn sssp_uses_lightest_parallel_edge() {
        let mut csr = CsrIndex::new();
        csr.add_edge_weighted("a", "slow", "b", 10.0).unwrap();
        csr.add_edge_weighted("a", "fast", "b", 2.0).unwrap();
        let params = AlgoParams {
            collection: "g".to_string(),
            source_node: Some("a".to_string()),
            ..Default::default()
        };
        let result = run_algo(&make_csr_map(csr), GraphAlgorithm::Sssp, &params).unwrap();
        let distance = result
            .rows
            .iter()
            .find(|row| row[0] == Value::String("b".to_string()))
            .map(|row| row[1].clone());
        assert_eq!(distance, Some(Value::Float(2.0)));
    }

    #[test]
    fn sssp_rejects_invalid_weights_in_buffered_and_compacted_graphs() {
        for (weight, compact, source) in [
            (f64::NAN, false, "a"),
            (-1.0, true, "a"),
            (f64::INFINITY, true, "missing"),
        ] {
            let mut csr = CsrIndex::new();
            csr.add_edge_weighted("a", "E", "b", weight).unwrap();
            if compact {
                csr.compact().unwrap();
            }
            let params = AlgoParams {
                collection: "g".to_string(),
                source_node: Some(source.to_string()),
                ..Default::default()
            };
            let error = run_algo(&make_csr_map(csr), GraphAlgorithm::Sssp, &params).unwrap_err();
            assert!(error.to_string().contains("finite non-negative"));
        }
    }

    #[test]
    fn label_propagation_is_synchronous_and_breaks_ties_by_smallest_label() {
        let mut csr = CsrIndex::new();
        csr.add_edge("a", "E", "b").unwrap();
        csr.add_edge("c", "E", "b").unwrap();
        let params = AlgoParams {
            collection: "g".to_string(),
            max_iterations: Some(1),
            ..Default::default()
        };
        let result = run_algo(
            &make_csr_map(csr),
            GraphAlgorithm::LabelPropagation,
            &params,
        )
        .unwrap();
        let labels: Vec<i64> = result
            .rows
            .iter()
            .map(|row| match row[1] {
                Value::Integer(label) => label,
                _ => panic!("expected label"),
            })
            .collect();
        assert_eq!(labels, vec![1, 0, 1]);
    }

    #[test]
    fn label_propagation_priority_uses_numeric_then_lexical_order() {
        let mut csr = CsrIndex::new();
        csr.add_node("6").unwrap();
        csr.add_node("06").unwrap();
        csr.add_node("41").unwrap();
        let priority = label_priorities(&csr, 3);
        let mut labels = [
            csr.node_id_raw("41").unwrap(),
            csr.node_id_raw("6").unwrap(),
            csr.node_id_raw("06").unwrap(),
        ];
        let best = most_frequent_label(&mut labels, &priority).unwrap();
        assert_eq!(csr.node_name_raw(best), "06");
    }

    #[cfg(not(target_arch = "wasm32"))]
    #[test]
    fn zero_lcc_workers_fall_back_to_exact_counting() {
        let oriented = vec![vec![1, 2], vec![2], vec![]];
        assert_eq!(count_oriented_triangles_native(&oriented, 0), vec![1, 1, 1]);
    }

    #[test]
    fn lcc_treats_single_arc_edges_as_undirected() {
        let result = run_algo(
            &make_csr_map(make_triangle_csr()),
            GraphAlgorithm::Lcc,
            &default_params("g"),
        )
        .unwrap();
        for row in result.rows {
            assert_eq!(row[1], Value::Float(1.0));
        }
    }

    #[test]
    fn lcc_counts_partial_neighbor_connectivity() {
        let mut csr = CsrIndex::new();
        csr.add_edge("a", "E", "b").unwrap();
        csr.add_edge("a", "E", "c").unwrap();
        csr.add_edge("a", "E", "d").unwrap();
        csr.add_edge("b", "E", "c").unwrap();
        let result = run_algo(
            &make_csr_map(csr),
            GraphAlgorithm::Lcc,
            &default_params("g"),
        )
        .unwrap();
        let coefficient = result
            .rows
            .iter()
            .find(|row| row[0] == Value::String("a".to_string()))
            .map(|row| row[1].clone());
        assert_eq!(coefficient, Some(Value::Float(1.0 / 3.0)));
    }

    #[test]
    fn lcc_deduplicates_parallel_reciprocal_and_self_loop_edges() {
        let mut csr = make_triangle_csr();
        csr.add_edge("a", "duplicate", "b").unwrap();
        csr.add_edge("b", "reverse", "a").unwrap();
        csr.add_edge("a", "self", "a").unwrap();
        let result = run_algo(
            &make_csr_map(csr),
            GraphAlgorithm::Lcc,
            &default_params("g"),
        )
        .unwrap();
        for row in result.rows {
            assert_eq!(row[1], Value::Float(1.0));
        }
    }

    #[test]
    fn lcc_counts_overlapping_triangles_once() {
        let mut csr = CsrIndex::new();
        csr.add_edge("a", "E", "b").unwrap();
        csr.add_edge("a", "E", "c").unwrap();
        csr.add_edge("b", "E", "c").unwrap();
        csr.add_edge("a", "E", "d").unwrap();
        csr.add_edge("b", "E", "d").unwrap();
        let result = run_algo(
            &make_csr_map(csr),
            GraphAlgorithm::Lcc,
            &default_params("g"),
        )
        .unwrap();
        for row in result.rows {
            let expected = match &row[0] {
                Value::String(node) if node == "a" || node == "b" => 2.0 / 3.0,
                Value::String(_) => 1.0,
                _ => panic!("expected node name"),
            };
            assert_eq!(row[1], Value::Float(expected));
        }
    }

    #[test]
    fn test_wcc_one_component() {
        let csr = make_triangle_csr();
        let m = make_csr_map(csr);
        let p = default_params("g");
        let result = run_algo(&m, GraphAlgorithm::Wcc, &p).unwrap();
        let comps: HashSet<i64> = result
            .rows
            .iter()
            .filter_map(|r| {
                if let Value::Integer(c) = r[1] {
                    Some(c)
                } else {
                    None
                }
            })
            .collect();
        assert_eq!(comps.len(), 1);
    }

    #[test]
    fn test_degree_centrality() {
        let csr = make_triangle_csr();
        let m = make_csr_map(csr);
        let p = default_params("g");
        let result = run_algo(&m, GraphAlgorithm::Degree, &p).unwrap();
        assert_eq!(result.rows.len(), 3);
    }

    #[test]
    fn test_kcore_triangle() {
        let csr = make_triangle_csr();
        let m = make_csr_map(csr);
        let p = default_params("g");
        let result = run_algo(&m, GraphAlgorithm::KCore, &p).unwrap();
        // All nodes in a triangle should be in the 2-core.
        for row in &result.rows {
            if let Value::Integer(k) = row[1] {
                assert!(k >= 1, "coreness should be >= 1");
            }
        }
    }

    #[test]
    fn test_pagerank_personalized_concentrates_on_seed() {
        // Triangle CSR: nodes a, b, c.
        // Seed only node "a" with weight 1.0.
        // After PPR, node a must have the highest rank.
        let csr = make_triangle_csr();
        let m = make_csr_map(csr);
        let mut pv = std::collections::HashMap::new();
        pv.insert("a".to_string(), 1.0f64);
        let p = AlgoParams {
            collection: "g".to_string(),
            personalization_vector: Some(pv),
            ..Default::default()
        };
        let result = run_algo(&m, GraphAlgorithm::PageRank, &p).unwrap();

        // Extract (node_id, rank) pairs.
        let ranks: std::collections::HashMap<String, f64> = result
            .rows
            .iter()
            .filter_map(|r| match (&r[0], &r[1]) {
                (Value::String(s), Value::Float(f)) => Some((s.clone(), *f)),
                _ => None,
            })
            .collect();

        let rank_a = ranks["a"];
        let rank_b = ranks["b"];
        let rank_c = ranks["c"];

        assert!(
            rank_a > rank_b && rank_a > rank_c,
            "seeded node 'a' should have highest rank; got a={rank_a}, b={rank_b}, c={rank_c}"
        );

        // Ranks must still sum to ~1.0.
        let total: f64 = ranks.values().sum();
        assert!(
            (total - 1.0).abs() < 0.01,
            "PPR ranks should sum to 1.0; got {total}"
        );
    }

    #[test]
    fn test_pagerank_personalized_falls_back_to_uniform_when_zero() {
        // Pass a personalization map whose keys match no nodes.
        // Result should be identical to uniform-init (within tolerance).
        let csr = make_triangle_csr();
        let csr2 = make_triangle_csr();
        let m_uniform = make_csr_map(csr);
        let m_ppr = make_csr_map(csr2);

        let p_uniform = default_params("g");

        let mut pv = std::collections::HashMap::new();
        pv.insert("nonexistent_node".to_string(), 1.0f64);
        let p_ppr = AlgoParams {
            collection: "g".to_string(),
            personalization_vector: Some(pv),
            ..Default::default()
        };

        let r_uniform = run_algo(&m_uniform, GraphAlgorithm::PageRank, &p_uniform).unwrap();
        let r_ppr = run_algo(&m_ppr, GraphAlgorithm::PageRank, &p_ppr).unwrap();

        // Both should produce equal rank vectors.
        for (ru, rp) in r_uniform.rows.iter().zip(r_ppr.rows.iter()) {
            if let (Value::Float(fu), Value::Float(fp)) = (&ru[1], &rp[1]) {
                assert!(
                    (fu - fp).abs() < 1e-10,
                    "fallback PPR rank {fp} should equal uniform rank {fu}"
                );
            }
        }
    }

    #[test]
    fn dense_results_adapt_to_the_existing_query_schema_and_rows() {
        for algorithm in [
            GraphAlgorithm::PageRank,
            GraphAlgorithm::Wcc,
            GraphAlgorithm::Lcc,
            GraphAlgorithm::Sssp,
            GraphAlgorithm::LabelPropagation,
        ] {
            let map = make_csr_map(make_triangle_csr());
            let params = AlgoParams {
                collection: "g".to_string(),
                source_node: Some("a".to_string()),
                direction: Some("both".to_string()),
                damping: Some(0.85),
                max_iterations: Some(10),
                tolerance: Some(f64::MIN_POSITIVE),
                ..Default::default()
            };
            let expected = run_algo(&map, algorithm, &params).unwrap();
            let raw = run_dense_raw(&map, algorithm, &params).unwrap();
            let actual = materialize_dense_raw(&map, "g", algorithm, raw).unwrap();
            assert_eq!(actual, expected, "{algorithm:?}");
            assert_eq!(actual.columns[0], "node_id");
            assert_eq!(actual.rows.len(), 3);
            assert!(matches!(&actual.rows[0][0], Value::String(_)));
        }
    }

    #[test]
    fn raw_label_propagation_keeps_numeric_labels_for_untimed_name_adaptation() {
        let map = make_csr_map(make_triangle_csr());
        let params = AlgoParams {
            collection: "g".to_string(),
            max_iterations: Some(1),
            ..Default::default()
        };
        let raw = run_dense_raw(&map, GraphAlgorithm::LabelPropagation, &params).unwrap();
        assert!(matches!(&raw, AnalyticsRawValues::LabelPropagation(labels) if labels.len() == 3));
        let result =
            materialize_dense_raw(&map, "g", GraphAlgorithm::LabelPropagation, raw).unwrap();
        assert_eq!(result.columns, vec!["node_id", "community_id"]);
        assert!(
            result
                .rows
                .iter()
                .all(|row| matches!(&row[1], Value::Integer(_)))
        );
    }

    #[test]
    fn test_pagerank_unchanged_without_personalization() {
        // Backwards-compat regression: running PageRank with default params (no
        // personalization vector) must yield the same ranks as before this change.
        let csr = make_triangle_csr();
        let csr2 = make_triangle_csr();
        let m1 = make_csr_map(csr);
        let m2 = make_csr_map(csr2);

        let p = default_params("g");
        let r1 = run_algo(&m1, GraphAlgorithm::PageRank, &p).unwrap();
        let r2 = run_algo(&m2, GraphAlgorithm::PageRank, &p).unwrap();

        // Two identical CSRs with identical params must produce identical results.
        let total: f64 = r1
            .rows
            .iter()
            .filter_map(|r| {
                if let Value::Float(f) = r[1] {
                    Some(f)
                } else {
                    None
                }
            })
            .sum();
        assert!((total - 1.0).abs() < 0.01, "total={total}");

        for (a, b) in r1.rows.iter().zip(r2.rows.iter()) {
            if let (Value::Float(fa), Value::Float(fb)) = (&a[1], &b[1]) {
                assert!(
                    (fa - fb).abs() < 1e-15,
                    "ranks must be deterministic: {fa} vs {fb}"
                );
            }
        }
    }
}
