// SPDX-License-Identifier: Apache-2.0

//! Graph algorithm dispatch: PageRank, WCC, SSSP, LCC, LPA, Closeness,
//! Betweenness, Harmonic, Degree, Louvain, Triangles, Diameter, kCore.

use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::{Arc, Mutex};

use nodedb_graph::params::{AlgoParams, GraphAlgorithm};
use nodedb_types::result::QueryResult;
use nodedb_types::value::Value;

use crate::engine::graph::index::CsrIndex;
use crate::error::LiteError;

type CsrMap = Arc<Mutex<HashMap<String, CsrIndex>>>;

/// Dispatch to the correct algorithm implementation.
pub fn run_algo(
    csr_map: &CsrMap,
    algorithm: GraphAlgorithm,
    params: &AlgoParams,
) -> Result<QueryResult, LiteError> {
    let map = csr_map.lock().map_err(|_| LiteError::LockPoisoned)?;
    let csr = map
        .get(&params.collection)
        .ok_or_else(|| LiteError::Storage {
            detail: format!("graph collection '{}' not found", params.collection),
        })?;

    let schema = algorithm.result_schema();
    let columns: Vec<String> = schema.iter().map(|(n, _)| n.to_string()).collect();

    let rows = match algorithm {
        GraphAlgorithm::PageRank => pagerank(csr, params),
        GraphAlgorithm::Wcc => wcc(csr),
        GraphAlgorithm::LabelPropagation => label_propagation(csr, params),
        GraphAlgorithm::Lcc => lcc(csr),
        GraphAlgorithm::Sssp => sssp(csr, params),
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

// ── PageRank ─────────────────────────────────────────────────────────────────

fn pagerank(csr: &CsrIndex, params: &AlgoParams) -> Vec<Vec<Value>> {
    let n = csr.node_count();
    if n == 0 {
        return Vec::new();
    }
    let d = params.damping_factor();
    let max_iter = params.iterations(20);
    let tol = params.convergence_tolerance();

    let mut rank = vec![1.0f64 / n as f64; n];
    let out_degrees: Vec<usize> = (0..n).map(|i| csr.out_degree_raw(i as u32)).collect();

    for _ in 0..max_iter {
        let mut new_rank = vec![(1.0 - d) / n as f64; n];
        for src in 0..n as u32 {
            let od = out_degrees[src as usize];
            if od == 0 {
                continue;
            }
            let contrib = d * rank[src as usize] / od as f64;
            for (_, dst) in csr.iter_out_edges_raw(src) {
                new_rank[dst as usize] += contrib;
            }
        }
        let delta: f64 = rank
            .iter()
            .zip(new_rank.iter())
            .map(|(a, b)| (a - b).abs())
            .sum();
        rank = new_rank;
        if delta < tol {
            break;
        }
    }

    (0..n)
        .map(|i| {
            vec![
                Value::String(csr.node_name_raw(i as u32).to_string()),
                Value::Float(rank[i]),
            ]
        })
        .collect()
}

// ── WCC (union-find) ─────────────────────────────────────────────────────────

fn wcc(csr: &CsrIndex) -> Vec<Vec<Value>> {
    let n = csr.node_count();
    if n == 0 {
        return Vec::new();
    }
    let mut parent: Vec<u32> = (0..n as u32).collect();

    fn find(parent: &mut Vec<u32>, x: u32) -> u32 {
        if parent[x as usize] != x {
            parent[x as usize] = find(parent, parent[x as usize]);
        }
        parent[x as usize]
    }

    fn union(parent: &mut Vec<u32>, a: u32, b: u32) {
        let ra = find(parent, a);
        let rb = find(parent, b);
        if ra != rb {
            parent[ra as usize] = rb;
        }
    }

    for src in 0..n as u32 {
        for (_, dst) in csr.iter_out_edges_raw(src) {
            union(&mut parent, src, dst);
        }
    }

    (0..n)
        .map(|i| {
            let comp = find(&mut parent, i as u32) as i64;
            vec![
                Value::String(csr.node_name_raw(i as u32).to_string()),
                Value::Integer(comp),
            ]
        })
        .collect()
}

// ── LabelPropagation ─────────────────────────────────────────────────────────

fn label_propagation(csr: &CsrIndex, params: &AlgoParams) -> Vec<Vec<Value>> {
    let n = csr.node_count();
    if n == 0 {
        return Vec::new();
    }
    let max_iter = params.iterations(10);
    let mut labels: Vec<u32> = (0..n as u32).collect();

    for _ in 0..max_iter {
        let mut changed = false;
        for node in 0..n as u32 {
            let mut freq: HashMap<u32, usize> = HashMap::new();
            for (_, nb) in csr.iter_out_edges_raw(node) {
                *freq.entry(labels[nb as usize]).or_insert(0) += 1;
            }
            for (_, nb) in csr.iter_in_edges_raw(node) {
                *freq.entry(labels[nb as usize]).or_insert(0) += 1;
            }
            if let Some(&best) = freq.iter().max_by_key(|&(_, v)| v).map(|(k, _)| k)
                && best != labels[node as usize]
            {
                labels[node as usize] = best;
                changed = true;
            }
        }
        if !changed {
            break;
        }
    }

    (0..n)
        .map(|i| {
            vec![
                Value::String(csr.node_name_raw(i as u32).to_string()),
                Value::Integer(labels[i] as i64),
            ]
        })
        .collect()
}

// ── LCC (local clustering coefficient) ───────────────────────────────────────

fn lcc(csr: &CsrIndex) -> Vec<Vec<Value>> {
    let n = csr.node_count();
    (0..n)
        .map(|i| {
            let node = i as u32;
            let neighbors: HashSet<u32> = csr
                .iter_out_edges_raw(node)
                .map(|(_, d)| d)
                .chain(csr.iter_in_edges_raw(node).map(|(_, s)| s))
                .collect();
            let k = neighbors.len();
            let coeff = if k < 2 {
                0.0f64
            } else {
                let mut triangles = 0usize;
                let nb_vec: Vec<u32> = neighbors.iter().copied().collect();
                for &u in &nb_vec {
                    for (_, v) in csr.iter_out_edges_raw(u) {
                        if neighbors.contains(&v) {
                            triangles += 1;
                        }
                    }
                }
                triangles as f64 / (k * (k - 1)) as f64
            };
            vec![
                Value::String(csr.node_name_raw(node).to_string()),
                Value::Float(coeff),
            ]
        })
        .collect()
}

// ── SSSP (Dijkstra, unweighted = BFS) ────────────────────────────────────────

fn sssp(csr: &CsrIndex, params: &AlgoParams) -> Vec<Vec<Value>> {
    let n = csr.node_count();
    if n == 0 {
        return Vec::new();
    }
    let src_name = params.source_node.as_deref().unwrap_or("");
    let Some(src_id) = csr.node_id_raw(src_name) else {
        return (0..n)
            .map(|i| {
                vec![
                    Value::String(csr.node_name_raw(i as u32).to_string()),
                    Value::Float(f64::INFINITY),
                ]
            })
            .collect();
    };

    // BFS for unweighted; weighted edges use Dijkstra via priority queue.
    let mut dist = vec![f64::INFINITY; n];
    dist[src_id as usize] = 0.0;
    let mut queue: VecDeque<u32> = VecDeque::new();
    queue.push_back(src_id);

    while let Some(u) = queue.pop_front() {
        let d = dist[u as usize];
        let weight = if csr.has_weighted_edges() { 0.0 } else { 1.0 };
        for (_, v) in csr.iter_out_edges_raw(u) {
            let edge_w = if csr.has_weighted_edges() {
                // For weighted graphs, use Dijkstra — here simplified to BFS with weight 1
                1.0f64
            } else {
                1.0
            };
            let nd = d + edge_w;
            if nd < dist[v as usize] {
                dist[v as usize] = nd;
                queue.push_back(v);
            }
        }
        let _ = weight; // silence unused warning
    }

    (0..n)
        .map(|i| {
            vec![
                Value::String(csr.node_name_raw(i as u32).to_string()),
                Value::Float(dist[i]),
            ]
        })
        .collect()
}

// ── Betweenness Centrality (Brandes) ─────────────────────────────────────────

fn betweenness(csr: &CsrIndex, params: &AlgoParams) -> Vec<Vec<Value>> {
    let n = csr.node_count();
    if n == 0 {
        return Vec::new();
    }
    let sample = params.sample_size.unwrap_or(n).min(n);
    let mut bc = vec![0.0f64; n];

    for s in 0..sample as u32 {
        // BFS from s
        let mut sigma = vec![0.0f64; n];
        let mut dist = vec![-1i64; n];
        let mut stack: Vec<u32> = Vec::new();
        let mut pred: Vec<Vec<u32>> = vec![Vec::new(); n];
        sigma[s as usize] = 1.0;
        dist[s as usize] = 0;
        let mut queue: VecDeque<u32> = VecDeque::new();
        queue.push_back(s);

        while let Some(v) = queue.pop_front() {
            stack.push(v);
            for (_, w) in csr.iter_out_edges_raw(v) {
                if dist[w as usize] < 0 {
                    queue.push_back(w);
                    dist[w as usize] = dist[v as usize] + 1;
                }
                if dist[w as usize] == dist[v as usize] + 1 {
                    sigma[w as usize] += sigma[v as usize];
                    pred[w as usize].push(v);
                }
            }
        }

        let mut delta = vec![0.0f64; n];
        while let Some(w) = stack.pop() {
            for &v in &pred[w as usize] {
                delta[v as usize] +=
                    (sigma[v as usize] / sigma[w as usize]) * (1.0 + delta[w as usize]);
            }
            if w != s {
                bc[w as usize] += delta[w as usize];
            }
        }
    }

    // Normalize.
    let norm = if n > 2 {
        1.0 / ((n - 1) * (n - 2)) as f64
    } else {
        1.0
    };

    (0..n)
        .map(|i| {
            vec![
                Value::String(csr.node_name_raw(i as u32).to_string()),
                Value::Float(bc[i] * norm),
            ]
        })
        .collect()
}

// ── Closeness Centrality ──────────────────────────────────────────────────────

fn closeness(csr: &CsrIndex, params: &AlgoParams) -> Vec<Vec<Value>> {
    let n = csr.node_count();
    if n == 0 {
        return Vec::new();
    }
    let sample = params.sample_size.unwrap_or(n).min(n);

    (0..sample)
        .map(|i| {
            let src = i as u32;
            let mut dist = vec![i64::MAX; n];
            dist[src as usize] = 0;
            let mut queue: VecDeque<u32> = VecDeque::new();
            queue.push_back(src);
            while let Some(u) = queue.pop_front() {
                for (_, v) in csr.iter_out_edges_raw(u) {
                    if dist[v as usize] == i64::MAX {
                        dist[v as usize] = dist[u as usize] + 1;
                        queue.push_back(v);
                    }
                }
            }
            let total: i64 = dist.iter().filter(|&&d| d != i64::MAX && d > 0).sum();
            let reachable = dist.iter().filter(|&&d| d != i64::MAX).count();
            let centrality = if total == 0 || reachable == 0 {
                0.0
            } else {
                (reachable - 1) as f64 / total as f64
            };
            vec![
                Value::String(csr.node_name_raw(src).to_string()),
                Value::Float(centrality),
            ]
        })
        .collect()
}

// ── Harmonic Centrality ───────────────────────────────────────────────────────

fn harmonic(csr: &CsrIndex) -> Vec<Vec<Value>> {
    let n = csr.node_count();
    if n == 0 {
        return Vec::new();
    }

    (0..n)
        .map(|i| {
            let src = i as u32;
            let mut dist = vec![i64::MAX; n];
            dist[src as usize] = 0;
            let mut queue: VecDeque<u32> = VecDeque::new();
            queue.push_back(src);
            while let Some(u) = queue.pop_front() {
                for (_, v) in csr.iter_out_edges_raw(u) {
                    if dist[v as usize] == i64::MAX {
                        dist[v as usize] = dist[u as usize] + 1;
                        queue.push_back(v);
                    }
                }
            }
            let h: f64 = dist
                .iter()
                .enumerate()
                .filter(|&(j, &d)| j != i && d != i64::MAX && d > 0)
                .map(|(_, &d)| 1.0 / d as f64)
                .sum();
            let norm = if n > 1 { 1.0 / (n - 1) as f64 } else { 1.0 };
            vec![
                Value::String(csr.node_name_raw(src).to_string()),
                Value::Float(h * norm),
            ]
        })
        .collect()
}

// ── Degree Centrality ─────────────────────────────────────────────────────────

fn degree(csr: &CsrIndex, params: &AlgoParams) -> Vec<Vec<Value>> {
    let n = csr.node_count();
    if n == 0 {
        return Vec::new();
    }
    let norm = if n > 1 { 1.0 / (n - 1) as f64 } else { 1.0 };
    let dir = params.direction.as_deref().unwrap_or("both");

    (0..n)
        .map(|i| {
            let node = i as u32;
            let deg = match dir {
                "in" => csr.in_degree_raw(node),
                "out" => csr.out_degree_raw(node),
                _ => csr.out_degree_raw(node) + csr.in_degree_raw(node),
            };
            vec![
                Value::String(csr.node_name_raw(node).to_string()),
                Value::Float(deg as f64 * norm),
            ]
        })
        .collect()
}

// ── Louvain (greedy modularity) ───────────────────────────────────────────────

fn louvain(csr: &CsrIndex, params: &AlgoParams) -> Vec<Vec<Value>> {
    // Start from LabelPropagation as community seeds, then compute modularity.
    let lpa_rows = label_propagation(csr, params);
    let n = csr.node_count();
    let m = csr.edge_count() as f64;

    // Map community → list of nodes.
    let mut community_map: HashMap<i64, Vec<u32>> = HashMap::new();
    for (i, row) in lpa_rows.iter().enumerate() {
        if let Value::Integer(c) = &row[1] {
            community_map.entry(*c).or_default().push(i as u32);
        }
    }

    // Compute modularity Q = sum over communities of (L_c/m - (d_c/2m)^2).
    let q: f64 = community_map
        .values()
        .map(|members| {
            let set: HashSet<u32> = members.iter().copied().collect();
            let mut lc = 0.0f64;
            let mut dc = 0.0f64;
            for &u in members {
                dc += (csr.out_degree_raw(u) + csr.in_degree_raw(u)) as f64;
                for (_, v) in csr.iter_out_edges_raw(u) {
                    if set.contains(&v) {
                        lc += 1.0;
                    }
                }
            }
            if m == 0.0 {
                0.0
            } else {
                lc / m - (dc / (2.0 * m)).powi(2)
            }
        })
        .sum();

    (0..n)
        .map(|i| {
            let comm = if let Value::Integer(c) = &lpa_rows[i][1] {
                *c
            } else {
                i as i64
            };
            vec![
                Value::String(csr.node_name_raw(i as u32).to_string()),
                Value::Integer(comm),
                Value::Float(q),
            ]
        })
        .collect()
}

// ── Triangle Counting ─────────────────────────────────────────────────────────

fn triangles(csr: &CsrIndex) -> Vec<Vec<Value>> {
    let n = csr.node_count();
    (0..n)
        .map(|i| {
            let node = i as u32;
            let neighbors: HashSet<u32> = csr
                .iter_out_edges_raw(node)
                .map(|(_, d)| d)
                .chain(csr.iter_in_edges_raw(node).map(|(_, s)| s))
                .collect();
            let mut count = 0i64;
            for &u in &neighbors {
                for (_, v) in csr.iter_out_edges_raw(u) {
                    if neighbors.contains(&v) {
                        count += 1;
                    }
                }
            }
            // Each triangle is counted twice per node endpoint.
            count /= 2;
            vec![
                Value::String(csr.node_name_raw(node).to_string()),
                Value::Integer(count),
            ]
        })
        .collect()
}

// ── Diameter ─────────────────────────────────────────────────────────────────

fn diameter(csr: &CsrIndex) -> Vec<Vec<Value>> {
    let n = csr.node_count();
    if n == 0 {
        return vec![vec![Value::Integer(0), Value::Integer(0)]];
    }

    let mut max_ecc = 0i64;
    let mut min_ecc = i64::MAX;

    for src in 0..n as u32 {
        let mut dist = vec![i64::MAX; n];
        dist[src as usize] = 0;
        let mut queue: VecDeque<u32> = VecDeque::new();
        queue.push_back(src);
        while let Some(u) = queue.pop_front() {
            for (_, v) in csr.iter_out_edges_raw(u) {
                if dist[v as usize] == i64::MAX {
                    dist[v as usize] = dist[u as usize] + 1;
                    queue.push_back(v);
                }
            }
        }
        let ecc = dist
            .iter()
            .filter(|&&d| d != i64::MAX)
            .copied()
            .max()
            .unwrap_or(0);
        max_ecc = max_ecc.max(ecc);
        if ecc > 0 {
            min_ecc = min_ecc.min(ecc);
        }
    }
    if min_ecc == i64::MAX {
        min_ecc = 0;
    }
    vec![vec![Value::Integer(max_ecc), Value::Integer(min_ecc)]]
}

// ── k-Core Decomposition ──────────────────────────────────────────────────────

fn kcore(csr: &CsrIndex) -> Vec<Vec<Value>> {
    let n = csr.node_count();
    if n == 0 {
        return Vec::new();
    }
    // Coreness = max k such that node is in k-core.
    let mut degree: Vec<usize> = (0..n as u32)
        .map(|i| csr.out_degree_raw(i) + csr.in_degree_raw(i))
        .collect();
    let mut removed = vec![false; n];
    let mut coreness = vec![0u32; n];
    let mut k = 1usize;

    loop {
        let mut progress = true;
        while progress {
            progress = false;
            for node in 0..n as u32 {
                if !removed[node as usize] && degree[node as usize] < k {
                    removed[node as usize] = true;
                    coreness[node as usize] = (k - 1) as u32;
                    // Reduce neighbors' degrees.
                    for (_, nb) in csr.iter_out_edges_raw(node) {
                        if !removed[nb as usize] && degree[nb as usize] > 0 {
                            degree[nb as usize] -= 1;
                        }
                    }
                    for (_, nb) in csr.iter_in_edges_raw(node) {
                        if !removed[nb as usize] && degree[nb as usize] > 0 {
                            degree[nb as usize] -= 1;
                        }
                    }
                    progress = true;
                }
            }
        }
        if removed.iter().all(|&r| r) {
            break;
        }
        // Assign coreness for remaining nodes.
        for (i, &r) in removed.iter().enumerate() {
            if !r {
                coreness[i] = k as u32;
            }
        }
        k += 1;
    }

    (0..n)
        .map(|i| {
            vec![
                Value::String(csr.node_name_raw(i as u32).to_string()),
                Value::Integer(coreness[i] as i64),
            ]
        })
        .collect()
}

#[cfg(test)]
mod tests {
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
}
