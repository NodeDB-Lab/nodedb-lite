// SPDX-License-Identifier: Apache-2.0

use std::collections::{HashMap, HashSet, VecDeque};

use nodedb_graph::params::AlgoParams;
use nodedb_types::value::Value;

use crate::engine::graph::index::CsrIndex;

use super::communities::label_propagation_raw;

// ── Betweenness Centrality (Brandes) ─────────────────────────────────────────

pub(super) fn betweenness(csr: &CsrIndex, params: &AlgoParams) -> Vec<Vec<Value>> {
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

pub(super) fn closeness(csr: &CsrIndex, params: &AlgoParams) -> Vec<Vec<Value>> {
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

pub(super) fn harmonic(csr: &CsrIndex) -> Vec<Vec<Value>> {
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

pub(super) fn degree(csr: &CsrIndex, params: &AlgoParams) -> Vec<Vec<Value>> {
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

pub(super) fn louvain(csr: &CsrIndex, params: &AlgoParams) -> Vec<Vec<Value>> {
    // Start from LabelPropagation as community seeds, then compute modularity.
    let lpa_labels = label_propagation_raw(csr, params);
    let n = csr.node_count();
    let m = csr.edge_count() as f64;

    // Map community → list of nodes.
    let mut community_map: HashMap<i64, Vec<u32>> = HashMap::new();
    for (node, &community) in lpa_labels.iter().enumerate() {
        community_map
            .entry(community as i64)
            .or_default()
            .push(node as u32);
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
            let comm = lpa_labels[i] as i64;
            vec![
                Value::String(csr.node_name_raw(i as u32).to_string()),
                Value::Integer(comm),
                Value::Float(q),
            ]
        })
        .collect()
}

// ── Triangle Counting ─────────────────────────────────────────────────────────

pub(super) fn triangles(csr: &CsrIndex) -> Vec<Vec<Value>> {
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

pub(super) fn diameter(csr: &CsrIndex) -> Vec<Vec<Value>> {
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

pub(super) fn kcore(csr: &CsrIndex) -> Vec<Vec<Value>> {
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
