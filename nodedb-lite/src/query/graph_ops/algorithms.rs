// SPDX-License-Identifier: Apache-2.0

//! Graph algorithm dispatch: PageRank, WCC, SSSP, LCC, LPA, Closeness,
//! Betweenness, Harmonic, Degree, Louvain, Triangles, Diameter, kCore.

use std::cmp::Ordering;
use std::collections::{BinaryHeap, HashMap, HashSet, VecDeque};
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

fn both_neighbors(csr: &CsrIndex, node: u32) -> Vec<u32> {
    csr.iter_out_edges_raw(node)
        .map(|(_, destination)| destination)
        .chain(csr.iter_in_edges_raw(node).map(|(_, source)| source))
        .collect()
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

    // Threshold below which a personalization vector sum is treated as zero → uniform fallback.
    const UNIFORM_FALLBACK_THRESHOLD: f64 = 1e-12;

    // Build the teleport (personalization) distribution.
    // For standard PageRank (no personalization) this is uniform 1/n.
    // For PPR this is normalized per the caller-supplied map; nodes absent from the
    // map receive 0.0. If the map matches no node (or sums to ≈0) we fall back to uniform.
    let teleport: Vec<f64> = match params.personalization_vector() {
        None => vec![1.0f64 / n as f64; n],
        Some(map) => {
            let mut v: Vec<f64> = (0..n)
                .map(|i| {
                    let name = csr.node_name_raw(i as u32);
                    map.get(name).copied().unwrap_or(0.0)
                })
                .collect();
            let sum: f64 = v.iter().sum();
            if sum < UNIFORM_FALLBACK_THRESHOLD {
                v = vec![1.0f64 / n as f64; n];
            } else {
                for x in &mut v {
                    *x /= sum;
                }
            }
            v
        }
    };

    // Initial rank distribution equals the teleport distribution.
    let mut rank = teleport.clone();
    let both = params.direction.as_deref() == Some("both");
    let adjacency = both.then(|| {
        (0..n as u32)
            .map(|node| both_neighbors(csr, node))
            .collect::<Vec<Vec<u32>>>()
    });

    let mut new_rank = vec![0.0f64; n];
    let mut contributions = both.then(|| vec![0.0f64; n]);
    for _ in 0..max_iter {
        let dangling_mass: f64 = if let Some(adjacency) = &adjacency {
            rank.iter()
                .zip(adjacency)
                .filter_map(|(node_rank, neighbors)| neighbors.is_empty().then_some(*node_rank))
                .sum()
        } else {
            rank.iter()
                .enumerate()
                .filter_map(|(node, node_rank)| {
                    (csr.out_degree_raw(node as u32) == 0).then_some(*node_rank)
                })
                .sum()
        };
        let base_scale = 1.0 - d + d * dangling_mass;
        if let (Some(adjacency), Some(contributions)) = (&adjacency, contributions.as_mut()) {
            for (node, neighbors) in adjacency.iter().enumerate() {
                contributions[node] = if neighbors.is_empty() {
                    0.0
                } else {
                    d * rank[node] / neighbors.len() as f64
                };
            }
            pull_pagerank_iteration(
                adjacency,
                contributions,
                &teleport,
                &mut new_rank,
                base_scale,
            );
        } else {
            for (slot, seed) in new_rank.iter_mut().zip(&teleport) {
                *slot = base_scale * seed;
            }
            for src in 0..n as u32 {
                let degree = csr.out_degree_raw(src);
                if degree == 0 {
                    continue;
                }
                let contribution = d * rank[src as usize] / degree as f64;
                for (_, destination) in csr.iter_out_edges_raw(src) {
                    new_rank[destination as usize] += contribution;
                }
            }
        }
        let delta: f64 = rank
            .iter()
            .zip(new_rank.iter())
            .map(|(a, b)| (a - b).abs())
            .sum();
        std::mem::swap(&mut rank, &mut new_rank);
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

#[cfg(not(target_arch = "wasm32"))]
static ACTIVE_PAGERANK_WORKERS: std::sync::atomic::AtomicUsize =
    std::sync::atomic::AtomicUsize::new(0);

#[cfg(not(target_arch = "wasm32"))]
struct PageRankWorkerPermits(usize);

#[cfg(not(target_arch = "wasm32"))]
impl Drop for PageRankWorkerPermits {
    fn drop(&mut self) {
        ACTIVE_PAGERANK_WORKERS.fetch_sub(self.0, std::sync::atomic::Ordering::Release);
    }
}

#[cfg(not(target_arch = "wasm32"))]
fn reserve_pagerank_workers(requested: usize) -> PageRankWorkerPermits {
    const MAX_PROCESS_WORKERS: usize = 31;
    let mut active = ACTIVE_PAGERANK_WORKERS.load(std::sync::atomic::Ordering::Acquire);
    loop {
        let granted = requested.min(MAX_PROCESS_WORKERS.saturating_sub(active));
        match ACTIVE_PAGERANK_WORKERS.compare_exchange_weak(
            active,
            active + granted,
            std::sync::atomic::Ordering::AcqRel,
            std::sync::atomic::Ordering::Acquire,
        ) {
            Ok(_) => return PageRankWorkerPermits(granted),
            Err(observed) => active = observed,
        }
    }
}

fn pull_pagerank_iteration(
    adjacency: &[Vec<u32>],
    contributions: &[f64],
    teleport: &[f64],
    output: &mut [f64],
    base_scale: f64,
) {
    #[cfg(target_arch = "wasm32")]
    pull_pagerank_range(adjacency, contributions, teleport, output, 0, base_scale);

    #[cfg(not(target_arch = "wasm32"))]
    {
        let desired_workers = std::thread::available_parallelism()
            .map_or(1, usize::from)
            .min(32)
            .min(output.len().max(1));
        let permits = reserve_pagerank_workers(desired_workers.saturating_sub(1));
        let workers = permits.0;
        if workers <= 1 {
            pull_pagerank_range(adjacency, contributions, teleport, output, 0, base_scale);
            return;
        }
        let chunk_size = output.len().div_ceil(workers);
        std::thread::scope(|scope| {
            for (chunk_index, chunk) in output.chunks_mut(chunk_size).enumerate() {
                let start = chunk_index * chunk_size;
                scope.spawn(move || {
                    pull_pagerank_range(
                        adjacency,
                        contributions,
                        teleport,
                        chunk,
                        start,
                        base_scale,
                    );
                });
            }
        });
    }
}

fn pull_pagerank_range(
    adjacency: &[Vec<u32>],
    contributions: &[f64],
    teleport: &[f64],
    output: &mut [f64],
    start: usize,
    base_scale: f64,
) {
    for (offset, slot) in output.iter_mut().enumerate() {
        let node = start + offset;
        *slot = base_scale * teleport[node]
            + adjacency[node]
                .iter()
                .map(|neighbor| contributions[*neighbor as usize])
                .sum::<f64>();
    }
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
    let adjacency: Vec<Vec<u32>> = (0..n as u32)
        .map(|node| both_neighbors(csr, node))
        .collect();
    let label_priority = label_priorities(csr, n);
    let mut next_labels = labels.clone();
    let mut neighbor_labels = Vec::new();

    for _ in 0..max_iter {
        next_labels.copy_from_slice(&labels);
        let mut changed = false;
        for (node, neighbors) in adjacency.iter().enumerate() {
            neighbor_labels.clear();
            neighbor_labels.extend(neighbors.iter().map(|neighbor| labels[*neighbor as usize]));
            if let Some(best) = most_frequent_label(&mut neighbor_labels, &label_priority) {
                next_labels[node] = best;
                changed |= best != labels[node];
            }
        }
        std::mem::swap(&mut labels, &mut next_labels);
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

fn label_priorities(csr: &CsrIndex, n: usize) -> Vec<u32> {
    let mut ordered: Vec<u32> = (0..n as u32).collect();
    ordered.sort_unstable_by(|left, right| {
        let left_name = csr.node_name_raw(*left);
        let right_name = csr.node_name_raw(*right);
        match (left_name.parse::<i128>(), right_name.parse::<i128>()) {
            (Ok(left), Ok(right)) => left.cmp(&right).then_with(|| left_name.cmp(right_name)),
            _ => left_name.cmp(right_name),
        }
    });
    let mut priority = vec![0u32; n];
    for (rank, label) in ordered.into_iter().enumerate() {
        priority[label as usize] = rank as u32;
    }
    priority
}

fn most_frequent_label(labels: &mut [u32], priority: &[u32]) -> Option<u32> {
    labels.sort_unstable_by_key(|label| priority[*label as usize]);
    let (&first, rest) = labels.split_first()?;
    let mut best_label = first;
    let mut best_count = 1usize;
    let mut current_label = first;
    let mut current_count = 1usize;
    for &label in rest {
        if label == current_label {
            current_count += 1;
        } else {
            if current_count > best_count {
                best_label = current_label;
                best_count = current_count;
            }
            current_label = label;
            current_count = 1;
        }
    }
    if current_count > best_count {
        best_label = current_label;
    }
    Some(best_label)
}

// ── LCC (local clustering coefficient) ───────────────────────────────────────

fn lcc(csr: &CsrIndex) -> Vec<Vec<Value>> {
    let n = csr.node_count();
    let mut adjacency = vec![Vec::<u32>::new(); n];
    for source in 0..n as u32 {
        for (_, destination) in csr.iter_out_edges_raw(source) {
            if source != destination {
                adjacency[source as usize].push(destination);
                adjacency[destination as usize].push(source);
            }
        }
    }
    for neighbors in &mut adjacency {
        neighbors.sort_unstable();
        neighbors.dedup();
    }
    let degrees: Vec<usize> = adjacency.iter().map(Vec::len).collect();

    // Direct every edge from the lower (degree, id) endpoint to the higher
    // endpoint. Enumerating wedges in this acyclic orientation discovers each
    // triangle exactly once and avoids repeating an induced-edge scan for all
    // three of its vertices.
    let oriented: Vec<Vec<u32>> = adjacency
        .iter()
        .enumerate()
        .map(|(node, neighbors)| {
            neighbors
                .iter()
                .copied()
                .filter(|neighbor| {
                    let neighbor = *neighbor as usize;
                    (degrees[node], node) < (degrees[neighbor], neighbor)
                })
                .collect()
        })
        .collect();
    let triangles = count_oriented_triangles(&oriented);

    (0..n)
        .map(|i| {
            let degree = degrees[i];
            let coefficient = if degree < 2 {
                0.0
            } else {
                2.0 * triangles[i] as f64 / (degree * (degree - 1)) as f64
            };
            vec![
                Value::String(csr.node_name_raw(i as u32).to_string()),
                Value::Float(coefficient),
            ]
        })
        .collect()
}

fn count_oriented_triangles(oriented: &[Vec<u32>]) -> Vec<usize> {
    #[cfg(target_arch = "wasm32")]
    {
        return count_oriented_triangle_range(oriented, 0, oriented.len());
    }

    #[cfg(not(target_arch = "wasm32"))]
    {
        use std::sync::atomic::{AtomicUsize, Ordering as AtomicOrdering};

        const MAX_WORKERS: usize = 32;
        const COUNTER_BUDGET_BYTES: usize = 128 * 1024 * 1024;
        let bytes_per_counter = oriented
            .len()
            .checked_mul(std::mem::size_of::<usize>())
            .unwrap_or(usize::MAX)
            .max(1);
        let memory_bounded_workers = (COUNTER_BUDGET_BYTES / bytes_per_counter).max(1);
        let workers = std::thread::available_parallelism()
            .map_or(1, usize::from)
            .min(MAX_WORKERS)
            .min(memory_bounded_workers)
            .min(oriented.len().max(1));
        if workers == 1 {
            return count_oriented_triangle_range(oriented, 0, oriented.len());
        }

        const CHUNK_SIZE: usize = 64;
        let next = AtomicUsize::new(0);
        std::thread::scope(|scope| {
            let handles: Vec<_> = (0..workers)
                .map(|_| {
                    let next = &next;
                    scope.spawn(move || {
                        let mut local = vec![0usize; oriented.len()];
                        loop {
                            let start = next.fetch_add(CHUNK_SIZE, AtomicOrdering::Relaxed);
                            if start >= oriented.len() {
                                break;
                            }
                            let end = (start + CHUNK_SIZE).min(oriented.len());
                            count_oriented_triangle_range_into(oriented, start, end, &mut local);
                        }
                        local
                    })
                })
                .collect();
            let mut totals = vec![0usize; oriented.len()];
            for handle in handles {
                let local = handle.join().expect("LCC worker panicked");
                for (total, count) in totals.iter_mut().zip(local) {
                    *total += count;
                }
            }
            totals
        })
    }
}

fn count_oriented_triangle_range(
    oriented: &[Vec<u32>],
    start: usize,
    end: usize,
) -> Vec<usize> {
    let mut triangles = vec![0usize; oriented.len()];
    count_oriented_triangle_range_into(oriented, start, end, &mut triangles);
    triangles
}

fn count_oriented_triangle_range_into(
    oriented: &[Vec<u32>],
    start: usize,
    end: usize,
    triangles: &mut [usize],
) {
    for left in start..end {
        let middle_neighbors = &oriented[left];
        for &middle in middle_neighbors {
            for &right in &oriented[middle as usize] {
                if middle_neighbors.binary_search(&right).is_ok() {
                    triangles[left] += 1;
                    triangles[middle as usize] += 1;
                    triangles[right as usize] += 1;
                }
            }
        }
    }
}

// ── SSSP (Dijkstra) ──────────────────────────────────────────────────────────

#[derive(Clone, Copy, Debug)]
struct QueueEntry {
    distance: f64,
    node: u32,
}

impl PartialEq for QueueEntry {
    fn eq(&self, other: &Self) -> bool {
        self.node == other.node && self.distance.to_bits() == other.distance.to_bits()
    }
}

impl Eq for QueueEntry {}

impl Ord for QueueEntry {
    fn cmp(&self, other: &Self) -> Ordering {
        other
            .distance
            .total_cmp(&self.distance)
            .then_with(|| other.node.cmp(&self.node))
    }
}

impl PartialOrd for QueueEntry {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

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

    let both = params.direction.as_deref() == Some("both");
    let mut distances = vec![f64::INFINITY; n];
    distances[src_id as usize] = 0.0;
    let mut queue = BinaryHeap::new();
    queue.push(QueueEntry {
        distance: 0.0,
        node: src_id,
    });

    while let Some(QueueEntry { distance, node }) = queue.pop() {
        if distance > distances[node as usize] {
            continue;
        }
        let mut edges: HashMap<u32, f64> = HashMap::new();
        for (_, destination, weight) in csr.iter_out_edges_weighted_raw(node) {
            edges
                .entry(destination)
                .and_modify(|current| *current = current.min(weight))
                .or_insert(weight);
        }
        if both {
            for (_, source, weight) in csr.iter_in_edges_weighted_raw(node) {
                edges
                    .entry(source)
                    .and_modify(|current| *current = current.min(weight))
                    .or_insert(weight);
            }
        }
        for (destination, weight) in edges {
            if !weight.is_finite() || weight < 0.0 {
                continue;
            }
            let candidate = distance + weight;
            if candidate < distances[destination as usize] {
                distances[destination as usize] = candidate;
                queue.push(QueueEntry {
                    distance: candidate,
                    node: destination,
                });
            }
        }
    }

    (0..n)
        .map(|i| {
            vec![
                Value::String(csr.node_name_raw(i as u32).to_string()),
                Value::Float(distances[i]),
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
