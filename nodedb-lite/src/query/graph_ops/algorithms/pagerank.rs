// SPDX-License-Identifier: Apache-2.0

use nodedb_graph::params::AlgoParams;

use crate::engine::graph::index::CsrIndex;

use super::both_neighbors;

pub(super) fn pagerank_raw(csr: &CsrIndex, params: &AlgoParams) -> Vec<f64> {
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
    let dense_out = csr.compacted_out_adjacency_raw();
    let dense_in = csr.compacted_in_adjacency_raw();
    let dense_pull = both && dense_out.is_some() && dense_in.is_some();
    let adjacency = (both && !dense_pull).then(|| {
        (0..n as u32)
            .map(|node| both_neighbors(csr, node))
            .collect::<Vec<Vec<u32>>>()
    });
    let degrees: Vec<usize> = if dense_pull {
        let (out_offsets, _) = dense_out.expect("dense pull checked");
        let (in_offsets, _) = dense_in.expect("dense pull checked");
        (0..n)
            .map(|node| {
                out_offsets[node + 1] as usize - out_offsets[node] as usize
                    + in_offsets[node + 1] as usize
                    - in_offsets[node] as usize
            })
            .collect()
    } else if let Some(adjacency) = &adjacency {
        adjacency.iter().map(Vec::len).collect()
    } else {
        (0..n as u32).map(|node| csr.out_degree_raw(node)).collect()
    };

    let mut new_rank = vec![0.0f64; n];
    let mut contributions = both.then(|| vec![0.0f64; n]);
    for _ in 0..max_iter {
        let dangling_mass: f64 = rank
            .iter()
            .zip(&degrees)
            .filter_map(|(node_rank, degree)| (*degree == 0).then_some(*node_rank))
            .sum();
        let base_scale = 1.0 - d + d * dangling_mass;
        if let Some(contributions) = contributions.as_mut() {
            for (node, degree) in degrees.iter().copied().enumerate() {
                contributions[node] = if degree == 0 {
                    0.0
                } else {
                    d * rank[node] / degree as f64
                };
            }
            if dense_pull {
                pull_pagerank_dense_iteration(
                    dense_in.expect("dense pull checked"),
                    dense_out.expect("dense pull checked"),
                    contributions,
                    &teleport,
                    &mut new_rank,
                    base_scale,
                );
            } else {
                pull_pagerank_iteration(
                    adjacency.as_ref().expect("BOTH fallback adjacency"),
                    contributions,
                    &teleport,
                    &mut new_rank,
                    base_scale,
                );
            }
        } else {
            for (slot, seed) in new_rank.iter_mut().zip(&teleport) {
                *slot = base_scale * seed;
            }
            for src in 0..n as u32 {
                let degree = degrees[src as usize];
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

    rank
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

fn pull_pagerank_dense_iteration(
    inbound: (&[u32], &[u32]),
    outbound: (&[u32], &[u32]),
    contributions: &[f64],
    teleport: &[f64],
    output: &mut [f64],
    base_scale: f64,
) {
    #[cfg(target_arch = "wasm32")]
    pull_pagerank_dense_range(
        inbound,
        outbound,
        contributions,
        teleport,
        output,
        0,
        base_scale,
    );

    #[cfg(not(target_arch = "wasm32"))]
    {
        let desired_workers = std::thread::available_parallelism()
            .map_or(1, usize::from)
            .min(32)
            .min(output.len().max(1));
        let permits = reserve_pagerank_workers(desired_workers.saturating_sub(1));
        let workers = permits.0;
        if workers <= 1 {
            pull_pagerank_dense_range(
                inbound,
                outbound,
                contributions,
                teleport,
                output,
                0,
                base_scale,
            );
            return;
        }
        let chunk_size = output.len().div_ceil(workers);
        std::thread::scope(|scope| {
            for (chunk_index, chunk) in output.chunks_mut(chunk_size).enumerate() {
                let start = chunk_index * chunk_size;
                scope.spawn(move || {
                    pull_pagerank_dense_range(
                        inbound,
                        outbound,
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

fn pull_pagerank_dense_range(
    inbound: (&[u32], &[u32]),
    outbound: (&[u32], &[u32]),
    contributions: &[f64],
    teleport: &[f64],
    output: &mut [f64],
    start: usize,
    base_scale: f64,
) {
    let (in_offsets, in_targets) = inbound;
    let (out_offsets, out_targets) = outbound;
    for (offset, slot) in output.iter_mut().enumerate() {
        let node = start + offset;
        let mut rank = base_scale * teleport[node];
        for &neighbor in &in_targets[in_offsets[node] as usize..in_offsets[node + 1] as usize] {
            rank += contributions[neighbor as usize];
        }
        for &neighbor in &out_targets[out_offsets[node] as usize..out_offsets[node + 1] as usize] {
            rank += contributions[neighbor as usize];
        }
        *slot = rank;
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
