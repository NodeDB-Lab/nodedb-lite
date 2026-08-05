// SPDX-License-Identifier: Apache-2.0

use crate::engine::graph::index::CsrIndex;

#[cfg(not(target_arch = "wasm32"))]
use std::sync::atomic::{AtomicUsize, Ordering as AtomicOrdering};

// ── LCC (local clustering coefficient) ───────────────────────────────────────

pub(super) fn lcc_raw(csr: &CsrIndex) -> Vec<f64> {
    let n = csr.node_count();
    let adjacency: Vec<Vec<u32>> =
        if let (Some((out_offsets, out_targets)), Some((in_offsets, in_targets))) = (
            csr.compacted_out_adjacency_raw(),
            csr.compacted_in_adjacency_raw(),
        ) {
            (0..n)
                .map(|node| {
                    let mut neighbors = Vec::with_capacity(
                        out_offsets[node + 1] as usize - out_offsets[node] as usize
                            + in_offsets[node + 1] as usize
                            - in_offsets[node] as usize,
                    );
                    neighbors.extend_from_slice(
                        &out_targets[out_offsets[node] as usize..out_offsets[node + 1] as usize],
                    );
                    neighbors.extend_from_slice(
                        &in_targets[in_offsets[node] as usize..in_offsets[node + 1] as usize],
                    );
                    neighbors.retain(|neighbor| *neighbor != node as u32);
                    neighbors.sort_unstable();
                    neighbors.dedup();
                    neighbors
                })
                .collect()
        } else {
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
            adjacency
        };
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
            if degree < 2 {
                0.0
            } else {
                2.0 * triangles[i] as f64 / (degree * (degree - 1)) as f64
            }
        })
        .collect()
}

#[cfg(not(target_arch = "wasm32"))]
static ACTIVE_LCC_WORKERS: AtomicUsize = AtomicUsize::new(0);

#[cfg(not(target_arch = "wasm32"))]
struct LccWorkerPermits(usize);

#[cfg(not(target_arch = "wasm32"))]
impl LccWorkerPermits {
    fn reserve(requested: usize) -> Self {
        const MAX_PROCESS_WORKERS: usize = 32;
        let mut active = ACTIVE_LCC_WORKERS.load(AtomicOrdering::Acquire);
        loop {
            let granted = requested.min(MAX_PROCESS_WORKERS.saturating_sub(active));
            match ACTIVE_LCC_WORKERS.compare_exchange_weak(
                active,
                active + granted,
                AtomicOrdering::AcqRel,
                AtomicOrdering::Acquire,
            ) {
                Ok(_) => return Self(granted),
                Err(updated) => active = updated,
            }
        }
    }

    fn workers(&self) -> usize {
        self.0
    }
}

#[cfg(not(target_arch = "wasm32"))]
impl Drop for LccWorkerPermits {
    fn drop(&mut self) {
        ACTIVE_LCC_WORKERS.fetch_sub(self.0, AtomicOrdering::Release);
    }
}

fn count_oriented_triangles(oriented: &[Vec<u32>]) -> Vec<usize> {
    #[cfg(target_arch = "wasm32")]
    {
        return count_oriented_triangle_range(oriented, 0, oriented.len());
    }

    #[cfg(not(target_arch = "wasm32"))]
    {
        const MAX_WORKERS: usize = 32;
        const COUNTER_BUDGET_BYTES: usize = 128 * 1024 * 1024;
        let bytes_per_counter = oriented
            .len()
            .checked_mul(std::mem::size_of::<usize>())
            .unwrap_or(usize::MAX)
            .max(1);
        let memory_bounded_workers = (COUNTER_BUDGET_BYTES / bytes_per_counter).max(1);
        let desired_workers = std::thread::available_parallelism()
            .map_or(1, usize::from)
            .min(MAX_WORKERS)
            .min(memory_bounded_workers)
            .min(oriented.len().max(1));
        let permits = LccWorkerPermits::reserve(desired_workers);
        count_oriented_triangles_native(oriented, permits.workers())
    }
}

#[cfg(not(target_arch = "wasm32"))]
pub(super) fn count_oriented_triangles_native(oriented: &[Vec<u32>], workers: usize) -> Vec<usize> {
    if workers <= 1 {
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

fn count_oriented_triangle_range(oriented: &[Vec<u32>], start: usize, end: usize) -> Vec<usize> {
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
