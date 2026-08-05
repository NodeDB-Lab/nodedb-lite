// SPDX-License-Identifier: Apache-2.0

use nodedb_graph::params::AlgoParams;

use crate::engine::graph::index::CsrIndex;

use super::both_neighbors;

// ── WCC (union-find) ─────────────────────────────────────────────────────────

pub(super) fn wcc_raw(csr: &CsrIndex) -> Vec<u32> {
    let n = csr.node_count();
    if n == 0 {
        return Vec::new();
    }
    let mut parent: Vec<u32> = (0..n as u32).collect();
    let mut rank = vec![0u8; n];

    fn find(parent: &mut [u32], mut node: u32) -> u32 {
        while parent[node as usize] != node {
            parent[node as usize] = parent[parent[node as usize] as usize];
            node = parent[node as usize];
        }
        node
    }

    fn union(parent: &mut [u32], rank: &mut [u8], left: u32, right: u32) {
        let left = find(parent, left);
        let right = find(parent, right);
        if left == right {
            return;
        }
        match rank[left as usize].cmp(&rank[right as usize]) {
            std::cmp::Ordering::Less => parent[left as usize] = right,
            std::cmp::Ordering::Greater => parent[right as usize] = left,
            std::cmp::Ordering::Equal => {
                parent[right as usize] = left;
                rank[left as usize] += 1;
            }
        }
    }

    if let Some((offsets, targets)) = csr.compacted_out_adjacency_raw() {
        for source in 0..n {
            for &destination in &targets[offsets[source] as usize..offsets[source + 1] as usize] {
                union(&mut parent, &mut rank, source as u32, destination);
            }
        }
    } else {
        for source in 0..n as u32 {
            for (_, destination) in csr.iter_out_edges_raw(source) {
                union(&mut parent, &mut rank, source, destination);
            }
        }
    }

    (0..n).map(|node| find(&mut parent, node as u32)).collect()
}

// ── LabelPropagation ─────────────────────────────────────────────────────────

pub(super) fn label_propagation_raw(csr: &CsrIndex, params: &AlgoParams) -> Vec<u32> {
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

    labels
}

pub(super) fn label_priorities(csr: &CsrIndex, n: usize) -> Vec<u32> {
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

pub(super) fn most_frequent_label(labels: &mut [u32], priority: &[u32]) -> Option<u32> {
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
