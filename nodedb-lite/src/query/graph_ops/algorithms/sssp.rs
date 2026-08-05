// SPDX-License-Identifier: Apache-2.0

use std::cmp::Ordering;
use std::collections::BinaryHeap;

use nodedb_graph::params::AlgoParams;

use crate::engine::graph::index::CsrIndex;
use crate::error::LiteError;

type CompactedAdjacency<'a> = (&'a [u32], &'a [u32], Option<&'a [f64]>);

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

pub(super) fn sssp_raw(
    csr: &CsrIndex,
    params: &AlgoParams,
    weights_prevalidated: bool,
) -> Result<Vec<f64>, LiteError> {
    let n = csr.node_count();
    if n == 0 {
        return Ok(Vec::new());
    }
    let compacted_out = csr.compacted_out_weighted_adjacency_raw();
    if !weights_prevalidated {
        validate_sssp_weights(csr, compacted_out)?;
    }
    let src_name = params.source_node.as_deref().unwrap_or("");
    let Some(src_id) = csr.node_id_raw(src_name) else {
        return Ok(vec![f64::INFINITY; n]);
    };

    let both = params.direction.as_deref() == Some("both");
    let compacted_in = both
        .then(|| csr.compacted_in_weighted_adjacency_raw())
        .flatten();
    let use_compacted = compacted_out.is_some() && (!both || compacted_in.is_some());
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
        if use_compacted {
            let (offsets, targets, weights) = compacted_out.expect("checked above");
            relax_compacted_lite(
                node,
                distance,
                offsets,
                targets,
                weights,
                &mut distances,
                &mut queue,
            );
            if both {
                let (offsets, targets, weights) = compacted_in.expect("checked above");
                relax_compacted_lite(
                    node,
                    distance,
                    offsets,
                    targets,
                    weights,
                    &mut distances,
                    &mut queue,
                );
            }
        } else {
            for (_, destination, weight) in csr.iter_out_edges_weighted_raw(node) {
                relax_lite(destination, weight, distance, &mut distances, &mut queue);
            }
            if both {
                for (_, source, weight) in csr.iter_in_edges_weighted_raw(node) {
                    relax_lite(source, weight, distance, &mut distances, &mut queue);
                }
            }
        }
    }

    Ok(distances)
}

pub(super) fn validate_sssp_weights(
    csr: &CsrIndex,
    compacted: Option<CompactedAdjacency<'_>>,
) -> Result<(), LiteError> {
    if !csr.has_weights() {
        return Ok(());
    }
    if let Some((offsets, _targets, Some(weights))) = compacted {
        for node in 0..csr.node_count() {
            for &weight in &weights[offsets[node] as usize..offsets[node + 1] as usize] {
                if !weight.is_finite() || weight < 0.0 {
                    return Err(invalid_sssp_weight(csr, node, weight));
                }
            }
        }
    } else {
        for node in 0..csr.node_count() {
            for (_label, _destination, weight) in csr.iter_out_edges_weighted_raw(node as u32) {
                if !weight.is_finite() || weight < 0.0 {
                    return Err(invalid_sssp_weight(csr, node, weight));
                }
            }
        }
    }
    Ok(())
}

fn invalid_sssp_weight(csr: &CsrIndex, node: usize, weight: f64) -> LiteError {
    LiteError::Storage {
        detail: format!(
            "SSSP requires finite non-negative edge weights, found {weight} on edge from '{}'",
            csr.node_name_raw(node as u32)
        ),
    }
}

fn relax_compacted_lite(
    node: u32,
    distance: f64,
    offsets: &[u32],
    targets: &[u32],
    weights: Option<&[f64]>,
    distances: &mut [f64],
    queue: &mut BinaryHeap<QueueEntry>,
) {
    let start = offsets[node as usize] as usize;
    let end = offsets[node as usize + 1] as usize;
    for edge in start..end {
        relax_lite(
            targets[edge],
            weights.map_or(1.0, |weights| weights[edge]),
            distance,
            distances,
            queue,
        );
    }
}

fn relax_lite(
    destination: u32,
    weight: f64,
    distance: f64,
    distances: &mut [f64],
    queue: &mut BinaryHeap<QueueEntry>,
) {
    if !weight.is_finite() || weight < 0.0 {
        return;
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
