// SPDX-License-Identifier: Apache-2.0

//! Feature-gated embedded runner support for LDBC Graphalytics.

use std::collections::{HashMap, VecDeque};
use std::fs::File;
use std::io::{BufRead, BufReader};
use std::path::Path;
use std::time::Instant;

use nodedb_graph::params::{AlgoParams, GraphAlgorithm};
use nodedb_types::Namespace;
use nodedb_types::result::QueryResult;
use nodedb_types::value::Value;

use crate::NodeDbLite;
use crate::error::LiteError;
use crate::query::graph_ops::algorithms::run_algo;
use crate::query::graph_ops::edges::{edge_store_key, edge_to_value};
use crate::storage::engine::{StorageEngine, WriteOp};

const COLLECTION: &str = "graphalytics";
const EDGE_LABEL: &str = "EDGE";
const BATCH_SIZE: usize = 100_000;

#[derive(Debug)]
pub struct ImportMetrics {
    pub vertices: usize,
    pub edges: usize,
    pub load_seconds: f64,
    pub prepare_seconds: f64,
}

impl<S: StorageEngine> NodeDbLite<S> {
    /// Import a weighted Graphalytics edge-list into the normal durable graph
    /// table while building the embedded CSR once in process.
    pub async fn graphalytics_import(
        &self,
        vertex_file: &Path,
        edge_file: &Path,
    ) -> Result<ImportMetrics, LiteError> {
        let load_start = Instant::now();
        let mut vertex_count = 0usize;
        {
            let file = File::open(vertex_file).map_err(io_error)?;
            let mut map = self.csr.lock().map_err(|_| LiteError::LockPoisoned)?;
            let csr = map.entry(COLLECTION.to_string()).or_default();
            for line in BufReader::with_capacity(1 << 20, file).lines() {
                let vertex = line.map_err(io_error)?;
                let vertex = vertex.trim();
                if vertex.is_empty() {
                    continue;
                }
                csr.add_node(vertex).map_err(graph_error)?;
                vertex_count += 1;
            }
        }

        let file = File::open(edge_file).map_err(io_error)?;
        let mut pending = Vec::with_capacity(BATCH_SIZE);
        let mut edge_count = 0usize;
        for line in BufReader::with_capacity(1 << 20, file).lines() {
            let line = line.map_err(io_error)?;
            let mut fields = line.split_whitespace();
            let source = fields.next().ok_or_else(|| malformed_edge(&line))?;
            let destination = fields.next().ok_or_else(|| malformed_edge(&line))?;
            let weight = fields
                .next()
                .ok_or_else(|| malformed_edge(&line))?
                .parse::<f64>()
                .map_err(|_| malformed_edge(&line))?;
            if !weight.is_finite() || weight < 0.0 {
                return Err(malformed_edge(&line));
            }
            pending.push((source.to_string(), destination.to_string(), weight));
            edge_count += 1;
            if pending.len() == BATCH_SIZE {
                self.graphalytics_write_edges(&pending).await?;
                pending.clear();
            }
        }
        if !pending.is_empty() {
            self.graphalytics_write_edges(&pending).await?;
        }
        let load_seconds = load_start.elapsed().as_secs_f64();

        let prepare_start = Instant::now();
        {
            let mut map = self.csr.lock().map_err(|_| LiteError::LockPoisoned)?;
            map.get_mut(COLLECTION)
                .ok_or_else(|| LiteError::Storage {
                    detail: "graphalytics CSR missing after import".to_string(),
                })?
                .compact()
                .map_err(graph_error)?;
        }
        Ok(ImportMetrics {
            vertices: vertex_count,
            edges: edge_count,
            load_seconds,
            prepare_seconds: prepare_start.elapsed().as_secs_f64(),
        })
    }

    async fn graphalytics_write_edges(
        &self,
        edges: &[(String, String, f64)],
    ) -> Result<(), LiteError> {
        {
            let mut map = self.csr.lock().map_err(|_| LiteError::LockPoisoned)?;
            let csr = map.entry(COLLECTION.to_string()).or_default();
            for (source, destination, weight) in edges {
                csr.add_edge_weighted(source, EDGE_LABEL, destination, *weight)
                    .map_err(graph_error)?;
            }
        }

        let mut writes = Vec::with_capacity(edges.len());
        for (source, destination, weight) in edges {
            let properties = zerompk::to_msgpack_vec(&Value::Object(HashMap::from([(
                "weight".to_string(),
                Value::Float(*weight),
            )])))
            .map_err(|error| LiteError::Serialization {
                detail: error.to_string(),
            })?;
            writes.push(WriteOp::Put {
                ns: Namespace::Graph,
                key: edge_store_key(COLLECTION, source, EDGE_LABEL, destination),
                value: edge_to_value(COLLECTION, source, EDGE_LABEL, destination, &properties)?,
            });
        }
        self.storage.batch_write(&writes).await
    }

    /// Run one Graphalytics algorithm, returning the full in-memory result.
    pub fn graphalytics_run(
        &self,
        algorithm: GraphAlgorithm,
        source: &str,
    ) -> Result<QueryResult, LiteError> {
        if algorithm == GraphAlgorithm::Diameter {
            return Err(LiteError::Storage {
                detail: "diameter is not part of the Graphalytics runner".to_string(),
            });
        }
        let params = AlgoParams {
            collection: COLLECTION.to_string(),
            damping: Some(0.85),
            max_iterations: Some(10),
            // Positive minimum bypasses the generic non-positive fallback and
            // effectively disables early stopping for the required fixed count.
            tolerance: Some(f64::MIN_POSITIVE),
            source_node: Some(source.to_string()),
            direction: Some("both".to_string()),
            ..Default::default()
        };
        let mut result = run_algo(&self.csr, algorithm, &params)?;
        if algorithm == GraphAlgorithm::LabelPropagation {
            let map = self.csr.lock().map_err(|_| LiteError::LockPoisoned)?;
            let csr = map.get(COLLECTION).ok_or_else(|| LiteError::Storage {
                detail: "graphalytics CSR is not loaded".to_string(),
            })?;
            for row in &mut result.rows {
                if let Some(Value::Integer(label)) = row.get(1) {
                    let label = u32::try_from(*label).map_err(|_| LiteError::Storage {
                        detail: format!("invalid label propagation result {label}"),
                    })?;
                    row[1] = Value::String(csr.node_name_raw(label).to_string());
                }
            }
        }
        Ok(result)
    }

    /// Run unweighted BFS over the undirected projection.
    pub fn graphalytics_bfs(&self, source: &str) -> Result<QueryResult, LiteError> {
        let map = self.csr.lock().map_err(|_| LiteError::LockPoisoned)?;
        let csr = map.get(COLLECTION).ok_or_else(|| LiteError::Storage {
            detail: "graphalytics CSR is not loaded".to_string(),
        })?;
        let source_id = csr.node_id_raw(source).ok_or_else(|| LiteError::Storage {
            detail: format!("source vertex '{source}' is absent"),
        })?;
        let mut distance = vec![-1i64; csr.node_count()];
        distance[source_id as usize] = 0;
        let mut queue = VecDeque::from([source_id]);
        while let Some(node) = queue.pop_front() {
            let next_distance = distance[node as usize] + 1;
            let neighbors = csr
                .iter_out_edges_raw(node)
                .map(|(_, destination)| destination)
                .chain(csr.iter_in_edges_raw(node).map(|(_, source)| source));
            for neighbor in neighbors {
                if distance[neighbor as usize] < 0 {
                    distance[neighbor as usize] = next_distance;
                    queue.push_back(neighbor);
                }
            }
        }
        Ok(QueryResult {
            columns: vec!["node_id".to_string(), "distance".to_string()],
            rows: distance
                .into_iter()
                .enumerate()
                .map(|(node, value)| {
                    vec![
                        Value::String(csr.node_name_raw(node as u32).to_string()),
                        Value::Integer(value),
                    ]
                })
                .collect(),
            rows_affected: 0,
        })
    }
}

fn io_error(error: std::io::Error) -> LiteError {
    LiteError::Storage {
        detail: error.to_string(),
    }
}

fn graph_error(error: impl std::fmt::Display) -> LiteError {
    LiteError::Storage {
        detail: error.to_string(),
    }
}

fn malformed_edge(line: &str) -> LiteError {
    LiteError::Storage {
        detail: format!("malformed Graphalytics edge: {line}"),
    }
}
