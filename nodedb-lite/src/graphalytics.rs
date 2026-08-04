// SPDX-License-Identifier: Apache-2.0

//! Feature-gated embedded runner support for LDBC Graphalytics.

use std::fs::File;
use std::io::{BufRead, BufReader};
use std::path::Path;
use std::time::{Duration, Instant};

use nodedb_graph::params::{AlgoParams, GraphAlgorithm};
use nodedb_types::result::QueryResult;
use nodedb_types::value::Value;

use crate::NodeDbLite;
use crate::engine::graph::index::CsrIndex;
use crate::error::LiteError;
use crate::graphalytics_external_sort::ExternalEdgeSorter;
use crate::query::graph_ops::algorithms::run_algo;
use crate::query::graph_ops::edges::edge_store_key;
use crate::storage::engine::StorageEngine;

const COLLECTION: &str = "graphalytics";
const EDGE_LABEL: &str = "EDGE";
const BATCH_SIZE: usize = 1_000_000;

#[derive(Debug)]
pub struct ImportMetrics {
    pub vertices: usize,
    pub edges: usize,
    pub load_seconds: f64,
    pub prepare_seconds: f64,
}

struct WeightProperties(f64);

impl zerompk::ToMessagePack for WeightProperties {
    fn write<W: zerompk::Write>(&self, writer: &mut W) -> zerompk::Result<()> {
        // Value::Object({"weight": Value::Float(weight)})
        writer.write_array_len(2)?;
        writer.write_u8(7)?;
        writer.write_map_len(1)?;
        writer.write_string("weight")?;
        writer.write_array_len(2)?;
        writer.write_u8(3)?;
        writer.write_f64(self.0)
    }
}

struct StoredGraphalyticsEdge<'a> {
    source: &'a str,
    destination: &'a str,
    properties: &'a [u8],
}

impl zerompk::ToMessagePack for StoredGraphalyticsEdge<'_> {
    fn write<W: zerompk::Write>(&self, writer: &mut W) -> zerompk::Result<()> {
        // Value::Object with the same fields produced by edge_to_value().
        writer.write_array_len(2)?;
        writer.write_u8(7)?;
        writer.write_map_len(5)?;
        write_string_value(writer, "collection", COLLECTION)?;
        write_string_value(writer, "src", self.source)?;
        write_string_value(writer, "label", EDGE_LABEL)?;
        write_string_value(writer, "dst", self.destination)?;
        writer.write_string("props")?;
        writer.write_array_len(2)?;
        writer.write_u8(5)?;
        writer.write_binary(self.properties)
    }
}

fn write_string_value<W: zerompk::Write>(
    writer: &mut W,
    key: &str,
    value: &str,
) -> zerompk::Result<()> {
    writer.write_string(key)?;
    writer.write_array_len(2)?;
    writer.write_u8(4)?;
    writer.write_string(value)
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
        let mut prepare_duration = Duration::ZERO;
        let file = File::open(vertex_file).map_err(io_error)?;
        let vertices: Vec<String> = BufReader::with_capacity(1 << 20, file)
            .lines()
            .map(|line| line.map_err(io_error))
            .collect::<Result<_, _>>()?;
        let vertex_count = vertices
            .iter()
            .filter(|vertex| !vertex.trim().is_empty())
            .count();
        let vertex_prepare_start = Instant::now();
        {
            let mut map = self.csr.lock().map_err(|_| LiteError::LockPoisoned)?;
            let csr = map.entry(COLLECTION.to_string()).or_default();
            for vertex in &vertices {
                let vertex = vertex.trim();
                if !vertex.is_empty() {
                    csr.add_node(vertex).map_err(graph_error)?;
                }
            }
        }
        prepare_duration += vertex_prepare_start.elapsed();

        let file = File::open(edge_file).map_err(io_error)?;
        let mut pending = Vec::with_capacity(BATCH_SIZE);
        let mut sorter = ExternalEdgeSorter::new(BATCH_SIZE)?;
        let mut edge_count = 0usize;
        let mut ordinal = 0u64;
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
                prepare_duration +=
                    self.graphalytics_stage_edges(&pending, &mut sorter, &mut ordinal)?;
                pending.clear();
            }
        }
        if !pending.is_empty() {
            prepare_duration +=
                self.graphalytics_stage_edges(&pending, &mut sorter, &mut ordinal)?;
        }
        let mut merge = sorter.finish()?;
        while let Some(writes) = merge.next_batch(BATCH_SIZE)? {
            self.storage.batch_write(&writes).await?;
        }
        let total_import = load_start.elapsed();
        let load_seconds = total_import.saturating_sub(prepare_duration).as_secs_f64();

        let prepare_start = Instant::now();
        {
            let mut map = self.csr.lock().map_err(|_| LiteError::LockPoisoned)?;
            map.get_mut(COLLECTION)
                .ok_or_else(|| LiteError::Storage {
                    detail: "graphalytics CSR missing after import".to_string(),
                })?
                .compact_initial_build()
                .map_err(graph_error)?;
        }
        Ok(ImportMetrics {
            vertices: vertex_count,
            edges: edge_count,
            load_seconds,
            prepare_seconds: (prepare_duration + prepare_start.elapsed()).as_secs_f64(),
        })
    }

    fn graphalytics_stage_edges(
        &self,
        edges: &[(String, String, f64)],
        sorter: &mut ExternalEdgeSorter,
        ordinal: &mut u64,
    ) -> Result<Duration, LiteError> {
        let prepare_start = Instant::now();
        {
            let mut map = self.csr.lock().map_err(|_| LiteError::LockPoisoned)?;
            let csr = map.entry(COLLECTION.to_string()).or_default();
            for (source, destination, weight) in edges {
                upsert_graphalytics_edge(csr, source, destination, *weight)?;
            }
        }
        let prepare_duration = prepare_start.elapsed();

        for (source, destination, weight) in edges {
            let properties =
                zerompk::to_msgpack_vec(&WeightProperties(*weight)).map_err(|error| {
                    LiteError::Serialization {
                        detail: error.to_string(),
                    }
                })?;
            let value = zerompk::to_msgpack_vec(&StoredGraphalyticsEdge {
                source,
                destination,
                properties: &properties,
            })
            .map_err(|error| LiteError::Serialization {
                detail: error.to_string(),
            })?;
            sorter.push(
                edge_store_key(COLLECTION, source, EDGE_LABEL, destination),
                value,
                *ordinal,
            )?;
            *ordinal = ordinal.checked_add(1).ok_or_else(|| LiteError::Storage {
                detail: "Graphalytics edge ordinal overflow".to_string(),
            })?;
        }
        Ok(prepare_duration)
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

    /// Compute unweighted BFS distances over the undirected projection.
    pub fn graphalytics_bfs_distances(&self, source: &str) -> Result<Vec<i64>, LiteError> {
        let map = self.csr.lock().map_err(|_| LiteError::LockPoisoned)?;
        let csr = map.get(COLLECTION).ok_or_else(|| LiteError::Storage {
            detail: "graphalytics CSR is not loaded".to_string(),
        })?;
        let source_id = csr.node_id_raw(source).ok_or_else(|| LiteError::Storage {
            detail: format!("source vertex '{source}' is absent"),
        })?;
        Ok(csr.bfs_both_distances_raw(source_id))
    }

    /// Materialize previously computed BFS distances as a query result.
    pub fn graphalytics_bfs_result(&self, distance: Vec<i64>) -> Result<QueryResult, LiteError> {
        let map = self.csr.lock().map_err(|_| LiteError::LockPoisoned)?;
        let csr = map.get(COLLECTION).ok_or_else(|| LiteError::Storage {
            detail: "graphalytics CSR is not loaded".to_string(),
        })?;
        if distance.len() != csr.node_count() {
            return Err(LiteError::Storage {
                detail: "Graphalytics BFS distance count does not match the CSR".to_string(),
            });
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

    /// Run and materialize BFS for callers that do not need separate timing.
    pub fn graphalytics_bfs(&self, source: &str) -> Result<QueryResult, LiteError> {
        self.graphalytics_bfs_result(self.graphalytics_bfs_distances(source)?)
    }
}

fn upsert_graphalytics_edge(
    csr: &mut CsrIndex,
    source: &str,
    destination: &str,
    weight: f64,
) -> Result<(), LiteError> {
    if csr.edge_weight(source, EDGE_LABEL, destination).is_some() {
        csr.remove_edge(source, EDGE_LABEL, destination);
        csr.compact().map_err(graph_error)?;
    }
    csr.add_edge_weighted(source, EDGE_LABEL, destination, weight)
        .map_err(graph_error)
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn duplicate_edge_uses_last_weight_in_csr() {
        let mut csr = CsrIndex::new();
        upsert_graphalytics_edge(&mut csr, "a", "b", 1.0).unwrap();
        upsert_graphalytics_edge(&mut csr, "a", "b", 2.5).unwrap();
        assert_eq!(csr.edge_weight("a", EDGE_LABEL, "b"), Some(2.5));
        assert_eq!(csr.edge_count(), 1);
    }

    #[test]
    fn specialized_edge_encoding_matches_stored_value_shape() {
        let properties = zerompk::to_msgpack_vec(&WeightProperties(2.5)).unwrap();
        let value = zerompk::to_msgpack_vec(&StoredGraphalyticsEdge {
            source: "a",
            destination: "b",
            properties: &properties,
        })
        .unwrap();
        let legacy = crate::query::graph_ops::edges::edge_to_value(
            COLLECTION,
            "a",
            EDGE_LABEL,
            "b",
            &properties,
        )
        .unwrap();
        assert_eq!(
            zerompk::from_msgpack::<Value>(&value).unwrap(),
            zerompk::from_msgpack::<Value>(&legacy).unwrap()
        );

        let Value::Object(edge) = zerompk::from_msgpack::<Value>(&value).unwrap() else {
            panic!("expected edge object");
        };
        assert_eq!(edge["collection"], Value::String(COLLECTION.to_string()));
        assert_eq!(edge["src"], Value::String("a".to_string()));
        assert_eq!(edge["label"], Value::String(EDGE_LABEL.to_string()));
        assert_eq!(edge["dst"], Value::String("b".to_string()));
        let Value::Bytes(properties) = &edge["props"] else {
            panic!("expected property bytes");
        };
        let Value::Object(properties) = zerompk::from_msgpack::<Value>(properties).unwrap() else {
            panic!("expected property object");
        };
        assert_eq!(properties["weight"], Value::Float(2.5));
    }
}
