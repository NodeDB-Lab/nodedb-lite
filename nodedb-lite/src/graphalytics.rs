// SPDX-License-Identifier: Apache-2.0

//! Feature-gated embedded runner support for LDBC Graphalytics.

use std::fs::File;
use std::io::{BufRead, BufReader, Read};
use std::path::Path;
use std::time::{Duration, Instant};

use nodedb_graph::params::{AlgoParams, GraphAlgorithm};
use nodedb_types::Namespace;
use nodedb_types::result::QueryResult;
use nodedb_types::value::Value;

use crate::NodeDbLite;
use crate::engine::graph::index::CsrIndex;
use crate::error::LiteError;
use crate::graphalytics_external_sort::{ExternalEdgeSorter, SortedEdge};
use crate::query::graph_ops::algorithms::run_algo;
use crate::query::graph_ops::edges::edge_store_key;
use crate::storage::engine::{StorageEngine, WriteOp};

const COLLECTION: &str = "graphalytics";
const EDGE_LABEL: &str = "EDGE";
const BATCH_SIZE: usize = 1_000_000;
const VERTEX_BATCH_SIZE: usize = 100_000;
const MAX_VERTEX_ID_BYTES: usize = 512 * 1024 - 64;
const MAX_EDGE_LINE_BYTES: usize = MAX_VERTEX_ID_BYTES * 2 + 128;
const MAX_PENDING_ID_BYTES: usize = 256 * 1024 * 1024;
const MAX_MERGE_KEY_BYTES: usize = 128 * 1024 * 1024;

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
        let mut reader = BufReader::with_capacity(1 << 20, file);
        let mut line = String::new();
        let mut pending_vertices = Vec::with_capacity(VERTEX_BATCH_SIZE);
        let mut pending_vertex_bytes = 0usize;
        let mut vertex_count = 0usize;
        while read_bounded_line(&mut reader, &mut line, MAX_VERTEX_ID_BYTES + 2)? != 0 {
            let vertex = line.trim();
            if vertex.is_empty() {
                continue;
            }
            validate_vertex_id(vertex, &line)?;
            if pending_vertices.len() == VERTEX_BATCH_SIZE
                || (!pending_vertices.is_empty()
                    && pending_vertex_bytes + vertex.len() > MAX_PENDING_ID_BYTES)
            {
                prepare_duration += self.graphalytics_stage_vertices(&pending_vertices)?;
                pending_vertices.clear();
                pending_vertex_bytes = 0;
            }
            pending_vertex_bytes += vertex.len();
            pending_vertices.push(vertex.to_string());
            vertex_count += 1;
        }
        if !pending_vertices.is_empty() {
            prepare_duration += self.graphalytics_stage_vertices(&pending_vertices)?;
        }

        let file = File::open(edge_file).map_err(io_error)?;
        let mut reader = BufReader::with_capacity(1 << 20, file);
        let mut pending = Vec::with_capacity(BATCH_SIZE);
        let mut pending_id_bytes = 0usize;
        let mut sorter = ExternalEdgeSorter::new(BATCH_SIZE)?;
        let mut edge_count = 0usize;
        let mut ordinal = 0u64;
        while read_bounded_line(&mut reader, &mut line, MAX_EDGE_LINE_BYTES)? != 0 {
            let mut fields = line.split_whitespace();
            let source = fields.next().ok_or_else(|| malformed_edge(&line))?;
            let destination = fields.next().ok_or_else(|| malformed_edge(&line))?;
            validate_vertex_id(source, &line)?;
            validate_vertex_id(destination, &line)?;
            let weight = fields
                .next()
                .ok_or_else(|| malformed_edge(&line))?
                .parse::<f64>()
                .map_err(|_| malformed_edge(&line))?;
            if !weight.is_finite() || weight < 0.0 {
                return Err(malformed_edge(&line));
            }
            let id_bytes = source.len() + destination.len();
            if pending.len() == BATCH_SIZE
                || (!pending.is_empty() && pending_id_bytes + id_bytes > MAX_PENDING_ID_BYTES)
            {
                prepare_duration +=
                    self.graphalytics_stage_edges(&pending, &mut sorter, &mut ordinal)?;
                pending.clear();
                pending_id_bytes = 0;
            }
            pending_id_bytes += id_bytes;
            pending.push((source.to_string(), destination.to_string(), weight));
            edge_count += 1;
        }
        if !pending.is_empty() {
            prepare_duration +=
                self.graphalytics_stage_edges(&pending, &mut sorter, &mut ordinal)?;
        }
        let mut merge = sorter.finish()?;
        while let Some(edges) = merge.next_batch(BATCH_SIZE, MAX_MERGE_KEY_BYTES)? {
            let writes = edges
                .into_iter()
                .map(sorted_edge_write)
                .collect::<Result<Vec<_>, _>>()?;
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

    fn graphalytics_stage_vertices(&self, vertices: &[String]) -> Result<Duration, LiteError> {
        let prepare_start = Instant::now();
        let mut map = self.csr.lock().map_err(|_| LiteError::LockPoisoned)?;
        let csr = map.entry(COLLECTION.to_string()).or_default();
        for vertex in vertices {
            csr.add_node(vertex).map_err(graph_error)?;
        }
        Ok(prepare_start.elapsed())
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
            sorter.push(
                edge_store_key(COLLECTION, source, EDGE_LABEL, destination),
                *weight,
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

fn sorted_edge_write(edge: SortedEdge) -> Result<WriteOp, LiteError> {
    let prefix_len = COLLECTION.len() + 1;
    if edge.key.get(..COLLECTION.len()) != Some(COLLECTION.as_bytes())
        || edge.key.get(COLLECTION.len()) != Some(&0)
    {
        return Err(malformed_stored_edge());
    }
    let suffix = edge
        .key
        .get(prefix_len..)
        .ok_or_else(malformed_stored_edge)?;
    let source_end = suffix
        .iter()
        .position(|byte| *byte == 0)
        .map(|offset| prefix_len + offset)
        .ok_or_else(malformed_stored_edge)?;
    let label_start = source_end + 1;
    let label_end = label_start + EDGE_LABEL.len();
    let destination_start = label_end + 1;
    if edge.key.get(label_start..label_end) != Some(EDGE_LABEL.as_bytes())
        || edge.key.get(label_end) != Some(&0)
        || destination_start > edge.key.len()
    {
        return Err(malformed_stored_edge());
    }
    let source = std::str::from_utf8(&edge.key[prefix_len..source_end])
        .map_err(|_| malformed_stored_edge())?;
    let destination =
        std::str::from_utf8(&edge.key[destination_start..]).map_err(|_| malformed_stored_edge())?;
    let properties =
        zerompk::to_msgpack_vec(&WeightProperties(edge.weight)).map_err(serialization_error)?;
    let value = zerompk::to_msgpack_vec(&StoredGraphalyticsEdge {
        source,
        destination,
        properties: &properties,
    })
    .map_err(serialization_error)?;
    Ok(WriteOp::Put {
        ns: Namespace::Graph,
        key: edge.key,
        value,
    })
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

fn read_bounded_line(
    reader: &mut impl BufRead,
    line: &mut String,
    max_bytes: usize,
) -> Result<usize, LiteError> {
    line.clear();
    let read = reader
        .take((max_bytes + 1) as u64)
        .read_line(line)
        .map_err(io_error)?;
    if read > max_bytes {
        return Err(LiteError::Storage {
            detail: format!("Graphalytics input line exceeds the {max_bytes}-byte bound"),
        });
    }
    Ok(read)
}

fn validate_vertex_id(vertex: &str, line: &str) -> Result<(), LiteError> {
    if vertex.as_bytes().contains(&0) || vertex.len() > MAX_VERTEX_ID_BYTES {
        return Err(malformed_edge(line));
    }
    Ok(())
}

fn serialization_error(error: impl std::fmt::Display) -> LiteError {
    LiteError::Serialization {
        detail: error.to_string(),
    }
}

fn malformed_stored_edge() -> LiteError {
    LiteError::Storage {
        detail: "malformed Graphalytics edge key".to_string(),
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn bounded_line_reader_rejects_oversized_records_before_allocation_growth() {
        let mut reader = std::io::Cursor::new(b"abcdef\n");
        let mut line = String::new();
        let error = read_bounded_line(&mut reader, &mut line, 4).unwrap_err();
        assert!(error.to_string().contains("exceeds the 4-byte bound"));
    }

    #[test]
    fn vertex_ids_reject_ambiguous_nul_delimiters() {
        let error = validate_vertex_id("a\0b", "a\0b c 1").unwrap_err();
        assert!(error.to_string().contains("malformed Graphalytics edge"));
    }

    #[test]
    fn duplicate_edge_uses_last_weight_in_csr() {
        let mut csr = CsrIndex::new();
        upsert_graphalytics_edge(&mut csr, "a", "b", 1.0).unwrap();
        upsert_graphalytics_edge(&mut csr, "a", "b", 2.5).unwrap();
        assert_eq!(csr.edge_weight("a", EDGE_LABEL, "b"), Some(2.5));
        assert_eq!(csr.edge_count(), 1);
    }

    #[test]
    fn compact_spill_regenerates_the_exact_stored_value_shape() {
        let WriteOp::Put { key, value, .. } = sorted_edge_write(SortedEdge {
            key: edge_store_key(COLLECTION, "a", EDGE_LABEL, "b"),
            weight: 2.5,
        })
        .unwrap() else {
            panic!("expected put");
        };
        assert_eq!(key, edge_store_key(COLLECTION, "a", EDGE_LABEL, "b"));
        let properties = zerompk::to_msgpack_vec(&WeightProperties(2.5)).unwrap();
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
