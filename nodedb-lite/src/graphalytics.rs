// SPDX-License-Identifier: Apache-2.0

//! Feature-gated embedded runner support for LDBC Graphalytics.

use std::fs::File;
use std::io::{BufRead, BufReader, Read};
use std::path::Path;
use std::time::{Duration, Instant};

use nodedb_graph::params::{AlgoParams, GraphAlgorithm};
use nodedb_types::result::QueryResult;
use nodedb_types::value::Value;

use crate::NodeDbLite;
use crate::engine::graph::index::CsrIndex;
use crate::error::LiteError;
use crate::graphalytics_diagnostics::GraphalyticsLoadDiagnostics;
use crate::graphalytics_external_sort::ExternalEdgeSorter;
use crate::graphalytics_storage::sorted_edge_write;
use crate::query::graph_ops::algorithms::{
    materialize_graphalytics_raw, run_algo, run_graphalytics_raw,
    run_graphalytics_raw_prevalidated_sssp, validate_graphalytics_sssp_weights,
};
use crate::query::graph_ops::edges::edge_store_key;
use crate::query::graph_ops::graphalytics_results::GraphalyticsRawValues;
use crate::storage::engine::StorageEngine;

const COLLECTION: &str = "graphalytics";
const EDGE_LABEL: &str = "EDGE";
const BATCH_SIZE: usize = 1_000_000;
const VERTEX_BATCH_SIZE: usize = 100_000;
const MAX_VERTEX_ID_BYTES: usize = 512 * 1024 - 64;
const MAX_EDGE_LINE_BYTES: usize = MAX_VERTEX_ID_BYTES * 2 + 128;
const MAX_PENDING_ID_BYTES: usize = 256 * 1024 * 1024;
const MAX_MERGE_KEY_BYTES: usize = 128 * 1024 * 1024;

/// Opaque dense primitive output passed from timed computation to untimed materialization.
#[doc(hidden)]
pub struct GraphalyticsRawResult(GraphalyticsRawValues);

/// Proof that the current Graphalytics CSR passed dataset-wide SSSP weight validation.
#[doc(hidden)]
pub struct GraphalyticsValidatedSssp(());

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
        self.graphalytics_import_internal(vertex_file, edge_file, None)
            .await
    }

    #[doc(hidden)]
    pub async fn graphalytics_import_with_diagnostics(
        &self,
        vertex_file: &Path,
        edge_file: &Path,
        enabled: bool,
    ) -> Result<(ImportMetrics, Option<GraphalyticsLoadDiagnostics>), LiteError> {
        let mut diagnostics = enabled.then(GraphalyticsLoadDiagnostics::new);
        let metrics = self
            .graphalytics_import_internal(vertex_file, edge_file, diagnostics.as_mut())
            .await?;
        Ok((metrics, diagnostics))
    }

    async fn graphalytics_import_internal(
        &self,
        vertex_file: &Path,
        edge_file: &Path,
        mut diagnostics: Option<&mut GraphalyticsLoadDiagnostics>,
    ) -> Result<ImportMetrics, LiteError> {
        let load_start = Instant::now();
        let mut prepare_duration = Duration::ZERO;
        let file = File::open(vertex_file).map_err(io_error)?;
        let mut reader = BufReader::with_capacity(1 << 20, file);
        let mut line = String::new();
        let mut pending_vertices = Vec::with_capacity(VERTEX_BATCH_SIZE);
        let mut pending_vertex_bytes = 0usize;
        let mut vertex_count = 0usize;
        let mut vertex_parse_start = diagnostics.is_some().then(Instant::now);
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
                if let (Some(diagnostics), Some(start)) =
                    (diagnostics.as_deref_mut(), vertex_parse_start)
                {
                    diagnostics.add_vertex_parse(start.elapsed());
                }
                let staging = self.graphalytics_stage_vertices(&pending_vertices)?;
                if let Some(diagnostics) = diagnostics.as_deref_mut() {
                    diagnostics.add_csr_staging(staging);
                }
                prepare_duration += staging;
                pending_vertices.clear();
                vertex_parse_start = diagnostics.is_some().then(Instant::now);
                pending_vertex_bytes = 0;
            }
            pending_vertex_bytes += vertex.len();
            pending_vertices.push(vertex.to_string());
            vertex_count += 1;
        }
        if !pending_vertices.is_empty() {
            if let (Some(diagnostics), Some(start)) =
                (diagnostics.as_deref_mut(), vertex_parse_start)
            {
                diagnostics.add_vertex_parse(start.elapsed());
            }
            let staging = self.graphalytics_stage_vertices(&pending_vertices)?;
            if let Some(diagnostics) = diagnostics.as_deref_mut() {
                diagnostics.add_csr_staging(staging);
            }
            prepare_duration += staging;
        } else if let (Some(diagnostics), Some(start)) =
            (diagnostics.as_deref_mut(), vertex_parse_start)
        {
            diagnostics.add_vertex_parse(start.elapsed());
        }

        let file = File::open(edge_file).map_err(io_error)?;
        let mut reader = BufReader::with_capacity(1 << 20, file);
        let mut pending = Vec::with_capacity(BATCH_SIZE);
        let mut pending_id_bytes = 0usize;
        let mut sorter = ExternalEdgeSorter::new(BATCH_SIZE, diagnostics.is_some())?;
        let mut edge_count = 0usize;
        let mut ordinal = 0u64;
        let mut edge_parse_start = diagnostics.is_some().then(Instant::now);
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
                if let (Some(diagnostics), Some(start)) =
                    (diagnostics.as_deref_mut(), edge_parse_start)
                {
                    diagnostics.add_edge_parse(start.elapsed());
                }
                let staging = self.graphalytics_stage_edges(&pending, &mut sorter, &mut ordinal)?;
                if let Some(diagnostics) = diagnostics.as_deref_mut() {
                    diagnostics.add_csr_staging(staging);
                }
                prepare_duration += staging;
                pending.clear();
                edge_parse_start = diagnostics.is_some().then(Instant::now);
                pending_id_bytes = 0;
            }
            pending_id_bytes += id_bytes;
            pending.push((source.to_string(), destination.to_string(), weight));
            edge_count += 1;
        }
        if !pending.is_empty() {
            if let (Some(diagnostics), Some(start)) = (diagnostics.as_deref_mut(), edge_parse_start)
            {
                diagnostics.add_edge_parse(start.elapsed());
            }
            let staging = self.graphalytics_stage_edges(&pending, &mut sorter, &mut ordinal)?;
            if let Some(diagnostics) = diagnostics.as_deref_mut() {
                diagnostics.add_csr_staging(staging);
            }
            prepare_duration += staging;
        } else if let (Some(diagnostics), Some(start)) =
            (diagnostics.as_deref_mut(), edge_parse_start)
        {
            diagnostics.add_edge_parse(start.elapsed());
        }
        let mut merge = sorter.finish()?;
        while let Some(edges) = merge.next_batch(BATCH_SIZE, MAX_MERGE_KEY_BYTES)? {
            let regeneration_start = diagnostics.is_some().then(Instant::now);
            let writes = edges
                .into_iter()
                .map(sorted_edge_write)
                .collect::<Result<Vec<_>, _>>()?;
            if let (Some(diagnostics), Some(regeneration_start)) =
                (diagnostics.as_deref_mut(), regeneration_start)
            {
                diagnostics.add_value_regeneration(regeneration_start.elapsed());
            }
            if let Some(diagnostics) = diagnostics.as_deref_mut() {
                diagnostics
                    .add_storage_batch_write(self.storage.batch_write_profiled(&writes).await?);
            } else {
                self.storage.batch_write(&writes).await?;
            }
        }
        if let (Some(diagnostics), Some(sort)) =
            (diagnostics.as_deref_mut(), merge.take_diagnostics())
        {
            diagnostics.add_sort(sort);
        }
        let total_import = load_start.elapsed();
        let load_duration = total_import.saturating_sub(prepare_duration);
        let load_seconds = load_duration.as_secs_f64();

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
        let prepare_duration = prepare_duration + prepare_start.elapsed();
        if let Some(diagnostics) = diagnostics.as_deref_mut() {
            diagnostics.finish(total_import, load_duration, prepare_duration);
            diagnostics.set_counts(vertex_count, edge_count);
        }
        Ok(ImportMetrics {
            vertices: vertex_count,
            edges: edge_count,
            load_seconds,
            prepare_seconds: prepare_duration.as_secs_f64(),
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
        if matches!(
            algorithm,
            GraphAlgorithm::PageRank
                | GraphAlgorithm::Wcc
                | GraphAlgorithm::Lcc
                | GraphAlgorithm::Sssp
                | GraphAlgorithm::LabelPropagation
        ) {
            let raw = self.graphalytics_raw_run(algorithm, source)?;
            return self.graphalytics_raw_result(algorithm, raw);
        }
        if algorithm == GraphAlgorithm::Diameter {
            return Err(LiteError::Storage {
                detail: "diameter is not part of the Graphalytics runner".to_string(),
            });
        }
        run_algo(&self.csr, algorithm, &graphalytics_params(source))
    }

    /// Validate SSSP weights before a separately timed primitive execution.
    #[doc(hidden)]
    pub fn graphalytics_validate_sssp_weights(
        &self,
    ) -> Result<GraphalyticsValidatedSssp, LiteError> {
        validate_graphalytics_sssp_weights(&self.csr, COLLECTION)?;
        Ok(GraphalyticsValidatedSssp(()))
    }

    /// Produce a dense primitive result, performing all required admission checks.
    #[doc(hidden)]
    pub fn graphalytics_raw_run(
        &self,
        algorithm: GraphAlgorithm,
        source: &str,
    ) -> Result<GraphalyticsRawResult, LiteError> {
        if algorithm == GraphAlgorithm::Diameter {
            return Err(LiteError::Storage {
                detail: "diameter is not part of the Graphalytics runner".to_string(),
            });
        }
        let raw = run_graphalytics_raw(&self.csr, algorithm, &graphalytics_params(source))?;
        Ok(GraphalyticsRawResult(raw))
    }

    /// Run timed SSSP using an unforgeable proof from the pre-timer validation step.
    #[doc(hidden)]
    pub fn graphalytics_sssp_raw_prevalidated(
        &self,
        source: &str,
        _validated: GraphalyticsValidatedSssp,
    ) -> Result<GraphalyticsRawResult, LiteError> {
        let raw = run_graphalytics_raw_prevalidated_sssp(
            &self.csr,
            GraphAlgorithm::Sssp,
            &graphalytics_params(source),
        )?;
        Ok(GraphalyticsRawResult(raw))
    }

    /// Convert a previously computed dense primitive result into Graphalytics output.
    #[doc(hidden)]
    pub fn graphalytics_raw_result(
        &self,
        algorithm: GraphAlgorithm,
        raw: GraphalyticsRawResult,
    ) -> Result<QueryResult, LiteError> {
        let GraphalyticsRawResult(raw) = raw;
        let mut result = materialize_graphalytics_raw(&self.csr, COLLECTION, algorithm, raw)?;
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

fn graphalytics_params(source: &str) -> AlgoParams {
    AlgoParams {
        collection: COLLECTION.to_string(),
        damping: Some(0.85),
        max_iterations: Some(10),
        // Positive minimum bypasses the generic non-positive fallback and
        // effectively disables early stopping for the required fixed count.
        tolerance: Some(f64::MIN_POSITIVE),
        source_node: Some(source.to_string()),
        direction: Some("both".to_string()),
        ..Default::default()
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
}
