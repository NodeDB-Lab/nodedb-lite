//! Shared bounded weighted edge-list import pipeline.

use std::collections::HashMap;
use std::fs::File;
use std::io::{BufRead, BufReader, Read};
use std::path::Path;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use crate::analytics::GraphImportMetrics;
use crate::engine::graph::index::CsrIndex;
use crate::error::LiteError;
use crate::graph_bulk::SortedBulkEntries;
use crate::graph_diagnostics::GraphImportMeasurements;
use crate::graph_external_sort::ExternalEdgeSorter;
use crate::graph_storage::sorted_edge_write;
use crate::query::graph_ops::edges::{durable_vertex_store_key, edge_store_key};
use crate::storage::engine::StorageEngine;

/// Domain-neutral weighted edge-list import specification.
#[derive(Debug, Clone)]
pub struct WeightedEdgeListSpec {
    pub collection: String,
    pub edge_label: String,
}

impl WeightedEdgeListSpec {
    #[must_use]
    pub fn new(collection: impl Into<String>, edge_label: impl Into<String>) -> Self {
        Self {
            collection: collection.into(),
            edge_label: edge_label.into(),
        }
    }
}
const BATCH_SIZE: usize = 1_000_000;
const VERTEX_BATCH_SIZE: usize = 100_000;
const MAX_VERTEX_ID_BYTES: usize = 512 * 1024 - 64;
const MAX_EDGE_LINE_BYTES: usize = MAX_VERTEX_ID_BYTES * 2 + 128;
const MAX_PENDING_ID_BYTES: usize = 256 * 1024 * 1024;
const MAX_MERGE_KEY_BYTES: usize = 128 * 1024 * 1024;

/// Import into `storage` and a caller-owned CSR map. `bulk_load` is only valid
/// for a freshly created PageDB tree; ordinary opened stores retain the normal
/// bounded batch-write path.
pub(crate) async fn import<S: StorageEngine>(
    storage: &S,
    csr_map: &Arc<Mutex<HashMap<String, CsrIndex>>>,
    vertex_file: &Path,
    edge_file: &Path,
    spec: &WeightedEdgeListSpec,
    bulk_load: bool,
    mut diagnostics: Option<&mut GraphImportMeasurements>,
) -> Result<GraphImportMetrics, LiteError> {
    validate_spec(spec)?;
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
            record_vertex_parse(&mut diagnostics, vertex_parse_start);
            let staging = stage_vertices(csr_map, &pending_vertices, spec)?;
            record_staging(&mut diagnostics, staging);
            prepare_duration += staging;
            pending_vertices.clear();
            pending_vertex_bytes = 0;
            vertex_parse_start = diagnostics.is_some().then(Instant::now);
        }
        pending_vertex_bytes += vertex.len();
        pending_vertices.push(vertex.to_string());
        vertex_count += 1;
    }
    record_vertex_parse(&mut diagnostics, vertex_parse_start);
    if !pending_vertices.is_empty() {
        let staging = stage_vertices(csr_map, &pending_vertices, spec)?;
        record_staging(&mut diagnostics, staging);
        prepare_duration += staging;
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
        if !weight.is_finite() || weight < 0.0 || fields.next().is_some() {
            return Err(malformed_edge(&line));
        }
        let id_bytes = source.len() + destination.len();
        if pending.len() == BATCH_SIZE
            || (!pending.is_empty() && pending_id_bytes + id_bytes > MAX_PENDING_ID_BYTES)
        {
            record_edge_parse(&mut diagnostics, edge_parse_start);
            let staging = stage_edges(csr_map, &pending, &mut sorter, &mut ordinal, spec)?;
            record_staging(&mut diagnostics, staging);
            prepare_duration += staging;
            pending.clear();
            pending_id_bytes = 0;
            edge_parse_start = diagnostics.is_some().then(Instant::now);
        }
        pending_id_bytes += id_bytes;
        pending.push((source.to_string(), destination.to_string(), weight));
        edge_count += 1;
    }
    record_edge_parse(&mut diagnostics, edge_parse_start);
    if !pending.is_empty() {
        let staging = stage_edges(csr_map, &pending, &mut sorter, &mut ordinal, spec)?;
        record_staging(&mut diagnostics, staging);
        prepare_duration += staging;
    }

    // Edge records recover every endpoint on a cold reopen. Persist only
    // explicit isolates, which have no edge key from which they can be rebuilt.
    stage_isolated_vertices(csr_map, &mut sorter, &mut ordinal, &spec.collection)?;

    let merge = sorter.finish()?;
    let profile_enabled = diagnostics.is_some();
    if bulk_load {
        let mut entries =
            SortedBulkEntries::new(merge, &spec.collection, &spec.edge_label, profile_enabled);
        let mut profile = storage
            .bulk_load_sorted_unique(&mut entries, profile_enabled)
            .await?;
        let summary = entries.finish();
        if let Some(diagnostics) = diagnostics.as_deref_mut() {
            diagnostics.add_value_regeneration(summary.value_regeneration);
            if let Some(sort) = summary.sort {
                profile.operations = sort.merge_unique_records;
                diagnostics.add_sort(sort);
            }
            diagnostics.add_storage_batch_write(profile);
        }
    } else {
        let mut merge = merge;
        while let Some(edges) = merge.next_batch(BATCH_SIZE, MAX_MERGE_KEY_BYTES)? {
            let regeneration_start = profile_enabled.then(Instant::now);
            let writes = edges
                .into_iter()
                .map(|edge| sorted_edge_write(edge, &spec.collection, &spec.edge_label))
                .collect::<Result<Vec<_>, _>>()?;
            if let (Some(diagnostics), Some(start)) =
                (diagnostics.as_deref_mut(), regeneration_start)
            {
                diagnostics.add_value_regeneration(start.elapsed());
            }
            if let Some(diagnostics) = diagnostics.as_deref_mut() {
                diagnostics.add_storage_batch_write(storage.batch_write_profiled(&writes).await?);
            } else {
                storage.batch_write(&writes).await?;
            }
        }
        if let (Some(diagnostics), Some(sort)) =
            (diagnostics.as_deref_mut(), merge.take_diagnostics())
        {
            diagnostics.add_sort(sort);
        }
    }

    let total_import = load_start.elapsed();
    let load_duration = total_import.saturating_sub(prepare_duration);
    let prepare_start = Instant::now();
    {
        let mut map = csr_map.lock().map_err(|_| LiteError::LockPoisoned)?;
        map.get_mut(&spec.collection)
            .ok_or_else(|| LiteError::Storage {
                detail: "collection CSR missing after import".to_string(),
            })?
            .compact_initial_build()
            .map_err(graph_error)?;
    }
    prepare_duration += prepare_start.elapsed();
    if let Some(diagnostics) = diagnostics {
        diagnostics.finish(total_import, load_duration, prepare_duration);
        diagnostics.set_counts(vertex_count, edge_count);
    }
    Ok(GraphImportMetrics {
        vertices: vertex_count,
        edges: edge_count,
        load_seconds: load_duration.as_secs_f64(),
        prepare_seconds: prepare_duration.as_secs_f64(),
    })
}

fn stage_vertices(
    csr_map: &Arc<Mutex<HashMap<String, CsrIndex>>>,
    vertices: &[String],
    spec: &WeightedEdgeListSpec,
) -> Result<Duration, LiteError> {
    let started = Instant::now();
    let mut map = csr_map.lock().map_err(|_| LiteError::LockPoisoned)?;
    let csr = map.entry(spec.collection.clone()).or_default();
    for vertex in vertices {
        csr.add_node(vertex).map_err(graph_error)?;
    }
    Ok(started.elapsed())
}

fn stage_edges(
    csr_map: &Arc<Mutex<HashMap<String, CsrIndex>>>,
    edges: &[(String, String, f64)],
    sorter: &mut ExternalEdgeSorter,
    ordinal: &mut u64,
    spec: &WeightedEdgeListSpec,
) -> Result<Duration, LiteError> {
    let started = Instant::now();
    {
        let mut map = csr_map.lock().map_err(|_| LiteError::LockPoisoned)?;
        let csr = map.entry(spec.collection.clone()).or_default();
        for (source, destination, weight) in edges {
            upsert_weighted_edge(csr, source, destination, &spec.edge_label, *weight)?;
        }
    }
    let elapsed = started.elapsed();
    for (source, destination, weight) in edges {
        sorter.push(
            edge_store_key(&spec.collection, source, &spec.edge_label, destination),
            *weight,
            *ordinal,
        )?;
        *ordinal = ordinal.checked_add(1).ok_or_else(|| LiteError::Storage {
            detail: "weighted edge ordinal overflow".to_string(),
        })?;
    }
    Ok(elapsed)
}

fn stage_isolated_vertices(
    csr_map: &Arc<Mutex<HashMap<String, CsrIndex>>>,
    sorter: &mut ExternalEdgeSorter,
    ordinal: &mut u64,
    collection: &str,
) -> Result<(), LiteError> {
    let map = csr_map.lock().map_err(|_| LiteError::LockPoisoned)?;
    let csr = map.get(collection).ok_or_else(|| LiteError::Storage {
        detail: "collection CSR missing while persisting isolated vertices".to_string(),
    })?;
    for node in 0..csr.node_count() as u32 {
        if csr.out_degree_raw(node) == 0 && csr.in_degree_raw(node) == 0 {
            sorter.push(
                durable_vertex_store_key(collection, csr.node_name_raw(node)),
                0.0,
                *ordinal,
            )?;
            *ordinal = ordinal.checked_add(1).ok_or_else(|| LiteError::Storage {
                detail: "sorted record ordinal overflow".to_string(),
            })?;
        }
    }
    Ok(())
}

fn upsert_weighted_edge(
    csr: &mut CsrIndex,
    source: &str,
    destination: &str,
    edge_label: &str,
    weight: f64,
) -> Result<(), LiteError> {
    if csr.edge_weight(source, edge_label, destination).is_some() {
        csr.remove_edge(source, edge_label, destination);
        csr.compact().map_err(graph_error)?;
    }
    csr.add_edge_weighted(source, edge_label, destination, weight)
        .map_err(graph_error)
}

fn record_vertex_parse(
    diagnostics: &mut Option<&mut GraphImportMeasurements>,
    start: Option<Instant>,
) {
    if let (Some(diagnostics), Some(start)) = (diagnostics.as_deref_mut(), start) {
        diagnostics.add_vertex_parse(start.elapsed());
    }
}
fn record_edge_parse(
    diagnostics: &mut Option<&mut GraphImportMeasurements>,
    start: Option<Instant>,
) {
    if let (Some(diagnostics), Some(start)) = (diagnostics.as_deref_mut(), start) {
        diagnostics.add_edge_parse(start.elapsed());
    }
}
fn record_staging(diagnostics: &mut Option<&mut GraphImportMeasurements>, elapsed: Duration) {
    if let Some(diagnostics) = diagnostics.as_deref_mut() {
        diagnostics.add_csr_staging(elapsed);
    }
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
            detail: format!("weighted edge-list line exceeds the {max_bytes}-byte bound"),
        });
    }
    Ok(read)
}
fn validate_spec(spec: &WeightedEdgeListSpec) -> Result<(), LiteError> {
    if spec.collection.is_empty()
        || spec.edge_label.is_empty()
        || spec.collection.as_bytes().contains(&0)
        || spec.edge_label.as_bytes().contains(&0)
    {
        return Err(LiteError::BadRequest {
            detail: "collection and edge label must be nonempty UTF-8 strings without NUL".into(),
        });
    }
    Ok(())
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
        detail: format!("malformed weighted edge-list record: {line}"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn vertex_id_rejects_nul_delimiters() {
        assert!(validate_vertex_id("bad\0id", "bad\\0id").is_err());
    }

    #[test]
    fn bounded_line_reader_rejects_oversized_records_before_allocation_growth() {
        let mut reader = std::io::Cursor::new(b"abcdef\n");
        let mut line = String::new();
        assert!(
            read_bounded_line(&mut reader, &mut line, 4)
                .unwrap_err()
                .to_string()
                .contains("exceeds the 4-byte bound")
        );
    }
}
