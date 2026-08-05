// SPDX-License-Identifier: Apache-2.0

use std::cmp::Ordering;
use std::collections::BinaryHeap;
use std::fs::File;
use std::io::{BufReader, BufWriter, Read, Write};
use std::path::PathBuf;
use std::time::Instant;

use crate::graphalytics_diagnostics::SortDiagnostics;

use crate::error::LiteError;

const MAX_OPEN_RUNS: usize = 64;
const MAX_KEY_BYTES: usize = 2 * 1024 * 1024;
const MAX_PENDING_KEY_BYTES: usize = 512 * 1024 * 1024;

#[derive(Debug)]
struct SortableEdge {
    key: Vec<u8>,
    weight: f64,
    ordinal: u64,
}

pub(crate) struct SortedEdge {
    pub(crate) key: Vec<u8>,
    pub(crate) weight: f64,
}

pub(crate) struct ExternalEdgeSorter {
    temp_dir: tempfile::TempDir,
    run_capacity: usize,
    pending: Vec<SortableEdge>,
    pending_key_bytes: usize,
    runs: Vec<PathBuf>,
    diagnostics: Option<SortDiagnostics>,
}

impl ExternalEdgeSorter {
    pub(crate) fn new(run_capacity: usize, diagnostics_enabled: bool) -> Result<Self, LiteError> {
        if run_capacity == 0 {
            return Err(storage_error("external-sort run capacity must be positive"));
        }
        Ok(Self {
            temp_dir: tempfile::Builder::new()
                .prefix("nodedb-lite-graphalytics-")
                .tempdir()
                .map_err(io_error)?,
            run_capacity,
            pending: Vec::with_capacity(run_capacity),
            pending_key_bytes: 0,
            runs: Vec::new(),
            diagnostics: diagnostics_enabled.then(SortDiagnostics::default),
        })
    }

    pub(crate) fn push(
        &mut self,
        key: Vec<u8>,
        weight: f64,
        ordinal: u64,
    ) -> Result<(), LiteError> {
        if key.len() > MAX_KEY_BYTES {
            return Err(storage_error(format!(
                "external-sort key exceeds the {MAX_KEY_BYTES}-byte bound"
            )));
        }
        if !self.pending.is_empty()
            && self.pending_key_bytes.saturating_add(key.len()) > MAX_PENDING_KEY_BYTES
        {
            self.spill_run()?;
        }
        self.pending_key_bytes += key.len();
        self.pending.push(SortableEdge {
            key,
            weight,
            ordinal,
        });
        if self.pending.len() == self.run_capacity {
            self.spill_run()?;
        }
        Ok(())
    }

    fn spill_run(&mut self) -> Result<(), LiteError> {
        if self.pending.is_empty() {
            return Ok(());
        }
        if self.runs.len() == MAX_OPEN_RUNS {
            return Err(storage_error(format!(
                "Graphalytics external sort exceeds the {MAX_OPEN_RUNS}-run bound"
            )));
        }
        let records = self.pending.len() as u64;
        let sort_start = self.diagnostics.as_ref().map(|_| Instant::now());
        self.pending.sort_unstable_by(|left, right| {
            left.key
                .cmp(&right.key)
                .then_with(|| left.ordinal.cmp(&right.ordinal))
        });
        if let (Some(diagnostics), Some(sort_start)) = (self.diagnostics.as_mut(), sort_start) {
            diagnostics.spill_sort += sort_start.elapsed();
        }
        let path = self
            .temp_dir
            .path()
            .join(format!("run-{:08}.bin", self.runs.len()));
        let write_start = self.diagnostics.as_ref().map(|_| Instant::now());
        let file = File::create(&path).map_err(io_error)?;
        let mut writer = BufWriter::with_capacity(1 << 20, file);
        for record in self.pending.drain(..) {
            write_record(&mut writer, &record)?;
        }
        self.pending_key_bytes = 0;
        writer.flush().map_err(io_error)?;
        if let (Some(diagnostics), Some(write_start)) = (self.diagnostics.as_mut(), write_start) {
            diagnostics.spill_write += write_start.elapsed();
            diagnostics.spill_runs += 1;
            diagnostics.spill_records += records;
            // Diagnostics cannot make an otherwise successful spill fail.
            diagnostics.spill_bytes += std::fs::metadata(&path)
                .map(|metadata| metadata.len())
                .unwrap_or_default();
        }
        self.runs.push(path);
        Ok(())
    }

    pub(crate) fn finish(mut self) -> Result<ExternalEdgeMerge, LiteError> {
        self.spill_run()?;
        let mut readers = Vec::with_capacity(self.runs.len());
        let mut heap = BinaryHeap::new();
        for (run, path) in self.runs.iter().enumerate() {
            let mut reader = BufReader::with_capacity(1 << 20, File::open(path).map_err(io_error)?);
            if let Some(record) = read_record(&mut reader)? {
                heap.push(HeapRecord { run, record });
            }
            readers.push(reader);
        }
        Ok(ExternalEdgeMerge {
            readers,
            heap,
            pending_output: None,
            diagnostics: self.diagnostics,
            _temp_dir: self.temp_dir,
        })
    }
}

pub(crate) struct ExternalEdgeMerge {
    readers: Vec<BufReader<File>>,
    heap: BinaryHeap<HeapRecord>,
    pending_output: Option<SortedEdge>,
    diagnostics: Option<SortDiagnostics>,
    // Declared last so run readers close before TempDir removes their files.
    _temp_dir: tempfile::TempDir,
}

impl ExternalEdgeMerge {
    pub(crate) fn next_batch(
        &mut self,
        capacity: usize,
        key_byte_capacity: usize,
    ) -> Result<Option<Vec<SortedEdge>>, LiteError> {
        if capacity == 0 || key_byte_capacity == 0 {
            return Err(storage_error("merge batch capacities must be positive"));
        }
        if self.diagnostics.is_some() {
            let started = Instant::now();
            let batch = self.next_batch_unprofiled(capacity, key_byte_capacity)?;
            if let Some(diagnostics) = self.diagnostics.as_mut() {
                diagnostics.merge_batches += started.elapsed();
                diagnostics.merge_unique_records +=
                    batch.as_ref().map_or(0, |edges| edges.len() as u64);
            }
            return Ok(batch);
        }
        self.next_batch_unprofiled(capacity, key_byte_capacity)
    }

    // Keep the diagnostics-off merge path identical to the original hot loop.
    fn next_batch_unprofiled(
        &mut self,
        capacity: usize,
        key_byte_capacity: usize,
    ) -> Result<Option<Vec<SortedEdge>>, LiteError> {
        let mut edges = Vec::with_capacity(capacity);
        let mut key_bytes = 0usize;
        while edges.len() < capacity {
            let next = match self.pending_output.take() {
                Some(edge) => Some(edge),
                None => self.next_unique_edge()?,
            };
            let Some(edge) = next else {
                break;
            };
            if !edges.is_empty() && key_bytes + edge.key.len() > key_byte_capacity {
                self.pending_output = Some(edge);
                break;
            }
            key_bytes += edge.key.len();
            edges.push(edge);
        }
        Ok((!edges.is_empty()).then_some(edges))
    }

    pub(crate) fn take_diagnostics(&mut self) -> Option<SortDiagnostics> {
        self.diagnostics.take()
    }

    fn next_unique_edge(&mut self) -> Result<Option<SortedEdge>, LiteError> {
        let Some(first) = self.pop_and_advance()? else {
            return Ok(None);
        };
        let key = first.key;
        let mut weight = first.weight;
        let mut ordinal = first.ordinal;
        while self
            .heap
            .peek()
            .is_some_and(|candidate| candidate.record.key == key)
        {
            let duplicate = self
                .pop_and_advance()?
                .expect("peeked merge record remains available");
            if duplicate.ordinal > ordinal {
                weight = duplicate.weight;
                ordinal = duplicate.ordinal;
            }
        }
        Ok(Some(SortedEdge { key, weight }))
    }

    fn pop_and_advance(&mut self) -> Result<Option<SortableEdge>, LiteError> {
        let Some(item) = self.heap.pop() else {
            return Ok(None);
        };
        if let Some(record) = read_record(&mut self.readers[item.run])? {
            self.heap.push(HeapRecord {
                run: item.run,
                record,
            });
        }
        Ok(Some(item.record))
    }
}

struct HeapRecord {
    run: usize,
    record: SortableEdge,
}

impl PartialEq for HeapRecord {
    fn eq(&self, other: &Self) -> bool {
        self.run == other.run
            && self.record.ordinal == other.record.ordinal
            && self.record.key == other.record.key
    }
}

impl Eq for HeapRecord {}

impl PartialOrd for HeapRecord {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for HeapRecord {
    fn cmp(&self, other: &Self) -> Ordering {
        other
            .record
            .key
            .cmp(&self.record.key)
            .then_with(|| other.record.ordinal.cmp(&self.record.ordinal))
            .then_with(|| other.run.cmp(&self.run))
    }
}

fn write_record(writer: &mut impl Write, record: &SortableEdge) -> Result<(), LiteError> {
    let key_len = u32::try_from(record.key.len())
        .map_err(|_| storage_error("external-sort key exceeds u32 length"))?;
    writer.write_all(&key_len.to_le_bytes()).map_err(io_error)?;
    writer
        .write_all(&record.ordinal.to_le_bytes())
        .map_err(io_error)?;
    writer
        .write_all(&record.weight.to_bits().to_le_bytes())
        .map_err(io_error)?;
    writer.write_all(&record.key).map_err(io_error)?;
    Ok(())
}

fn read_record(reader: &mut impl Read) -> Result<Option<SortableEdge>, LiteError> {
    let mut header = [0u8; 20];
    let mut read = 0usize;
    while read < header.len() {
        let count = reader.read(&mut header[read..]).map_err(io_error)?;
        if count == 0 {
            if read == 0 {
                return Ok(None);
            }
            return Err(storage_error("truncated external-sort record header"));
        }
        read += count;
    }
    let key_len = u32::from_le_bytes(header[0..4].try_into().expect("fixed header")) as usize;
    if key_len > MAX_KEY_BYTES {
        return Err(storage_error(format!(
            "external-sort key exceeds the {MAX_KEY_BYTES}-byte bound"
        )));
    }
    let ordinal = u64::from_le_bytes(header[4..12].try_into().expect("fixed header"));
    let weight_bits = u64::from_le_bytes(header[12..20].try_into().expect("fixed header"));
    let mut key = vec![0u8; key_len];
    reader.read_exact(&mut key).map_err(io_error)?;
    Ok(Some(SortableEdge {
        key,
        weight: f64::from_bits(weight_bits),
        ordinal,
    }))
}

fn io_error(error: std::io::Error) -> LiteError {
    LiteError::Storage {
        detail: error.to_string(),
    }
}

fn storage_error(detail: impl Into<String>) -> LiteError {
    LiteError::Storage {
        detail: detail.into(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn merge_orders_runs_and_keeps_last_duplicate() {
        let mut sorter = ExternalEdgeSorter::new(2, true).unwrap();
        sorter.push(b"c".to_vec(), 0.0, 0).unwrap();
        sorter.push(b"a".to_vec(), 1.0, 1).unwrap();
        sorter.push(b"b".to_vec(), 2.0, 2).unwrap();
        sorter.push(b"a".to_vec(), 3.0, 3).unwrap();
        sorter.push(b"d".to_vec(), 4.0, 4).unwrap();
        let mut merge = sorter.finish().unwrap();
        let first: Vec<_> = merge
            .next_batch(2, 1024)
            .unwrap()
            .unwrap()
            .into_iter()
            .map(|edge| (edge.key, edge.weight))
            .collect();
        let second: Vec<_> = merge
            .next_batch(2, 1024)
            .unwrap()
            .unwrap()
            .into_iter()
            .map(|edge| (edge.key, edge.weight))
            .collect();
        assert_eq!(first, vec![(b"a".to_vec(), 3.0), (b"b".to_vec(), 2.0)]);
        assert_eq!(second, vec![(b"c".to_vec(), 0.0), (b"d".to_vec(), 4.0)]);
        assert!(merge.next_batch(2, 1024).unwrap().is_none());
    }

    #[test]
    fn disabled_diagnostics_keep_the_merge_path_uninstrumented() {
        let mut sorter = ExternalEdgeSorter::new(1, false).unwrap();
        sorter.push(b"a".to_vec(), 1.0, 0).unwrap();
        sorter.push(b"b".to_vec(), 2.0, 1).unwrap();
        let mut merge = sorter.finish().unwrap();
        assert_eq!(merge.next_batch(1, 1024).unwrap().unwrap().len(), 1);
        assert_eq!(merge.next_batch(1, 1024).unwrap().unwrap().len(), 1);
        assert!(merge.next_batch(1, 1024).unwrap().is_none());
        assert!(merge.take_diagnostics().is_none());
    }

    #[test]
    fn diagnostics_count_spills_and_merge_records() {
        let mut sorter = ExternalEdgeSorter::new(2, true).unwrap();
        sorter.push(b"a".to_vec(), 1.0, 0).unwrap();
        sorter.push(b"a".to_vec(), 2.0, 1).unwrap();
        sorter.push(b"b".to_vec(), 3.0, 2).unwrap();
        let mut merge = sorter.finish().unwrap();
        assert_eq!(merge.next_batch(10, 1024).unwrap().unwrap().len(), 2);
        let diagnostics = merge.take_diagnostics().unwrap();
        assert_eq!(diagnostics.spill_runs, 2);
        assert_eq!(diagnostics.spill_records, 3);
        assert!(diagnostics.spill_bytes > 0);
        assert_eq!(diagnostics.merge_unique_records, 2);
        assert!(diagnostics.merge_batches > std::time::Duration::ZERO);
    }

    #[test]
    fn merge_keeps_last_duplicate_within_one_run() {
        let mut sorter = ExternalEdgeSorter::new(3, true).unwrap();
        sorter.push(b"a".to_vec(), 1.0, 1).unwrap();
        sorter.push(b"a".to_vec(), 2.0, 2).unwrap();
        sorter.push(b"b".to_vec(), 3.0, 3).unwrap();
        let mut merge = sorter.finish().unwrap();
        let edges: Vec<_> = merge
            .next_batch(3, 1024)
            .unwrap()
            .unwrap()
            .into_iter()
            .map(|edge| (edge.key, edge.weight))
            .collect();
        assert_eq!(edges, vec![(b"a".to_vec(), 2.0), (b"b".to_vec(), 3.0)]);
    }

    #[test]
    fn diagnostics_count_unique_outputs_across_pending_batches() {
        let mut sorter = ExternalEdgeSorter::new(2, true).unwrap();
        sorter.push(b"aa".to_vec(), 1.0, 0).unwrap();
        sorter.push(b"bb".to_vec(), 2.0, 1).unwrap();
        let mut merge = sorter.finish().unwrap();
        assert_eq!(merge.next_batch(2, 2).unwrap().unwrap().len(), 1);
        assert_eq!(merge.next_batch(2, 2).unwrap().unwrap().len(), 1);
        assert!(merge.next_batch(2, 2).unwrap().is_none());
        let diagnostics = merge.take_diagnostics().unwrap();
        assert_eq!(diagnostics.spill_records, 2);
        assert_eq!(diagnostics.merge_unique_records, 2);
    }

    #[test]
    fn spill_preserves_negative_zero_weight_bits() {
        let mut sorter = ExternalEdgeSorter::new(1, true).unwrap();
        sorter.push(b"a".to_vec(), -0.0, 0).unwrap();
        let mut merge = sorter.finish().unwrap();
        let edge = merge.next_batch(1, 1024).unwrap().unwrap().pop().unwrap();
        assert_eq!(edge.weight.to_bits(), (-0.0f64).to_bits());
    }

    #[test]
    fn dropping_merge_removes_spill_directory() {
        let mut sorter = ExternalEdgeSorter::new(1, true).unwrap();
        let path = sorter.temp_dir.path().to_path_buf();
        sorter.push(b"a".to_vec(), 1.0, 0).unwrap();
        let merge = sorter.finish().unwrap();
        assert!(path.exists());
        drop(merge);
        assert!(!path.exists());
    }

    #[test]
    fn truncated_record_is_rejected() {
        let mut header = Vec::new();
        header.extend_from_slice(&1u32.to_le_bytes());
        header.extend_from_slice(&0u64.to_le_bytes());
        header.extend_from_slice(&1.0f64.to_bits().to_le_bytes());
        let error = read_record(&mut header.as_slice()).unwrap_err();
        assert!(error.to_string().contains("failed to fill whole buffer"));
    }
}
