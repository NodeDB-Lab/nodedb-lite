// SPDX-License-Identifier: Apache-2.0

use std::cmp::Ordering;
use std::collections::BinaryHeap;
use std::fs::File;
use std::io::{BufReader, BufWriter, Read, Write};
use std::path::PathBuf;

use nodedb_types::Namespace;

use crate::error::LiteError;
use crate::storage::engine::WriteOp;

const MAX_OPEN_RUNS: usize = 64;

#[derive(Debug)]
struct SortableEdge {
    key: Vec<u8>,
    value: Vec<u8>,
    ordinal: u64,
}

pub(crate) struct ExternalEdgeSorter {
    temp_dir: tempfile::TempDir,
    run_capacity: usize,
    pending: Vec<SortableEdge>,
    runs: Vec<PathBuf>,
}

impl ExternalEdgeSorter {
    pub(crate) fn new(run_capacity: usize) -> Result<Self, LiteError> {
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
            runs: Vec::new(),
        })
    }

    pub(crate) fn push(
        &mut self,
        key: Vec<u8>,
        value: Vec<u8>,
        ordinal: u64,
    ) -> Result<(), LiteError> {
        self.pending.push(SortableEdge {
            key,
            value,
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
        self.pending.sort_unstable_by(|left, right| {
            left.key
                .cmp(&right.key)
                .then_with(|| left.ordinal.cmp(&right.ordinal))
        });
        let path = self
            .temp_dir
            .path()
            .join(format!("run-{:08}.bin", self.runs.len()));
        let file = File::create(&path).map_err(io_error)?;
        let mut writer = BufWriter::with_capacity(1 << 20, file);
        for record in self.pending.drain(..) {
            write_record(&mut writer, &record)?;
        }
        writer.flush().map_err(io_error)?;
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
            _temp_dir: self.temp_dir,
        })
    }
}

pub(crate) struct ExternalEdgeMerge {
    readers: Vec<BufReader<File>>,
    heap: BinaryHeap<HeapRecord>,
    // Declared last so run readers close before TempDir removes their files.
    _temp_dir: tempfile::TempDir,
}

impl ExternalEdgeMerge {
    pub(crate) fn next_batch(
        &mut self,
        capacity: usize,
    ) -> Result<Option<Vec<WriteOp>>, LiteError> {
        if capacity == 0 {
            return Err(storage_error("merge batch capacity must be positive"));
        }
        let mut writes = Vec::with_capacity(capacity);
        while writes.len() < capacity {
            let Some(first) = self.pop_and_advance()? else {
                break;
            };
            let key = first.key;
            let mut value = first.value;
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
                    value = duplicate.value;
                    ordinal = duplicate.ordinal;
                }
            }
            writes.push(WriteOp::Put {
                ns: Namespace::Graph,
                key,
                value,
            });
        }
        Ok((!writes.is_empty()).then_some(writes))
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
    let value_len = u32::try_from(record.value.len())
        .map_err(|_| storage_error("external-sort value exceeds u32 length"))?;
    writer.write_all(&key_len.to_le_bytes()).map_err(io_error)?;
    writer
        .write_all(&value_len.to_le_bytes())
        .map_err(io_error)?;
    writer
        .write_all(&record.ordinal.to_le_bytes())
        .map_err(io_error)?;
    writer.write_all(&record.key).map_err(io_error)?;
    writer.write_all(&record.value).map_err(io_error)?;
    Ok(())
}

fn read_record(reader: &mut impl Read) -> Result<Option<SortableEdge>, LiteError> {
    let mut header = [0u8; 16];
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
    let value_len = u32::from_le_bytes(header[4..8].try_into().expect("fixed header")) as usize;
    let ordinal = u64::from_le_bytes(header[8..16].try_into().expect("fixed header"));
    let mut key = vec![0u8; key_len];
    let mut value = vec![0u8; value_len];
    reader.read_exact(&mut key).map_err(io_error)?;
    reader.read_exact(&mut value).map_err(io_error)?;
    Ok(Some(SortableEdge {
        key,
        value,
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

    fn put_parts(write: WriteOp) -> (Vec<u8>, Vec<u8>) {
        match write {
            WriteOp::Put { key, value, .. } => (key, value),
            WriteOp::Delete { .. } => panic!("unexpected delete"),
        }
    }

    #[test]
    fn merge_orders_runs_and_keeps_last_duplicate() {
        let mut sorter = ExternalEdgeSorter::new(2).unwrap();
        sorter.push(b"c".to_vec(), b"c0".to_vec(), 0).unwrap();
        sorter.push(b"a".to_vec(), b"a1".to_vec(), 1).unwrap();
        sorter.push(b"b".to_vec(), b"b2".to_vec(), 2).unwrap();
        sorter.push(b"a".to_vec(), b"a3".to_vec(), 3).unwrap();
        sorter.push(b"d".to_vec(), b"d4".to_vec(), 4).unwrap();
        let mut merge = sorter.finish().unwrap();
        let first: Vec<_> = merge
            .next_batch(2)
            .unwrap()
            .unwrap()
            .into_iter()
            .map(put_parts)
            .collect();
        let second: Vec<_> = merge
            .next_batch(2)
            .unwrap()
            .unwrap()
            .into_iter()
            .map(put_parts)
            .collect();
        assert_eq!(
            first,
            vec![
                (b"a".to_vec(), b"a3".to_vec()),
                (b"b".to_vec(), b"b2".to_vec())
            ]
        );
        assert_eq!(
            second,
            vec![
                (b"c".to_vec(), b"c0".to_vec()),
                (b"d".to_vec(), b"d4".to_vec())
            ]
        );
        assert!(merge.next_batch(2).unwrap().is_none());
    }

    #[test]
    fn merge_keeps_last_duplicate_within_one_run() {
        let mut sorter = ExternalEdgeSorter::new(3).unwrap();
        sorter.push(b"a".to_vec(), b"first".to_vec(), 1).unwrap();
        sorter.push(b"a".to_vec(), b"last".to_vec(), 2).unwrap();
        sorter.push(b"b".to_vec(), b"value".to_vec(), 3).unwrap();
        let mut merge = sorter.finish().unwrap();
        let writes: Vec<_> = merge
            .next_batch(3)
            .unwrap()
            .unwrap()
            .into_iter()
            .map(put_parts)
            .collect();
        assert_eq!(
            writes,
            vec![
                (b"a".to_vec(), b"last".to_vec()),
                (b"b".to_vec(), b"value".to_vec())
            ]
        );
    }

    #[test]
    fn dropping_merge_removes_spill_directory() {
        let mut sorter = ExternalEdgeSorter::new(1).unwrap();
        let path = sorter.temp_dir.path().to_path_buf();
        sorter.push(b"a".to_vec(), b"value".to_vec(), 0).unwrap();
        let merge = sorter.finish().unwrap();
        assert!(path.exists());
        drop(merge);
        assert!(!path.exists());
    }

    #[test]
    fn truncated_record_is_rejected() {
        let mut header = Vec::new();
        header.extend_from_slice(&1u32.to_le_bytes());
        header.extend_from_slice(&2u32.to_le_bytes());
        header.extend_from_slice(&0u64.to_le_bytes());
        header.extend_from_slice(b"ab");
        let error = read_record(&mut header.as_slice()).unwrap_err();
        assert!(error.to_string().contains("failed to fill whole buffer"));
    }
}
