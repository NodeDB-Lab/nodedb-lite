//! Flush memtable to Gorilla-compressed redb entries.

use nodedb_codec::gorilla::GorillaEncoder;
use nodedb_types::timeseries::{PartitionMeta, PartitionState};

use super::core::{FlushedPartition, TimeseriesEngine};

/// A key-value entry for redb persistence.
pub type RedbEntry = (Vec<u8>, Vec<u8>);

/// Result of flushing a collection's memtable.
pub struct FlushResult {
    /// redb key prefix for this partition.
    pub key_prefix: String,
    /// Gorilla-encoded timestamps.
    pub ts_block: Vec<u8>,
    /// Gorilla-encoded values.
    pub val_block: Vec<u8>,
    /// Raw LE u64 series IDs.
    pub series_block: Vec<u8>,
    /// Partition metadata.
    pub meta: PartitionMeta,
}

impl FlushResult {
    /// redb key-value pairs to persist this partition.
    ///
    /// Returns `Err` if metadata serialization fails.
    pub fn to_redb_entries(&self) -> Result<Vec<RedbEntry>, sonic_rs::Error> {
        let meta_bytes = sonic_rs::to_vec(&self.meta)?;
        Ok(vec![
            (
                format!("{}:ts", self.key_prefix).into_bytes(),
                self.ts_block.clone(),
            ),
            (
                format!("{}:val", self.key_prefix).into_bytes(),
                self.val_block.clone(),
            ),
            (
                format!("{}:series", self.key_prefix).into_bytes(),
                self.series_block.clone(),
            ),
            (format!("{}:meta", self.key_prefix).into_bytes(), meta_bytes),
        ])
    }
}

impl TimeseriesEngine {
    /// Flush a collection's memtable to Gorilla-compressed redb entries.
    pub fn flush(&mut self, collection: &str) -> Option<FlushResult> {
        let coll = self.collections.get_mut(collection)?;
        if coll.timestamps.is_empty() {
            return None;
        }

        let row_count = coll.row_count();
        let min_ts = *coll.timestamps.iter().min().unwrap_or(&0);
        let max_ts = *coll.timestamps.iter().max().unwrap_or(&0);

        let mut ts_encoder = GorillaEncoder::new();
        for &ts in &coll.timestamps {
            ts_encoder.encode(ts, 0.0);
        }
        let ts_block = ts_encoder.finish();

        let mut val_encoder = GorillaEncoder::new();
        for (i, &val) in coll.values.iter().enumerate() {
            val_encoder.encode(i as i64, val);
        }
        let val_block = val_encoder.finish();

        let series_block: Vec<u8> = coll
            .series_ids
            .iter()
            .flat_map(|&id| id.to_le_bytes())
            .collect();

        let key_prefix = format!("ts:{collection}:{min_ts}");

        let meta = PartitionMeta {
            min_ts,
            max_ts,
            row_count: row_count as u64,
            size_bytes: (ts_block.len() + val_block.len() + series_block.len()) as u64,
            schema_version: 1,
            state: PartitionState::Sealed,
            interval_ms: (max_ts - min_ts) as u64,
            last_flushed_wal_lsn: 0,
            column_stats: std::collections::HashMap::new(),
        };

        let partition = FlushedPartition {
            meta: meta.clone(),
            key_prefix: key_prefix.clone(),
        };
        coll.partitions.push(partition);

        // Snapshot timestamps/values for continuous aggregate refresh
        // before clearing the memtable. We need to release the mutable
        // borrow on `coll` (via `self.collections`) before borrowing
        // `self.continuous_agg_mgr`.
        let ts_snapshot: Vec<i64> = coll.timestamps.clone();
        let val_snapshot: Vec<f64> = coll.values.clone();

        coll.timestamps.clear();
        coll.values.clear();
        coll.series_ids.clear();
        coll.memory_bytes = 0;

        // Fire continuous aggregate refresh now that `coll` borrow is released.
        self.continuous_agg_mgr
            .on_flush_simple(collection, &ts_snapshot, &val_snapshot);

        Some(FlushResult {
            key_prefix,
            ts_block,
            val_block,
            series_block,
            meta,
        })
    }
}
