//! WAL operations for Pattern C crash safety.

use nodedb_types::timeseries::SeriesId;

use super::core::TimeseriesEngine;

/// A WAL entry for Lite Pattern C crash safety.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct WalEntry {
    /// Monotonically increasing sequence number.
    pub seq: u64,
    /// Collection name.
    pub collection: String,
    /// Series ID.
    pub series_id: SeriesId,
    /// Timestamp in milliseconds.
    pub timestamp_ms: i64,
    /// Value.
    pub value: f64,
}

impl TimeseriesEngine {
    /// Get pending WAL entries (for persistence to redb by the caller).
    pub fn pending_wal_entries(&self, since_seq: u64) -> &[WalEntry] {
        let start = self.wal_entries.partition_point(|e| e.seq <= since_seq);
        &self.wal_entries[start..]
    }

    /// Advance the WAL watermark after successful persistence.
    pub fn advance_wal_watermark(&mut self, watermark: u64) {
        self.wal_flush_watermark = watermark;
    }

    /// Compact the WAL by removing entries below the flush watermark.
    pub fn compact_wal(&mut self) {
        let watermark = self.wal_flush_watermark;
        self.wal_entries.retain(|e| e.seq > watermark);
    }

    /// Replay WAL entries after a crash. Called on cold start.
    pub fn replay_wal(&mut self, entries: &[WalEntry]) {
        for entry in entries {
            if entry.seq <= self.wal_flush_watermark {
                continue;
            }
            self.append_to_collection(
                &entry.collection,
                entry.series_id,
                entry.timestamp_ms,
                entry.value,
            );
        }
    }

    /// Current WAL sequence number.
    pub fn wal_seq(&self) -> u64 {
        self.wal_seq
    }

    /// Current WAL flush watermark.
    pub fn wal_watermark(&self) -> u64 {
        self.wal_flush_watermark
    }
}
