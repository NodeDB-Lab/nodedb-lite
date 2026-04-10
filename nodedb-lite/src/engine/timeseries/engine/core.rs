//! Core `TimeseriesEngine` struct, constructors, and accessors.

use std::collections::HashMap;

use nodedb_codec::gorilla::GorillaDecoder;
use nodedb_types::timeseries::{PartitionMeta, SeriesCatalog, SeriesId, TieredPartitionConfig};

use super::ingest::{DownsampleAccumulator, IngestRateEstimator};
use super::wal::WalEntry;

/// Lite timeseries engine.
///
/// Not `Send` — owned by a single task. The `NodeDbLite` wrapper handles
/// async bridging.
pub struct TimeseriesEngine {
    pub(super) collections: HashMap<String, CollectionTs>,
    pub(super) catalog: SeriesCatalog,
    pub(super) config: TieredPartitionConfig,
    pub(super) sync_watermarks: HashMap<SeriesId, u64>,
    pub(super) next_sync_lsn: u64,
    pub(super) battery_state: nodedb_types::timeseries::BatteryState,
    pub(super) downsample_accumulators: HashMap<(String, SeriesId), DownsampleAccumulator>,
    pub(super) wal_entries: Vec<WalEntry>,
    pub(super) wal_flush_watermark: u64,
    pub(super) wal_seq: u64,
    pub(super) rate_estimator: IngestRateEstimator,
    pub(super) bulk_import_active: bool,
    /// Continuous aggregate manager: auto-refreshes on flush.
    pub(crate) continuous_agg_mgr: super::continuous_agg::LiteContinuousAggManager,
}

/// Per-collection timeseries state.
pub(super) struct CollectionTs {
    pub timestamps: Vec<i64>,
    pub values: Vec<f64>,
    pub series_ids: Vec<SeriesId>,
    pub memory_bytes: usize,
    pub partitions: Vec<FlushedPartition>,
    pub dirty: bool,
}

/// A flushed partition stored in redb.
#[derive(Debug, Clone)]
pub(super) struct FlushedPartition {
    pub meta: PartitionMeta,
    pub key_prefix: String,
}

impl CollectionTs {
    pub fn new() -> Self {
        Self {
            timestamps: Vec::with_capacity(4096),
            values: Vec::with_capacity(4096),
            series_ids: Vec::with_capacity(4096),
            dirty: false,
            memory_bytes: 0,
            partitions: Vec::new(),
        }
    }

    pub fn row_count(&self) -> usize {
        self.timestamps.len()
    }
}

impl Default for TimeseriesEngine {
    fn default() -> Self {
        Self::new()
    }
}

impl TimeseriesEngine {
    /// Create with Lite defaults.
    pub fn new() -> Self {
        Self::with_config(TieredPartitionConfig::lite_defaults())
    }

    /// Create with custom config.
    pub fn with_config(config: TieredPartitionConfig) -> Self {
        Self {
            collections: HashMap::new(),
            catalog: SeriesCatalog::new(),
            config,
            sync_watermarks: HashMap::new(),
            next_sync_lsn: 1,
            battery_state: nodedb_types::timeseries::BatteryState::Unknown,
            downsample_accumulators: HashMap::new(),
            wal_entries: Vec::new(),
            wal_flush_watermark: 0,
            wal_seq: 0,
            rate_estimator: IngestRateEstimator::new(),
            bulk_import_active: false,
            continuous_agg_mgr: super::continuous_agg::LiteContinuousAggManager::new(),
        }
    }

    /// Update battery state (called by host application).
    pub fn set_battery_state(&mut self, state: nodedb_types::timeseries::BatteryState) {
        self.battery_state = state;
    }

    pub fn collection_names(&self) -> Vec<&str> {
        self.collections.keys().map(|s| s.as_str()).collect()
    }

    pub fn row_count(&self, collection: &str) -> usize {
        self.collections
            .get(collection)
            .map(|c| c.row_count())
            .unwrap_or(0)
    }

    pub fn memory_bytes(&self, collection: &str) -> usize {
        self.collections
            .get(collection)
            .map(|c| c.memory_bytes)
            .unwrap_or(0)
    }

    pub fn partition_count(&self, collection: &str) -> usize {
        self.collections
            .get(collection)
            .map(|c| c.partitions.len())
            .unwrap_or(0)
    }

    pub fn catalog(&self) -> &SeriesCatalog {
        &self.catalog
    }

    pub fn config(&self) -> &TieredPartitionConfig {
        &self.config
    }

    /// Decode a flushed timestamp block (Gorilla-encoded).
    pub fn decode_timestamps(block: &[u8]) -> Vec<i64> {
        let mut dec = GorillaDecoder::new(block);
        dec.decode_all().into_iter().map(|(ts, _)| ts).collect()
    }

    /// Decode a flushed value block (Gorilla-encoded).
    pub fn decode_values(block: &[u8]) -> Vec<f64> {
        let mut dec = GorillaDecoder::new(block);
        dec.decode_all().into_iter().map(|(_, v)| v).collect()
    }

    /// Decode a flushed series_id block (raw LE u64).
    pub fn decode_series_ids(block: &[u8]) -> Vec<SeriesId> {
        block
            .chunks_exact(8)
            .map(|chunk| {
                let arr: [u8; 8] = chunk
                    .try_into()
                    .expect("chunks_exact(8) guarantees 8 bytes");
                u64::from_le_bytes(arr)
            })
            .collect()
    }
}
