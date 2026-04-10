//! Timeseries ingest: metric ingestion, downsampling, burst detection.

use nodedb_types::timeseries::{MetricSample, SeriesKey};

use super::core::{CollectionTs, TimeseriesEngine};

/// Downsampling accumulator for a single series within a resolution window.
pub(super) struct DownsampleAccumulator {
    pub window_start_ms: i64,
    pub sum: f64,
    pub count: u64,
    pub min_ts: i64,
}

/// Ingest rate estimator for burst detection (EWMA-based).
pub(super) struct IngestRateEstimator {
    rate: f64,
    baseline: f64,
    last_update_ms: i64,
    rows_since: u64,
    pub burst_duration_ms: i64,
}

impl IngestRateEstimator {
    pub fn new() -> Self {
        Self {
            rate: 0.0,
            baseline: 0.0,
            last_update_ms: 0,
            rows_since: 0,
            burst_duration_ms: 0,
        }
    }

    /// Record ingested rows. Returns whether burst mode should be active.
    pub fn record(&mut self, rows: u64, now_ms: i64) -> bool {
        self.rows_since += rows;

        if self.last_update_ms == 0 {
            self.last_update_ms = now_ms;
            return false;
        }

        let elapsed_ms = now_ms - self.last_update_ms;
        if elapsed_ms < 1000 {
            return self.burst_duration_ms > 30_000;
        }

        let elapsed_s = elapsed_ms as f64 / 1000.0;
        let instant_rate = self.rows_since as f64 / elapsed_s;

        // EWMA update.
        let alpha = 0.2;
        self.rate = alpha * instant_rate + (1.0 - alpha) * self.rate;

        // Baseline uses slower decay.
        let baseline_alpha = 0.02;
        self.baseline = baseline_alpha * instant_rate + (1.0 - baseline_alpha) * self.baseline;

        // Burst detection: rate > 10x baseline for > 30 seconds.
        if self.baseline > 0.0 && self.rate > self.baseline * 10.0 {
            self.burst_duration_ms += elapsed_ms;
        } else {
            self.burst_duration_ms = 0;
        }

        self.last_update_ms = now_ms;
        self.rows_since = 0;

        self.burst_duration_ms > 30_000
    }
}

impl TimeseriesEngine {
    /// Ingest a metric sample.
    ///
    /// Returns true if the memtable needs flushing (memory pressure).
    ///
    /// Features:
    /// - **Pre-sync downsampling**: if `sync_resolution_ms > 0`, accumulates
    ///   samples per resolution window and emits one averaged sample per window.
    /// - **WAL append**: if `wal_enabled`, appends raw sample to WAL for crash safety.
    /// - **Battery-aware**: if `battery_aware` and battery is low, doubles the
    ///   flush threshold to defer I/O.
    /// - **Burst detection**: if ingestion rate > 10x baseline for > 30s, enters
    ///   bulk import mode with higher flush threshold.
    pub fn ingest_metric(
        &mut self,
        collection: &str,
        metric_name: &str,
        tags: Vec<(String, String)>,
        sample: MetricSample,
    ) -> bool {
        let key = SeriesKey::new(metric_name, tags);
        let series_id = self.catalog.resolve(&key);

        // WAL append for Pattern C crash safety.
        if self.config.wal_enabled {
            self.wal_seq += 1;
            self.wal_entries.push(super::wal::WalEntry {
                seq: self.wal_seq,
                collection: collection.to_string(),
                series_id,
                timestamp_ms: sample.timestamp_ms,
                value: sample.value,
            });
        }

        // Pre-sync downsampling: accumulate and emit averaged samples.
        let resolution = self.config.sync_resolution_ms;
        if resolution > 0 {
            let acc_key = (collection.to_string(), series_id);
            let window_start = sample.timestamp_ms - (sample.timestamp_ms % resolution as i64);

            let emit = {
                let acc = self
                    .downsample_accumulators
                    .entry(acc_key.clone())
                    .or_insert_with(|| DownsampleAccumulator {
                        window_start_ms: window_start,
                        sum: 0.0,
                        count: 0,
                        min_ts: sample.timestamp_ms,
                    });

                if window_start != acc.window_start_ms && acc.count > 0 {
                    let avg_value = acc.sum / acc.count as f64;
                    let emit_ts = acc.window_start_ms + resolution as i64 / 2;
                    acc.window_start_ms = window_start;
                    acc.sum = sample.value;
                    acc.count = 1;
                    acc.min_ts = sample.timestamp_ms;
                    Some((emit_ts, avg_value))
                } else {
                    acc.sum += sample.value;
                    acc.count += 1;
                    if sample.timestamp_ms < acc.min_ts {
                        acc.min_ts = sample.timestamp_ms;
                    }
                    None
                }
            };

            if let Some((emit_ts, avg_value)) = emit {
                self.append_to_collection(collection, series_id, emit_ts, avg_value);
            }
        } else {
            self.append_to_collection(collection, series_id, sample.timestamp_ms, sample.value);
        }

        // Update burst detection.
        self.rate_estimator.record(1, sample.timestamp_ms);
        let bulk_threshold = self.config.bulk_import_threshold_rows;
        if bulk_threshold > 0 {
            self.bulk_import_active = self.rate_estimator.burst_duration_ms > 30_000;
        }

        self.should_flush(collection)
    }

    /// Append a sample to a collection's memtable.
    pub(super) fn append_to_collection(
        &mut self,
        collection: &str,
        series_id: nodedb_types::timeseries::SeriesId,
        timestamp_ms: i64,
        value: f64,
    ) {
        let coll = self
            .collections
            .entry(collection.to_string())
            .or_insert_with(CollectionTs::new);

        coll.timestamps.push(timestamp_ms);
        coll.values.push(value);
        coll.series_ids.push(series_id);
        coll.memory_bytes += 24;
        coll.dirty = true;
    }

    /// Determine if a collection's memtable should be flushed.
    fn should_flush(&self, collection: &str) -> bool {
        let coll = match self.collections.get(collection) {
            Some(c) => c,
            None => return false,
        };

        let base_limit = self.config.memtable_max_memory_bytes as usize;

        let limit = if self.config.battery_aware && self.battery_state.should_defer_flush() {
            base_limit * 2
        } else {
            base_limit
        };

        if self.bulk_import_active && self.config.bulk_import_threshold_rows > 0 {
            return coll.row_count() as u64 >= self.config.bulk_import_threshold_rows;
        }

        coll.memory_bytes >= limit
    }

    /// Ingest a batch of samples for one series.
    pub fn ingest_batch(
        &mut self,
        collection: &str,
        metric_name: &str,
        tags: Vec<(String, String)>,
        samples: &[MetricSample],
    ) -> bool {
        for sample in samples {
            self.ingest_metric(collection, metric_name, tags.clone(), *sample);
        }
        self.should_flush(collection)
    }
}
