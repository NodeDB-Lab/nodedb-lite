//! Sync watermarks and push payload construction.

use std::collections::HashMap;

use nodedb_types::timeseries::SeriesId;

use super::core::TimeseriesEngine;

impl TimeseriesEngine {
    /// Get the sync watermark for a series.
    pub fn sync_watermark(&self, series_id: SeriesId) -> u64 {
        self.sync_watermarks.get(&series_id).copied().unwrap_or(0)
    }

    /// Update sync watermark after Origin acknowledges a push.
    pub fn acknowledge_sync(&mut self, collection: &str, max_synced_ts: u64) {
        if let Some(coll) = self.collections.get_mut(collection) {
            for i in 0..coll.series_ids.len() {
                let sid = coll.series_ids[i];
                let ts = coll.timestamps[i] as u64;
                if ts <= max_synced_ts {
                    self.sync_watermarks
                        .entry(sid)
                        .and_modify(|w| *w = (*w).max(ts))
                        .or_insert(ts);
                }
            }
            coll.dirty = false;
        }
    }

    /// Get all current sync watermarks (for persistence).
    pub fn export_watermarks(&self) -> &HashMap<SeriesId, u64> {
        &self.sync_watermarks
    }

    /// Import watermarks from redb (cold start restore).
    pub fn import_watermarks(&mut self, watermarks: HashMap<SeriesId, u64>) {
        self.sync_watermarks = watermarks;
    }

    /// Get collections that have unsynced data.
    pub fn dirty_collections(&self) -> Vec<&str> {
        self.collections
            .iter()
            .filter(|(_, c)| c.dirty || !c.timestamps.is_empty())
            .map(|(name, _)| name.as_str())
            .collect()
    }

    /// Assign a sync LSN and advance the counter.
    pub fn assign_sync_lsn(&mut self) -> u64 {
        let lsn = self.next_sync_lsn;
        self.next_sync_lsn += 1;
        lsn
    }

    /// Build a sync push payload for a collection.
    pub fn build_sync_payload(
        &mut self,
        collection: &str,
        lite_id: &str,
    ) -> Option<nodedb_types::sync::wire::TimeseriesPushMsg> {
        let coll = self.collections.get(collection)?;
        if coll.timestamps.is_empty() {
            return None;
        }

        let sync_resolution = self.config.sync_resolution_ms;

        let mut ts_to_sync = Vec::new();
        let mut val_to_sync = Vec::new();
        for i in 0..coll.timestamps.len() {
            let sid = coll.series_ids[i];
            let ts = coll.timestamps[i];
            let last_synced_ts = self.sync_watermark(sid) as i64;
            if ts > last_synced_ts {
                ts_to_sync.push(ts);
                val_to_sync.push(coll.values[i]);
            }
        }

        if ts_to_sync.is_empty() {
            return None;
        }

        let (final_ts, final_vals) = if sync_resolution > 0 {
            let mut buckets: std::collections::BTreeMap<i64, (f64, u64)> =
                std::collections::BTreeMap::new();
            for i in 0..ts_to_sync.len() {
                let bucket = (ts_to_sync[i] / sync_resolution as i64) * sync_resolution as i64;
                let entry = buckets.entry(bucket).or_insert((0.0, 0));
                entry.0 += val_to_sync[i];
                entry.1 += 1;
            }
            let ts: Vec<i64> = buckets.keys().copied().collect();
            let vals: Vec<f64> = buckets
                .values()
                .map(|(sum, count)| sum / *count as f64)
                .collect();
            (ts, vals)
        } else {
            (ts_to_sync, val_to_sync)
        };

        let mut ts_enc = nodedb_codec::GorillaEncoder::new();
        for &t in &final_ts {
            ts_enc.encode(t, 0.0);
        }
        let mut val_enc = nodedb_codec::GorillaEncoder::new();
        for (i, &v) in final_vals.iter().enumerate() {
            val_enc.encode(i as i64, v);
        }

        let min_ts = final_ts.iter().copied().min().unwrap_or(0);
        let max_ts = final_ts.iter().copied().max().unwrap_or(0);

        let watermarks: HashMap<u64, u64> =
            self.sync_watermarks.iter().map(|(&k, &v)| (k, v)).collect();

        Some(nodedb_types::sync::wire::TimeseriesPushMsg {
            lite_id: lite_id.to_string(),
            collection: collection.to_string(),
            ts_block: ts_enc.finish(),
            val_block: val_enc.finish(),
            series_block: Vec::new(),
            sample_count: final_ts.len() as u64,
            min_ts,
            max_ts,
            watermarks,
        })
    }

    /// Get the configured sync interval in milliseconds.
    pub fn sync_interval_ms(&self) -> u64 {
        self.config.sync_interval_ms
    }
}
