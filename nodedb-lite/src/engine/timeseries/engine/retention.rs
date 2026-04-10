//! Retention and sync-aware lifecycle management.

use super::core::TimeseriesEngine;

/// Warning emitted when retention drops unsynced data.
#[derive(Debug, Clone)]
pub struct UnsyncedDropWarning {
    pub collection: String,
    pub partition_key: String,
    pub min_ts: i64,
    pub max_ts: i64,
    pub row_count: u64,
}

impl TimeseriesEngine {
    /// Drop partitions older than the retention period.
    pub fn apply_retention(&mut self, now_ms: i64) -> Vec<String> {
        if self.config.retention_period_ms == 0 {
            return Vec::new();
        }
        let cutoff = now_ms - self.config.retention_period_ms as i64;
        let mut dropped = Vec::new();

        for coll in self.collections.values_mut() {
            coll.partitions.retain(|p| {
                if p.meta.max_ts < cutoff {
                    dropped.push(p.key_prefix.clone());
                    false
                } else {
                    true
                }
            });
        }
        dropped
    }

    /// Apply retention with sync-awareness.
    ///
    /// **Default behavior** (`retain_until_synced = false`): drops by age.
    /// **Guarded behavior** (`retain_until_synced = true`): keeps unsynced data.
    pub fn apply_retention_with_sync(
        &mut self,
        now_ms: i64,
    ) -> (Vec<String>, Vec<UnsyncedDropWarning>) {
        if self.config.retention_period_ms == 0 {
            return (Vec::new(), Vec::new());
        }
        let cutoff = now_ms - self.config.retention_period_ms as i64;
        let retain_until_synced = self.config.retain_until_synced;
        let mut dropped = Vec::new();
        let mut warnings = Vec::new();

        for (collection, coll) in &mut self.collections {
            coll.partitions.retain(|p| {
                if p.meta.max_ts >= cutoff {
                    return true;
                }
                let has_unsynced = p.meta.last_flushed_wal_lsn == 0;
                if has_unsynced && retain_until_synced {
                    return true;
                }
                if has_unsynced {
                    warnings.push(UnsyncedDropWarning {
                        collection: collection.clone(),
                        partition_key: p.key_prefix.clone(),
                        min_ts: p.meta.min_ts,
                        max_ts: p.meta.max_ts,
                        row_count: p.meta.row_count,
                    });
                }
                dropped.push(p.key_prefix.clone());
                false
            });
        }

        (dropped, warnings)
    }
}
