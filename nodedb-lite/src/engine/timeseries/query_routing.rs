//! Timeseries query routing: Local, Cloud, or Hybrid execution.
//!
//! Lite holds its own agent's data. Origin holds aggregated data from
//! all agents. Query routing determines where execution happens.
//!
//! - **Local** (default): Lite columnar engine scans local partitions only.
//!   No network needed. No `__source` tag — Lite only has its own data.
//!
//! - **Cloud**: Lite forwards SQL to Origin via sync WebSocket. Origin
//!   auto-filters by `__source=<this_lite_id>` unless the query explicitly
//!   asks for fleet-wide data (e.g., `GROUP BY __source`).
//!
//! - **Hybrid**: Local data + pre-computed fleet aggregates from Origin
//!   via shape subscriptions. Returns local + stale shape data with
//!   `shape_staleness_ms` indicator. No per-query round-trip for fleet data.

use std::collections::HashMap;

use serde::{Deserialize, Serialize};

use nodedb_types::timeseries::{SeriesId, TimeRange};

/// Query execution scope.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
pub enum QueryScope {
    /// Execute locally on Lite engine only. Default. No network.
    #[default]
    Local,
    /// Forward to Origin via sync WebSocket. Requires connectivity.
    Cloud,
    /// Local data + cached fleet aggregates from shape subscriptions.
    /// Works offline with stale fleet data.
    Hybrid,
}

/// A timeseries shape subscription: Origin pushes pre-computed
/// downsampled aggregates to Lite at a fixed interval.
///
/// Example: `fleet_avg_cpu` at 5m resolution — Lite receives the
/// fleet-wide average CPU every 5 minutes without per-query round-trips.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TimeseriesShape {
    /// Shape identifier.
    pub shape_id: String,
    /// Collection on Origin.
    pub collection: String,
    /// Metric to aggregate.
    pub metric: String,
    /// Tags to group by (empty = aggregate all devices).
    pub group_by: Vec<String>,
    /// Aggregation function: "avg", "sum", "min", "max", "count".
    pub aggregate: String,
    /// Bucket interval in milliseconds (e.g., 300_000 = 5 minutes).
    pub interval_ms: u64,
}

/// Cached shape data received from Origin via sync.
///
/// Stored locally as read-only series, updated by each shape push.
#[derive(Debug, Clone)]
pub struct CachedShapeData {
    pub shape: TimeseriesShape,
    /// Cached aggregated buckets: `(bucket_start_ms, value)`.
    pub buckets: Vec<(i64, f64)>,
    /// When this cache was last updated (epoch ms).
    pub last_updated_ms: u64,
    /// Max timestamp in cached data.
    pub max_ts: i64,
}

impl CachedShapeData {
    /// How stale the cached data is, relative to `now_ms`.
    pub fn staleness_ms(&self, now_ms: u64) -> u64 {
        now_ms.saturating_sub(self.last_updated_ms)
    }
}

/// Manages timeseries shape subscriptions and cached fleet data on Lite.
#[derive(Debug, Default)]
pub struct TimeseriesShapeManager {
    /// Active shape subscriptions: `shape_id → cached data`.
    shapes: HashMap<String, CachedShapeData>,
}

impl TimeseriesShapeManager {
    pub fn new() -> Self {
        Self::default()
    }

    /// Register a shape subscription.
    pub fn subscribe(&mut self, shape: TimeseriesShape) {
        let shape_id = shape.shape_id.clone();
        self.shapes.insert(
            shape_id,
            CachedShapeData {
                shape,
                buckets: Vec::new(),
                last_updated_ms: 0,
                max_ts: 0,
            },
        );
    }

    /// Unsubscribe from a shape.
    pub fn unsubscribe(&mut self, shape_id: &str) {
        self.shapes.remove(shape_id);
    }

    /// Update cached data from an Origin shape push.
    pub fn update(&mut self, shape_id: &str, buckets: Vec<(i64, f64)>, now_ms: u64) {
        if let Some(cached) = self.shapes.get_mut(shape_id) {
            cached.max_ts = buckets.iter().map(|(ts, _)| *ts).max().unwrap_or(0);
            cached.buckets = buckets;
            cached.last_updated_ms = now_ms;
        }
    }

    /// Query cached fleet data for a shape.
    ///
    /// Returns `(buckets, staleness_ms)`. Staleness = 0 means fresh data.
    /// If the shape doesn't exist, returns empty with max staleness.
    pub fn query(&self, shape_id: &str, range: &TimeRange, now_ms: u64) -> (Vec<(i64, f64)>, u64) {
        match self.shapes.get(shape_id) {
            Some(cached) => {
                let filtered: Vec<(i64, f64)> = cached
                    .buckets
                    .iter()
                    .filter(|(ts, _)| range.contains(*ts))
                    .copied()
                    .collect();
                (filtered, cached.staleness_ms(now_ms))
            }
            None => (Vec::new(), u64::MAX),
        }
    }

    /// List active shape subscriptions.
    pub fn active_shapes(&self) -> Vec<&TimeseriesShape> {
        self.shapes.values().map(|c| &c.shape).collect()
    }

    /// Number of active subscriptions.
    pub fn len(&self) -> usize {
        self.shapes.len()
    }

    pub fn is_empty(&self) -> bool {
        self.shapes.is_empty()
    }

    /// Export for persistence (redb serialization).
    pub fn export(&self) -> Vec<(String, CachedShapeData)> {
        self.shapes
            .iter()
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect()
    }

    /// Import from persistence.
    pub fn import(&mut self, entries: Vec<(String, CachedShapeData)>) {
        for (k, v) in entries {
            self.shapes.insert(k, v);
        }
    }
}

/// Result of a hybrid query: local data + fleet shape data.
#[derive(Debug)]
pub struct HybridQueryResult {
    /// Local scan results: `(timestamp, value, series_id)`.
    pub local: Vec<(i64, f64, SeriesId)>,
    /// Fleet aggregate from shape cache: `(bucket_start, value)`.
    pub fleet: Vec<(i64, f64)>,
    /// Staleness of the fleet data in milliseconds.
    /// 0 = fresh. u64::MAX = no shape subscription.
    pub shape_staleness_ms: u64,
    /// Whether local data was available (always true for Local scope).
    pub local_available: bool,
}

/// Parameters for a routed timeseries query.
pub struct RoutedQueryParams<'a> {
    pub scope: QueryScope,
    pub collection: &'a str,
    pub range: &'a TimeRange,
    pub bucket_ms: Option<i64>,
    pub shape_id: Option<&'a str>,
    pub now_ms: u64,
}

/// Execute a timeseries query with routing based on `QueryScope`.
///
/// - `Local`: scans the local `TimeseriesEngine`.
/// - `Cloud`: forwards SQL to Origin via `NodeDbRemote` (pgwire). Requires
///   an active pgwire connection — NOT the sync WebSocket. Query and sync
///   are separate channels (production pattern).
/// - `Hybrid`: local scan + cached shape data. No per-query network hop.
///   Fleet data degrades gracefully with staleness indicator.
pub fn execute_routed_query(
    params: &RoutedQueryParams<'_>,
    engine: &super::engine::TimeseriesEngine,
    shape_mgr: &TimeseriesShapeManager,
) -> HybridQueryResult {
    let RoutedQueryParams {
        scope,
        collection,
        range,
        bucket_ms,
        shape_id,
        now_ms,
    } = params;
    let _ = bucket_ms; // Reserved for future aggregation pushdown.

    let local = match scope {
        QueryScope::Local | QueryScope::Hybrid => engine.scan(collection, range),
        QueryScope::Cloud => {
            // Cloud-only: no local scan. Caller must forward SQL
            // via NodeDbRemote::execute_sql() over pgwire separately.
            Vec::new()
        }
    };

    let (fleet, shape_staleness_ms) = match scope {
        QueryScope::Hybrid => {
            if let Some(sid) = shape_id {
                shape_mgr.query(sid, range, *now_ms)
            } else {
                (Vec::new(), u64::MAX)
            }
        }
        _ => (Vec::new(), u64::MAX),
    };

    HybridQueryResult {
        local_available: !matches!(scope, QueryScope::Cloud),
        local,
        fleet,
        shape_staleness_ms,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_shape(id: &str) -> TimeseriesShape {
        TimeseriesShape {
            shape_id: id.into(),
            collection: "metrics".into(),
            metric: "cpu_usage".into(),
            group_by: vec![],
            aggregate: "avg".into(),
            interval_ms: 300_000,
        }
    }

    #[test]
    fn subscribe_and_query() {
        let mut mgr = TimeseriesShapeManager::new();
        mgr.subscribe(make_shape("fleet_cpu"));
        assert_eq!(mgr.len(), 1);

        // No data yet — empty result.
        let (buckets, staleness) = mgr.query("fleet_cpu", &TimeRange::new(0, 1_000_000), 1000);
        assert!(buckets.is_empty());
        assert_eq!(staleness, 1000); // 1000 - 0 = 1000ms stale

        // Update with data.
        mgr.update(
            "fleet_cpu",
            vec![(300_000, 45.0), (600_000, 52.0), (900_000, 48.0)],
            1_000_000,
        );

        let (buckets, staleness) = mgr.query("fleet_cpu", &TimeRange::new(0, 1_000_000), 1_000_000);
        assert_eq!(buckets.len(), 3);
        assert_eq!(staleness, 0); // Just updated.
    }

    #[test]
    fn unsubscribe_removes() {
        let mut mgr = TimeseriesShapeManager::new();
        mgr.subscribe(make_shape("s1"));
        assert_eq!(mgr.len(), 1);
        mgr.unsubscribe("s1");
        assert_eq!(mgr.len(), 0);
    }

    #[test]
    fn query_range_filtering() {
        let mut mgr = TimeseriesShapeManager::new();
        mgr.subscribe(make_shape("s1"));
        mgr.update(
            "s1",
            vec![(100, 1.0), (200, 2.0), (300, 3.0), (400, 4.0)],
            500,
        );

        // Query range [200, 300].
        let (buckets, _) = mgr.query("s1", &TimeRange::new(200, 300), 500);
        assert_eq!(buckets.len(), 2);
        assert_eq!(buckets[0], (200, 2.0));
        assert_eq!(buckets[1], (300, 3.0));
    }

    #[test]
    fn missing_shape_returns_max_staleness() {
        let mgr = TimeseriesShapeManager::new();
        let (buckets, staleness) = mgr.query("nonexistent", &TimeRange::new(0, 1000), 1000);
        assert!(buckets.is_empty());
        assert_eq!(staleness, u64::MAX);
    }

    #[test]
    fn export_import_roundtrip() {
        let mut mgr = TimeseriesShapeManager::new();
        mgr.subscribe(make_shape("s1"));
        mgr.update("s1", vec![(100, 42.0)], 200);

        let exported = mgr.export();
        let mut mgr2 = TimeseriesShapeManager::new();
        mgr2.import(exported);
        assert_eq!(mgr2.len(), 1);

        let (buckets, _) = mgr2.query("s1", &TimeRange::new(0, 1000), 200);
        assert_eq!(buckets.len(), 1);
    }

    #[test]
    fn query_scope_default_is_local() {
        assert_eq!(QueryScope::default(), QueryScope::Local);
    }
}
