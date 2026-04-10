//! HTAP bridge: CDC from strict document collections to columnar materialized views.
//!
//! When a materialized view is created, every INSERT/UPDATE/DELETE on the source
//! strict collection is replicated to the target columnar collection. In Lite,
//! this happens synchronously at the API level (no background WAL reader needed,
//! since redb handles durability).
//!
//! The bridge tracks:
//! - Source → target collection mapping
//! - Last replicated timestamp (for lag measurement)
//! - Row count delta (for consistency checks)

use std::collections::HashMap;
use std::sync::Mutex;
use std::time::{SystemTime, UNIX_EPOCH};

use nodedb_types::value::Value;

use crate::engine::columnar::ColumnarEngine;
use crate::storage::engine::StorageEngine;

/// Metadata for a single materialized view.
#[derive(Debug, Clone)]
pub struct MaterializedView {
    /// Source strict collection name.
    pub source: String,
    /// Target columnar collection name.
    pub target: String,
    /// Timestamp of the last replicated change (millis since epoch).
    pub last_replicated_ms: u64,
    /// Number of rows replicated.
    pub rows_replicated: u64,
}

/// Manages CDC bridges between strict document collections and columnar
/// materialized views.
///
/// Each bridge replicates changes from a source strict collection into a
/// target columnar collection. Multiple views can be created from the same source.
pub struct HtapBridge {
    /// Source collection name → list of materialized views.
    views: HashMap<String, Vec<MaterializedView>>,
}

impl HtapBridge {
    /// Create an empty bridge with no materialized views.
    pub fn new() -> Self {
        Self {
            views: HashMap::new(),
        }
    }

    /// Register a new materialized view.
    ///
    /// The target columnar collection must already exist in the ColumnarEngine.
    pub fn register_view(&mut self, source: &str, target: &str) {
        let view = MaterializedView {
            source: source.to_string(),
            target: target.to_string(),
            last_replicated_ms: now_ms(),
            rows_replicated: 0,
        };
        self.views.entry(source.to_string()).or_default().push(view);
    }

    /// Remove a materialized view by target name.
    pub fn remove_view(&mut self, target: &str) {
        for views in self.views.values_mut() {
            views.retain(|v| v.target != target);
        }
        self.views.retain(|_, views| !views.is_empty());
    }

    /// Get all materialized views for a source collection.
    pub fn views_for_source(&self, source: &str) -> &[MaterializedView] {
        self.views.get(source).map(|v| v.as_slice()).unwrap_or(&[])
    }

    /// Get a materialized view by target name.
    pub fn view_by_target(&self, target: &str) -> Option<&MaterializedView> {
        self.views.values().flatten().find(|v| v.target == target)
    }

    /// List all materialized view target names.
    pub fn all_targets(&self) -> Vec<&str> {
        self.views
            .values()
            .flatten()
            .map(|v| v.target.as_str())
            .collect()
    }

    /// Replicate an INSERT from a source strict collection to all its
    /// materialized columnar views.
    ///
    /// Called after `strict_insert()` succeeds. Writes the same row into
    /// each target columnar collection's memtable.
    pub fn replicate_insert<S: StorageEngine>(
        &mut self,
        source: &str,
        values: &[Value],
        columnar: &Mutex<ColumnarEngine<S>>,
    ) {
        let Some(views) = self.views.get_mut(source) else {
            return;
        };

        let mut engine = match columnar.lock() {
            Ok(e) => e,
            Err(p) => p.into_inner(),
        };

        for view in views.iter_mut() {
            if engine.insert(&view.target, values).is_ok() {
                view.rows_replicated += 1;
                view.last_replicated_ms = now_ms();
            }
        }
    }

    /// Replicate a DELETE from a source strict collection to all its
    /// materialized columnar views.
    pub fn replicate_delete<S: StorageEngine>(
        &mut self,
        source: &str,
        pk: &Value,
        columnar: &Mutex<ColumnarEngine<S>>,
    ) {
        let Some(views) = self.views.get_mut(source) else {
            return;
        };

        let mut engine = match columnar.lock() {
            Ok(e) => e,
            Err(p) => p.into_inner(),
        };

        for view in views.iter_mut() {
            if engine.delete(&view.target, pk).unwrap_or(false) {
                view.last_replicated_ms = now_ms();
            }
        }
    }

    /// Get the replication lag in milliseconds for a materialized view.
    pub fn lag_ms(&self, target: &str) -> u64 {
        self.view_by_target(target)
            .map(|v| now_ms().saturating_sub(v.last_replicated_ms))
            .unwrap_or(0)
    }

    /// Whether any materialized views exist.
    pub fn is_empty(&self) -> bool {
        self.views.is_empty()
    }
}

impl Default for HtapBridge {
    fn default() -> Self {
        Self::new()
    }
}

fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn register_and_lookup_view() {
        let mut bridge = HtapBridge::new();
        bridge.register_view("customers", "customer_analytics");

        assert!(!bridge.is_empty());
        assert_eq!(bridge.views_for_source("customers").len(), 1);
        assert_eq!(
            bridge.views_for_source("customers")[0].target,
            "customer_analytics"
        );
        assert!(bridge.view_by_target("customer_analytics").is_some());
        assert!(bridge.view_by_target("nonexistent").is_none());
    }

    #[test]
    fn remove_view() {
        let mut bridge = HtapBridge::new();
        bridge.register_view("customers", "analytics_1");
        bridge.register_view("customers", "analytics_2");

        assert_eq!(bridge.views_for_source("customers").len(), 2);

        bridge.remove_view("analytics_1");
        assert_eq!(bridge.views_for_source("customers").len(), 1);
        assert_eq!(
            bridge.views_for_source("customers")[0].target,
            "analytics_2"
        );
    }

    #[test]
    fn multiple_sources() {
        let mut bridge = HtapBridge::new();
        bridge.register_view("orders", "order_analytics");
        bridge.register_view("customers", "customer_analytics");

        assert_eq!(bridge.all_targets().len(), 2);
    }
}
