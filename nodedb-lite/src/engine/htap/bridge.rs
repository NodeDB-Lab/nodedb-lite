//! HTAP brige: CDC from strict document collections to columnar materialized views.
//!
//! When a materialized view is created, every INSERT/UPDATE/DELETE on the source
//! strict collection is replicated to the target columnar collection. In Lite,
//! this happens synchronously at the API level (no background WAL reader needed,
//! since the KV store handles durability).
//!
//! The bridge tracks:
//! - Source → target collection mapping
//! - Last replicated timestamp (for lag measurement)
//! - Row count delta (for consistency checks)
//!
//! All methods take `&self`; the view map lives behind a `Mutex` that is
//! held only briefly and never across `.await`. The bridge is therefore
//! natively `Send + Sync` and is stored as `Arc<HtapBridge>` (no outer
//! `Mutex<HtapBridge>` needed).

use std::collections::HashMap;
use std::sync::Mutex;

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
pub struct HtapBridge {
    /// Source collection name → list of materialized views.
    views: Mutex<HashMap<String, Vec<MaterializedView>>>,
}

impl HtapBridge {
    /// Create an empty bridge with no materialized views.
    pub fn new() -> Self {
        Self {
            views: Mutex::new(HashMap::new()),
        }
    }

    fn lock_views(&self) -> std::sync::MutexGuard<'_, HashMap<String, Vec<MaterializedView>>> {
        match self.views.lock() {
            Ok(g) => g,
            Err(p) => p.into_inner(),
        }
    }

    /// Register a new materialized view.
    pub fn register_view(&self, source: &str, target: &str) {
        let view = MaterializedView {
            source: source.to_string(),
            target: target.to_string(),
            last_replicated_ms: crate::runtime::now_millis(),
            rows_replicated: 0,
        };
        self.lock_views()
            .entry(source.to_string())
            .or_default()
            .push(view);
    }

    /// Remove a materialized view by target name.
    pub fn remove_view(&self, target: &str) {
        let mut g = self.lock_views();
        for views in g.values_mut() {
            views.retain(|v| v.target != target);
        }
        g.retain(|_, views| !views.is_empty());
    }

    /// Get all materialized views for a source collection (returns clones).
    pub fn views_for_source(&self, source: &str) -> Vec<MaterializedView> {
        self.lock_views().get(source).cloned().unwrap_or_default()
    }

    /// Get a materialized view by target name (returns clone).
    pub fn view_by_target(&self, target: &str) -> Option<MaterializedView> {
        self.lock_views()
            .values()
            .flatten()
            .find(|v| v.target == target)
            .cloned()
    }

    /// List all materialized view target names.
    pub fn all_targets(&self) -> Vec<String> {
        self.lock_views()
            .values()
            .flatten()
            .map(|v| v.target.clone())
            .collect()
    }

    /// Replicate an INSERT from a source strict collection to all its
    /// materialized columnar views.
    pub fn replicate_insert<S: StorageEngine>(
        &self,
        source: &str,
        values: &[Value],
        columnar: &ColumnarEngine<S>,
    ) {
        let mut g = self.lock_views();
        let Some(views) = g.get_mut(source) else {
            return;
        };
        for view in views.iter_mut() {
            if columnar.insert(&view.target, values).is_ok() {
                view.rows_replicated += 1;
                view.last_replicated_ms = crate::runtime::now_millis();
            }
        }
    }

    /// Replicate a DELETE from a source strict collection to all its
    /// materialized columnar views.
    pub fn replicate_delete<S: StorageEngine>(
        &self,
        source: &str,
        pk: &Value,
        columnar: &ColumnarEngine<S>,
    ) {
        let mut g = self.lock_views();
        let Some(views) = g.get_mut(source) else {
            return;
        };
        for view in views.iter_mut() {
            if columnar.delete(&view.target, pk).unwrap_or(false) {
                view.last_replicated_ms = crate::runtime::now_millis();
            }
        }
    }

    /// Get the replication lag in milliseconds for a materialized view.
    pub fn lag_ms(&self, target: &str) -> u64 {
        self.view_by_target(target)
            .map(|v| crate::runtime::now_millis().saturating_sub(v.last_replicated_ms))
            .unwrap_or(0)
    }

    /// Whether any materialized views exist.
    pub fn is_empty(&self) -> bool {
        self.lock_views().is_empty()
    }
}

impl Default for HtapBridge {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn register_and_lookup_view() {
        let bridge = HtapBridge::new();
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
        let bridge = HtapBridge::new();
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
        let bridge = HtapBridge::new();
        bridge.register_view("orders", "order_analytics");
        bridge.register_view("customers", "customer_analytics");

        assert_eq!(bridge.all_targets().len(), 2);
    }
}
