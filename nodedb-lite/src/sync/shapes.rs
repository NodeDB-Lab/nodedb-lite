//! Shape subscription management on the edge side.
//!
//! Tracks which shapes the client is subscribed to, handles initial
//! snapshot loading and incremental delta application.

use std::collections::HashMap;

use nodedb_types::sync::shape::ShapeDefinition;

/// Manages shape subscriptions for a Lite client.
///
/// On reconnect, the client re-sends all active subscriptions in the
/// handshake. The ShapeManager tracks what's active and the LSN
/// watermark for each shape (so the Origin knows what deltas to send).
pub struct ShapeManager {
    /// Active subscriptions: shape_id → (definition, last_seen_lsn).
    subscriptions: HashMap<String, ShapeSubscription>,
}

/// A single active shape subscription.
#[derive(Debug, Clone)]
pub struct ShapeSubscription {
    /// The shape definition (what data this subscription covers).
    pub definition: ShapeDefinition,
    /// LSN of the last delta received for this shape.
    /// Used to resume sync after reconnect.
    pub last_lsn: u64,
    /// Whether the initial snapshot has been loaded.
    pub snapshot_loaded: bool,
}

impl ShapeManager {
    pub fn new() -> Self {
        Self {
            subscriptions: HashMap::new(),
        }
    }

    /// Subscribe to a shape. Returns the shape_id.
    pub fn subscribe(&mut self, definition: ShapeDefinition) -> String {
        let shape_id = definition.shape_id.clone();
        self.subscriptions.insert(
            shape_id.clone(),
            ShapeSubscription {
                definition,
                last_lsn: 0,
                snapshot_loaded: false,
            },
        );
        shape_id
    }

    /// Unsubscribe from a shape.
    pub fn unsubscribe(&mut self, shape_id: &str) -> bool {
        self.subscriptions.remove(shape_id).is_some()
    }

    /// Mark a shape's snapshot as loaded.
    pub fn mark_snapshot_loaded(&mut self, shape_id: &str, lsn: u64) {
        if let Some(sub) = self.subscriptions.get_mut(shape_id) {
            sub.snapshot_loaded = true;
            sub.last_lsn = lsn;
        }
    }

    /// Update the last-seen LSN for a shape (after receiving a delta).
    pub fn advance_lsn(&mut self, shape_id: &str, lsn: u64) {
        if let Some(sub) = self.subscriptions.get_mut(shape_id)
            && lsn > sub.last_lsn
        {
            sub.last_lsn = lsn;
        }
    }

    /// Get a subscription by shape_id.
    pub fn get(&self, shape_id: &str) -> Option<&ShapeSubscription> {
        self.subscriptions.get(shape_id)
    }

    /// All active shape IDs (for handshake).
    pub fn active_shape_ids(&self) -> Vec<String> {
        self.subscriptions.keys().cloned().collect()
    }

    /// All active shape definitions (for re-subscription on reconnect).
    pub fn active_definitions(&self) -> Vec<&ShapeDefinition> {
        self.subscriptions.values().map(|s| &s.definition).collect()
    }

    /// Number of active subscriptions.
    pub fn count(&self) -> usize {
        self.subscriptions.len()
    }

    /// Check if a specific shape is subscribed.
    pub fn is_subscribed(&self, shape_id: &str) -> bool {
        self.subscriptions.contains_key(shape_id)
    }
}

impl Default for ShapeManager {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use nodedb_types::sync::shape::ShapeType;

    fn make_shape(id: &str, collection: &str) -> ShapeDefinition {
        ShapeDefinition {
            shape_id: id.into(),
            tenant_id: 1,
            shape_type: ShapeType::Document {
                collection: collection.into(),
                predicate: Vec::new(),
            },
            description: format!("all {collection}"),
            field_filter: vec![],
        }
    }

    #[test]
    fn subscribe_and_query() {
        let mut mgr = ShapeManager::new();
        let id = mgr.subscribe(make_shape("s1", "orders"));

        assert_eq!(id, "s1");
        assert!(mgr.is_subscribed("s1"));
        assert_eq!(mgr.count(), 1);

        let sub = mgr.get("s1").unwrap();
        assert!(!sub.snapshot_loaded);
        assert_eq!(sub.last_lsn, 0);
    }

    #[test]
    fn unsubscribe() {
        let mut mgr = ShapeManager::new();
        mgr.subscribe(make_shape("s1", "orders"));
        assert!(mgr.unsubscribe("s1"));
        assert!(!mgr.is_subscribed("s1"));
        assert_eq!(mgr.count(), 0);
        assert!(!mgr.unsubscribe("s1")); // Already removed.
    }

    #[test]
    fn snapshot_and_lsn_tracking() {
        let mut mgr = ShapeManager::new();
        mgr.subscribe(make_shape("s1", "orders"));

        mgr.mark_snapshot_loaded("s1", 100);
        let sub = mgr.get("s1").unwrap();
        assert!(sub.snapshot_loaded);
        assert_eq!(sub.last_lsn, 100);

        mgr.advance_lsn("s1", 150);
        assert_eq!(mgr.get("s1").unwrap().last_lsn, 150);

        // Should not decrease.
        mgr.advance_lsn("s1", 50);
        assert_eq!(mgr.get("s1").unwrap().last_lsn, 150);
    }

    #[test]
    fn active_ids_for_handshake() {
        let mut mgr = ShapeManager::new();
        mgr.subscribe(make_shape("s1", "orders"));
        mgr.subscribe(make_shape("s2", "users"));

        let mut ids = mgr.active_shape_ids();
        ids.sort();
        assert_eq!(ids, vec!["s1", "s2"]);
    }

    #[test]
    fn active_definitions() {
        let mut mgr = ShapeManager::new();
        mgr.subscribe(make_shape("s1", "orders"));
        mgr.subscribe(make_shape("s2", "users"));

        let defs = mgr.active_definitions();
        assert_eq!(defs.len(), 2);
    }
}
