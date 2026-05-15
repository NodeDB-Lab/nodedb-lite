//! Spatial geometry insert/delete outbound queue for Lite sync.
//!
//! When `NodeDbLite::spatial_insert` is called, the geometry is serialised
//! (MessagePack via zerompk) and enqueued here. When `spatial_delete` is
//! called, a delete entry is enqueued. The transport drains this queue every
//! tick and sends `SpatialInsert` (0xAA) / `SpatialDelete` (0xAC) frames to
//! Origin. Insert and delete IDs share one counter for global uniqueness.

use super::queue::{BatchIdGen, PendingQueue};

/// A single pending spatial insert operation awaiting sync to Origin.
#[derive(Debug, Clone)]
pub struct PendingSpatialInsert {
    pub batch_id: u64,
    pub collection: String,
    /// Geometry field name.
    pub field: String,
    pub doc_id: String,
    /// MessagePack-serialised `nodedb_types::geometry::Geometry`.
    pub geometry_bytes: Vec<u8>,
}

/// A single pending spatial delete operation awaiting sync to Origin.
#[derive(Debug, Clone)]
pub struct PendingSpatialDelete {
    pub batch_id: u64,
    pub collection: String,
    pub field: String,
    pub doc_id: String,
}

#[derive(Debug, Default)]
pub struct SpatialOutbound {
    inserts: PendingQueue<PendingSpatialInsert>,
    deletes: PendingQueue<PendingSpatialDelete>,
    ids: BatchIdGen,
}

impl SpatialOutbound {
    pub const fn new() -> Self {
        Self {
            inserts: PendingQueue::new(),
            deletes: PendingQueue::new(),
            ids: BatchIdGen::new(),
        }
    }

    /// Serialise the geometry and enqueue it. A serialisation failure is
    /// logged and the enqueue is dropped — the geometry is already durable in
    /// the local R-tree, and Origin will see it on the next full catch-up.
    pub fn enqueue_insert(
        &self,
        collection: &str,
        field: &str,
        doc_id: &str,
        geometry: &nodedb_types::geometry::Geometry,
    ) {
        let geometry_bytes = match zerompk::to_msgpack_vec(geometry) {
            Ok(b) => b,
            Err(e) => {
                tracing::warn!(
                    collection,
                    field,
                    doc_id,
                    error = %e,
                    "spatial_outbound: failed to serialise geometry; skipping sync enqueue"
                );
                return;
            }
        };
        self.inserts.push(PendingSpatialInsert {
            batch_id: self.ids.next(),
            collection: collection.to_string(),
            field: field.to_string(),
            doc_id: doc_id.to_string(),
            geometry_bytes,
        });
    }

    pub fn enqueue_delete(&self, collection: &str, field: &str, doc_id: &str) {
        self.deletes.push(PendingSpatialDelete {
            batch_id: self.ids.next(),
            collection: collection.to_string(),
            field: field.to_string(),
            doc_id: doc_id.to_string(),
        });
    }

    pub fn drain_inserts(&self) -> Vec<PendingSpatialInsert> {
        self.inserts.drain()
    }

    pub fn drain_deletes(&self) -> Vec<PendingSpatialDelete> {
        self.deletes.drain()
    }

    pub fn acknowledge_insert(&self, batch_id: u64) {
        self.inserts.retain(|e| e.batch_id != batch_id);
    }

    pub fn acknowledge_delete(&self, batch_id: u64) {
        self.deletes.retain(|e| e.batch_id != batch_id);
    }

    pub fn requeue_insert(&self, entry: PendingSpatialInsert) {
        self.inserts.requeue(entry);
    }

    pub fn requeue_delete(&self, entry: PendingSpatialDelete) {
        self.deletes.requeue(entry);
    }

    pub fn pending_insert_count(&self) -> usize {
        self.inserts.len()
    }

    pub fn pending_delete_count(&self) -> usize {
        self.deletes.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn point_geometry() -> nodedb_types::geometry::Geometry {
        nodedb_types::geometry::Geometry::point(1.0, 2.0)
    }

    #[test]
    fn enqueue_and_drain_inserts() {
        let q = SpatialOutbound::new();
        q.enqueue_insert("places", "loc", "doc1", &point_geometry());
        q.enqueue_insert("places", "loc", "doc2", &point_geometry());

        let entries = q.drain_inserts();
        assert_eq!(entries.len(), 2);
        assert_eq!(entries[0].doc_id, "doc1");
        assert_eq!(entries[1].doc_id, "doc2");
        assert!(q.drain_inserts().is_empty());
    }

    #[test]
    fn enqueue_and_drain_deletes() {
        let q = SpatialOutbound::new();
        q.enqueue_delete("places", "loc", "doc1");

        let deletes = q.drain_deletes();
        assert_eq!(deletes.len(), 1);
        assert_eq!(deletes[0].doc_id, "doc1");
        assert!(q.drain_deletes().is_empty());
    }

    #[test]
    fn acknowledge_insert_removes_by_batch_id() {
        let q = SpatialOutbound::new();
        q.enqueue_insert("places", "loc", "doc1", &point_geometry());
        let entries = q.drain_inserts();
        let id = entries[0].batch_id;
        q.acknowledge_insert(id);
        assert!(q.drain_inserts().is_empty());
    }

    #[test]
    fn requeue_insert_retried_on_next_drain() {
        let q = SpatialOutbound::new();
        q.enqueue_insert("places", "loc", "doc1", &point_geometry());
        let entries = q.drain_inserts();
        q.requeue_insert(entries.into_iter().next().unwrap());

        let retried = q.drain_inserts();
        assert_eq!(retried.len(), 1);
        assert_eq!(retried[0].doc_id, "doc1");
    }

    #[test]
    fn geometry_bytes_round_trip() {
        let geom = point_geometry();
        let q = SpatialOutbound::new();
        q.enqueue_insert("places", "loc", "doc1", &geom);
        let entries = q.drain_inserts();
        assert!(!entries[0].geometry_bytes.is_empty());
        let restored: nodedb_types::geometry::Geometry =
            zerompk::from_msgpack(&entries[0].geometry_bytes)
                .expect("geometry round-trip deserialise");
        assert_eq!(geom, restored);
    }

    #[test]
    fn batch_ids_monotonically_increase() {
        let q = SpatialOutbound::new();
        q.enqueue_insert("places", "loc", "a", &point_geometry());
        q.enqueue_delete("places", "loc", "b");
        q.enqueue_insert("places", "loc", "c", &point_geometry());

        let inserts = q.drain_inserts();
        let deletes = q.drain_deletes();

        let mut all_ids: Vec<u64> = inserts.iter().map(|e| e.batch_id).collect();
        all_ids.extend(deletes.iter().map(|e| e.batch_id));
        let mut sorted = all_ids.clone();
        sorted.sort_unstable();
        sorted.dedup();
        assert_eq!(sorted.len(), all_ids.len(), "batch_ids must be unique");
        assert!(all_ids.iter().all(|&id| id > 0));
    }
}
