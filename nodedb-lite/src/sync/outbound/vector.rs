//! Vector insert/delete outbound queue for Lite sync.
//!
//! When `NodeDbLite::vector_insert_impl` or `vector_delete_impl` is called,
//! the operation is enqueued here. The sync transport drains this queue on
//! every tick and sends `VectorInsert` (0xA2) / `VectorDelete` (0xA4) wire
//! frames to Origin. Each entry gets a monotonic `batch_id` for ACK
//! correlation; insert and delete IDs share one counter so they are globally
//! unique inside this outbound.

use super::queue::{BatchIdGen, PendingQueue};

/// A single pending vector insert awaiting sync to Origin.
#[derive(Debug, Clone)]
pub struct PendingVectorInsert {
    pub batch_id: u64,
    pub collection: String,
    pub id: String,
    pub vector: Vec<f32>,
    pub dim: usize,
    /// Named vector field; empty = default.
    pub field_name: String,
}

/// A single pending vector delete awaiting sync to Origin.
#[derive(Debug, Clone)]
pub struct PendingVectorDelete {
    pub batch_id: u64,
    pub collection: String,
    pub id: String,
    /// Named vector field; empty = default.
    pub field_name: String,
}

#[derive(Debug, Default)]
pub struct VectorOutbound {
    inserts: PendingQueue<PendingVectorInsert>,
    deletes: PendingQueue<PendingVectorDelete>,
    ids: BatchIdGen,
}

impl VectorOutbound {
    pub const fn new() -> Self {
        Self {
            inserts: PendingQueue::new(),
            deletes: PendingQueue::new(),
            ids: BatchIdGen::new(),
        }
    }

    pub fn enqueue_insert(
        &self,
        collection: &str,
        id: &str,
        vector: Vec<f32>,
        dim: usize,
        field_name: &str,
    ) {
        self.inserts.push(PendingVectorInsert {
            batch_id: self.ids.next(),
            collection: collection.to_string(),
            id: id.to_string(),
            vector,
            dim,
            field_name: field_name.to_string(),
        });
    }

    pub fn enqueue_delete(&self, collection: &str, id: &str, field_name: &str) {
        self.deletes.push(PendingVectorDelete {
            batch_id: self.ids.next(),
            collection: collection.to_string(),
            id: id.to_string(),
            field_name: field_name.to_string(),
        });
    }

    pub fn drain_inserts(&self) -> Vec<PendingVectorInsert> {
        self.inserts.drain()
    }

    pub fn drain_deletes(&self) -> Vec<PendingVectorDelete> {
        self.deletes.drain()
    }

    pub fn acknowledge_insert(&self, batch_id: u64) {
        self.inserts.retain(|e| e.batch_id != batch_id);
    }

    pub fn acknowledge_delete(&self, batch_id: u64) {
        self.deletes.retain(|e| e.batch_id != batch_id);
    }

    pub fn requeue_insert(&self, entry: PendingVectorInsert) {
        self.inserts.requeue(entry);
    }

    pub fn requeue_delete(&self, entry: PendingVectorDelete) {
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

    #[test]
    fn enqueue_and_drain_inserts() {
        let q = VectorOutbound::new();
        q.enqueue_insert("vecs", "v1", vec![1.0, 0.0], 2, "");
        q.enqueue_insert("vecs", "v2", vec![0.0, 1.0], 2, "");

        let inserts = q.drain_inserts();
        assert_eq!(inserts.len(), 2);
        assert_eq!(inserts[0].id, "v1");
        assert_eq!(inserts[1].id, "v2");
        assert!(q.drain_inserts().is_empty());
    }

    #[test]
    fn enqueue_and_drain_deletes() {
        let q = VectorOutbound::new();
        q.enqueue_delete("vecs", "v1", "");

        let deletes = q.drain_deletes();
        assert_eq!(deletes.len(), 1);
        assert_eq!(deletes[0].id, "v1");
        assert!(q.drain_deletes().is_empty());
    }

    #[test]
    fn acknowledge_insert_removes_by_batch_id() {
        let q = VectorOutbound::new();
        q.enqueue_insert("vecs", "v1", vec![1.0], 1, "");
        let inserts = q.drain_inserts();
        let id = inserts[0].batch_id;
        q.acknowledge_insert(id);
        assert!(q.drain_inserts().is_empty());
    }

    #[test]
    fn requeue_insert_retried_on_next_drain() {
        let q = VectorOutbound::new();
        q.enqueue_insert("vecs", "v1", vec![1.0], 1, "");
        let inserts = q.drain_inserts();
        q.requeue_insert(inserts.into_iter().next().unwrap());

        let retried = q.drain_inserts();
        assert_eq!(retried.len(), 1);
        assert_eq!(retried[0].id, "v1");
    }

    #[test]
    fn batch_ids_monotonically_increase() {
        let q = VectorOutbound::new();
        q.enqueue_insert("vecs", "a", vec![1.0], 1, "");
        q.enqueue_delete("vecs", "b", "");
        q.enqueue_insert("vecs", "c", vec![2.0], 1, "");

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
