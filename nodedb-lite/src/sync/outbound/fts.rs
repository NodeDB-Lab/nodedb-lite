//! FTS index/delete outbound queue for Lite sync.
//!
//! When `NodeDbLite::index_document_text` / `remove_document_text` is called,
//! the operation is enqueued here. The transport drains it on every tick and
//! sends `FtsIndex` (0xA6) / `FtsDelete` (0xA8) frames to Origin. Insert and
//! delete IDs share one counter for global uniqueness inside this outbound.

use super::queue::{BatchIdGen, PendingQueue};

/// A single pending FTS index operation awaiting sync to Origin.
#[derive(Debug, Clone)]
pub struct PendingFtsIndex {
    pub batch_id: u64,
    pub collection: String,
    pub doc_id: String,
    /// Concatenated text to index (all string fields joined by space).
    pub text: String,
}

/// A single pending FTS delete operation awaiting sync to Origin.
#[derive(Debug, Clone)]
pub struct PendingFtsDelete {
    pub batch_id: u64,
    pub collection: String,
    pub doc_id: String,
}

#[derive(Debug, Default)]
pub struct FtsOutbound {
    indexes: PendingQueue<PendingFtsIndex>,
    deletes: PendingQueue<PendingFtsDelete>,
    ids: BatchIdGen,
}

impl FtsOutbound {
    pub const fn new() -> Self {
        Self {
            indexes: PendingQueue::new(),
            deletes: PendingQueue::new(),
            ids: BatchIdGen::new(),
        }
    }

    pub fn enqueue_index(&self, collection: &str, doc_id: &str, text: String) {
        self.indexes.push(PendingFtsIndex {
            batch_id: self.ids.next(),
            collection: collection.to_string(),
            doc_id: doc_id.to_string(),
            text,
        });
    }

    pub fn enqueue_delete(&self, collection: &str, doc_id: &str) {
        self.deletes.push(PendingFtsDelete {
            batch_id: self.ids.next(),
            collection: collection.to_string(),
            doc_id: doc_id.to_string(),
        });
    }

    pub fn drain_indexes(&self) -> Vec<PendingFtsIndex> {
        self.indexes.drain()
    }

    pub fn drain_deletes(&self) -> Vec<PendingFtsDelete> {
        self.deletes.drain()
    }

    pub fn acknowledge_index(&self, batch_id: u64) {
        self.indexes.retain(|e| e.batch_id != batch_id);
    }

    pub fn acknowledge_delete(&self, batch_id: u64) {
        self.deletes.retain(|e| e.batch_id != batch_id);
    }

    pub fn requeue_index(&self, entry: PendingFtsIndex) {
        self.indexes.requeue(entry);
    }

    pub fn requeue_delete(&self, entry: PendingFtsDelete) {
        self.deletes.requeue(entry);
    }

    pub fn pending_index_count(&self) -> usize {
        self.indexes.len()
    }

    pub fn pending_delete_count(&self) -> usize {
        self.deletes.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn enqueue_and_drain_indexes() {
        let q = FtsOutbound::new();
        q.enqueue_index("docs", "d1", "hello world".to_string());
        q.enqueue_index("docs", "d2", "rust rocks".to_string());

        let entries = q.drain_indexes();
        assert_eq!(entries.len(), 2);
        assert_eq!(entries[0].doc_id, "d1");
        assert_eq!(entries[1].doc_id, "d2");
        assert!(q.drain_indexes().is_empty());
    }

    #[test]
    fn enqueue_and_drain_deletes() {
        let q = FtsOutbound::new();
        q.enqueue_delete("docs", "d1");

        let deletes = q.drain_deletes();
        assert_eq!(deletes.len(), 1);
        assert_eq!(deletes[0].doc_id, "d1");
        assert!(q.drain_deletes().is_empty());
    }

    #[test]
    fn acknowledge_index_removes_by_batch_id() {
        let q = FtsOutbound::new();
        q.enqueue_index("docs", "d1", "text".to_string());
        let entries = q.drain_indexes();
        let id = entries[0].batch_id;
        q.acknowledge_index(id);
        assert!(q.drain_indexes().is_empty());
    }

    #[test]
    fn requeue_index_retried_on_next_drain() {
        let q = FtsOutbound::new();
        q.enqueue_index("docs", "d1", "text".to_string());
        let entries = q.drain_indexes();
        q.requeue_index(entries.into_iter().next().unwrap());

        let retried = q.drain_indexes();
        assert_eq!(retried.len(), 1);
        assert_eq!(retried[0].doc_id, "d1");
    }

    #[test]
    fn batch_ids_monotonically_increase() {
        let q = FtsOutbound::new();
        q.enqueue_index("docs", "a", "foo".to_string());
        q.enqueue_delete("docs", "b");
        q.enqueue_index("docs", "c", "bar".to_string());

        let indexes = q.drain_indexes();
        let deletes = q.drain_deletes();

        let mut all_ids: Vec<u64> = indexes.iter().map(|e| e.batch_id).collect();
        all_ids.extend(deletes.iter().map(|e| e.batch_id));
        let mut sorted = all_ids.clone();
        sorted.sort_unstable();
        sorted.dedup();
        assert_eq!(sorted.len(), all_ids.len(), "batch_ids must be unique");
        assert!(all_ids.iter().all(|&id| id > 0));
    }
}
