//! Local change event broadcast for app-level reactivity.
//!
//! Lightweight CDC for NodeDB-Lite: apps can subscribe to local write events
//! for real-time UI updates, caching invalidation, etc. Events include
//! both user writes and CRDT sync merges (inbound deltas from Origin).
//!
//! **Platform-agnostic:** Works on native (iOS/Android/desktop) and WASM
//! (browsers). Uses a callback registry (`Fn(&LiteChangeEvent)`) instead
//! of Tokio channels — no async runtime required.

use std::sync::{Arc, Mutex};

use serde::{Deserialize, Serialize};

/// A local change event emitted after a successful write.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LiteChangeEvent {
    /// Collection affected.
    pub collection: String,
    /// Document/row ID.
    pub document_id: String,
    /// Operation type.
    pub op: LiteChangeOp,
    /// Timestamp (epoch milliseconds).
    pub timestamp_ms: u64,
    /// Whether this event originated from CRDT sync (inbound delta from Origin).
    pub from_sync: bool,
}

/// Operation types for Lite change events.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum LiteChangeOp {
    Insert,
    Update,
    Delete,
}

/// Subscription handle. Automatically unsubscribes on drop.
pub struct Subscription {
    id: u64,
    stream: Arc<LiteChangeStreamInner>,
}

impl Drop for Subscription {
    fn drop(&mut self) {
        let mut cbs = self
            .stream
            .callbacks
            .lock()
            .unwrap_or_else(|p| p.into_inner());
        cbs.retain(|(id, _)| *id != self.id);
    }
}

/// Type-erased callback for change events.
/// `Send + Sync` for native (multi-threaded Tokio); trivially satisfied on WASM.
type ChangeCallback = Box<dyn Fn(&LiteChangeEvent) + Send + Sync>;

struct LiteChangeStreamInner {
    callbacks: Mutex<Vec<(u64, ChangeCallback)>>,
    next_id: std::sync::atomic::AtomicU64,
    events_published: std::sync::atomic::AtomicU64,
}

/// Local change event bus for NodeDB-Lite.
///
/// Subscribers register callbacks that fire synchronously on each write.
/// No persistence, no durability — purely for app-level reactivity.
///
/// Works on all platforms: native (iOS/Android/desktop) and WASM (browser).
pub struct LiteChangeStream {
    inner: Arc<LiteChangeStreamInner>,
}

impl LiteChangeStream {
    pub fn new() -> Self {
        Self {
            inner: Arc::new(LiteChangeStreamInner {
                callbacks: Mutex::new(Vec::new()),
                next_id: std::sync::atomic::AtomicU64::new(1),
                events_published: std::sync::atomic::AtomicU64::new(0),
            }),
        }
    }

    /// Subscribe to local change events. Returns a handle that unsubscribes on drop.
    ///
    /// The callback fires synchronously on each `publish()` call — keep it fast.
    /// For async processing, use the callback to send into your own channel.
    pub fn subscribe<F>(&self, callback: F) -> Subscription
    where
        F: Fn(&LiteChangeEvent) + Send + Sync + 'static,
    {
        let id = self
            .inner
            .next_id
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let mut cbs = self
            .inner
            .callbacks
            .lock()
            .unwrap_or_else(|p| p.into_inner());
        cbs.push((id, Box::new(callback)));
        Subscription {
            id,
            stream: Arc::clone(&self.inner),
        }
    }

    /// Publish a change event to all subscribers.
    pub fn publish(&self, event: &LiteChangeEvent) {
        self.inner
            .events_published
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let cbs = self
            .inner
            .callbacks
            .lock()
            .unwrap_or_else(|p| p.into_inner());
        for (_, cb) in cbs.iter() {
            cb(event);
        }
    }

    /// Publish a user-originated write event.
    pub fn publish_write(&self, collection: &str, document_id: &str, op: LiteChangeOp) {
        let event = LiteChangeEvent {
            collection: collection.to_string(),
            document_id: document_id.to_string(),
            op,
            timestamp_ms: now_ms(),
            from_sync: false,
        };
        self.publish(&event);
    }

    /// Publish a CRDT sync merge event (inbound delta from Origin).
    pub fn publish_sync(&self, collection: &str, document_id: &str, op: LiteChangeOp) {
        let event = LiteChangeEvent {
            collection: collection.to_string(),
            document_id: document_id.to_string(),
            op,
            timestamp_ms: now_ms(),
            from_sync: true,
        };
        self.publish(&event);
    }

    /// Total events published since creation.
    pub fn events_published(&self) -> u64 {
        self.inner
            .events_published
            .load(std::sync::atomic::Ordering::Relaxed)
    }

    /// Number of active subscribers.
    pub fn subscriber_count(&self) -> usize {
        self.inner
            .callbacks
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .len()
    }
}

impl Default for LiteChangeStream {
    fn default() -> Self {
        Self::new()
    }
}

fn now_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicU64, Ordering};

    #[test]
    fn publish_and_receive() {
        let stream = LiteChangeStream::new();
        let received = Arc::new(AtomicU64::new(0));
        let received_clone = Arc::clone(&received);

        let _sub = stream.subscribe(move |_event| {
            received_clone.fetch_add(1, Ordering::Relaxed);
        });

        stream.publish_write("users", "u1", LiteChangeOp::Insert);
        assert_eq!(received.load(Ordering::Relaxed), 1);
    }

    #[test]
    fn sync_events_flagged() {
        let stream = LiteChangeStream::new();
        let was_sync = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let was_sync_clone = Arc::clone(&was_sync);

        let _sub = stream.subscribe(move |event| {
            was_sync_clone.store(event.from_sync, std::sync::atomic::Ordering::Relaxed);
        });

        stream.publish_sync("orders", "o1", LiteChangeOp::Update);
        assert!(was_sync.load(std::sync::atomic::Ordering::Relaxed));
    }

    #[test]
    fn unsubscribe_on_drop() {
        let stream = LiteChangeStream::new();
        let count = Arc::new(AtomicU64::new(0));
        let count_clone = Arc::clone(&count);

        let sub = stream.subscribe(move |_| {
            count_clone.fetch_add(1, Ordering::Relaxed);
        });
        assert_eq!(stream.subscriber_count(), 1);

        stream.publish_write("a", "1", LiteChangeOp::Insert);
        assert_eq!(count.load(Ordering::Relaxed), 1);

        drop(sub);
        assert_eq!(stream.subscriber_count(), 0);

        stream.publish_write("a", "2", LiteChangeOp::Insert);
        assert_eq!(count.load(Ordering::Relaxed), 1); // No increment — unsubscribed.
    }

    #[test]
    fn multiple_subscribers() {
        let stream = LiteChangeStream::new();
        let count1 = Arc::new(AtomicU64::new(0));
        let count2 = Arc::new(AtomicU64::new(0));
        let c1 = Arc::clone(&count1);
        let c2 = Arc::clone(&count2);

        let _sub1 = stream.subscribe(move |_| {
            c1.fetch_add(1, Ordering::Relaxed);
        });
        let _sub2 = stream.subscribe(move |_| {
            c2.fetch_add(1, Ordering::Relaxed);
        });

        stream.publish_write("x", "1", LiteChangeOp::Delete);
        assert_eq!(count1.load(Ordering::Relaxed), 1);
        assert_eq!(count2.load(Ordering::Relaxed), 1);
    }

    #[test]
    fn no_subscriber_doesnt_panic() {
        let stream = LiteChangeStream::new();
        stream.publish_write("test", "t1", LiteChangeOp::Delete);
        assert_eq!(stream.events_published(), 1);
    }
}
