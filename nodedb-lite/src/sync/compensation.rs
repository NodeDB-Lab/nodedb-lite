//! Compensation handling for rejected sync deltas.
//!
//! When Origin rejects a delta (UNIQUE violation, RLS, rate limit), the
//! edge receives a `DeltaRejectMsg` with a `CompensationHint`. This module
//! rolls back the optimistic local state and notifies the application.

use std::sync::{Arc, Mutex};

use nodedb_types::sync::compensation::CompensationHint;

/// Event delivered to the application when a delta is rejected.
#[derive(Debug, Clone)]
pub struct CompensationEvent {
    /// Mutation ID that was rejected.
    pub mutation_id: u64,
    /// Collection the rejected operation targeted.
    pub collection: String,
    /// Document ID affected.
    pub document_id: String,
    /// Why it was rejected.
    pub hint: CompensationHint,
}

/// Application callback for compensation events.
///
/// The application registers a handler to decide how to react:
/// - Prompt the user ("username taken")
/// - Auto-retry with a modified value
/// - Silently accept the rejection
pub trait CompensationHandler: Send + Sync + 'static {
    fn on_compensation(&self, event: CompensationEvent);
}

/// Function-based compensation handler for convenience.
impl<F: Fn(CompensationEvent) + Send + Sync + 'static> CompensationHandler for F {
    fn on_compensation(&self, event: CompensationEvent) {
        self(event);
    }
}

/// Registry for compensation handlers.
///
/// Thread-safe — the sync client calls handlers from its background task
/// while the application may register/deregister from the main thread.
pub struct CompensationRegistry {
    handler: Mutex<Option<Arc<dyn CompensationHandler>>>,
    /// Events captured when no handler is registered (buffered for late binding).
    buffered: Mutex<Vec<CompensationEvent>>,
    /// Max buffer size to prevent unbounded growth.
    max_buffer: usize,
}

impl CompensationRegistry {
    pub fn new() -> Self {
        Self {
            handler: Mutex::new(None),
            buffered: Mutex::new(Vec::new()),
            max_buffer: 1000,
        }
    }

    /// Register a compensation handler. Drains any buffered events to it.
    pub fn set_handler(&self, handler: Arc<dyn CompensationHandler>) {
        // Drain buffer first.
        if let Ok(mut buf) = self.buffered.lock() {
            for event in buf.drain(..) {
                handler.on_compensation(event);
            }
        }
        if let Ok(mut h) = self.handler.lock() {
            *h = Some(handler);
        }
    }

    /// Remove the compensation handler.
    pub fn clear_handler(&self) {
        if let Ok(mut h) = self.handler.lock() {
            *h = None;
        }
    }

    /// Dispatch a compensation event. If no handler is registered, buffer it.
    pub fn dispatch(&self, event: CompensationEvent) {
        if let Ok(h) = self.handler.lock()
            && let Some(handler) = h.as_ref()
        {
            handler.on_compensation(event);
            return;
        }
        // No handler — buffer.
        if let Ok(mut buf) = self.buffered.lock()
            && buf.len() < self.max_buffer
        {
            buf.push(event);
        }
    }

    /// Number of buffered events (pending handler registration).
    pub fn buffered_count(&self) -> usize {
        self.buffered.lock().map(|b| b.len()).unwrap_or(0)
    }
}

impl Default for CompensationRegistry {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicU32, Ordering};

    fn make_event(mutation_id: u64) -> CompensationEvent {
        CompensationEvent {
            mutation_id,
            collection: "users".into(),
            document_id: "u1".into(),
            hint: CompensationHint::UniqueViolation {
                field: "email".into(),
                conflicting_value: "a@b.com".into(),
            },
        }
    }

    #[test]
    fn dispatch_with_handler() {
        let count = Arc::new(AtomicU32::new(0));
        let count_clone = count.clone();
        let registry = CompensationRegistry::new();

        registry.set_handler(Arc::new(move |_: CompensationEvent| {
            count_clone.fetch_add(1, Ordering::Relaxed);
        }));

        registry.dispatch(make_event(1));
        registry.dispatch(make_event(2));

        assert_eq!(count.load(Ordering::Relaxed), 2);
    }

    #[test]
    fn dispatch_without_handler_buffers() {
        let registry = CompensationRegistry::new();

        registry.dispatch(make_event(1));
        registry.dispatch(make_event(2));

        assert_eq!(registry.buffered_count(), 2);
    }

    #[test]
    fn late_handler_drains_buffer() {
        let count = Arc::new(AtomicU32::new(0));
        let count_clone = count.clone();
        let registry = CompensationRegistry::new();

        // Dispatch before handler is set.
        registry.dispatch(make_event(1));
        registry.dispatch(make_event(2));
        assert_eq!(registry.buffered_count(), 2);

        // Set handler — should drain buffer.
        registry.set_handler(Arc::new(move |_: CompensationEvent| {
            count_clone.fetch_add(1, Ordering::Relaxed);
        }));

        assert_eq!(count.load(Ordering::Relaxed), 2);
        assert_eq!(registry.buffered_count(), 0);

        // New events go directly to handler.
        registry.dispatch(make_event(3));
        assert_eq!(count.load(Ordering::Relaxed), 3);
    }

    #[test]
    fn clear_handler_resumes_buffering() {
        let registry = CompensationRegistry::new();
        let count = Arc::new(AtomicU32::new(0));
        let count_clone = count.clone();

        registry.set_handler(Arc::new(move |_: CompensationEvent| {
            count_clone.fetch_add(1, Ordering::Relaxed);
        }));

        registry.dispatch(make_event(1));
        assert_eq!(count.load(Ordering::Relaxed), 1);

        registry.clear_handler();
        registry.dispatch(make_event(2));
        assert_eq!(count.load(Ordering::Relaxed), 1); // Not incremented.
        assert_eq!(registry.buffered_count(), 1);
    }
}
