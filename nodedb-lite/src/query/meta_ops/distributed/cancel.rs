// SPDX-License-Identifier: Apache-2.0
//! Cooperative query cancellation for Lite.
//!
//! On Origin, Cancel is routed through the distributed coordinator to find
//! the executing Data Plane core. On Lite every query runs in-process on the
//! caller's async task, so cancellation is achieved by inserting the
//! `RequestId` into a shared `HashSet` that running handlers poll at safe
//! points (chunk boundaries, iteration boundaries, etc.).

use std::collections::HashSet;
use std::sync::{Arc, Mutex};

use nodedb_types::id::RequestId;
use nodedb_types::result::QueryResult;

use crate::error::LiteError;

/// Shared registry of request IDs that have been cancelled.
///
/// Constructed once on `LiteQueryEngine` and shared via `Arc`. Handlers that
/// process data in chunks call `is_cancelled` to check whether their request
/// has been signalled for early exit.
#[derive(Clone, Default)]
pub struct CancellationRegistry {
    cancelled: Arc<Mutex<HashSet<u64>>>,
}

impl CancellationRegistry {
    /// Create an empty registry.
    pub fn new() -> Self {
        Self::default()
    }

    /// Mark `rid` as cancelled. Idempotent.
    pub fn cancel(&self, rid: RequestId) -> Result<(), LiteError> {
        let mut set = self.cancelled.lock().map_err(|_| LiteError::LockPoisoned)?;
        set.insert(rid.as_u64());
        Ok(())
    }

    /// Return `true` if `rid` has been cancelled.
    pub fn is_cancelled(&self, rid: RequestId) -> bool {
        self.cancelled
            .lock()
            .map(|s| s.contains(&rid.as_u64()))
            .unwrap_or(false)
    }

    /// Remove `rid` from the cancelled set once the handler has acknowledged
    /// the cancellation. Prevents the set from growing without bound.
    pub fn clear(&self, rid: RequestId) {
        if let Ok(mut s) = self.cancelled.lock() {
            s.remove(&rid.as_u64());
        }
    }
}

/// Handle a `MetaOp::Cancel { target_request_id }`.
///
/// Inserts the target request ID into the cancellation registry so any running
/// handler polling `is_cancelled` will exit early. Returns success immediately;
/// the handler may not yet have checked — cancellation is cooperative.
pub fn handle_cancel(
    registry: &CancellationRegistry,
    target_request_id: RequestId,
) -> Result<QueryResult, LiteError> {
    registry.cancel(target_request_id)?;
    Ok(QueryResult::empty())
}

#[cfg(test)]
mod tests {
    use super::*;
    use nodedb_types::id::RequestId;

    #[test]
    fn cancel_marks_and_clears_request() {
        let registry = CancellationRegistry::new();
        let rid = RequestId::new(42);

        assert!(!registry.is_cancelled(rid));

        let result = handle_cancel(&registry, rid).unwrap();
        assert_eq!(result.rows_affected, 0);
        assert!(registry.is_cancelled(rid));

        registry.clear(rid);
        assert!(!registry.is_cancelled(rid));
    }

    #[test]
    fn cancel_idempotent() {
        let registry = CancellationRegistry::new();
        let rid = RequestId::new(99);
        // Cancelling twice must not panic or error.
        handle_cancel(&registry, rid).unwrap();
        handle_cancel(&registry, rid).unwrap();
        assert!(registry.is_cancelled(rid));
    }
}
