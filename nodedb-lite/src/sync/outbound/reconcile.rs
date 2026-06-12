// SPDX-License-Identifier: Apache-2.0
//! Shared failure policy for durable outbound-queue enqueues.

use crate::error::LiteError;

/// Apply the outbound-enqueue failure policy to a write that has already
/// committed to the local store.
///
/// `Backpressure` is the single fatal class: the durable outbound queue is
/// full, so the local write must be rejected and surfaced to the caller for
/// retry — otherwise the local store would silently diverge from the replica.
/// Every other enqueue error is non-fatal: the row is already durable locally
/// and will reconcile on the next full resync, so it is logged and swallowed.
///
/// Centralising this here keeps the fatal-vs-recoverable decision in one place;
/// every write path (vector, document, columnar, timeseries — via either the
/// trait-impl or the physical-visitor adapters) routes its enqueue result
/// through this function instead of repeating the `matches!`/`warn!` dance.
///
/// `op` / `collection` / `id` are recorded on the warning for diagnosis; pass an
/// empty `id` for collection-level operations that have no per-row identity.
///
/// Returns the original `LiteError` on backpressure so callers can propagate it
/// directly (when their future is `Result<_, LiteError>`) or convert it with
/// `NodeDbError::storage` (when they return `NodeDbResult`).
pub(crate) fn reconcile_outbound_enqueue(
    result: Result<(), LiteError>,
    op: &str,
    collection: &str,
    id: &str,
) -> Result<(), LiteError> {
    match result {
        Ok(()) => Ok(()),
        Err(e @ LiteError::Backpressure { .. }) => Err(e),
        Err(e) => {
            tracing::warn!(
                op,
                collection,
                id,
                error = %e,
                "outbound enqueue failed; write committed locally"
            );
            Ok(())
        }
    }
}
