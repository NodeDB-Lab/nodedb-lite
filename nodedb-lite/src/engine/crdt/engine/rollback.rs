// SPDX-License-Identifier: BUSL-1.1

//! Undo of a row mutation that fails after it committed operations.
//!
//! Every `CrdtState` mutator commits on return, on success and on error, and
//! Loro cannot roll back. A failed mutation can therefore leave operations in
//! the document: `set_fields` writes `a`, then refuses `blocks` because a
//! nested container holds it. The undo is a set of new, compensating
//! operations. Both the partial and the compensating operations stay
//! committed, so the caller exports them like any write. A peer that misses
//! them diverges from this replica.

use nodedb_crdt::CrdtState;

use crate::error::LiteError;

/// How a row mutation that fails is undone.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum Rollback {
    /// Put the row's scalar fields back to their image from before the
    /// mutation. For writes that change only scalar fields: `upsert`,
    /// `set_fields`, `remove_fields`.
    RowImage,
    /// Keep what the mutation committed. For mutations whose only failure
    /// after a write is a Loro fault, and for writes to nested containers,
    /// which a row image does not hold.
    Keep,
}

/// Run `body` against `state`. When it fails and `row` is `Some`, put the
/// row's scalar fields back with compensating operations, then return the
/// body's error.
///
/// A row image that cannot be captured fails the call before `body` runs, so
/// nothing is written. A rollback that fails leaves the row changed and
/// returns a storage error that names both errors.
pub(super) fn run_with_rollback<F, T>(
    state: &CrdtState,
    collection: &str,
    row: Option<&str>,
    body: F,
) -> Result<T, LiteError>
where
    F: FnOnce(&CrdtState) -> Result<T, LiteError>,
{
    let image = match row {
        Some(row) => Some((
            row,
            state
                .row_image(collection, row)
                .map_err(|e| LiteError::crdt("row image capture", e))?,
        )),
        None => None,
    };
    let error = match body(state) {
        Ok(value) => return Ok(value),
        Err(error) => error,
    };
    if let Some((row, image)) = image
        && let Err(restore) = state.restore_row_image(collection, row, &image)
    {
        return Err(LiteError::Storage {
            detail: format!(
                "{error}; restoring row '{row}' of '{collection}' failed and the row keeps \
                 the partial write: {restore}"
            ),
        });
    }
    Err(error)
}
