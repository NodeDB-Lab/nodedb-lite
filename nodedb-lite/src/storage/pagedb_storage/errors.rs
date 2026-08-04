// SPDX-License-Identifier: Apache-2.0

//! `PagedbError` → `LiteError` mapping.
//!
//! `PagedbError::NotFound` is **not** mapped here — callers that expect a
//! missing-key result should convert the `Ok(None)` / empty-vec at the call
//! site rather than going through the error path.
//!
//! `PagedbError::Quota` is mapped to `LiteError::Storage` for now. A dedicated
//! `LiteError::Quota` variant should be added so that quota pressure is
//! distinguishable at the application layer without string-matching.

use pagedb::PagedbError;

use crate::error::LiteError;

impl From<PagedbError> for LiteError {
    fn from(e: PagedbError) -> Self {
        // Corruption-class errors are typed distinctly so the signal survives
        // the conversion chain and reaches the caller as a corruption it can
        // match on, rather than as a generic storage failure.
        if is_corruption(&e) {
            return LiteError::Corrupted {
                detail: format!("pagedb corruption: {e}"),
            };
        }
        match e {
            PagedbError::Quota { .. } => LiteError::Storage {
                detail: format!("pagedb quota exceeded: {e}"),
            },
            // `Unsupported` is returned by the OPFS VFS shim when the `opfs`
            // feature is absent, and also by `OpfsVfs::new` if the worker
            // spawn fails. Surface it as `WorkerFailed` so callers can
            // distinguish it from generic I/O failures without string-matching.
            PagedbError::Unsupported => LiteError::WorkerFailed {
                detail: "pagedb OPFS VFS returned Unsupported — ensure the opfs feature is \
                         enabled and the worker URL is correct"
                    .to_string(),
            },
            other => LiteError::Storage {
                detail: other.to_string(),
            },
        }
    }
}

/// Returns `true` when the error is a corruption-class error.
///
/// Used by `From<PagedbError>` on all targets to type the error as
/// [`LiteError::Corrupted`], and by the native open path to decide whether the
/// caller's corruption policy applies.
pub(crate) fn is_corruption(e: &PagedbError) -> bool {
    matches!(e, PagedbError::Corruption(_) | PagedbError::ChecksumFailure)
}
