// SPDX-License-Identifier: Apache-2.0
//! `KvOp` arms with no single-node Lite execution path.

use crate::error::LiteError;

/// `KvOp::ResolveWrite` is the resolve-before-propose wire shape
/// Origin's cross-vshard write path uses to decide a policy once and
/// replay it identically on every replica. Lite is single-node with no
/// Raft replay, so its SQL visitor and CRDT sync resolve every write
/// directly and never emit this variant.
pub(super) fn resolve_write() -> LiteError {
    LiteError::Unsupported {
        detail: "KvOp::ResolveWrite is the resolve-before-propose wire shape \
                 of Origin's cross-vshard write path, which Lite's single-node \
                 engine never emits or needs to interpret"
            .into(),
    }
}

/// `KvOp::ResolvedWrite` replays a decision made by Origin's Raft
/// leader, which has no equivalent on the single-node Lite engine.
pub(super) fn resolved_write() -> LiteError {
    LiteError::Unsupported {
        detail: "KvOp::ResolvedWrite replays a decision made by Origin's Raft \
                 leader, which has no equivalent on the single-node Lite engine"
            .into(),
    }
}

/// `KvOp::PredicateUpdate` carries a WHERE predicate for the Data
/// Plane to resolve against current state. Lite's SQL visitor always
/// resolves WHERE to an explicit key list before building a `KvOp`, so
/// it never constructs this variant.
pub(super) fn predicate_update(collection: &str) -> LiteError {
    LiteError::Unsupported {
        detail: format!(
            "KvOp::PredicateUpdate on {collection}: Lite's SQL visitor always \
             resolves WHERE to an explicit key list before building a KvOp"
        ),
    }
}

/// `KvOp::PredicateDelete` mirrors `PredicateUpdate` — see above.
pub(super) fn predicate_delete(collection: &str) -> LiteError {
    LiteError::Unsupported {
        detail: format!(
            "KvOp::PredicateDelete on {collection}: Lite's SQL visitor always \
             resolves WHERE to an explicit key list before building a KvOp"
        ),
    }
}
