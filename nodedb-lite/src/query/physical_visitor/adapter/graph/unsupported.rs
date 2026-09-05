// SPDX-License-Identifier: Apache-2.0
//! `GraphOp` arms with no single-node Lite execution path.
//!
//! Cross-shard MATCH continuation / var-len resume and the BSP superstep
//! primitives (PageRank/WCC) exist to let a distributed coordinator
//! round-trip partial state across owning shards. Lite is single-node —
//! there are no shards to resume on or stitch together — so these have
//! no local execution path.

use crate::error::LiteError;

pub(super) fn match_continuation() -> LiteError {
    LiteError::Unsupported {
        detail: "MatchContinuation is a cross-shard MATCH resume primitive; \
                 unsupported on the single-node Lite engine"
            .into(),
    }
}

pub(super) fn match_var_len_resume() -> LiteError {
    LiteError::Unsupported {
        detail: "MatchVarLenResume is a cross-shard MATCH resume primitive; \
                 unsupported on the single-node Lite engine"
            .into(),
    }
}

pub(super) fn bsp_superstep() -> LiteError {
    LiteError::Unsupported {
        detail: "BspSuperstep is a distributed PageRank BSP primitive; \
                 unsupported on the single-node Lite engine"
            .into(),
    }
}

pub(super) fn wcc_superstep() -> LiteError {
    LiteError::Unsupported {
        detail: "WccSuperstep is a distributed WCC contraction primitive; \
                 unsupported on the single-node Lite engine"
            .into(),
    }
}
