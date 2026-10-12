// SPDX-License-Identifier: Apache-2.0

//! Physical execution of `TextOp` variants for the Lite data plane, split by
//! responsibility: `dispatch` (variant routing), `search` (BM25 and phrase
//! search), `score_scan` (the all-rows score scan), `scores` (per-row score
//! columns), `rows` (filter gate), `hybrid` (vector + text fusion), `sync`
//! (Origin index frames).

mod dispatch;
mod hybrid;
mod rows;
mod score_scan;
mod scores;
mod search;
mod sync;

pub(crate) use dispatch::{execute_text_op, execute_text_op_admitted};
