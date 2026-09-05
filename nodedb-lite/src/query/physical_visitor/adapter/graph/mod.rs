// SPDX-License-Identifier: Apache-2.0
//! `GraphOp` dispatch for the Lite physical visitor, split by concern.

mod analytics;
mod dispatch;
mod edges;
mod fusion_match;
mod traversal;
mod unsupported;

pub(super) use dispatch::dispatch;
