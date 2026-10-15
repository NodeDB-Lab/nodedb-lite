// SPDX-License-Identifier: Apache-2.0
//! Document operation dispatch for the Lite physical visitor.

mod dispatch;
mod row_source;
mod writes;

pub(super) use dispatch::dispatch;
