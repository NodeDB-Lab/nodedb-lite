// SPDX-License-Identifier: Apache-2.0
//! KvOp dispatch for the Lite physical visitor, split by concern.

mod dispatch;
mod indexes;
mod reads;
mod unsupported;
mod writes;

pub(super) use dispatch::dispatch;
