// SPDX-License-Identifier: Apache-2.0
mod adapter;
mod text_op;
mod unsupported;

pub(crate) use adapter::LiteDataPlaneVisitor;
pub(crate) use adapter::execute_surrogate_scan;
