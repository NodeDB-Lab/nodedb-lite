// SPDX-License-Identifier: Apache-2.0
mod adapter;
mod text_op;
mod unsupported;
mod vector_op;
mod vector_write;

pub(crate) use adapter::LiteDataPlaneVisitor;
pub(crate) use adapter::execute_surrogate_scan;
