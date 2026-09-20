// SPDX-License-Identifier: Apache-2.0
mod adapter;
mod text_config;
mod text_op;
mod vector_direct;
mod vector_op;
mod vector_sparse;
mod vector_write;

pub(crate) use adapter::LiteDataPlaneVisitor;
pub(crate) use adapter::execute_surrogate_scan;
pub(crate) use vector_direct::clear_collection_indexes;
