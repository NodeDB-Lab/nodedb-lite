// SPDX-License-Identifier: Apache-2.0
//! Vector-primary direct writes: the insert family, `DirectDelete`,
//! `DirectUpdate`, and `DirectTruncate`, sharing one set of row primitives.

mod common;
mod mutate;
mod truncate;
mod write;

pub(super) use common::remove_live_node;
pub(super) use mutate::{DirectUpdateArgs, vector_direct_delete, vector_direct_update};
pub(crate) use truncate::clear_collection_indexes;
pub(super) use truncate::vector_direct_truncate;
pub(super) use write::{DirectWriteArgs, vector_direct_write};
