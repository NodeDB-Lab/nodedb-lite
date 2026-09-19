// SPDX-License-Identifier: Apache-2.0
//! Vector-primary direct writes: the insert family, `DirectDelete`, and
//! `DirectUpdate`, sharing one set of row primitives.

mod common;
mod mutate;
mod write;

pub(super) use common::remove_live_node;
pub(super) use mutate::{DirectUpdateArgs, vector_direct_delete, vector_direct_update};
pub(super) use write::{DirectWriteArgs, vector_direct_write};
