// SPDX-License-Identifier: Apache-2.0

//! SORTED INDEX DDL and the sorted-index read functions.

mod claim;
mod create;
mod cursor;
mod execute;
mod read;
mod statement;

pub(in crate::query) use claim::parse_sorted_index_sql;
