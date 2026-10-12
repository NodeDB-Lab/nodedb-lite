// SPDX-License-Identifier: Apache-2.0

//! SQL-visitor lowering for DML SqlPlan variants, split by statement:
//! `insert_select`, `update_from`, `merge`, and their shared `rows` helpers.

mod insert_select;
mod merge;
mod rows;
mod update_from;

pub(super) use crate::query::assignments::convert_assignments;
pub(super) use insert_select::lower_insert_select;
pub(super) use merge::lower_merge;
pub(super) use update_from::lower_update_from;
