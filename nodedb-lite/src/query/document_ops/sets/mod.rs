// SPDX-License-Identifier: Apache-2.0
//! Document set operations with shared text mutation admission.
mod actions;
mod copy;
mod join;
mod materialize;
mod merge;
mod source;
pub(in crate::query) use actions::{MergeWrite, apply_merge_write, plan_merge_write};
pub use copy::insert_select;
pub(crate) use copy::insert_select_coordinated;
pub use join::update_from_join;
pub(crate) use join::update_from_join_coordinated;
pub use materialize::materialize_scan;
pub use merge::merge;
pub(crate) use merge::merge_coordinated;
use nodedb_physical::physical_plan::document::types::UpdateValue;
pub(crate) use source::DocumentJoin;
pub(in crate::query) use source::{collect_ids_pub, fetch_document_value_pub};
