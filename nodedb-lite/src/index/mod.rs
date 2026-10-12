// SPDX-License-Identifier: Apache-2.0

//! Secondary indexes: definitions, entry keys, maintenance, and reads.

pub mod catalog;
pub(crate) mod ddl;
pub(crate) mod document;
pub(crate) mod durable;
pub(crate) mod key;
pub(crate) mod legacy;
pub(crate) mod lookup;
pub(crate) mod maintain;
pub(crate) mod rebuild;
pub(crate) mod sorted_def;
pub(crate) mod store;

pub use catalog::{
    IndexDef, IndexEngine, IndexPredicate, canonical_field, default_index_name, field_spec,
};
pub use store::IndexCatalog;
