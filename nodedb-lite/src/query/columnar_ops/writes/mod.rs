// SPDX-License-Identifier: Apache-2.0
//! Write operations for the columnar engine physical visitor.

mod ops;
mod payload;
mod rows;

pub use ops::{InsertParams, delete, insert, update};
