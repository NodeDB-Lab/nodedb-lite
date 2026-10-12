// SPDX-License-Identifier: Apache-2.0

//! SEARCH INDEX parsing and admitted declaration execution.

mod execute;
mod parser;

pub(in crate::query) use parser::{SearchIndexStatement, parse_search_index_ddl};
