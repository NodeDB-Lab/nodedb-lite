// SPDX-License-Identifier: Apache-2.0
//! The source relation of a document copy or join write, as Lite reads it.
//!
//! Lite reads a source collection directly: every current row, unchanged.
//! A source that needs a filter, an offset, a time cut, a column map, a
//! query plan, or rows a coordinator already read is refused, never read in
//! part.

use nodedb_physical::physical_plan::{DocumentPageSource, RowSource};

use crate::error::LiteError;

/// The collection an `INSERT ... SELECT` copy reads, and the most rows it
/// copies. `None` copies every row.
pub(super) fn copy_source<'s>(
    op: &str,
    source: &'s RowSource,
) -> Result<(&'s str, Option<usize>), LiteError> {
    let page = plain_page(op, source)?;
    Ok((page.collection.as_str(), page.limit))
}

/// The collection a join write (`UPDATE ... FROM`, `MERGE`) reads every row
/// of. `source_rows` are rows a coordinator read for the Data Plane.
pub(super) fn join_source<'s>(
    op: &str,
    source: &'s RowSource,
    source_rows: Option<&[Vec<u8>]>,
) -> Result<&'s str, LiteError> {
    if source_rows.is_some() {
        return Err(refuse(op, "coordinator-read source rows"));
    }
    let page = plain_page(op, source)?;
    if page.limit.is_some() {
        return Err(refuse(op, "a source LIMIT"));
    }
    Ok(page.collection.as_str())
}

/// The page source of `source`, when it reads current rows unchanged.
fn plain_page<'s>(op: &str, source: &'s RowSource) -> Result<&'s DocumentPageSource, LiteError> {
    let page = match source {
        RowSource::DocumentPages(page) => page,
        RowSource::Query(_) => return Err(refuse(op, "a query source")),
    };
    let DocumentPageSource {
        collection: _,
        filters,
        system_as_of_ms,
        offset,
        limit: _,
        column_map,
    } = page;
    if !filters.is_empty() {
        return Err(refuse(op, "a source filter"));
    }
    if system_as_of_ms.is_some() {
        return Err(refuse(op, "an AS OF SYSTEM TIME source"));
    }
    if *offset != 0 {
        return Err(refuse(op, "a source OFFSET"));
    }
    if !column_map.is_empty() {
        return Err(refuse(op, "a source column map"));
    }
    Ok(page)
}

fn refuse(op: &str, what: &str) -> LiteError {
    LiteError::Unsupported {
        detail: format!("{op} with {what} is not supported in Lite"),
    }
}

#[cfg(test)]
mod tests {
    use nodedb_physical::physical_plan::{DocumentPageSource, RowSource};
    use nodedb_types::QualifiedCollection;

    use super::{copy_source, join_source};
    use crate::error::LiteError;

    fn page(limit: Option<usize>, filters: Vec<u8>) -> RowSource {
        RowSource::DocumentPages(DocumentPageSource {
            collection: QualifiedCollection::from_stored("src".to_string()),
            filters,
            system_as_of_ms: None,
            offset: 0,
            limit,
            column_map: Vec::new(),
        })
    }

    #[test]
    fn a_whole_collection_copy_reads_its_collection_and_limit() {
        let source = page(Some(3), Vec::new());
        let (collection, limit) = copy_source("InsertSelect", &source).expect("plain page");
        assert_eq!(collection, "src");
        assert_eq!(limit, Some(3));
    }

    #[test]
    fn a_filtered_source_is_refused() {
        let source = page(None, vec![0x90]);
        let err = copy_source("InsertSelect", &source).expect_err("filter refused");
        assert!(matches!(err, LiteError::Unsupported { .. }), "{err}");
    }

    #[test]
    fn a_join_source_refuses_a_limit_and_coordinator_rows() {
        let limited = page(Some(1), Vec::new());
        assert!(join_source("Merge", &limited, None).is_err());
        let whole = page(None, Vec::new());
        assert!(join_source("Merge", &whole, Some(&[])).is_err());
        assert_eq!(join_source("Merge", &whole, None).expect("whole"), "src");
    }
}
