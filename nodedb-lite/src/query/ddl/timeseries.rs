//! DDL handlers for CREATE TIMESERIES COLLECTION.
//!
//! Maps the `CREATE TIMESERIES COLLECTION` sugar to
//! `CollectionType::Columnar(ColumnarProfile::Timeseries { .. })`.

use nodedb_types::columnar::{ColumnDef, ColumnType, ColumnarProfile, ColumnarSchema};
use nodedb_types::result::QueryResult;
use nodedb_types::value::Value;

use crate::error::LiteError;
use crate::query::engine::LiteQueryEngine;
use crate::storage::engine::StorageEngine;

impl<S: StorageEngine> LiteQueryEngine<S> {
    /// Handle: CREATE TIMESERIES COLLECTION <name> (<col_defs>) [PARTITION BY TIME(<interval>)]
    ///
    /// Creates a columnar collection with the Timeseries profile.
    /// If no columns specified, defaults to (time TIMESTAMP NOT NULL, value FLOAT64).
    pub(in crate::query) async fn handle_create_timeseries(
        &self,
        sql: &str,
    ) -> Result<QueryResult, LiteError> {
        let (name, schema, interval) = parse_timeseries_create(sql)?;

        // Find the time key column (first TIMESTAMP column).
        let time_key = schema
            .columns
            .iter()
            .find(|c| matches!(c.column_type, ColumnType::Timestamp))
            .map(|c| c.name.clone())
            .ok_or_else(|| {
                LiteError::Query(
                    "timeseries collection requires at least one TIMESTAMP column".into(),
                )
            })?;

        let profile = ColumnarProfile::Timeseries {
            time_key,
            interval: interval.clone(),
        };

        {
            let mut columnar = match self.columnar.lock() {
                Ok(c) => c,
                Err(p) => p.into_inner(),
            };
            tokio::task::block_in_place(|| {
                let handle = tokio::runtime::Handle::current();
                handle.block_on(columnar.create_collection(&name, schema, profile))
            })?;
        }

        self.register_columnar_collection(&name);

        Ok(QueryResult {
            columns: vec!["result".into()],
            rows: vec![vec![Value::String(format!(
                "timeseries collection '{name}' created (interval: {interval})"
            ))]],
            rows_affected: 0,
        })
    }
}

/// Parse `CREATE TIMESERIES COLLECTION <name> [(<col_defs>)] [PARTITION BY TIME(<interval>)]`.
///
/// If no column definitions are provided, defaults to:
///   `time TIMESTAMP NOT NULL, value FLOAT64`
fn parse_timeseries_create(sql: &str) -> Result<(String, ColumnarSchema, String), LiteError> {
    let upper = sql.to_uppercase();

    // Extract collection name: word after "TIMESERIES COLLECTION" (or just "TIMESERIES").
    let ts_pos = upper
        .find("TIMESERIES")
        .ok_or(LiteError::Query("expected TIMESERIES keyword".into()))?;

    let after_ts = sql[ts_pos + 10..].trim();
    // Skip optional "COLLECTION" keyword.
    let after_keyword = if after_ts.to_uppercase().starts_with("COLLECTION") {
        after_ts[10..].trim()
    } else {
        after_ts
    };

    let name_end = after_keyword
        .find(|c: char| c == '(' || c.is_whitespace())
        .unwrap_or(after_keyword.len());
    let name = after_keyword[..name_end].trim().to_lowercase();

    if name.is_empty() {
        return Err(LiteError::Query("missing collection name".into()));
    }

    // Parse column definitions if present.
    let columns = if let Some(paren_start) = sql.find('(') {
        // Check if this paren is for column defs or PARTITION BY TIME(...).
        let before_paren = sql[..paren_start].to_uppercase();
        if before_paren.contains("TIME") && before_paren.ends_with("TIME") {
            // This is PARTITION BY TIME(...), not column defs.
            default_timeseries_columns()
        } else {
            parse_ts_columns(sql)?
        }
    } else {
        default_timeseries_columns()
    };

    // Parse interval from PARTITION BY TIME(<interval>).
    let interval = parse_partition_interval(&upper, sql);

    let schema = ColumnarSchema::new(columns)?;

    Ok((name, schema, interval))
}

/// Default timeseries columns: (time TIMESTAMP NOT NULL, value FLOAT64).
fn default_timeseries_columns() -> Vec<ColumnDef> {
    vec![
        ColumnDef::required("time", ColumnType::Timestamp),
        ColumnDef::nullable("value", ColumnType::Float64),
    ]
}

/// Parse column definitions from the CREATE TIMESERIES statement.
fn parse_ts_columns(sql: &str) -> Result<Vec<ColumnDef>, LiteError> {
    use super::parser::{parse_column_def, split_top_level_commas};

    let paren_start = sql
        .find('(')
        .ok_or(LiteError::Query("expected column definitions".into()))?;

    // Find matching close paren.
    let mut depth = 0;
    let mut paren_end = None;
    for (i, b) in sql.bytes().enumerate().skip(paren_start) {
        match b {
            b'(' => depth += 1,
            b')' => {
                depth -= 1;
                if depth == 0 {
                    paren_end = Some(i);
                    break;
                }
            }
            _ => {}
        }
    }
    let paren_end = paren_end.ok_or(LiteError::Query("unmatched parenthesis".into()))?;

    let col_defs_str = &sql[paren_start + 1..paren_end];
    let parts = split_top_level_commas(col_defs_str);

    let mut columns = Vec::new();
    for part in &parts {
        let trimmed = part.trim();
        if trimmed.is_empty() {
            continue;
        }
        columns.push(parse_column_def(trimmed)?);
    }

    if columns.is_empty() {
        return Ok(default_timeseries_columns());
    }

    // Validate: must have at least one TIMESTAMP column.
    if !columns
        .iter()
        .any(|c| matches!(c.column_type, ColumnType::Timestamp))
    {
        return Err(LiteError::Query(
            "timeseries collection requires at least one TIMESTAMP column".into(),
        ));
    }

    Ok(columns)
}

/// Parse the PARTITION BY TIME(<interval>) clause. Defaults to "1h".
fn parse_partition_interval(upper: &str, _sql: &str) -> String {
    if let Some(pos) = upper.find("PARTITION BY TIME(") {
        let start = pos + 18;
        if let Some(end) = upper[start..].find(')') {
            let interval = upper[start..start + end].trim();
            if !interval.is_empty() {
                return interval.to_lowercase();
            }
        }
    }
    "1h".to_string()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_basic_timeseries() {
        let sql = "CREATE TIMESERIES COLLECTION metrics (
            time TIMESTAMP NOT NULL,
            host TEXT,
            cpu FLOAT64,
            mem FLOAT64
        ) PARTITION BY TIME(1h)";

        let (name, schema, interval) = parse_timeseries_create(sql).expect("parse");
        assert_eq!(name, "metrics");
        assert_eq!(schema.columns.len(), 4);
        assert_eq!(schema.columns[0].name, "time");
        assert!(matches!(
            schema.columns[0].column_type,
            ColumnType::Timestamp
        ));
        assert_eq!(interval, "1h");
    }

    #[test]
    fn parse_timeseries_no_columns() {
        let sql = "CREATE TIMESERIES COLLECTION metrics";

        let (name, schema, interval) = parse_timeseries_create(sql).expect("parse");
        assert_eq!(name, "metrics");
        assert_eq!(schema.columns.len(), 2); // Default: time + value.
        assert!(matches!(
            schema.columns[0].column_type,
            ColumnType::Timestamp
        ));
        assert_eq!(interval, "1h"); // Default interval.
    }

    #[test]
    fn parse_timeseries_custom_interval() {
        let sql = "CREATE TIMESERIES COLLECTION logs PARTITION BY TIME(1d)";

        let (_, _, interval) = parse_timeseries_create(sql).expect("parse");
        assert_eq!(interval, "1d");
    }

    #[test]
    fn parse_timeseries_without_collection_keyword() {
        let sql = "CREATE TIMESERIES sensors (
            ts TIMESTAMP NOT NULL,
            temperature FLOAT64
        )";

        let (name, schema, _) = parse_timeseries_create(sql).expect("parse");
        assert_eq!(name, "sensors");
        assert_eq!(schema.columns.len(), 2);
    }

    #[test]
    fn parse_timeseries_no_timestamp_rejected() {
        let sql = "CREATE TIMESERIES COLLECTION bad (
            name TEXT,
            value FLOAT64
        )";

        assert!(parse_timeseries_create(sql).is_err());
    }
}
