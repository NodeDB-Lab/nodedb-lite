//! SQL parser helpers for DDL statements.
//!
//! Parses `CREATE COLLECTION` and column definition syntax into typed schema objects.

use nodedb_types::value::Value;

use crate::error::LiteError;

/// Parse `CREATE COLLECTION <name> (<col_defs>) WITH storage = 'strict'`.
///
/// Column definitions: `name TYPE [NOT NULL] [PRIMARY KEY] [DEFAULT expr], ...`
pub(crate) fn parse_strict_create_sql(
    sql: &str,
) -> Result<(String, nodedb_types::columnar::StrictSchema), LiteError> {
    use nodedb_types::columnar::{ColumnDef, ColumnType, StrictSchema};

    // Extract collection name: word after "CREATE COLLECTION".
    let upper = sql.to_uppercase();
    let after_create = sql
        .get(
            upper
                .find("COLLECTION")
                .ok_or(LiteError::Query("expected COLLECTION keyword".into()))?
                + 10..,
        )
        .ok_or(LiteError::Query("unexpected end of SQL".into()))?
        .trim();

    let name_end = after_create
        .find(|c: char| c == '(' || c.is_whitespace())
        .unwrap_or(after_create.len());
    let name = after_create[..name_end].trim().to_lowercase();

    if name.is_empty() {
        return Err(LiteError::Query("missing collection name".into()));
    }

    // Extract column definitions between parentheses.
    let paren_start = sql.find('(').ok_or(LiteError::Query(
        "expected column definitions in parentheses".into(),
    ))?;

    // Find the matching closing paren (handle nested parens for VECTOR(dim)).
    let sql_bytes = sql.as_bytes();
    let mut depth = 0;
    let mut paren_end = None;
    for (i, &b) in sql_bytes.iter().enumerate().skip(paren_start) {
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

    // Split by comma, but respect parentheses inside type names like VECTOR(768).
    let col_parts = split_top_level_commas(col_defs_str);

    let mut columns = Vec::new();
    for part in &col_parts {
        let col = parse_column_def(part.trim())?;
        columns.push(col);
    }

    if columns.is_empty() {
        return Err(LiteError::Query("at least one column required".into()));
    }

    // Auto-generate _rowid PK if no PK column specified.
    if !columns.iter().any(|c| c.primary_key) {
        columns.insert(
            0,
            ColumnDef::required("_rowid", ColumnType::Int64).with_primary_key(),
        );
    }

    let schema = StrictSchema::new(columns).map_err(|e| LiteError::Query(e.to_string()))?;
    Ok((name, schema))
}

/// Split a string by commas at the top level (not inside parentheses).
pub(crate) fn split_top_level_commas(s: &str) -> Vec<&str> {
    let mut parts = Vec::new();
    let mut depth = 0;
    let mut start = 0;
    for (i, c) in s.char_indices() {
        match c {
            '(' => depth += 1,
            ')' => depth -= 1,
            ',' if depth == 0 => {
                parts.push(&s[start..i]);
                start = i + 1;
            }
            _ => {}
        }
    }
    if start < s.len() {
        parts.push(&s[start..]);
    }
    parts
}

/// Parse a single column definition: `name TYPE [NOT NULL] [PRIMARY KEY] [DEFAULT expr]`
pub(crate) fn parse_column_def(s: &str) -> Result<nodedb_types::columnar::ColumnDef, LiteError> {
    use nodedb_types::columnar::{ColumnDef, ColumnType};

    let upper = s.to_uppercase();
    let tokens: Vec<&str> = s.split_whitespace().collect();

    if tokens.len() < 2 {
        return Err(LiteError::Query(format!(
            "column definition requires at least name and type, got: '{s}'"
        )));
    }

    let name = tokens[0].to_lowercase();

    // Find the type — may span multiple tokens for VECTOR(dim).
    // Rejoin everything after the name until we hit a keyword.
    let after_name = s.split_whitespace().skip(1).collect::<Vec<_>>().join(" ");
    let type_end = find_keyword_start(&after_name);
    let type_str = after_name[..type_end].trim();

    let column_type: ColumnType =
        type_str
            .parse()
            .map_err(|e: nodedb_types::columnar::ColumnTypeParseError| {
                LiteError::Query(e.to_string())
            })?;

    let is_not_null = upper.contains("NOT NULL");
    let is_pk = upper.contains("PRIMARY KEY");
    let nullable = !is_not_null && !is_pk;

    // Parse DEFAULT value.
    let default = if let Some(pos) = upper.find("DEFAULT ") {
        let after_default = s[pos + 8..].trim();
        // Take until next keyword or end.
        let end = find_keyword_start(after_default);
        let expr = after_default[..end].trim();
        if expr.is_empty() {
            None
        } else {
            Some(expr.to_string())
        }
    } else {
        None
    };

    let mut col = if nullable {
        ColumnDef::nullable(name, column_type)
    } else {
        ColumnDef::required(name, column_type)
    };

    if is_pk {
        col = col.with_primary_key();
    }
    if let Some(d) = default {
        col = col.with_default(d);
    }

    Ok(col)
}

/// Find the start position of the first SQL keyword in a column definition suffix.
/// Matches at word boundaries (start of string or preceded by whitespace).
pub(crate) fn find_keyword_start(s: &str) -> usize {
    let upper = s.to_uppercase();
    let keywords = ["NOT", "NULL", "PRIMARY", "DEFAULT"];
    let mut earliest = s.len();
    for kw in &keywords {
        if let Some(pos) = upper.find(kw) {
            // Ensure word boundary: at start or preceded by whitespace.
            let at_boundary = pos == 0
                || upper
                    .as_bytes()
                    .get(pos - 1)
                    .is_some_and(|b| b.is_ascii_whitespace());
            if at_boundary && pos < earliest {
                earliest = pos;
            }
        }
    }
    earliest
}

/// Build a DESCRIBE result for a strict collection.
pub(crate) fn describe_strict_collection(
    name: &str,
    schema: &nodedb_types::columnar::StrictSchema,
) -> nodedb_types::result::QueryResult {
    use nodedb_types::columnar::SchemaOps;

    let mut rows = Vec::with_capacity(schema.len() + 2);

    // Collection name and storage mode info.
    rows.push(vec![
        Value::String("__collection".into()),
        Value::String(name.to_string()),
        Value::String(String::new()),
        Value::String(String::new()),
        Value::String(String::new()),
    ]);
    rows.push(vec![
        Value::String("__storage".into()),
        Value::String("document".into()),
        Value::String("strict".into()),
        Value::String(String::new()),
        Value::String(format!("v{}", schema.version)),
    ]);

    for col in &schema.columns {
        rows.push(vec![
            Value::String(col.name.clone()),
            Value::String(col.column_type.to_string()),
            Value::String(if col.nullable { "YES" } else { "NO" }.into()),
            Value::String(if col.primary_key { "YES" } else { "NO" }.into()),
            Value::String(col.default.clone().unwrap_or_default()),
        ]);
    }

    nodedb_types::result::QueryResult {
        columns: vec![
            "field".into(),
            "type".into(),
            "nullable".into(),
            "primary_key".into(),
            "default".into(),
        ],
        rows,
        rows_affected: 0,
    }
}
