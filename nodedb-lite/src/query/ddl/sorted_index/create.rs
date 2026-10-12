// SPDX-License-Identifier: Apache-2.0

//! `CREATE SORTED INDEX` and `DROP SORTED INDEX` parsing.
//!
//! ```text
//! CREATE SORTED INDEX name ON collection (col [ASC|DESC] [, ...]) KEY key_col
//!     [WINDOW NONE
//!     | WINDOW {DAILY | WEEKLY | MONTHLY} [ON ts_col]
//!     | WINDOW CUSTOM {START ms | END ms | ON ts_col} ...]
//! DROP SORTED INDEX name
//! ```
//!
//! A window with no `ON` clause stamps entries from `updated_at`.

use crate::error::LiteError;
use crate::query::ddl::tokens::Token;

use super::cursor::{Cursor, describe, syntax};
use super::statement::{
    CreateSortedIndex, DEFAULT_WINDOW_COLUMN, SortedIndexStatement, WindowClause,
};

pub(super) fn parse_create(mut c: Cursor<'_>) -> Result<SortedIndexStatement, LiteError> {
    if !(c.eat_keyword("CREATE") && c.eat_keyword("SORTED") && c.eat_keyword("INDEX")) {
        return Err(syntax("expected CREATE SORTED INDEX"));
    }
    let name = c.identifier("missing index name")?;
    if !c.eat_keyword("ON") {
        return Err(syntax("missing ON clause"));
    }
    let collection = c.identifier("missing collection name after ON")?;
    if !matches!(c.peek(), Some(Token::Open)) {
        return Err(syntax("missing '(' for sort columns"));
    }
    c.advance();
    let sort_columns = sort_columns(&mut c)?;
    if !c.eat_keyword("KEY") {
        return Err(syntax("missing KEY clause"));
    }
    let key_column = c.identifier("missing key column name")?;
    let window = if c.eat_keyword("WINDOW") {
        window(&mut c)?
    } else {
        WindowClause::none()
    };
    c.finish()?;
    Ok(SortedIndexStatement::Create(CreateSortedIndex {
        name,
        collection,
        sort_columns,
        key_column,
        window,
    }))
}

pub(super) fn parse_drop(mut c: Cursor<'_>) -> Result<SortedIndexStatement, LiteError> {
    if !(c.eat_keyword("DROP") && c.eat_keyword("SORTED") && c.eat_keyword("INDEX")) {
        return Err(syntax("expected DROP SORTED INDEX"));
    }
    let name = c.identifier("missing index name")?;
    c.finish()?;
    Ok(SortedIndexStatement::Drop { name })
}

/// The sort columns after the opening parenthesis, through the closing one.
fn sort_columns(c: &mut Cursor<'_>) -> Result<Vec<(String, String)>, LiteError> {
    if matches!(c.peek(), Some(Token::Close)) {
        return Err(syntax("at least one sort column required"));
    }
    let mut columns = Vec::new();
    loop {
        let name = c.identifier("missing sort column name")?;
        let direction = match c.peek() {
            Some(Token::Comma | Token::Close) | None => "ASC".to_string(),
            Some(Token::Bare(raw)) => {
                let direction = raw.to_ascii_uppercase();
                if direction != "ASC" && direction != "DESC" {
                    return Err(syntax(format!(
                        "invalid sort direction '{direction}', expected ASC or DESC"
                    )));
                }
                c.advance();
                direction
            }
            Some(other) => {
                return Err(syntax(format!(
                    "invalid sort direction '{}', expected ASC or DESC",
                    describe(other)
                )));
            }
        };
        columns.push((name.clone(), direction));
        match c.peek() {
            Some(Token::Comma) => c.advance(),
            Some(Token::Close) => {
                c.advance();
                return Ok(columns);
            }
            None | Some(Token::Semicolon) => {
                return Err(syntax("missing ')' for sort columns"));
            }
            Some(other) => {
                return Err(syntax(format!(
                    "expected ',' or ')' after sort column '{name}', got '{}'",
                    describe(other)
                )));
            }
        }
    }
}

/// The clause after `WINDOW`.
fn window(c: &mut Cursor<'_>) -> Result<WindowClause, LiteError> {
    let kind = match c.peek() {
        Some(Token::Bare(raw)) => raw.to_ascii_lowercase(),
        _ => {
            return Err(syntax(
                "missing WINDOW type, expected NONE, DAILY, WEEKLY, MONTHLY or CUSTOM",
            ));
        }
    };
    let kind: &'static str = match kind.as_str() {
        "none" => "none",
        "daily" => "daily",
        "weekly" => "weekly",
        "monthly" => "monthly",
        "custom" => "custom",
        other => {
            return Err(syntax(format!(
                "unknown WINDOW type '{}', expected NONE, DAILY, WEEKLY, MONTHLY or CUSTOM",
                other.to_ascii_uppercase()
            )));
        }
    };
    c.advance();
    match kind {
        "none" => Ok(WindowClause::none()),
        "custom" => custom_window(c),
        calendar => {
            let timestamp_column = if c.eat_keyword("ON") {
                c.identifier("missing timestamp column after WINDOW ... ON")?
            } else {
                DEFAULT_WINDOW_COLUMN.to_string()
            };
            Ok(WindowClause {
                kind: calendar,
                timestamp_column,
                start_ms: 0,
                end_ms: 0,
            })
        }
    }
}

/// `START ms`, `END ms` and `ON ts_col`, in any order, each at most once.
/// START and END are required, and START must precede END.
fn custom_window(c: &mut Cursor<'_>) -> Result<WindowClause, LiteError> {
    let mut start = None;
    let mut end = None;
    let mut column = None;
    while !c.at_end() {
        if c.eat_keyword("START") {
            set_once(&mut start, epoch_ms(c, "START")?, "START")?;
        } else if c.eat_keyword("END") {
            set_once(&mut end, epoch_ms(c, "END")?, "END")?;
        } else if c.eat_keyword("ON") {
            let name = c.identifier("missing timestamp column after WINDOW CUSTOM ... ON")?;
            set_once(&mut column, name, "ON")?;
        } else {
            let got = c.peek().map(describe).unwrap_or_default();
            return Err(syntax(format!(
                "unexpected '{got}' in WINDOW CUSTOM, expected START, END or ON"
            )));
        }
    }
    let (Some(start_ms), Some(end_ms)) = (start, end) else {
        return Err(syntax(
            "WINDOW CUSTOM requires START and END in ms since epoch",
        ));
    };
    if start_ms >= end_ms {
        return Err(syntax(format!(
            "WINDOW CUSTOM START {start_ms} must be before END {end_ms}"
        )));
    }
    Ok(WindowClause {
        kind: "custom",
        timestamp_column: column.unwrap_or_else(|| DEFAULT_WINDOW_COLUMN.to_string()),
        start_ms,
        end_ms,
    })
}

fn set_once<T>(slot: &mut Option<T>, value: T, clause: &str) -> Result<(), LiteError> {
    if slot.is_some() {
        return Err(syntax(format!("duplicate {clause} in WINDOW CUSTOM")));
    }
    *slot = Some(value);
    Ok(())
}

/// A ms-since-epoch value, bare or single-quoted.
fn epoch_ms(c: &mut Cursor<'_>, clause: &str) -> Result<u64, LiteError> {
    let text = match c.peek() {
        Some(Token::Bare(raw)) => (*raw).to_string(),
        Some(Token::String(value)) => value.clone(),
        other => {
            let got = other.map(describe).unwrap_or_default();
            return Err(syntax(format!(
                "WINDOW CUSTOM {clause} must be ms since epoch, got '{got}'"
            )));
        }
    };
    let ms = text.trim().parse::<u64>().map_err(|_| {
        syntax(format!(
            "WINDOW CUSTOM {clause} must be ms since epoch, got '{text}'"
        ))
    })?;
    c.advance();
    Ok(ms)
}
