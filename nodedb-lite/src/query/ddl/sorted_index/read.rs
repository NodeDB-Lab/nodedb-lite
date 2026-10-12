// SPDX-License-Identifier: Apache-2.0

//! The sorted-index read functions.
//!
//! ```text
//! SELECT RANK(index, key)
//! SELECT TOPK(index, k)            | SELECT * FROM TOPK(index, k)
//! SELECT * FROM RANGE(index, min, max)
//! SELECT SORTED_COUNT(index)
//! ```
//!
//! An index argument is an identifier or a string literal. A string literal
//! names the index exactly as written. A `RANGE` bound is a number, or
//! `NULL`, `NONE` or `*` for an open bound.

use crate::error::LiteError;
use crate::query::ddl::tokens::Token;

use super::cursor::{check_ident, describe, syntax};
use super::statement::SortedIndexStatement;

/// Which read function a statement calls.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum ReadFn {
    Rank,
    TopK,
    Range,
    SortedCount,
}

impl ReadFn {
    /// The function `name` names in the `SELECT f(...)` form, or in the
    /// `SELECT * FROM f(...)` form when `from_form` is set.
    pub(super) fn from_name(name: &str, from_form: bool) -> Option<Self> {
        let upper = name.to_ascii_uppercase();
        match (upper.as_str(), from_form) {
            ("RANK", false) => Some(Self::Rank),
            ("TOPK", _) => Some(Self::TopK),
            ("RANGE", true) => Some(Self::Range),
            ("SORTED_COUNT", false) => Some(Self::SortedCount),
            _ => None,
        }
    }

    /// `RANK` and `RANGE` share their names with standard SQL, so a call
    /// followed by more of the statement (`RANK() OVER (...)`) is left to
    /// the SQL planner. `TOPK` and `SORTED_COUNT` name only sorted-index
    /// reads, so trailing input is an error.
    pub(super) fn shares_sql_name(self) -> bool {
        matches!(self, Self::Rank | Self::Range)
    }

    fn name(self) -> &'static str {
        match self {
            Self::Rank => "RANK",
            Self::TopK => "TOPK",
            Self::Range => "RANGE",
            Self::SortedCount => "SORTED_COUNT",
        }
    }
}

/// Parse the call of `func` whose name is the token at `name_at`.
///
/// `None` when a function sharing its name with standard SQL has more
/// statement after the call: the statement is not a sorted-index read.
pub(super) fn parse_read(
    func: ReadFn,
    tokens: &[Token<'_>],
    name_at: usize,
) -> Option<Result<SortedIndexStatement, LiteError>> {
    let (args, after) = match call_args(tokens, name_at + 1) {
        Ok(found) => found,
        Err(error) => return Some(Err(error)),
    };
    let trailing = match &tokens[after..] {
        [] | [Token::Semicolon] => None,
        [first, ..] => Some(first),
    };
    if let Some(first) = trailing {
        if func.shares_sql_name() {
            return None;
        }
        return Some(Err(syntax(format!(
            "unexpected trailing input '{}' after {}(...): remove it or split the statement",
            describe(first),
            func.name()
        ))));
    }
    Some(statement(func, &args))
}

/// The single-token arguments of the call whose `(` is at `open_at`, and
/// the offset after its `)`.
fn call_args<'t, 'a>(
    tokens: &'t [Token<'a>],
    open_at: usize,
) -> Result<(Vec<&'t Token<'a>>, usize), LiteError> {
    if !matches!(tokens.get(open_at), Some(Token::Open)) {
        return Err(syntax("expected '(' in function call"));
    }
    let mut args = Vec::new();
    let mut offset = open_at + 1;
    if matches!(tokens.get(offset), Some(Token::Close)) {
        return Ok((args, offset + 1));
    }
    loop {
        match tokens.get(offset) {
            Some(token @ (Token::Bare(_) | Token::Identifier(_) | Token::String(_))) => {
                args.push(token);
            }
            None | Some(Token::Semicolon) => {
                return Err(syntax("expected ')' in function call"));
            }
            Some(other) => {
                return Err(syntax(format!(
                    "expected a literal or identifier argument, got '{}'",
                    describe(other)
                )));
            }
        }
        offset += 1;
        match tokens.get(offset) {
            Some(Token::Comma) => offset += 1,
            Some(Token::Close) => return Ok((args, offset + 1)),
            None | Some(Token::Semicolon) => {
                return Err(syntax("expected ')' in function call"));
            }
            Some(other) => {
                return Err(syntax(format!(
                    "expected ',' or ')' between arguments, got '{}'",
                    describe(other)
                )));
            }
        }
    }
}

fn statement(func: ReadFn, args: &[&Token<'_>]) -> Result<SortedIndexStatement, LiteError> {
    for arg in args {
        if let Token::Bare(raw) = arg
            && raw.starts_with('$')
        {
            return Err(LiteError::Unsupported {
                detail: format!(
                    "bound parameter {raw} in {}(): inline the value as a literal",
                    func.name()
                ),
            });
        }
    }
    match func {
        ReadFn::Rank => {
            let [index, key] = args else {
                return Err(syntax("RANK requires 2 arguments: (index_name, key_value)"));
            };
            Ok(SortedIndexStatement::Rank {
                index: index_name(func, index)?,
                key: key_bytes(key)?,
            })
        }
        ReadFn::TopK => {
            let [index, k] = args else {
                return Err(syntax("TOPK requires 2 arguments: (index_name, k)"));
            };
            let k = match k {
                Token::Bare(raw) => raw.parse::<u32>().ok(),
                _ => None,
            }
            .ok_or_else(|| {
                syntax(format!(
                    "TOPK: k must be a positive integer, got '{}'",
                    describe(k)
                ))
            })?;
            Ok(SortedIndexStatement::TopK {
                index: index_name(func, index)?,
                k,
            })
        }
        ReadFn::Range => {
            let [index, min, max] = args else {
                return Err(syntax(
                    "RANGE requires 3 arguments: (index_name, score_min, score_max)",
                ));
            };
            Ok(SortedIndexStatement::Range {
                index: index_name(func, index)?,
                min: score_bound(min)?,
                max: score_bound(max)?,
            })
        }
        ReadFn::SortedCount => {
            let [index] = args else {
                return Err(syntax("SORTED_COUNT requires 1 argument: (index_name)"));
            };
            Ok(SortedIndexStatement::Count {
                index: index_name(func, index)?,
            })
        }
    }
}

/// An index argument: a string literal names the index exactly as written,
/// an identifier resolves as identifiers do.
fn index_name(func: ReadFn, token: &Token<'_>) -> Result<String, LiteError> {
    match token {
        Token::String(name) => Ok(name.clone()),
        Token::Bare(raw) | Token::Identifier(raw) => check_ident(raw),
        other => Err(syntax(format!(
            "{}: index name must be an identifier or a string literal, got '{}'",
            func.name(),
            describe(other)
        ))),
    }
}

/// A `RANK` key: a string literal's text, or a bare literal's text (an
/// integer key `42` names the row stored under `'42'`).
fn key_bytes(token: &Token<'_>) -> Result<Vec<u8>, LiteError> {
    match token {
        Token::String(text) => Ok(text.as_bytes().to_vec()),
        Token::Bare(raw) => Ok(raw.as_bytes().to_vec()),
        other => Err(syntax(format!(
            "RANK: key_value must be a string literal or a number, got '{}'",
            describe(other)
        ))),
    }
}

/// A `RANGE` bound. `NULL`, `NONE` and `*` are open. A number is a score.
/// Lite ranks by one numeric score, so a non-numeric bound is unsupported.
fn score_bound(token: &Token<'_>) -> Result<Option<f64>, LiteError> {
    let text = match token {
        Token::Bare(raw) => (*raw).to_string(),
        Token::String(text) => text.trim().to_string(),
        other => {
            return Err(syntax(format!(
                "RANGE: score bound must be a number, NULL, NONE or *, got '{}'",
                describe(other)
            )));
        }
    };
    if text.eq_ignore_ascii_case("NULL") || text.eq_ignore_ascii_case("NONE") || text == "*" {
        return Ok(None);
    }
    match text.parse::<f64>() {
        Ok(score) if score.is_finite() => Ok(Some(score)),
        Ok(_) => Err(syntax(format!(
            "RANGE: score bound '{text}' must be a finite number"
        ))),
        Err(_) => Err(LiteError::Unsupported {
            detail: format!(
                "RANGE string score bound '{text}': Lite sorted indexes rank by one numeric \
                 score column; pass a number, NULL, NONE or *"
            ),
        }),
    }
}
