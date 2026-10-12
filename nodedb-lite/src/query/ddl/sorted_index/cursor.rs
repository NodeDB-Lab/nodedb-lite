// SPDX-License-Identifier: Apache-2.0

//! Token cursor and error helpers shared by the sorted-index parsers.

use crate::error::LiteError;
use crate::query::ddl::tokens::Token;

/// Statement family named in tokenizer errors.
pub(super) const STATEMENT: &str = "SORTED INDEX";

/// A syntax error. Messages follow the wording Origin uses for the same
/// statement.
pub(super) fn syntax(message: impl Into<String>) -> LiteError {
    LiteError::Query(message.into())
}

/// The text of `token` as an error message quotes it.
pub(super) fn describe(token: &Token<'_>) -> String {
    match token {
        Token::Bare(raw) | Token::Identifier(raw) => (*raw).to_string(),
        Token::String(value) => format!("'{}'", value.replace('\'', "''")),
        Token::Open => "(".into(),
        Token::Close => ")".into(),
        Token::Comma => ",".into(),
        Token::Semicolon => ";".into(),
    }
}

/// Resolve a raw identifier token: a bare name folds to lower case, a
/// double-quoted name keeps its case.
pub(super) fn check_ident(raw: &str) -> Result<String, LiteError> {
    nodedb_sql::reserved::check_identifier(raw).map_err(|error| syntax(error.to_string()))
}

pub(super) struct Cursor<'a> {
    tokens: Vec<Token<'a>>,
    offset: usize,
}

impl<'a> Cursor<'a> {
    pub(super) fn new(tokens: Vec<Token<'a>>) -> Self {
        Self { tokens, offset: 0 }
    }

    pub(super) fn peek(&self) -> Option<&Token<'a>> {
        self.tokens.get(self.offset)
    }

    pub(super) fn advance(&mut self) {
        self.offset += 1;
    }

    /// Consume the next token when it is the bare keyword `keyword`.
    pub(super) fn eat_keyword(&mut self, keyword: &str) -> bool {
        let matched = self.peek().is_some_and(|token| token.keyword(keyword));
        if matched {
            self.offset += 1;
        }
        matched
    }

    /// Whether only an optional `;` remains.
    pub(super) fn at_end(&self) -> bool {
        matches!(
            &self.tokens[self.offset.min(self.tokens.len())..],
            [] | [Token::Semicolon]
        )
    }

    /// Consume an identifier. `missing` is the error when the next token
    /// is not one.
    pub(super) fn identifier(&mut self, missing: &str) -> Result<String, LiteError> {
        let raw = match self.peek() {
            Some(Token::Bare(raw) | Token::Identifier(raw)) => *raw,
            _ => return Err(syntax(missing)),
        };
        self.offset += 1;
        check_ident(raw)
    }

    /// Fail unless only an optional `;` remains.
    pub(super) fn finish(&self) -> Result<(), LiteError> {
        if self.at_end() {
            return Ok(());
        }
        let trailing = self.peek().map(describe).unwrap_or_default();
        Err(syntax(format!(
            "unexpected trailing input '{trailing}': remove it or split the statement"
        )))
    }
}
