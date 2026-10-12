// SPDX-License-Identifier: Apache-2.0

//! SQL token boundaries for the Lite-parsed statement families
//! (`SEARCH INDEX`, `SORTED INDEX`).

use crate::error::LiteError;

#[derive(Debug, PartialEq)]
pub(in crate::query::ddl) enum Token<'a> {
    Bare(&'a str),
    Identifier(&'a str),
    String(String),
    Open,
    Close,
    Comma,
    Semicolon,
}

impl Token<'_> {
    pub(in crate::query::ddl) fn keyword(&self, keyword: &str) -> bool {
        matches!(self, Self::Bare(value) if value.eq_ignore_ascii_case(keyword))
    }
}

/// Split `sql` into tokens. `statement` names the statement family in the
/// error an unterminated quoted token returns.
pub(in crate::query::ddl) fn tokenize<'a>(
    sql: &'a str,
    statement: &str,
) -> Result<Vec<Token<'a>>, LiteError> {
    let mut tokens = Vec::new();
    let mut offset = 0;
    while offset < sql.len() {
        let rest = &sql[offset..];
        let Some(ch) = rest.chars().next() else { break };
        if ch.is_whitespace() {
            offset += ch.len_utf8();
            continue;
        }
        let punctuation = match ch {
            '(' => Some(Token::Open),
            ')' => Some(Token::Close),
            ',' => Some(Token::Comma),
            ';' => Some(Token::Semicolon),
            _ => None,
        };
        if let Some(token) = punctuation {
            tokens.push(token);
            offset += ch.len_utf8();
        } else if ch == '\'' || ch == '"' {
            let end = quoted_end(sql, offset, ch, statement)?;
            let raw = &sql[offset..end];
            tokens.push(if ch == '"' {
                Token::Identifier(raw)
            } else {
                Token::String(raw[1..raw.len() - 1].replace("''", "'"))
            });
            offset = end;
        } else {
            let len = rest
                .find(|c: char| {
                    c.is_whitespace() || matches!(c, '(' | ')' | ',' | ';' | '\'' | '"')
                })
                .unwrap_or(rest.len());
            tokens.push(Token::Bare(&rest[..len]));
            offset += len;
        }
    }
    Ok(tokens)
}

fn quoted_end(sql: &str, start: usize, quote: char, statement: &str) -> Result<usize, LiteError> {
    let mut offset = start + 1;
    while offset < sql.len() {
        let rest = &sql[offset..];
        let Some(ch) = rest.chars().next() else { break };
        offset += ch.len_utf8();
        if ch == quote {
            if sql[offset..].starts_with(quote) {
                offset += 1;
            } else {
                return Ok(offset);
            }
        }
    }
    Err(LiteError::Query(format!(
        "{statement} parse error at '{}': close the quoted token",
        &sql[start..]
    )))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn attached_punctuation_and_escaped_quotes_keep_token_boundaries() {
        assert_eq!(
            tokenize("ON\t\"Case\"(body,'a''b');", "TEST").expect("tokens"),
            vec![
                Token::Bare("ON"),
                Token::Identifier("\"Case\""),
                Token::Open,
                Token::Bare("body"),
                Token::Comma,
                Token::String("a'b".into()),
                Token::Close,
                Token::Semicolon
            ]
        );
    }

    #[test]
    fn incomplete_quoted_tokens_return_errors() {
        for sql in ["'abc", "\"abc", "'abc''", "\"abc\"\""] {
            assert!(tokenize(sql, "TEST").is_err(), "{sql}");
        }
    }
}
