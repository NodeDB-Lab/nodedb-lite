// SPDX-License-Identifier: Apache-2.0

//! Anonymous SEARCH INDEX declarations with complete input consumption.

use std::collections::HashSet;

use nodedb_fts::index::analyzer_config::analyzer_exists;

use crate::error::LiteError;
use crate::query::ddl::tokens::{Token, tokenize};

#[derive(Debug, PartialEq)]
pub(in crate::query) enum SearchIndexStatement {
    Create {
        collection: String,
        fields: Vec<String>,
        analyzer: String,
        fuzzy: bool,
    },
    Drop {
        name: String,
        if_exists: bool,
    },
}

/// Claim SEARCH DDL before the shared parser, including incomplete declarations.
pub(in crate::query) fn parse_search_index_ddl(
    sql: &str,
) -> Option<Result<SearchIndexStatement, LiteError>> {
    let (first, rest) = leading_word(sql)?;
    let (second, _) = leading_word(rest)?;
    if !(first.eq_ignore_ascii_case("CREATE") || first.eq_ignore_ascii_case("DROP"))
        || !second.eq_ignore_ascii_case("SEARCH")
    {
        return None;
    }
    Some(parse(sql))
}

fn leading_word(sql: &str) -> Option<(&str, &str)> {
    let sql = sql.trim_start();
    let end = sql
        .find(|ch: char| ch.is_whitespace() || matches!(ch, '(' | ')' | ',' | ';' | '\'' | '"'))
        .unwrap_or(sql.len());
    (end > 0).then_some((&sql[..end], &sql[end..]))
}

fn parse(sql: &str) -> Result<SearchIndexStatement, LiteError> {
    let tokens = tokenize(sql, "SEARCH INDEX")?;
    let mut parser = Parser {
        tokens,
        offset: 0,
        sql,
    };
    let create = parser.next().is_some_and(|token| token.keyword("CREATE"));
    parser.require_keyword("SEARCH")?;
    parser.require_keyword("INDEX")?;
    let statement = if create {
        parser.create()?
    } else {
        parser.drop()?
    };
    if matches!(parser.peek(), Some(Token::Semicolon)) {
        parser.offset += 1;
    }
    if parser.peek().is_some() {
        return Err(parser.error("remove trailing tokens or additional statements"));
    }
    Ok(statement)
}

struct Parser<'a> {
    tokens: Vec<Token<'a>>,
    offset: usize,
    sql: &'a str,
}

impl Parser<'_> {
    fn error(&self, action: &str) -> LiteError {
        LiteError::Query(format!(
            "SEARCH INDEX parse error in '{}': {action}",
            self.sql
        ))
    }

    fn peek(&self) -> Option<&Token<'_>> {
        self.tokens.get(self.offset)
    }

    fn next(&mut self) -> Option<&Token<'_>> {
        let offset = self.offset;
        self.offset += 1;
        self.tokens.get(offset)
    }

    fn require_keyword(&mut self, keyword: &str) -> Result<(), LiteError> {
        if self.next().is_some_and(|token| token.keyword(keyword)) {
            return Ok(());
        }
        Err(self.error(&format!("supply {keyword}")))
    }

    fn identifier(&mut self) -> Result<String, LiteError> {
        let raw = match self.next() {
            Some(Token::Bare(raw) | Token::Identifier(raw)) => raw.to_string(),
            _ => return Err(self.error("supply a collection, field, or index identifier")),
        };
        nodedb_sql::reserved::check_identifier(&raw)
            .map_err(|error| self.error(&format!("replace identifier '{raw}': {error}")))
    }

    fn create(&mut self) -> Result<SearchIndexStatement, LiteError> {
        self.require_keyword("ON")?;
        let collection = self.identifier()?;
        if !matches!(self.next(), Some(Token::Open)) {
            return Err(self.error("enclose selected fields in parentheses"));
        }
        let mut fields = Vec::new();
        let mut seen = HashSet::new();
        loop {
            let field = self.identifier()?;
            if !seen.insert(field.clone()) {
                return Err(self.error(&format!("remove duplicate field '{field}'")));
            }
            fields.push(field);
            match self.next() {
                Some(Token::Comma) => {}
                Some(Token::Close) => break,
                _ => {
                    return Err(self.error("separate fields with commas and close the parentheses"));
                }
            }
        }
        let mut analyzer = None;
        let mut fuzzy = None;
        while self
            .peek()
            .is_some_and(|token| !matches!(token, Token::Semicolon))
        {
            if self.peek().is_some_and(|token| token.keyword("ANALYZER")) {
                self.offset += 1;
                if analyzer.is_some() {
                    return Err(self.error("remove duplicate ANALYZER"));
                }
                let name = match self.next() {
                    Some(Token::String(name)) => name.trim().to_lowercase(),
                    _ => return Err(self.error("supply a single-quoted ANALYZER name")),
                };
                if name.is_empty() || !analyzer_exists(&name) {
                    return Err(self.error(&format!(
                        "replace unknown ANALYZER '{name}' with a registered name"
                    )));
                }
                analyzer = Some(name);
            } else if self.peek().is_some_and(|token| token.keyword("FUZZY")) {
                self.offset += 1;
                if fuzzy.is_some() {
                    return Err(self.error("remove duplicate FUZZY"));
                }
                fuzzy = Some(match self.next() {
                    Some(Token::Bare(value)) if value.eq_ignore_ascii_case("true") => true,
                    Some(Token::Bare(value)) if value.eq_ignore_ascii_case("false") => false,
                    _ => return Err(self.error("supply bare true or false after FUZZY")),
                });
            } else {
                return Err(self.error("use ANALYZER or FUZZY after the field list"));
            }
        }
        Ok(SearchIndexStatement::Create {
            collection,
            fields,
            analyzer: analyzer.unwrap_or_else(|| "standard".into()),
            fuzzy: fuzzy.unwrap_or(false),
        })
    }

    fn drop(&mut self) -> Result<SearchIndexStatement, LiteError> {
        let if_exists = self.peek().is_some_and(|token| token.keyword("IF"));
        if if_exists {
            self.offset += 1;
            self.require_keyword("EXISTS")?;
        }
        let name = self.identifier()?;
        Ok(SearchIndexStatement::Drop { name, if_exists })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn anonymous_create_preserves_quoted_names_and_normalizes_bare_names() {
        let statement = parse_search_index_ddl("cReAtE\tSEARCH INDEX ON \"ArticleCase\"(TITLE,\"Body\") FUZZY TrUe ANALYZER ' Standard ';\n")
            .expect("search declaration").expect("parse");
        assert_eq!(
            statement,
            SearchIndexStatement::Create {
                collection: "ArticleCase".into(),
                fields: vec!["title".into(), "Body".into()],
                analyzer: "standard".into(),
                fuzzy: true,
            }
        );
    }

    #[test]
    fn anonymous_create_defaults_and_drop_if_exists_parse() {
        assert_eq!(
            parse("CREATE SEARCH INDEX ON articles(body)").expect("create"),
            SearchIndexStatement::Create {
                collection: "articles".into(),
                fields: vec!["body".into()],
                analyzer: "standard".into(),
                fuzzy: false
            }
        );
        assert_eq!(
            parse("DROP SEARCH INDEX IF EXISTS FTS_articles;").expect("drop"),
            SearchIndexStatement::Drop {
                name: "fts_articles".into(),
                if_exists: true
            }
        );
        assert_eq!(
            parse("DROP SEARCH INDEX \"fts_ArticleCase\"").expect("quoted drop"),
            SearchIndexStatement::Drop {
                name: "fts_ArticleCase".into(),
                if_exists: false
            }
        );
    }

    #[test]
    fn unrelated_index_declarations_remain_unclaimed() {
        for sql in [
            "CREATE INDEX a ON b(c)",
            "DROP INDEX a",
            "CREATE \"SEARCH\" INDEX",
            "SELECT 1",
            "CREATE",
        ] {
            assert!(parse_search_index_ddl(sql).is_none(), "{sql}");
        }
    }

    #[test]
    fn malformed_search_declarations_remain_claimed_and_return_errors() {
        for sql in [
            "CREATE SEARCH",
            "DROP SEARCH",
            "CREATE SEARCH;",
            "DROP SEARCH(a)",
            "CREATE SEARCH ON a(b)",
            "CREATE SEARCH INDEX name ON a(b)",
            "CREATE SEARCH INDEX IF NOT EXISTS ON a(b)",
            "CREATE SEARCH INDEX ON a()",
            "CREATE SEARCH INDEX ON a(b,)",
            "CREATE SEARCH INDEX ON a(b B)",
            "CREATE SEARCH INDEX ON a(b,B)",
            "CREATE SEARCH INDEX ON a(SEARCH)",
            "CREATE SEARCH INDEX ON a(\"b\"\"c\")",
            "CREATE SEARCH INDEX ON a(b) ANALYZER ''",
            "CREATE SEARCH INDEX ON a(b) ANALYZER 'unknown'",
            "CREATE SEARCH INDEX ON a(b) ANALYZER standard",
            "CREATE SEARCH INDEX ON a(b) ANALYZER 'a''b'",
            "CREATE SEARCH INDEX ON a(b) ANALYZER 'standard' ANALYZER 'standard'",
            "CREATE SEARCH INDEX ON a(b) FUZZY true FUZZY false",
            "CREATE SEARCH INDEX ON a(b) FUZZY 'true'",
            "CREATE SEARCH INDEX ON a(b) FUZZY 1",
            "CREATE SEARCH INDEX ON a(b) FUZZY =true",
            "CREATE SEARCH INDEX ON a(b) FUZZY",
            "CREATE SEARCH INDEX ON a(b) EXTRA",
            "CREATE SEARCH INDEX ON a(b); SELECT 1",
            "CREATE SEARCH INDEX ON a(b);;",
            "DROP SEARCH INDEX",
            "DROP SEARCH INDEX IF a",
            "DROP SEARCH INDEX a trailing",
            "CREATE SEARCH INDEX ON \"unterminated",
        ] {
            assert!(
                parse_search_index_ddl(sql).expect("claimed").is_err(),
                "{sql}"
            );
        }
    }
}
