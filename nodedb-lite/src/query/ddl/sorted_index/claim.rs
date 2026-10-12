// SPDX-License-Identifier: Apache-2.0

//! Which statements the sorted-index family claims before the SQL planner.
//!
//! `CREATE SORTED ...` and `DROP SORTED ...` are always claimed, so a
//! malformed declaration reports its own syntax error. A `SELECT` is claimed
//! when it calls a read function in its leading position:
//! `SELECT RANK(`, `SELECT TOPK(`, `SELECT SORTED_COUNT(`,
//! `SELECT * FROM TOPK(` or `SELECT * FROM RANGE(`. A `RANK` or `RANGE` call
//! followed by more statement is standard SQL and is not claimed.

use crate::error::LiteError;
use crate::query::ddl::tokens::tokenize;

use super::create::{parse_create, parse_drop};
use super::cursor::{Cursor, STATEMENT};
use super::read::{ReadFn, parse_read};
use super::statement::SortedIndexStatement;

/// Claim and parse a sorted-index statement. `None` when `sql` is not one.
pub(in crate::query) fn parse_sorted_index_sql(
    sql: &str,
) -> Option<Result<SortedIndexStatement, LiteError>> {
    let (first, rest) = leading_word(sql)?;
    let create = first.eq_ignore_ascii_case("CREATE");
    if create || first.eq_ignore_ascii_case("DROP") {
        let (second, _) = leading_word(rest)?;
        if !second.eq_ignore_ascii_case("SORTED") {
            return None;
        }
        return Some(tokenize(sql, STATEMENT).and_then(|tokens| {
            let cursor = Cursor::new(tokens);
            if create {
                parse_create(cursor)
            } else {
                parse_drop(cursor)
            }
        }));
    }
    if !first.eq_ignore_ascii_case("SELECT") {
        return None;
    }
    let (func, name_at) = read_call(rest)?;
    let tokens = match tokenize(sql, STATEMENT) {
        Ok(tokens) => tokens,
        // An unterminated quote after `RANK(` can belong to a standard SQL
        // statement: the planner reports it.
        Err(_) if func.shares_sql_name() => return None,
        Err(error) => return Some(Err(error)),
    };
    parse_read(func, &tokens, name_at)
}

/// The read function `rest` (the text after `SELECT`) leads with, and the
/// token offset of its name in the whole statement.
fn read_call(rest: &str) -> Option<(ReadFn, usize)> {
    let (second, after) = leading_word(rest)?;
    let (name, after, name_at) = if second == "*" {
        let (third, after) = leading_word(after)?;
        if !third.eq_ignore_ascii_case("FROM") {
            return None;
        }
        let (name, after) = leading_word(after)?;
        (name, after, 3)
    } else {
        (second, after, 1)
    };
    if !after.trim_start().starts_with('(') {
        return None;
    }
    ReadFn::from_name(name, name_at == 3).map(|func| (func, name_at))
}

/// The first word of `sql` and the text after it. Words end where the
/// tokenizer ends a bare token.
fn leading_word(sql: &str) -> Option<(&str, &str)> {
    let sql = sql.trim_start();
    let end = sql
        .find(|ch: char| ch.is_whitespace() || matches!(ch, '(' | ')' | ',' | ';' | '\'' | '"'))
        .unwrap_or(sql.len());
    (end > 0).then_some((&sql[..end], &sql[end..]))
}

#[cfg(test)]
mod tests {
    use super::super::statement::{CreateSortedIndex, WindowClause};
    use super::*;

    fn parse(sql: &str) -> Result<SortedIndexStatement, LiteError> {
        parse_sorted_index_sql(sql).expect("claimed")
    }

    #[test]
    fn create_parses_columns_key_and_calendar_window() {
        assert_eq!(
            parse("create sorted index LB on Players (score desc, ts) key id window daily on ts;")
                .expect("parse"),
            SortedIndexStatement::Create(CreateSortedIndex {
                name: "lb".into(),
                collection: "players".into(),
                sort_columns: vec![("score".into(), "DESC".into()), ("ts".into(), "ASC".into())],
                key_column: "id".into(),
                window: WindowClause {
                    kind: "daily",
                    timestamp_column: "ts".into(),
                    start_ms: 0,
                    end_ms: 0,
                },
            })
        );
    }

    #[test]
    fn window_without_on_stamps_from_updated_at() {
        let SortedIndexStatement::Create(create) =
            parse("CREATE SORTED INDEX lb ON p (score) KEY id WINDOW WEEKLY").expect("parse")
        else {
            panic!("not a create");
        };
        assert_eq!(create.window.kind, "weekly");
        assert_eq!(create.window.timestamp_column, "updated_at");
    }

    #[test]
    fn custom_window_takes_start_end_and_column_in_any_order() {
        let SortedIndexStatement::Create(create) = parse(
            "CREATE SORTED INDEX lb ON p (score, ts) KEY id WINDOW CUSTOM ON ts END '20' START 10",
        )
        .expect("parse") else {
            panic!("not a create");
        };
        assert_eq!(
            create.window,
            WindowClause {
                kind: "custom",
                timestamp_column: "ts".into(),
                start_ms: 10,
                end_ms: 20,
            }
        );
    }

    #[test]
    fn reads_parse_their_arguments() {
        assert_eq!(
            parse("SELECT RANK(lb, 'alice')").expect("rank"),
            SortedIndexStatement::Rank {
                index: "lb".into(),
                key: b"alice".to_vec(),
            }
        );
        assert_eq!(
            parse("select * from topk('LB', 3);").expect("topk"),
            SortedIndexStatement::TopK {
                index: "LB".into(),
                k: 3,
            }
        );
        assert_eq!(
            parse("SELECT * FROM RANGE(lb, -1.5, NULL)").expect("range"),
            SortedIndexStatement::Range {
                index: "lb".into(),
                min: Some(-1.5),
                max: None,
            }
        );
        assert_eq!(
            parse("SELECT SORTED_COUNT (lb)").expect("count"),
            SortedIndexStatement::Count { index: "lb".into() }
        );
    }

    #[test]
    fn standard_sql_sharing_a_function_name_is_not_claimed() {
        for sql in [
            "SELECT RANK() OVER (ORDER BY score) FROM players",
            "SELECT * FROM RANGE(1, 10) WHERE x > 1",
            "SELECT RANGE(lb, 1, 2)",
            "SELECT * FROM SORTED_COUNT(lb)",
            "SELECT rank FROM players",
            "CREATE INDEX lb ON p (score)",
            "DROP INDEX lb",
            "SELECT 1",
        ] {
            assert!(parse_sorted_index_sql(sql).is_none(), "{sql}");
        }
    }

    #[test]
    fn malformed_sorted_index_statements_are_claimed_and_return_errors() {
        for sql in [
            "CREATE SORTED lb ON p (score) KEY id",
            "CREATE SORTED INDEX",
            "CREATE SORTED INDEX lb p (score) KEY id",
            "CREATE SORTED INDEX lb ON",
            "CREATE SORTED INDEX lb ON p score KEY id",
            "CREATE SORTED INDEX lb ON p () KEY id",
            "CREATE SORTED INDEX lb ON p (score DOWN) KEY id",
            "CREATE SORTED INDEX lb ON p (score DESC extra) KEY id",
            "CREATE SORTED INDEX lb ON p (score",
            "CREATE SORTED INDEX lb ON p (score)",
            "CREATE SORTED INDEX lb ON p (score) KEY",
            "CREATE SORTED INDEX lb ON p (score) KEY id WINDOW",
            "CREATE SORTED INDEX lb ON p (score) KEY id WINDOW HOURLY",
            "CREATE SORTED INDEX lb ON p (score) KEY id WINDOW CUSTOM START 10",
            "CREATE SORTED INDEX lb ON p (score) KEY id WINDOW CUSTOM START 20 END 10",
            "CREATE SORTED INDEX lb ON p (score) KEY id WINDOW CUSTOM START '2026-01-01' END 5",
            "CREATE SORTED INDEX lb ON p (score) KEY id WINDOW CUSTOM START 1 START 2 END 3",
            "CREATE SORTED INDEX lb ON p (score) KEY id trailing",
            "CREATE SORTED INDEX lb ON p (score) KEY id; SELECT 1",
            "DROP SORTED INDEX",
            "DROP SORTED INDEX lb extra",
            "SELECT RANK(lb)",
            "SELECT RANK(lb, 'a', 'b')",
            "SELECT TOPK(lb)",
            "SELECT TOPK(lb, -1)",
            "SELECT TOPK(lb, '3')",
            "SELECT TOPK(lb, 3) LIMIT 1",
            "SELECT * FROM RANGE(lb, 1)",
            "SELECT * FROM RANGE(lb, 'inf', 2)",
            "SELECT SORTED_COUNT()",
            "SELECT SORTED_COUNT(lb",
            "SELECT TOPK(lb, (3))",
        ] {
            assert!(parse(sql).is_err(), "{sql}");
        }
    }

    #[test]
    fn a_non_numeric_range_bound_is_unsupported() {
        let error = parse("SELECT * FROM RANGE(lb, 'gold', NULL)").expect_err("refused");
        assert!(matches!(error, LiteError::Unsupported { .. }), "{error:?}");
        assert!(error.to_string().contains("gold"), "{error}");
    }
}
