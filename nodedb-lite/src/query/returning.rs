// SPDX-License-Identifier: Apache-2.0

//! Refusal of a DML `RETURNING` clause.
//!
//! Lite executes no RETURNING projection. A write that ran and answered
//! with a row count would hide that the clause was dropped, so every DML
//! statement that carries one is refused before it writes.
//!
//! UPDATE, UPDATE ... FROM, and MERGE plans carry a `returning` flag, which
//! the visitor checks. INSERT, UPSERT, and DELETE plans carry none, so their
//! clause is found in the statement text, the way Origin finds it.

use nodedb_sql::parser::preprocess::lex::keyword_position_outside_literals;
use nodedb_types::starts_with_ascii_case_insensitive;

use crate::error::LiteError;

/// DML verbs whose plans do not record a RETURNING clause.
const UNFLAGGED_VERBS: [&str; 3] = ["INSERT", "UPSERT", "DELETE"];

/// Refuse a plan whose `returning` flag is set. `statement` names the
/// statement in the error.
pub(crate) fn refuse_returning(statement: &str, returning: bool) -> Result<(), LiteError> {
    if returning {
        return Err(returning_unsupported(statement));
    }
    Ok(())
}

/// Refuse an INSERT, UPSERT, or DELETE statement whose text has a
/// `RETURNING` keyword outside string literals and quoted identifiers.
pub(crate) fn refuse_unflagged_returning(sql: &str) -> Result<(), LiteError> {
    let trimmed = sql.trim_start();
    let Some(verb) = UNFLAGGED_VERBS
        .iter()
        .find(|verb| starts_with_ascii_case_insensitive(trimmed, verb))
    else {
        return Ok(());
    };
    if keyword_position_outside_literals(sql, "RETURNING").is_some() {
        return Err(returning_unsupported(verb));
    }
    Ok(())
}

fn returning_unsupported(statement: &str) -> LiteError {
    LiteError::Unsupported {
        detail: format!("{statement} ... RETURNING is not supported in Lite"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn set_flag_is_refused() {
        let err = refuse_returning("UPDATE", true).unwrap_err();
        assert!(matches!(err, LiteError::Unsupported { .. }));
        refuse_returning("UPDATE", false).unwrap();
    }

    #[test]
    fn unflagged_verbs_with_returning_are_refused() {
        for sql in [
            "INSERT INTO t (id) VALUES ('a') RETURNING id",
            "upsert into t (id) values ('a') returning *",
            "  DELETE FROM t WHERE id = 'a' RETURNING id",
        ] {
            let err = refuse_unflagged_returning(sql).unwrap_err();
            assert!(matches!(err, LiteError::Unsupported { .. }), "{sql}");
        }
    }

    #[test]
    fn returning_inside_literals_or_other_statements_passes() {
        for sql in [
            "INSERT INTO t (id, note) VALUES ('a', 'RETURNING')",
            "INSERT INTO t (id, \"returning\") VALUES ('a', 1)",
            "SELECT 'RETURNING'",
            "UPDATE t SET n = 1 RETURNING n",
        ] {
            refuse_unflagged_returning(sql).unwrap();
        }
    }
}
