// SPDX-License-Identifier: Apache-2.0

//! The class of each SQL planner error.

use nodedb_sql::SqlError;

use super::LiteError;

/// Exhaustive, so a new planner error picks its own class rather than
/// defaulting to a plan error.
impl From<SqlError> for LiteError {
    fn from(e: SqlError) -> Self {
        match e {
            // A value of the wrong kind for its column. `42804`.
            mismatch @ (SqlError::TypeMismatch { .. }
            | SqlError::VectorElementNotNumeric { .. }) => Self::TypeMismatch {
                collection: String::new(),
                detail: mismatch.to_string(),
            },
            // A value past the range of its type. `22003`.
            range @ (SqlError::ConstantOverflow { .. }
            | SqlError::NumericLiteralOutOfRange { .. }
            | SqlError::IntegerOutOfRange { .. }
            | SqlError::FloatOutOfRange { .. }
            | SqlError::DecimalOutOfRange { .. }) => Self::NumericValueOutOfRange {
                detail: range.to_string(),
            },
            // A constant the planner evaluated and could not compute. `22`.
            data @ (SqlError::DivisionByZero | SqlError::DataException { .. }) => {
                Self::DataException {
                    detail: data.to_string(),
                }
            }
            SqlError::UndefinedObject { kind, name } => Self::UndefinedObject {
                object: format!("{kind} \"{name}\""),
            },
            SqlError::ObjectNotInPrerequisiteState { object, detail } => {
                Self::ObjectNotReady { object, detail }
            }
            SqlError::Unsupported { detail } => Self::Unsupported { detail },
            // A statement the object's kind does not take, such as a write to
            // a continuous aggregate. Origin gives it the bad-request class.
            SqlError::WrongObjectType { detail } => Self::BadRequest { detail },
            // Refusals of a construct NodeDB does not implement. `0A000`.
            refused @ (SqlError::SequencePerRowUnsupported { .. }
            | SqlError::SearchFunctionOutsideSearch { .. }
            | SqlError::UnsupportedConstraint { .. }
            | SqlError::ConflictingEngineClause { .. }) => Self::Unsupported {
                detail: refused.to_string(),
            },
            retry @ SqlError::RetryableSchemaChanged { .. } => Self::RetryableRefusal {
                detail: retry.to_string(),
            },
            // Statement errors the caller must fix.
            plan @ (SqlError::Parse { .. }
            | SqlError::UnknownTable { .. }
            | SqlError::UndefinedFunction { .. }
            | SqlError::UnknownColumn { .. }
            | SqlError::TextColumn { .. }
            | SqlError::AmbiguousColumn { .. }
            | SqlError::Arity { .. }
            | SqlError::InvalidLimitValue { .. }
            | SqlError::UnevaluableDefault { .. }
            | SqlError::SetvalInColumnDefault { .. }
            | SqlError::InvalidFunction { .. }
            | SqlError::InvalidWindowFrame { .. }
            | SqlError::MissingField { .. }
            | SqlError::InsertColumnArityMismatch { .. }
            | SqlError::PositionalKvInsertUnsupported { .. }
            | SqlError::InvalidIdentifier { .. }
            | SqlError::ReservedIdentifier { .. }
            | SqlError::InvalidRecursiveSetOp { .. }
            | SqlError::InvalidRecursiveSelfRef { .. }
            | SqlError::RecursiveColumnMismatch { .. }
            | SqlError::DuplicateRecursiveColumn { .. }
            | SqlError::RecursionDepthExceeded { .. }
            | SqlError::CollectionDeactivated { .. }) => Self::Query(plan.to_string()),
        }
    }
}

#[cfg(test)]
mod tests {
    use nodedb_types::error::{ErrorCode, NodeDbError};

    use super::*;

    fn public(e: SqlError) -> NodeDbError {
        NodeDbError::from(LiteError::from(e))
    }

    #[test]
    fn a_text_element_in_a_vector_literal_is_a_type_mismatch() {
        let e = LiteError::from(SqlError::VectorElementNotNumeric {
            column: "embedding".into(),
            dim: 3,
            element: "text 'a'".into(),
        });
        assert!(matches!(e, LiteError::TypeMismatch { .. }), "{e:?}");
        assert_eq!(NodeDbError::from(e).code(), ErrorCode::TYPE_MISMATCH);
        let plain = public(SqlError::TypeMismatch { detail: "x".into() });
        assert_eq!(plain.code(), ErrorCode::TYPE_MISMATCH);
    }

    #[test]
    fn an_out_of_range_value_is_numeric_value_out_of_range() {
        for e in [
            SqlError::NumericLiteralOutOfRange {
                literal: "1e40".into(),
            },
            SqlError::ConstantOverflow { detail: "x".into() },
            SqlError::IntegerOutOfRange {
                column: "n".into(),
                value: i64::MAX,
                declared_type: "INTEGER",
            },
        ] {
            let mapped = public(e);
            assert_eq!(mapped.code(), ErrorCode::OVERFLOW);
            assert!(mapped.is_overflow(), "{mapped}");
        }
    }

    #[test]
    fn a_failed_constant_is_a_data_exception() {
        let mapped = public(SqlError::DataException { detail: "x".into() });
        assert_eq!(mapped.code(), ErrorCode::DATA_EXCEPTION);
    }

    #[test]
    fn a_statement_error_is_a_plan_error() {
        for e in [
            SqlError::Parse { detail: "x".into() },
            SqlError::UndefinedFunction { name: "f".into() },
        ] {
            assert_eq!(public(e).code(), ErrorCode::PLAN_ERROR);
        }
        assert_eq!(
            NodeDbError::from(LiteError::Query("x".into())).code(),
            ErrorCode::PLAN_ERROR
        );
    }

    #[test]
    fn an_absent_object_is_an_undefined_object() {
        let mapped = public(SqlError::UndefinedObject {
            kind: "sequence",
            name: "s".into(),
        });
        assert_eq!(mapped.code(), ErrorCode::UNDEFINED_OBJECT);
    }
}
