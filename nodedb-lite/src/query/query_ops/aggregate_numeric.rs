// SPDX-License-Identifier: Apache-2.0
//! Numeric folds for the generic aggregate: SUM, AVG, MIN, MAX.
//!
//! SUM and AVG total in an [`ExactSum`], the rule Origin applies: integer
//! inputs add exactly, a SUM past `i64` is a `Decimal`, and a total past the
//! `Decimal` range is [`EvalError::NumericOverflow`].

use nodedb_query::numeric_sum::sum_input;
use nodedb_query::simd_agg::ts_runtime;
use nodedb_query::simd_agg_i64::i64_runtime;
use nodedb_query::{EvalError, ExactSum};
use nodedb_types::value::Value;

/// The numeric inputs of one aggregate. MIN and MAX read them split by type:
/// a mix of integers and floats compares as floats. SUM and AVG read the
/// exact total. Non-numeric inputs are skipped, except numeric text, which
/// SUM and AVG add as the number it spells.
pub(super) struct Numeric {
    floats: Vec<f64>,
    ints: Vec<i64>,
    exact: ExactSum,
}

impl Numeric {
    pub(super) fn collect<'v>(values: impl Iterator<Item = &'v Value>) -> Self {
        let mut floats = Vec::new();
        let mut ints = Vec::new();
        let mut exact = ExactSum::new();
        for value in values {
            if let Some(number) = sum_input(value) {
                exact.add_value(&number);
            }
            match value {
                Value::Float(f) => floats.push(*f),
                Value::Integer(i) => ints.push(*i),
                _ => {}
            }
        }
        Self {
            floats,
            ints,
            exact,
        }
    }

    pub(super) fn is_empty(&self) -> bool {
        self.floats.is_empty() && self.ints.is_empty()
    }

    /// Every integer and float input as `f64`.
    pub(super) fn as_f64(&self) -> Vec<f64> {
        self.floats
            .iter()
            .copied()
            .chain(self.ints.iter().map(|i| *i as f64))
            .collect()
    }

    /// NULL when empty. An integer total that fits `i64` is an `Integer`, a
    /// larger one a `Decimal`. A total past the `Decimal` range is
    /// `NumericOverflow`.
    pub(super) fn sum(&self) -> Result<Value, EvalError> {
        self.exact.sum()
    }

    /// NULL when empty, else the mean as a float.
    pub(super) fn avg(&self) -> Result<Value, EvalError> {
        self.exact.avg()
    }

    /// NULL when empty, else the smallest input.
    pub(super) fn min(&self) -> Value {
        if self.is_empty() {
            Value::Null
        } else if self.floats.is_empty() {
            Value::Integer((i64_runtime().min_i64)(&self.ints))
        } else {
            Value::Float((ts_runtime().min_f64)(&self.as_f64()))
        }
    }

    /// NULL when empty, else the largest input.
    pub(super) fn max(&self) -> Value {
        if self.is_empty() {
            Value::Null
        } else if self.floats.is_empty() {
            Value::Integer((i64_runtime().max_i64)(&self.ints))
        } else {
            Value::Float((ts_runtime().max_f64)(&self.as_f64()))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::Numeric;
    use nodedb_types::value::Value;
    use rust_decimal::Decimal;

    #[test]
    fn mixed_inputs_fold_as_floats() {
        let values = [Value::Integer(2), Value::Float(0.5), Value::Integer(1)];
        let numeric = Numeric::collect(values.iter());
        assert_eq!(numeric.sum().unwrap(), Value::Float(3.5));
        assert_eq!(numeric.min(), Value::Float(0.5));
        assert_eq!(numeric.max(), Value::Float(2.0));
    }

    #[test]
    fn integer_inputs_stay_integers_and_empty_is_null() {
        let values = [Value::Integer(5), Value::Integer(1)];
        let numeric = Numeric::collect(values.iter());
        assert_eq!(numeric.sum().unwrap(), Value::Integer(6));
        assert_eq!(numeric.avg().unwrap(), Value::Float(3.0));
        assert_eq!(numeric.min(), Value::Integer(1));
        let empty = Numeric::collect(std::iter::empty());
        assert_eq!(empty.sum().unwrap(), Value::Null);
        assert_eq!(empty.avg().unwrap(), Value::Null);
    }

    #[test]
    fn integer_sum_past_i64_is_exact_decimal() {
        let values = [Value::Integer(i64::MAX), Value::Integer(1)];
        let numeric = Numeric::collect(values.iter());
        assert_eq!(
            numeric.sum().unwrap(),
            Value::Decimal(Decimal::from_i128_with_scale(i128::from(i64::MAX) + 1, 0))
        );
    }

    #[test]
    fn integer_sum_past_decimal_range_is_overflow() {
        let values = [Value::Decimal(Decimal::MAX), Value::Decimal(Decimal::MAX)];
        let numeric = Numeric::collect(values.iter());
        assert!(matches!(
            numeric.sum(),
            Err(nodedb_query::EvalError::NumericOverflow { function: "sum" })
        ));
    }
}
