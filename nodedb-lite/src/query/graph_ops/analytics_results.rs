// SPDX-License-Identifier: Apache-2.0

//! Dense analytics result values and their untimed query-result adapter.

use nodedb_graph::params::GraphAlgorithm;
use nodedb_types::result::QueryResult;
use nodedb_types::value::Value;

use crate::engine::graph::index::CsrIndex;
use crate::error::LiteError;

/// Dense primitive algorithm output, intentionally separated from presentation.
#[derive(Debug, PartialEq)]
pub enum AnalyticsRawValues {
    PageRank(Vec<f64>),
    Wcc(Vec<u32>),
    Lcc(Vec<f64>),
    Sssp(Vec<f64>),
    LabelPropagation(Vec<u32>),
}

impl AnalyticsRawValues {
    fn algorithm(&self) -> GraphAlgorithm {
        match self {
            Self::PageRank(_) => GraphAlgorithm::PageRank,
            Self::Wcc(_) => GraphAlgorithm::Wcc,
            Self::Lcc(_) => GraphAlgorithm::Lcc,
            Self::Sssp(_) => GraphAlgorithm::Sssp,
            Self::LabelPropagation(_) => GraphAlgorithm::LabelPropagation,
        }
    }

    fn len(&self) -> usize {
        match self {
            Self::PageRank(values) | Self::Lcc(values) | Self::Sssp(values) => values.len(),
            Self::Wcc(values) | Self::LabelPropagation(values) => values.len(),
        }
    }
}

/// Materialize a dense primitive result as the public query shape.
pub(crate) fn raw_to_query(
    csr: &CsrIndex,
    algorithm: GraphAlgorithm,
    raw: AnalyticsRawValues,
) -> Result<QueryResult, LiteError> {
    if raw.algorithm() != algorithm {
        return Err(LiteError::Storage {
            detail: format!(
                "dense primitive result kind {:?} does not match {algorithm:?}",
                raw.algorithm()
            ),
        });
    }
    if raw.len() != csr.node_count() {
        return Err(LiteError::Storage {
            detail: format!(
                "dense primitive result has {} values for {} CSR nodes",
                raw.len(),
                csr.node_count()
            ),
        });
    }
    let columns = algorithm
        .result_schema()
        .iter()
        .map(|(name, _)| (*name).to_string())
        .collect();
    let rows = match raw {
        AnalyticsRawValues::PageRank(values)
        | AnalyticsRawValues::Lcc(values)
        | AnalyticsRawValues::Sssp(values) => named_floats(csr, values),
        AnalyticsRawValues::Wcc(values) => named_u32s(csr, values),
        AnalyticsRawValues::LabelPropagation(values) => values
            .into_iter()
            .enumerate()
            .map(|(node, label)| {
                vec![
                    Value::String(csr.node_name_raw(node as u32).to_string()),
                    Value::Integer(label as i64),
                ]
            })
            .collect(),
    };
    Ok(QueryResult {
        columns,
        rows,
        rows_affected: 0,
    })
}

fn named_floats(csr: &CsrIndex, values: Vec<f64>) -> Vec<Vec<Value>> {
    values
        .into_iter()
        .enumerate()
        .map(|(node, value)| {
            vec![
                Value::String(csr.node_name_raw(node as u32).to_string()),
                Value::Float(value),
            ]
        })
        .collect()
}

fn named_u32s(csr: &CsrIndex, values: Vec<u32>) -> Vec<Vec<Value>> {
    values
        .into_iter()
        .enumerate()
        .map(|(node, value)| {
            vec![
                Value::String(csr.node_name_raw(node as u32).to_string()),
                Value::Integer(value as i64),
            ]
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn two_nodes() -> CsrIndex {
        let mut csr = CsrIndex::new();
        csr.add_edge("a", "E", "b").unwrap();
        csr
    }

    #[test]
    fn materialization_rejects_mismatched_kind_and_dense_length() {
        let csr = two_nodes();
        assert!(
            raw_to_query(
                &csr,
                GraphAlgorithm::Wcc,
                AnalyticsRawValues::PageRank(vec![0.5, 0.5]),
            )
            .is_err()
        );
        assert!(
            raw_to_query(&csr, GraphAlgorithm::Wcc, AnalyticsRawValues::Wcc(vec![0]),).is_err()
        );
    }
}
