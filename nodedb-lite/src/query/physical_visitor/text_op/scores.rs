// SPDX-License-Identifier: Apache-2.0

//! Per-row `bm25_score(field, query)` columns of a text read.
//!
//! A row the score's index holds scores the BM25 value of its match, or
//! `0.0` when the query does not match it. A row the index does not hold
//! scores `null`.

use std::collections::{HashMap, HashSet};

use nodedb_physical::physical_plan::TextScoreSpec;
use nodedb_types::text_search::TextSearchParams;
use nodedb_types::value::Value;

use crate::engine::fts::state::FtsState;
use crate::error::LiteError;
use crate::nodedb::lock_ext::LockExt;

/// One score column: its output alias and the score of every held document.
struct ScoreColumn {
    alias: String,
    scores: HashMap<String, f32>,
}

/// The score columns of one read, in spec order.
pub(super) struct ScoreColumns {
    columns: Vec<ScoreColumn>,
}

impl ScoreColumns {
    /// Score every document each spec's index holds. `admitted` restricts
    /// the documents to the rows the read admits.
    pub(super) fn compute(
        fts_state: &FtsState,
        collection: &str,
        specs: &[TextScoreSpec],
        admitted: Option<&HashSet<String>>,
    ) -> Result<Self, LiteError> {
        if specs.is_empty() {
            return Ok(Self {
                columns: Vec::new(),
            });
        }
        let mgr = fts_state.manager.lock_or_recover();
        let mut columns = Vec::with_capacity(specs.len());
        for spec in specs {
            let params = TextSearchParams {
                mode: spec.mode,
                fuzzy: spec.fuzzy,
            };
            let scores = mgr
                .scan_all_with_scores(
                    collection,
                    spec.field.as_deref().unwrap_or(""),
                    &spec.query,
                    &params,
                    admitted,
                )?
                .into_iter()
                .collect();
            columns.push(ScoreColumn {
                alias: spec.alias.clone(),
                scores,
            });
        }
        Ok(Self { columns })
    }

    /// The output column names, in spec order.
    pub(super) fn aliases(&self) -> impl Iterator<Item = String> + '_ {
        self.columns.iter().map(|c| c.alias.clone())
    }

    /// The position of the column named `alias`.
    pub(super) fn position(&self, alias: &str) -> Option<usize> {
        self.columns.iter().position(|c| c.alias == alias)
    }

    /// The score of `id` in column `column`. `None` when the column's index
    /// does not hold the document.
    pub(super) fn score(&self, column: usize, id: &str) -> Option<f32> {
        self.columns
            .get(column)
            .and_then(|c| c.scores.get(id).copied())
    }

    /// Every column's value for `id`, in spec order.
    pub(super) fn values(&self, id: &str) -> Vec<Value> {
        self.columns
            .iter()
            .map(|c| {
                c.scores
                    .get(id)
                    .map_or(Value::Null, |s| Value::Float(f64::from(*s)))
            })
            .collect()
    }

    /// Every document some column's index holds.
    pub(super) fn held_ids(&self) -> impl Iterator<Item = &String> + '_ {
        self.columns.iter().flat_map(|c| c.scores.keys())
    }
}
