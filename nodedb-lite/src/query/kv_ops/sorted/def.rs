// SPDX-License-Identifier: Apache-2.0
//! Building a sorted-index definition from a `RegisterSortedIndex` plan, and
//! the entry a stored row contributes to it.

use nodedb_types::value::Value;

use crate::error::LiteError;
use crate::index::sorted_def::{SortedIndexDef, WindowDef, WindowKind};
use crate::query::kv_ops::body::decode_kv_map;
use crate::query::kv_ops::reads::decode_value;

use super::keys::{PkEntry, encode_score};
use super::window::{encode_ts, extract_ts_from_value};

/// The fields of a `RegisterSortedIndex` plan.
#[derive(Debug, Clone, Copy)]
pub struct RegisterSortedIndex<'a> {
    pub collection: &'a str,
    pub index_name: &'a str,
    /// `(column, "ASC" | "DESC")` pairs. The first is the score column.
    pub sort_columns: &'a [(String, String)],
    pub key_column: &'a str,
    /// `none`, `daily`, `weekly`, `monthly` or `custom`, any case. Empty
    /// means `none`.
    pub window_type: &'a str,
    pub window_timestamp_column: &'a str,
    pub window_start_ms: u64,
    pub window_end_ms: u64,
}

fn is_desc(direction: &str) -> bool {
    direction.eq_ignore_ascii_case("DESC")
}

fn bad_request(detail: String) -> LiteError {
    LiteError::BadRequest { detail }
}

fn window_kind(window_type: &str) -> Result<WindowKind, LiteError> {
    match window_type.to_ascii_lowercase().as_str() {
        "" | "none" => Ok(WindowKind::None),
        "daily" => Ok(WindowKind::Daily),
        "weekly" => Ok(WindowKind::Weekly),
        "monthly" => Ok(WindowKind::Monthly),
        "custom" => Ok(WindowKind::Custom),
        other => Err(bad_request(format!(
            "RegisterSortedIndex: unknown window type '{other}'; \
             expected none, daily, weekly, monthly or custom"
        ))),
    }
}

impl SortedIndexDef {
    /// Validate `req` and build its definition.
    ///
    /// Lite ranks by one score column, plus the window timestamp column of a
    /// windowed index. Any other sort column is `Unsupported`.
    pub(crate) fn build(req: &RegisterSortedIndex<'_>) -> Result<Self, LiteError> {
        if req.index_name.is_empty() {
            return Err(bad_request(
                "RegisterSortedIndex: index name is empty".into(),
            ));
        }
        if req.collection.is_empty() {
            return Err(bad_request(format!(
                "RegisterSortedIndex '{}': collection is empty",
                req.index_name
            )));
        }
        let Some((score_column, score_direction)) = req.sort_columns.first() else {
            return Err(bad_request(format!(
                "RegisterSortedIndex '{}': no sort column; the first sort column is the score",
                req.index_name
            )));
        };

        let kind = window_kind(req.window_type)?;
        let window = if kind == WindowKind::None {
            WindowDef::none()
        } else {
            let column = req.window_timestamp_column;
            let Some((_, direction)) = req.sort_columns.iter().find(|(name, _)| name == column)
            else {
                return Err(bad_request(format!(
                    "RegisterSortedIndex '{}': WINDOW timestamp column '{column}' \
                     must be included in sort columns",
                    req.index_name
                )));
            };
            WindowDef {
                kind,
                timestamp_column: column.to_string(),
                timestamp_desc: is_desc(direction),
                start_ms: req.window_start_ms,
                end_ms: req.window_end_ms,
            }
        };

        if let Some((extra, _)) = req.sort_columns[1..]
            .iter()
            .find(|(name, _)| !window.is_windowed() || *name != window.timestamp_column)
        {
            return Err(LiteError::Unsupported {
                detail: format!(
                    "RegisterSortedIndex '{}': sort column '{extra}' is not supported; \
                     Lite ranks by one score column plus an optional window timestamp column",
                    req.index_name
                ),
            });
        }

        Ok(Self {
            name: req.index_name.to_string(),
            collection: req.collection.to_string(),
            score_column: score_column.clone(),
            score_desc: is_desc(score_direction),
            key_column: req.key_column.to_string(),
            window,
        })
    }
}

/// The entry the stored KV value `stored` contributes to `def` at `now_ms`.
///
/// No entry for a row expired at `now_ms`, a raw body, a score column that
/// is absent or not numeric, or, on a windowed index, a timestamp column
/// that is absent, not numeric, or negative.
pub(super) fn entry_for(
    def: &SortedIndexDef,
    stored: &[u8],
    now_ms: u64,
) -> Result<Option<PkEntry>, LiteError> {
    let Some((deadline, body)) = decode_value(stored) else {
        return Ok(None);
    };
    if deadline != 0 && now_ms >= deadline {
        return Ok(None);
    }
    let Some(columns) = decode_kv_map(body)? else {
        return Ok(None);
    };
    let score = match columns.get(&def.score_column) {
        Some(Value::Integer(n)) => *n as f64,
        Some(Value::Float(f)) => *f,
        _ => return Ok(None),
    };
    let ts = if def.window.is_windowed() {
        match extract_ts_from_value(&columns, &def.window.timestamp_column) {
            Some(ts_ms) => Some(encode_ts(ts_ms, def.window.timestamp_desc)),
            None => return Ok(None),
        }
    } else {
        None
    };
    Ok(Some(PkEntry {
        score: encode_score(score, def.score_desc),
        ts,
        deadline,
    }))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn columns(list: &[(&str, &str)]) -> Vec<(String, String)> {
        list.iter()
            .map(|(c, d)| ((*c).to_string(), (*d).to_string()))
            .collect()
    }

    fn request<'a>(sort: &'a [(String, String)], window_type: &'a str) -> RegisterSortedIndex<'a> {
        RegisterSortedIndex {
            collection: "players",
            index_name: "lb",
            sort_columns: sort,
            key_column: "id",
            window_type,
            window_timestamp_column: "ts",
            window_start_ms: 0,
            window_end_ms: 0,
        }
    }

    #[test]
    fn the_first_sort_column_is_the_score() {
        let sort = columns(&[("score", "DESC")]);
        let def = SortedIndexDef::build(&request(&sort, "none")).expect("build");
        assert_eq!(def.score_column, "score");
        assert!(def.score_desc);
        assert!(!def.window.is_windowed());
    }

    #[test]
    fn a_window_takes_its_direction_from_the_sort_columns() {
        let sort = columns(&[("score", "ASC"), ("ts", "DESC")]);
        let def = SortedIndexDef::build(&request(&sort, "Weekly")).expect("build");
        assert_eq!(def.window.kind, WindowKind::Weekly);
        assert!(def.window.timestamp_desc);
    }

    #[test]
    fn an_empty_sort_column_list_is_refused() {
        let err = SortedIndexDef::build(&request(&[], "none")).expect_err("refused");
        assert!(matches!(err, LiteError::BadRequest { .. }), "{err:?}");
    }
}
