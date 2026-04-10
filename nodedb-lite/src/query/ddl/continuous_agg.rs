//! DDL handler for CREATE/DROP/SHOW CONTINUOUS AGGREGATE in Lite.
//!
//! SQL syntax (same as Origin for wire compatibility):
//!
//! ```text
//! CREATE CONTINUOUS AGGREGATE <name> ON <source>
//!   BUCKET '<interval>'
//!   AGGREGATE <func>(col) [AS alias], ...
//!   [GROUP BY col, ...]
//!   [WITH (refresh_policy = '...', retention = '...')]
//!
//! DROP CONTINUOUS AGGREGATE <name>
//!
//! SHOW CONTINUOUS AGGREGATES [FOR <source>]
//! ```

use nodedb_types::result::QueryResult;
use nodedb_types::timeseries::{AggFunction, AggregateExpr, ContinuousAggregateDef, RefreshPolicy};
use nodedb_types::value::Value;

use crate::error::LiteError;
use crate::query::engine::LiteQueryEngine;
use crate::storage::engine::StorageEngine;

impl<S: StorageEngine> LiteQueryEngine<S> {
    /// Handle CREATE CONTINUOUS AGGREGATE.
    pub(in crate::query) async fn handle_create_continuous_aggregate(
        &self,
        sql: &str,
    ) -> Result<QueryResult, LiteError> {
        let def = parse_create_sql(sql)?;

        let mut ts = match self.timeseries.lock() {
            Ok(t) => t,
            Err(p) => p.into_inner(),
        };
        ts.continuous_agg_mgr.register(def.clone());

        Ok(QueryResult {
            columns: vec!["result".into()],
            rows: vec![vec![Value::String(format!(
                "continuous aggregate '{}' created on '{}'",
                def.name, def.source
            ))]],
            rows_affected: 0,
        })
    }

    /// Handle DROP CONTINUOUS AGGREGATE.
    pub(in crate::query) async fn handle_drop_continuous_aggregate(
        &self,
        sql: &str,
    ) -> Result<QueryResult, LiteError> {
        let upper = sql.trim().to_uppercase();
        let name = upper
            .strip_prefix("DROP CONTINUOUS AGGREGATE ")
            .ok_or_else(|| LiteError::Query("expected DROP CONTINUOUS AGGREGATE <name>".into()))?
            .trim()
            .to_lowercase();

        let mut ts = match self.timeseries.lock() {
            Ok(t) => t,
            Err(p) => p.into_inner(),
        };
        ts.continuous_agg_mgr.unregister(&name);

        Ok(QueryResult {
            columns: vec!["result".into()],
            rows: vec![vec![Value::String(format!(
                "continuous aggregate '{name}' dropped"
            ))]],
            rows_affected: 0,
        })
    }

    /// Handle SHOW CONTINUOUS AGGREGATES [FOR <source>].
    pub(in crate::query) async fn handle_show_continuous_aggregates(
        &self,
        sql: &str,
    ) -> Result<QueryResult, LiteError> {
        let upper = sql.trim().to_uppercase();
        let source_filter = upper
            .strip_prefix("SHOW CONTINUOUS AGGREGATES FOR ")
            .map(|s| s.trim().to_lowercase());

        let ts = match self.timeseries.lock() {
            Ok(t) => t,
            Err(p) => p.into_inner(),
        };

        let names = ts.continuous_agg_mgr.list();
        let columns = vec![
            "name".into(),
            "source".into(),
            "bucket_interval".into(),
            "aggregates".into(),
            "group_by".into(),
            "refresh_policy".into(),
            "retention_ms".into(),
            "watermark".into(),
        ];

        let mut rows: Vec<Vec<Value>> = Vec::new();
        for name in names {
            let Some(def) = ts.continuous_agg_mgr.get(name) else {
                continue;
            };
            if source_filter.as_deref().is_some_and(|f| def.source != f) {
                continue;
            }
            let agg_str = def
                .aggregates
                .iter()
                .map(|a| format!("{}({})", a.function.as_str(), a.source_column))
                .collect::<Vec<_>>()
                .join(", ");
            let gb_str = def.group_by.join(", ");

            rows.push(vec![
                Value::String(def.name.clone()),
                Value::String(def.source.clone()),
                Value::String(def.bucket_interval.clone()),
                Value::String(agg_str),
                Value::String(gb_str),
                Value::String(format!("{:?}", def.refresh_policy)),
                Value::Integer(def.retention_period_ms as i64),
                Value::Integer(ts.continuous_agg_mgr.watermark(name)),
            ]);
        }

        Ok(QueryResult {
            columns,
            rows,
            rows_affected: 0,
        })
    }
}

// ---------------------------------------------------------------------------
// SQL parser
// ---------------------------------------------------------------------------

fn parse_create_sql(sql: &str) -> Result<ContinuousAggregateDef, LiteError> {
    let upper = sql.to_uppercase();

    // Extract name: word after "CONTINUOUS AGGREGATE"
    let ca_kw = "CONTINUOUS AGGREGATE ";
    let ca_pos = upper
        .find(ca_kw)
        .ok_or_else(|| LiteError::Query("expected CONTINUOUS AGGREGATE keyword".into()))?;
    let after_ca = sql[ca_pos + ca_kw.len()..].trim_start();
    let name = after_ca
        .split_whitespace()
        .next()
        .ok_or_else(|| LiteError::Query("missing aggregate name".into()))?
        .to_lowercase();

    // Extract source: word after " ON "
    let on_kw = " ON ";
    let on_pos = upper[ca_pos..]
        .find(on_kw)
        .ok_or_else(|| LiteError::Query("expected ON <source> clause".into()))?;
    let after_on = sql[ca_pos + on_pos + on_kw.len()..].trim_start();
    let source = after_on
        .split_whitespace()
        .next()
        .ok_or_else(|| LiteError::Query("missing source collection name".into()))?
        .to_lowercase();

    // Extract bucket interval: BUCKET '<interval>'
    let bucket_interval = extract_quoted_value(&upper, sql, "BUCKET ")
        .ok_or_else(|| LiteError::Query("expected BUCKET '<interval>' clause".into()))?;
    let bucket_interval_ms = nodedb_types::kv_parsing::parse_interval_to_ms(&bucket_interval)
        .map_err(|e| LiteError::Query(format!("invalid bucket interval: {e}")))?
        as i64;

    // Extract aggregates: between AGGREGATE and GROUP BY / WITH / end
    let aggregates = extract_aggregates(&upper, sql)?;
    if aggregates.is_empty() {
        return Err(LiteError::Query(
            "expected AGGREGATE <func>(col), ... clause".into(),
        ));
    }

    // Extract GROUP BY columns (optional).
    let group_by = extract_group_by(&upper, sql);

    // Extract WITH options (optional).
    let (refresh_policy, retention_period_ms) = extract_with_options(&upper, sql);

    Ok(ContinuousAggregateDef {
        name,
        source,
        bucket_interval,
        bucket_interval_ms,
        group_by,
        aggregates,
        refresh_policy,
        retention_period_ms,
        stale: false,
    })
}

fn extract_quoted_value(upper: &str, sql: &str, keyword: &str) -> Option<String> {
    let pos = upper.find(keyword)?;
    let after = sql[pos + keyword.len()..].trim_start();
    let start = after.find('\'')?;
    let end = after[start + 1..].find('\'')?;
    Some(after[start + 1..start + 1 + end].to_string())
}

fn extract_aggregates(upper: &str, sql: &str) -> Result<Vec<AggregateExpr>, LiteError> {
    let bucket_pos = upper.find("BUCKET ").unwrap_or(0);
    let agg_kw = "AGGREGATE ";
    let agg_pos = match upper[bucket_pos..].find(agg_kw) {
        Some(p) => bucket_pos + p,
        None => return Ok(Vec::new()),
    };
    let after_agg = &sql[agg_pos + agg_kw.len()..];

    // Find end: GROUP BY, WITH, or end of string.
    let end_pos = ["GROUP BY", "WITH (", "WITH("]
        .iter()
        .filter_map(|kw| upper[agg_pos + agg_kw.len()..].find(kw))
        .min()
        .unwrap_or(after_agg.len());

    let agg_str = after_agg[..end_pos].trim().trim_end_matches(',');
    let mut exprs = Vec::new();

    for part in agg_str.split(',') {
        let part = part.trim();
        if part.is_empty() {
            continue;
        }
        let expr = parse_single_aggregate(part)?;
        exprs.push(expr);
    }

    Ok(exprs)
}

fn parse_single_aggregate(s: &str) -> Result<AggregateExpr, LiteError> {
    let upper = s.to_uppercase();

    let (func_part, alias) = if let Some(as_pos) = upper.find(" AS ") {
        (&s[..as_pos], Some(s[as_pos + 4..].trim().to_lowercase()))
    } else {
        (s, None)
    };
    let func_part = func_part.trim();

    let open = func_part
        .find('(')
        .ok_or_else(|| LiteError::Query(format!("expected function(column) syntax: {s}")))?;
    let close = func_part
        .rfind(')')
        .ok_or_else(|| LiteError::Query(format!("missing closing parenthesis: {s}")))?;

    let func_name = func_part[..open].trim().to_lowercase();
    let col_name = func_part[open + 1..close].trim().to_lowercase();

    let function = match func_name.as_str() {
        "sum" => AggFunction::Sum,
        "count" => AggFunction::Count,
        "min" => AggFunction::Min,
        "max" => AggFunction::Max,
        "avg" => AggFunction::Avg,
        "first" => AggFunction::First,
        "last" => AggFunction::Last,
        "count_distinct" => AggFunction::CountDistinct,
        other => {
            return Err(LiteError::Query(format!(
                "unknown aggregate function: {other}"
            )));
        }
    };

    let output_column = alias.unwrap_or_else(|| {
        if col_name == "*" {
            func_name.clone()
        } else {
            format!("{func_name}_{col_name}")
        }
    });

    Ok(AggregateExpr {
        function,
        source_column: col_name,
        output_column,
    })
}

fn extract_group_by(upper: &str, sql: &str) -> Vec<String> {
    let gb_kw = "GROUP BY ";
    let gb_pos = match upper.find(gb_kw) {
        Some(p) => p,
        None => return Vec::new(),
    };
    let after_gb = &sql[gb_pos + gb_kw.len()..];

    let end_pos = ["WITH (", "WITH("]
        .iter()
        .filter_map(|kw| upper[gb_pos + gb_kw.len()..].find(kw))
        .min()
        .unwrap_or(after_gb.len());

    after_gb[..end_pos]
        .split(',')
        .map(|s| s.trim().to_lowercase())
        .filter(|s| !s.is_empty())
        .collect()
}

fn extract_with_options(upper: &str, sql: &str) -> (RefreshPolicy, u64) {
    let with_pos = match upper.find("WITH (").or_else(|| upper.find("WITH(")) {
        Some(p) => p,
        None => return (RefreshPolicy::OnFlush, 0),
    };
    let after_with = &sql[with_pos..];
    let close = after_with.find(')').unwrap_or(after_with.len());
    let opts = &after_with[..close];

    let refresh_policy = if opts.to_uppercase().contains("ON_FLUSH") {
        RefreshPolicy::OnFlush
    } else if opts.to_uppercase().contains("ON_SEAL") {
        RefreshPolicy::OnSeal
    } else if opts.to_uppercase().contains("MANUAL") {
        RefreshPolicy::Manual
    } else {
        RefreshPolicy::OnFlush
    };

    let retention = extract_quoted_value(&opts.to_uppercase(), opts, "RETENTION = ")
        .or_else(|| extract_quoted_value(&opts.to_uppercase(), opts, "RETENTION="))
        .and_then(|s| nodedb_types::kv_parsing::parse_interval_to_ms(&s).ok())
        .unwrap_or(0);

    (refresh_policy, retention)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_basic_create() {
        let sql = "CREATE CONTINUOUS AGGREGATE metrics_1m ON metrics BUCKET '1m' AGGREGATE sum(value) AS value_sum, count(*) AS row_count";
        let def = parse_create_sql(sql).unwrap();
        assert_eq!(def.name, "metrics_1m");
        assert_eq!(def.source, "metrics");
        assert_eq!(def.bucket_interval_ms, 60_000);
        assert_eq!(def.aggregates.len(), 2);
        assert_eq!(def.aggregates[0].function, AggFunction::Sum);
        assert_eq!(def.aggregates[0].source_column, "value");
        assert_eq!(def.aggregates[0].output_column, "value_sum");
        assert_eq!(def.aggregates[1].function, AggFunction::Count);
        assert!(def.group_by.is_empty());
    }

    #[test]
    fn parse_with_group_by() {
        let sql = "CREATE CONTINUOUS AGGREGATE cpu_5m ON cpu_metrics BUCKET '5m' AGGREGATE avg(cpu) AS cpu_avg, max(cpu) AS cpu_max GROUP BY host, region";
        let def = parse_create_sql(sql).unwrap();
        assert_eq!(def.name, "cpu_5m");
        assert_eq!(def.group_by, vec!["host", "region"]);
        assert_eq!(def.aggregates.len(), 2);
    }

    #[test]
    fn parse_with_options() {
        let sql = "CREATE CONTINUOUS AGGREGATE m_1h ON m BUCKET '1h' AGGREGATE sum(v) WITH (refresh_policy = 'on_flush', retention = '7d')";
        let def = parse_create_sql(sql).unwrap();
        assert_eq!(def.refresh_policy, RefreshPolicy::OnFlush);
        assert_eq!(def.retention_period_ms, 7 * 86_400_000);
    }

    #[test]
    fn parse_auto_alias() {
        let sql = "CREATE CONTINUOUS AGGREGATE m_1m ON m BUCKET '1m' AGGREGATE avg(cpu)";
        let def = parse_create_sql(sql).unwrap();
        assert_eq!(def.aggregates[0].output_column, "avg_cpu");
    }

    #[test]
    fn parse_missing_bucket_fails() {
        let sql = "CREATE CONTINUOUS AGGREGATE m ON metrics AGGREGATE sum(v)";
        assert!(parse_create_sql(sql).is_err());
    }

    #[test]
    fn parse_missing_aggregate_fails() {
        let sql = "CREATE CONTINUOUS AGGREGATE m ON metrics BUCKET '1m'";
        assert!(parse_create_sql(sql).is_err());
    }
}
