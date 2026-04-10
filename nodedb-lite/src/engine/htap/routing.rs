//! HTAP query routing: decides whether to read from strict (source) or
//! columnar (materialized view) based on session settings.
//!
//! Default: `read_source = 'source'` — all queries go to strict document.
//! Opt-in: `read_source = 'auto'` — planner routes analytical queries
//! (GROUP BY, aggregations, full scans) to columnar, point lookups to strict.

/// Where to read data from when a materialized view exists.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum ReadSource {
    /// Always read from the source (strict document). Default.
    /// No risk of stale reads.
    #[default]
    Source,
    /// Planner decides: point lookups → source, analytical scans → materialized.
    /// May return data that lags behind the source by the CDC interval.
    Auto,
}

impl ReadSource {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Source => "source",
            Self::Auto => "auto",
        }
    }
}

impl std::str::FromStr for ReadSource {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            "source" => Ok(Self::Source),
            "auto" => Ok(Self::Auto),
            other => Err(format!(
                "unknown read_source: '{other}' (use 'source' or 'auto')"
            )),
        }
    }
}

/// Consistency level for reading materialized views.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum MaterializedConsistency {
    /// Accept bounded lag — reads whatever is materialized. Default for `auto` mode.
    #[default]
    Eventual,
    /// Force a CDC flush before reading. Higher latency, zero lag.
    Strong,
}

impl std::str::FromStr for MaterializedConsistency {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            "eventual" => Ok(Self::Eventual),
            "strong" => Ok(Self::Strong),
            other => Err(format!(
                "unknown materialized_consistency: '{other}' (use 'eventual' or 'strong')"
            )),
        }
    }
}

/// Per-session HTAP routing configuration.
///
/// Set via `SET read_source = 'auto'` and `SET materialized_consistency = 'strong'`.
#[derive(Debug, Clone, Default)]
pub struct HtapSession {
    pub read_source: ReadSource,
    pub materialized_consistency: MaterializedConsistency,
}

impl HtapSession {
    /// Whether this session should use the materialized view for a given query.
    ///
    /// Returns `true` if `read_source = 'auto'` AND the query looks analytical
    /// (indicated by `is_analytical` — determined by the caller from query shape).
    pub fn should_use_materialized(&self, is_analytical: bool) -> bool {
        self.read_source == ReadSource::Auto && is_analytical
    }

    /// Whether a CDC flush should be forced before reading the materialized view.
    pub fn requires_strong_consistency(&self) -> bool {
        self.materialized_consistency == MaterializedConsistency::Strong
    }
}

/// Simple heuristic to detect analytical queries from SQL.
///
/// Returns `true` if the query contains GROUP BY, aggregate functions,
/// or is a full scan without a PK filter.
pub fn is_analytical_query(sql: &str) -> bool {
    let upper = sql.to_uppercase();
    upper.contains("GROUP BY")
        || upper.contains("SUM(")
        || upper.contains("AVG(")
        || upper.contains("COUNT(")
        || upper.contains("MIN(")
        || upper.contains("MAX(")
        || (upper.contains("SELECT") && !upper.contains("WHERE") && upper.contains("FROM"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_session() {
        let session = HtapSession::default();
        assert_eq!(session.read_source, ReadSource::Source);
        assert!(!session.should_use_materialized(true));
        assert!(!session.should_use_materialized(false));
    }

    #[test]
    fn auto_routing() {
        let session = HtapSession {
            read_source: ReadSource::Auto,
            materialized_consistency: MaterializedConsistency::Eventual,
        };
        assert!(session.should_use_materialized(true));
        assert!(!session.should_use_materialized(false));
    }

    #[test]
    fn strong_consistency() {
        let session = HtapSession {
            read_source: ReadSource::Auto,
            materialized_consistency: MaterializedConsistency::Strong,
        };
        assert!(session.requires_strong_consistency());
    }

    #[test]
    fn analytical_detection() {
        assert!(is_analytical_query(
            "SELECT SUM(balance) FROM customers GROUP BY status"
        ));
        assert!(is_analytical_query("SELECT COUNT(*) FROM orders"));
        assert!(is_analytical_query("SELECT AVG(score) FROM metrics"));
        assert!(!is_analytical_query(
            "SELECT * FROM customers WHERE id = 42"
        ));
        assert!(!is_analytical_query(
            "INSERT INTO customers VALUES (1, 'a')"
        ));
    }

    #[test]
    fn read_source_parse() {
        assert_eq!("source".parse::<ReadSource>().unwrap(), ReadSource::Source);
        assert_eq!("auto".parse::<ReadSource>().unwrap(), ReadSource::Auto);
        assert!("bogus".parse::<ReadSource>().is_err());
    }
}
