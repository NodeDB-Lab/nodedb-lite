// SPDX-License-Identifier: Apache-2.0

//! [`LiteConfig::validate`] — coherence checks for the percentage fields.

use nodedb_types::error::{NodeDbError, NodeDbResult};

use super::defaults::MAX_TOTAL_ENGINE_PERCENT;
use super::types::LiteConfig;

impl LiteConfig {
    /// Validate that percentage fields are coherent.
    ///
    /// Returns an error if:
    /// - Any individual percentage exceeds 100
    /// - The sum of all engine percentages exceeds `MAX_TOTAL_ENGINE_PERCENT`
    pub fn validate(&self) -> NodeDbResult<()> {
        for (name, pct) in [
            ("hnsw_percent", self.hnsw_percent),
            ("csr_percent", self.csr_percent),
            ("loro_percent", self.loro_percent),
            ("query_percent", self.query_percent),
        ] {
            if pct > 100 {
                return Err(NodeDbError::config(format!(
                    "{name} must be 0–100, got {pct}"
                )));
            }
        }

        let total = self
            .hnsw_percent
            .saturating_add(self.csr_percent)
            .saturating_add(self.loro_percent)
            .saturating_add(self.query_percent);

        if total > MAX_TOTAL_ENGINE_PERCENT {
            return Err(NodeDbError::config(format!(
                "sum of engine percentages is {total}%, must not exceed {MAX_TOTAL_ENGINE_PERCENT}% \
                 (at least 1% headroom required)"
            )));
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_config_validates() {
        assert!(LiteConfig::default().validate().is_ok());
    }

    #[test]
    fn validate_rejects_percent_over_100() {
        let cfg = LiteConfig {
            hnsw_percent: 101,
            ..LiteConfig::default()
        };
        assert!(cfg.validate().is_err());
    }

    #[test]
    fn validate_rejects_sum_over_max() {
        let cfg = LiteConfig {
            hnsw_percent: 40,
            csr_percent: 25,
            loro_percent: 25,
            query_percent: 15,
            ..LiteConfig::default()
        };
        // Sum = 105 > 99.
        assert!(cfg.validate().is_err());
    }
}
