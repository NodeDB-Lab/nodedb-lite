// SPDX-License-Identifier: Apache-2.0

//! Conversion from [`LiteConfig`] percentages to a shared [`nodedb_mem::GovernorConfig`].

use nodedb_mem::{EngineId, EngineLimits, GovernorConfig};

use super::types::LiteConfig;

impl LiteConfig {
    /// Build a [`GovernorConfig`] from this config's budget and percentages.
    ///
    /// Every `_percent` field maps to one [`EngineId`] Lite runs. `Wal` and
    /// `Bridge` get a zero limit: Lite has no WAL crate and no cross-plane
    /// bridge, so neither engine ever allocates here.
    pub fn to_governor_config(&self) -> GovernorConfig {
        let budget = self.memory_budget;
        let pct = |percent: usize| budget * percent / 100;

        let engine_limits = EngineLimits::zeroed()
            .with(EngineId::Vector, pct(self.hnsw_percent))
            .with(EngineId::Graph, pct(self.csr_percent))
            .with(EngineId::Crdt, pct(self.loro_percent))
            .with(EngineId::Query, pct(self.query_percent))
            .with(EngineId::Kv, pct(self.kv_percent))
            .with(EngineId::DocumentSchemaless, pct(self.document_percent))
            .with(EngineId::DocumentStrict, pct(self.strict_percent))
            .with(EngineId::Columnar, pct(self.columnar_percent))
            .with(EngineId::Timeseries, pct(self.timeseries_percent))
            .with(EngineId::Spatial, pct(self.spatial_percent))
            .with(EngineId::Fts, pct(self.fts_percent))
            .with(EngineId::Array, pct(self.array_percent))
            .with(EngineId::Sparse, pct(self.sparse_percent));

        GovernorConfig {
            global_ceiling: budget,
            engine_limits,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_config_stays_under_the_global_ceiling() {
        let cfg = LiteConfig::default();
        let governor_config = cfg.to_governor_config();
        assert!(governor_config.engine_limits.total() <= governor_config.global_ceiling);
    }

    #[test]
    fn percentages_map_onto_the_matching_engine() {
        let cfg = LiteConfig {
            memory_budget: 1_000_000,
            ..LiteConfig::default()
        };
        let governor_config = cfg.to_governor_config();
        assert_eq!(
            governor_config.engine_limits.get(EngineId::Vector),
            1_000_000 * cfg.hnsw_percent / 100
        );
        assert_eq!(
            governor_config.engine_limits.get(EngineId::Kv),
            1_000_000 * cfg.kv_percent / 100
        );
        assert_eq!(governor_config.engine_limits.get(EngineId::Wal), 0);
        assert_eq!(governor_config.engine_limits.get(EngineId::Bridge), 0);
    }
}
