// SPDX-License-Identifier: Apache-2.0

//! Shared test fixture for `graph_ops` inline `#[cfg(test)]` modules.

/// Build a real graph-scoped `ScopedMemory` for graph_ops tests.
#[cfg(test)]
pub(crate) fn test_memory() -> nodedb_mem::ScopedMemory {
    super::super::engine::test_scoped_memory(
        &super::super::engine::test_governor(),
        nodedb_mem::EngineId::Graph,
    )
}
