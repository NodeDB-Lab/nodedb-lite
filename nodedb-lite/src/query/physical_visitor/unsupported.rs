// SPDX-License-Identifier: Apache-2.0
//! Macro that expands to the remaining `PhysicalTaskVisitor` method stubs
//! returning `LiteError::Unsupported`. Invoked from `adapter/mod.rs` inside
//! the `impl PhysicalTaskVisitor for LiteDataPlaneVisitor` block.

macro_rules! impl_unsupported_lite_physical_visitor_methods {
    () => {
        fn graph(
            &mut self,
            _op: &nodedb_physical::physical_plan::GraphOp,
        ) -> Result<LitePhysicalFut<'a>, LiteError> {
            u_phys!("Graph")
        }

        fn timeseries(
            &mut self,
            _op: &nodedb_physical::physical_plan::TimeseriesOp,
        ) -> Result<LitePhysicalFut<'a>, LiteError> {
            u_phys!("Timeseries")
        }

        fn spatial(
            &mut self,
            _op: &nodedb_physical::physical_plan::SpatialOp,
        ) -> Result<LitePhysicalFut<'a>, LiteError> {
            u_phys!("Spatial")
        }

        fn query(
            &mut self,
            _op: &nodedb_physical::physical_plan::QueryOp,
        ) -> Result<LitePhysicalFut<'a>, LiteError> {
            u_phys!("Query")
        }

        fn cluster_array(
            &mut self,
            _op: &nodedb_physical::physical_plan::ClusterArrayOp,
        ) -> Result<LitePhysicalFut<'a>, LiteError> {
            u_phys!("ClusterArray")
        }
    };
}

pub(super) use impl_unsupported_lite_physical_visitor_methods;
