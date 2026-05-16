// SPDX-License-Identifier: Apache-2.0
//! Macro that expands to 10 PhysicalTaskVisitor method stubs returning `LiteError::Unsupported`.
//! Invoked once from `adapter.rs` inside the single `impl PhysicalTaskVisitor for LiteDataPlaneVisitor` block.

macro_rules! impl_unsupported_lite_physical_visitor_methods {
    () => {
        fn graph(
            &mut self,
            _op: &nodedb_physical::physical_plan::GraphOp,
        ) -> Result<LitePhysicalFut<'a>, LiteError> {
            u_phys!("Graph")
        }

        fn document(
            &mut self,
            _op: &nodedb_physical::physical_plan::DocumentOp,
        ) -> Result<LitePhysicalFut<'a>, LiteError> {
            u_phys!("Document")
        }

        fn kv(
            &mut self,
            _op: &nodedb_physical::physical_plan::KvOp,
        ) -> Result<LitePhysicalFut<'a>, LiteError> {
            u_phys!("Kv")
        }

        fn columnar(
            &mut self,
            _op: &nodedb_physical::physical_plan::ColumnarOp,
        ) -> Result<LitePhysicalFut<'a>, LiteError> {
            u_phys!("Columnar")
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

        fn crdt(
            &mut self,
            _op: &nodedb_physical::physical_plan::CrdtOp,
        ) -> Result<LitePhysicalFut<'a>, LiteError> {
            u_phys!("Crdt")
        }

        fn query(
            &mut self,
            _op: &nodedb_physical::physical_plan::QueryOp,
        ) -> Result<LitePhysicalFut<'a>, LiteError> {
            u_phys!("Query")
        }

        fn meta(
            &mut self,
            _op: &nodedb_physical::physical_plan::MetaOp,
        ) -> Result<LitePhysicalFut<'a>, LiteError> {
            u_phys!("Meta")
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
