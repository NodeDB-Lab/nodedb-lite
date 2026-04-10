pub mod client;
pub mod clock;
pub mod compensation;
pub mod flow_control;
pub mod shapes;
pub mod transport;

pub use client::{SyncClient, SyncConfig, SyncState};
pub use clock::VectorClock;
pub use compensation::{CompensationEvent, CompensationHandler, CompensationRegistry};
pub use flow_control::{FlowControlConfig, FlowController, SyncMetrics, SyncMetricsSnapshot};
pub use shapes::ShapeManager;
pub use transport::{SyncDelegate, run_sync_loop};
