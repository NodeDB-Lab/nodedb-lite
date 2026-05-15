//! Replica-id and HLC helpers for array-sync tests.

use nodedb_array::sync::hlc::Hlc;
use nodedb_array::sync::replica_id::ReplicaId;

pub fn replica(id: u64) -> ReplicaId {
    ReplicaId::new(id)
}

pub fn hlc(ms: u64, rep: ReplicaId) -> Hlc {
    Hlc::new(ms, 0, rep).expect("valid HLC")
}

pub fn hlc1(ms: u64) -> Hlc {
    hlc(ms, replica(1))
}

pub fn hlc2(ms: u64) -> Hlc {
    hlc(ms, replica(2))
}
