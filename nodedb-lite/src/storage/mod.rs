#[cfg(not(target_arch = "wasm32"))]
pub mod array_segment_ext;
pub mod checksum;
#[cfg(not(target_arch = "wasm32"))]
pub mod columnar_segment_ext;
pub mod encrypted;
pub mod engine;
#[cfg(not(target_arch = "wasm32"))]
pub mod fts_segment_ext;
#[cfg(not(target_arch = "wasm32"))]
pub mod graph_segment_ext;
pub mod pagedb_storage;
#[cfg(not(target_arch = "wasm32"))]
mod pagedb_storage_columnar;
#[cfg(not(target_arch = "wasm32"))]
mod pagedb_storage_fts;
#[cfg(not(target_arch = "wasm32"))]
mod pagedb_storage_graph;
#[cfg(not(target_arch = "wasm32"))]
mod pagedb_storage_spatial;
#[cfg(not(target_arch = "wasm32"))]
pub mod spatial_segment_ext;
#[cfg(not(target_arch = "wasm32"))]
pub mod vector_segment_ext;
