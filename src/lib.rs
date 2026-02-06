//! Manifold Core - Universal artifact registry library
//!
//! Supports multiple artifact types:
//! - OCI/Docker container images
//! - `OpenAPI` specifications
//! - `GraphQL` schemas
//! - JSON schemas
//! - `WebAssembly` modules
//! - Generic blobs

pub mod registry;
pub mod storage;
pub mod types;

pub use registry::Registry;
pub use storage::Storage;
pub use types::*;
