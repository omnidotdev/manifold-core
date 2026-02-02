//! Manifold Core - Universal artifact registry library
//!
//! Supports multiple artifact types:
//! - OCI/Docker container images
//! - OpenAPI specifications
//! - GraphQL schemas
//! - JSON schemas
//! - WebAssembly modules
//! - Generic blobs

pub mod types;
pub mod storage;
pub mod registry;

pub use types::*;
pub use storage::Storage;
pub use registry::Registry;
