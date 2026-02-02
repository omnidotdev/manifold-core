//! Core types for Manifold artifact registry

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

/// Artifact type classification
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, sqlx::Type)]
#[sqlx(type_name = "artifact_type", rename_all = "lowercase")]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum ArtifactType {
    /// OCI container image
    Container,
    /// OpenAPI specification (JSON/YAML)
    OpenApi,
    /// GraphQL schema (SDL)
    GraphQl,
    /// JSON Schema
    JsonSchema,
    /// WebAssembly module
    Wasm,
    /// Generic blob
    Blob,
}

impl ArtifactType {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Container => "container",
            Self::OpenApi => "openapi",
            Self::GraphQl => "graphql",
            Self::JsonSchema => "json-schema",
            Self::Wasm => "wasm",
            Self::Blob => "blob",
        }
    }

    pub fn media_type(&self) -> &'static str {
        match self {
            Self::Container => "application/vnd.oci.image.manifest.v1+json",
            Self::OpenApi => "application/vnd.oai.openapi+json",
            Self::GraphQl => "application/graphql",
            Self::JsonSchema => "application/schema+json",
            Self::Wasm => "application/wasm",
            Self::Blob => "application/octet-stream",
        }
    }
}

/// A namespace/organization that owns artifacts
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Namespace {
    pub id: Uuid,
    pub name: String,
    pub description: Option<String>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

/// A repository within a namespace (e.g., "myorg/myapp")
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Repository {
    pub id: Uuid,
    pub namespace_id: Uuid,
    pub name: String,
    pub description: Option<String>,
    pub artifact_type: ArtifactType,
    pub is_public: bool,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

/// An artifact stored in the registry
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Artifact {
    pub id: Uuid,
    pub repository_id: Uuid,
    /// Content-addressable digest (sha256:...)
    pub digest: String,
    /// Size in bytes
    pub size: i64,
    /// Media type (e.g., application/vnd.oci.image.manifest.v1+json)
    pub media_type: String,
    /// Raw content (for small artifacts) or path to blob storage
    pub content: Option<Vec<u8>>,
    pub created_at: DateTime<Utc>,
}

/// A tag pointing to an artifact (e.g., "latest", "v1.0.0")
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Tag {
    pub id: Uuid,
    pub repository_id: Uuid,
    pub artifact_id: Uuid,
    pub name: String,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

/// Schema-specific metadata
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SchemaMetadata {
    pub artifact_id: Uuid,
    /// Schema version (semver)
    pub version: Option<String>,
    /// Parsed schema title
    pub title: Option<String>,
    /// Whether schema is valid
    pub is_valid: bool,
    /// Validation errors if any
    pub validation_errors: Option<Vec<String>>,
}

/// Container-specific metadata (OCI)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ContainerMetadata {
    pub artifact_id: Uuid,
    /// OCI manifest schema version
    pub schema_version: i32,
    /// Architecture (e.g., amd64, arm64)
    pub architecture: Option<String>,
    /// OS (e.g., linux, windows)
    pub os: Option<String>,
    /// Config digest
    pub config_digest: Option<String>,
    /// Layer digests
    pub layer_digests: Vec<String>,
}

/// WASM-specific metadata
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WasmMetadata {
    pub artifact_id: Uuid,
    /// WASM runtime compatibility
    pub runtime: Option<String>,
    /// Exported functions
    pub exports: Vec<String>,
    /// Imported functions
    pub imports: Vec<String>,
}

/// Upload session for chunked uploads
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UploadSession {
    pub id: Uuid,
    pub repository_id: Uuid,
    /// Bytes uploaded so far
    pub offset: i64,
    pub created_at: DateTime<Utc>,
    pub expires_at: DateTime<Utc>,
}

/// API response for artifact listing
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ArtifactSummary {
    pub id: Uuid,
    pub name: String,
    pub description: Option<String>,
    pub artifact_type: ArtifactType,
    pub digest: String,
    pub size: i64,
    pub tags: Vec<String>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

/// Statistics for dashboard
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RegistryStats {
    pub total_artifacts: i64,
    pub total_repositories: i64,
    pub total_namespaces: i64,
    pub by_type: Vec<(ArtifactType, i64)>,
    pub total_size_bytes: i64,
}
