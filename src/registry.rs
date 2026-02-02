//! Registry operations for Manifold

use crate::storage::{BlobStorage, Storage};
use crate::types::*;
use bytes::Bytes;
use chrono::Utc;
use sqlx::PgPool;
use thiserror::Error;
use uuid::Uuid;

#[derive(Error, Debug)]
pub enum RegistryError {
    #[error("Not found: {0}")]
    NotFound(String),
    #[error("Already exists: {0}")]
    AlreadyExists(String),
    #[error("Invalid request: {0}")]
    InvalidRequest(String),
    #[error("Storage error: {0}")]
    Storage(#[from] crate::storage::StorageError),
    #[error("Database error: {0}")]
    Database(#[from] sqlx::Error),
}

/// Main registry interface
pub struct Registry<B: BlobStorage> {
    storage: Storage<B>,
}

impl<B: BlobStorage> Registry<B> {
    pub fn new(pool: PgPool, blobs: B) -> Self {
        Self {
            storage: Storage::new(pool, blobs),
        }
    }

    pub fn pool(&self) -> &PgPool {
        &self.storage.pool
    }

    // =========================================================================
    // Namespace operations
    // =========================================================================

    pub async fn create_namespace(&self, name: &str, description: Option<&str>) -> Result<Namespace, RegistryError> {
        let now = Utc::now();
        let ns = Namespace {
            id: Uuid::new_v4(),
            name: name.to_string(),
            description: description.map(String::from),
            created_at: now,
            updated_at: now,
        };

        sqlx::query(
            r#"
            INSERT INTO namespaces (id, name, description, created_at, updated_at)
            VALUES ($1, $2, $3, $4, $5)
            "#,
        )
        .bind(ns.id)
        .bind(&ns.name)
        .bind(&ns.description)
        .bind(ns.created_at)
        .bind(ns.updated_at)
        .execute(&self.storage.pool)
        .await?;

        Ok(ns)
    }

    pub async fn get_namespace(&self, name: &str) -> Result<Namespace, RegistryError> {
        sqlx::query_as::<_, (Uuid, String, Option<String>, chrono::DateTime<Utc>, chrono::DateTime<Utc>)>(
            "SELECT id, name, description, created_at, updated_at FROM namespaces WHERE name = $1",
        )
        .bind(name)
        .fetch_optional(&self.storage.pool)
        .await?
        .map(|(id, name, description, created_at, updated_at)| Namespace {
            id,
            name,
            description,
            created_at,
            updated_at,
        })
        .ok_or_else(|| RegistryError::NotFound(format!("namespace: {}", name)))
    }

    pub async fn list_namespaces(&self) -> Result<Vec<Namespace>, RegistryError> {
        let rows = sqlx::query_as::<_, (Uuid, String, Option<String>, chrono::DateTime<Utc>, chrono::DateTime<Utc>)>(
            "SELECT id, name, description, created_at, updated_at FROM namespaces ORDER BY name",
        )
        .fetch_all(&self.storage.pool)
        .await?;

        Ok(rows
            .into_iter()
            .map(|(id, name, description, created_at, updated_at)| Namespace {
                id,
                name,
                description,
                created_at,
                updated_at,
            })
            .collect())
    }

    // =========================================================================
    // Repository operations
    // =========================================================================

    pub async fn create_repository(
        &self,
        namespace: &str,
        name: &str,
        artifact_type: ArtifactType,
        description: Option<&str>,
        is_public: bool,
    ) -> Result<Repository, RegistryError> {
        let ns = self.get_namespace(namespace).await?;
        let now = Utc::now();

        let repo = Repository {
            id: Uuid::new_v4(),
            namespace_id: ns.id,
            name: name.to_string(),
            description: description.map(String::from),
            artifact_type,
            is_public,
            created_at: now,
            updated_at: now,
        };

        sqlx::query(
            r#"
            INSERT INTO repositories (id, namespace_id, name, description, artifact_type, is_public, created_at, updated_at)
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
            "#,
        )
        .bind(repo.id)
        .bind(repo.namespace_id)
        .bind(&repo.name)
        .bind(&repo.description)
        .bind(repo.artifact_type.as_str())
        .bind(repo.is_public)
        .bind(repo.created_at)
        .bind(repo.updated_at)
        .execute(&self.storage.pool)
        .await?;

        Ok(repo)
    }

    pub async fn get_repository(&self, namespace: &str, name: &str) -> Result<Repository, RegistryError> {
        sqlx::query_as::<_, (Uuid, Uuid, String, Option<String>, String, bool, chrono::DateTime<Utc>, chrono::DateTime<Utc>)>(
            r#"
            SELECT r.id, r.namespace_id, r.name, r.description, r.artifact_type, r.is_public, r.created_at, r.updated_at
            FROM repositories r
            JOIN namespaces n ON r.namespace_id = n.id
            WHERE n.name = $1 AND r.name = $2
            "#,
        )
        .bind(namespace)
        .bind(name)
        .fetch_optional(&self.storage.pool)
        .await?
        .map(|(id, namespace_id, name, description, artifact_type, is_public, created_at, updated_at)| {
            Repository {
                id,
                namespace_id,
                name,
                description,
                artifact_type: match artifact_type.as_str() {
                    "container" => ArtifactType::Container,
                    "openapi" => ArtifactType::OpenApi,
                    "graphql" => ArtifactType::GraphQl,
                    "json-schema" => ArtifactType::JsonSchema,
                    "wasm" => ArtifactType::Wasm,
                    _ => ArtifactType::Blob,
                },
                is_public,
                created_at,
                updated_at,
            }
        })
        .ok_or_else(|| RegistryError::NotFound(format!("repository: {}/{}", namespace, name)))
    }

    // =========================================================================
    // Artifact operations
    // =========================================================================

    pub async fn push_artifact(
        &self,
        namespace: &str,
        repo_name: &str,
        data: Bytes,
        media_type: &str,
        tag: Option<&str>,
    ) -> Result<Artifact, RegistryError> {
        let repo = self.get_repository(namespace, repo_name).await?;

        // Store blob
        let digest = self.storage.blobs.put(data.clone()).await?;
        let size = data.len() as i64;
        let now = Utc::now();

        let artifact = Artifact {
            id: Uuid::new_v4(),
            repository_id: repo.id,
            digest: digest.clone(),
            size,
            media_type: media_type.to_string(),
            content: if size < 1024 * 1024 { Some(data.to_vec()) } else { None },
            created_at: now,
        };

        sqlx::query(
            r#"
            INSERT INTO artifacts (id, repository_id, digest, size, media_type, content, created_at)
            VALUES ($1, $2, $3, $4, $5, $6, $7)
            ON CONFLICT (repository_id, digest) DO UPDATE SET
                size = EXCLUDED.size,
                media_type = EXCLUDED.media_type,
                content = EXCLUDED.content
            "#,
        )
        .bind(artifact.id)
        .bind(artifact.repository_id)
        .bind(&artifact.digest)
        .bind(artifact.size)
        .bind(&artifact.media_type)
        .bind(&artifact.content)
        .bind(artifact.created_at)
        .execute(&self.storage.pool)
        .await?;

        // Create tag if provided
        if let Some(tag_name) = tag {
            self.tag_artifact(namespace, repo_name, &digest, tag_name).await?;
        }

        Ok(artifact)
    }

    pub async fn get_artifact(&self, namespace: &str, repo_name: &str, reference: &str) -> Result<Artifact, RegistryError> {
        let repo = self.get_repository(namespace, repo_name).await?;

        // Reference can be a digest or a tag
        let digest = if reference.starts_with("sha256:") {
            reference.to_string()
        } else {
            // Look up tag
            let tag = sqlx::query_as::<_, (String,)>(
                r#"
                SELECT a.digest
                FROM tags t
                JOIN artifacts a ON t.artifact_id = a.id
                WHERE t.repository_id = $1 AND t.name = $2
                "#,
            )
            .bind(repo.id)
            .bind(reference)
            .fetch_optional(&self.storage.pool)
            .await?
            .ok_or_else(|| RegistryError::NotFound(format!("tag: {}", reference)))?;
            tag.0
        };

        sqlx::query_as::<_, (Uuid, Uuid, String, i64, String, Option<Vec<u8>>, chrono::DateTime<Utc>)>(
            "SELECT id, repository_id, digest, size, media_type, content, created_at FROM artifacts WHERE repository_id = $1 AND digest = $2",
        )
        .bind(repo.id)
        .bind(&digest)
        .fetch_optional(&self.storage.pool)
        .await?
        .map(|(id, repository_id, digest, size, media_type, content, created_at)| Artifact {
            id,
            repository_id,
            digest,
            size,
            media_type,
            content,
            created_at,
        })
        .ok_or_else(|| RegistryError::NotFound(format!("artifact: {}", digest)))
    }

    pub async fn get_artifact_content(&self, namespace: &str, repo_name: &str, reference: &str) -> Result<Bytes, RegistryError> {
        let artifact = self.get_artifact(namespace, repo_name, reference).await?;

        // Try inline content first
        if let Some(content) = artifact.content {
            return Ok(Bytes::from(content));
        }

        // Fetch from blob storage
        self.storage.blobs.get(&artifact.digest).await.map_err(Into::into)
    }

    // =========================================================================
    // Tag operations
    // =========================================================================

    pub async fn tag_artifact(&self, namespace: &str, repo_name: &str, digest: &str, tag_name: &str) -> Result<Tag, RegistryError> {
        let artifact = self.get_artifact(namespace, repo_name, digest).await?;
        let now = Utc::now();

        let tag = Tag {
            id: Uuid::new_v4(),
            repository_id: artifact.repository_id,
            artifact_id: artifact.id,
            name: tag_name.to_string(),
            created_at: now,
            updated_at: now,
        };

        sqlx::query(
            r#"
            INSERT INTO tags (id, repository_id, artifact_id, name, created_at, updated_at)
            VALUES ($1, $2, $3, $4, $5, $6)
            ON CONFLICT (repository_id, name) DO UPDATE SET
                artifact_id = EXCLUDED.artifact_id,
                updated_at = EXCLUDED.updated_at
            "#,
        )
        .bind(tag.id)
        .bind(tag.repository_id)
        .bind(tag.artifact_id)
        .bind(&tag.name)
        .bind(tag.created_at)
        .bind(tag.updated_at)
        .execute(&self.storage.pool)
        .await?;

        Ok(tag)
    }

    pub async fn list_tags(&self, namespace: &str, repo_name: &str) -> Result<Vec<String>, RegistryError> {
        let repo = self.get_repository(namespace, repo_name).await?;

        let tags = sqlx::query_as::<_, (String,)>(
            "SELECT name FROM tags WHERE repository_id = $1 ORDER BY updated_at DESC",
        )
        .bind(repo.id)
        .fetch_all(&self.storage.pool)
        .await?;

        Ok(tags.into_iter().map(|(name,)| name).collect())
    }

    // =========================================================================
    // Statistics
    // =========================================================================

    pub async fn get_stats(&self) -> Result<RegistryStats, RegistryError> {
        let total_artifacts: (i64,) = sqlx::query_as("SELECT COUNT(*) FROM artifacts")
            .fetch_one(&self.storage.pool)
            .await?;

        let total_repositories: (i64,) = sqlx::query_as("SELECT COUNT(*) FROM repositories")
            .fetch_one(&self.storage.pool)
            .await?;

        let total_namespaces: (i64,) = sqlx::query_as("SELECT COUNT(*) FROM namespaces")
            .fetch_one(&self.storage.pool)
            .await?;

        let total_size: (Option<i64>,) = sqlx::query_as("SELECT SUM(size) FROM artifacts")
            .fetch_one(&self.storage.pool)
            .await?;

        let by_type = sqlx::query_as::<_, (String, i64)>(
            r#"
            SELECT r.artifact_type, COUNT(a.id)
            FROM repositories r
            LEFT JOIN artifacts a ON r.id = a.repository_id
            GROUP BY r.artifact_type
            "#,
        )
        .fetch_all(&self.storage.pool)
        .await?
        .into_iter()
        .map(|(t, c)| {
            let artifact_type = match t.as_str() {
                "container" => ArtifactType::Container,
                "openapi" => ArtifactType::OpenApi,
                "graphql" => ArtifactType::GraphQl,
                "json-schema" => ArtifactType::JsonSchema,
                "wasm" => ArtifactType::Wasm,
                _ => ArtifactType::Blob,
            };
            (artifact_type, c)
        })
        .collect();

        Ok(RegistryStats {
            total_artifacts: total_artifacts.0,
            total_repositories: total_repositories.0,
            total_namespaces: total_namespaces.0,
            by_type,
            total_size_bytes: total_size.0.unwrap_or(0),
        })
    }
}
