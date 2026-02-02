//! Storage backends for Manifold artifacts

use async_trait::async_trait;
use bytes::Bytes;
use sha2::{Digest, Sha256};
use std::path::PathBuf;
use thiserror::Error;
use tokio::fs;
use tokio::io::AsyncWriteExt;

#[derive(Error, Debug)]
pub enum StorageError {
    #[error("Blob not found: {0}")]
    NotFound(String),
    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),
    #[error("Digest mismatch: expected {expected}, got {actual}")]
    DigestMismatch { expected: String, actual: String },
}

/// Storage backend trait for blob storage
#[async_trait]
pub trait BlobStorage: Send + Sync {
    /// Store a blob, returns the digest
    async fn put(&self, data: Bytes) -> Result<String, StorageError>;

    /// Store a blob with expected digest (validates on write)
    async fn put_with_digest(&self, digest: &str, data: Bytes) -> Result<(), StorageError>;

    /// Retrieve a blob by digest
    async fn get(&self, digest: &str) -> Result<Bytes, StorageError>;

    /// Check if a blob exists
    async fn exists(&self, digest: &str) -> Result<bool, StorageError>;

    /// Delete a blob
    async fn delete(&self, digest: &str) -> Result<(), StorageError>;

    /// Get blob size without fetching content
    async fn size(&self, digest: &str) -> Result<u64, StorageError>;
}

/// Compute SHA256 digest of data
pub fn compute_digest(data: &[u8]) -> String {
    let mut hasher = Sha256::new();
    hasher.update(data);
    let result = hasher.finalize();
    format!("sha256:{}", hex::encode(result))
}

/// Parse digest into algorithm and hash
pub fn parse_digest(digest: &str) -> Option<(&str, &str)> {
    digest.split_once(':')
}

/// Filesystem-based blob storage
pub struct FileStorage {
    root: PathBuf,
}

impl FileStorage {
    pub async fn new(root: PathBuf) -> Result<Self, StorageError> {
        fs::create_dir_all(&root).await?;
        Ok(Self { root })
    }

    fn blob_path(&self, digest: &str) -> PathBuf {
        // sha256:abc123... -> blobs/sha256/ab/abc123...
        let (algo, hash) = parse_digest(digest).unwrap_or(("sha256", digest));
        let prefix = &hash[..2.min(hash.len())];
        self.root.join("blobs").join(algo).join(prefix).join(hash)
    }
}

#[async_trait]
impl BlobStorage for FileStorage {
    async fn put(&self, data: Bytes) -> Result<String, StorageError> {
        let digest = compute_digest(&data);
        self.put_with_digest(&digest, data).await?;
        Ok(digest)
    }

    async fn put_with_digest(&self, digest: &str, data: Bytes) -> Result<(), StorageError> {
        // Verify digest
        let actual = compute_digest(&data);
        if actual != digest {
            return Err(StorageError::DigestMismatch {
                expected: digest.to_string(),
                actual,
            });
        }

        let path = self.blob_path(digest);
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent).await?;
        }

        let mut file = fs::File::create(&path).await?;
        file.write_all(&data).await?;
        file.sync_all().await?;

        Ok(())
    }

    async fn get(&self, digest: &str) -> Result<Bytes, StorageError> {
        let path = self.blob_path(digest);
        let data = fs::read(&path)
            .await
            .map_err(|_| StorageError::NotFound(digest.to_string()))?;
        Ok(Bytes::from(data))
    }

    async fn exists(&self, digest: &str) -> Result<bool, StorageError> {
        let path = self.blob_path(digest);
        Ok(path.exists())
    }

    async fn delete(&self, digest: &str) -> Result<(), StorageError> {
        let path = self.blob_path(digest);
        fs::remove_file(&path)
            .await
            .map_err(|_| StorageError::NotFound(digest.to_string()))?;
        Ok(())
    }

    async fn size(&self, digest: &str) -> Result<u64, StorageError> {
        let path = self.blob_path(digest);
        let metadata = fs::metadata(&path)
            .await
            .map_err(|_| StorageError::NotFound(digest.to_string()))?;
        Ok(metadata.len())
    }
}

/// In-memory blob storage (for testing)
pub struct MemoryStorage {
    blobs: std::sync::Arc<tokio::sync::RwLock<std::collections::HashMap<String, Bytes>>>,
}

impl Default for MemoryStorage {
    fn default() -> Self {
        Self::new()
    }
}

impl MemoryStorage {
    pub fn new() -> Self {
        Self {
            blobs: std::sync::Arc::new(tokio::sync::RwLock::new(std::collections::HashMap::new())),
        }
    }
}

#[async_trait]
impl BlobStorage for MemoryStorage {
    async fn put(&self, data: Bytes) -> Result<String, StorageError> {
        let digest = compute_digest(&data);
        self.blobs.write().await.insert(digest.clone(), data);
        Ok(digest)
    }

    async fn put_with_digest(&self, digest: &str, data: Bytes) -> Result<(), StorageError> {
        let actual = compute_digest(&data);
        if actual != digest {
            return Err(StorageError::DigestMismatch {
                expected: digest.to_string(),
                actual,
            });
        }
        self.blobs.write().await.insert(digest.to_string(), data);
        Ok(())
    }

    async fn get(&self, digest: &str) -> Result<Bytes, StorageError> {
        self.blobs
            .read()
            .await
            .get(digest)
            .cloned()
            .ok_or_else(|| StorageError::NotFound(digest.to_string()))
    }

    async fn exists(&self, digest: &str) -> Result<bool, StorageError> {
        Ok(self.blobs.read().await.contains_key(digest))
    }

    async fn delete(&self, digest: &str) -> Result<(), StorageError> {
        self.blobs
            .write()
            .await
            .remove(digest)
            .ok_or_else(|| StorageError::NotFound(digest.to_string()))?;
        Ok(())
    }

    async fn size(&self, digest: &str) -> Result<u64, StorageError> {
        self.blobs
            .read()
            .await
            .get(digest)
            .map(|b| b.len() as u64)
            .ok_or_else(|| StorageError::NotFound(digest.to_string()))
    }
}

/// Combined storage that stores metadata in DB and blobs in blob storage
pub struct Storage<B: BlobStorage> {
    pub blobs: B,
    pub pool: sqlx::PgPool,
}

impl<B: BlobStorage> Storage<B> {
    pub fn new(pool: sqlx::PgPool, blobs: B) -> Self {
        Self { blobs, pool }
    }
}
