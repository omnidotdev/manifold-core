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
#[must_use]
pub fn compute_digest(data: &[u8]) -> String {
    let mut hasher = Sha256::new();
    hasher.update(data);
    let result = hasher.finalize();
    format!("sha256:{}", hex::encode(result))
}

/// Parse digest into algorithm and hash
#[must_use]
pub fn parse_digest(digest: &str) -> Option<(&str, &str)> {
    digest.split_once(':')
}

/// Filesystem-based blob storage
pub struct FileStorage {
    root: PathBuf,
}

impl FileStorage {
    /// Create a new file storage backend.
    ///
    /// # Errors
    ///
    /// Returns `StorageError::Io` if the root directory cannot be created.
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
    #[must_use]
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
    pub const fn new(pool: sqlx::PgPool, blobs: B) -> Self {
        Self { blobs, pool }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn compute_digest_produces_sha256_format() {
        let digest = compute_digest(b"hello world");
        assert!(digest.starts_with("sha256:"));
        // sha256 hex is 64 chars
        assert_eq!(digest.len(), 7 + 64);
    }

    #[test]
    fn compute_digest_is_deterministic() {
        let d1 = compute_digest(b"test data");
        let d2 = compute_digest(b"test data");
        assert_eq!(d1, d2);
    }

    #[test]
    fn parse_digest_splits_correctly() {
        let result = parse_digest("sha256:abc123");
        assert_eq!(result, Some(("sha256", "abc123")));
    }

    #[test]
    fn parse_digest_returns_none_on_invalid() {
        let result = parse_digest("nocolon");
        assert!(result.is_none());
    }

    #[tokio::test]
    async fn memory_storage_put_get_roundtrip() {
        let storage = MemoryStorage::new();
        let data = Bytes::from("hello world");
        let digest = storage.put(data.clone()).await.unwrap();
        let retrieved = storage.get(&digest).await.unwrap();
        assert_eq!(retrieved, data);
    }

    #[tokio::test]
    async fn memory_storage_put_with_digest_validates() {
        let storage = MemoryStorage::new();
        let data = Bytes::from("hello world");
        let digest = compute_digest(&data);
        storage.put_with_digest(&digest, data).await.unwrap();
        assert!(storage.exists(&digest).await.unwrap());
    }

    #[tokio::test]
    async fn memory_storage_put_with_digest_rejects_mismatch() {
        let storage = MemoryStorage::new();
        let data = Bytes::from("hello world");
        let result = storage
            .put_with_digest(
                "sha256:0000000000000000000000000000000000000000000000000000000000000000",
                data,
            )
            .await;
        assert!(matches!(result, Err(StorageError::DigestMismatch { .. })));
    }

    #[tokio::test]
    async fn memory_storage_exists_returns_correctly() {
        let storage = MemoryStorage::new();
        let data = Bytes::from("test");
        let digest = storage.put(data).await.unwrap();
        assert!(storage.exists(&digest).await.unwrap());
        assert!(!storage.exists("sha256:nonexistent").await.unwrap());
    }

    #[tokio::test]
    async fn memory_storage_delete_removes_blob() {
        let storage = MemoryStorage::new();
        let data = Bytes::from("delete me");
        let digest = storage.put(data).await.unwrap();
        storage.delete(&digest).await.unwrap();
        assert!(storage.get(&digest).await.is_err());
    }

    #[tokio::test]
    async fn memory_storage_size_returns_correct_length() {
        let storage = MemoryStorage::new();
        let data = Bytes::from("12345");
        let digest = storage.put(data).await.unwrap();
        assert_eq!(storage.size(&digest).await.unwrap(), 5);
    }

    #[tokio::test]
    async fn file_storage_creates_directory() {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path().join("storage");
        let _storage = FileStorage::new(root.clone()).await.unwrap();
        assert!(root.exists());
    }

    #[tokio::test]
    async fn file_storage_roundtrip() {
        let dir = tempfile::tempdir().unwrap();
        let storage = FileStorage::new(dir.path().join("blobs")).await.unwrap();
        let data = Bytes::from("file storage test");
        let digest = storage.put(data.clone()).await.unwrap();
        let retrieved = storage.get(&digest).await.unwrap();
        assert_eq!(retrieved, data);

        // Verify exists
        assert!(storage.exists(&digest).await.unwrap());

        // Verify size
        assert_eq!(storage.size(&digest).await.unwrap(), data.len() as u64);

        // Delete and verify gone
        storage.delete(&digest).await.unwrap();
        assert!(!storage.exists(&digest).await.unwrap());
    }
}
