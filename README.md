# manifold-core

Core library for [Manifold](https://manifold.omni.dev), the universal artifact
registry. It provides the storage, digest, and registry primitives the Manifold
services build on, independent of any transport or database.

## Artifact types

- OCI / Docker container images
- OpenAPI specifications
- GraphQL schemas
- JSON schemas
- WebAssembly modules
- Generic blobs

## Overview

- `BlobStorage` — a trait for content-addressed blob storage, with two built-in
  implementations: `FileStorage` (on-disk) and `MemoryStorage` (in-memory, for
  tests).
- `Storage<B>` — digest-addressed blob operations over any `BlobStorage`.
- `Registry<B>` — namespace, repository, artifact, and tag operations over a
  backing store.
- `compute_digest` / `parse_digest` — SHA-256 digest helpers in the
  `sha256:<hex>` form.
- Domain types in `types` (`ArtifactType`, `Namespace`, `Repository`,
  `Artifact`, `Tag`, and per-artifact metadata).

## Usage

Add the dependency:

```toml
[dependencies]
manifold-core = "0.1"
```

Store and retrieve a blob with the in-memory backend. `put` is
content-addressed: it hashes the data and returns the `sha256:<hex>` digest.

```rust
use bytes::Bytes;
use manifold_core::storage::{BlobStorage, MemoryStorage};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let storage = MemoryStorage::new();

    let digest = storage.put(Bytes::from_static(b"hello manifold")).await?;

    let fetched = storage.get(&digest).await?;
    assert_eq!(&fetched[..], b"hello manifold");
    Ok(())
}
```

## Development

```sh
cargo build
cargo test
```

## License

Apache-2.0. See [LICENSE.md](./LICENSE.md).
