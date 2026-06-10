//! Artifact backend — durable binary blob storage for the engine.
//!
//! Backed by [`object_store`], so the same code serves three deployments from
//! one tested implementation:
//! - **local filesystem** (`local`) — the durable default for the single
//!   binary / self-host; survives restart, participates in crash recovery.
//! - **S3-compatible** (`s3`) — AWS S3, Cloudflare R2, `MinIO`, … for cloud/scale.
//! - **in-memory** (`memory`) — **tests only**; never a production default,
//!   because losing artifacts on restart would silently break the engine's
//!   durability contract.
//!
//! Bytes live in the object store under `"<instance_id>/<artifact_id>"`; the
//! small [`ArtifactRef`] returned at store time travels in step outputs.

use std::sync::Arc;

use bytes::Bytes;
use futures::StreamExt;
use object_store::{path::Path, ObjectStore, PutPayload};
use uuid::Uuid;

use orch8_types::artifact::{ArtifactMeta, ArtifactRef};
use orch8_types::error::StorageError;

/// Resolve an optional artifact store, or fail with the canonical permanent
/// "not configured" error. Shared by the storage backends so the message and
/// the (permanent) error variant live in exactly one place.
///
/// # Errors
/// Returns [`StorageError::Unsupported`] when no artifact backend is wired in.
pub fn require_store(
    store: Option<&Arc<ObjectArtifactStore>>,
) -> Result<&ObjectArtifactStore, StorageError> {
    store
        .map(Arc::as_ref)
        .ok_or_else(|| StorageError::Unsupported("artifact storage is not configured".into()))
}

/// Configuration for an S3-compatible artifact backend.
#[derive(Debug, Clone, Default)]
pub struct S3Config {
    pub bucket: String,
    pub region: String,
    /// Custom endpoint for non-AWS providers (R2/`MinIO`). Empty → AWS default.
    pub endpoint: String,
    pub access_key_id: String,
    pub secret_access_key: String,
    /// Allow plain HTTP (e.g. local `MinIO`). Production S3/R2 use HTTPS.
    pub allow_http: bool,
}

/// Durable artifact store over an `object_store` backend.
#[derive(Clone)]
pub struct ObjectArtifactStore {
    store: Arc<dyn ObjectStore>,
}

impl std::fmt::Debug for ObjectArtifactStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ObjectArtifactStore")
            .finish_non_exhaustive()
    }
}

// Takes the error by value so it works as `.map_err(map_err)` (object_store
// methods yield an owned error). Object-store failures are transient
// (network / throttling / temporary unavailability) → `Backend` (retryable),
// NOT `Unsupported` (reserved for permanent "not configured / not supported").
#[allow(clippy::needless_pass_by_value)]
fn map_err(e: object_store::Error) -> StorageError {
    StorageError::Backend(format!("artifact backend: {e}"))
}

impl ObjectArtifactStore {
    /// In-memory backend. **Tests/dev only** — not durable across restarts.
    #[must_use]
    pub fn memory() -> Self {
        Self {
            store: Arc::new(object_store::memory::InMemory::new()),
        }
    }

    /// Local-filesystem backend rooted at `path` (created if missing).
    ///
    /// # Errors
    /// Returns an error if the directory cannot be created or opened.
    pub fn local(path: &str) -> Result<Self, StorageError> {
        std::fs::create_dir_all(path)
            .map_err(|e| StorageError::Unsupported(format!("artifact dir {path}: {e}")))?;
        let fs = object_store::local::LocalFileSystem::new_with_prefix(path).map_err(map_err)?;
        Ok(Self {
            store: Arc::new(fs),
        })
    }

    /// S3-compatible backend (AWS S3 / Cloudflare R2 / `MinIO`).
    ///
    /// # Errors
    /// Returns an error if the client cannot be constructed from `cfg`.
    pub fn s3(cfg: &S3Config) -> Result<Self, StorageError> {
        let mut builder = object_store::aws::AmazonS3Builder::new()
            .with_bucket_name(&cfg.bucket)
            .with_access_key_id(&cfg.access_key_id)
            .with_secret_access_key(&cfg.secret_access_key)
            .with_allow_http(cfg.allow_http);
        if !cfg.region.is_empty() {
            builder = builder.with_region(&cfg.region);
        }
        if !cfg.endpoint.is_empty() {
            builder = builder.with_endpoint(&cfg.endpoint);
        }
        let s3 = builder.build().map_err(map_err)?;
        Ok(Self {
            store: Arc::new(s3),
        })
    }

    /// Wrap an arbitrary `object_store` backend (used by tests / advanced setups).
    #[must_use]
    pub fn from_store(store: Arc<dyn ObjectStore>) -> Self {
        Self { store }
    }

    fn object_key(instance_id: &str, artifact_id: &str) -> String {
        format!("{instance_id}/{artifact_id}")
    }

    /// Store `bytes` and return a durable reference. Takes owned [`Bytes`] so
    /// the buffer reqwest already allocated flows into `object_store` without a
    /// second copy.
    ///
    /// # Errors
    /// Returns an error if the write to the backend fails.
    pub async fn put(
        &self,
        instance_id: &str,
        content_type: &str,
        bytes: Bytes,
    ) -> Result<ArtifactRef, StorageError> {
        let id = Uuid::new_v4().to_string();
        let key = Self::object_key(instance_id, &id);
        let size = bytes.len() as u64;
        self.store
            .put(&Path::from(key.clone()), PutPayload::from_bytes(bytes))
            .await
            .map_err(map_err)?;
        Ok(ArtifactRef {
            id,
            instance_id: instance_id.to_string(),
            key: key.clone(),
            content_type: content_type.to_string(),
            size,
            uri: format!("artifact://{key}"),
        })
    }

    /// Fetch the bytes for an artifact key. Returns `None` if absent.
    ///
    /// # Errors
    /// Returns an error on a non-not-found backend failure.
    pub async fn get(&self, key: &str) -> Result<Option<Vec<u8>>, StorageError> {
        match self.store.get(&Path::from(key.to_string())).await {
            Ok(res) => {
                let bytes = res.bytes().await.map_err(map_err)?;
                Ok(Some(bytes.to_vec()))
            }
            Err(object_store::Error::NotFound { .. }) => Ok(None),
            Err(e) => Err(map_err(e)),
        }
    }

    /// Delete an artifact by key. Missing keys are treated as success.
    ///
    /// # Errors
    /// Returns an error on a non-not-found backend failure.
    pub async fn delete(&self, key: &str) -> Result<(), StorageError> {
        match self.store.delete(&Path::from(key.to_string())).await {
            Ok(()) | Err(object_store::Error::NotFound { .. }) => Ok(()),
            Err(e) => Err(map_err(e)),
        }
    }

    /// List metadata for all artifacts of an instance.
    ///
    /// # Errors
    /// Returns an error if the backend listing fails.
    pub async fn list(&self, instance_id: &str) -> Result<Vec<ArtifactMeta>, StorageError> {
        let prefix = Path::from(instance_id.to_string());
        let mut stream = self.store.list(Some(&prefix));
        let mut out = Vec::new();
        while let Some(meta) = stream.next().await {
            let meta = meta.map_err(map_err)?;
            out.push(ArtifactMeta {
                key: meta.location.to_string(),
                size: meta.size as u64,
            });
        }
        Ok(out)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    async fn roundtrip(store: ObjectArtifactStore) {
        let r = store
            .put("inst1", "text/plain", Bytes::from_static(b"hello"))
            .await
            .unwrap();
        assert_eq!(r.instance_id, "inst1");
        assert_eq!(r.content_type, "text/plain");
        assert_eq!(r.size, 5);
        assert_eq!(r.key, format!("inst1/{}", r.id));
        assert_eq!(r.uri, format!("artifact://{}", r.key));

        let got = store.get(&r.key).await.unwrap();
        assert_eq!(got.as_deref(), Some(&b"hello"[..]));

        let listed = store.list("inst1").await.unwrap();
        assert!(listed.iter().any(|m| m.key == r.key && m.size == 5));

        store.delete(&r.key).await.unwrap();
        assert!(store.get(&r.key).await.unwrap().is_none());
        // Deleting a missing key is a no-op.
        store.delete(&r.key).await.unwrap();
    }

    #[tokio::test]
    async fn memory_backend_roundtrip() {
        roundtrip(ObjectArtifactStore::memory()).await;
    }

    #[tokio::test]
    async fn local_backend_roundtrip() {
        let dir = std::env::temp_dir().join(format!("orch8-art-{}", Uuid::new_v4()));
        let store = ObjectArtifactStore::local(dir.to_str().unwrap()).unwrap();
        roundtrip(store).await;
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[tokio::test]
    async fn get_missing_returns_none() {
        let store = ObjectArtifactStore::memory();
        assert!(store.get("inst1/nope").await.unwrap().is_none());
    }

    #[tokio::test]
    async fn list_scopes_to_instance() {
        let store = ObjectArtifactStore::memory();
        store
            .put("instA", "text/plain", Bytes::from_static(b"a"))
            .await
            .unwrap();
        store
            .put("instB", "text/plain", Bytes::from_static(b"b"))
            .await
            .unwrap();
        let a = store.list("instA").await.unwrap();
        assert_eq!(a.len(), 1);
        let b = store.list("instB").await.unwrap();
        assert_eq!(b.len(), 1);
    }

    #[test]
    fn s3_backend_builds_from_config() {
        // Construction only — no network. Proves the config → client path.
        let store = ObjectArtifactStore::s3(&S3Config {
            bucket: "my-bucket".into(),
            region: "us-east-1".into(),
            endpoint: "https://example.r2.cloudflarestorage.com".into(),
            access_key_id: "ak".into(),
            secret_access_key: "sk".into(),
            allow_http: false,
        });
        assert!(store.is_ok(), "S3 client should build: {:?}", store.err());
    }

    #[test]
    fn debug_does_not_leak_internals() {
        let s = format!("{:?}", ObjectArtifactStore::memory());
        assert!(s.contains("ObjectArtifactStore"));
    }

    #[tokio::test]
    async fn delete_instance_artifacts_removes_all_blobs() {
        use crate::ResourceStore;
        use orch8_types::ids::InstanceId;

        let storage = crate::sqlite::SqliteStorage::in_memory()
            .await
            .unwrap()
            .with_artifact_store(Arc::new(ObjectArtifactStore::memory()));
        let inst = InstanceId::new();
        let other = InstanceId::new();
        storage
            .put_artifact(inst, "text/plain", Bytes::from_static(b"a"))
            .await
            .unwrap();
        storage
            .put_artifact(inst, "text/plain", Bytes::from_static(b"b"))
            .await
            .unwrap();
        storage
            .put_artifact(other, "text/plain", Bytes::from_static(b"c"))
            .await
            .unwrap();

        let removed = storage.delete_instance_artifacts(inst).await.unwrap();
        assert_eq!(removed, 2);
        assert!(storage.list_artifacts(inst).await.unwrap().is_empty());
        // Other instances are untouched.
        assert_eq!(storage.list_artifacts(other).await.unwrap().len(), 1);
    }

    #[tokio::test]
    async fn artifact_gc_candidate_lifecycle() {
        use crate::{InstanceStore, ResourceStore};
        use orch8_types::context::ExecutionContext;
        use orch8_types::ids::{InstanceId, Namespace, SequenceId, TenantId};
        use orch8_types::instance::{InstanceState, Priority, TaskInstance};

        let storage = crate::sqlite::SqliteStorage::in_memory()
            .await
            .unwrap()
            .with_artifact_store(Arc::new(ObjectArtifactStore::memory()));

        let mk = |state: InstanceState| {
            let now = chrono::Utc::now();
            TaskInstance {
                id: InstanceId::new(),
                sequence_id: SequenceId::new(),
                tenant_id: TenantId::unchecked("t"),
                namespace: Namespace::new("default"),
                state,
                next_fire_at: Some(now),
                priority: Priority::Normal,
                timezone: "UTC".into(),
                metadata: serde_json::json!({}),
                context: ExecutionContext::default(),
                concurrency_key: None,
                max_concurrency: None,
                idempotency_key: None,
                session_id: None,
                parent_instance_id: None,
                budget: None,
                created_at: now,
                updated_at: now,
            }
        };

        let done = mk(InstanceState::Completed);
        let running = mk(InstanceState::Running);
        storage.create_instance(&done).await.unwrap();
        storage.create_instance(&running).await.unwrap();
        storage
            .put_artifact(done.id, "text/plain", Bytes::from_static(b"x"))
            .await
            .unwrap();
        storage
            .put_artifact(running.id, "text/plain", Bytes::from_static(b"y"))
            .await
            .unwrap();

        // Future cutoff so both instances are "old enough"; only the terminal
        // one is a candidate.
        let cutoff = chrono::Utc::now() + chrono::Duration::hours(1);
        let cands = storage
            .list_artifact_gc_candidates(cutoff, 100)
            .await
            .unwrap();
        assert!(cands.contains(&done.id));
        assert!(
            !cands.contains(&running.id),
            "non-terminal must not be a candidate"
        );

        // Sweep the terminal instance and mark it.
        assert_eq!(storage.delete_instance_artifacts(done.id).await.unwrap(), 1);
        storage.mark_artifacts_gced(done.id).await.unwrap();

        // Marked → no longer a candidate; its blobs are gone; the other instance
        // is untouched.
        let after = storage
            .list_artifact_gc_candidates(cutoff, 100)
            .await
            .unwrap();
        assert!(
            !after.contains(&done.id),
            "marked instance must be excluded"
        );
        assert!(storage.list_artifacts(done.id).await.unwrap().is_empty());
        assert_eq!(storage.list_artifacts(running.id).await.unwrap().len(), 1);
    }

    #[tokio::test]
    async fn delete_instance_artifacts_noop_without_backend() {
        use crate::ResourceStore;
        use orch8_types::ids::InstanceId;
        // No artifact store wired in → Ok(0), not an error.
        let storage = crate::sqlite::SqliteStorage::in_memory().await.unwrap();
        assert_eq!(
            storage
                .delete_instance_artifacts(InstanceId::new())
                .await
                .unwrap(),
            0
        );
    }
}
