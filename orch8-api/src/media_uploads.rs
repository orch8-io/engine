//! Durable fixed-size range uploads for the mobile uploader. Each range is
//! an immutable instance artifact, so the configured storage and encryption
//! policy apply on every server replica.

use axum::Json;
use axum::Router;
use axum::body::Bytes;
use axum::extract::{DefaultBodyLimit, Path, State};
use axum::http::StatusCode;
use axum::routing::put;
use orch8_storage::StorageBackend;
use orch8_types::ids::InstanceId;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use uuid::Uuid;

use crate::AppState;
use crate::auth::{OptionalTenant, enforce_tenant_access};
use crate::error::ApiError;
use crate::sub_tenants::SubTenantHeader;

const MAX_RANGE_BYTES: usize = 4 * 1024 * 1024;
const MAX_UPLOAD_BYTES: u64 = 128 * 1024 * 1024;
const MAX_RANGES: u64 = 4096;
const UPLOAD: &str = "/instances/{instance}/media-uploads/{upload}";
const RANGE: &str = "/instances/{instance}/media-uploads/{upload}/ranges/{offset}";

pub(crate) fn routes() -> Router<AppState> {
    Router::new().route(UPLOAD, put(begin)).route(
        RANGE,
        put(append)
            .get(read)
            .layer(DefaultBodyLimit::max(MAX_RANGE_BYTES)),
    )
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct Manifest {
    length: u64,
    chunk_bytes: u64,
    sha256: String,
}

impl Manifest {
    fn validate(&self) -> Result<(), ApiError> {
        if !(1..=MAX_UPLOAD_BYTES).contains(&self.length)
            || !(1..=MAX_RANGE_BYTES as u64).contains(&self.chunk_bytes)
            || self.length.div_ceil(self.chunk_bytes) > MAX_RANGES
            || self.sha256.len() != 64
            || self
                .sha256
                .bytes()
                .any(|byte| !(byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte)))
        {
            return Err(ApiError::InvalidArgument(
                "invalid media upload manifest".into(),
            ));
        }
        Ok(())
    }

    fn range_length(&self, offset: u64) -> Result<u64, ApiError> {
        if offset >= self.length || !offset.is_multiple_of(self.chunk_bytes) {
            return Err(ApiError::InvalidArgument(
                "invalid media range offset".into(),
            ));
        }
        Ok(self.chunk_bytes.min(self.length - offset))
    }
}

#[derive(Clone, Copy)]
struct Upload {
    instance: InstanceId,
    id: Uuid,
}

impl Upload {
    fn artifact_id(self, kind: &[u8], offset: u64) -> Uuid {
        let mut hash = Sha256::new();
        hash.update(b"orch8-media-upload-v1\0");
        hash.update(self.id.as_bytes());
        hash.update(kind);
        hash.update(offset.to_be_bytes());
        let digest = hash.finalize();
        let mut id = [0; 16];
        id.copy_from_slice(&digest[..16]);
        Uuid::from_bytes(id)
    }

    async fn get(
        self,
        store: &dyn StorageBackend,
        kind: &[u8],
        offset: u64,
    ) -> Result<Option<Vec<u8>>, ApiError> {
        let key = format!("{}/{}", self.instance, self.artifact_id(kind, offset));
        store
            .get_artifact(&key)
            .await
            .map_err(|error| ApiError::from_storage(error, "media upload"))
    }

    async fn put(
        self,
        store: &dyn StorageBackend,
        kind: &[u8],
        offset: u64,
        bytes: Bytes,
    ) -> Result<(), ApiError> {
        store
            .put_artifact_with_id(
                self.instance,
                self.artifact_id(kind, offset),
                "application/octet-stream",
                bytes,
            )
            .await
            .map_err(|error| ApiError::from_storage(error, "media upload"))?;
        Ok(())
    }

    async fn manifest(self, store: &dyn StorageBackend) -> Result<Manifest, ApiError> {
        let data = self
            .get(store, b"manifest", 0)
            .await?
            .ok_or_else(|| ApiError::NotFound("media upload".into()))?;
        let manifest: Manifest = serde_json::from_slice(&data)
            .map_err(|_| ApiError::Conflict("invalid stored media upload manifest".into()))?;
        manifest.validate()?;
        Ok(manifest)
    }

    async fn acknowledge(
        self,
        store: &dyn StorageBackend,
        manifest: &Manifest,
        offset: u64,
        bytes: Bytes,
    ) -> Result<u64, ApiError> {
        let length = manifest.range_length(offset)?;
        if bytes.len() as u64 != length {
            return Err(ApiError::InvalidArgument(
                "media range length does not match manifest".into(),
            ));
        }
        if offset > 0
            && self
                .get(store, b"range", offset - manifest.chunk_bytes)
                .await?
                .is_none()
        {
            return Err(ApiError::Conflict(
                "preceding media range is missing".into(),
            ));
        }
        let end = offset + length;
        if end < manifest.length {
            self.put(store, b"range", offset, bytes).await?;
            return Ok(end);
        }
        if self.get(store, b"complete", 0).await?.is_none() {
            self.verify_final_range(store, manifest, offset, &bytes)
                .await?;
        }
        self.put(store, b"range", offset, bytes).await?;
        // Publishing this marker is the final durable acknowledgement. A
        // crash after writing the last chunk repeats digest verification.
        self.put(
            store,
            b"complete",
            0,
            Bytes::copy_from_slice(manifest.sha256.as_bytes()),
        )
        .await?;
        Ok(end)
    }

    async fn verify_final_range(
        self,
        store: &dyn StorageBackend,
        manifest: &Manifest,
        offset: u64,
        last: &[u8],
    ) -> Result<(), ApiError> {
        let mut hash = Sha256::new();
        let mut current = 0;
        while current < offset {
            let bytes = self
                .get(store, b"range", current)
                .await?
                .ok_or_else(|| ApiError::Conflict("preceding media range is missing".into()))?;
            if bytes.len() as u64 != manifest.chunk_bytes {
                return Err(ApiError::Conflict(
                    "stored media range has the wrong length".into(),
                ));
            }
            hash.update(bytes);
            current += manifest.chunk_bytes;
        }
        hash.update(last);
        let digest = orch8_types::plugin::WasmArtifactDigest::from_sha256(hash.finalize().into());
        if digest.as_str() != manifest.sha256 {
            return Err(ApiError::InvalidArgument(
                "media upload SHA-256 mismatch".into(),
            ));
        }
        Ok(())
    }
}

async fn authorize(
    state: &AppState,
    tenant: &OptionalTenant,
    sub_tenant: &SubTenantHeader,
    id: InstanceId,
) -> Result<(), ApiError> {
    let instance = state
        .storage
        .get_instance(id)
        .await
        .map_err(|error| ApiError::from_storage(error, "instance"))?
        .ok_or_else(|| ApiError::NotFound(format!("instance {id}")))?;
    enforce_tenant_access(tenant, &instance.tenant_id, &format!("instance {id}"))?;
    sub_tenant.enforce_access(instance.sub_tenant.as_deref(), &format!("instance {id}"))
}

async fn begin(
    State(state): State<AppState>,
    tenant: OptionalTenant,
    sub_tenant: SubTenantHeader,
    Path((instance, id)): Path<(InstanceId, Uuid)>,
    Json(manifest): Json<Manifest>,
) -> Result<StatusCode, ApiError> {
    authorize(&state, &tenant, &sub_tenant, instance).await?;
    manifest.validate()?;
    let bytes = serde_json::to_vec(&manifest)
        .map_err(|error| ApiError::InvalidArgument(error.to_string()))?;
    Upload { instance, id }
        .put(state.storage.as_ref(), b"manifest", 0, bytes.into())
        .await?;
    Ok(StatusCode::OK)
}

async fn append(
    State(state): State<AppState>,
    tenant: OptionalTenant,
    sub_tenant: SubTenantHeader,
    Path((instance, id, offset)): Path<(InstanceId, Uuid, u64)>,
    bytes: Bytes,
) -> Result<Json<serde_json::Value>, ApiError> {
    authorize(&state, &tenant, &sub_tenant, instance).await?;
    let upload = Upload { instance, id };
    let store = state.storage.as_ref();
    let manifest = upload.manifest(store).await?;
    let offset = upload.acknowledge(store, &manifest, offset, bytes).await?;
    Ok(Json(serde_json::json!({"offset": offset})))
}

async fn read(
    State(state): State<AppState>,
    tenant: OptionalTenant,
    sub_tenant: SubTenantHeader,
    Path((instance, id, offset)): Path<(InstanceId, Uuid, u64)>,
) -> Result<Bytes, ApiError> {
    authorize(&state, &tenant, &sub_tenant, instance).await?;
    let upload = Upload { instance, id };
    let store = state.storage.as_ref();
    let manifest = upload.manifest(store).await?;
    manifest.range_length(offset)?;
    if upload.get(store, b"complete", 0).await?.as_deref() != Some(manifest.sha256.as_bytes()) {
        return Err(ApiError::Conflict("media upload is incomplete".into()));
    }
    upload
        .get(store, b"range", offset)
        .await?
        .map(Bytes::from)
        .ok_or_else(|| ApiError::NotFound("media upload range".into()))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn manifest_accepts_only_aligned_ranges_with_bounded_counts_and_tail() {
        let mut manifest = Manifest {
            length: 9,
            chunk_bytes: 4,
            sha256: "f".repeat(64),
        };
        manifest.validate().unwrap();
        assert_eq!(manifest.range_length(0).unwrap(), 4);
        assert_eq!(manifest.range_length(4).unwrap(), 4);
        assert_eq!(manifest.range_length(8).unwrap(), 1);
        assert!(manifest.range_length(1).is_err());
        assert!(manifest.range_length(12).is_err());
        manifest.chunk_bytes = 0;
        assert!(manifest.validate().is_err());
        manifest.length = MAX_RANGES + 1;
        manifest.chunk_bytes = 1;
        assert!(manifest.validate().is_err());
    }
}
