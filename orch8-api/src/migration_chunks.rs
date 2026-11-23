//! Staged encoding for migration batches that exceed one HTTP request.

use std::io::SeekFrom;

use axum::body::Bytes;
use axum::extract::{DefaultBodyLimit, Path, Query, State};
use axum::http::StatusCode;
use axum::routing::{get, post, put};
use axum::{Json, Router};
use orch8_types::ids::TenantId;
use orch8_types::migration::{
    MIGRATION_CHUNK_BYTES, MIGRATION_MAX_UPLOAD_BYTES, MigrationImportRequest,
    MigrationImportResponse, MigrationUploadCommit,
};
use serde::Deserialize;
use sha2::{Digest, Sha256};
use tokio::io::{AsyncReadExt as _, AsyncSeekExt as _, AsyncWrite, AsyncWriteExt as _};

use crate::AppState;
use crate::auth::{OptionalTenant, enforce_tenant_create};
use crate::error::ApiError;

pub(crate) fn routes() -> Router<AppState> {
    Router::new()
        .route(
            "/migrations/uploads/v1/{digest}/chunks/{index}",
            put(stage).layer(DefaultBodyLimit::max(MIGRATION_CHUNK_BYTES)),
        )
        .route("/migrations/uploads/v1/{digest}/commit", post(commit))
        .route("/migrations/uploads/v1/{digest}/verify", get(verify))
}

#[derive(Deserialize)]
struct UploadScope {
    tenant_id: TenantId,
}

pub(crate) fn valid_digest(digest: &str) -> bool {
    digest.len() == 64
        && digest
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
}

async fn stage(
    State(state): State<AppState>,
    auth: OptionalTenant,
    Path((digest, index)): Path<(String, u32)>,
    Query(scope): Query<UploadScope>,
    bytes: Bytes,
) -> Result<StatusCode, ApiError> {
    if !valid_digest(&digest) || index >= 64 || bytes.is_empty() {
        return Err(ApiError::InvalidArgument("invalid migration chunk".into()));
    }
    let tenant = enforce_tenant_create(&auth, &scope.tenant_id)?;
    let storage = state.storage.as_ref();
    let inserted = storage
        .stage_migration_chunk(&tenant, &digest, index, &bytes)
        .await
        .map_err(|error| ApiError::from_storage(error, "migration chunk"))?;
    if !inserted {
        let existing = storage
            .read_migration_chunk(&tenant, &digest, index)
            .await
            .map_err(|error| ApiError::from_storage(error, "migration chunk"))?;
        if existing.as_deref() != Some(bytes.as_ref()) {
            return Err(ApiError::Conflict(
                "migration chunk already contains different bytes".into(),
            ));
        }
    }
    Ok(StatusCode::NO_CONTENT)
}

async fn commit(
    State(state): State<AppState>,
    auth: OptionalTenant,
    Path(digest): Path<String>,
    Json(commit): Json<MigrationUploadCommit>,
) -> Result<Json<MigrationImportResponse>, ApiError> {
    if !valid_digest(&digest) || !(1..=MIGRATION_MAX_UPLOAD_BYTES).contains(&commit.length) {
        return Err(ApiError::InvalidArgument(
            "invalid migration upload digest or length".into(),
        ));
    }
    let tenant = enforce_tenant_create(&auth, &commit.tenant_id)?;
    let request = assemble(&state, &tenant, &digest, commit.length).await?;
    if request
        .sequences
        .iter()
        .any(|sequence| sequence.tenant_id != tenant)
        || request
            .instances
            .iter()
            .any(|entry| entry.instance.tenant_id != tenant)
    {
        return Err(ApiError::Forbidden(
            "migration batch does not belong to the upload tenant".into(),
        ));
    }
    let storage = state.storage.clone();
    let response = crate::migrations::import(State(state), auth, Json(request)).await?;
    if let Err(error) = storage.clear_migration_chunks(&tenant, &digest).await {
        tracing::warn!(%error, "could not clear completed migration upload");
    }
    Ok(response)
}

async fn assemble(
    state: &AppState,
    tenant: &TenantId,
    digest: &str,
    length: usize,
) -> Result<MigrationImportRequest, ApiError> {
    let mut file =
        tokio::fs::File::from_std(tempfile::tempfile().map_err(|error| disk_error(&error))?);
    if !copy_verified(state.storage.as_ref(), tenant, digest, length, &mut file).await? {
        return Err(ApiError::Conflict(
            "one or more migration chunks are missing".into(),
        ));
    }
    file.flush().await.map_err(|error| disk_error(&error))?;
    file.seek(SeekFrom::Start(0))
        .await
        .map_err(|error| disk_error(&error))?;
    let file = file.into_std().await;
    tokio::task::spawn_blocking(move || serde_json::from_reader(std::io::BufReader::new(file)))
        .await
        .map_err(|error| ApiError::Internal(format!("decode migration upload: {error}")))?
        .map_err(|error| ApiError::InvalidArgument(format!("invalid migration upload: {error}")))
}

fn disk_error(error: &std::io::Error) -> ApiError {
    ApiError::Internal(format!("stage migration import: {error}"))
}

async fn verify(
    State(state): State<AppState>,
    auth: OptionalTenant,
    Path(digest): Path<String>,
    Query(upload): Query<MigrationUploadCommit>,
) -> Result<StatusCode, ApiError> {
    if !valid_digest(&digest) || upload.length > MIGRATION_MAX_UPLOAD_BYTES {
        return Err(ApiError::InvalidArgument(
            "invalid migration upload digest or length".into(),
        ));
    }
    let tenant = enforce_tenant_create(&auth, &upload.tenant_id)?;
    let complete = copy_verified(
        state.storage.as_ref(),
        &tenant,
        &digest,
        upload.length,
        &mut tokio::io::sink(),
    )
    .await?;
    Ok(if complete {
        StatusCode::NO_CONTENT
    } else {
        StatusCode::NOT_FOUND
    })
}

pub(crate) async fn read_verified_artifact(
    storage: &dyn orch8_storage::StorageBackend,
    tenant: &TenantId,
    artifact: &orch8_types::migration::MigrationArtifact,
) -> Result<Bytes, ApiError> {
    let mut file =
        tokio::fs::File::from_std(tempfile::tempfile().map_err(|error| disk_error(&error))?);
    if !copy_verified(
        storage,
        tenant,
        &artifact.sha256,
        artifact.length,
        &mut file,
    )
    .await?
    {
        return Err(ApiError::Conflict(
            "migration artifact chunks are missing".into(),
        ));
    }
    file.flush().await.map_err(|error| disk_error(&error))?;
    file.seek(SeekFrom::Start(0))
        .await
        .map_err(|error| disk_error(&error))?;
    let mut bytes = Vec::with_capacity(artifact.length);
    file.read_to_end(&mut bytes)
        .await
        .map_err(|error| disk_error(&error))?;
    Ok(bytes.into())
}

async fn copy_verified(
    storage: &dyn orch8_storage::StorageBackend,
    tenant: &TenantId,
    digest: &str,
    length: usize,
    output: &mut (impl AsyncWrite + Unpin),
) -> Result<bool, ApiError> {
    let mut hash = Sha256::new();
    for (index, offset) in (0..length).step_by(MIGRATION_CHUNK_BYTES).enumerate() {
        let index = u32::try_from(index).expect("bounded migration chunk index");
        let Some(part) = storage
            .read_migration_chunk(tenant, digest, index)
            .await
            .map_err(|error| ApiError::from_storage(error, "migration chunk"))?
        else {
            return Ok(false);
        };
        if part.len() != MIGRATION_CHUNK_BYTES.min(length - offset) {
            return Err(ApiError::Conflict(format!(
                "migration chunk {index} has the wrong size"
            )));
        }
        hash.update(&part);
        output
            .write_all(&part)
            .await
            .map_err(|error| disk_error(&error))?;
    }
    let actual = orch8_types::plugin::WasmArtifactDigest::from_sha256(hash.finalize().into());
    if actual.as_str() != digest {
        return Err(ApiError::Conflict(
            "migration upload SHA-256 mismatch".into(),
        ));
    }
    Ok(true)
}
