//! Staged encoding for migration batches that exceed one HTTP request.

use std::{collections::HashSet, io::SeekFrom};

use axum::body::Bytes;
use axum::extract::{DefaultBodyLimit, Path, Query, State};
use axum::http::StatusCode;
use axum::routing::{get, post, put};
use axum::{Json, Router};
use chrono::Utc;
use orch8_types::continuity::ContinuityId;
use orch8_types::error::StorageError;
use orch8_types::ids::{InstanceId, SequenceId, TenantId};
use orch8_types::instance::InstanceState;
use orch8_types::migration::{
    MIGRATION_CHUNK_BYTES, MIGRATION_MAX_UPLOAD_BYTES, MigrationImportRequest,
    MigrationImportResponse, MigrationUploadCommit, StagedMigrationImportReceipt,
    target_runtime_id,
};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use tokio::io::{AsyncReadExt as _, AsyncSeekExt as _, AsyncWrite, AsyncWriteExt as _};
use uuid::Uuid;

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
        .route("/migrations/uploads/v1/{digest}/preview", get(preview))
        .route("/migrations/uploads/v1/{digest}/receipt", get(read_receipt))
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
    validate_upload_tenant(&request, &tenant)?;
    let storage = state.storage.clone();
    let response = crate::migrations::import(State(state), auth, Json(request)).await?;
    let receipt = StagedMigrationImportReceipt {
        tenant_id: tenant,
        digest,
        length: commit.length,
        completed_at: Utc::now(),
        result: response.0.clone(),
    };
    match storage.complete_migration_upload(&receipt).await {
        Ok(()) => {}
        Err(StorageError::Unsupported(_)) => {
            if let Err(error) = storage
                .clear_migration_chunks(&receipt.tenant_id, &receipt.digest)
                .await
            {
                tracing::warn!(%error, "could not clear completed migration upload");
            }
        }
        Err(error) => return Err(ApiError::from_storage(error, "staged migration receipt")),
    }
    Ok(response)
}

/// Read the completed result of an exact staged import after its chunks are cleared.
#[utoipa::path(
    get, path = "/migrations/uploads/v1/{digest}/receipt", tag = "migrations",
    operation_id = "read_staged_migration_import_receipt",
    params(
        ("digest" = String, Path, description = "SHA-256 of the staged bytes"),
        ("tenant_id" = String, Query, description = "Upload tenant"),
        ("length" = u64, Query, description = "Exact staged byte length")
    ),
    responses(
        (status = 200, description = "The first completed result for the exact upload", body = StagedMigrationImportReceipt),
        (status = 400, description = "Invalid upload coordinates"),
        (status = 404, description = "No completed receipt matches this tenant, digest and length")
    )
)]
pub(crate) async fn read_receipt(
    State(state): State<AppState>,
    auth: OptionalTenant,
    Path(digest): Path<String>,
    Query(upload): Query<MigrationUploadCommit>,
) -> Result<Json<StagedMigrationImportReceipt>, ApiError> {
    if !valid_digest(&digest) || !(1..=MIGRATION_MAX_UPLOAD_BYTES).contains(&upload.length) {
        return Err(ApiError::InvalidArgument(
            "invalid migration upload digest or length".into(),
        ));
    }
    let tenant = enforce_tenant_create(&auth, &upload.tenant_id)?;
    let receipt = state
        .storage
        .get_migration_upload_receipt(&tenant, &digest)
        .await
        .map_err(|error| ApiError::from_storage(error, "staged migration receipt"))?
        .filter(|receipt| receipt.length == upload.length)
        .ok_or_else(|| ApiError::NotFound("staged migration receipt".into()))?;
    if receipt.tenant_id != tenant
        || receipt.digest != digest
        || receipt.result.target_runtime_id != target_runtime_id(receipt.result.migration_id)
    {
        return Err(ApiError::Internal(
            "invalid staged migration receipt binding".into(),
        ));
    }
    Ok(Json(receipt))
}

fn validate_upload_tenant(
    request: &MigrationImportRequest,
    tenant: &TenantId,
) -> Result<(), ApiError> {
    if request
        .sequences
        .iter()
        .any(|sequence| &sequence.tenant_id != tenant)
        || request
            .instances
            .iter()
            .any(|entry| &entry.instance.tenant_id != tenant)
    {
        return Err(ApiError::Forbidden(
            "migration batch does not belong to the upload tenant".into(),
        ));
    }
    Ok(())
}

#[derive(Serialize, utoipa::ToSchema)]
pub(crate) struct StagedMigrationExecutionPreview {
    instance_id: InstanceId,
    continuity_id: ContinuityId,
    sequence_id: SequenceId,
    target_epoch: u64,
    target_state: InstanceState,
}

#[derive(Serialize, utoipa::ToSchema)]
pub(crate) struct StagedMigrationImportPreview {
    tenant_id: TenantId,
    digest: String,
    length: usize,
    migration_id: Uuid,
    source_engine_id: String,
    target_runtime_id: Uuid,
    sequence_ids: Vec<SequenceId>,
    instances: Vec<StagedMigrationExecutionPreview>,
}

/// Inspect the import manifest of a staged batch without importing it.
#[utoipa::path(
    get, path = "/migrations/uploads/v1/{digest}/preview", tag = "migrations",
    params(
        ("digest" = String, Path, description = "SHA-256 of the staged bytes"),
        ("tenant_id" = String, Query, description = "Upload tenant"),
        ("length" = u64, Query, description = "Exact staged byte length")
    ),
    responses(
        (status = 200, description = "Validated, tenant-scoped import manifest", body = StagedMigrationImportPreview),
        (status = 400, description = "Invalid migration batch or upload coordinates"),
        (status = 403, description = "Batch contains records from another tenant"),
        (status = 409, description = "Staged bytes missing or inconsistent")
    )
)]
pub(crate) async fn preview(
    State(state): State<AppState>,
    auth: OptionalTenant,
    Path(digest): Path<String>,
    Query(upload): Query<MigrationUploadCommit>,
) -> Result<Json<StagedMigrationImportPreview>, ApiError> {
    if !valid_digest(&digest) || !(1..=MIGRATION_MAX_UPLOAD_BYTES).contains(&upload.length) {
        return Err(ApiError::InvalidArgument(
            "invalid migration upload digest or length".into(),
        ));
    }
    let tenant = enforce_tenant_create(&auth, &upload.tenant_id)?;
    let request = assemble(&state, &tenant, &digest, upload.length).await?;
    validate_upload_tenant(&request, &tenant)?;
    crate::migrations::validate_import_request(&auth, &request)?;

    let mut seen_sequences = HashSet::new();
    let mut sequence_ids = Vec::with_capacity(request.sequences.len());
    for sequence in &request.sequences {
        if !seen_sequences.insert(sequence.id) {
            return Err(ApiError::InvalidArgument(
                "duplicate migration sequence".into(),
            ));
        }
        sequence
            .validate()
            .map_err(|_| ApiError::InvalidArgument("invalid migration sequence".into()))?;
        sequence_ids.push(sequence.id);
    }
    let mut seen_instances = HashSet::new();
    let mut seen_continuity = HashSet::new();
    let mut instances = Vec::with_capacity(request.instances.len());
    for entry in &request.instances {
        if !seen_instances.insert(entry.instance.id)
            || !seen_continuity.insert(entry.continuity.continuity_id)
        {
            return Err(ApiError::InvalidArgument(
                "duplicate migration execution".into(),
            ));
        }
        instances.push(StagedMigrationExecutionPreview {
            instance_id: entry.instance.id,
            continuity_id: entry.continuity.continuity_id,
            sequence_id: entry.instance.sequence_id,
            target_epoch: entry
                .continuity
                .epoch
                .checked_next()
                .map_err(|error| ApiError::InvalidArgument(error.to_string()))?
                .get(),
            target_state: crate::migrations::imported_state(entry.instance.state),
        });
    }
    Ok(Json(StagedMigrationImportPreview {
        tenant_id: tenant,
        digest,
        length: upload.length,
        migration_id: request.migration_id,
        source_engine_id: request.source_engine_id,
        target_runtime_id: target_runtime_id(request.migration_id),
        sequence_ids,
        instances,
    }))
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
