//! Portable-continuity control-plane endpoints.

use std::fmt::Write as _;

use axum::extract::{Path, Query, State};
use axum::http::StatusCode;
use axum::response::IntoResponse;
use axum::routing::{get, post};
use axum::{Json, Router};
use base64::{Engine as _, engine::general_purpose::STANDARD as BASE64};
use chrono::{Duration, Utc};
use orch8_publisher::capsule::SignedCapsuleManifest;
use orch8_publisher::grant::{
    SignedContinuationGrant, sign_continuation_grant, verify_signed_continuation_grant,
};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use orch8_engine::continuity::{CompatibilityFinding, assess_compatibility};
use orch8_types::continuity::{
    CapsuleRequirements, ContinuationGrant, ContinuationGrantId, ContinuationGrantState,
    ContinuityExecution, ContinuityId, ContinuityStream, DataClassification, EffectReceipt,
    EffectState, ExecutionEpoch, ExecutionHandoff, GrantAction, HandoffId, HandoffState,
    LocalityPolicy, OwnershipState, PlacementDecision, RuntimeCapabilities, RuntimeId,
    RuntimeTrustLevel, StreamFrame, StreamFrameState, StreamId,
};
use orch8_types::ids::{InstanceId, TenantId};

use crate::AppState;
use crate::error::ApiError;

pub fn routes() -> Router<AppState> {
    Router::new()
        .route("/continuity/executions", post(create_execution))
        .route("/continuity/executions/{id}", get(get_execution))
        .route(
            "/continuity/executions/{id}/handoff-preview",
            post(handoff_preview),
        )
        .route("/continuity/handoffs", post(create_handoff))
        .route("/continuity/handoffs/{id}", get(get_handoff))
        .route("/continuity/handoffs/{id}/export", post(export_handoff))
        .route("/continuity/handoffs/{id}/accept", post(accept_handoff))
        .route("/continuity/handoffs/{id}/reject", post(reject_handoff))
        .route("/continuity/handoffs/{id}/resume", post(resume_handoff))
        .route("/continuity/handoffs/{id}/revoke", post(revoke_handoff))
        .route("/continuity/capsules/import", post(import_capsule))
        .route("/continuity/grants", post(issue_continuation_grant))
        .route(
            "/continuity/grants/consume",
            post(consume_continuation_grant),
        )
        .route("/continuity/executions/{id}/effects", get(list_effects))
        .route("/continuity/effects/{id}/resolve", post(resolve_effect))
        .route(
            "/continuity/executions/{id}/provenance",
            get(list_provenance),
        )
        .route(
            "/continuity/executions/{id}/provenance/verify",
            get(verify_provenance),
        )
        .route("/runtimes/register", post(register_runtime))
        .route("/runtimes", get(list_runtimes))
        .route(
            "/continuity/executions/{id}/placement",
            post(choose_placement),
        )
        .route("/continuity/streams", post(create_stream))
        .route(
            "/continuity/streams/{id}/frames",
            get(list_stream_frames).post(append_stream_frame),
        )
        .route(
            "/continuity/streams/{id}/retract",
            post(retract_stream_frames),
        )
}

#[derive(Debug, Deserialize)]
struct TenantQuery {
    tenant_id: String,
}

fn query_tenant(
    tenant_ctx: &crate::auth::OptionalTenant,
    value: &str,
) -> Result<TenantId, ApiError> {
    let requested = TenantId::new(value).map_err(ApiError::InvalidArgument)?;
    crate::auth::enforce_tenant_create(tenant_ctx, &requested)
}

fn hex_sha256(bytes: &[u8]) -> String {
    let digest = Sha256::digest(bytes);
    let mut encoded = String::with_capacity(64);
    for byte in digest {
        write!(&mut encoded, "{byte:02x}").expect("writing to a String cannot fail");
    }
    encoded
}

#[derive(Debug, Deserialize)]
struct IssueGrantRequest {
    tenant_id: TenantId,
    continuity_id: ContinuityId,
    destination_runtime_id: RuntimeId,
    #[serde(default)]
    subject: Option<String>,
    allowed_actions: Vec<GrantAction>,
    ttl_seconds: u32,
}

#[derive(Debug, Serialize)]
struct IssueGrantResponse {
    signed_grant: SignedContinuationGrant,
    token: String,
}

async fn issue_continuation_grant(
    State(state): State<AppState>,
    tenant_ctx: crate::auth::OptionalTenant,
    Json(body): Json<IssueGrantRequest>,
) -> Result<impl IntoResponse, ApiError> {
    let tenant_id = crate::auth::enforce_tenant_create(&tenant_ctx, &body.tenant_id)?;
    if !(1..=86_400).contains(&body.ttl_seconds) {
        return Err(ApiError::InvalidArgument(
            "grant ttl_seconds must be between 1 and 86400".into(),
        ));
    }
    if body.allowed_actions.is_empty() || body.allowed_actions.len() > 3 {
        return Err(ApiError::InvalidArgument(
            "a grant requires between one and three actions".into(),
        ));
    }
    let mut unique_actions = body.allowed_actions.clone();
    unique_actions.sort_by_key(|action| *action as u8);
    unique_actions.dedup();
    if unique_actions.len() != body.allowed_actions.len() {
        return Err(ApiError::InvalidArgument(
            "grant actions must not contain duplicates".into(),
        ));
    }
    if body.subject.as_ref().is_some_and(|value| value.len() > 128) {
        return Err(ApiError::InvalidArgument(
            "grant subject must not exceed 128 bytes".into(),
        ));
    }
    let crypto = state.continuity_crypto.as_ref().ok_or_else(|| {
        ApiError::Unavailable(
            "continuation grants are disabled without a configured engine encryption key".into(),
        )
    })?;
    let execution = state
        .storage
        .get_continuity_execution(&tenant_id, body.continuity_id)
        .await
        .map_err(|error| ApiError::from_storage(error, "continuity execution"))?
        .ok_or_else(|| ApiError::NotFound("continuity execution".into()))?;
    let now = Utc::now();
    let destination_is_known = state
        .storage
        .list_runtime_capabilities(&tenant_id, now, 10_000)
        .await
        .map_err(|error| ApiError::from_storage(error, "runtime capabilities"))?
        .iter()
        .any(|runtime| runtime.runtime_id == body.destination_runtime_id);
    if !destination_is_known {
        return Err(ApiError::Conflict(
            "grant destination must have a live capability registration".into(),
        ));
    }
    let token_bytes: [u8; 32] = rand::random();
    let token = BASE64.encode(token_bytes);
    let grant = ContinuationGrant {
        id: ContinuationGrantId::new(),
        tenant_id,
        continuity_id: body.continuity_id,
        expected_epoch: execution.epoch,
        destination_runtime_id: body.destination_runtime_id,
        subject: body.subject,
        allowed_actions: body.allowed_actions,
        nonce_sha256: hex_sha256(&token_bytes),
        state: ContinuationGrantState::Active,
        issued_at: now,
        expires_at: now + Duration::seconds(i64::from(body.ttl_seconds)),
        consumed_at: None,
        signing_key_id: crypto.signing_key_id.clone(),
    };
    let signed_grant = sign_continuation_grant(grant.clone(), &crypto.signing_key)
        .map_err(|error| ApiError::Internal(error.to_string()))?;
    state
        .storage
        .create_continuation_grant(&grant)
        .await
        .map_err(|error| ApiError::from_storage(error, "continuation grant"))?;
    Ok((
        StatusCode::CREATED,
        Json(IssueGrantResponse {
            signed_grant,
            token,
        }),
    ))
}

#[derive(Debug, Deserialize)]
struct ConsumeGrantRequest {
    tenant_id: TenantId,
    action: GrantAction,
    token: String,
    signed_grant: SignedContinuationGrant,
}

async fn consume_continuation_grant(
    State(state): State<AppState>,
    tenant_ctx: crate::auth::OptionalTenant,
    Json(body): Json<ConsumeGrantRequest>,
) -> Result<Json<ContinuationGrant>, ApiError> {
    let tenant_id = crate::auth::enforce_tenant_create(&tenant_ctx, &body.tenant_id)?;
    let crypto = state.continuity_crypto.as_ref().ok_or_else(|| {
        ApiError::Unavailable(
            "continuation grants are disabled without a configured engine encryption key".into(),
        )
    })?;
    let trusted_key = BASE64.encode(crypto.signing_key.verifying_key().to_bytes());
    verify_signed_continuation_grant(&body.signed_grant, &[trusted_key])
        .map_err(|error| ApiError::Conflict(error.to_string()))?;
    let grant = &body.signed_grant.grant;
    grant
        .validate_claim(
            Utc::now(),
            &tenant_id,
            grant.continuity_id,
            grant.expected_epoch,
            grant.destination_runtime_id,
            body.action,
        )
        .map_err(|error| ApiError::Conflict(error.to_string()))?;
    let token = BASE64
        .decode(&body.token)
        .map_err(|_| ApiError::InvalidArgument("grant token is not valid base64".into()))?;
    if token.len() != 32 {
        return Err(ApiError::InvalidArgument(
            "grant token must decode to 32 bytes".into(),
        ));
    }
    let now = Utc::now();
    let consumed = state
        .storage
        .consume_continuation_grant(&tenant_id, grant.id, &hex_sha256(&token), now)
        .await
        .map_err(|error| ApiError::from_storage(error, "continuation grant"))?;
    if !consumed {
        return Err(ApiError::Conflict(
            "continuation grant is expired, revoked, invalid, or already consumed".into(),
        ));
    }
    let mut result = grant.clone();
    result.state = ContinuationGrantState::Consumed;
    result.consumed_at = Some(now);
    Ok(Json(result))
}

#[derive(Debug, Deserialize)]
#[allow(clippy::struct_field_names)] // wire names are explicit domain identifiers
struct CreateExecutionRequest {
    tenant_id: TenantId,
    instance_id: InstanceId,
    runtime_id: RuntimeId,
}

async fn create_execution(
    State(state): State<AppState>,
    tenant_ctx: crate::auth::OptionalTenant,
    Json(body): Json<CreateExecutionRequest>,
) -> Result<impl IntoResponse, ApiError> {
    let tenant_id = crate::auth::enforce_tenant_create(&tenant_ctx, &body.tenant_id)?;
    let instance = state
        .storage
        .get_instance(body.instance_id)
        .await
        .map_err(|error| ApiError::from_storage(error, "instance"))?
        .ok_or_else(|| ApiError::NotFound(format!("instance {}", body.instance_id)))?;
    crate::auth::enforce_tenant_access(&tenant_ctx, &instance.tenant_id, "instance")?;
    if instance.tenant_id != tenant_id {
        return Err(ApiError::NotFound("instance".into()));
    }
    let execution = ContinuityExecution {
        continuity_id: ContinuityId::new(),
        tenant_id,
        current_instance_id: body.instance_id,
        owner_runtime_id: body.runtime_id,
        epoch: ExecutionEpoch::initial(),
        state: OwnershipState::Owned,
        updated_at: Utc::now(),
    };
    state
        .storage
        .create_continuity_execution(&execution)
        .await
        .map_err(|error| ApiError::from_storage(error, "continuity execution"))?;
    Ok((StatusCode::CREATED, Json(execution)))
}

async fn get_execution(
    State(state): State<AppState>,
    tenant_ctx: crate::auth::OptionalTenant,
    Path(id): Path<ContinuityId>,
    Query(query): Query<TenantQuery>,
) -> Result<Json<ContinuityExecution>, ApiError> {
    let tenant_id = query_tenant(&tenant_ctx, &query.tenant_id)?;
    let execution = state
        .storage
        .get_continuity_execution(&tenant_id, id)
        .await
        .map_err(|error| ApiError::from_storage(error, "continuity execution"))?
        .ok_or_else(|| ApiError::NotFound(format!("continuity execution {id}")))?;
    Ok(Json(execution))
}

#[derive(Debug, Deserialize)]
struct RuntimeRegistrationRequest {
    tenant_id: TenantId,
    capabilities: RuntimeCapabilities,
}

async fn register_runtime(
    State(state): State<AppState>,
    tenant_ctx: crate::auth::OptionalTenant,
    Json(body): Json<RuntimeRegistrationRequest>,
) -> Result<impl IntoResponse, ApiError> {
    let tenant_id = crate::auth::enforce_tenant_create(&tenant_ctx, &body.tenant_id)?;
    let now = Utc::now();
    if body.capabilities.expires_at <= now {
        return Err(ApiError::InvalidArgument(
            "runtime capability expiry must be in the future".into(),
        ));
    }
    if body.capabilities.observed_at > now + Duration::seconds(30) {
        return Err(ApiError::InvalidArgument(
            "runtime capability observation is too far in the future".into(),
        ));
    }
    if body.capabilities.expires_at <= body.capabilities.observed_at
        || body.capabilities.expires_at - body.capabilities.observed_at > Duration::minutes(5)
    {
        return Err(ApiError::InvalidArgument(
            "runtime capability lifetime must be positive and no longer than five minutes".into(),
        ));
    }
    if body.capabilities.trust > RuntimeTrustLevel::Registered {
        return Err(ApiError::InvalidArgument(
            "signed or attested runtime trust requires a verified attestation flow".into(),
        ));
    }
    state
        .storage
        .upsert_runtime_capabilities(&tenant_id, &body.capabilities)
        .await
        .map_err(|error| ApiError::from_storage(error, "runtime capabilities"))?;
    Ok((StatusCode::CREATED, Json(body.capabilities)))
}

async fn list_runtimes(
    State(state): State<AppState>,
    tenant_ctx: crate::auth::OptionalTenant,
    Query(query): Query<TenantQuery>,
) -> Result<Json<Vec<RuntimeCapabilities>>, ApiError> {
    let tenant_id = query_tenant(&tenant_ctx, &query.tenant_id)?;
    let runtimes = state
        .storage
        .list_runtime_capabilities(&tenant_id, Utc::now(), 1_000)
        .await
        .map_err(|error| ApiError::from_storage(error, "runtime capabilities"))?;
    Ok(Json(runtimes))
}

#[derive(Debug, Deserialize)]
struct HandoffPreviewRequest {
    tenant_id: TenantId,
    destination_runtime_id: RuntimeId,
    #[serde(default)]
    requirements: CapsuleRequirements,
}

#[derive(Debug, Serialize)]
struct HandoffPreviewResponse {
    continuity_id: ContinuityId,
    source_runtime_id: RuntimeId,
    destination_runtime_id: RuntimeId,
    compatible: bool,
    findings: Vec<CompatibilityFinding>,
    unresolved_effects: Vec<EffectReceipt>,
    preview_sha256: String,
}

#[derive(Serialize)]
struct HandoffPreviewEvidence<'a> {
    continuity_id: ContinuityId,
    epoch: ExecutionEpoch,
    source_runtime_id: RuntimeId,
    destination: &'a RuntimeCapabilities,
    requirements: &'a CapsuleRequirements,
    compatible: bool,
    findings: &'a [CompatibilityFinding],
    unresolved_effects: &'a [EffectReceipt],
}

async fn build_handoff_preview(
    state: &AppState,
    tenant_id: &TenantId,
    continuity_id: ContinuityId,
    destination_runtime_id: RuntimeId,
    requirements: &CapsuleRequirements,
) -> Result<HandoffPreviewResponse, ApiError> {
    let execution = state
        .storage
        .get_continuity_execution(tenant_id, continuity_id)
        .await
        .map_err(|error| ApiError::from_storage(error, "continuity execution"))?
        .ok_or_else(|| ApiError::NotFound(format!("continuity execution {continuity_id}")))?;
    let runtimes = state
        .storage
        .list_runtime_capabilities(tenant_id, Utc::now(), 1_000)
        .await
        .map_err(|error| ApiError::from_storage(error, "runtime capabilities"))?;
    let destination = runtimes
        .iter()
        .find(|runtime| runtime.runtime_id == destination_runtime_id)
        .ok_or_else(|| ApiError::NotFound("destination runtime".into()))?;
    let findings = assess_compatibility(requirements, destination, Utc::now());
    let unresolved_effects: Vec<_> = state
        .storage
        .list_effect_receipts(tenant_id, continuity_id, 10_000)
        .await
        .map_err(|error| ApiError::from_storage(error, "effect receipts"))?
        .into_iter()
        .filter(|receipt| !receipt.state.is_resolved())
        .collect();
    let compatible = findings
        .iter()
        .all(|finding| finding.status != orch8_engine::continuity::CompatibilityStatus::Fail)
        && unresolved_effects.is_empty();
    let evidence = HandoffPreviewEvidence {
        continuity_id,
        epoch: execution.epoch,
        source_runtime_id: execution.owner_runtime_id,
        destination,
        requirements,
        compatible,
        findings: &findings,
        unresolved_effects: &unresolved_effects,
    };
    let encoded = serde_json::to_vec(&evidence)
        .map_err(|error| ApiError::Internal(format!("serialize handoff preview: {error}")))?;
    let digest = Sha256::digest(encoded);
    let mut preview_sha256 = String::with_capacity(digest.len() * 2);
    for byte in digest {
        write!(&mut preview_sha256, "{byte:02x}").expect("writing to a String cannot fail");
    }

    Ok(HandoffPreviewResponse {
        continuity_id,
        source_runtime_id: execution.owner_runtime_id,
        destination_runtime_id,
        compatible,
        findings,
        unresolved_effects,
        preview_sha256,
    })
}

async fn handoff_preview(
    State(state): State<AppState>,
    tenant_ctx: crate::auth::OptionalTenant,
    Path(id): Path<ContinuityId>,
    Json(body): Json<HandoffPreviewRequest>,
) -> Result<Json<HandoffPreviewResponse>, ApiError> {
    let tenant_id = crate::auth::enforce_tenant_create(&tenant_ctx, &body.tenant_id)?;
    build_handoff_preview(
        &state,
        &tenant_id,
        id,
        body.destination_runtime_id,
        &body.requirements,
    )
    .await
    .map(Json)
}

#[derive(Debug, Deserialize)]
struct CreateHandoffRequest {
    tenant_id: TenantId,
    continuity_id: ContinuityId,
    destination_runtime_id: RuntimeId,
    #[serde(default)]
    requirements: CapsuleRequirements,
    preview_sha256: String,
}

async fn create_handoff(
    State(state): State<AppState>,
    tenant_ctx: crate::auth::OptionalTenant,
    Json(body): Json<CreateHandoffRequest>,
) -> Result<impl IntoResponse, ApiError> {
    let tenant_id = crate::auth::enforce_tenant_create(&tenant_ctx, &body.tenant_id)?;
    if body.preview_sha256.len() != 64
        || !body
            .preview_sha256
            .bytes()
            .all(|byte| byte.is_ascii_hexdigit())
    {
        return Err(ApiError::InvalidArgument(
            "preview_sha256 must be a 64-character hexadecimal digest".into(),
        ));
    }
    let preview = build_handoff_preview(
        &state,
        &tenant_id,
        body.continuity_id,
        body.destination_runtime_id,
        &body.requirements,
    )
    .await?;
    if !preview.compatible {
        return Err(ApiError::Conflict(
            "handoff preview is incompatible or has unresolved effects".into(),
        ));
    }
    if !preview
        .preview_sha256
        .eq_ignore_ascii_case(&body.preview_sha256)
    {
        return Err(ApiError::Conflict(
            "handoff preview is stale or does not match the requested requirements".into(),
        ));
    }
    let execution = state
        .storage
        .get_continuity_execution(&tenant_id, body.continuity_id)
        .await
        .map_err(|error| ApiError::from_storage(error, "continuity execution"))?
        .ok_or_else(|| ApiError::NotFound("continuity execution".into()))?;
    let now = Utc::now();
    let handoff = ExecutionHandoff {
        id: HandoffId::new(),
        continuity_id: body.continuity_id,
        tenant_id,
        source_runtime_id: execution.owner_runtime_id,
        destination_runtime_id: body.destination_runtime_id,
        expected_epoch: execution.epoch,
        state: HandoffState::Requested,
        capsule_id: None,
        preview_sha256: body.preview_sha256,
        version: 0,
        failure_code: None,
        created_at: now,
        updated_at: now,
    };
    state
        .storage
        .create_handoff(&handoff)
        .await
        .map_err(|error| ApiError::from_storage(error, "handoff"))?;
    Ok((StatusCode::CREATED, Json(handoff)))
}

async fn get_handoff(
    State(state): State<AppState>,
    tenant_ctx: crate::auth::OptionalTenant,
    Path(id): Path<HandoffId>,
    Query(query): Query<TenantQuery>,
) -> Result<Json<ExecutionHandoff>, ApiError> {
    let tenant_id = query_tenant(&tenant_ctx, &query.tenant_id)?;
    let handoff = state
        .storage
        .get_handoff(&tenant_id, id)
        .await
        .map_err(|error| ApiError::from_storage(error, "handoff"))?
        .ok_or_else(|| ApiError::NotFound(format!("handoff {id}")))?;
    Ok(Json(handoff))
}

#[derive(Debug, Deserialize)]
struct ExportHandoffRequest {
    tenant_id: TenantId,
    #[serde(default)]
    requirements: CapsuleRequirements,
    #[serde(default = "default_capsule_ttl_seconds")]
    expires_in_seconds: u32,
}

const fn default_capsule_ttl_seconds() -> u32 {
    300
}

#[derive(Debug, Serialize)]
struct ExportHandoffResponse {
    handoff: ExecutionHandoff,
    capsule: SignedCapsuleManifest,
}

async fn mark_capsule_export_failed(
    state: &AppState,
    tenant_id: &TenantId,
    quiescing: &ExecutionHandoff,
    message: String,
) -> ApiError {
    let mut failed = quiescing.clone();
    failed.state = HandoffState::Failed;
    failed.failure_code = Some("CAPSULE_EXPORT_FAILED".into());
    failed.version = quiescing.version.saturating_add(1);
    failed.updated_at = Utc::now();
    let _ = state
        .storage
        .cas_handoff(
            tenant_id,
            quiescing.id,
            HandoffState::Quiescing,
            quiescing.version,
            &failed,
        )
        .await;
    ApiError::Conflict(message)
}

#[allow(clippy::too_many_lines)] // explicit phases mirror the audited transfer protocol
async fn export_handoff(
    State(state): State<AppState>,
    tenant_ctx: crate::auth::OptionalTenant,
    Path(id): Path<HandoffId>,
    Json(body): Json<ExportHandoffRequest>,
) -> Result<Json<ExportHandoffResponse>, ApiError> {
    let tenant_id = crate::auth::enforce_tenant_create(&tenant_ctx, &body.tenant_id)?;
    if !(1..=3_600).contains(&body.expires_in_seconds) {
        return Err(ApiError::InvalidArgument(
            "expires_in_seconds must be between 1 and 3600".into(),
        ));
    }
    let crypto = state.continuity_crypto.as_ref().ok_or_else(|| {
        ApiError::Unavailable(
            "capsule export is disabled without a configured engine encryption key".into(),
        )
    })?;
    let handoff = state
        .storage
        .get_handoff(&tenant_id, id)
        .await
        .map_err(|error| ApiError::from_storage(error, "handoff"))?
        .ok_or_else(|| ApiError::NotFound(format!("handoff {id}")))?;
    if handoff.state != HandoffState::Requested {
        return Err(ApiError::Conflict(
            "only a requested handoff can begin export".into(),
        ));
    }
    let execution = state
        .storage
        .get_continuity_execution(&tenant_id, handoff.continuity_id)
        .await
        .map_err(|error| ApiError::from_storage(error, "continuity execution"))?
        .ok_or_else(|| ApiError::NotFound("continuity execution".into()))?;
    let instance = state
        .storage
        .get_instance(execution.current_instance_id)
        .await
        .map_err(|error| ApiError::from_storage(error, "instance"))?
        .ok_or_else(|| ApiError::NotFound("source instance".into()))?;
    if !matches!(
        instance.state,
        orch8_types::instance::InstanceState::Paused
            | orch8_types::instance::InstanceState::Waiting
    ) {
        return Err(ApiError::Conflict(
            "source instance must be paused or durably waiting before export".into(),
        ));
    }
    let now = Utc::now();
    let mut quiescing = handoff.clone();
    quiescing.state = HandoffState::Quiescing;
    quiescing.version = handoff.version.saturating_add(1);
    quiescing.updated_at = now;
    if !state
        .storage
        .cas_handoff(
            &tenant_id,
            id,
            HandoffState::Requested,
            handoff.version,
            &quiescing,
        )
        .await
        .map_err(|error| ApiError::from_storage(error, "handoff"))?
    {
        return Err(ApiError::Conflict("handoff changed concurrently".into()));
    }
    let capsule_result = orch8_engine::capsule::export_paused_capsule(
        state.storage.as_ref(),
        orch8_engine::capsule::CapsuleExportRequest {
            continuity: execution.clone(),
            destination_runtime_id: Some(handoff.destination_runtime_id),
            requirements: body.requirements,
            expires_at: now + Duration::seconds(i64::from(body.expires_in_seconds)),
            signing_key_id: crypto.signing_key_id.clone(),
            encryption_key_id: crypto.encryption_key_id.clone(),
        },
        &crypto.signing_key,
        &crypto.payload_encryptor,
    )
    .await;
    let capsule = match capsule_result {
        Ok(capsule) => capsule,
        Err(error) => {
            return Err(mark_capsule_export_failed(
                &state,
                &tenant_id,
                &quiescing,
                error.to_string(),
            )
            .await);
        }
    };
    let mut exported = quiescing.clone();
    exported.state = HandoffState::Exported;
    exported.capsule_id = Some(capsule.manifest.capsule_id);
    exported.version = quiescing.version.saturating_add(1);
    exported.updated_at = Utc::now();
    let mut transferring = execution.clone();
    transferring.state = OwnershipState::Transferring;
    transferring.updated_at = exported.updated_at;
    orch8_engine::continuity::commit_handoff_export(
        state.storage.as_ref(),
        &quiescing,
        &exported,
        &execution,
        &transferring,
    )
    .await
    .map_err(|error| ApiError::Conflict(error.to_string()))?;
    Ok(Json(ExportHandoffResponse {
        handoff: exported,
        capsule,
    }))
}

#[derive(Debug, Deserialize)]
struct ImportCapsuleRequest {
    tenant_id: TenantId,
    destination_runtime_id: RuntimeId,
    expected_epoch: ExecutionEpoch,
    capsule: SignedCapsuleManifest,
}

#[derive(Debug, Serialize)]
struct ImportCapsuleResponse {
    instance_id: InstanceId,
    state: &'static str,
}

async fn import_capsule(
    State(state): State<AppState>,
    tenant_ctx: crate::auth::OptionalTenant,
    Json(body): Json<ImportCapsuleRequest>,
) -> Result<impl IntoResponse, ApiError> {
    let tenant_id = crate::auth::enforce_tenant_create(&tenant_ctx, &body.tenant_id)?;
    let crypto = state.continuity_crypto.as_ref().ok_or_else(|| {
        ApiError::Unavailable(
            "capsule import is disabled without a configured engine encryption key".into(),
        )
    })?;
    let trusted_key = BASE64.encode(crypto.signing_key.verifying_key().to_bytes());
    let (instance, _) = orch8_engine::capsule::verify_and_import_paused_capsule(
        state.storage.as_ref(),
        &body.capsule,
        orch8_engine::capsule::CapsuleImportRequest {
            tenant_id: &tenant_id,
            destination_runtime_id: body.destination_runtime_id,
            expected_epoch: body.expected_epoch,
            trusted_public_keys: &[trusted_key],
            now: Utc::now(),
        },
        &crypto.payload_encryptor,
    )
    .await
    .map_err(|error| ApiError::Conflict(error.to_string()))?;
    Ok((
        StatusCode::CREATED,
        Json(ImportCapsuleResponse {
            instance_id: instance.id,
            state: "paused",
        }),
    ))
}

#[derive(Debug, Deserialize)]
struct HandoffActionRequest {
    tenant_id: TenantId,
}

#[derive(Debug, Deserialize)]
struct AcceptHandoffRequest {
    tenant_id: TenantId,
    destination_instance_id: InstanceId,
}

#[derive(Debug, Serialize)]
struct AcceptHandoffResponse {
    handoff: ExecutionHandoff,
    execution: ContinuityExecution,
}

async fn accept_handoff(
    State(state): State<AppState>,
    tenant_ctx: crate::auth::OptionalTenant,
    Path(id): Path<HandoffId>,
    Json(body): Json<AcceptHandoffRequest>,
) -> Result<Json<AcceptHandoffResponse>, ApiError> {
    let tenant_id = crate::auth::enforce_tenant_create(&tenant_ctx, &body.tenant_id)?;
    let handoff = state
        .storage
        .get_handoff(&tenant_id, id)
        .await
        .map_err(|error| ApiError::from_storage(error, "handoff"))?
        .ok_or_else(|| ApiError::NotFound(format!("handoff {id}")))?;
    let destination = state
        .storage
        .get_instance(body.destination_instance_id)
        .await
        .map_err(|error| ApiError::from_storage(error, "destination instance"))?
        .ok_or_else(|| ApiError::NotFound("destination instance".into()))?;
    if destination.tenant_id != tenant_id
        || destination.state != orch8_types::instance::InstanceState::Paused
    {
        return Err(ApiError::Conflict(
            "destination instance must be tenant-owned and paused".into(),
        ));
    }
    let execution = state
        .storage
        .get_continuity_execution(&tenant_id, handoff.continuity_id)
        .await
        .map_err(|error| ApiError::from_storage(error, "continuity execution"))?
        .ok_or_else(|| ApiError::NotFound("continuity execution".into()))?;
    let mut accepted_handoff = handoff.clone();
    accepted_handoff.state = HandoffState::Accepted;
    accepted_handoff.version = handoff.version.saturating_add(1);
    accepted_handoff.updated_at = Utc::now();
    let mut accepted_execution = execution.clone();
    accepted_execution.current_instance_id = destination.id;
    accepted_execution.owner_runtime_id = handoff.destination_runtime_id;
    accepted_execution.epoch = execution
        .epoch
        .checked_next()
        .map_err(|error| ApiError::Conflict(error.to_string()))?;
    accepted_execution.state = OwnershipState::Owned;
    accepted_execution.updated_at = accepted_handoff.updated_at;
    orch8_engine::continuity::accept_handoff(
        state.storage.as_ref(),
        &handoff,
        &accepted_handoff,
        &execution,
        &accepted_execution,
    )
    .await
    .map_err(|error| ApiError::Conflict(error.to_string()))?;
    Ok(Json(AcceptHandoffResponse {
        handoff: accepted_handoff,
        execution: accepted_execution,
    }))
}

async fn transition_handoff_action(
    state: &AppState,
    tenant_id: &TenantId,
    id: HandoffId,
    expected: HandoffState,
    next: HandoffState,
) -> Result<ExecutionHandoff, ApiError> {
    let current = state
        .storage
        .get_handoff(tenant_id, id)
        .await
        .map_err(|error| ApiError::from_storage(error, "handoff"))?
        .ok_or_else(|| ApiError::NotFound(format!("handoff {id}")))?;
    if current.state != expected || !current.state.can_transition_to(next) {
        return Err(ApiError::Conflict(format!(
            "handoff must be {expected:?} before transition to {next:?}"
        )));
    }
    let mut updated = current.clone();
    updated.state = next;
    updated.version = current.version.saturating_add(1);
    updated.updated_at = Utc::now();
    if !state
        .storage
        .cas_handoff(tenant_id, id, expected, current.version, &updated)
        .await
        .map_err(|error| ApiError::from_storage(error, "handoff"))?
    {
        return Err(ApiError::Conflict("handoff changed concurrently".into()));
    }
    Ok(updated)
}

async fn reject_handoff(
    State(state): State<AppState>,
    tenant_ctx: crate::auth::OptionalTenant,
    Path(id): Path<HandoffId>,
    Json(body): Json<HandoffActionRequest>,
) -> Result<Json<ExecutionHandoff>, ApiError> {
    let tenant_id = crate::auth::enforce_tenant_create(&tenant_ctx, &body.tenant_id)?;
    transition_handoff_action(
        &state,
        &tenant_id,
        id,
        HandoffState::Requested,
        HandoffState::Rejected,
    )
    .await
    .map(Json)
}

async fn revoke_handoff(
    State(state): State<AppState>,
    tenant_ctx: crate::auth::OptionalTenant,
    Path(id): Path<HandoffId>,
    Json(body): Json<HandoffActionRequest>,
) -> Result<Json<ExecutionHandoff>, ApiError> {
    let tenant_id = crate::auth::enforce_tenant_create(&tenant_ctx, &body.tenant_id)?;
    transition_handoff_action(
        &state,
        &tenant_id,
        id,
        HandoffState::Exported,
        HandoffState::Revoked,
    )
    .await
    .map(Json)
}

async fn resume_handoff(
    State(state): State<AppState>,
    tenant_ctx: crate::auth::OptionalTenant,
    Path(id): Path<HandoffId>,
    Json(body): Json<HandoffActionRequest>,
) -> Result<Json<ExecutionHandoff>, ApiError> {
    let tenant_id = crate::auth::enforce_tenant_create(&tenant_ctx, &body.tenant_id)?;
    let handoff = state
        .storage
        .get_handoff(&tenant_id, id)
        .await
        .map_err(|error| ApiError::from_storage(error, "handoff"))?
        .ok_or_else(|| ApiError::NotFound(format!("handoff {id}")))?;
    if handoff.state != HandoffState::Accepted {
        return Err(ApiError::Conflict(
            "handoff must be accepted before resume".into(),
        ));
    }
    let execution = state
        .storage
        .get_continuity_execution(&tenant_id, handoff.continuity_id)
        .await
        .map_err(|error| ApiError::from_storage(error, "continuity execution"))?
        .ok_or_else(|| ApiError::NotFound("continuity execution".into()))?;
    if execution.owner_runtime_id != handoff.destination_runtime_id {
        return Err(ApiError::Conflict(
            "destination runtime does not own the accepted epoch".into(),
        ));
    }
    let mut resumed = handoff.clone();
    resumed.state = HandoffState::Resumed;
    resumed.version = handoff.version.saturating_add(1);
    resumed.updated_at = Utc::now();
    orch8_engine::continuity::resume_handoff(
        state.storage.as_ref(),
        &handoff,
        &resumed,
        execution.current_instance_id,
    )
    .await
    .map_err(|error| ApiError::Conflict(error.to_string()))?;
    Ok(Json(resumed))
}

async fn list_effects(
    State(state): State<AppState>,
    tenant_ctx: crate::auth::OptionalTenant,
    Path(id): Path<ContinuityId>,
    Query(query): Query<TenantQuery>,
) -> Result<Json<Vec<EffectReceipt>>, ApiError> {
    let tenant_id = query_tenant(&tenant_ctx, &query.tenant_id)?;
    let receipts = state
        .storage
        .list_effect_receipts(&tenant_id, id, 10_000)
        .await
        .map_err(|error| ApiError::from_storage(error, "effect receipts"))?;
    Ok(Json(receipts))
}

#[derive(Debug, Deserialize)]
struct ResolveEffectRequest {
    tenant_id: TenantId,
    state: EffectState,
    provider_receipt_id: Option<String>,
}

async fn resolve_effect(
    State(state): State<AppState>,
    tenant_ctx: crate::auth::OptionalTenant,
    Path(id): Path<orch8_types::continuity::EffectId>,
    Json(body): Json<ResolveEffectRequest>,
) -> Result<Json<EffectReceipt>, ApiError> {
    let tenant_id = crate::auth::enforce_tenant_create(&tenant_ctx, &body.tenant_id)?;
    let mut receipt = state
        .storage
        .get_effect_receipt(&tenant_id, id)
        .await
        .map_err(|error| ApiError::from_storage(error, "effect receipt"))?
        .ok_or_else(|| ApiError::NotFound(format!("effect receipt {id}")))?;
    let expected = receipt.state;
    receipt.provider_receipt_id = body.provider_receipt_id;
    receipt
        .transition(body.state, Utc::now())
        .map_err(|error| ApiError::Conflict(error.to_string()))?;
    if !state
        .storage
        .cas_effect_receipt(&tenant_id, id, expected, &receipt)
        .await
        .map_err(|error| ApiError::from_storage(error, "effect receipt"))?
    {
        return Err(ApiError::Conflict(
            "effect receipt changed concurrently; reload before resolving".into(),
        ));
    }
    Ok(Json(receipt))
}

async fn list_provenance(
    State(state): State<AppState>,
    tenant_ctx: crate::auth::OptionalTenant,
    Path(id): Path<ContinuityId>,
    Query(query): Query<TenantQuery>,
) -> Result<Json<Vec<orch8_types::continuity::ProvenanceEntry>>, ApiError> {
    let tenant_id = query_tenant(&tenant_ctx, &query.tenant_id)?;
    let entries = state
        .storage
        .list_provenance(&tenant_id, id, 10_000)
        .await
        .map_err(|error| ApiError::from_storage(error, "provenance"))?;
    Ok(Json(entries))
}

#[derive(Debug, Deserialize)]
struct VerifyProvenanceQuery {
    tenant_id: String,
    expected_head: Option<String>,
}

async fn verify_provenance(
    State(state): State<AppState>,
    tenant_ctx: crate::auth::OptionalTenant,
    Path(id): Path<ContinuityId>,
    Query(query): Query<VerifyProvenanceQuery>,
) -> Result<Json<orch8_engine::continuity::ProvenanceVerification>, ApiError> {
    let tenant_id = query_tenant(&tenant_ctx, &query.tenant_id)?;
    let entries = state
        .storage
        .list_provenance(&tenant_id, id, 10_000)
        .await
        .map_err(|error| ApiError::from_storage(error, "provenance"))?;
    Ok(Json(orch8_engine::continuity::verify_provenance_chain(
        &entries,
        query.expected_head.as_deref(),
    )))
}

#[derive(Debug, Deserialize)]
struct PlacementRequest {
    tenant_id: TenantId,
    #[serde(default)]
    requirements: CapsuleRequirements,
    policy: Option<LocalityPolicy>,
    #[serde(default = "default_classification")]
    classification: DataClassification,
}

const fn default_classification() -> DataClassification {
    DataClassification::Internal
}

async fn choose_placement(
    State(state): State<AppState>,
    tenant_ctx: crate::auth::OptionalTenant,
    Path(id): Path<ContinuityId>,
    Json(body): Json<PlacementRequest>,
) -> Result<Json<PlacementDecision>, ApiError> {
    let tenant_id = crate::auth::enforce_tenant_create(&tenant_ctx, &body.tenant_id)?;
    if let Some(policy) = &body.policy {
        orch8_engine::placement::validate_policy(policy)
            .map_err(|error| ApiError::InvalidArgument(error.to_string()))?;
    }
    let execution = state
        .storage
        .get_continuity_execution(&tenant_id, id)
        .await
        .map_err(|error| ApiError::from_storage(error, "continuity execution"))?
        .ok_or_else(|| ApiError::NotFound(format!("continuity execution {id}")))?;
    let now = Utc::now();
    let candidates = state
        .storage
        .list_runtime_capabilities(&tenant_id, now, 1_000)
        .await
        .map_err(|error| ApiError::from_storage(error, "runtime capabilities"))?;
    let decision = orch8_engine::placement::choose_runtime(
        tenant_id,
        id,
        execution.epoch,
        &body.requirements,
        body.policy.as_ref(),
        body.classification,
        &candidates,
        Some(execution.owner_runtime_id),
        now,
    );
    state
        .storage
        .save_placement_decision(&decision)
        .await
        .map_err(|error| ApiError::from_storage(error, "placement decision"))?;
    Ok(Json(decision))
}

#[derive(Debug, Deserialize)]
struct CreateStreamRequest {
    tenant_id: TenantId,
    continuity_id: ContinuityId,
    ttl_seconds: u32,
}

async fn create_stream(
    State(state): State<AppState>,
    tenant_ctx: crate::auth::OptionalTenant,
    Json(body): Json<CreateStreamRequest>,
) -> Result<impl IntoResponse, ApiError> {
    let tenant_id = crate::auth::enforce_tenant_create(&tenant_ctx, &body.tenant_id)?;
    if !(1..=86_400).contains(&body.ttl_seconds) {
        return Err(ApiError::InvalidArgument(
            "stream ttl_seconds must be between 1 and 86400".into(),
        ));
    }
    let execution = state
        .storage
        .get_continuity_execution(&tenant_id, body.continuity_id)
        .await
        .map_err(|error| ApiError::from_storage(error, "continuity execution"))?
        .ok_or_else(|| ApiError::NotFound("continuity execution".into()))?;
    let now = Utc::now();
    let stream = ContinuityStream {
        stream_id: StreamId::new(),
        tenant_id,
        continuity_id: body.continuity_id,
        epoch: execution.epoch,
        created_at: now,
        expires_at: now + Duration::seconds(i64::from(body.ttl_seconds)),
    };
    state
        .storage
        .create_continuity_stream(&stream)
        .await
        .map_err(|error| ApiError::from_storage(error, "continuity stream"))?;
    Ok((StatusCode::CREATED, Json(stream)))
}

#[derive(Debug, Deserialize)]
struct AppendStreamFrameRequest {
    tenant_id: TenantId,
    sequence: u64,
    checkpoint_sha256: String,
    payload: serde_json::Value,
}

async fn append_stream_frame(
    State(state): State<AppState>,
    tenant_ctx: crate::auth::OptionalTenant,
    Path(id): Path<StreamId>,
    Json(body): Json<AppendStreamFrameRequest>,
) -> Result<impl IntoResponse, ApiError> {
    let tenant_id = crate::auth::enforce_tenant_create(&tenant_ctx, &body.tenant_id)?;
    if body.checkpoint_sha256.len() != 64
        || !body
            .checkpoint_sha256
            .bytes()
            .all(|byte| byte.is_ascii_hexdigit())
    {
        return Err(ApiError::InvalidArgument(
            "checkpoint_sha256 must be a 64-character hexadecimal digest".into(),
        ));
    }
    let payload_bytes = serde_json::to_vec(&body.payload)
        .map_err(|error| ApiError::InvalidArgument(error.to_string()))?;
    if payload_bytes.len() > 64 * 1024 {
        return Err(ApiError::PayloadTooLarge(
            "stream frame payload exceeds 64 KiB".into(),
        ));
    }
    let stream = state
        .storage
        .get_continuity_stream(&tenant_id, id)
        .await
        .map_err(|error| ApiError::from_storage(error, "continuity stream"))?
        .ok_or_else(|| ApiError::NotFound(format!("continuity stream {id}")))?;
    let now = Utc::now();
    if stream.expires_at <= now {
        return Err(ApiError::Conflict("continuity stream has expired".into()));
    }
    let execution = state
        .storage
        .get_continuity_execution(&tenant_id, stream.continuity_id)
        .await
        .map_err(|error| ApiError::from_storage(error, "continuity execution"))?
        .ok_or_else(|| ApiError::NotFound("continuity execution".into()))?;
    if execution.epoch != stream.epoch {
        return Err(ApiError::Conflict(
            "stream belongs to a stale execution epoch".into(),
        ));
    }
    let frame = StreamFrame {
        stream_id: id,
        tenant_id,
        continuity_id: stream.continuity_id,
        epoch: stream.epoch,
        sequence: body.sequence,
        checkpoint_sha256: body.checkpoint_sha256,
        state: StreamFrameState::Committed,
        payload: body.payload,
        created_at: now,
        expires_at: stream.expires_at,
    };
    let appended = state
        .storage
        .append_stream_frame(&frame)
        .await
        .map_err(|error| ApiError::from_storage(error, "stream frame"))?;
    if !appended {
        return Err(ApiError::Conflict(
            "stream frame epoch, expiry, or sequence is stale".into(),
        ));
    }
    Ok((StatusCode::CREATED, Json(frame)))
}

#[derive(Debug, Deserialize)]
struct StreamFramesQuery {
    tenant_id: String,
    after_sequence: Option<u64>,
    limit: Option<u32>,
}

async fn list_stream_frames(
    State(state): State<AppState>,
    tenant_ctx: crate::auth::OptionalTenant,
    Path(id): Path<StreamId>,
    Query(query): Query<StreamFramesQuery>,
) -> Result<Json<Vec<StreamFrame>>, ApiError> {
    let tenant_id = query_tenant(&tenant_ctx, &query.tenant_id)?;
    if state
        .storage
        .get_continuity_stream(&tenant_id, id)
        .await
        .map_err(|error| ApiError::from_storage(error, "continuity stream"))?
        .is_none()
    {
        return Err(ApiError::NotFound(format!("continuity stream {id}")));
    }
    let frames = state
        .storage
        .list_stream_frames(
            &tenant_id,
            id,
            query.after_sequence,
            Utc::now(),
            query.limit.unwrap_or(1_000).min(10_000),
        )
        .await
        .map_err(|error| ApiError::from_storage(error, "stream frames"))?;
    Ok(Json(frames))
}

#[derive(Debug, Deserialize)]
struct RetractStreamRequest {
    tenant_id: TenantId,
    epoch: ExecutionEpoch,
    after_sequence: u64,
}

#[derive(Debug, Serialize)]
struct RetractStreamResponse {
    retracted: u64,
}

async fn retract_stream_frames(
    State(state): State<AppState>,
    tenant_ctx: crate::auth::OptionalTenant,
    Path(id): Path<StreamId>,
    Json(body): Json<RetractStreamRequest>,
) -> Result<Json<RetractStreamResponse>, ApiError> {
    let tenant_id = crate::auth::enforce_tenant_create(&tenant_ctx, &body.tenant_id)?;
    let stream = state
        .storage
        .get_continuity_stream(&tenant_id, id)
        .await
        .map_err(|error| ApiError::from_storage(error, "continuity stream"))?
        .ok_or_else(|| ApiError::NotFound(format!("continuity stream {id}")))?;
    if stream.epoch != body.epoch {
        return Err(ApiError::Conflict("stream epoch does not match".into()));
    }
    let retracted = state
        .storage
        .retract_stream_frames(&tenant_id, id, body.epoch, body.after_sequence)
        .await
        .map_err(|error| ApiError::from_storage(error, "stream frames"))?;
    Ok(Json(RetractStreamResponse { retracted }))
}
