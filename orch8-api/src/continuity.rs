//! Portable-continuity control-plane endpoints.

use std::fmt::Write as _;

use axum::extract::{Path, Query, State};
use axum::http::StatusCode;
use axum::response::IntoResponse;
use axum::routing::{get, post};
use axum::{Json, Router};
use chrono::{Duration, Utc};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use orch8_engine::continuity::{CompatibilityFinding, assess_compatibility};
use orch8_types::continuity::{
    CapsuleRequirements, ContinuityExecution, ContinuityId, EffectReceipt, EffectState,
    ExecutionEpoch, ExecutionHandoff, HandoffId, HandoffState, OwnershipState, RuntimeCapabilities,
    RuntimeId, RuntimeTrustLevel,
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
        .route("/continuity/executions/{id}/effects", get(list_effects))
        .route("/continuity/effects/{id}/resolve", post(resolve_effect))
        .route(
            "/continuity/executions/{id}/provenance",
            get(list_provenance),
        )
        .route("/runtimes/register", post(register_runtime))
        .route("/runtimes", get(list_runtimes))
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

#[derive(Debug, Deserialize)]
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
