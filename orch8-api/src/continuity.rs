//! Portable-continuity control-plane endpoints.

use std::fmt::Write as _;
use std::sync::Arc;

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
use zeroize::{Zeroize, Zeroizing};

use orch8_engine::continuity::{CompatibilityFinding, assess_compatibility};
use orch8_types::continuity::{
    CapsuleRequirements, ContinuationGrant, ContinuationGrantId, ContinuationGrantState,
    ContinuityExecution, ContinuityId, ContinuityStream, DataClassification, EffectReceipt,
    EffectState, ExecutionEpoch, ExecutionHandoff, GrantAction, HandoffId, HandoffState,
    LocalityPolicy, OwnershipState, PlacementDecision, PlacementDecisionId, PlacementEvidence,
    PolicyOutcome, RuntimeCapabilities, RuntimeId, RuntimeTrustLevel, StreamFrame,
    StreamFrameState, StreamId,
};
use orch8_types::continuity_advanced::{
    AttentionState, AttentionTask, AttentionTaskId, BudgetReservation, BudgetReservationId,
    CheckpointBoundary, DeviceDelegation, EvaluationId, EvaluationScore, ExtractedEffectMock,
    ExtractedTestFixture, FaultInjection, FaultKind, FederationEnvelope, FederationPeer,
    ForkEffectMode, GeneratedScenario, InvariantId, InvariantResult, InvariantRule,
    LiveMigrationPlan, MigrationDisposition, MigrationPlanId, ProviderCandidate, ReservationState,
    ResidencyEvidence, ReviewerCapabilities, ScenarioId, StateTransform, WhatIfScenario,
    WorkflowInvariant,
};
use orch8_types::ids::{InstanceId, SequenceId, TenantId};

use crate::AppState;
use crate::error::ApiError;

#[allow(clippy::too_many_lines)] // one declarative map of the continuity HTTP surface
pub fn routes() -> Router<AppState> {
    Router::new()
        .route("/continuity/executions", post(create_execution))
        .route("/continuity/executions/{id}", get(get_execution))
        .route("/continuity/executions/{id}/locations", get(list_locations))
        .route(
            "/continuity/executions/{id}/handoff-preview",
            post(handoff_preview),
        )
        .route("/continuity/handoffs", post(create_handoff))
        .route("/continuity/handoffs/{id}", get(get_handoff))
        .route("/continuity/handoffs/{id}/export", post(export_handoff))
        .route(
            "/continuity/handoffs/{id}/attach-device-capsule",
            post(attach_device_capsule),
        )
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
        .route(
            "/continuity/invariants",
            get(list_invariants).post(create_invariant),
        )
        .route(
            "/continuity/executions/{id}/invariants/evaluate",
            post(evaluate_invariants),
        )
        .route(
            "/continuity/executions/{id}/invariants/results",
            get(list_invariant_results),
        )
        .route(
            "/continuity/executions/{id}/evaluations",
            get(list_evaluations).post(append_evaluation),
        )
        .route(
            "/continuity/executions/{id}/budget-reservations",
            post(reserve_execution_budget),
        )
        .route("/continuity/attention", post(create_attention_task))
        .route(
            "/continuity/attention/{id}/assign",
            post(assign_attention_task),
        )
        .route(
            "/continuity/executions/{id}/checkpoints",
            get(list_continuity_checkpoints),
        )
        .route(
            "/continuity/executions/{id}/checkpoints/{checkpoint_id}",
            get(get_continuity_checkpoint),
        )
        .route("/continuity/executions/{id}/what-if", post(run_what_if))
        .route(
            "/continuity/executions/{id}/test-fixture",
            post(extract_test_fixture),
        )
        .route("/continuity/migrations/plan", post(plan_live_migration))
        .route("/continuity/scenarios/generate", post(generate_scenarios))
        .route("/continuity/scenarios/reproduce", post(reproduce_incident))
        .route("/continuity/providers/choose", post(choose_provider))
        .route(
            "/continuity/optimizations/recommend",
            post(recommend_optimizations),
        )
        .route("/continuity/evaluations/gate", post(evaluate_gate))
        .route("/continuity/residency/evaluate", post(evaluate_residency))
        .route("/continuity/disclosure/minimize", post(minimize_disclosure))
        .route("/continuity/federation/verify", post(verify_federation))
        .route("/continuity/delegations/claim", post(claim_delegation))
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
    if let Some(existing) = state
        .storage
        .get_continuity_execution_by_instance(&tenant_id, body.instance_id)
        .await
        .map_err(|error| ApiError::from_storage(error, "continuity execution"))?
    {
        return Err(ApiError::Conflict(format!(
            "instance already belongs to continuity execution {}",
            existing.continuity_id
        )));
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

async fn list_locations(
    State(state): State<AppState>,
    tenant_ctx: crate::auth::OptionalTenant,
    Path(id): Path<ContinuityId>,
    Query(query): Query<TenantQuery>,
) -> Result<Json<Vec<orch8_types::continuity::ContinuityLocation>>, ApiError> {
    let tenant_id = query_tenant(&tenant_ctx, &query.tenant_id)?;
    let locations = state
        .storage
        .list_continuity_locations(&tenant_id, id, 10_000)
        .await
        .map_err(|error| ApiError::from_storage(error, "continuity locations"))?;
    if locations.is_empty() {
        let exists = state
            .storage
            .get_continuity_execution(&tenant_id, id)
            .await
            .map_err(|error| ApiError::from_storage(error, "continuity execution"))?
            .is_some();
        if !exists {
            return Err(ApiError::NotFound(format!("continuity execution {id}")));
        }
    }
    Ok(Json(locations))
}

#[derive(Debug, Deserialize)]
struct RuntimeRegistrationRequest {
    tenant_id: TenantId,
    capabilities: RuntimeCapabilities,
}

const MAX_RUNTIME_FACTS_PER_KIND: usize = 256;
const MAX_RUNTIME_FACT_LENGTH: usize = 256;

fn validate_runtime_facts(capabilities: &RuntimeCapabilities) -> Result<(), ApiError> {
    let fact_groups = [
        capabilities.handlers.as_slice(),
        capabilities.plugins.as_slice(),
        capabilities.credentials.as_slice(),
        capabilities.regions.as_slice(),
        capabilities.hardware.as_slice(),
    ];
    if fact_groups
        .iter()
        .any(|facts| facts.len() > MAX_RUNTIME_FACTS_PER_KIND)
    {
        return Err(ApiError::InvalidArgument(format!(
            "runtime capability lists may contain at most {MAX_RUNTIME_FACTS_PER_KIND} facts"
        )));
    }
    if fact_groups.iter().any(|facts| {
        facts
            .iter()
            .any(|fact| fact.trim().is_empty() || fact.len() > MAX_RUNTIME_FACT_LENGTH)
    }) {
        return Err(ApiError::InvalidArgument(format!(
            "runtime capability facts must be non-empty and at most {MAX_RUNTIME_FACT_LENGTH} bytes"
        )));
    }
    Ok(())
}

async fn register_runtime(
    State(state): State<AppState>,
    tenant_ctx: crate::auth::OptionalTenant,
    Json(body): Json<RuntimeRegistrationRequest>,
) -> Result<impl IntoResponse, ApiError> {
    let tenant_id = crate::auth::enforce_tenant_create(&tenant_ctx, &body.tenant_id)?;
    validate_runtime_facts(&body.capabilities)?;
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
    if body
        .capabilities
        .battery_percent
        .is_some_and(|percentage| percentage > 100)
    {
        return Err(ApiError::InvalidArgument(
            "runtime battery_percent must be at most 100".into(),
        ));
    }
    if body.capabilities.trust > RuntimeTrustLevel::Registered {
        return Err(ApiError::InvalidArgument(
            "signed or attested runtime trust requires a verified attestation flow".into(),
        ));
    }
    if let Some(public_key) = &body.capabilities.capsule_signing_public_key {
        let bytes: [u8; 32] = BASE64
            .decode(public_key)
            .map_err(|_| {
                ApiError::InvalidArgument("capsule signing public key is not valid base64".into())
            })?
            .try_into()
            .map_err(|_| {
                ApiError::InvalidArgument(
                    "capsule signing public key must decode to 32 bytes".into(),
                )
            })?;
        ed25519_dalek::VerifyingKey::from_bytes(&bytes).map_err(|_| {
            ApiError::InvalidArgument("capsule signing public key is invalid".into())
        })?;
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
    policy: Option<LocalityPolicy>,
    #[serde(default = "default_classification")]
    classification: DataClassification,
}

#[derive(Debug, Serialize)]
struct HandoffPreviewResponse {
    continuity_id: ContinuityId,
    source_runtime_id: RuntimeId,
    destination_runtime_id: RuntimeId,
    compatible: bool,
    findings: Vec<CompatibilityFinding>,
    unresolved_effects: Vec<EffectReceipt>,
    placement_decision: PlacementDecision,
    preview_sha256: String,
}

#[derive(Serialize)]
struct HandoffPreviewEvidence<'a> {
    continuity_id: ContinuityId,
    epoch: ExecutionEpoch,
    source_runtime_id: RuntimeId,
    destination: &'a RuntimeCapabilities,
    requirements: &'a CapsuleRequirements,
    policy: Option<&'a LocalityPolicy>,
    classification: DataClassification,
    selected_runtime_id: Option<RuntimeId>,
    placement_candidates: &'a [PlacementEvidence],
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
    policy: Option<&LocalityPolicy>,
    classification: DataClassification,
) -> Result<HandoffPreviewResponse, ApiError> {
    orch8_engine::placement::validate_requirements(requirements)
        .map_err(|error| ApiError::InvalidArgument(error.to_string()))?;
    if let Some(policy) = policy {
        orch8_engine::placement::validate_policy(policy)
            .map_err(|error| ApiError::InvalidArgument(error.to_string()))?;
    }
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
    let mut placement_decision = orch8_engine::placement::choose_runtime(
        tenant_id.clone(),
        continuity_id,
        execution.epoch,
        requirements,
        policy,
        classification,
        &runtimes,
        Some(execution.owner_runtime_id),
        Utc::now(),
    );
    let unresolved_effects: Vec<_> = state
        .storage
        .list_effect_receipts(tenant_id, continuity_id, 10_000)
        .await
        .map_err(|error| ApiError::from_storage(error, "effect receipts"))?
        .into_iter()
        .filter(|receipt| !receipt.state.is_resolved())
        .collect();
    let destination_allowed = placement_decision.candidates.iter().any(|candidate| {
        candidate.runtime_id == destination_runtime_id && candidate.outcome == PolicyOutcome::Allow
    });
    // An explicit handoff destination overrides soft score preferences (for
    // example, staying on the current runtime), but never hard capability or
    // locality outcomes. The persisted decision records that explicit choice.
    if destination_allowed {
        placement_decision.selected_runtime_id = Some(destination_runtime_id);
    }
    let compatible = destination_allowed
        && findings
            .iter()
            .all(|finding| finding.status != orch8_engine::continuity::CompatibilityStatus::Fail)
        && unresolved_effects.is_empty();
    let evidence = HandoffPreviewEvidence {
        continuity_id,
        epoch: execution.epoch,
        source_runtime_id: execution.owner_runtime_id,
        destination,
        requirements,
        policy,
        classification,
        selected_runtime_id: placement_decision.selected_runtime_id,
        placement_candidates: &placement_decision.candidates,
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
        placement_decision,
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
    let preview = build_handoff_preview(
        &state,
        &tenant_id,
        id,
        body.destination_runtime_id,
        &body.requirements,
        body.policy.as_ref(),
        body.classification,
    )
    .await?;
    state
        .storage
        .save_placement_decision(&preview.placement_decision)
        .await
        .map_err(|error| ApiError::from_storage(error, "placement decision"))?;
    Ok(Json(preview))
}

#[derive(Debug, Deserialize)]
struct CreateHandoffRequest {
    tenant_id: TenantId,
    continuity_id: ContinuityId,
    destination_runtime_id: RuntimeId,
    #[serde(default)]
    requirements: CapsuleRequirements,
    policy: Option<LocalityPolicy>,
    #[serde(default = "default_classification")]
    classification: DataClassification,
    placement_decision_id: PlacementDecisionId,
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
    let authorized = state
        .storage
        .get_placement_decision(&tenant_id, body.placement_decision_id)
        .await
        .map_err(|error| ApiError::from_storage(error, "placement decision"))?
        .ok_or_else(|| ApiError::NotFound("placement decision".into()))?;
    let preview = build_handoff_preview(
        &state,
        &tenant_id,
        body.continuity_id,
        body.destination_runtime_id,
        &body.requirements,
        body.policy.as_ref(),
        body.classification,
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
    if authorized.continuity_id != body.continuity_id
        || authorized.tenant_id != tenant_id
        || authorized.epoch != preview.placement_decision.epoch
        || authorized.selected_runtime_id != Some(body.destination_runtime_id)
        || preview.placement_decision.selected_runtime_id != Some(body.destination_runtime_id)
        || authorized.requirements != body.requirements
        || authorized.policy != body.policy
        || authorized.classification != body.classification
        || authorized.policy_version != preview.placement_decision.policy_version
        || authorized.candidates != preview.placement_decision.candidates
    {
        return Err(ApiError::Conflict(
            "placement decision is stale or does not authorize this handoff".into(),
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
        placement_decision_id: Some(body.placement_decision_id),
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

#[derive(Deserialize)]
struct ExportHandoffRequest {
    tenant_id: TenantId,
    #[serde(default)]
    requirements: CapsuleRequirements,
    #[serde(default = "default_capsule_ttl_seconds")]
    expires_in_seconds: u32,
    /// Destination-generated, single-transfer AES-256 key. Keeping this key
    /// out of server configuration lets an isolated runtime decrypt the
    /// transported payload without receiving the engine master key.
    payload_key_base64: Option<TransferPayloadKey>,
}

#[derive(Deserialize)]
#[serde(transparent)]
struct TransferPayloadKey(String);

impl TransferPayloadKey {
    fn expose(&self) -> &str {
        &self.0
    }
}

impl Drop for TransferPayloadKey {
    fn drop(&mut self) {
        self.0.zeroize();
    }
}

const fn default_capsule_ttl_seconds() -> u32 {
    300
}

#[derive(Debug, Serialize)]
struct ExportHandoffResponse {
    handoff: ExecutionHandoff,
    capsule: SignedCapsuleManifest,
    /// Encrypted capsule artifact transported independently of object storage.
    payload_base64: String,
}

fn transfer_payload_encryptor(
    encoded: &str,
) -> Result<(orch8_types::encryption::FieldEncryptor, String), ApiError> {
    let decoded = Zeroizing::new(
        BASE64
            .decode(encoded)
            .map_err(|_| ApiError::InvalidArgument("payload key is not valid base64".into()))?,
    );
    let key: &[u8; 32] = decoded.as_slice().try_into().map_err(|_| {
        ApiError::InvalidArgument("payload key must decode to exactly 32 bytes".into())
    })?;
    Ok((
        orch8_types::encryption::FieldEncryptor::from_bytes(key),
        format!("continuity-transfer-{}", &hex_sha256(key)[..16]),
    ))
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
    let transfer_crypto = body
        .payload_key_base64
        .as_ref()
        .map(TransferPayloadKey::expose)
        .map(transfer_payload_encryptor)
        .transpose()?;
    let (payload_encryptor, encryption_key_id) = transfer_crypto.as_ref().map_or_else(
        || (&crypto.payload_encryptor, crypto.encryption_key_id.clone()),
        |(encryptor, key_id)| (encryptor, key_id.clone()),
    );
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
    let placement_id = handoff.placement_decision_id.ok_or_else(|| {
        ApiError::Conflict("handoff is missing dispatch placement evidence".into())
    })?;
    let authorized = state
        .storage
        .get_placement_decision(&tenant_id, placement_id)
        .await
        .map_err(|error| ApiError::from_storage(error, "placement decision"))?
        .ok_or_else(|| ApiError::Conflict("handoff placement evidence no longer exists".into()))?;
    if authorized.requirements != body.requirements
        || authorized.continuity_id != handoff.continuity_id
        || authorized.epoch != execution.epoch
        || authorized.selected_runtime_id != Some(handoff.destination_runtime_id)
    {
        return Err(ApiError::Conflict(
            "handoff requirements or ownership no longer match placement evidence".into(),
        ));
    }
    let placement_now = Utc::now();
    let candidates = state
        .storage
        .list_runtime_capabilities(&tenant_id, placement_now, 1_000)
        .await
        .map_err(|error| ApiError::from_storage(error, "runtime capabilities"))?;
    let current = orch8_engine::placement::choose_runtime(
        tenant_id.clone(),
        handoff.continuity_id,
        execution.epoch,
        &authorized.requirements,
        authorized.policy.as_ref(),
        authorized.classification,
        &candidates,
        Some(execution.owner_runtime_id),
        placement_now,
    );
    let destination_allowed = current.candidates.iter().any(|candidate| {
        candidate.runtime_id == handoff.destination_runtime_id
            && candidate.outcome == PolicyOutcome::Allow
    });
    if !destination_allowed {
        return Err(ApiError::Conflict(
            "runtime capabilities or locality policy changed before dispatch".into(),
        ));
    }
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
            encryption_key_id,
        },
        &crypto.signing_key,
        payload_encryptor,
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
    let payload_result = state
        .storage
        .get_artifact(&capsule.manifest.payload_artifact.key)
        .await;
    let payload = match payload_result {
        Ok(Some(payload)) => payload,
        Ok(None) => {
            return Err(mark_capsule_export_failed(
                &state,
                &tenant_id,
                &quiescing,
                "exported capsule payload is unavailable".into(),
            )
            .await);
        }
        Err(error) => {
            return Err(mark_capsule_export_failed(
                &state,
                &tenant_id,
                &quiescing,
                format!("cannot read exported capsule payload: {error}"),
            )
            .await);
        }
    };
    if payload.len() as u64 != capsule.manifest.payload_artifact.bytes
        || hex_sha256(&payload) != capsule.manifest.payload_artifact.sha256
    {
        return Err(mark_capsule_export_failed(
            &state,
            &tenant_id,
            &quiescing,
            "exported capsule payload does not match its signed manifest".into(),
        )
        .await);
    }
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
        payload_base64: BASE64.encode(payload),
    }))
}

#[derive(Deserialize)]
struct ImportCapsuleRequest {
    tenant_id: TenantId,
    destination_runtime_id: RuntimeId,
    expected_epoch: ExecutionEpoch,
    destination_instance_id: Option<InstanceId>,
    capsule: SignedCapsuleManifest,
    payload_base64: Option<String>,
    payload_key_base64: Option<TransferPayloadKey>,
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
    let transfer_crypto = body
        .payload_key_base64
        .as_ref()
        .map(TransferPayloadKey::expose)
        .map(transfer_payload_encryptor)
        .transpose()?;
    let payload_encryptor = transfer_crypto
        .as_ref()
        .map_or(&crypto.payload_encryptor, |(encryptor, _)| encryptor);
    let trusted_key = BASE64.encode(crypto.signing_key.verifying_key().to_bytes());
    let request = orch8_engine::capsule::CapsuleImportRequest {
        tenant_id: &tenant_id,
        destination_runtime_id: body.destination_runtime_id,
        destination_instance_id: body.destination_instance_id,
        expected_epoch: body.expected_epoch,
        trusted_public_keys: &[trusted_key],
        now: Utc::now(),
    };
    let imported = if let Some(encoded) = body.payload_base64 {
        let declared_bytes = usize::try_from(body.capsule.manifest.payload_artifact.bytes)
            .map_err(|_| ApiError::PayloadTooLarge("capsule payload size is unsupported".into()))?;
        let max_sealed_bytes = orch8_types::continuity::CapsulePayload::MAX_ENCODED_BYTES + 64;
        let max_base64_bytes = declared_bytes.saturating_add(2) / 3 * 4;
        if declared_bytes > max_sealed_bytes || encoded.len() > max_base64_bytes {
            return Err(ApiError::PayloadTooLarge(
                "transported capsule payload exceeds protocol bounds".into(),
            ));
        }
        let sealed = BASE64
            .decode(encoded)
            .map_err(|_| ApiError::InvalidArgument("capsule payload is not valid base64".into()))?;
        orch8_engine::capsule::verify_and_import_paused_capsule_bytes(
            state.storage.as_ref(),
            &body.capsule,
            &sealed,
            request,
            payload_encryptor,
        )
        .await
    } else {
        orch8_engine::capsule::verify_and_import_paused_capsule(
            state.storage.as_ref(),
            &body.capsule,
            request,
            payload_encryptor,
        )
        .await
    };
    let (instance, _) = imported.map_err(|error| ApiError::Conflict(error.to_string()))?;
    Ok((
        StatusCode::CREATED,
        Json(ImportCapsuleResponse {
            instance_id: instance.id,
            state: "paused",
        }),
    ))
}

#[derive(Deserialize)]
struct AttachDeviceCapsuleRequest {
    tenant_id: TenantId,
    destination_instance_id: InstanceId,
    capsule: SignedCapsuleManifest,
    payload_base64: String,
    payload_key_base64: TransferPayloadKey,
}

#[derive(Debug, Serialize)]
struct AttachDeviceCapsuleResponse {
    handoff: ExecutionHandoff,
    destination_instance_id: InstanceId,
}

#[allow(clippy::too_many_lines)] // audited external-runtime transfer phases stay explicit
async fn attach_device_capsule(
    State(state): State<AppState>,
    tenant_ctx: crate::auth::OptionalTenant,
    Path(id): Path<HandoffId>,
    Json(body): Json<AttachDeviceCapsuleRequest>,
) -> Result<Json<AttachDeviceCapsuleResponse>, ApiError> {
    let tenant_id = crate::auth::enforce_tenant_create(&tenant_ctx, &body.tenant_id)?;
    let handoff = state
        .storage
        .get_handoff(&tenant_id, id)
        .await
        .map_err(|error| ApiError::from_storage(error, "handoff"))?
        .ok_or_else(|| ApiError::NotFound(format!("handoff {id}")))?;
    if handoff.state == HandoffState::Exported
        && handoff.capsule_id == Some(body.capsule.manifest.capsule_id)
        && state
            .storage
            .is_capsule_import_instance(
                &tenant_id,
                body.capsule.manifest.capsule_id,
                handoff.destination_runtime_id,
                body.destination_instance_id,
            )
            .await
            .map_err(|error| ApiError::from_storage(error, "capsule import"))?
    {
        return Ok(Json(AttachDeviceCapsuleResponse {
            handoff,
            destination_instance_id: body.destination_instance_id,
        }));
    }
    if !matches!(
        handoff.state,
        HandoffState::Requested | HandoffState::Quiescing
    ) {
        return Err(ApiError::Conflict(
            "only a requested or recovering handoff can attach a device capsule".into(),
        ));
    }
    if handoff.state == HandoffState::Quiescing
        && handoff.capsule_id != Some(body.capsule.manifest.capsule_id)
    {
        return Err(ApiError::Conflict(
            "recovering handoff is bound to another device capsule".into(),
        ));
    }
    let execution = state
        .storage
        .get_continuity_execution(&tenant_id, handoff.continuity_id)
        .await
        .map_err(|error| ApiError::from_storage(error, "continuity execution"))?
        .ok_or_else(|| ApiError::NotFound("continuity execution".into()))?;
    if execution.state != OwnershipState::Owned
        || execution.owner_runtime_id != handoff.source_runtime_id
        || execution.epoch != handoff.expected_epoch
    {
        return Err(ApiError::Conflict(
            "handoff source no longer owns the expected epoch".into(),
        ));
    }
    let now = Utc::now();
    let source_key = state
        .storage
        .list_runtime_capabilities(&tenant_id, now, 10_000)
        .await
        .map_err(|error| ApiError::from_storage(error, "runtime capabilities"))?
        .into_iter()
        .find(|runtime| runtime.runtime_id == handoff.source_runtime_id)
        .and_then(|runtime| runtime.capsule_signing_public_key)
        .ok_or_else(|| {
            ApiError::Conflict("source runtime has no live registered capsule signing key".into())
        })?;
    if body.capsule.public_key != source_key
        || body.capsule.manifest.continuity_id != handoff.continuity_id
        || body.capsule.manifest.source_runtime_id != handoff.source_runtime_id
        || body.capsule.manifest.allowed_destination_runtime_id
            != Some(handoff.destination_runtime_id)
    {
        return Err(ApiError::Conflict(
            "device capsule identity does not match the requested handoff".into(),
        ));
    }
    let declared_bytes = usize::try_from(body.capsule.manifest.payload_artifact.bytes)
        .map_err(|_| ApiError::PayloadTooLarge("capsule payload size is unsupported".into()))?;
    let max_sealed_bytes = orch8_types::continuity::CapsulePayload::MAX_ENCODED_BYTES + 64;
    let max_base64_bytes = declared_bytes.saturating_add(2) / 3 * 4;
    if declared_bytes > max_sealed_bytes || body.payload_base64.len() > max_base64_bytes {
        return Err(ApiError::PayloadTooLarge(
            "transported capsule payload exceeds protocol bounds".into(),
        ));
    }
    let sealed = BASE64
        .decode(&body.payload_base64)
        .map_err(|_| ApiError::InvalidArgument("capsule payload is not valid base64".into()))?;
    let (payload_encryptor, _) = transfer_payload_encryptor(body.payload_key_base64.expose())?;
    let quiescing = if handoff.state == HandoffState::Requested {
        let mut next = handoff.clone();
        next.state = HandoffState::Quiescing;
        next.capsule_id = Some(body.capsule.manifest.capsule_id);
        next.version = handoff.version.saturating_add(1);
        next.updated_at = now;
        if !state
            .storage
            .cas_handoff(
                &tenant_id,
                id,
                HandoffState::Requested,
                handoff.version,
                &next,
            )
            .await
            .map_err(|error| ApiError::from_storage(error, "handoff"))?
        {
            return Err(ApiError::Conflict("handoff changed concurrently".into()));
        }
        next
    } else {
        handoff.clone()
    };
    let import = orch8_engine::capsule::verify_and_import_paused_capsule_bytes(
        state.storage.as_ref(),
        &body.capsule,
        &sealed,
        orch8_engine::capsule::CapsuleImportRequest {
            tenant_id: &tenant_id,
            destination_runtime_id: handoff.destination_runtime_id,
            destination_instance_id: Some(body.destination_instance_id),
            expected_epoch: handoff.expected_epoch,
            trusted_public_keys: &[source_key],
            now,
        },
        &payload_encryptor,
    )
    .await;
    if let Err(error) = import {
        return Err(
            mark_capsule_export_failed(&state, &tenant_id, &quiescing, error.to_string()).await,
        );
    }
    let mut exported = quiescing.clone();
    exported.state = HandoffState::Exported;
    exported.capsule_id = Some(body.capsule.manifest.capsule_id);
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
    Ok(Json(AttachDeviceCapsuleResponse {
        handoff: exported,
        destination_instance_id: body.destination_instance_id,
    }))
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
    let trusted_keys =
        state
            .continuity_crypto
            .as_ref()
            .map_or_else(std::collections::BTreeMap::new, |crypto| {
                std::collections::BTreeMap::from([(
                    crypto.signing_key_id.clone(),
                    BASE64.encode(crypto.signing_key.verifying_key().to_bytes()),
                )])
            });
    Ok(Json(
        orch8_engine::continuity::verify_provenance_chain_with_keys(
            &entries,
            query.expected_head.as_deref(),
            &trusted_keys,
        ),
    ))
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
    orch8_engine::placement::validate_requirements(&body.requirements)
        .map_err(|error| ApiError::InvalidArgument(error.to_string()))?;
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

#[derive(Debug, Deserialize)]
struct CreateInvariantRequest {
    tenant_id: TenantId,
    sequence_id: SequenceId,
    sequence_version: Option<i32>,
    name: String,
    rule: InvariantRule,
    #[serde(default)]
    commit_guard: bool,
}

async fn create_invariant(
    State(state): State<AppState>,
    tenant_ctx: crate::auth::OptionalTenant,
    Json(body): Json<CreateInvariantRequest>,
) -> Result<impl IntoResponse, ApiError> {
    let tenant_id = crate::auth::enforce_tenant_create(&tenant_ctx, &body.tenant_id)?;
    if body.name.is_empty() || body.name.len() > 128 {
        return Err(ApiError::InvalidArgument(
            "invariant name must contain between 1 and 128 bytes".into(),
        ));
    }
    let sequence = state
        .storage
        .get_sequence(body.sequence_id)
        .await
        .map_err(|error| ApiError::from_storage(error, "sequence"))?
        .ok_or_else(|| ApiError::NotFound("sequence".into()))?;
    crate::auth::enforce_tenant_access(&tenant_ctx, &sequence.tenant_id, "sequence")?;
    if sequence.tenant_id != tenant_id {
        return Err(ApiError::NotFound("sequence".into()));
    }
    if let InvariantRule::TerminalStateIn { states } = &body.rule
        && (states.is_empty() || states.len() > 16)
    {
        return Err(ApiError::InvalidArgument(
            "terminal-state invariants require between 1 and 16 states".into(),
        ));
    }
    if body.commit_guard && !matches!(&body.rule, InvariantRule::EffectAtMostOnce { .. }) {
        return Err(ApiError::InvalidArgument(
            "commit_guard is supported only for effect_at_most_once invariants".into(),
        ));
    }
    let invariant = WorkflowInvariant {
        id: InvariantId::new(),
        tenant_id,
        sequence_id: body.sequence_id,
        sequence_version: body.sequence_version,
        name: body.name,
        rule: body.rule,
        commit_guard: body.commit_guard,
        enabled: true,
        created_at: Utc::now(),
    };
    state
        .storage
        .create_workflow_invariant(&invariant)
        .await
        .map_err(|error| ApiError::from_storage(error, "workflow invariant"))?;
    Ok((StatusCode::CREATED, Json(invariant)))
}

#[derive(Debug, Deserialize)]
struct InvariantQuery {
    tenant_id: String,
    sequence_id: SequenceId,
    sequence_version: i32,
    limit: Option<u32>,
}

async fn list_invariants(
    State(state): State<AppState>,
    tenant_ctx: crate::auth::OptionalTenant,
    Query(query): Query<InvariantQuery>,
) -> Result<Json<Vec<WorkflowInvariant>>, ApiError> {
    let tenant_id = query_tenant(&tenant_ctx, &query.tenant_id)?;
    let invariants = state
        .storage
        .list_workflow_invariants(
            &tenant_id,
            query.sequence_id,
            query.sequence_version,
            query.limit.unwrap_or(1_000).min(10_000),
        )
        .await
        .map_err(|error| ApiError::from_storage(error, "workflow invariants"))?;
    Ok(Json(invariants))
}

#[derive(Debug, Deserialize)]
struct EvaluateInvariantsRequest {
    tenant_id: TenantId,
}

async fn evaluate_invariants(
    State(state): State<AppState>,
    tenant_ctx: crate::auth::OptionalTenant,
    Path(id): Path<ContinuityId>,
    Json(body): Json<EvaluateInvariantsRequest>,
) -> Result<Json<Vec<InvariantResult>>, ApiError> {
    let tenant_id = crate::auth::enforce_tenant_create(&tenant_ctx, &body.tenant_id)?;
    let execution = state
        .storage
        .get_continuity_execution(&tenant_id, id)
        .await
        .map_err(|error| ApiError::from_storage(error, "continuity execution"))?
        .ok_or_else(|| ApiError::NotFound("continuity execution".into()))?;
    let instance = state
        .storage
        .get_instance(execution.current_instance_id)
        .await
        .map_err(|error| ApiError::from_storage(error, "instance"))?
        .ok_or_else(|| ApiError::NotFound("instance".into()))?;
    let sequence = state
        .storage
        .get_sequence(instance.sequence_id)
        .await
        .map_err(|error| ApiError::from_storage(error, "sequence"))?
        .ok_or_else(|| ApiError::NotFound("sequence".into()))?;
    let (invariants, receipts, outputs) = tokio::try_join!(
        state
            .storage
            .list_workflow_invariants(&tenant_id, sequence.id, sequence.version, 10_000,),
        state.storage.list_effect_receipts(&tenant_id, id, 10_000),
        state.storage.get_all_outputs(instance.id),
    )
    .map_err(|error| ApiError::from_storage(error, "invariant evidence"))?;
    let output_paths = collect_output_paths(&outputs);
    let terminal = instance
        .state
        .is_terminal()
        .then(|| instance.state.to_string());
    let evidence = orch8_engine::continuity_advanced::InvariantEvidence {
        receipts: &receipts,
        terminal_state: terminal.as_deref(),
        budget_breached: None,
        output_paths: &output_paths,
    };
    let now = Utc::now();
    let mut results = Vec::with_capacity(invariants.len());
    for invariant in invariants {
        let result = orch8_engine::continuity_advanced::evaluate_invariant(
            &invariant,
            id,
            execution.epoch,
            &evidence,
            now,
        );
        state
            .storage
            .append_invariant_result(&tenant_id, &result)
            .await
            .map_err(|error| ApiError::from_storage(error, "invariant result"))?;
        results.push(result);
    }
    Ok(Json(results))
}

async fn list_invariant_results(
    State(state): State<AppState>,
    tenant_ctx: crate::auth::OptionalTenant,
    Path(id): Path<ContinuityId>,
    Query(query): Query<TenantQuery>,
) -> Result<Json<Vec<InvariantResult>>, ApiError> {
    let tenant_id = query_tenant(&tenant_ctx, &query.tenant_id)?;
    state
        .storage
        .get_continuity_execution(&tenant_id, id)
        .await
        .map_err(|error| ApiError::from_storage(error, "continuity execution"))?
        .ok_or_else(|| ApiError::NotFound("continuity execution".into()))?;
    let results = state
        .storage
        .list_invariant_results(&tenant_id, id, 10_000)
        .await
        .map_err(|error| ApiError::from_storage(error, "invariant results"))?;
    Ok(Json(results))
}

fn collect_output_paths(
    outputs: &[orch8_types::output::BlockOutput],
) -> std::collections::BTreeSet<(orch8_types::ids::BlockId, String)> {
    let mut paths = std::collections::BTreeSet::new();
    for output in outputs.iter().take(10_000) {
        collect_json_paths(&output.block_id, &output.output, "", 0, &mut paths);
    }
    paths
}

fn collect_json_paths(
    block_id: &orch8_types::ids::BlockId,
    value: &serde_json::Value,
    prefix: &str,
    depth: u8,
    paths: &mut std::collections::BTreeSet<(orch8_types::ids::BlockId, String)>,
) {
    if depth >= 16 || paths.len() >= 100_000 {
        return;
    }
    if !prefix.is_empty() {
        paths.insert((block_id.clone(), prefix.to_owned()));
    }
    if let Some(object) = value.as_object() {
        for (key, child) in object.iter().take(1_000) {
            let next = if prefix.is_empty() {
                key.clone()
            } else {
                format!("{prefix}.{key}")
            };
            collect_json_paths(block_id, child, &next, depth + 1, paths);
        }
    }
}

#[derive(Debug, Deserialize)]
struct AppendEvaluationRequest {
    tenant_id: TenantId,
    evaluator: String,
    score_millipoints: i64,
    sample_size: u64,
    #[serde(default)]
    deferred: bool,
    evidence_sha256: String,
}

async fn append_evaluation(
    State(state): State<AppState>,
    tenant_ctx: crate::auth::OptionalTenant,
    Path(id): Path<ContinuityId>,
    Json(body): Json<AppendEvaluationRequest>,
) -> Result<impl IntoResponse, ApiError> {
    let tenant_id = crate::auth::enforce_tenant_create(&tenant_ctx, &body.tenant_id)?;
    if body.evaluator.is_empty() || body.evaluator.len() > 128 || body.sample_size == 0 {
        return Err(ApiError::InvalidArgument(
            "evaluation requires a bounded evaluator name and positive sample_size".into(),
        ));
    }
    validate_sha256(&body.evidence_sha256, "evidence_sha256")?;
    if state
        .storage
        .get_continuity_execution(&tenant_id, id)
        .await
        .map_err(|error| ApiError::from_storage(error, "continuity execution"))?
        .is_none()
    {
        return Err(ApiError::NotFound("continuity execution".into()));
    }
    let dedupe_key = hex_sha256(
        format!(
            "{}:{id}:{}:{}",
            tenant_id.as_str(),
            body.evaluator,
            body.evidence_sha256
        )
        .as_bytes(),
    );
    let score = EvaluationScore {
        id: EvaluationId::new(),
        tenant_id,
        continuity_id: id,
        evaluator: body.evaluator,
        score_millipoints: body.score_millipoints,
        sample_size: body.sample_size,
        deferred: body.deferred,
        dedupe_key,
        evidence_sha256: body.evidence_sha256,
        created_at: Utc::now(),
    };
    let inserted = state
        .storage
        .append_evaluation_score(&score)
        .await
        .map_err(|error| ApiError::from_storage(error, "evaluation score"))?;
    if !inserted {
        return Err(ApiError::Conflict("evaluation was already recorded".into()));
    }
    Ok((StatusCode::CREATED, Json(score)))
}

async fn list_evaluations(
    State(state): State<AppState>,
    tenant_ctx: crate::auth::OptionalTenant,
    Path(id): Path<ContinuityId>,
    Query(query): Query<TenantQuery>,
) -> Result<Json<Vec<EvaluationScore>>, ApiError> {
    let tenant_id = query_tenant(&tenant_ctx, &query.tenant_id)?;
    let scores = state
        .storage
        .list_evaluation_scores(&tenant_id, id, 10_000)
        .await
        .map_err(|error| ApiError::from_storage(error, "evaluation scores"))?;
    Ok(Json(scores))
}

#[derive(Debug, Deserialize)]
struct ReserveBudgetRequest {
    tenant_id: TenantId,
    requested: orch8_types::instance::BudgetUsage,
    estimation_version: String,
}

async fn reserve_execution_budget(
    State(state): State<AppState>,
    tenant_ctx: crate::auth::OptionalTenant,
    Path(id): Path<ContinuityId>,
    Json(body): Json<ReserveBudgetRequest>,
) -> Result<impl IntoResponse, ApiError> {
    let tenant_id = crate::auth::enforce_tenant_create(&tenant_ctx, &body.tenant_id)?;
    if body.estimation_version.is_empty() || body.estimation_version.len() > 128 {
        return Err(ApiError::InvalidArgument(
            "estimation_version must contain between 1 and 128 bytes".into(),
        ));
    }
    let (execution, instance) = continuity_instance(&state, &tenant_id, id).await?;
    let budget = instance.budget.ok_or_else(|| {
        ApiError::Conflict("execution has no configured multidimensional budget".into())
    })?;
    let reservation = BudgetReservation {
        id: BudgetReservationId::new(),
        tenant_id,
        continuity_id: id,
        epoch: execution.epoch,
        requested: body.requested,
        actual: None,
        estimation_version: body.estimation_version,
        state: ReservationState::Reserved,
        created_at: Utc::now(),
    };
    let reserved = state
        .storage
        .reserve_budget(&reservation, &budget)
        .await
        .map_err(|error| ApiError::from_storage(error, "budget reservation"))?;
    if !reserved {
        return Err(ApiError::Conflict(
            "budget reservation is stale, negative, or exceeds a hard limit".into(),
        ));
    }
    Ok((StatusCode::CREATED, Json(reservation)))
}

#[derive(Debug, Deserialize)]
struct CreateAttentionRequest {
    tenant_id: TenantId,
    continuity_id: ContinuityId,
    required_skills: Vec<String>,
    classification: DataClassification,
    #[serde(default)]
    allowed_regions: Vec<String>,
    priority: u8,
    deadline: chrono::DateTime<Utc>,
    estimated_attention_units: i64,
}

async fn create_attention_task(
    State(state): State<AppState>,
    tenant_ctx: crate::auth::OptionalTenant,
    Json(body): Json<CreateAttentionRequest>,
) -> Result<impl IntoResponse, ApiError> {
    let tenant_id = crate::auth::enforce_tenant_create(&tenant_ctx, &body.tenant_id)?;
    let now = Utc::now();
    if body.required_skills.is_empty()
        || body.required_skills.len() > 32
        || body.priority > 3
        || body.estimated_attention_units <= 0
        || body.deadline <= now
        || body.deadline > now + Duration::days(30)
    {
        return Err(ApiError::InvalidArgument(
            "attention task has invalid skills, priority, units, or deadline".into(),
        ));
    }
    if state
        .storage
        .get_continuity_execution(&tenant_id, body.continuity_id)
        .await
        .map_err(|error| ApiError::from_storage(error, "continuity execution"))?
        .is_none()
    {
        return Err(ApiError::NotFound("continuity execution".into()));
    }
    let task = AttentionTask {
        id: AttentionTaskId::new(),
        tenant_id,
        continuity_id: body.continuity_id,
        required_skills: body.required_skills,
        classification: body.classification,
        allowed_regions: body.allowed_regions,
        priority: body.priority,
        deadline: body.deadline,
        estimated_attention_units: body.estimated_attention_units,
        state: AttentionState::Pending,
        assignee: None,
        lease_expires_at: None,
    };
    state
        .storage
        .create_attention_task(&task)
        .await
        .map_err(|error| ApiError::from_storage(error, "attention task"))?;
    Ok((StatusCode::CREATED, Json(task)))
}

#[derive(Debug, Deserialize)]
struct AssignAttentionRequest {
    tenant_id: TenantId,
    reviewers: Vec<ReviewerCapabilities>,
    lease_seconds: i64,
}

async fn assign_attention_task(
    State(state): State<AppState>,
    tenant_ctx: crate::auth::OptionalTenant,
    Path(id): Path<AttentionTaskId>,
    Json(body): Json<AssignAttentionRequest>,
) -> Result<Json<AttentionTask>, ApiError> {
    let tenant_id = crate::auth::enforce_tenant_create(&tenant_ctx, &body.tenant_id)?;
    if body.reviewers.len() > 1_000 || !(1..=86_400).contains(&body.lease_seconds) {
        return Err(ApiError::InvalidArgument(
            "attention assignment has too many reviewers or an invalid lease".into(),
        ));
    }
    let task = state
        .storage
        .get_attention_task(&tenant_id, id)
        .await
        .map_err(|error| ApiError::from_storage(error, "attention task"))?
        .ok_or_else(|| ApiError::NotFound("attention task".into()))?;
    let mut assigned = task.clone();
    if orch8_engine::continuity_advanced::assign_attention_task(
        &mut assigned,
        &body.reviewers,
        Utc::now(),
        body.lease_seconds,
    )
    .is_none()
    {
        return Err(ApiError::Conflict(
            "no eligible reviewer is available".into(),
        ));
    }
    let claimed = state
        .storage
        .claim_attention_task(&tenant_id, &task, &assigned, Utc::now())
        .await
        .map_err(|error| ApiError::from_storage(error, "attention task"))?;
    if !claimed {
        return Err(ApiError::Conflict(
            "attention task was assigned concurrently".into(),
        ));
    }
    Ok(Json(assigned))
}

fn validate_sha256(value: &str, field: &str) -> Result<(), ApiError> {
    if value.len() == 64 && value.bytes().all(|byte| byte.is_ascii_hexdigit()) {
        Ok(())
    } else {
        Err(ApiError::InvalidArgument(format!(
            "{field} must be a 64-character hexadecimal digest"
        )))
    }
}

#[derive(Debug, Deserialize)]
struct ContinuityCheckpointQuery {
    tenant_id: String,
    limit: Option<u32>,
}

async fn list_continuity_checkpoints(
    State(state): State<AppState>,
    tenant_ctx: crate::auth::OptionalTenant,
    Path(id): Path<ContinuityId>,
    Query(query): Query<ContinuityCheckpointQuery>,
) -> Result<Json<Vec<CheckpointBoundary>>, ApiError> {
    let tenant_id = query_tenant(&tenant_ctx, &query.tenant_id)?;
    let checkpoints = continuity_checkpoint_records(
        &state,
        &tenant_id,
        id,
        query.limit.unwrap_or(1_000).min(10_000),
    )
    .await?;
    Ok(Json(
        checkpoints
            .into_iter()
            .map(|checkpoint| checkpoint.boundary)
            .collect(),
    ))
}

struct ContinuityCheckpointRecord {
    boundary: CheckpointBoundary,
    checkpoint: orch8_types::checkpoint::Checkpoint,
    instance: Arc<orch8_types::instance::TaskInstance>,
    sequence: Arc<orch8_types::sequence::SequenceDefinition>,
}

#[derive(Debug, Serialize)]
struct CheckpointStateChange {
    path: String,
    before: serde_json::Value,
    after: serde_json::Value,
}

#[derive(Debug, Serialize)]
struct ContinuityCheckpointDetail {
    boundary: CheckpointBoundary,
    checkpoint_data: serde_json::Value,
    previous_checkpoint_id: Option<uuid::Uuid>,
    redacted_state_diff: Vec<CheckpointStateChange>,
}

async fn get_continuity_checkpoint(
    State(state): State<AppState>,
    tenant_ctx: crate::auth::OptionalTenant,
    Path((id, checkpoint_id)): Path<(ContinuityId, uuid::Uuid)>,
    Query(query): Query<TenantQuery>,
) -> Result<Json<ContinuityCheckpointDetail>, ApiError> {
    let tenant_id = query_tenant(&tenant_ctx, &query.tenant_id)?;
    let records = continuity_checkpoint_records(&state, &tenant_id, id, 10_000).await?;
    let position = records
        .iter()
        .position(|record| record.checkpoint.id == checkpoint_id)
        .ok_or_else(|| ApiError::NotFound("checkpoint".into()))?;
    let current = &records[position];
    let previous = position.checked_sub(1).and_then(|index| records.get(index));
    let redaction = orch8_types::redaction::RedactionPolicy::default();
    let before = previous.map_or_else(
        || serde_json::json!({}),
        |record| redaction.redacted(&record.checkpoint.checkpoint_data),
    );
    let after = redaction.redacted(&current.checkpoint.checkpoint_data);
    let mut redacted_state_diff = Vec::new();
    collect_checkpoint_changes("", &before, &after, 0, &mut redacted_state_diff);
    Ok(Json(ContinuityCheckpointDetail {
        boundary: current.boundary.clone(),
        checkpoint_data: current.checkpoint.checkpoint_data.clone(),
        previous_checkpoint_id: previous.map(|record| record.checkpoint.id),
        redacted_state_diff,
    }))
}

fn collect_checkpoint_changes(
    path: &str,
    before: &serde_json::Value,
    after: &serde_json::Value,
    depth: usize,
    changes: &mut Vec<CheckpointStateChange>,
) {
    const MAX_CHANGES: usize = 1_000;
    const MAX_DEPTH: usize = 32;
    if before == after || changes.len() >= MAX_CHANGES {
        return;
    }
    if depth >= MAX_DEPTH {
        changes.push(CheckpointStateChange {
            path: path.to_owned(),
            before: before.clone(),
            after: after.clone(),
        });
        return;
    }
    match (before, after) {
        (serde_json::Value::Object(left), serde_json::Value::Object(right)) => {
            let keys: std::collections::BTreeSet<_> = left
                .keys()
                .chain(right.keys())
                .map(String::as_str)
                .collect();
            for key in keys {
                let child_path = if path.is_empty() {
                    key.to_owned()
                } else {
                    format!("{path}.{key}")
                };
                collect_checkpoint_changes(
                    &child_path,
                    left.get(key).unwrap_or(&serde_json::Value::Null),
                    right.get(key).unwrap_or(&serde_json::Value::Null),
                    depth + 1,
                    changes,
                );
                if changes.len() >= MAX_CHANGES {
                    break;
                }
            }
        }
        (serde_json::Value::Object(left), serde_json::Value::Null) => {
            for (key, value) in left {
                let child_path = if path.is_empty() {
                    key.clone()
                } else {
                    format!("{path}.{key}")
                };
                collect_checkpoint_changes(
                    &child_path,
                    value,
                    &serde_json::Value::Null,
                    depth + 1,
                    changes,
                );
                if changes.len() >= MAX_CHANGES {
                    break;
                }
            }
        }
        (serde_json::Value::Null, serde_json::Value::Object(right)) => {
            for (key, value) in right {
                let child_path = if path.is_empty() {
                    key.clone()
                } else {
                    format!("{path}.{key}")
                };
                collect_checkpoint_changes(
                    &child_path,
                    &serde_json::Value::Null,
                    value,
                    depth + 1,
                    changes,
                );
                if changes.len() >= MAX_CHANGES {
                    break;
                }
            }
        }
        _ => changes.push(CheckpointStateChange {
            path: path.to_owned(),
            before: before.clone(),
            after: after.clone(),
        }),
    }
}

async fn continuity_checkpoint_records(
    state: &AppState,
    tenant_id: &TenantId,
    continuity_id: ContinuityId,
    limit: u32,
) -> Result<Vec<ContinuityCheckpointRecord>, ApiError> {
    state
        .storage
        .get_continuity_execution(tenant_id, continuity_id)
        .await
        .map_err(|error| ApiError::from_storage(error, "continuity execution"))?
        .ok_or_else(|| ApiError::NotFound("continuity execution".into()))?;
    let locations = state
        .storage
        .list_continuity_locations(tenant_id, continuity_id, 10_000)
        .await
        .map_err(|error| ApiError::from_storage(error, "continuity locations"))?;
    let mut records = Vec::new();
    for location in locations {
        let remaining = usize::try_from(limit)
            .unwrap_or(usize::MAX)
            .saturating_sub(records.len());
        if remaining == 0 {
            break;
        }
        let instance = state
            .storage
            .get_instance(location.instance_id)
            .await
            .map_err(|error| ApiError::from_storage(error, "checkpoint instance"))?
            .ok_or_else(|| ApiError::NotFound("checkpoint instance".into()))?;
        if &instance.tenant_id != tenant_id {
            return Err(ApiError::NotFound("checkpoint instance".into()));
        }
        let sequence = state
            .storage
            .get_sequence(instance.sequence_id)
            .await
            .map_err(|error| ApiError::from_storage(error, "checkpoint sequence"))?
            .ok_or_else(|| ApiError::NotFound("checkpoint sequence".into()))?;
        let checkpoints = state
            .storage
            .list_checkpoints(
                instance.id,
                u32::try_from(remaining).unwrap_or(u32::MAX).min(10_000),
            )
            .await
            .map_err(|error| ApiError::from_storage(error, "checkpoints"))?;
        let instance = Arc::new(instance);
        let sequence = Arc::new(sequence);
        for checkpoint in checkpoints {
            records.push(ContinuityCheckpointRecord {
                boundary: checkpoint_boundary(
                    continuity_id,
                    location.epoch,
                    &sequence,
                    &checkpoint,
                )?,
                checkpoint,
                instance: Arc::clone(&instance),
                sequence: Arc::clone(&sequence),
            });
        }
    }
    records.sort_by_key(|record| (record.boundary.epoch, record.boundary.created_at));
    Ok(records)
}

async fn find_continuity_checkpoint(
    state: &AppState,
    tenant_id: &TenantId,
    continuity_id: ContinuityId,
    checkpoint_id: uuid::Uuid,
) -> Result<ContinuityCheckpointRecord, ApiError> {
    continuity_checkpoint_records(state, tenant_id, continuity_id, 10_000)
        .await?
        .into_iter()
        .find(|record| record.checkpoint.id == checkpoint_id)
        .ok_or_else(|| ApiError::NotFound("checkpoint".into()))
}

async fn continuity_instance(
    state: &AppState,
    tenant_id: &TenantId,
    continuity_id: ContinuityId,
) -> Result<(ContinuityExecution, orch8_types::instance::TaskInstance), ApiError> {
    let execution = state
        .storage
        .get_continuity_execution(tenant_id, continuity_id)
        .await
        .map_err(|error| ApiError::from_storage(error, "continuity execution"))?
        .ok_or_else(|| ApiError::NotFound("continuity execution".into()))?;
    let instance = state
        .storage
        .get_instance(execution.current_instance_id)
        .await
        .map_err(|error| ApiError::from_storage(error, "instance"))?
        .ok_or_else(|| ApiError::NotFound("instance".into()))?;
    if &instance.tenant_id != tenant_id {
        return Err(ApiError::NotFound("instance".into()));
    }
    Ok((execution, instance))
}

fn checkpoint_boundary(
    continuity_id: ContinuityId,
    epoch: ExecutionEpoch,
    sequence: &orch8_types::sequence::SequenceDefinition,
    checkpoint: &orch8_types::checkpoint::Checkpoint,
) -> Result<CheckpointBoundary, ApiError> {
    let block = checkpoint
        .checkpoint_data
        .get("safe_boundary")
        .or_else(|| checkpoint.checkpoint_data.get("block_id"))
        .and_then(serde_json::Value::as_str)
        .unwrap_or("unknown_boundary");
    let canonical = orch8_publisher::manifest::canonical_json(&checkpoint.checkpoint_data)
        .map_err(|error| ApiError::Internal(error.to_string()))?;
    Ok(CheckpointBoundary {
        checkpoint_id: checkpoint.id,
        instance_id: checkpoint.instance_id,
        continuity_id,
        epoch,
        sequence_id: sequence.id,
        sequence_version: sequence.version,
        block_id: orch8_types::ids::BlockId::new(block),
        checkpoint_sha256: hex_sha256(canonical.as_bytes()),
        // Historical provenance heads were not stored on legacy checkpoints;
        // absence stays explicit instead of attaching the current head.
        provenance_head: None,
        created_at: checkpoint.created_at,
    })
}

#[derive(Debug, Deserialize)]
struct WhatIfRequest {
    tenant_id: TenantId,
    checkpoint_id: uuid::Uuid,
    #[serde(default)]
    context_patch: serde_json::Value,
    #[serde(default)]
    output_overrides: serde_json::Value,
    #[serde(default)]
    handler_mocks: serde_json::Value,
    target_sequence_version: Option<i32>,
    max_ticks: Option<u32>,
}

#[derive(Debug, Serialize)]
struct WhatIfResponse {
    scenario: WhatIfScenario,
    report: orch8_types::contract::CaseReport,
}

#[allow(clippy::too_many_lines)] // assembles one bounded, effect-free simulation from durable evidence
async fn run_what_if(
    State(state): State<AppState>,
    tenant_ctx: crate::auth::OptionalTenant,
    Path(id): Path<ContinuityId>,
    Json(body): Json<WhatIfRequest>,
) -> Result<Json<WhatIfResponse>, ApiError> {
    let tenant_id = crate::auth::enforce_tenant_create(&tenant_ctx, &body.tenant_id)?;
    let located = find_continuity_checkpoint(&state, &tenant_id, id, body.checkpoint_id).await?;
    let instance = Arc::unwrap_or_clone(located.instance);
    let source_sequence = Arc::unwrap_or_clone(located.sequence);
    let checkpoint = located.checkpoint;
    let boundary = located.boundary;
    let sequence = if let Some(version) = body.target_sequence_version {
        state
            .storage
            .get_sequence_by_name(
                &tenant_id,
                &source_sequence.namespace,
                &source_sequence.name,
                Some(version),
            )
            .await
            .map_err(|error| ApiError::from_storage(error, "target sequence"))?
            .ok_or_else(|| ApiError::NotFound("target sequence version".into()))?
    } else {
        source_sequence
    };
    let mut input = checkpoint
        .checkpoint_data
        .get("context_snapshot")
        .cloned()
        .unwrap_or_else(|| instance.context.data.clone());
    merge_object_patch(&mut input, &body.context_patch)?;
    let outputs = state
        .storage
        .get_all_outputs(instance.id)
        .await
        .map_err(|error| ApiError::from_storage(error, "outputs"))?;
    let initial_outputs = outputs
        .iter()
        .filter(|output| output.created_at <= checkpoint.created_at)
        .map(|output| (output.block_id.as_str().to_owned(), output.output.clone()))
        .collect::<std::collections::BTreeMap<_, _>>();
    let recorded = outputs
        .into_iter()
        .map(|output| (output.block_id.as_str().to_owned(), output.output))
        .collect::<std::collections::HashMap<_, _>>();
    let overrides = body.output_overrides.as_object().ok_or_else(|| {
        ApiError::InvalidArgument("output_overrides must be a JSON object".into())
    })?;
    let explicit_mocks = body
        .handler_mocks
        .as_object()
        .ok_or_else(|| ApiError::InvalidArgument("handler_mocks must be a JSON object".into()))?;
    if overrides.len() > 1_000 || explicit_mocks.len() > 1_000 {
        return Err(ApiError::PayloadTooLarge(
            "what-if overrides exceed 1000 entries".into(),
        ));
    }
    let mocks = sequence_step_blocks(&sequence)
        .into_iter()
        .filter_map(|block| {
            let output = explicit_mocks
                .get(&block)
                .or_else(|| overrides.get(&block))
                .cloned()
                .or_else(|| recorded.get(&block).cloned())?;
            Some(orch8_types::contract::MockDef {
                handler: None,
                block: Some(block),
                policy: orch8_types::contract::MockPolicy::Success { output },
            })
        })
        .collect();
    let max_ticks = body.max_ticks.unwrap_or(5_000).min(10_000);
    let case = orch8_types::contract::ContractCase {
        name: "continuity-what-if".into(),
        description: Some("effect-free continuity simulation".into()),
        input,
        initial_outputs,
        config: None,
        mocks,
        signals: Vec::new(),
        expect: orch8_types::contract::Expectations::default(),
        max_logical_duration_ms: Some(86_400_000),
        max_ticks: Some(max_ticks),
    };
    let report = orch8::contract::run_case(
        &sequence,
        orch8_types::contract::UnmockedHandlerPolicy::Fail,
        &case,
        &orch8::contract::RunOptions::default(),
    )
    .await
    .map_err(|error| ApiError::Internal(format!("what-if simulation failed: {error}")))?;
    let scenario = WhatIfScenario {
        id: ScenarioId::new(),
        tenant_id,
        source: boundary,
        context_patch: body.context_patch,
        output_overrides: body.output_overrides,
        handler_mocks: body.handler_mocks,
        target_sequence_version: body.target_sequence_version,
        effect_mode: ForkEffectMode::Blocked,
        virtual_time: true,
        retain_full_evidence: false,
    };
    Ok(Json(WhatIfResponse { scenario, report }))
}

fn merge_object_patch(
    target: &mut serde_json::Value,
    patch: &serde_json::Value,
) -> Result<(), ApiError> {
    let patch = patch
        .as_object()
        .ok_or_else(|| ApiError::InvalidArgument("context_patch must be a JSON object".into()))?;
    if patch.len() > 1_000 {
        return Err(ApiError::PayloadTooLarge(
            "context_patch exceeds 1000 top-level entries".into(),
        ));
    }
    let target = target
        .as_object_mut()
        .ok_or_else(|| ApiError::Conflict("checkpoint context is not an object".into()))?;
    for (key, value) in patch {
        target.insert(key.clone(), value.clone());
    }
    Ok(())
}

fn sequence_step_blocks(sequence: &orch8_types::sequence::SequenceDefinition) -> Vec<String> {
    fn walk(value: &serde_json::Value, blocks: &mut Vec<String>) {
        match value {
            serde_json::Value::Object(object) => {
                if let (Some(id), Some(_)) = (
                    object.get("id").and_then(serde_json::Value::as_str),
                    object.get("handler").and_then(serde_json::Value::as_str),
                ) && !blocks.iter().any(|candidate| candidate == id)
                {
                    blocks.push(id.to_owned());
                }
                for child in object.values() {
                    walk(child, blocks);
                }
            }
            serde_json::Value::Array(values) => {
                for child in values {
                    walk(child, blocks);
                }
            }
            _ => {}
        }
    }
    let mut blocks = Vec::new();
    if let Ok(value) = serde_json::to_value(sequence) {
        walk(&value, &mut blocks);
    }
    blocks
}

#[derive(Debug, Deserialize)]
struct ExtractFixtureRequest {
    tenant_id: TenantId,
    checkpoint_id: uuid::Uuid,
    #[serde(default)]
    allowlisted_fields: Vec<String>,
}

fn build_extracted_contract(
    sequence: &orch8_types::sequence::SequenceDefinition,
    checkpoint: &orch8_types::checkpoint::Checkpoint,
    sanitized_context: &serde_json::Value,
    outputs: &[orch8_types::output::BlockOutput],
    redaction: &orch8_types::redaction::RedactionPolicy,
) -> Result<
    (
        orch8_types::sequence::SequenceDefinition,
        orch8_types::contract::ContractSuite,
    ),
    ApiError,
> {
    let sanitized_sequence: orch8_types::sequence::SequenceDefinition = serde_json::from_value(
        redaction.redacted(
            &serde_json::to_value(sequence)
                .map_err(|error| ApiError::Internal(error.to_string()))?,
        ),
    )
    .map_err(|error| ApiError::Internal(format!("redacted sequence is invalid: {error}")))?;
    let initial_outputs = outputs
        .iter()
        .filter(|output| output.created_at <= checkpoint.created_at)
        .filter(|output| !output.block_id.as_str().starts_with('_'))
        .map(|output| {
            (
                output.block_id.as_str().to_owned(),
                redaction.redacted(&output.output),
            )
        })
        .collect::<std::collections::BTreeMap<_, _>>();
    let mocks = outputs
        .iter()
        .filter(|output| output.created_at > checkpoint.created_at)
        .filter(|output| !output.block_id.as_str().starts_with('_'))
        .map(|output| orch8_types::contract::MockDef {
            handler: None,
            block: Some(output.block_id.as_str().to_owned()),
            policy: orch8_types::contract::MockPolicy::Success {
                output: redaction.redacted(&output.output),
            },
        })
        .collect();
    let contract = orch8_types::contract::ContractSuite {
        schema_version: orch8_types::contract::CONTRACT_SCHEMA_VERSION,
        sequence_name: Some(sanitized_sequence.name.clone()),
        sequence_version: Some(i64::from(sanitized_sequence.version)),
        unmocked_handlers: orch8_types::contract::UnmockedHandlerPolicy::Fail,
        cases: vec![orch8_types::contract::ContractCase {
            name: format!("continuation-{}", checkpoint.id),
            description: Some(
                "Sanitized continuation fixture extracted from durable evidence".into(),
            ),
            input: sanitized_context.clone(),
            initial_outputs,
            config: None,
            mocks,
            signals: Vec::new(),
            expect: orch8_types::contract::Expectations::default(),
            max_logical_duration_ms: Some(86_400_000),
            max_ticks: Some(5_000),
        }],
    };
    Ok((sanitized_sequence, contract))
}

fn build_extracted_effect_mocks(
    receipts: &[EffectReceipt],
    redaction: &orch8_types::redaction::RedactionPolicy,
) -> (Vec<String>, Vec<ExtractedEffectMock>) {
    let receipt_mocks = receipts
        .iter()
        .map(|receipt| receipt.request_sha256.clone())
        .collect();
    let effect_mocks = receipts
        .iter()
        .map(|receipt| ExtractedEffectMock {
            block_id: receipt.block_id.clone(),
            kind: receipt.kind,
            state: receipt.state,
            request_sha256: receipt.request_sha256.clone(),
            destination_fingerprint: receipt.destination_fingerprint.clone(),
            provider_receipt_id: receipt.provider_receipt_id.as_ref().map(|value| {
                redaction
                    .redacted(&serde_json::Value::String(value.clone()))
                    .as_str()
                    .unwrap_or(orch8_types::redaction::REDACTED)
                    .to_owned()
            }),
        })
        .collect();
    (receipt_mocks, effect_mocks)
}

async fn extract_test_fixture(
    State(state): State<AppState>,
    tenant_ctx: crate::auth::OptionalTenant,
    Path(id): Path<ContinuityId>,
    Json(body): Json<ExtractFixtureRequest>,
) -> Result<Json<ExtractedTestFixture>, ApiError> {
    let tenant_id = crate::auth::enforce_tenant_create(&tenant_ctx, &body.tenant_id)?;
    if body.allowlisted_fields.len() > 256 {
        return Err(ApiError::PayloadTooLarge(
            "test fixture allowlist exceeds 256 fields".into(),
        ));
    }
    let located = find_continuity_checkpoint(&state, &tenant_id, id, body.checkpoint_id).await?;
    let instance = Arc::unwrap_or_clone(located.instance);
    let sequence = Arc::unwrap_or_clone(located.sequence);
    let checkpoint = located.checkpoint;
    let source = located.boundary;
    let context = checkpoint
        .checkpoint_data
        .get("context_snapshot")
        .cloned()
        .unwrap_or_else(|| instance.context.data.clone());
    let allowlist: std::collections::BTreeSet<_> = body.allowlisted_fields.into_iter().collect();
    let selected = context.as_object().map_or_else(
        || serde_json::json!({}),
        |object| {
            serde_json::Value::Object(
                object
                    .iter()
                    .filter(|(key, _)| allowlist.contains(*key))
                    .map(|(key, value)| (key.clone(), value.clone()))
                    .collect(),
            )
        },
    );
    let redaction = orch8_types::redaction::RedactionPolicy::default();
    let sanitized_context = redaction.redacted(&selected);
    let outputs = state
        .storage
        .get_all_outputs(instance.id)
        .await
        .map_err(|error| ApiError::from_storage(error, "fixture outputs"))?;
    let (sanitized_sequence, contract) = build_extracted_contract(
        &sequence,
        &checkpoint,
        &sanitized_context,
        &outputs,
        &redaction,
    )?;
    let receipts = state
        .storage
        .list_effect_receipts(&tenant_id, id, 10_000)
        .await
        .map_err(|error| ApiError::from_storage(error, "effect receipts"))?;
    let (receipt_mocks, effect_mocks) = build_extracted_effect_mocks(&receipts, &redaction);
    let mut missing_evidence = Vec::new();
    if checkpoint.checkpoint_data.get("context_snapshot").is_none() {
        missing_evidence.push("checkpoint.context_snapshot".into());
    }
    if receipts
        .iter()
        .any(|receipt| receipt.state == EffectState::Unknown)
    {
        missing_evidence.push("resolved_effect_receipt".into());
    }
    let fixture_report = orch8::contract::run_suite(
        &sanitized_sequence,
        &contract,
        &orch8::contract::RunOptions::default(),
    )
    .await
    .map_err(|error| ApiError::Internal(format!("fixture validation failed: {error}")))?;
    for failure in fixture_report
        .cases
        .iter()
        .flat_map(|case| case.failures.iter())
        .take(1_000)
    {
        missing_evidence.push(format!("offline_replay: {failure}"));
    }
    missing_evidence.sort();
    missing_evidence.dedup();
    let stable_material = orch8_publisher::manifest::canonical_json(&serde_json::json!({
        "source": source,
        "sequence": sanitized_sequence,
        "contract": contract,
        "effects": effect_mocks,
        "missing": missing_evidence,
    }))
    .map_err(|error| ApiError::Internal(error.to_string()))?;
    Ok(Json(ExtractedTestFixture {
        source,
        stable_id: hex_sha256(stable_material.as_bytes()),
        sanitized_context,
        receipt_mocks,
        effect_mocks,
        sequence: sanitized_sequence,
        contract,
        complete: missing_evidence.is_empty(),
        missing_evidence,
    }))
}

#[derive(Debug, Deserialize)]
struct MigrationPlanRequest {
    tenant_id: TenantId,
    continuity_id: ContinuityId,
    to_sequence_id: SequenceId,
    to_version: i32,
    #[serde(default)]
    typed_finding_codes: Vec<String>,
    #[serde(default)]
    transforms: Vec<StateTransform>,
    historical_validation_passed: Option<bool>,
}

async fn plan_live_migration(
    State(state): State<AppState>,
    tenant_ctx: crate::auth::OptionalTenant,
    Json(body): Json<MigrationPlanRequest>,
) -> Result<Json<LiveMigrationPlan>, ApiError> {
    let tenant_id = crate::auth::enforce_tenant_create(&tenant_ctx, &body.tenant_id)?;
    if body.typed_finding_codes.len() > 1_000 {
        return Err(ApiError::PayloadTooLarge(
            "migration findings exceed 1000 entries".into(),
        ));
    }
    let (_, instance) = continuity_instance(&state, &tenant_id, body.continuity_id).await?;
    let source = state
        .storage
        .get_sequence(instance.sequence_id)
        .await
        .map_err(|error| ApiError::from_storage(error, "source sequence"))?
        .ok_or_else(|| ApiError::NotFound("source sequence".into()))?;
    let target = state
        .storage
        .get_sequence(body.to_sequence_id)
        .await
        .map_err(|error| ApiError::from_storage(error, "target sequence"))?
        .ok_or_else(|| ApiError::NotFound("target sequence".into()))?;
    if target.tenant_id != tenant_id || target.version != body.to_version {
        return Err(ApiError::NotFound("target sequence".into()));
    }
    let seed = LiveMigrationPlan {
        id: MigrationPlanId::new(),
        tenant_id,
        continuity_id: body.continuity_id,
        from_sequence_id: source.id,
        from_version: source.version,
        to_sequence_id: target.id,
        to_version: target.version,
        disposition: MigrationDisposition::Pin,
        transforms: Vec::new(),
        finding_codes: Vec::new(),
        rollback_capsule_required: true,
        created_at: Utc::now(),
    };
    orch8_engine::continuity_advanced::compile_migration_plan(
        seed,
        &body.typed_finding_codes,
        body.transforms,
        body.historical_validation_passed,
    )
    .map(Json)
    .map_err(|error| ApiError::InvalidArgument(error.to_string()))
}

#[derive(Debug, Deserialize)]
struct GenerateScenariosRequest {
    #[serde(default)]
    events: Vec<String>,
    #[serde(default)]
    faults: Vec<FaultInjection>,
    max_scenarios: usize,
    seed: u64,
}

async fn generate_scenarios(
    State(state): State<AppState>,
    Json(body): Json<GenerateScenariosRequest>,
) -> Result<Json<Vec<GeneratedScenario>>, ApiError> {
    if !state.continuity_lab_enabled {
        return Err(ApiError::Unavailable(
            "continuity fault laboratory is disabled".into(),
        ));
    }
    orch8_engine::continuity_advanced::generate_scenarios(
        &body.events,
        &body.faults,
        body.max_scenarios,
        body.seed,
    )
    .map(Json)
    .map_err(|error| ApiError::InvalidArgument(error.to_string()))
}

#[derive(Debug, Deserialize)]
struct ReproduceIncidentRequest {
    scenario: GeneratedScenario,
    required_fault_kind: FaultKind,
}

async fn reproduce_incident(
    State(state): State<AppState>,
    Json(body): Json<ReproduceIncidentRequest>,
) -> Result<Json<orch8_types::continuity_advanced::IncidentReproduction>, ApiError> {
    if !state.continuity_lab_enabled {
        return Err(ApiError::Unavailable(
            "continuity fault laboratory is disabled".into(),
        ));
    }
    Ok(Json(
        orch8_engine::continuity_advanced::minimize_reproducing_scenario(
            body.scenario,
            |candidate| {
                candidate
                    .faults
                    .iter()
                    .any(|fault| fault.kind == body.required_fault_kind)
            },
        ),
    ))
}

#[derive(Debug, Deserialize)]
struct ChooseProviderRequest {
    candidates: Vec<ProviderCandidate>,
    #[serde(default)]
    allowed_regions: std::collections::BTreeSet<String>,
    max_price_microunits: Option<i64>,
    max_latency_ms: Option<u64>,
    minimum_quality_millipoints: Option<i64>,
    #[serde(default)]
    require_idempotency: bool,
    cohort_key: String,
}

async fn choose_provider(
    Json(body): Json<ChooseProviderRequest>,
) -> Result<Json<orch8_types::continuity_advanced::ProviderDecision>, ApiError> {
    if body.candidates.len() > 1_000 || body.cohort_key.len() > 256 {
        return Err(ApiError::PayloadTooLarge(
            "provider candidates or cohort key exceed bounded limits".into(),
        ));
    }
    Ok(Json(orch8_engine::continuity_advanced::choose_provider(
        &body.candidates,
        &orch8_engine::continuity_advanced::ProviderRequirements {
            allowed_regions: body.allowed_regions,
            max_price_microunits: body.max_price_microunits,
            max_latency_ms: body.max_latency_ms,
            minimum_quality_millipoints: body.minimum_quality_millipoints,
            require_idempotency: body.require_idempotency,
        },
        &body.cohort_key,
        Utc::now(),
    )))
}

#[derive(Debug, Deserialize)]
struct OptimizationRequest {
    serial_work_millipoints: u16,
    retry_rate_millipoints: u16,
    average_payload_bytes: u64,
    average_cost_microunits: i64,
    dead_branch_count: u32,
    base_scenario: WhatIfScenario,
}

async fn recommend_optimizations(
    Json(body): Json<OptimizationRequest>,
) -> Json<Vec<orch8_types::continuity_advanced::OptimizationRecommendation>> {
    Json(orch8_engine::continuity_advanced::recommend_optimizations(
        orch8_engine::continuity_advanced::WorkflowAggregate {
            serial_work_millipoints: body.serial_work_millipoints,
            retry_rate_millipoints: body.retry_rate_millipoints,
            average_payload_bytes: body.average_payload_bytes,
            average_cost_microunits: body.average_cost_microunits,
            dead_branch_count: body.dead_branch_count,
        },
        &body.base_scenario,
    ))
}

#[derive(Debug, Deserialize)]
struct EvaluationGateRequest {
    baseline_scores: Vec<i64>,
    candidate_scores: Vec<i64>,
    minimum_samples: usize,
    maximum_regression_millipoints: i64,
}

#[derive(Debug, Serialize)]
struct EvaluationGateResponse {
    status: orch8_types::continuity_advanced::EvidenceStatus,
}

async fn evaluate_gate(
    Json(body): Json<EvaluationGateRequest>,
) -> Result<Json<EvaluationGateResponse>, ApiError> {
    if body.baseline_scores.len() > 100_000 || body.candidate_scores.len() > 100_000 {
        return Err(ApiError::PayloadTooLarge(
            "evaluation samples exceed 100000 values".into(),
        ));
    }
    Ok(Json(EvaluationGateResponse {
        status: orch8_engine::continuity_advanced::evaluation_gate(
            &body.baseline_scores,
            &body.candidate_scores,
            body.minimum_samples,
            body.maximum_regression_millipoints,
        ),
    }))
}

#[derive(Debug, Deserialize)]
struct ResidencyRequest {
    classification: DataClassification,
    operation: String,
    source_region: Option<String>,
    destination_region: Option<String>,
    #[serde(default)]
    allowed_regions: std::collections::BTreeSet<String>,
    destination_trust: Option<RuntimeTrustLevel>,
}

async fn evaluate_residency(Json(body): Json<ResidencyRequest>) -> Json<ResidencyEvidence> {
    Json(orch8_engine::continuity_advanced::evaluate_residency(
        body.classification,
        body.operation,
        body.source_region,
        body.destination_region,
        &body.allowed_regions,
        body.destination_trust,
    ))
}

#[derive(Debug, Deserialize)]
struct MinimizeDisclosureRequest {
    classification: DataClassification,
    payload: serde_json::Value,
    #[serde(default)]
    allowed_top_level_fields: std::collections::BTreeSet<String>,
}

async fn minimize_disclosure(
    Json(body): Json<MinimizeDisclosureRequest>,
) -> Result<Json<orch8_types::continuity_advanced::DisclosureResult>, ApiError> {
    if body.allowed_top_level_fields.len() > 256 {
        return Err(ApiError::PayloadTooLarge(
            "disclosure allowlist exceeds 256 fields".into(),
        ));
    }
    Ok(Json(
        orch8_engine::continuity_advanced::minimize_disclosure(
            &body.payload,
            &body.allowed_top_level_fields,
            body.classification,
        ),
    ))
}

#[derive(Debug, Deserialize)]
struct VerifyFederationRequest {
    peer: FederationPeer,
    envelope: FederationEnvelope,
    payload_base64: String,
}

#[derive(Debug, Serialize)]
struct VerifyFederationResponse {
    valid: bool,
}

async fn verify_federation(
    Json(body): Json<VerifyFederationRequest>,
) -> Result<Json<VerifyFederationResponse>, ApiError> {
    if body.payload_base64.len() > 16 * 1024 * 1024 {
        return Err(ApiError::PayloadTooLarge(
            "federation payload exceeds the encoded 16 MiB limit".into(),
        ));
    }
    let payload = BASE64
        .decode(body.payload_base64)
        .map_err(|_| ApiError::InvalidArgument("payload_base64 is invalid".into()))?;
    orch8_engine::continuity_advanced::verify_federation_envelope(
        &body.peer,
        &body.envelope,
        &payload,
        Utc::now(),
    )
    .map_err(|error| ApiError::Conflict(error.to_string()))?;
    Ok(Json(VerifyFederationResponse { valid: true }))
}

#[derive(Debug, Deserialize)]
struct ClaimDelegationRequest {
    tenant_id: TenantId,
    delegation: DeviceDelegation,
    signed_grant: SignedContinuationGrant,
    token: String,
}

async fn claim_delegation(
    State(state): State<AppState>,
    tenant_ctx: crate::auth::OptionalTenant,
    Json(body): Json<ClaimDelegationRequest>,
) -> Result<Json<DeviceDelegation>, ApiError> {
    let tenant_id = crate::auth::enforce_tenant_create(&tenant_ctx, &body.tenant_id)?;
    if body.delegation.tenant_id != tenant_id {
        return Err(ApiError::NotFound("delegation".into()));
    }
    let crypto = state.continuity_crypto.as_ref().ok_or_else(|| {
        ApiError::Unavailable(
            "device delegation is disabled without a configured engine encryption key".into(),
        )
    })?;
    let trusted_key = BASE64.encode(crypto.signing_key.verifying_key().to_bytes());
    verify_signed_continuation_grant(&body.signed_grant, &[trusted_key])
        .map_err(|error| ApiError::Conflict(error.to_string()))?;
    let now = Utc::now();
    orch8_engine::continuity_advanced::validate_device_delegation(
        &body.delegation,
        &body.signed_grant.grant,
        now,
    )
    .map_err(|error| ApiError::Conflict(error.to_string()))?;
    let token = BASE64
        .decode(&body.token)
        .map_err(|_| ApiError::InvalidArgument("delegation token is not valid base64".into()))?;
    if token.len() != 32 {
        return Err(ApiError::InvalidArgument(
            "delegation token must decode to 32 bytes".into(),
        ));
    }
    let consumed = state
        .storage
        .consume_continuation_grant(
            &tenant_id,
            body.signed_grant.grant.id,
            &hex_sha256(&token),
            now,
        )
        .await
        .map_err(|error| ApiError::from_storage(error, "continuation grant"))?;
    if !consumed {
        return Err(ApiError::Conflict(
            "delegation grant is expired, revoked, invalid, or already consumed".into(),
        ));
    }
    Ok(Json(body.delegation))
}
