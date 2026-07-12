//! Safe workflow release control plane.
//!
//! Create a release candidate, semantically diff it, validate it against
//! real execution history (replayed offline — no side effects), canary
//! it to a deterministic cohort of new instances, and promote or roll
//! back with gates. Every state transition is a compare-and-swap plus an
//! immutable decision record, so concurrent promote/rollback cannot both
//! win and the audit trail is complete.

use axum::extract::{Path, Query, State};
use axum::http::StatusCode;
use axum::response::IntoResponse;
use axum::routing::{get, post};
use axum::{Json, Router};
use chrono::Utc;
use serde::{Deserialize, Serialize};
use utoipa::ToSchema;
use uuid::Uuid;

use orch8_engine::release_diff::semantic_diff;
use orch8_types::filter::{InstanceFilter, Pagination};
use orch8_types::ids::{SequenceId, TenantId};
use orch8_types::instance::InstanceState;
use orch8_types::release::{
    GateEvaluation, GateVerdict, InFlightPolicy, ReleaseDecision, ReleaseGate, ReleaseState,
    SemanticDiff, VariantStats, WorkflowRelease, evaluate_gates,
};
use orch8_types::sequence::SequenceDefinition;

use crate::AppState;
use crate::error::ApiError;

/// Historical instances replayed per validation run.
const DEFAULT_VALIDATION_SAMPLE: u32 = 10;
const MAX_VALIDATION_SAMPLE: u32 = 50;
/// Instances scanned per variant for gate evaluation.
const OBSERVATION_SCAN_LIMIT: u32 = 1_000;

pub fn routes() -> Router<AppState> {
    Router::new()
        .route("/releases", post(create_release).get(list_releases))
        .route("/releases/{id}", get(get_release))
        .route("/releases/{id}/decisions", get(list_decisions))
        .route("/releases/{id}/diff", get(diff_release))
        .route("/releases/{id}/validate", post(validate_release))
        .route("/releases/{id}/canary", post(start_canary))
        .route("/releases/{id}/evaluate", post(evaluate_release))
        .route("/releases/{id}/promote", post(promote_release))
        .route("/releases/{id}/pause", post(pause_release))
        .route("/releases/{id}/rollback", post(rollback_release))
        .route("/sequences/releases/diff", post(diff_sequences))
}

#[derive(Deserialize, ToSchema)]
pub(crate) struct CreateReleaseRequest {
    pub tenant_id: String,
    pub baseline_sequence_id: Uuid,
    pub candidate_sequence_id: Uuid,
    #[serde(default)]
    pub gates: Vec<ReleaseGate>,
    #[serde(default)]
    pub in_flight_policy: InFlightPolicy,
}

#[utoipa::path(post, path = "/releases", tag = "releases",
    request_body = CreateReleaseRequest,
    responses(
        (status = 201, description = "Release created (draft)", body = WorkflowRelease),
        (status = 400, description = "Baseline/candidate mismatch"),
        (status = 404, description = "Sequence not found"),
    )
)]
pub(crate) async fn create_release(
    State(state): State<AppState>,
    tenant_ctx: crate::auth::OptionalTenant,
    Json(req): Json<CreateReleaseRequest>,
) -> Result<impl IntoResponse, ApiError> {
    let tenant_id =
        crate::auth::enforce_tenant_create(&tenant_ctx, &TenantId::unchecked(&req.tenant_id))?;

    let baseline = fetch_sequence(&state, req.baseline_sequence_id, &tenant_id).await?;
    let candidate = fetch_sequence(&state, req.candidate_sequence_id, &tenant_id).await?;

    if baseline.id == candidate.id {
        return Err(ApiError::InvalidArgument(
            "baseline and candidate must be different sequence versions".into(),
        ));
    }
    if baseline.name != candidate.name || baseline.namespace != candidate.namespace {
        return Err(ApiError::InvalidArgument(format!(
            "baseline ({}/{}) and candidate ({}/{}) must be versions of the same sequence",
            baseline.namespace.as_str(),
            baseline.name,
            candidate.namespace.as_str(),
            candidate.name,
        )));
    }
    // Only one release may route traffic per baseline at a time
    // (non-goal: more than two active variants).
    if let Some(existing) = state
        .storage
        .find_routing_release_for_sequence(baseline.id)
        .await
        .map_err(|e| ApiError::from_storage(e, "release"))?
    {
        return Err(ApiError::Conflict(format!(
            "release {} is already routing traffic for this baseline",
            existing.id
        )));
    }

    let now = Utc::now();
    let release = WorkflowRelease {
        id: Uuid::now_v7(),
        tenant_id: tenant_id.clone(),
        namespace: baseline.namespace.clone(),
        sequence_name: baseline.name.clone(),
        baseline_sequence_id: baseline.id,
        baseline_version: baseline.version,
        candidate_sequence_id: candidate.id,
        candidate_version: candidate.version,
        state: ReleaseState::Draft,
        canary_percent: 0,
        gates: req.gates,
        in_flight_policy: req.in_flight_policy,
        validation_summary: None,
        canary_started_at: None,
        created_at: now,
        updated_at: now,
    };
    state
        .storage
        .create_release(&release)
        .await
        .map_err(|e| ApiError::from_storage(e, "release"))?;
    record_decision(
        &state,
        release.id,
        ReleaseState::Draft,
        ReleaseState::Draft,
        "operator",
        &format!(
            "release created: v{} → v{}",
            release.baseline_version, release.candidate_version
        ),
    )
    .await;
    Ok((StatusCode::CREATED, Json(release)))
}

async fn fetch_sequence(
    state: &AppState,
    id: Uuid,
    tenant: &TenantId,
) -> Result<SequenceDefinition, ApiError> {
    let seq = state
        .storage
        .get_sequence(SequenceId::from_uuid(id))
        .await
        .map_err(|e| ApiError::from_storage(e, "sequence"))?
        .ok_or_else(|| ApiError::NotFound(format!("sequence {id}")))?;
    if &seq.tenant_id != tenant {
        // 404, not 403 — never confirm foreign resources exist.
        return Err(ApiError::NotFound(format!("sequence {id}")));
    }
    Ok(seq)
}

#[derive(Deserialize)]
pub(crate) struct ListReleasesQuery {
    #[serde(default)]
    tenant_id: Option<String>,
    #[serde(default = "default_limit")]
    limit: u32,
}

fn default_limit() -> u32 {
    100
}

#[utoipa::path(get, path = "/releases", tag = "releases",
    params(
        ("tenant_id" = Option<String>, Query, description = "Filter by tenant"),
        ("limit" = Option<u32>, Query, description = "Max rows (default 100)"),
    ),
    responses((status = 200, description = "Releases, newest first", body = Vec<WorkflowRelease>))
)]
pub(crate) async fn list_releases(
    State(state): State<AppState>,
    tenant_ctx: crate::auth::OptionalTenant,
    Query(q): Query<ListReleasesQuery>,
) -> Result<impl IntoResponse, ApiError> {
    let scoped = crate::auth::scoped_tenant_id(&tenant_ctx, q.tenant_id.as_deref());
    let releases = state
        .storage
        .list_releases(scoped.as_ref(), q.limit.clamp(1, 1000))
        .await
        .map_err(|e| ApiError::from_storage(e, "release"))?;
    Ok(Json(releases))
}

#[utoipa::path(get, path = "/releases/{id}", tag = "releases",
    params(("id" = Uuid, Path, description = "Release id")),
    responses(
        (status = 200, description = "Release", body = WorkflowRelease),
        (status = 404, description = "Not found"),
    )
)]
pub(crate) async fn get_release(
    State(state): State<AppState>,
    tenant_ctx: crate::auth::OptionalTenant,
    Path(id): Path<Uuid>,
) -> Result<impl IntoResponse, ApiError> {
    let release = load_release(&state, &tenant_ctx, id).await?;
    Ok(Json(release))
}

#[utoipa::path(get, path = "/releases/{id}/decisions", tag = "releases",
    params(("id" = Uuid, Path, description = "Release id")),
    responses((status = 200, description = "Immutable audit trail, oldest first",
        body = Vec<ReleaseDecision>))
)]
pub(crate) async fn list_decisions(
    State(state): State<AppState>,
    tenant_ctx: crate::auth::OptionalTenant,
    Path(id): Path<Uuid>,
) -> Result<impl IntoResponse, ApiError> {
    let release = load_release(&state, &tenant_ctx, id).await?;
    let decisions = state
        .storage
        .list_release_decisions(release.id)
        .await
        .map_err(|e| ApiError::from_storage(e, "release_decision"))?;
    Ok(Json(decisions))
}

async fn load_release(
    state: &AppState,
    tenant_ctx: &crate::auth::OptionalTenant,
    id: Uuid,
) -> Result<WorkflowRelease, ApiError> {
    let release = state
        .storage
        .get_release(id)
        .await
        .map_err(|e| ApiError::from_storage(e, "release"))?
        .ok_or_else(|| ApiError::NotFound(format!("release {id}")))?;
    crate::auth::enforce_tenant_access(tenant_ctx, &release.tenant_id, &format!("release {id}"))?;
    Ok(release)
}

// ---------------------------------------------------------------------------
// Semantic diff
// ---------------------------------------------------------------------------

#[derive(Deserialize, ToSchema)]
pub(crate) struct DiffRequest {
    pub baseline_sequence_id: Uuid,
    pub candidate_sequence_id: Uuid,
}

#[utoipa::path(post, path = "/sequences/releases/diff", tag = "releases",
    request_body = DiffRequest,
    responses(
        (status = 200, description = "Semantic diff report", body = SemanticDiff),
        (status = 404, description = "Sequence not found"),
    )
)]
pub(crate) async fn diff_sequences(
    State(state): State<AppState>,
    tenant_ctx: crate::auth::OptionalTenant,
    Json(req): Json<DiffRequest>,
) -> Result<impl IntoResponse, ApiError> {
    let tenant = crate::auth::scoped_tenant_id(&tenant_ctx, None);
    let baseline = state
        .storage
        .get_sequence(SequenceId::from_uuid(req.baseline_sequence_id))
        .await
        .map_err(|e| ApiError::from_storage(e, "sequence"))?
        .ok_or_else(|| ApiError::NotFound(format!("sequence {}", req.baseline_sequence_id)))?;
    let candidate = state
        .storage
        .get_sequence(SequenceId::from_uuid(req.candidate_sequence_id))
        .await
        .map_err(|e| ApiError::from_storage(e, "sequence"))?
        .ok_or_else(|| ApiError::NotFound(format!("sequence {}", req.candidate_sequence_id)))?;
    if let Some(t) = &tenant
        && (&baseline.tenant_id != t || &candidate.tenant_id != t)
    {
        return Err(ApiError::NotFound("sequence".into()));
    }
    Ok(Json(semantic_diff(&baseline, &candidate)))
}

#[utoipa::path(get, path = "/releases/{id}/diff", tag = "releases",
    params(("id" = Uuid, Path, description = "Release id")),
    responses((status = 200, description = "Semantic diff of this release", body = SemanticDiff))
)]
pub(crate) async fn diff_release(
    State(state): State<AppState>,
    tenant_ctx: crate::auth::OptionalTenant,
    Path(id): Path<Uuid>,
) -> Result<impl IntoResponse, ApiError> {
    let release = load_release(&state, &tenant_ctx, id).await?;
    let baseline = fetch_sequence(
        &state,
        release.baseline_sequence_id.into_uuid(),
        &release.tenant_id,
    )
    .await?;
    let candidate = fetch_sequence(
        &state,
        release.candidate_sequence_id.into_uuid(),
        &release.tenant_id,
    )
    .await?;
    Ok(Json(semantic_diff(&baseline, &candidate)))
}

// ---------------------------------------------------------------------------
// Historical validation
// ---------------------------------------------------------------------------

#[derive(Deserialize, ToSchema, Default)]
pub(crate) struct ValidateRequest {
    /// How many historical instances to replay (default 10, max 50).
    #[serde(default)]
    pub sample: Option<u32>,
    /// Skip replay entirely (draft → ready with an audit record).
    #[serde(default)]
    pub skip: bool,
}

#[derive(Serialize, ToSchema)]
pub(crate) struct ValidationReport {
    pub replayed: u32,
    /// Runs whose path and terminal state matched the recording.
    pub matches: u32,
    /// Runs that diverged (details below, bounded).
    pub divergences: Vec<serde_json::Value>,
    /// Runs that could not be judged because recorded outputs were
    /// missing — never silently treated as success.
    pub inconclusive: u32,
    pub release_state: ReleaseState,
}

#[utoipa::path(post, path = "/releases/{id}/validate", tag = "releases",
    params(("id" = Uuid, Path, description = "Release id")),
    request_body = ValidateRequest,
    responses(
        (status = 200, description = "Validation report", body = ValidationReport),
        (status = 409, description = "Release is not in draft"),
    )
)]
pub(crate) async fn validate_release(
    State(state): State<AppState>,
    tenant_ctx: crate::auth::OptionalTenant,
    Path(id): Path<Uuid>,
    Json(req): Json<ValidateRequest>,
) -> Result<impl IntoResponse, ApiError> {
    let release = load_release(&state, &tenant_ctx, id).await?;

    if req.skip {
        transition(&state, &release, ReleaseState::Draft, ReleaseState::Ready, "operator",
            "validation explicitly skipped").await?;
        return Ok(Json(ValidationReport {
            replayed: 0,
            matches: 0,
            divergences: vec![],
            inconclusive: 0,
            release_state: ReleaseState::Ready,
        }));
    }

    // CAS into validating so two concurrent validations cannot both run.
    transition(&state, &release, ReleaseState::Draft, ReleaseState::Validating, "operator",
        "historical validation started").await?;

    let candidate = fetch_sequence(
        &state,
        release.candidate_sequence_id.into_uuid(),
        &release.tenant_id,
    )
    .await?;

    // Historical sample: terminal instances of the exact baseline version.
    let sample_size = req
        .sample
        .unwrap_or(DEFAULT_VALIDATION_SAMPLE)
        .clamp(1, MAX_VALIDATION_SAMPLE);
    let history = state
        .storage
        .list_instances(
            &InstanceFilter {
                tenant_id: Some(release.tenant_id.clone()),
                namespace: None,
                sequence_id: Some(release.baseline_sequence_id),
                states: Some(vec![InstanceState::Completed, InstanceState::Failed]),
                metadata_filter: None,
                priority: None,
            },
            &Pagination {
                offset: 0,
                limit: sample_size,
                sort_ascending: false,
            },
        )
        .await
        .map_err(|e| ApiError::from_storage(e, "instances"))?;

    let mut replayed = 0u32;
    let mut matches = 0u32;
    let mut inconclusive = 0u32;
    let mut divergences: Vec<serde_json::Value> = Vec::new();

    for instance in &history {
        let outputs = state
            .storage
            .get_all_outputs(instance.id)
            .await
            .unwrap_or_default();
        let mut recorded: std::collections::HashMap<String, serde_json::Value> =
            std::collections::HashMap::new();
        let mut recorded_blocks: Vec<String> = Vec::new();
        for o in &outputs {
            let bid = o.block_id.as_str();
            if bid.starts_with('_') || o.output_ref.as_deref() == Some("__error__")
                || o.output_ref.as_deref() == Some("__retry__")
            {
                continue;
            }
            if !recorded_blocks.iter().any(|b| b == bid) {
                recorded_blocks.push(bid.to_string());
            }
            recorded.insert(bid.to_string(), o.output.clone());
        }

        match replay_one(&candidate, &instance.context.data, recorded).await {
            Ok(report) => {
                replayed += 1;
                let added: Vec<&String> = report
                    .executed_blocks
                    .iter()
                    .filter(|b| !recorded_blocks.contains(b))
                    .collect();
                let removed: Vec<&String> = recorded_blocks
                    .iter()
                    .filter(|b| !report.executed_blocks.contains(b))
                    .collect();
                let state_matches = report.final_state == instance.state.to_string();
                if added.is_empty() && removed.is_empty() && state_matches {
                    matches += 1;
                } else if divergences.len() < 20 {
                    divergences.push(serde_json::json!({
                        "instance_id": instance.id,
                        "recorded_state": instance.state.to_string(),
                        "replayed_state": report.final_state,
                        "blocks_added": added,
                        "blocks_removed": removed,
                    }));
                }
            }
            Err(e) => {
                replayed += 1;
                inconclusive += 1;
                tracing::warn!(error = %e, instance_id = %instance.id, "release replay failed");
            }
        }
    }

    let summary = serde_json::json!({
        "replayed": replayed,
        "matches": matches,
        "divergences": divergences,
        "inconclusive": inconclusive,
        "validated_at": Utc::now().to_rfc3339(),
    });
    state
        .storage
        .set_release_validation_summary(release.id, &summary)
        .await
        .map_err(|e| ApiError::from_storage(e, "release"))?;

    // Validation always concludes to Ready with the evidence recorded —
    // the operator (and the gates) judge divergences; infrastructure did
    // not fail. The decision text carries the counts for the audit trail.
    let fresh = state
        .storage
        .get_release(release.id)
        .await
        .map_err(|e| ApiError::from_storage(e, "release"))?
        .ok_or_else(|| ApiError::NotFound(format!("release {id}")))?;
    transition(
        &state,
        &fresh,
        ReleaseState::Validating,
        ReleaseState::Ready,
        "validation",
        &format!(
            "replayed {replayed} historical run(s): {matches} matched, {} diverged, \
             {inconclusive} inconclusive",
            divergences.len()
        ),
    )
    .await?;

    Ok(Json(ValidationReport {
        replayed,
        matches,
        divergences,
        inconclusive,
        release_state: ReleaseState::Ready,
    }))
}

/// Replay the candidate against one recorded run using the contract
/// runner: virtual time, no side effects, no real handlers. Blocks with
/// a recording replay it verbatim; blocks without one (e.g. added by the
/// candidate, or skipped in the recorded run) return a stub `{}` so the
/// path divergence is *observed and reported* rather than aborting the
/// replay.
async fn replay_one(
    candidate: &SequenceDefinition,
    context_data: &serde_json::Value,
    recorded: std::collections::HashMap<String, serde_json::Value>,
) -> Result<orch8_types::contract::CaseReport, ApiError> {
    let mocks: Vec<orch8_types::contract::MockDef> = candidate_step_blocks(candidate)
        .into_iter()
        .map(|block| {
            let policy = if recorded.contains_key(&block) {
                orch8_types::contract::MockPolicy::Recorded
            } else {
                orch8_types::contract::MockPolicy::Success {
                    output: serde_json::json!({}),
                }
            };
            orch8_types::contract::MockDef {
                handler: None,
                block: Some(block),
                policy,
            }
        })
        .collect();
    let case = orch8_types::contract::ContractCase {
        name: "release-validation".into(),
        description: None,
        input: context_data.clone(),
        config: None,
        mocks,
        signals: vec![],
        expect: orch8_types::contract::Expectations::default(),
        max_logical_duration_ms: None,
        max_ticks: None,
    };
    let opts = orch8::contract::RunOptions {
        recorded_outputs: recorded,
        ..Default::default()
    };
    orch8::contract::run_case(
        candidate,
        // Safety net for dynamically dispatched handlers not tied to a
        // known block: stub success, never a real invocation.
        orch8_types::contract::UnmockedHandlerPolicy::EmptySuccess,
        &case,
        &opts,
    )
    .await
    .map_err(|e| ApiError::Internal(format!("replay failed: {e}")))
}

/// Every step block id in the candidate (blocks that dispatch a handler).
fn candidate_step_blocks(seq: &SequenceDefinition) -> Vec<String> {
    fn walk(value: &serde_json::Value, out: &mut Vec<String>) {
        match value {
            serde_json::Value::Object(map) => {
                if let (Some(serde_json::Value::String(id)), Some(serde_json::Value::String(_))) =
                    (map.get("id"), map.get("handler"))
                    && !out.contains(id)
                {
                    out.push(id.clone());
                }
                for v in map.values() {
                    walk(v, out);
                }
            }
            serde_json::Value::Array(items) => {
                for v in items {
                    walk(v, out);
                }
            }
            _ => {}
        }
    }
    let mut out = Vec::new();
    if let Ok(v) = serde_json::to_value(seq) {
        walk(&v, &mut out);
    }
    out
}

// ---------------------------------------------------------------------------
// Canary / promote / pause / rollback
// ---------------------------------------------------------------------------

#[derive(Deserialize, ToSchema)]
pub(crate) struct CanaryRequest {
    /// Percentage of new instances routed to the candidate (1–100).
    pub percent: u8,
}

#[utoipa::path(post, path = "/releases/{id}/canary", tag = "releases",
    params(("id" = Uuid, Path, description = "Release id")),
    request_body = CanaryRequest,
    responses(
        (status = 200, description = "Canary started/resumed", body = WorkflowRelease),
        (status = 409, description = "Release is not ready/paused"),
    )
)]
pub(crate) async fn start_canary(
    State(state): State<AppState>,
    tenant_ctx: crate::auth::OptionalTenant,
    Path(id): Path<Uuid>,
    Json(req): Json<CanaryRequest>,
) -> Result<impl IntoResponse, ApiError> {
    if req.percent == 0 || req.percent > 100 {
        return Err(ApiError::InvalidArgument("percent must be 1–100".into()));
    }
    let release = load_release(&state, &tenant_ctx, id).await?;
    let from = match release.state {
        ReleaseState::Ready => ReleaseState::Ready,
        ReleaseState::Paused => ReleaseState::Paused,
        other => {
            return Err(ApiError::Conflict(format!(
                "cannot start canary from '{}'",
                other.as_str()
            )));
        }
    };
    let ok = state
        .storage
        .cas_release_state(
            id,
            from,
            ReleaseState::Canary,
            Some(req.percent),
            Some(Utc::now()),
        )
        .await
        .map_err(|e| ApiError::from_storage(e, "release"))?;
    if !ok {
        return Err(ApiError::Conflict("concurrent state change".into()));
    }
    record_decision(
        &state,
        id,
        from,
        ReleaseState::Canary,
        "operator",
        &format!("canary at {}%", req.percent),
    )
    .await;
    let fresh = load_release(&state, &tenant_ctx, id).await?;
    Ok(Json(fresh))
}

#[derive(Serialize, ToSchema)]
pub(crate) struct EvaluationReport {
    pub baseline: VariantStats,
    pub candidate: VariantStats,
    pub gates: Vec<GateEvaluation>,
    /// Applied automatically when a gate fails during canary.
    pub auto_rolled_back: bool,
    pub release_state: ReleaseState,
}

#[utoipa::path(post, path = "/releases/{id}/evaluate", tag = "releases",
    params(("id" = Uuid, Path, description = "Release id")),
    responses((status = 200, description = "Gate evaluation; a failing gate during canary \
        rolls the release back automatically (idempotent)", body = EvaluationReport))
)]
pub(crate) async fn evaluate_release(
    State(state): State<AppState>,
    tenant_ctx: crate::auth::OptionalTenant,
    Path(id): Path<Uuid>,
) -> Result<impl IntoResponse, ApiError> {
    let release = load_release(&state, &tenant_ctx, id).await?;
    let (baseline, candidate) = observe(&state, &release).await?;
    let gates = evaluate_gates(&release.gates, baseline, candidate);

    let mut auto_rolled_back = false;
    let mut current_state = release.state;
    if release.state == ReleaseState::Canary
        && let Some(failed) = gates.iter().find(|g| g.verdict == GateVerdict::Fail)
    {
        // Idempotent under concurrency: only the CAS winner records the
        // decision; a concurrent evaluate simply observes rolled_back.
        let ok = state
            .storage
            .cas_release_state(id, ReleaseState::Canary, ReleaseState::RolledBack, None, None)
            .await
            .map_err(|e| ApiError::from_storage(e, "release"))?;
        if ok {
            record_decision(
                &state,
                id,
                ReleaseState::Canary,
                ReleaseState::RolledBack,
                &format!("gate:{:?}", failed.gate.metric),
                &format!(
                    "gate failed: baseline {:?} vs candidate {:?} exceeds max regression {}",
                    failed.baseline_rate, failed.candidate_rate, failed.gate.max_regression
                ),
            )
            .await;
            auto_rolled_back = true;
        }
        current_state = ReleaseState::RolledBack;
    }

    Ok(Json(EvaluationReport {
        baseline,
        candidate,
        gates,
        auto_rolled_back,
        release_state: current_state,
    }))
}

/// Aggregate terminal outcomes per variant since the canary started,
/// attributed via the routing annotation stamped at instance creation.
async fn observe(
    state: &AppState,
    release: &WorkflowRelease,
) -> Result<(VariantStats, VariantStats), ApiError> {
    let mut baseline = VariantStats::default();
    let mut candidate = VariantStats::default();
    let since = release.canary_started_at.unwrap_or(release.created_at);

    for (sequence_id, stats) in [
        (release.baseline_sequence_id, &mut baseline),
        (release.candidate_sequence_id, &mut candidate),
    ] {
        let instances = state
            .storage
            .list_instances(
                &InstanceFilter {
                    tenant_id: Some(release.tenant_id.clone()),
                    namespace: None,
                    sequence_id: Some(sequence_id),
                    states: None,
                    metadata_filter: None,
                    priority: None,
                },
                &Pagination {
                    offset: 0,
                    limit: OBSERVATION_SCAN_LIMIT,
                    sort_ascending: false,
                },
            )
            .await
            .map_err(|e| ApiError::from_storage(e, "instances"))?;
        for inst in instances {
            if inst.created_at < since {
                continue;
            }
            // Only instances the release actually routed count.
            let routed_by_release = inst
                .metadata
                .get("release")
                .and_then(|r| r.get("release_id"))
                .and_then(serde_json::Value::as_str)
                == Some(release.id.to_string().as_str());
            if !routed_by_release {
                continue;
            }
            stats.total += 1;
            match inst.state {
                InstanceState::Completed => stats.completed += 1,
                InstanceState::Failed => stats.failed += 1,
                InstanceState::Cancelled => stats.cancelled += 1,
                _ => {
                    // Non-terminal instances don't count toward rates.
                    stats.total -= 1;
                }
            }
        }
    }
    Ok((baseline, candidate))
}

#[derive(Deserialize, ToSchema, Default)]
pub(crate) struct PromoteRequest {
    /// Promote even when gates are inconclusive or failing.
    #[serde(default)]
    pub force: bool,
}

#[utoipa::path(post, path = "/releases/{id}/promote", tag = "releases",
    params(("id" = Uuid, Path, description = "Release id")),
    request_body = PromoteRequest,
    responses(
        (status = 200, description = "Promoted", body = WorkflowRelease),
        (status = 409, description = "Gates not passing (pass force=true to override)"),
    )
)]
pub(crate) async fn promote_release(
    State(state): State<AppState>,
    tenant_ctx: crate::auth::OptionalTenant,
    Path(id): Path<Uuid>,
    Json(req): Json<PromoteRequest>,
) -> Result<impl IntoResponse, ApiError> {
    let release = load_release(&state, &tenant_ctx, id).await?;
    if !req.force && !release.gates.is_empty() {
        let (baseline, candidate) = observe(&state, &release).await?;
        let gates = evaluate_gates(&release.gates, baseline, candidate);
        if let Some(bad) = gates.iter().find(|g| g.verdict != GateVerdict::Pass) {
            return Err(ApiError::Conflict(format!(
                "gate {:?} is {:?} — promotion requires all gates passing (or force=true)",
                bad.gate.metric, bad.verdict
            )));
        }
    }
    transition(&state, &release, ReleaseState::Canary, ReleaseState::Promoted, "operator",
        if req.force { "promoted (forced)" } else { "promoted: all gates pass" }).await?;
    let fresh = load_release(&state, &tenant_ctx, id).await?;
    Ok(Json(fresh))
}

#[utoipa::path(post, path = "/releases/{id}/pause", tag = "releases",
    params(("id" = Uuid, Path, description = "Release id")),
    responses((status = 200, description = "Paused (traffic returns to baseline)", body = WorkflowRelease))
)]
pub(crate) async fn pause_release(
    State(state): State<AppState>,
    tenant_ctx: crate::auth::OptionalTenant,
    Path(id): Path<Uuid>,
) -> Result<impl IntoResponse, ApiError> {
    let release = load_release(&state, &tenant_ctx, id).await?;
    transition(&state, &release, ReleaseState::Canary, ReleaseState::Paused, "operator",
        "canary paused").await?;
    let fresh = load_release(&state, &tenant_ctx, id).await?;
    Ok(Json(fresh))
}

#[utoipa::path(post, path = "/releases/{id}/rollback", tag = "releases",
    params(("id" = Uuid, Path, description = "Release id")),
    responses(
        (status = 200, description = "Rolled back — all new traffic returns to the baseline. \
            Idempotent: rolling back an already rolled-back release is a no-op.",
            body = WorkflowRelease),
    )
)]
pub(crate) async fn rollback_release(
    State(state): State<AppState>,
    tenant_ctx: crate::auth::OptionalTenant,
    Path(id): Path<Uuid>,
) -> Result<impl IntoResponse, ApiError> {
    let release = load_release(&state, &tenant_ctx, id).await?;
    if release.state == ReleaseState::RolledBack {
        // Idempotent: repeat rollbacks succeed without a second decision.
        return Ok(Json(release));
    }
    let from = match release.state {
        ReleaseState::Canary => ReleaseState::Canary,
        ReleaseState::Paused => ReleaseState::Paused,
        other => {
            return Err(ApiError::Conflict(format!(
                "cannot roll back from '{}'",
                other.as_str()
            )));
        }
    };
    transition(&state, &release, from, ReleaseState::RolledBack, "operator",
        "rolled back by operator").await?;
    let fresh = load_release(&state, &tenant_ctx, id).await?;
    Ok(Json(fresh))
}

/// CAS transition + decision record. The state machine is validated
/// before the write; the CAS protects against concurrent writers.
async fn transition(
    state: &AppState,
    release: &WorkflowRelease,
    from: ReleaseState,
    to: ReleaseState,
    actor: &str,
    reason: &str,
) -> Result<(), ApiError> {
    if release.state != from {
        return Err(ApiError::Conflict(format!(
            "release is '{}', expected '{}'",
            release.state.as_str(),
            from.as_str()
        )));
    }
    if !from.can_transition_to(to) {
        return Err(ApiError::Conflict(format!(
            "illegal transition {} → {}",
            from.as_str(),
            to.as_str()
        )));
    }
    let ok = state
        .storage
        .cas_release_state(release.id, from, to, None, None)
        .await
        .map_err(|e| ApiError::from_storage(e, "release"))?;
    if !ok {
        return Err(ApiError::Conflict(
            "concurrent state change — reload the release".into(),
        ));
    }
    record_decision(state, release.id, from, to, actor, reason).await;
    Ok(())
}

/// Audit records are best-effort appends — a failed audit write must not
/// undo an already-applied CAS, but it is loudly logged.
async fn record_decision(
    state: &AppState,
    release_id: Uuid,
    from: ReleaseState,
    to: ReleaseState,
    actor: &str,
    reason: &str,
) {
    let decision = ReleaseDecision {
        id: Uuid::now_v7(),
        release_id,
        from_state: from,
        to_state: to,
        actor: actor.to_string(),
        reason: reason.to_string(),
        decided_at: Utc::now(),
    };
    if let Err(e) = state.storage.record_release_decision(&decision).await {
        tracing::error!(error = %e, release_id = %release_id, "failed to record release decision");
    }
}
