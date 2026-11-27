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
use axum::routing::{get, post, put};
use axum::{Json, Router};
use chrono::{DateTime, Utc};
use orch8_storage::release_inventory::ReleasePageFilter;
use serde::{Deserialize, Serialize};
use utoipa::ToSchema;
use uuid::Uuid;

use orch8_engine::release_diff::semantic_diff;
use orch8_types::filter::{InstanceFilter, Pagination};
use orch8_types::ids::{SequenceId, TenantId};
use orch8_types::instance::InstanceState;
use orch8_types::release::{
    GateEvaluation, GateVerdict, InFlightPolicy, ReleaseDecision, ReleaseGate, ReleaseState,
    ReleaseTarget, ReleaseVariant, SemanticDiff, VariantStats, WorkflowRelease, evaluate_gates,
};
use orch8_types::sequence::SequenceDefinition;

use crate::AppState;
use crate::error::ApiError;

pub(crate) mod replay_fixture;

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
        .route(
            "/releases/{id}/divergences/{instance_id}/fixture",
            post(replay_fixture::export_replay_fixture),
        )
        .route("/releases/{id}/canary", post(start_canary))
        .route(
            "/releases/{id}/executor-readiness",
            get(release_executor_readiness).post(transition_with_executor_readiness),
        )
        .route("/releases/{id}/evaluate", post(evaluate_release))
        .route("/releases/{id}/gate-preview", get(preview_release_gates))
        .route("/releases/{id}/promote", post(promote_release))
        .route("/releases/{id}/pause", post(pause_release))
        .route("/releases/{id}/rollback", post(rollback_release))
        .route("/releases/{id}/target", put(set_release_target))
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
    /// Optional sub-tenant staged rollout: sub-tenant scoped instances are
    /// routed by sub-tenant instead of per-instance cohorts; unscoped
    /// instances stay on the baseline until promotion. Unknown fields in
    /// the target are rejected (the persisted type itself is lenient).
    #[serde(default, deserialize_with = "orch8_types::strict::deserialize")]
    pub target: Option<ReleaseTarget>,
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
    if let Some(target) = &req.target {
        target.validate().map_err(ApiError::InvalidArgument)?;
    }

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
        // Cohort assignment hashes the release ID together with the cohort
        // key. UUIDv7 IDs created close together share a long timestamp
        // prefix, which weakens separation with the legacy FNV hash. A
        // random release ID improves new-release independence without
        // reshuffling cohorts for already-persisted releases.
        id: Uuid::new_v4(),
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
        target: req.target,
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
    view: Option<String>,
    search: Option<String>,
    release_state: Option<ReleaseState>,
    before_created_at: Option<DateTime<Utc>>,
    before_id: Option<Uuid>,
    page_size: Option<u32>,
}

#[derive(Serialize, ToSchema)]
struct ReleaseCursor {
    created_at: DateTime<Utc>,
    id: Uuid,
}

#[derive(Serialize, ToSchema)]
struct ReleaseInventoryPage {
    view: &'static str,
    tenant_id: String,
    search: Option<String>,
    release_state: Option<ReleaseState>,
    before: Option<ReleaseCursor>,
    next_before: Option<ReleaseCursor>,
    page_size: u32,
    items: Vec<WorkflowRelease>,
}

#[derive(Serialize, ToSchema)]
#[serde(untagged)]
enum ReleaseListResponse {
    Inventory(ReleaseInventoryPage),
    Legacy(Vec<WorkflowRelease>),
}

fn default_limit() -> u32 {
    100
}

#[utoipa::path(get, path = "/releases", tag = "releases",
    params(
        ("tenant_id" = Option<String>, Query, description = "Filter by tenant"),
        ("limit" = Option<u32>, Query, description = "Max rows (default 100)"),
        ("view" = Option<String>, Query, description = "Use page for tenant-scoped cursor pagination"),
        ("page_size" = Option<u32>, Query, description = "Page size for page view (default 25, maximum 100)"),
        ("search" = Option<String>, Query, description = "Whitespace-separated literal terms, ignoring ASCII letter case"),
        ("release_state" = Option<ReleaseState>, Query, description = "Filter page view by release state"),
        ("before_created_at" = Option<DateTime<Utc>>, Query, description = "Creation timestamp from next_before"),
        ("before_id" = Option<Uuid>, Query, description = "Release UUID from next_before"),
    ),
    responses((status = 200, description = "Releases, newest first", body = ReleaseListResponse))
)]
pub(crate) async fn list_releases(
    State(state): State<AppState>,
    tenant_ctx: crate::auth::OptionalTenant,
    Query(q): Query<ListReleasesQuery>,
) -> Result<impl IntoResponse, ApiError> {
    let scoped = crate::auth::scoped_tenant_id(&tenant_ctx, q.tenant_id.as_deref());
    if let Some(view) = q.view.as_deref() {
        if view != "page" {
            return Err(ApiError::InvalidArgument(
                "Unknown release inventory view".into(),
            ));
        }
        let tenant = scoped.as_ref().ok_or_else(|| {
            ApiError::InvalidArgument("Release pagination requires one tenant".into())
        })?;
        let before = match (q.before_created_at, q.before_id) {
            (Some(created_at), Some(id)) => Some((created_at, id)),
            (None, None) => None,
            _ => {
                return Err(ApiError::InvalidArgument(
                    "Supply both release cursor fields".into(),
                ));
            }
        };
        let terms: Vec<String> = q
            .search
            .as_deref()
            .unwrap_or_default()
            .split_whitespace()
            .map(str::to_ascii_lowercase)
            .collect();
        if q.search
            .as_ref()
            .is_some_and(|value| value.len() > 256 || value.contains('\0'))
            || terms.len() > 16
        {
            return Err(ApiError::InvalidArgument(
                "Choose up to 16 search terms using at most 256 bytes".into(),
            ));
        }
        let page_size = q.page_size.unwrap_or(25);
        if !(1..=100).contains(&page_size) {
            return Err(ApiError::InvalidArgument(
                "Release page size must be 1–100".into(),
            ));
        }
        let query = ReleasePageFilter {
            before,
            state: q.release_state,
            terms,
        };
        let mut items = state
            .storage
            .list_release_page(tenant, &query, page_size + 1)
            .await
            .map_err(|e| ApiError::from_storage(e, "release"))?;
        let next_before = if items.len() > page_size as usize {
            items.truncate(page_size as usize);
            items.last().map(|row| ReleaseCursor {
                created_at: row.created_at,
                id: row.id,
            })
        } else {
            None
        };
        return Ok(Json(ReleaseListResponse::Inventory(ReleaseInventoryPage {
            view: "page",
            tenant_id: tenant.as_str().into(),
            search: q.search,
            release_state: q.release_state,
            before: before.map(|(created_at, id)| ReleaseCursor { created_at, id }),
            next_before,
            page_size,
            items,
        })));
    }
    let releases = state
        .storage
        .list_releases(scoped.as_ref(), q.limit.clamp(1, 1000))
        .await
        .map_err(|e| ApiError::from_storage(e, "release"))?;
    Ok(Json(ReleaseListResponse::Legacy(releases)))
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
        transition(
            &state,
            &release,
            ReleaseState::Draft,
            ReleaseState::Ready,
            "operator",
            "validation explicitly skipped",
        )
        .await?;
        return Ok(Json(ValidationReport {
            replayed: 0,
            matches: 0,
            divergences: vec![],
            inconclusive: 0,
            release_state: ReleaseState::Ready,
        }));
    }

    // CAS into validating so two concurrent validations cannot both run.
    transition(
        &state,
        &release,
        ReleaseState::Draft,
        ReleaseState::Validating,
        "operator",
        "historical validation started",
    )
    .await?;

    // Any failure past this point must not strand the release in
    // `Validating` — no API transition leads out of it. Roll back to
    // `Draft` (best effort) so the operator can fix the cause and
    // re-validate.
    match run_validation(&state, &release, req.sample).await {
        Ok(report) => Ok(Json(report)),
        Err(e) => {
            rollback_validating(&state, &release).await;
            Err(e)
        }
    }
}

/// The replay pipeline that runs while the release is in `Validating`.
/// Kept separate from `validate_release` so every error path funnels
/// through the Validating → Draft rollback above.
#[allow(clippy::too_many_lines)] // sample → replay loop → summary → CAS, one linear pipeline
async fn run_validation(
    state: &AppState,
    release: &WorkflowRelease,
    sample: Option<u32>,
) -> Result<ValidationReport, ApiError> {
    let candidate = fetch_sequence(
        state,
        release.candidate_sequence_id.into_uuid(),
        &release.tenant_id,
    )
    .await?;

    // Historical sample: terminal instances of the exact baseline version.
    let sample_size = sample
        .unwrap_or(DEFAULT_VALIDATION_SAMPLE)
        .clamp(1, MAX_VALIDATION_SAMPLE);
    let history = state
        .storage
        .list_instances(
            &InstanceFilter {
                sub_tenant: None,
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
            .map_err(|e| ApiError::from_storage(e, "block_outputs"))?;
        let mut recorded: std::collections::HashMap<String, serde_json::Value> =
            std::collections::HashMap::new();
        let mut recorded_blocks: Vec<String> = Vec::new();
        for o in outputs {
            let bid = o.block_id.as_str();
            if !is_replay_output(bid, o.output_ref.as_deref()) {
                continue;
            }
            match recorded.entry(bid.to_string()) {
                std::collections::hash_map::Entry::Vacant(entry) => {
                    recorded_blocks.push(entry.key().clone());
                    entry.insert(o.output);
                }
                std::collections::hash_map::Entry::Occupied(mut entry) => {
                    entry.insert(o.output);
                }
            }
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
        .ok_or_else(|| ApiError::NotFound(format!("release {}", release.id)))?;
    transition(
        state,
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

    Ok(ValidationReport {
        replayed,
        matches,
        divergences,
        inconclusive,
        release_state: ReleaseState::Ready,
    })
}

/// Best-effort recovery from a failed validation: CAS the release back to
/// `Draft` so it can be re-validated. `Validating → Draft` is deliberately
/// not part of the operator-facing state machine (`can_transition_to`) — it
/// exists only as this internal recovery path, so it goes through the
/// storage CAS directly. A failed rollback is logged, never propagated: the
/// caller needs the original validation error.
async fn rollback_validating(state: &AppState, release: &WorkflowRelease) {
    match state
        .storage
        .cas_release_state(
            release.id,
            ReleaseState::Validating,
            ReleaseState::Draft,
            None,
            None,
        )
        .await
    {
        Ok(true) => {
            record_decision(
                state,
                release.id,
                ReleaseState::Validating,
                ReleaseState::Draft,
                "validation",
                "validation failed — returned to draft for re-validation",
            )
            .await;
        }
        Ok(false) => {
            tracing::warn!(
                release_id = %release.id,
                "validation failed but release was no longer Validating — left as-is"
            );
        }
        Err(e) => {
            tracing::error!(
                error = %e,
                release_id = %release.id,
                "validation failed and the Validating → Draft rollback did too — \
                 release is stranded in Validating"
            );
        }
    }
}

/// Replay the candidate against one recorded run using the contract
/// runner: virtual time, no side effects, no real handlers. Blocks with
/// a recording replay it verbatim; blocks without one (e.g. added by the
/// candidate, or skipped in the recorded run) return a stub `{}` so the
/// path divergence is *observed and reported* rather than aborting the
/// replay.
fn is_replay_output(block: &str, reference: Option<&str>) -> bool {
    !block.starts_with('_')
        && !matches!(
            reference,
            Some("__error__" | "__retry__" | "__in_progress__")
        )
}

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
                definition: None,
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
        initial_outputs: std::collections::BTreeMap::new(),
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
    let mut ids = Vec::new();
    for block in seq
        .blocks
        .iter()
        .chain(seq.on_failure.iter().flatten())
        .chain(seq.on_cancel.iter().flatten())
    {
        orch8_types::sequence::visit_steps(block, &mut |step| {
            ids.push(step.id.to_string());
        });
    }
    ids
}

// ---------------------------------------------------------------------------
// Canary / promote / pause / rollback
// ---------------------------------------------------------------------------

#[derive(Deserialize, ToSchema)]
pub(crate) struct CanaryRequest {
    /// Percentage of new instances routed to the candidate (1–100).
    pub percent: u8,
    /// Require these native executor nodes to have prepared the candidate.
    #[serde(default)]
    pub ready_executor_ids: Vec<Uuid>,
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
    require_candidate_ready_executors(&state, &release, &req.ready_executor_ids).await?;
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
        &describe_executor_check(
            format!("canary at {}%", req.percent),
            &req.ready_executor_ids,
        ),
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

#[derive(Serialize, ToSchema)]
pub(crate) struct ReleaseGatePreview {
    pub release: WorkflowRelease,
    pub observed_at: DateTime<Utc>,
    pub scan_limit_per_version: u32,
    pub evaluation: EvaluationReport,
}

#[utoipa::path(get, path = "/releases/{id}/gate-preview", tag = "releases",
    params(("id" = Uuid, Path, description = "Release id")),
    responses(
        (status = 200, description = "Read-only gate preview; never changes routing", body = ReleaseGatePreview),
        (status = 409, description = "Release changed during observation or is outside canary"),
    )
)]
pub(crate) async fn preview_release_gates(
    State(state): State<AppState>,
    tenant_ctx: crate::auth::OptionalTenant,
    Path(id): Path<Uuid>,
) -> Result<impl IntoResponse, ApiError> {
    let release = load_release(&state, &tenant_ctx, id).await?;
    if release.state != ReleaseState::Canary {
        return Err(ApiError::Conflict(
            "gate preview requires an active canary".into(),
        ));
    }
    let (baseline, candidate) = observe(&state, &release).await?;
    let gates = evaluate_gates(&release.gates, baseline, candidate);
    if load_release(&state, &tenant_ctx, id).await? != release {
        return Err(ApiError::Conflict(
            "release changed during gate preview".into(),
        ));
    }
    Ok(Json(ReleaseGatePreview {
        release,
        observed_at: Utc::now(),
        scan_limit_per_version: OBSERVATION_SCAN_LIMIT,
        evaluation: EvaluationReport {
            baseline,
            candidate,
            gates,
            auto_rolled_back: false,
            release_state: ReleaseState::Canary,
        },
    }))
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
            .cas_release_state(
                id,
                ReleaseState::Canary,
                ReleaseState::RolledBack,
                None,
                None,
            )
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
    for (variant, stats) in [
        (ReleaseVariant::Baseline, &mut baseline),
        (ReleaseVariant::Candidate, &mut candidate),
    ] {
        let states = state
            .storage
            .list_release_terminal_states(release, variant, OBSERVATION_SCAN_LIMIT)
            .await
            .map_err(|e| ApiError::from_storage(e, "instances"))?;
        for observed in states {
            match observed {
                InstanceState::Completed => stats.completed += 1,
                InstanceState::Failed => stats.failed += 1,
                InstanceState::Cancelled => stats.cancelled += 1,
                _ => {
                    return Err(ApiError::Conflict(
                        "invalid release gate observation".into(),
                    ));
                }
            }
            stats.total += 1;
        }
    }
    Ok((baseline, candidate))
}

#[derive(Deserialize, ToSchema, Default)]
pub(crate) struct PromoteRequest {
    /// Promote even when gates are inconclusive or failing.
    #[serde(default)]
    pub force: bool,
    /// Require a fresh candidate readiness acknowledgement from each node.
    #[serde(default)]
    pub ready_executor_ids: Vec<Uuid>,
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
    require_candidate_ready_executors(&state, &release, &req.ready_executor_ids).await?;
    if !req.force && !release.gates.is_empty() {
        let (baseline, candidate) = observe(&state, &release).await?;
        let gates = evaluate_gates(&release.gates, baseline, candidate);
        if let Some(bad) = gates.iter().find(|g| g.verdict != GateVerdict::Pass) {
            return Err(ApiError::Conflict(format!(
                "gate {:?} is {:?} — promotion requires all gates passing (or force=true)",
                bad.gate.metric, bad.verdict
            )));
        }
        require_candidate_ready_executors(&state, &release, &req.ready_executor_ids).await?;
    }
    let reason = if req.force {
        "promoted (forced)"
    } else {
        "promoted: all gates pass"
    };
    transition(
        &state,
        &release,
        ReleaseState::Canary,
        ReleaseState::Promoted,
        "operator",
        &describe_executor_check(reason.to_owned(), &req.ready_executor_ids),
    )
    .await?;
    let fresh = load_release(&state, &tenant_ctx, id).await?;
    Ok(Json(fresh))
}

async fn require_candidate_ready_executors(
    state: &AppState,
    release: &WorkflowRelease,
    requested: &[Uuid],
) -> Result<(), ApiError> {
    if requested.is_empty() {
        return Ok(());
    }
    if requested.len() > 64 {
        return Err(ApiError::InvalidArgument(
            "at most 64 designated executors are supported".into(),
        ));
    }
    let ready = state
        .storage
        .list_wasm_ready_nodes(release.candidate_sequence_id, Utc::now())
        .await
        .map_err(|error| ApiError::from_storage(error, "executor readiness"))?;
    let pending: Vec<_> = requested
        .iter()
        .filter(|id| !ready.contains(id))
        .map(ToString::to_string)
        .collect();
    if !pending.is_empty() {
        return Err(ApiError::Conflict(format!(
            "candidate revision {} awaits WASM prewarm on executors {}",
            release.candidate_sequence_id,
            pending.join(", ")
        )));
    }
    Ok(())
}

fn describe_executor_check(reason: String, requested: &[Uuid]) -> String {
    if requested.is_empty() {
        return reason;
    }
    let count = requested
        .iter()
        .collect::<std::collections::HashSet<_>>()
        .len();
    let noun = if count == 1 { "executor" } else { "executors" };
    format!("{reason} · readiness verified for {count} {noun}")
}

#[derive(Serialize, ToSchema)]
struct ReleaseExecutorStatus {
    id: Uuid,
    name: String,
    ready: bool,
}

#[derive(Serialize, ToSchema)]
struct ReleaseExecutorReadiness {
    release_id: Uuid,
    tenant_id: String,
    candidate_sequence_id: SequenceId,
    checked_at: DateTime<Utc>,
    executors: Vec<ReleaseExecutorStatus>,
}

#[utoipa::path(get, path = "/releases/{id}/executor-readiness", tag = "releases",
    params(("id" = Uuid, Path, description = "Release id")),
    responses((status = 200, body = ReleaseExecutorReadiness), (status = 404, description = "Release not found"))
)]
pub(crate) async fn release_executor_readiness(
    State(state): State<AppState>,
    admin: crate::auth::OptionalAdmin,
    tenant_ctx: crate::auth::OptionalTenant,
    Path(id): Path<Uuid>,
) -> Result<impl IntoResponse, ApiError> {
    crate::api_keys::require_admin(&admin)?;
    let release = load_release(&state, &tenant_ctx, id).await?;
    let nodes = state
        .storage
        .list_nodes()
        .await
        .map_err(|error| ApiError::from_storage(error, "executor inventory"))?;
    let checked_at = Utc::now();
    let ready: std::collections::HashSet<_> = state
        .storage
        .list_wasm_ready_nodes(release.candidate_sequence_id, checked_at)
        .await
        .map_err(|error| ApiError::from_storage(error, "executor readiness"))?
        .into_iter()
        .collect();
    let mut executors: Vec<_> = nodes
        .into_iter()
        .map(|node| ReleaseExecutorStatus {
            id: node.id,
            name: node.name,
            ready: ready.contains(&node.id),
        })
        .collect();
    executors.sort_unstable_by_key(|node| node.id);
    Ok(Json(ReleaseExecutorReadiness {
        release_id: id,
        tenant_id: release.tenant_id.to_string(),
        candidate_sequence_id: release.candidate_sequence_id,
        checked_at,
        executors,
    }))
}

#[derive(Deserialize, ToSchema)]
#[serde(tag = "action", rename_all = "snake_case", deny_unknown_fields)]
pub(crate) enum ExecutorCheckedTransition {
    Canary {
        percent: u8,
        ready_executor_ids: Vec<Uuid>,
    },
    Promote {
        ready_executor_ids: Vec<Uuid>,
        /// Override promotion gates while still requiring designated executors.
        #[serde(default)]
        force: bool,
    },
}

#[utoipa::path(post, path = "/releases/{id}/executor-readiness", tag = "releases",
    params(("id" = Uuid, Path, description = "Release id")),
    request_body = ExecutorCheckedTransition,
    responses(
        (status = 200, body = WorkflowRelease),
        (status = 400, description = "Invalid executor selection"),
        (status = 409, description = "Readiness or release conditions are not satisfied"),
    )
)]
pub(crate) async fn transition_with_executor_readiness(
    state: State<AppState>,
    admin: crate::auth::OptionalAdmin,
    tenant_ctx: crate::auth::OptionalTenant,
    path: Path<Uuid>,
    Json(req): Json<ExecutorCheckedTransition>,
) -> Result<axum::response::Response, ApiError> {
    crate::api_keys::require_admin(&admin)?;
    let ids = match &req {
        ExecutorCheckedTransition::Canary {
            ready_executor_ids, ..
        }
        | ExecutorCheckedTransition::Promote {
            ready_executor_ids, ..
        } => ready_executor_ids,
    };
    if ids.is_empty()
        || ids.len() > 64
        || ids.iter().collect::<std::collections::HashSet<_>>().len() != ids.len()
    {
        return Err(ApiError::InvalidArgument(
            "select 1–64 distinct designated executors".into(),
        ));
    }
    match req {
        ExecutorCheckedTransition::Canary {
            percent,
            ready_executor_ids,
        } => Ok(start_canary(
            state,
            tenant_ctx,
            path,
            Json(CanaryRequest {
                percent,
                ready_executor_ids,
            }),
        )
        .await?
        .into_response()),
        ExecutorCheckedTransition::Promote {
            ready_executor_ids,
            force,
        } => Ok(promote_release(
            state,
            tenant_ctx,
            path,
            Json(PromoteRequest {
                force,
                ready_executor_ids,
            }),
        )
        .await?
        .into_response()),
    }
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
    transition(
        &state,
        &release,
        ReleaseState::Canary,
        ReleaseState::Paused,
        "operator",
        "canary paused",
    )
    .await?;
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
    transition(
        &state,
        &release,
        from,
        ReleaseState::RolledBack,
        "operator",
        "rolled back by operator",
    )
    .await?;
    let fresh = load_release(&state, &tenant_ctx, id).await?;
    Ok(Json(fresh))
}

#[utoipa::path(put, path = "/releases/{id}/target", tag = "releases",
    params(("id" = Uuid, Path, description = "Release id")),
    request_body(content = Option<ReleaseTarget>,
        description = "Sub-tenant rollout target, or null to return to per-instance cohorts"),
    responses(
        (status = 200, description = "Target replaced (audited)", body = WorkflowRelease),
        (status = 400, description = "Invalid target"),
        (status = 409, description = "Release is promoted or rolled back"),
    )
)]
pub(crate) async fn set_release_target(
    State(state): State<AppState>,
    tenant_ctx: crate::auth::OptionalTenant,
    Path(id): Path<Uuid>,
    Json(orch8_types::strict::Strict(target)): Json<
        orch8_types::strict::Strict<Option<ReleaseTarget>>,
    >,
) -> Result<impl IntoResponse, ApiError> {
    if let Some(target) = &target {
        target.validate().map_err(ApiError::InvalidArgument)?;
    }
    let release = load_release(&state, &tenant_ctx, id).await?;
    if matches!(
        release.state,
        ReleaseState::Promoted | ReleaseState::RolledBack
    ) {
        return Err(ApiError::Conflict(format!(
            "cannot retarget a release in '{}'",
            release.state.as_str()
        )));
    }
    let updated = state
        .storage
        .set_release_target(id, target.as_ref())
        .await
        .map_err(|e| ApiError::from_storage(e, "release"))?;
    if !updated {
        return Err(ApiError::NotFound(format!("release {id}")));
    }
    let reason = match &target {
        Some(t) => format!(
            "rollout target set: {} listed sub-tenant(s), percentage {}",
            t.sub_tenants.as_ref().map_or(0, Vec::len),
            t.percentage
                .map_or_else(|| "none".to_string(), |p| format!("{p}%"))
        ),
        None => "rollout target cleared".to_string(),
    };
    record_decision(
        &state,
        id,
        release.state,
        release.state,
        "operator",
        &reason,
    )
    .await;
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
