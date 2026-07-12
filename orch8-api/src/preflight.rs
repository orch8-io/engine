//! Sequence preflight endpoints: one readiness report before publishing
//! or starting a workflow.
//!
//! `POST /sequences/preflight` checks a draft definition from the request
//! body; `GET /sequences/{id}/preflight` checks a stored sequence. Both
//! snapshot the runtime inventory (workers, pins, credentials, plugins,
//! queues, sub-sequences) fresh on every call — only static validation is
//! cacheable, runtime readiness never is.

use axum::extract::{Path, State};
use axum::response::IntoResponse;
use axum::routing::{get, post};
use axum::{Json, Router};
use chrono::Utc;
use uuid::Uuid;

use orch8_engine::preflight::{
    CredentialInfo, PluginInfo, RuntimeInventory, SubSequenceInfo, run_preflight,
};
use orch8_types::finding::{Confidence, Finding, FindingSeverity};
use orch8_types::ids::{SequenceId, TenantId};
use orch8_types::preflight::{PreflightCheck, PreflightReport, PreflightStatus};
use orch8_types::sequence::SequenceDefinition;

use crate::AppState;
use crate::error::ApiError;

/// Worker registrations older than this are not considered live.
const WORKER_LIVENESS_SECS: i64 = 120;

/// Upper bound on sequences fetched for sub-sequence resolution.
const SEQUENCE_INVENTORY_LIMIT: u32 = 1_000;

pub fn routes() -> Router<AppState> {
    Router::new()
        .route("/sequences/preflight", post(preflight_draft))
        .route("/sequences/{id}/preflight", get(preflight_stored))
}

#[utoipa::path(post, path = "/sequences/preflight", tag = "sequences",
    request_body = SequenceDefinition,
    responses(
        (status = 200, description = "Preflight report for the draft definition", body = PreflightReport),
        (status = 400, description = "Body is not a sequence definition"),
    )
)]
pub(crate) async fn preflight_draft(
    State(state): State<AppState>,
    tenant_ctx: crate::auth::OptionalTenant,
    Json(seq): Json<SequenceDefinition>,
) -> Result<impl IntoResponse, ApiError> {
    let tenant_id = crate::auth::enforce_tenant_create(&tenant_ctx, &seq.tenant_id)?;
    let report = build_report(&state, &tenant_id, &seq).await;
    Ok(Json(report))
}

#[utoipa::path(get, path = "/sequences/{id}/preflight", tag = "sequences",
    params(("id" = Uuid, Path, description = "Sequence id")),
    responses(
        (status = 200, description = "Preflight report for the stored sequence", body = PreflightReport),
        (status = 404, description = "Sequence not found"),
    )
)]
pub(crate) async fn preflight_stored(
    State(state): State<AppState>,
    tenant_ctx: crate::auth::OptionalTenant,
    Path(id): Path<Uuid>,
) -> Result<impl IntoResponse, ApiError> {
    let seq = state
        .storage
        .get_sequence(SequenceId::from_uuid(id))
        .await
        .map_err(|e| ApiError::from_storage(e, "sequence"))?
        .ok_or_else(|| ApiError::NotFound(format!("sequence {id}")))?;
    crate::auth::enforce_tenant_access(&tenant_ctx, &seq.tenant_id, &format!("sequence {id}"))?;
    let tenant_id = seq.tenant_id.clone();
    let report = build_report(&state, &tenant_id, &seq).await;
    Ok(Json(report))
}

async fn build_report(
    state: &AppState,
    tenant_id: &TenantId,
    seq: &SequenceDefinition,
) -> PreflightReport {
    let now = Utc::now();
    let inventory = collect_inventory(state, tenant_id).await;
    let mut report = run_preflight(seq, &inventory, now);
    deep_check_input_schema(seq, &mut report, now);
    report
}

/// Replace the engine's structural `input_schema_valid` check with a full
/// JSON Schema compilation — the API layer owns the jsonschema dependency.
fn deep_check_input_schema(
    seq: &SequenceDefinition,
    report: &mut PreflightReport,
    now: chrono::DateTime<Utc>,
) {
    let Some(schema) = &seq.input_schema else {
        return;
    };
    if !schema.is_object() {
        return; // the structural check already failed it
    }
    if let Err(e) = crate::input_schema::validate_schema_is_well_formed(schema)
        && let Some(check) = report
            .checks
            .iter_mut()
            .find(|c| c.id == "input_schema_valid")
    {
        *check = PreflightCheck::with_status(
            "input_schema_valid",
            PreflightStatus::Fail,
            "input schema does not compile as JSON Schema",
            vec![Finding::new(
                "INVALID_INPUT_SCHEMA",
                FindingSeverity::Error,
                e.to_string(),
                Confidence::Certain,
                now,
            )],
        );
        report.overall = report
            .checks
            .iter()
            .map(|c| c.status)
            .max()
            .unwrap_or(PreflightStatus::Pass);
    }
}

/// Snapshot runtime facts. Each section that fails to load is reported as
/// `None` so its checks come back `unknown` — a flaky inventory source
/// must not fail (or silently pass) the whole preflight.
async fn collect_inventory(state: &AppState, tenant: &TenantId) -> RuntimeInventory {
    let storage = &state.storage;

    let worker_registrations = storage
        .list_worker_registrations(Some(WORKER_LIVENESS_SECS))
        .await
        .ok();

    let version_pins = storage.list_worker_version_pins(None).await.ok();

    let credentials = storage
        .list_credentials(Some(tenant), 1_000)
        .await
        .ok()
        .map(|creds| {
            creds
                .into_iter()
                .map(|c| CredentialInfo {
                    id: c.id,
                    enabled: c.enabled,
                    expires_at: c.expires_at,
                })
                .collect()
        });

    let plugins = storage.list_plugins(Some(tenant)).await.ok().map(|ps| {
        ps.into_iter()
            .map(|p| PluginInfo {
                name: p.name,
                enabled: p.enabled,
            })
            .collect()
    });

    let queue_dispatch = storage.list_queue_dispatch(Some(tenant.as_str())).await.ok();

    let routing_rules = storage
        .list_queue_routing_rules(Some(tenant), None)
        .await
        .ok();

    let sequences = storage
        .list_sequences(Some(tenant), None, SEQUENCE_INVENTORY_LIMIT, 0)
        .await
        .ok()
        .map(|seqs| {
            seqs.into_iter()
                .map(|s| SubSequenceInfo {
                    name: s.name,
                    namespace: s.namespace.as_str().to_string(),
                    version: s.version,
                    status: s.status,
                })
                .collect()
        });

    RuntimeInventory {
        worker_registrations,
        version_pins,
        credentials,
        plugins,
        queue_dispatch,
        routing_rules,
        sequences,
    }
}
