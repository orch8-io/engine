//! Sequence preflight endpoints: one readiness report before publishing
//! or starting a workflow.
//!
//! `POST /sequences/preflight` checks a draft definition from the request
//! body; `GET /sequences/{id}/preflight` checks a stored sequence. Both
//! snapshot the runtime inventory (workers, pins, credentials, plugins,
//! queues, sub-sequences) fresh on every call — only static validation is
//! cacheable, runtime readiness never is.

use axum::extract::{Path, Query, State};
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
    request_body(content(
        (SequenceDefinition = "application/json"),
        (SequenceDefinition = "application/yaml"),
    ), description = "Draft definition as JSON, or as YAML with `Content-Type: application/yaml`"),
    responses(
        (status = 200, description = "Preflight report for the draft definition", body = PreflightReport),
        (status = 400, description = "Body is not a sequence definition"),
    )
)]
pub(crate) async fn preflight_draft(
    State(state): State<AppState>,
    tenant_ctx: crate::auth::OptionalTenant,
    Query(options): Query<crate::sequences::DraftDecodeOptions>,
    crate::sequences::SequenceDocument(value): crate::sequences::SequenceDocument,
) -> Result<impl IntoResponse, ApiError> {
    let (seq, decode_warnings) = crate::sequences::decode_draft_sequence(&value, options.strict)?;
    let tenant_id = crate::auth::enforce_tenant_create(&tenant_ctx, &seq.tenant_id)?;
    let mut report = build_report(&state, &tenant_id, &seq).await;
    if !decode_warnings.is_empty() {
        let now = Utc::now();
        report.checks.push(PreflightCheck::with_status(
            "unknown_fields",
            PreflightStatus::Warning,
            "the draft contains fields ignored for compatibility",
            decode_warnings
                .into_iter()
                .map(|message| {
                    Finding::new(
                        "UNKNOWN_SEQUENCE_FIELD",
                        FindingSeverity::Warning,
                        message,
                        Confidence::Certain,
                        now,
                    )
                })
                .collect(),
        ));
        report.overall = report
            .checks
            .iter()
            .map(|check| check.status)
            .max()
            .unwrap_or(PreflightStatus::Pass);
    }
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
    let mut inventory = collect_inventory(state, tenant_id).await;
    collect_pinned_wasm_modules(state, tenant_id, seq, &mut inventory).await;
    let mut report = run_preflight(seq, &inventory, now);
    deep_check_input_schema(seq, &mut report, now);
    if !orch8_engine::child_contracts::pinned_child_revisions(seq).is_empty() {
        let check = match pinned_child_findings(state, tenant_id, seq, now).await {
            Ok(findings) if findings.is_empty() => PreflightCheck::pass(
                "pinned_child_contracts",
                "no pinned child boundary mismatch",
            ),
            Ok(findings) => PreflightCheck::with_status(
                "pinned_child_contracts",
                PreflightStatus::Fail,
                "incompatible pinned child boundaries",
                findings,
            ),
            Err(_) => PreflightCheck::with_status(
                "pinned_child_contracts",
                PreflightStatus::Unknown,
                "could not load pinned child contracts",
                vec![],
            ),
        };
        report.overall = report.overall.max(check.status);
        report.checks.push(check);
    }
    report
}

pub(crate) async fn pinned_child_findings(
    state: &AppState,
    tenant: &TenantId,
    parent: &SequenceDefinition,
    now: chrono::DateTime<Utc>,
) -> Result<Vec<Finding>, ApiError> {
    let mut children = std::collections::BTreeMap::new();
    for (name, version) in orch8_engine::child_contracts::pinned_child_revisions(parent) {
        if let Some(child) = state
            .storage
            .get_sequence_by_name(tenant, &parent.namespace, &name, Some(version))
            .await
            .map_err(|error| ApiError::from_storage(error, "pinned child sequence"))?
        {
            children.insert((name, version), child);
        }
    }
    Ok(orch8_engine::child_contracts::analyze(
        parent, &children, now,
    ))
}

async fn collect_pinned_wasm_modules(
    state: &AppState,
    tenant_id: &TenantId,
    sequence: &SequenceDefinition,
    inventory: &mut RuntimeInventory,
) {
    use orch8_types::plugin::{MAX_SHARED_WASM_BYTES, WasmArtifactDigest};
    use orch8_types::sequence::{StepDef, visit_steps};
    let mut addresses = std::collections::BTreeSet::new();
    let mut collect = |step: &StepDef| {
        if let Some(source) = orch8_engine::handlers::wasm_plugin::parse_plugin_name(&step.handler)
            && WasmArtifactDigest::from_source(source)
                .ok()
                .flatten()
                .is_some()
        {
            addresses.insert(source.to_owned());
        }
    };
    for block in sequence
        .blocks
        .iter()
        .chain(sequence.on_cancel.iter().flatten())
        .chain(sequence.on_failure.iter().flatten())
    {
        visit_steps(block, &mut collect);
    }
    for source in addresses {
        let Ok(Some(digest)) = WasmArtifactDigest::from_source(&source) else {
            continue;
        };
        if state
            .storage
            .get_shared_wasm_module(tenant_id.as_str(), &digest, MAX_SHARED_WASM_BYTES as u64)
            .await
            .is_ok_and(|bytes| bytes.is_some())
        {
            inventory
                .plugins
                .get_or_insert_with(Vec::new)
                .push(PluginInfo {
                    name: source,
                    enabled: true,
                });
        }
    }
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

    let queue_dispatch = storage
        .list_queue_dispatch(Some(tenant.as_str()))
        .await
        .ok();

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
