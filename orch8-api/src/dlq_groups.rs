//! DLQ root-cause fingerprinting and guided recovery.
//!
//! `GET /instances/dlq/groups` groups failed instances by a stable,
//! explainable failure fingerprint (computed lazily from the recorded
//! error outputs — the roadmap's lazy backfill). `POST
//! /instances/dlq/groups/{fingerprint}/retry` implements the guided
//! recovery flow: retry one representative sample first; bulk retry is
//! unlocked only by a verified successful sample or an explicit
//! `force: true` override, and is always bounded and audited.

use std::collections::BTreeMap;

use axum::extract::{Path, Query, State};
use axum::http::StatusCode;
use axum::response::IntoResponse;
use axum::routing::{get, post};
use axum::{Json, Router};
use chrono::Utc;
use serde::Deserialize;
use uuid::Uuid;

use orch8_types::dlq::{DlqGroup, DlqGroupRetryRequest, DlqGroupRetryResponse, DlqRetryMode};
use orch8_types::failure::{FailureEnvelope, FingerprintScope, envelope_from_message, fingerprint};
use orch8_types::filter::{InstanceFilter, Pagination};
use orch8_types::ids::{InstanceId, SequenceId, TenantId};
use orch8_types::instance::{InstanceState, TaskInstance};
use orch8_types::redaction::RedactionPolicy;
use orch8_types::sequence::SequenceDefinition;

use crate::AppState;
use crate::error::ApiError;

/// Newest failed instances scanned per grouping request.
const GROUP_SCAN_LIMIT: u32 = 500;
/// Sample instance ids kept per group.
const SAMPLES_PER_GROUP: usize = 10;
/// Bulk retry bounds.
const BULK_DEFAULT_LIMIT: u32 = 100;
const BULK_MAX_LIMIT: u32 = 500;
/// Metadata key marking a sample retry (bulk verification reads it).
const SAMPLE_MARKER_KEY: &str = "dlq_sample_retry";

pub fn routes() -> Router<AppState> {
    Router::new()
        .route("/instances/dlq/groups", get(list_groups))
        .route(
            "/instances/dlq/groups/{fingerprint}/retry",
            post(retry_group),
        )
}

#[derive(Deserialize)]
pub(crate) struct GroupsQuery {
    #[serde(default)]
    tenant_id: Option<String>,
    #[serde(default)]
    sequence_id: Option<Uuid>,
}

/// The failure evidence extracted for one failed instance.
struct InstanceFailure {
    instance: TaskInstance,
    envelope: FailureEnvelope,
    scope: FingerprintScope,
}

#[utoipa::path(get, path = "/instances/dlq/groups", tag = "instances",
    params(
        ("tenant_id" = Option<String>, Query, description = "Filter by tenant"),
        ("sequence_id" = Option<Uuid>, Query, description = "Filter by sequence"),
    ),
    responses((status = 200, description = "Failed instances grouped by root-cause fingerprint,
        ordered by affected count then recency", body = Vec<DlqGroup>))
)]
pub(crate) async fn list_groups(
    State(state): State<AppState>,
    tenant_ctx: crate::auth::OptionalTenant,
    Query(q): Query<GroupsQuery>,
) -> Result<impl IntoResponse, ApiError> {
    let scoped = crate::auth::scoped_tenant_id(&tenant_ctx, q.tenant_id.as_deref());
    let failures = collect_failures(&state, scoped.as_ref(), q.sequence_id).await?;
    Ok(Json(build_groups(failures)))
}

/// Fetch the newest failed instances and derive a failure envelope for
/// each from its recorded error/retry outputs.
async fn collect_failures(
    state: &AppState,
    tenant: Option<&TenantId>,
    sequence_id: Option<Uuid>,
) -> Result<Vec<InstanceFailure>, ApiError> {
    let filter = InstanceFilter {
        tenant_id: tenant.cloned(),
        namespace: None,
        sequence_id: sequence_id.map(SequenceId::from_uuid),
        states: Some(vec![InstanceState::Failed]),
        metadata_filter: None,
        priority: None,
    };
    let pagination = Pagination {
        offset: 0,
        limit: GROUP_SCAN_LIMIT,
        sort_ascending: false,
    };
    let instances = state
        .storage
        .list_instances(&filter, &pagination)
        .await
        .map_err(|e| ApiError::from_storage(e, "instances"))?;

    // Batch-resolve sequence definitions for version + handler lookup.
    let mut seq_ids: Vec<SequenceId> = instances.iter().map(|i| i.sequence_id).collect();
    seq_ids.sort_by_key(|s| *s.as_uuid());
    seq_ids.dedup();
    let sequences: BTreeMap<Uuid, SequenceDefinition> = state
        .storage
        .get_sequences(&seq_ids)
        .await
        .map_err(|e| ApiError::from_storage(e, "sequences"))?
        .into_iter()
        .map(|s| (*s.id.as_uuid(), s))
        .collect();

    let mut failures = Vec::with_capacity(instances.len());
    for instance in instances {
        let seq = sequences.get(instance.sequence_id.as_uuid());
        let envelope = derive_envelope(state, &instance, seq).await;
        let scope = FingerprintScope {
            sequence_id: instance.sequence_id.as_uuid().to_string(),
            sequence_version: seq.map_or(0, |s| i64::from(s.version)),
        };
        failures.push(InstanceFailure {
            instance,
            envelope,
            scope,
        });
    }
    Ok(failures)
}

/// Derive the failure envelope for one failed instance from its recorded
/// outputs: the last `__error__` (permanent) row wins, then the last
/// `__retry__` marker; instances with no recorded error get an `unknown`
/// envelope so they still group (by sequence/version) instead of
/// disappearing.
async fn derive_envelope(
    state: &AppState,
    instance: &TaskInstance,
    seq: Option<&SequenceDefinition>,
) -> FailureEnvelope {
    let outputs = state
        .storage
        .get_all_outputs(instance.id)
        .await
        .unwrap_or_default();

    let mut best: Option<(bool, String, String)> = None; // (is_error_row, block, message)
    for o in &outputs {
        let (is_error, message) = match o.output_ref.as_deref() {
            Some("__error__") => (
                true,
                o.output
                    .get("message")
                    .and_then(serde_json::Value::as_str)
                    .unwrap_or("unknown error")
                    .to_string(),
            ),
            Some("__retry__") => (
                false,
                o.output
                    .get("error")
                    .and_then(serde_json::Value::as_str)
                    .unwrap_or("unknown error")
                    .to_string(),
            ),
            _ => continue,
        };
        // Later rows win within the same kind; __error__ always beats
        // __retry__.
        let replace = match &best {
            None => true,
            Some((best_is_error, ..)) => is_error || !best_is_error,
        };
        if replace {
            best = Some((is_error, o.block_id.as_str().to_string(), message));
        }
    }

    let (block_id, message) = match best {
        Some((_, block, message)) => (Some(block), message),
        None => (
            None,
            "instance failed with no recorded step error".to_string(),
        ),
    };

    let mut envelope = envelope_from_message(&message, false, instance.updated_at);
    if let Some(block) = &block_id {
        envelope = envelope.with_block(block.clone());
        if let Some(seq) = seq
            && let Some(step) =
                crate::approvals::find_step_by_id(seq, &orch8_types::ids::BlockId::new(block))
        {
            envelope = envelope.with_handler(step.handler.clone());
        }
    }
    envelope
}

/// Group derived failures by fingerprint, ordered by count then recency.
fn build_groups(failures: Vec<InstanceFailure>) -> Vec<DlqGroup> {
    let redaction = RedactionPolicy::default();
    let mut groups: BTreeMap<String, DlqGroup> = BTreeMap::new();

    for f in failures {
        let fp = fingerprint(&f.scope, &f.envelope);
        let failed_at = f.instance.updated_at;
        let entry = groups.entry(fp.hash.clone()).or_insert_with(|| DlqGroup {
            fingerprint: fp.hash.clone(),
            components: fp.components.clone(),
            error_class: f.envelope.error_class,
            error_code: f.envelope.error_code.clone(),
            sample_message: redaction.safe_excerpt(&f.envelope.message),
            count: 0,
            first_occurrence: failed_at,
            last_occurrence: failed_at,
            sequences: Vec::new(),
            blocks: Vec::new(),
            handlers: Vec::new(),
            sample_instance_ids: Vec::new(),
        });
        entry.count += 1;
        entry.first_occurrence = entry.first_occurrence.min(failed_at);
        entry.last_occurrence = entry.last_occurrence.max(failed_at);
        if entry.sample_instance_ids.len() < SAMPLES_PER_GROUP {
            entry.sample_instance_ids.push(f.instance.id.into_uuid());
        }
        push_bounded(&mut entry.blocks, f.envelope.block_id.clone());
        push_bounded(&mut entry.handlers, f.envelope.handler.clone());
        push_bounded(&mut entry.sequences, Some(f.scope.sequence_id.clone()));
    }

    let mut out: Vec<DlqGroup> = groups.into_values().collect();
    out.sort_by(|a, b| {
        b.count
            .cmp(&a.count)
            .then(b.last_occurrence.cmp(&a.last_occurrence))
    });
    out
}

fn push_bounded(list: &mut Vec<String>, value: Option<String>) {
    const MAX: usize = 10;
    if let Some(v) = value
        && !list.contains(&v)
        && list.len() < MAX
    {
        list.push(v);
    }
}

#[utoipa::path(post, path = "/instances/dlq/groups/{fingerprint}/retry", tag = "instances",
    params(("fingerprint" = String, Path, description = "Group fingerprint hash")),
    request_body = DlqGroupRetryRequest,
    responses(
        (status = 200, description = "Retry outcome", body = DlqGroupRetryResponse),
        (status = 404, description = "No failed instances match this fingerprint"),
        (status = 409, description = "Bulk retry not unlocked: no verified sample and no force"),
    )
)]
pub(crate) async fn retry_group(
    State(state): State<AppState>,
    tenant_ctx: crate::auth::OptionalTenant,
    Path(fingerprint_hash): Path<String>,
    Json(req): Json<DlqGroupRetryRequest>,
) -> Result<impl IntoResponse, ApiError> {
    let scoped = crate::auth::scoped_tenant_id(&tenant_ctx, None);
    // Recompute membership server-side — the client-supplied hash only
    // selects the group, never the instances.
    let failures = collect_failures(&state, scoped.as_ref(), None).await?;
    let members: Vec<&InstanceFailure> = failures
        .iter()
        .filter(|f| fingerprint(&f.scope, &f.envelope).hash == fingerprint_hash)
        .collect();
    if members.is_empty() {
        return Err(ApiError::NotFound(format!(
            "no failed instances with fingerprint {fingerprint_hash}"
        )));
    }

    match req.mode {
        DlqRetryMode::Sample => {
            // Newest member is the representative.
            let sample = members
                .iter()
                .max_by_key(|f| f.instance.updated_at)
                .expect("non-empty");
            let id = sample.instance.id;
            // Mark before retrying so bulk verification can later prove
            // this instance belonged to this group.
            state
                .storage
                .merge_instance_metadata(
                    id,
                    &serde_json::json!({
                        SAMPLE_MARKER_KEY: {
                            "fingerprint": fingerprint_hash,
                            "at": Utc::now().to_rfc3339(),
                        }
                    }),
                )
                .await
                .map_err(|e| ApiError::from_storage(e, "instance"))?;
            retry_one(&state, id).await?;
            Ok((
                StatusCode::OK,
                Json(DlqGroupRetryResponse {
                    fingerprint: fingerprint_hash,
                    mode: DlqRetryMode::Sample,
                    retried: vec![id.into_uuid()],
                    skipped: 0,
                }),
            ))
        }
        DlqRetryMode::Bulk => {
            if !req.force {
                verify_sample(&state, &fingerprint_hash, req.sample_verified_instance_id).await?;
            }
            let limit = req.limit.unwrap_or(BULK_DEFAULT_LIMIT).min(BULK_MAX_LIMIT) as usize;
            let mut retried = Vec::new();
            let mut skipped = 0u64;
            for member in members.iter().take(limit) {
                // Idempotency: an instance retried by a concurrent call is
                // no longer Failed and is skipped, not errored.
                match retry_one(&state, member.instance.id).await {
                    Ok(()) => retried.push(member.instance.id.into_uuid()),
                    Err(ApiError::InvalidArgument(_) | ApiError::NotFound(_)) => skipped += 1,
                    Err(e) => return Err(e),
                }
            }
            Ok((
                StatusCode::OK,
                Json(DlqGroupRetryResponse {
                    fingerprint: fingerprint_hash,
                    mode: DlqRetryMode::Bulk,
                    retried,
                    skipped,
                }),
            ))
        }
    }
}

/// Bulk unlock check: the referenced instance must carry this group's
/// sample marker and have actually completed.
async fn verify_sample(
    state: &AppState,
    fingerprint_hash: &str,
    sample_id: Option<Uuid>,
) -> Result<(), ApiError> {
    let Some(sample_id) = sample_id else {
        return Err(ApiError::Conflict(
            "bulk retry requires a successful sample retry \
             (sample_verified_instance_id) or force=true"
                .into(),
        ));
    };
    let sample = state
        .storage
        .get_instance(InstanceId::from_uuid(sample_id))
        .await
        .map_err(|e| ApiError::from_storage(e, "instance"))?
        .ok_or_else(|| ApiError::NotFound(format!("sample instance {sample_id}")))?;
    let marker_matches = sample
        .metadata
        .get(SAMPLE_MARKER_KEY)
        .and_then(|m| m.get("fingerprint"))
        .and_then(serde_json::Value::as_str)
        == Some(fingerprint_hash);
    if !marker_matches {
        return Err(ApiError::Conflict(format!(
            "instance {sample_id} was not sample-retried for this group"
        )));
    }
    if sample.state != InstanceState::Completed {
        return Err(ApiError::Conflict(format!(
            "sample instance {sample_id} is '{}' — bulk retry unlocks only after \
             the sample completes (or pass force=true)",
            sample.state
        )));
    }
    Ok(())
}

/// Retry one failed instance with the same semantics as
/// `POST /instances/{id}/retry`: wipe the stale tree and sentinel
/// outputs, then reschedule.
async fn retry_one(state: &AppState, id: InstanceId) -> Result<(), ApiError> {
    let instance = state
        .storage
        .get_instance(id)
        .await
        .map_err(|e| ApiError::from_storage(e, "instance"))?
        .ok_or_else(|| ApiError::NotFound(format!("instance {id}")))?;
    if instance.state != InstanceState::Failed {
        return Err(ApiError::InvalidArgument(format!(
            "instance {id} is no longer failed"
        )));
    }
    state
        .storage
        .delete_execution_tree(id)
        .await
        .map_err(|e| ApiError::from_storage(e, "execution_tree"))?;
    state
        .storage
        .delete_sentinel_block_outputs(id)
        .await
        .map_err(|e| ApiError::from_storage(e, "block_outputs"))?;
    state
        .storage
        .update_instance_state(id, InstanceState::Scheduled, Some(Utc::now()))
        .await
        .map_err(|e| ApiError::from_storage(e, "instance"))?;
    Ok(())
}
