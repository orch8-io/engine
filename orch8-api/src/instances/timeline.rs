//! `GET /instances/{id}/timeline` — state-at-every-step view.
//!
//! The engine is snapshot-based: the state after every executed block lives
//! in `block_outputs` as data, not as an event log. The timeline endpoint is
//! the flat chronological complement of `GET /instances/{id}/tree`: one entry
//! per `block_outputs` row in execution order (`created_at ASC`), plus
//! instance-level context (creation time, recorded state transitions from the
//! audit log, current state / context).
//!
//! Internal bookkeeping rows (`__in_progress__` / `__retry__` / `__error__`)
//! are *flagged* via `is_sentinel`, not hidden — time travel needs to see
//! retries and crash markers, unlike `GET /instances/{id}/outputs` which
//! strips them.

use axum::Json;
use axum::extract::{Path, Query, State};
use axum::response::IntoResponse;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use utoipa::ToSchema;
use uuid::Uuid;

use orch8_types::context::ExecutionContext;
use orch8_types::ids::{InstanceId, SequenceId};
use orch8_types::instance::InstanceState;

use crate::AppState;
use crate::error::ApiError;

/// Default page size for timeline entries. Outputs can be large, so the
/// endpoint never returns the unbounded history in one response.
const DEFAULT_TIMELINE_LIMIT: u32 = 200;
/// Hard ceiling on the page size (same cap as `Pagination::capped`).
const MAX_TIMELINE_LIMIT: u32 = 1000;

#[derive(Deserialize)]
pub struct TimelineQuery {
    #[serde(default)]
    pub(crate) offset: u64,
    #[serde(default = "default_timeline_limit")]
    pub(crate) limit: u32,
    /// When `false`, entries carry metadata only (no `output` payloads) and
    /// the instance summary omits the current context. Default `true`.
    #[serde(default = "super::types::default_true")]
    pub(crate) include_outputs: bool,
}

const fn default_timeline_limit() -> u32 {
    DEFAULT_TIMELINE_LIMIT
}

/// One executed-block entry: the persisted snapshot of a single block
/// execution (first attempt, retry marker, loop iteration, ...).
#[derive(Serialize, ToSchema)]
pub struct TimelineEntry {
    pub block_id: String,
    pub attempt: u16,
    /// When this row was persisted — i.e. when the block's execution (or the
    /// sentinel write) completed.
    pub completed_at: DateTime<Utc>,
    /// Inline output payload. Omitted when `include_outputs=false`. For
    /// artifact-backed (externalized) rows the stored reference marker is
    /// returned as-is — resolve via `output_ref`.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub output: Option<serde_json::Value>,
    /// Externalized-payload reference, or a sentinel tag
    /// (`__in_progress__` / `__retry__` / `__error__`).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub output_ref: Option<String>,
    /// `true` for internal bookkeeping rows (crash-recovery sentinels, retry
    /// markers, error markers) — flagged rather than hidden.
    pub is_sentinel: bool,
}

/// Instance-level summary heading the timeline.
#[derive(Serialize, ToSchema)]
pub struct TimelineInstance {
    pub id: InstanceId,
    pub sequence_id: SequenceId,
    pub state: InstanceState,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
    /// Current execution context. Omitted when `include_outputs=false`
    /// (contexts can be as large as outputs).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub context: Option<ExecutionContext>,
}

/// A recorded instance state transition (from the audit log).
#[derive(Serialize, ToSchema)]
pub struct TimelineStateTransition {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub from_state: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub to_state: Option<String>,
    pub at: DateTime<Utc>,
}

#[derive(Serialize, ToSchema)]
pub struct TimelineResponse {
    pub instance: TimelineInstance,
    /// State transitions recorded in the audit log (chronological, capped at
    /// the most recent 200). Empty when audit logging is not enabled.
    pub state_transitions: Vec<TimelineStateTransition>,
    /// Executed-block entries in execution order (`created_at ASC`),
    /// paginated by `offset` / `limit`.
    pub entries: Vec<TimelineEntry>,
    pub offset: u64,
    pub limit: u32,
    pub has_more: bool,
}

/// `true` for internal bookkeeping markers that are not real step outputs.
pub(super) fn is_sentinel_ref(output_ref: Option<&str>) -> bool {
    matches!(
        output_ref,
        Some("__in_progress__" | "__retry__" | "__error__")
    )
}

#[utoipa::path(get, path = "/instances/{id}/timeline", tag = "instances",
    params(
        ("id" = Uuid, Path, description = "Instance ID"),
        ("offset" = u64, Query, description = "Pagination offset over executed-block entries"),
        ("limit" = u32, Query, description = "Page size (default 200, max 1000)"),
        ("include_outputs" = bool, Query, description = "Include output payloads and current context (default true)"),
    ),
    responses(
        (status = 200, description = "Execution timeline", body = TimelineResponse),
        (status = 404, description = "Instance not found"),
    )
)]
pub async fn get_timeline(
    State(state): State<AppState>,
    tenant_ctx: crate::auth::OptionalTenant,
    Path(id): Path<Uuid>,
    Query(q): Query<TimelineQuery>,
) -> Result<impl IntoResponse, ApiError> {
    let instance_id = InstanceId::from_uuid(id);

    let instance = state
        .storage
        .get_instance(instance_id)
        .await
        .map_err(|e| ApiError::from_storage(e, "instance"))?
        .ok_or_else(|| ApiError::NotFound(format!("instance {id}")))?;

    crate::auth::enforce_tenant_access(
        &tenant_ctx,
        &instance.tenant_id,
        &format!("instance {id}"),
    )?;

    let limit = q.limit.clamp(1, MAX_TIMELINE_LIMIT);

    let outputs = state
        .storage
        .get_outputs_page(instance_id, limit, q.offset)
        .await
        .map_err(|e| ApiError::from_storage(e, "outputs"))?;

    // Same has-more heuristic as `PaginatedResponse::from_vec`: a full page
    // means there may be more rows.
    let has_more = u32::try_from(outputs.len()).unwrap_or(u32::MAX) >= limit;

    let entries: Vec<TimelineEntry> = outputs
        .into_iter()
        .map(|o| TimelineEntry {
            is_sentinel: is_sentinel_ref(o.output_ref.as_deref()),
            block_id: o.block_id.as_str().to_owned(),
            attempt: o.attempt,
            completed_at: o.created_at,
            output: q.include_outputs.then_some(o.output),
            output_ref: o.output_ref,
        })
        .collect();

    // State transitions, if the deployment records them (audit log is
    // best-effort). The storage returns newest-first; the timeline is
    // chronological, so reverse.
    let mut state_transitions: Vec<TimelineStateTransition> = state
        .storage
        .list_audit_log(instance_id, 200)
        .await
        .map_err(|e| ApiError::from_storage(e, "audit_log"))?
        .into_iter()
        .filter(|e| e.event_type == "state_transition")
        .map(|e| TimelineStateTransition {
            from_state: e.from_state,
            to_state: e.to_state,
            at: e.created_at,
        })
        .collect();
    state_transitions.reverse();

    Ok(Json(TimelineResponse {
        instance: TimelineInstance {
            id: instance.id,
            sequence_id: instance.sequence_id,
            state: instance.state,
            created_at: instance.created_at,
            updated_at: instance.updated_at,
            context: q.include_outputs.then_some(instance.context),
        },
        state_transitions,
        entries,
        offset: q.offset,
        limit,
        has_more,
    }))
}
