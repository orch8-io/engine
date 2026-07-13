//! Execution workbench: one joined, redacted view of a run.
//!
//! The engine already stores everything needed to understand a run —
//! instance state, execution tree, outputs, logs, worker attempts, and
//! the audit trail — but split across endpoints. The workbench joins
//! them with a stable event ordering and redaction applied, plus a
//! run-to-run comparison and a *preview* for fork (mutations keep their
//! existing endpoints; previews never mutate).

use axum::extract::{Path, Query, State};
use axum::response::IntoResponse;
use axum::routing::get;
use axum::{Json, Router};
use serde::{Deserialize, Serialize};
use utoipa::ToSchema;
use uuid::Uuid;

use orch8_types::execution::ExecutionNode;
use orch8_types::ids::InstanceId;
use orch8_types::redaction::RedactionPolicy;
use orch8_types::step_log::StepLog;

use crate::AppState;
use crate::error::ApiError;

/// Handlers with external side effects (mirror of the release-diff set):
/// re-running one repeats the effect, so fork previews flag them.
const SIDE_EFFECT_BUILTINS: &[&str] = &[
    "http_request",
    "llm_call",
    "tool_call",
    "mcp_call",
    "agent",
    "emit_event",
    "send_signal",
    "self_modify",
    "memory_store",
    "blob_put",
    "embed",
];

pub fn routes() -> Router<AppState> {
    Router::new()
        .route("/instances/{id}/workbench", get(get_workbench))
        .route("/instances/{id}/compare/{other}", get(compare_runs))
        .route("/instances/{id}/fork-preview", get(fork_preview))
}

/// One event on the unified timeline. Ordering contract: `(timestamp,
/// kind, id)` — equal timestamps order by kind (state transitions before
/// outputs before logs), then by id, so repeated queries render
/// identically.
#[derive(Debug, Clone, Serialize, ToSchema)]
pub struct WorkbenchEvent {
    pub timestamp: chrono::DateTime<chrono::Utc>,
    /// `state_transition` | `block_output` | `log` | `audit`.
    pub kind: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub block_id: Option<String>,
    pub summary: String,
    /// Stable tiebreaker id.
    pub id: String,
}

/// Summary of one recorded output — the payload itself stays behind the
/// lazy detail endpoint (`/instances/{id}/outputs`).
#[derive(Debug, Clone, Serialize, ToSchema)]
pub struct BlockOutputSummary {
    pub block_id: String,
    pub attempt: u16,
    pub output_size: u32,
    /// `ok` | `error` | `retry`.
    pub status: String,
    pub recorded_at: chrono::DateTime<chrono::Utc>,
}

#[derive(Debug, Clone, Serialize, ToSchema)]
pub struct ExecutionWorkbenchView {
    pub instance_id: Uuid,
    pub sequence_id: Uuid,
    pub sequence_name: String,
    pub sequence_version: i32,
    pub state: String,
    /// Redacted context data.
    pub context_data: serde_json::Value,
    /// Definition tree nodes with their execution state.
    pub nodes: Vec<ExecutionNode>,
    /// Unified, stably-ordered event timeline (bounded by `limit`).
    pub events: Vec<WorkbenchEvent>,
    pub outputs: Vec<BlockOutputSummary>,
    /// True when `events` was truncated at the requested limit.
    pub events_truncated: bool,
    /// Shareable deep-link path (ids only, never payloads).
    pub share_path: String,
}

#[derive(Deserialize)]
pub(crate) struct WorkbenchQuery {
    /// Max timeline events returned (default 500, max 2000).
    #[serde(default)]
    limit: Option<u32>,
}

#[utoipa::path(get, path = "/instances/{id}/workbench", tag = "instances",
    params(
        ("id" = Uuid, Path, description = "Instance id"),
        ("limit" = Option<u32>, Query, description = "Max timeline events (default 500)"),
    ),
    responses(
        (status = 200, description = "Joined, redacted execution view", body = ExecutionWorkbenchView),
        (status = 404, description = "Instance not found"),
    )
)]
#[allow(clippy::too_many_lines)] // joins five sources into one view; splitting obscures the ordering contract
pub(crate) async fn get_workbench(
    State(state): State<AppState>,
    tenant_ctx: crate::auth::OptionalTenant,
    Path(id): Path<Uuid>,
    Query(q): Query<WorkbenchQuery>,
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

    let sequence = state
        .storage
        .get_sequence(instance.sequence_id)
        .await
        .map_err(|e| ApiError::from_storage(e, "sequence"))?;

    let nodes = state
        .storage
        .get_execution_tree(instance_id)
        .await
        .unwrap_or_default();
    let outputs_raw = state
        .storage
        .get_all_outputs(instance_id)
        .await
        .unwrap_or_default();
    let logs: Vec<StepLog> = state
        .storage
        .list_step_logs(instance_id)
        .await
        .unwrap_or_default();
    let audit = state
        .storage
        .list_audit_log(instance_id, 500)
        .await
        .unwrap_or_default();

    // --- unified timeline with a stable ordering contract ---
    let mut events: Vec<WorkbenchEvent> = Vec::new();
    for a in &audit {
        let kind = if a.event_type == "state_transition" {
            "state_transition"
        } else {
            "audit"
        };
        events.push(WorkbenchEvent {
            timestamp: a.created_at,
            kind: kind.to_string(),
            block_id: None,
            summary: match (&a.from_state, &a.to_state) {
                (Some(from), Some(to)) => format!("{}: {from} → {to}", a.event_type),
                _ => a.event_type.clone(),
            },
            id: a.id.to_string(),
        });
    }
    let mut outputs = Vec::new();
    for o in &outputs_raw {
        let block = o.block_id.as_str();
        if block.starts_with('_') {
            continue;
        }
        let status = match o.output_ref.as_deref() {
            Some("__error__") => "error",
            Some("__retry__") => "retry",
            _ => "ok",
        };
        outputs.push(BlockOutputSummary {
            block_id: block.to_string(),
            attempt: o.attempt,
            output_size: o.output_size,
            status: status.to_string(),
            recorded_at: o.created_at,
        });
        events.push(WorkbenchEvent {
            timestamp: o.created_at,
            kind: "block_output".to_string(),
            block_id: Some(block.to_string()),
            summary: format!("block '{block}' attempt {} recorded ({status})", o.attempt),
            id: o.id.to_string(),
        });
    }
    for (i, log) in logs.iter().enumerate() {
        events.push(WorkbenchEvent {
            timestamp: log.ts,
            kind: "log".to_string(),
            block_id: Some(log.block_id.clone()),
            summary: format!("[{}] {}", log.level, truncate(&log.message, 200)),
            id: format!("log-{i}"),
        });
    }
    // Stable ordering: timestamp, then kind, then id — equal timestamps
    // never flap between requests.
    events.sort_by(|a, b| {
        a.timestamp
            .cmp(&b.timestamp)
            .then_with(|| kind_rank(&a.kind).cmp(&kind_rank(&b.kind)))
            .then_with(|| a.id.cmp(&b.id))
    });
    let limit = q.limit.unwrap_or(500).clamp(1, 2000) as usize;
    let events_truncated = events.len() > limit;
    events.truncate(limit);

    let redaction = RedactionPolicy::default();
    Ok(Json(ExecutionWorkbenchView {
        instance_id: id,
        sequence_id: *instance.sequence_id.as_uuid(),
        sequence_name: sequence
            .as_ref()
            .map(|s| s.name.clone())
            .unwrap_or_default(),
        sequence_version: sequence.as_ref().map_or(0, |s| s.version),
        state: instance.state.to_string(),
        context_data: redaction.redacted(&instance.context.data),
        nodes,
        events,
        outputs,
        events_truncated,
        share_path: format!("/instances/{id}/workbench"),
    }))
}

const fn kind_rank(kind: &str) -> u8 {
    match kind.as_bytes() {
        b"state_transition" => 0,
        b"audit" => 1,
        b"block_output" => 2,
        _ => 3, // logs and anything else
    }
}

fn truncate(s: &str, max: usize) -> String {
    if s.len() <= max {
        return s.to_string();
    }
    let mut end = max;
    while end > 0 && !s.is_char_boundary(end) {
        end -= 1;
    }
    format!("{}…", &s[..end])
}

// ---------------------------------------------------------------------------
// Run-to-run comparison
// ---------------------------------------------------------------------------

#[derive(Debug, Serialize, ToSchema)]
pub struct RunComparison {
    pub left: Uuid,
    pub right: Uuid,
    pub left_state: String,
    pub right_state: String,
    /// Blocks executed only by the left run, in execution order.
    pub only_left: Vec<String>,
    /// Blocks executed only by the right run, in execution order.
    pub only_right: Vec<String>,
    /// Common blocks whose (last-attempt) outputs differ.
    pub differing_outputs: Vec<String>,
    /// Common blocks with identical outputs.
    pub matching_blocks: u32,
}

#[utoipa::path(get, path = "/instances/{id}/compare/{other}", tag = "instances",
    params(
        ("id" = Uuid, Path, description = "Left instance"),
        ("other" = Uuid, Path, description = "Right instance"),
    ),
    responses(
        (status = 200, description = "Path and output comparison (no payloads)", body = RunComparison),
        (status = 404, description = "Instance not found"),
    )
)]
pub(crate) async fn compare_runs(
    State(state): State<AppState>,
    tenant_ctx: crate::auth::OptionalTenant,
    Path((id, other)): Path<(Uuid, Uuid)>,
) -> Result<impl IntoResponse, ApiError> {
    let mut sides = Vec::new();
    for iid in [id, other] {
        let instance = state
            .storage
            .get_instance(InstanceId::from_uuid(iid))
            .await
            .map_err(|e| ApiError::from_storage(e, "instance"))?
            .ok_or_else(|| ApiError::NotFound(format!("instance {iid}")))?;
        crate::auth::enforce_tenant_access(
            &tenant_ctx,
            &instance.tenant_id,
            &format!("instance {iid}"),
        )?;
        let outputs = state
            .storage
            .get_all_outputs(instance.id)
            .await
            .unwrap_or_default();
        let mut order: Vec<String> = Vec::new();
        let mut last = std::collections::BTreeMap::<String, serde_json::Value>::default();
        for o in &outputs {
            let block = o.block_id.as_str();
            if block.starts_with('_') || o.output_ref.as_deref() == Some("__retry__") {
                continue;
            }
            if !order.iter().any(|b| b == block) {
                order.push(block.to_string());
            }
            last.insert(block.to_string(), o.output.clone());
        }
        sides.push((instance, order, last));
    }
    let (right_inst, right_order, right_outputs) = sides.pop().expect("two sides");
    let (left_inst, left_order, left_outputs) = sides.pop().expect("two sides");

    let only_left: Vec<String> = left_order
        .iter()
        .filter(|b| !right_order.contains(b))
        .cloned()
        .collect();
    let only_right: Vec<String> = right_order
        .iter()
        .filter(|b| !left_order.contains(b))
        .cloned()
        .collect();
    let mut differing = Vec::new();
    let mut matching = 0u32;
    for block in left_order.iter().filter(|b| right_order.contains(b)) {
        if left_outputs.get(block) == right_outputs.get(block) {
            matching += 1;
        } else {
            differing.push(block.clone());
        }
    }

    Ok(Json(RunComparison {
        left: id,
        right: other,
        left_state: left_inst.state.to_string(),
        right_state: right_inst.state.to_string(),
        only_left,
        only_right,
        differing_outputs: differing,
        matching_blocks: matching,
    }))
}

// ---------------------------------------------------------------------------
// Fork preview (read-only)
// ---------------------------------------------------------------------------

#[derive(Debug, Serialize, ToSchema)]
pub struct ForkPreview {
    pub instance_id: Uuid,
    pub from_block_id: String,
    /// Blocks whose recorded outputs would be copied into the fork.
    pub copied_blocks: Vec<String>,
    /// Blocks that would execute again in the fork.
    pub re_executed_blocks: Vec<String>,
    /// Re-executed blocks with external side effects — the UI must
    /// require explicit confirmation for these.
    pub side_effect_blocks: Vec<String>,
    /// Forks start in dry-run (sandbox) mode unless explicitly overridden.
    pub sandbox_default: bool,
}

#[derive(Deserialize)]
pub(crate) struct ForkPreviewQuery {
    pub from_block_id: String,
}

#[utoipa::path(get, path = "/instances/{id}/fork-preview", tag = "instances",
    params(
        ("id" = Uuid, Path, description = "Instance id"),
        ("from_block_id" = String, Query, description = "Fork point"),
    ),
    responses(
        (status = 200, description = "What a fork from this block would do — nothing is mutated",
            body = ForkPreview),
        (status = 404, description = "Instance or block not found"),
    )
)]
pub(crate) async fn fork_preview(
    State(state): State<AppState>,
    tenant_ctx: crate::auth::OptionalTenant,
    Path(id): Path<Uuid>,
    Query(q): Query<ForkPreviewQuery>,
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
    let sequence = state
        .storage
        .get_sequence(instance.sequence_id)
        .await
        .map_err(|e| ApiError::from_storage(e, "sequence"))?
        .ok_or_else(|| ApiError::NotFound("sequence for instance".into()))?;

    // Block → handler map from the definition, in DFS order.
    let mut block_order: Vec<(String, Option<String>)> = Vec::new();
    collect_blocks(&sequence, &mut block_order);
    let Some(fork_pos) = block_order.iter().position(|(b, _)| b == &q.from_block_id) else {
        return Err(ApiError::NotFound(format!("block '{}'", q.from_block_id)));
    };

    let recorded: std::collections::HashSet<String> = state
        .storage
        .get_all_outputs(instance_id)
        .await
        .unwrap_or_default()
        .iter()
        .filter(|o| !o.block_id.as_str().starts_with('_'))
        .map(|o| o.block_id.as_str().to_string())
        .collect();

    let mut copied = Vec::new();
    let mut re_executed = Vec::new();
    let mut side_effects = Vec::new();
    for (i, (block, handler)) in block_order.iter().enumerate() {
        if i < fork_pos && recorded.contains(block) {
            copied.push(block.clone());
        } else {
            re_executed.push(block.clone());
            if let Some(h) = handler
                && (SIDE_EFFECT_BUILTINS.contains(&h.as_str())
                    || !orch8_types::sequence::BUILTIN_HANDLER_NAMES.contains(&h.as_str()))
            {
                side_effects.push(block.clone());
            }
        }
    }

    Ok(Json(ForkPreview {
        instance_id: id,
        from_block_id: q.from_block_id,
        copied_blocks: copied,
        re_executed_blocks: re_executed,
        side_effect_blocks: side_effects,
        sandbox_default: true,
    }))
}

fn collect_blocks(
    seq: &orch8_types::sequence::SequenceDefinition,
    out: &mut Vec<(String, Option<String>)>,
) {
    fn walk(value: &serde_json::Value, out: &mut Vec<(String, Option<String>)>) {
        match value {
            serde_json::Value::Object(map) => {
                if let Some(serde_json::Value::String(id)) = map.get("id") {
                    let handler = map
                        .get("handler")
                        .and_then(serde_json::Value::as_str)
                        .map(ToString::to_string);
                    out.push((id.clone(), handler));
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
    if let Ok(v) = serde_json::to_value(seq)
        && let Some(blocks) = v.get("blocks")
    {
        walk(blocks, out);
    }
}
