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
use base64::Engine as _;
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use orch8_storage::workbench::{self as storage_workbench, EventData, EventKey};
use serde::{Deserialize, Serialize};
use utoipa::ToSchema;
use uuid::Uuid;

use orch8_types::error::StorageError;
use orch8_types::execution::ExecutionNode;
use orch8_types::ids::{ExecutionNodeId, InstanceId};
use orch8_types::redaction::RedactionPolicy;

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
/// kind, id)` — equal timestamps order by kind (state transitions, audit,
/// outputs, then logs), then by id, so repeated queries render
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
    /// Pass this to `nodes_after` to continue the node list.
    pub next_node: Option<ExecutionNodeId>,
    /// Unified, stably-ordered event timeline (bounded by `limit`).
    pub events: Vec<WorkbenchEvent>,
    /// Output metadata for the events on this page. Payloads are fetched separately.
    pub outputs: Vec<BlockOutputSummary>,
    /// True when `events` was truncated at the requested limit.
    pub events_truncated: bool,
    /// Pass this to `cursor` to continue after the last event.
    pub next_cursor: Option<String>,
    /// Shareable deep-link path (ids only, never payloads).
    pub share_path: String,
}

#[derive(Deserialize)]
pub(crate) struct WorkbenchQuery {
    /// Max timeline events returned (default 500, max 2000).
    #[serde(default)]
    limit: Option<u32>,
    cursor: Option<String>,
    nodes_after: Option<ExecutionNodeId>,
}

#[derive(Deserialize, Serialize)]
struct WorkbenchCursor {
    instance: Uuid,
    key: EventKey,
}

fn decode_cursor(encoded: &str, instance: Uuid) -> Result<EventKey, ApiError> {
    let invalid = || ApiError::InvalidArgument("invalid workbench cursor".into());
    if encoded.len() > 512 {
        return Err(invalid());
    }
    let bytes = URL_SAFE_NO_PAD.decode(encoded).map_err(|_| invalid())?;
    let cursor: WorkbenchCursor = serde_json::from_slice(&bytes).map_err(|_| invalid())?;
    if cursor.instance != instance || !(0..=3).contains(&cursor.key.rank) {
        return Err(invalid());
    }
    Ok(cursor.key)
}

async fn workbench_nodes(
    storage: &dyn orch8_storage::StorageBackend,
    instance: InstanceId,
    after: Option<ExecutionNodeId>,
) -> Result<(Vec<ExecutionNode>, Option<ExecutionNodeId>), StorageError> {
    const PAGE_SIZE: u32 = 500;
    let mut nodes = match storage
        .get_execution_tree_page(instance, after, PAGE_SIZE + 1)
        .await
    {
        Ok(nodes) => nodes,
        Err(StorageError::Unsupported(_)) => {
            let mut nodes = storage.get_execution_tree(instance).await?;
            nodes.retain(|node| after.is_none_or(|id| node.id > id));
            nodes.sort_unstable_by_key(|node| node.id);
            nodes
        }
        Err(error) => return Err(error),
    };
    let next_node = if nodes.len() > PAGE_SIZE as usize {
        nodes.truncate(PAGE_SIZE as usize);
        nodes.last().map(|node| node.id)
    } else {
        None
    };
    Ok((nodes, next_node))
}

#[utoipa::path(get, path = "/instances/{id}/workbench", tag = "instances",
    params(
        ("id" = Uuid, Path, description = "Instance id"),
        ("limit" = Option<u32>, Query, description = "Max timeline events (default 500, max 2000)"),
        ("cursor" = Option<String>, Query, description = "Continue after the previous page"),
        ("nodes_after" = Option<ExecutionNodeId>, Query, description = "Continue the node list after the previous node"),
    ),
    responses(
        (status = 200, description = "Joined, redacted execution view", body = ExecutionWorkbenchView),
        (status = 404, description = "Instance not found"),
    )
)]
#[allow(clippy::too_many_lines)] // constructs the joined view in timeline order
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

    let limit = q.limit.unwrap_or(500).clamp(1, 2000);
    let after = q
        .cursor
        .as_deref()
        .map(|cursor| decode_cursor(cursor, id))
        .transpose()?;
    let (sequence, (nodes, next_node), mut rows) = tokio::try_join!(
        async {
            state
                .storage
                .get_sequence(instance.sequence_id)
                .await
                .map_err(|e| ApiError::from_storage(e, "sequence"))
        },
        async {
            workbench_nodes(state.storage.as_ref(), instance_id, q.nodes_after)
                .await
                .map_err(|e| ApiError::from_storage(e, "execution_tree"))
        },
        async {
            storage_workbench::event_page(
                state.storage.as_ref(),
                instance_id,
                &instance.tenant_id,
                after,
                limit + 1,
            )
            .await
            .map_err(|e| ApiError::from_storage(e, "workbench_events"))
        },
    )?;
    let events_truncated = rows.len() > limit as usize;
    let next_cursor = if events_truncated {
        rows.truncate(limit as usize);
        rows.last().map(|row| {
            let bytes = serde_json::to_vec(&WorkbenchCursor {
                instance: id,
                key: row.key,
            })
            .expect("workbench cursor consists of serializable values");
            URL_SAFE_NO_PAD.encode(bytes)
        })
    } else {
        None
    };
    let mut outputs = Vec::new();
    let mut events = Vec::with_capacity(rows.len());
    for row in rows {
        let timestamp = row.key.timestamp;
        let mut event = WorkbenchEvent {
            timestamp,
            kind: String::new(),
            block_id: None,
            summary: String::new(),
            id: row.key.id.to_string(),
        };
        match row.data {
            EventData::Audit {
                event_type,
                from,
                to,
            } => {
                event.kind = if event_type == "state_transition" {
                    "state_transition".into()
                } else {
                    "audit".into()
                };
                event.summary = match (from, to) {
                    (Some(from), Some(to)) => format!("{event_type}: {from} → {to}"),
                    _ => event_type,
                };
            }
            EventData::Output {
                block_id,
                output_ref,
                attempt,
                size,
            } => {
                let status = match output_ref.as_deref() {
                    Some("__error__") => "error",
                    Some("__retry__") => "retry",
                    _ => "ok",
                };
                outputs.push(BlockOutputSummary {
                    block_id: block_id.clone(),
                    attempt,
                    output_size: size,
                    status: status.into(),
                    recorded_at: timestamp,
                });
                event.kind = "block_output".into();
                event.summary = format!("block '{block_id}' attempt {attempt} recorded ({status})");
                event.block_id = Some(block_id);
            }
            EventData::Log {
                block_id,
                level,
                message,
            } => {
                event.kind = "log".into();
                event.block_id = Some(block_id);
                event.summary = format!("[{level}] {}", truncate(&message, 200));
                event.id.insert_str(0, "log-");
            }
        }
        events.push(event);
    }

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
        next_node,
        events,
        outputs,
        events_truncated,
        next_cursor,
        share_path: format!("/instances/{id}/workbench"),
    }))
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

type ComparisonSide = (
    String,
    Vec<String>,
    std::collections::BTreeMap<String, serde_json::Value>,
);

async fn load_comparison_side(
    state: &AppState,
    tenant_ctx: &crate::auth::OptionalTenant,
    id: Uuid,
) -> Result<ComparisonSide, ApiError> {
    let instance = state
        .storage
        .get_instance(InstanceId::from_uuid(id))
        .await
        .map_err(|e| ApiError::from_storage(e, "instance"))?
        .ok_or_else(|| ApiError::NotFound(format!("instance {id}")))?;
    crate::auth::enforce_tenant_access(tenant_ctx, &instance.tenant_id, &format!("instance {id}"))?;
    let outputs = state
        .storage
        .get_comparison_outputs(instance.id)
        .await
        .map_err(|e| ApiError::from_storage(e, "block_outputs"))?;

    let mut order = Vec::with_capacity(outputs.len());
    let mut last = std::collections::BTreeMap::new();
    for output in outputs {
        let block = output.block_id.to_string();
        order.push(block.clone());
        last.insert(block, output.output);
    }

    Ok((instance.state.to_string(), order, last))
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
    // The two runs are independent; load them concurrently and avoid the
    // temporary Vec/`expect` pair that encoded a runtime-only length invariant.
    let (left, right) = tokio::try_join!(
        load_comparison_side(&state, &tenant_ctx, id),
        load_comparison_side(&state, &tenant_ctx, other),
    )?;
    let (left_state, left_order, left_outputs) = left;
    let (right_state, right_order, right_outputs) = right;

    let left_blocks: std::collections::HashSet<&str> =
        left_order.iter().map(String::as_str).collect();
    let right_blocks: std::collections::HashSet<&str> =
        right_order.iter().map(String::as_str).collect();

    let only_left: Vec<String> = left_order
        .iter()
        .filter(|b| !right_blocks.contains(b.as_str()))
        .cloned()
        .collect();
    let only_right: Vec<String> = right_order
        .iter()
        .filter(|b| !left_blocks.contains(b.as_str()))
        .cloned()
        .collect();
    let mut differing = Vec::new();
    let mut matching = 0u32;
    for block in left_order
        .iter()
        .filter(|b| right_blocks.contains(b.as_str()))
    {
        if left_outputs.get(block) == right_outputs.get(block) {
            matching += 1;
        } else {
            differing.push(block.clone());
        }
    }

    Ok(Json(RunComparison {
        left: id,
        right: other,
        left_state,
        right_state,
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
        (status = 400, description = "Block is not a top-level block of the sequence"),
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

    // Mirror the real fork exactly: `from_block_id` must be a top-level
    // block, and the copy/re-run partition comes from the same helper the
    // fork uses — a block counts as "copied" only when its whole top-level
    // group is copyable (executed, and every member's latest row inline).
    let target_idx =
        crate::instances::fork_target_idx(&sequence, &q.from_block_id).ok_or_else(|| {
            ApiError::InvalidArgument(format!(
                "block '{}' is not a top-level block of sequence {}",
                q.from_block_id,
                instance.sequence_id.into_uuid()
            ))
        })?;
    let source_outputs = state
        .storage
        .get_all_outputs(instance_id)
        .await
        .map_err(|e| ApiError::from_storage(e, "block_outputs"))?;
    let (copy_groups, _rerun) =
        crate::instances::partition_fork_blocks(&sequence, &source_outputs, target_idx);
    let copy_set: std::collections::HashSet<&str> = copy_groups
        .iter()
        .flatten()
        .map(orch8_types::ids::BlockId::as_str)
        .collect();
    let recorded: std::collections::HashSet<String> = source_outputs
        .iter()
        .filter(|o| !o.block_id.as_str().starts_with('_'))
        .map(|o| o.block_id.as_str().to_string())
        .collect();

    let mut copied = Vec::new();
    let mut re_executed = Vec::new();
    let mut side_effects = Vec::new();
    for (i, (block, handler)) in block_order.iter().enumerate() {
        if i < fork_pos && copy_set.contains(block.as_str()) && recorded.contains(block) {
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
