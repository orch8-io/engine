//! Embed runs + approvals: sub-tenant-scoped views that never expose raw
//! context, metadata or step outputs the sequence did not opt into.

use std::collections::{HashMap, HashSet};

use axum::Json;
use axum::extract::{Path, Query, State};
use axum::http::StatusCode;
use axum::response::IntoResponse;
use base64::Engine as _;
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use utoipa::{IntoParams, ToSchema};
use uuid::Uuid;

use orch8_types::execution::{BlockType, ExecutionNode, NodeState};
use orch8_types::filter::{InstanceFilter, Pagination};
use orch8_types::ids::{BlockId, InstanceId, Namespace, SequenceId};
use orch8_types::instance::{InstanceState, TaskInstance};
use orch8_types::sequence::{BlockDefinition, HumanChoice, SequenceDefinition};
use orch8_types::signal::{Signal, SignalType};

use super::token::{EmbedPrincipal, EmbedScope};
use crate::AppState;
use crate::error::ApiError;

const DEFAULT_PAGE: u32 = 20;
const MAX_PAGE: u32 = 100;
const MAX_COMMENT_CHARS: usize = 2_000;
const MAX_INPUT_BYTES: usize = 256 * 1024;

fn encode_cursor(offset: u64) -> String {
    URL_SAFE_NO_PAD.encode(format!("o:{offset}"))
}

fn decode_cursor(cursor: Option<&str>) -> Result<u64, ApiError> {
    let Some(cursor) = cursor.filter(|c| !c.is_empty()) else {
        return Ok(0);
    };
    URL_SAFE_NO_PAD
        .decode(cursor)
        .ok()
        .and_then(|raw| String::from_utf8(raw).ok())
        .and_then(|text| text.strip_prefix("o:").and_then(|n| n.parse().ok()))
        .ok_or_else(|| ApiError::InvalidArgument("invalid cursor".into()))
}

/// Load an instance the principal may see: same tenant, same sub-tenant,
/// sequence admitted by the token. Anything else is indistinguishable from
/// a missing instance (404).
async fn load_visible_instance(
    state: &AppState,
    principal: &EmbedPrincipal,
    id: InstanceId,
) -> Result<(TaskInstance, SequenceDefinition), ApiError> {
    let not_found = || ApiError::NotFound("run".into());
    let instance = state
        .storage
        .get_instance(id)
        .await
        .map_err(|e| ApiError::from_storage(e, "instance"))?
        .ok_or_else(not_found)?;
    if instance.tenant_id != principal.tenant_id
        || instance.sub_tenant.as_deref() != Some(principal.sub_tenant.as_str())
    {
        return Err(not_found());
    }
    let sequence = state
        .storage
        .get_sequence(instance.sequence_id)
        .await
        .map_err(|e| ApiError::from_storage(e, "sequence"))?
        .ok_or_else(not_found)?;
    if !principal.can_run(&sequence) {
        return Err(not_found());
    }
    Ok((instance, sequence))
}

/// Display name of a block: the step handler, else the block kind.
fn block_name(sequence: &SequenceDefinition, node: &ExecutionNode) -> String {
    crate::approvals::find_step_by_id(sequence, &node.block_id)
        .map_or_else(|| node.block_type.to_string(), |step| step.handler.clone())
}

/// Step currently executing / waiting, if any.
fn current_step(
    instance: &TaskInstance,
    tree: &[ExecutionNode],
    sequence: Option<&SequenceDefinition>,
    completed: &HashSet<BlockId>,
) -> Option<String> {
    if instance.state.is_terminal() {
        return None;
    }
    if !tree.is_empty() {
        return tree
            .iter()
            .filter(|n| matches!(n.state, NodeState::Running | NodeState::Waiting))
            .max_by_key(|n| n.block_type == BlockType::Step)
            .map(|n| n.block_id.as_str().to_string());
    }
    sequence?.blocks.iter().find_map(|block| match block {
        BlockDefinition::Step(step) if !completed.contains(&step.id) => {
            Some(step.id.as_str().to_string())
        }
        _ => None,
    })
}

#[derive(Debug, Deserialize, IntoParams)]
pub(crate) struct ListRunsQuery {
    /// Page size (default 20, max 100).
    #[serde(default)]
    pub limit: Option<u32>,
    /// Opaque cursor from a previous page.
    #[serde(default)]
    pub cursor: Option<String>,
}

#[derive(Debug, Serialize, ToSchema)]
pub(crate) struct EmbedRunSummary {
    pub id: InstanceId,
    pub sequence: String,
    pub state: String,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
    pub current_step: Option<String>,
}

#[derive(Debug, Serialize, ToSchema)]
pub(crate) struct EmbedRunList {
    pub items: Vec<EmbedRunSummary>,
    pub next_cursor: Option<String>,
}

#[utoipa::path(get, path = "/embed/runs", tag = "embed", operation_id = "embed_list_runs", params(ListRunsQuery),
    responses(
        (status = 200, description = "The sub-tenant's runs, newest first", body = EmbedRunList),
        (status = 401, description = "Missing/invalid embed token"),
        (status = 403, description = "Token lacks runs:read"),
        (status = 404, description = "Embedding is disabled"),
    ),
    security(("embed_token" = []))
)]
pub(crate) async fn list_runs(
    State(state): State<AppState>,
    principal: EmbedPrincipal,
    Query(q): Query<ListRunsQuery>,
) -> Result<impl IntoResponse, ApiError> {
    principal.require(EmbedScope::RunsRead)?;
    let limit = q.limit.unwrap_or(DEFAULT_PAGE).clamp(1, MAX_PAGE);
    let offset = decode_cursor(q.cursor.as_deref())?;
    let filter = InstanceFilter {
        tenant_id: Some(principal.tenant_id.clone()),
        sub_tenant: Some(principal.sub_tenant.clone()),
        ..InstanceFilter::default()
    };
    let mut instances = state
        .storage
        .list_instances(
            &filter,
            &Pagination {
                offset,
                limit: limit + 1,
                sort_ascending: false,
            },
        )
        .await
        .map_err(|e| ApiError::from_storage(e, "instances"))?;
    let has_more = instances.len() > limit as usize;
    instances.truncate(limit as usize);

    let sequence_ids: Vec<SequenceId> = instances
        .iter()
        .map(|i| i.sequence_id)
        .collect::<HashSet<_>>()
        .into_iter()
        .collect();
    let sequences: HashMap<SequenceId, SequenceDefinition> = state
        .storage
        .get_sequences(&sequence_ids)
        .await
        .map_err(|e| ApiError::from_storage(e, "sequence"))?
        .into_iter()
        .filter(|s| s.tenant_id == principal.tenant_id)
        .map(|s| (s.id, s))
        .collect();

    let mut items = Vec::with_capacity(instances.len());
    for instance in &instances {
        let Some(sequence) = sequences.get(&instance.sequence_id) else {
            continue;
        };
        if !principal.can_run(sequence) {
            continue;
        }
        let current = if instance.state.is_terminal() {
            None
        } else {
            let tree = state
                .storage
                .get_execution_tree(instance.id)
                .await
                .map_err(|e| ApiError::from_storage(e, "execution_tree"))?;
            let completed: HashSet<BlockId> = if tree.is_empty() {
                state
                    .storage
                    .get_all_outputs(instance.id)
                    .await
                    .map_err(|e| ApiError::from_storage(e, "block_outputs"))?
                    .into_iter()
                    .map(|o| o.block_id)
                    .collect()
            } else {
                HashSet::new()
            };
            current_step(instance, &tree, Some(sequence), &completed)
        };
        items.push(EmbedRunSummary {
            id: instance.id,
            sequence: sequence.name.clone(),
            state: instance.state.to_string(),
            created_at: instance.created_at,
            updated_at: instance.updated_at,
            current_step: current,
        });
    }
    let next_cursor = has_more.then(|| encode_cursor(offset + u64::from(limit)));
    Ok(Json(EmbedRunList { items, next_cursor }))
}

#[derive(Debug, Serialize, ToSchema)]
pub(crate) struct EmbedStep {
    pub id: String,
    pub name: String,
    pub state: String,
    pub started_at: Option<DateTime<Utc>>,
    pub finished_at: Option<DateTime<Utc>>,
    /// Present only for steps listed in the sequence's
    /// `embed.visible_outputs`.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub output: Option<serde_json::Value>,
}

#[derive(Debug, Serialize, ToSchema)]
pub(crate) struct EmbedRunDetail {
    pub id: InstanceId,
    pub sequence: String,
    pub state: String,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
    pub steps: Vec<EmbedStep>,
}

#[utoipa::path(get, path = "/embed/runs/{id}", tag = "embed", operation_id = "embed_get_run",
    params(("id" = Uuid, Path, description = "Run (instance) id")),
    responses(
        (status = 200, description = "Run timeline; outputs only for opted-in steps", body = EmbedRunDetail),
        (status = 404, description = "Unknown, foreign, or embedding disabled"),
    ),
    security(("embed_token" = []))
)]
pub(crate) async fn get_run(
    State(state): State<AppState>,
    principal: EmbedPrincipal,
    Path(id): Path<Uuid>,
) -> Result<impl IntoResponse, ApiError> {
    principal.require(EmbedScope::RunsRead)?;
    let (instance, sequence) =
        load_visible_instance(&state, &principal, InstanceId::from_uuid(id)).await?;
    let visible: HashSet<&str> = sequence
        .embed
        .as_ref()
        .map(|e| e.visible_outputs.iter().map(String::as_str).collect())
        .unwrap_or_default();
    let tree = state
        .storage
        .get_execution_tree(instance.id)
        .await
        .map_err(|e| ApiError::from_storage(e, "execution_tree"))?;
    let outputs: HashMap<BlockId, orch8_types::output::BlockOutput> = state
        .storage
        .get_all_outputs(instance.id)
        .await
        .map_err(|e| ApiError::from_storage(e, "block_outputs"))?
        .into_iter()
        .filter(|o| !o.block_id.as_str().starts_with('_'))
        .map(|o| (o.block_id.clone(), o))
        .collect();
    let visible_output = |block: &BlockId| -> Option<serde_json::Value> {
        if !visible.contains(block.as_str()) {
            return None;
        }
        outputs
            .get(block)
            .filter(|o| o.output_ref.is_none())
            .map(|o| o.output.clone())
    };

    let steps = if tree.is_empty() {
        // Flat path: top-level steps, completed ones carry an output row.
        let mut current_assigned = false;
        sequence
            .blocks
            .iter()
            .filter_map(|block| match block {
                BlockDefinition::Step(step) => Some(step),
                _ => None,
            })
            .map(|step| {
                let done = outputs.get(&step.id);
                let state_label = if done.is_some() {
                    "completed".to_string()
                } else if !current_assigned && !instance.state.is_terminal() {
                    current_assigned = true;
                    match instance.state {
                        InstanceState::Waiting => "waiting",
                        InstanceState::Running => "running",
                        _ => "pending",
                    }
                    .to_string()
                } else {
                    "pending".to_string()
                };
                EmbedStep {
                    id: step.id.as_str().to_string(),
                    name: step.handler.clone(),
                    state: state_label,
                    started_at: None,
                    finished_at: done.map(|o| o.created_at),
                    output: visible_output(&step.id),
                }
            })
            .collect()
    } else {
        tree.iter()
            .filter(|node| !node.block_id.as_str().starts_with('_'))
            .map(|node| EmbedStep {
                id: node.block_id.as_str().to_string(),
                name: block_name(&sequence, node),
                state: node.state.to_string(),
                started_at: node.started_at,
                finished_at: node.completed_at,
                output: if node.state == NodeState::Completed {
                    visible_output(&node.block_id)
                } else {
                    None
                },
            })
            .collect()
    };
    Ok(Json(EmbedRunDetail {
        id: instance.id,
        sequence: sequence.name.clone(),
        state: instance.state.to_string(),
        created_at: instance.created_at,
        updated_at: instance.updated_at,
        steps,
    }))
}

#[derive(Debug, Deserialize, ToSchema)]
#[serde(deny_unknown_fields)]
pub(crate) struct StartRunRequest {
    /// Sequence name (latest version in `namespace`).
    pub sequence: String,
    /// Becomes the run's `context.data`.
    #[serde(default)]
    pub input: serde_json::Value,
    /// Defaults to `default`.
    #[serde(default)]
    pub namespace: Option<String>,
    /// Optional dedupe key (scoped to the tenant).
    #[serde(default)]
    pub idempotency_key: Option<String>,
}

/// Resolve the latest version of a sequence the principal can see (see
/// [`EmbedPrincipal::visibility`]); anything else is a 404.
pub(crate) async fn resolve_visible_sequence(
    state: &AppState,
    principal: &EmbedPrincipal,
    namespace: &Namespace,
    name: &str,
) -> Result<(SequenceDefinition, super::token::SequenceVisibility), ApiError> {
    let not_found = || ApiError::NotFound(format!("sequence {name}"));
    let sequence = state
        .storage
        .get_sequence_by_name(&principal.tenant_id, namespace, name, None)
        .await
        .map_err(|e| ApiError::from_storage(e, "sequence"))?
        .ok_or_else(not_found)?;
    let visibility = principal.visibility(&sequence).ok_or_else(not_found)?;
    Ok((sequence, visibility))
}

#[utoipa::path(post, path = "/embed/runs", tag = "embed", operation_id = "embed_start_run",
    request_body = StartRunRequest,
    responses(
        (status = 201, description = "Run started for the token's sub-tenant", body = serde_json::Value),
        (status = 403, description = "Token lacks runs:start or the sequence"),
        (status = 404, description = "Unknown/invisible sequence, or embedding disabled"),
        (status = 429, description = "Tenant pool or sub-tenant cap reached (`sub_tenant_quota_exceeded`)"),
    ),
    security(("embed_token" = []))
)]
pub(crate) async fn start_run(
    State(state): State<AppState>,
    principal: EmbedPrincipal,
    Json(req): Json<StartRunRequest>,
) -> Result<impl IntoResponse, ApiError> {
    principal.require(EmbedScope::RunsStart)?;
    let input_size = serde_json::to_vec(&req.input).map_or(usize::MAX, |v| v.len());
    if input_size > MAX_INPUT_BYTES {
        return Err(ApiError::PayloadTooLarge(format!(
            "input exceeds {MAX_INPUT_BYTES} bytes"
        )));
    }
    let namespace = Namespace::new(req.namespace.unwrap_or_else(|| "default".into()));
    let (sequence, _) =
        resolve_visible_sequence(&state, &principal, &namespace, &req.sequence).await?;
    if !principal.can_run(&sequence) {
        return Err(ApiError::EmbedScopeDenied(
            "embed token does not admit runs of this sequence".into(),
        ));
    }
    let context = orch8_types::context::ExecutionContext {
        data: if req.input.is_null() {
            serde_json::json!({})
        } else {
            req.input
        },
        ..Default::default()
    };
    let create = crate::instances::CreateInstanceRequest {
        sequence_id: sequence.id,
        tenant_id: principal.tenant_id.clone(),
        namespace,
        // Resolved from the sequence / plan priority lane (docs/PLACEMENT.md).
        priority: None,
        priority_lane: None,
        timezone: "UTC".to_string(),
        metadata: serde_json::json!({ "started_via": "embed" }),
        context,
        dry_run: false,
        dry_run_auto_approve: false,
        next_fire_at: None,
        concurrency_key: None,
        max_concurrency: None,
        idempotency_key: req.idempotency_key.filter(|k| !k.is_empty()),
        budget: None,
        sub_tenant: None,
    };
    let (status, body) = crate::instances::create_instance_scoped(
        &state,
        principal.tenant_id.clone(),
        Some(principal.sub_tenant.clone()),
        create,
    )
    .await?;
    let id = body.get("id").cloned().unwrap_or(serde_json::Value::Null);
    Ok((status, Json(serde_json::json!({ "id": id }))))
}

// ---------------------------------------------------------------------------
// Approvals
// ---------------------------------------------------------------------------

fn approval_id(instance: InstanceId, block: &BlockId) -> String {
    URL_SAFE_NO_PAD.encode(format!("{}:{}", instance.into_uuid(), block.as_str()))
}

fn parse_approval_id(id: &str) -> Option<(InstanceId, BlockId)> {
    let raw = String::from_utf8(URL_SAFE_NO_PAD.decode(id).ok()?).ok()?;
    let (instance, block) = raw.split_once(':')?;
    if block.is_empty() {
        return None;
    }
    Some((
        InstanceId::from_uuid(Uuid::parse_str(instance).ok()?),
        BlockId::new(block.to_string()),
    ))
}

#[derive(Debug, Serialize, ToSchema)]
pub(crate) struct EmbedApproval {
    pub id: String,
    pub instance_id: InstanceId,
    pub step_id: String,
    pub prompt: String,
    pub choices: Vec<HumanChoice>,
    pub created_at: DateTime<Utc>,
}

#[derive(Debug, Serialize, ToSchema)]
pub(crate) struct EmbedApprovalList {
    pub items: Vec<EmbedApproval>,
}

/// Pending approvals of one waiting instance.
async fn pending_for(
    state: &AppState,
    instance: &TaskInstance,
    tree: &[ExecutionNode],
    sequence: &SequenceDefinition,
) -> Result<Vec<crate::approvals::ApprovalItem>, ApiError> {
    if instance.state != InstanceState::Waiting {
        return Ok(Vec::new());
    }
    if !tree.is_empty() {
        return Ok(tree
            .iter()
            .filter_map(|node| crate::approvals::try_build_item(instance, node, sequence))
            .collect());
    }
    let completed: HashSet<BlockId> = state
        .storage
        .get_all_outputs(instance.id)
        .await
        .map_err(|e| ApiError::from_storage(e, "block_outputs"))?
        .into_iter()
        .map(|o| o.block_id)
        .collect();
    for block in &sequence.blocks {
        if let BlockDefinition::Step(step) = block {
            if completed.contains(&step.id) {
                continue;
            }
            if let Some(human) = &step.wait_for_input {
                return Ok(vec![crate::approvals::build_item_from_step(
                    instance, step, human, sequence,
                )]);
            }
        }
    }
    Ok(Vec::new())
}

fn to_embed(item: crate::approvals::ApprovalItem) -> EmbedApproval {
    EmbedApproval {
        id: approval_id(item.instance_id, &item.block_id),
        instance_id: item.instance_id,
        step_id: item.block_id.as_str().to_string(),
        prompt: item.prompt,
        choices: item.choices,
        created_at: item.waiting_since,
    }
}

#[utoipa::path(get, path = "/embed/approvals", tag = "embed", operation_id = "embed_list_approvals",
    responses(
        (status = 200, description = "Pending approvals of the sub-tenant's runs", body = EmbedApprovalList),
        (status = 403, description = "Token lacks approvals:resolve"),
    ),
    security(("embed_token" = []))
)]
pub(crate) async fn list_approvals(
    State(state): State<AppState>,
    principal: EmbedPrincipal,
) -> Result<impl IntoResponse, ApiError> {
    principal.require(EmbedScope::ApprovalsResolve)?;
    let filter = InstanceFilter {
        tenant_id: Some(principal.tenant_id.clone()),
        sub_tenant: Some(principal.sub_tenant.clone()),
        ..InstanceFilter::default()
    };
    let pairs = state
        .storage
        .list_waiting_with_trees(
            &filter,
            &Pagination {
                offset: 0,
                limit: 200,
                sort_ascending: false,
            },
        )
        .await
        .map_err(|e| ApiError::from_storage(e, "instances"))?;
    let sequence_ids: Vec<SequenceId> = pairs
        .iter()
        .map(|(i, _)| i.sequence_id)
        .collect::<HashSet<_>>()
        .into_iter()
        .collect();
    let sequences: HashMap<SequenceId, SequenceDefinition> = state
        .storage
        .get_sequences(&sequence_ids)
        .await
        .map_err(|e| ApiError::from_storage(e, "sequence"))?
        .into_iter()
        .map(|s| (s.id, s))
        .collect();
    let mut items = Vec::new();
    for (instance, tree) in &pairs {
        // Defence in depth: the storage filter already scoped these.
        if instance.tenant_id != principal.tenant_id
            || instance.sub_tenant.as_deref() != Some(principal.sub_tenant.as_str())
        {
            continue;
        }
        let Some(sequence) = sequences.get(&instance.sequence_id) else {
            continue;
        };
        if !principal.can_run(sequence) {
            continue;
        }
        items.extend(
            pending_for(&state, instance, tree, sequence)
                .await?
                .into_iter()
                .map(to_embed),
        );
    }
    Ok(Json(EmbedApprovalList { items }))
}

#[derive(Debug, Deserialize, ToSchema)]
#[serde(deny_unknown_fields)]
pub(crate) struct ResolveApprovalRequest {
    /// One of the approval's `choices[].value`.
    pub choice: String,
    #[serde(default)]
    pub comment: Option<String>,
}

#[utoipa::path(post, path = "/embed/approvals/{id}", tag = "embed", operation_id = "embed_resolve_approval",
    params(("id" = String, Path, description = "Approval id from GET /embed/approvals")),
    request_body = ResolveApprovalRequest,
    responses(
        (status = 202, description = "Decision recorded", body = serde_json::Value),
        (status = 400, description = "Choice is not offered by the approval"),
        (status = 404, description = "Unknown or foreign approval"),
        (status = 409, description = "Already resolved, or the run already finished"),
    ),
    security(("embed_token" = []))
)]
#[allow(clippy::too_many_lines)] // decode → visibility → pending check → signal → wake → audit
pub(crate) async fn resolve_approval(
    State(state): State<AppState>,
    principal: EmbedPrincipal,
    Path(id): Path<String>,
    Json(req): Json<ResolveApprovalRequest>,
) -> Result<impl IntoResponse, ApiError> {
    principal.require(EmbedScope::ApprovalsResolve)?;
    let not_found = || ApiError::NotFound("approval".into());
    let (instance_id, block_id) = parse_approval_id(&id).ok_or_else(not_found)?;
    let (instance, sequence) = load_visible_instance(&state, &principal, instance_id)
        .await
        .map_err(|_| not_found())?;
    let tree = state
        .storage
        .get_execution_tree(instance.id)
        .await
        .map_err(|e| ApiError::from_storage(e, "execution_tree"))?;
    // A gate that exists but is no longer pending was already resolved (or
    // timed out): 409, which the embed kit treats as "already resolved".
    let pending = match pending_for(&state, &instance, &tree, &sequence)
        .await?
        .into_iter()
        .find(|item| item.block_id == block_id)
    {
        Some(pending) => pending,
        None if crate::approvals::find_step_by_id(&sequence, &block_id)
            .is_some_and(|step| step.wait_for_input.is_some()) =>
        {
            return Err(ApiError::Conflict("approval is no longer pending".into()));
        }
        None => return Err(not_found()),
    };
    if !pending.choices.iter().any(|c| c.value == req.choice) {
        return Err(ApiError::InvalidArgument(
            "choice is not one of the approval's choices".into(),
        ));
    }
    let comment = req
        .comment
        .filter(|c| !c.trim().is_empty())
        .map(|c| c.chars().take(MAX_COMMENT_CHARS).collect::<String>());
    let decided_by = serde_json::json!({
        "kind": "embed",
        "sub_tenant": principal.sub_tenant,
        "token_id": principal.token_id,
    });
    let mut payload =
        serde_json::json!({ "value": req.choice.clone(), "decided_by": decided_by.clone() });
    if let Some(comment) = &comment
        && pending.allow_comment
    {
        payload["comment"] = serde_json::Value::String(comment.clone());
    }
    let signal = Signal {
        id: Uuid::now_v7(),
        instance_id: instance.id,
        signal_type: SignalType::Custom(format!("human_input:{}", block_id.as_str())),
        payload,
        delivered: false,
        created_at: Utc::now(),
        delivered_at: None,
    };
    state
        .storage
        .enqueue_signal_if_active(&signal)
        .await
        .map_err(|e| match e {
            orch8_types::error::StorageError::TerminalTarget { .. } => {
                ApiError::Conflict("the run has already finished".into())
            }
            orch8_types::error::StorageError::NotFound { .. } => not_found(),
            other => ApiError::from_storage(other, "signal"),
        })?;
    if let Ok(Some(fresh)) = state.storage.get_instance(instance.id).await
        && fresh.state == InstanceState::Scheduled
    {
        let _ = state
            .storage
            .conditional_update_instance_state(
                instance.id,
                InstanceState::Scheduled,
                InstanceState::Scheduled,
                Some(Utc::now()),
            )
            .await;
    }
    let entry = orch8_types::audit::AuditLogEntry {
        id: Uuid::now_v7(),
        instance_id: instance.id,
        tenant_id: instance.tenant_id.clone(),
        event_type: "approval_decision".into(),
        from_state: Some(instance.state.to_string()),
        to_state: None,
        block_id: Some(block_id.as_str().to_owned()),
        details: serde_json::json!({
            "channel": "embed",
            "choice": req.choice,
            "decided_by": decided_by,
            "signal_id": signal.id,
        }),
        created_at: Utc::now(),
    };
    if let Err(e) = state.storage.append_audit_log(&entry).await {
        tracing::warn!(instance_id = %instance.id, error = %e, "failed to write embed approval audit entry");
    }
    Ok((
        StatusCode::ACCEPTED,
        Json(serde_json::json!({ "id": id, "signal_id": signal.id })),
    ))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn cursor_and_approval_ids_round_trip() {
        assert_eq!(decode_cursor(Some(&encode_cursor(40))).unwrap(), 40);
        assert_eq!(decode_cursor(None).unwrap(), 0);
        assert!(decode_cursor(Some("garbage!")).is_err());
        let instance = InstanceId::new();
        let block = BlockId::new("approve:manager".to_string());
        let id = approval_id(instance, &block);
        assert_eq!(parse_approval_id(&id), Some((instance, block)));
        assert_eq!(parse_approval_id("nope"), None);
    }
}
