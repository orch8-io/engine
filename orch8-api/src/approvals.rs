//! `GET /approvals` — list instances waiting on human-in-the-loop review.
//!
//! Walks `Storage::list_waiting_with_trees` and maps each (instance, node)
//! pair to an `ApprovalItem` iff the matching step has `wait_for_input`.

use axum::extract::{Query, State};
use axum::response::IntoResponse;
use axum::routing::get;
use axum::{Json, Router};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use utoipa::ToSchema;

use orch8_types::filter::{InstanceFilter, Pagination};
use orch8_types::ids::{BlockId, InstanceId, SequenceId};
use orch8_types::sequence::HumanChoice;

use crate::error::ApiError;
use crate::AppState;

pub fn routes() -> Router<AppState> {
    Router::new().route("/approvals", get(list_approvals))
}

#[derive(Deserialize)]
pub(crate) struct ListApprovalsQuery {
    tenant_id: Option<String>,
    namespace: Option<String>,
    #[serde(default)]
    offset: u64,
    #[serde(default = "default_limit")]
    limit: u32,
}

const fn default_limit() -> u32 {
    100
}

#[derive(Serialize, ToSchema)]
pub(crate) struct ApprovalItem {
    pub instance_id: InstanceId,
    pub tenant_id: String,
    pub namespace: String,
    pub sequence_id: SequenceId,
    pub sequence_name: String,
    pub block_id: BlockId,
    pub prompt: String,
    pub choices: Vec<HumanChoice>,
    pub store_as: Option<String>,
    pub timeout_seconds: Option<u64>,
    pub escalation_handler: Option<String>,
    pub waiting_since: DateTime<Utc>,
    pub deadline: Option<DateTime<Utc>>,
    pub metadata: serde_json::Value,
}

#[derive(Serialize, ToSchema)]
pub(crate) struct ApprovalsResponse {
    pub items: Vec<ApprovalItem>,
    pub total: u64,
}

pub(crate) async fn list_approvals(
    State(state): State<AppState>,
    tenant_ctx: crate::auth::OptionalTenant,
    Query(params): Query<ListApprovalsQuery>,
) -> Result<impl IntoResponse, ApiError> {
    // Tenant scoping: prefer the header-injected tenant, fall back to query param.
    let tenant_from_ctx = tenant_ctx
        .as_ref()
        .map(|axum::Extension(ctx)| ctx.tenant_id.clone());
    let filter = InstanceFilter {
        tenant_id: tenant_from_ctx
            .or_else(|| params.tenant_id.clone().map(orch8_types::ids::TenantId)),
        namespace: params.namespace.clone().map(orch8_types::ids::Namespace),
        ..InstanceFilter::default()
    };
    let pagination = Pagination {
        offset: params.offset,
        limit: params.limit.min(1000),
        sort_ascending: false,
    };

    let pairs = state
        .storage
        .list_waiting_with_trees(&filter, &pagination)
        .await
        .map_err(|e| ApiError::from_storage(e, "instances"))?;

    // Perf: batch-fetch sequences and completed_block_ids concurrently
    // instead of N+1 sequential round-trips.
    let sequence_ids: Vec<_> = pairs.iter().map(|(i, _)| i.sequence_id).collect();
    let instance_ids: Vec<_> = pairs.iter().map(|(i, _)| i.id).collect();

    let (sequences_map, completed_map) = tokio::join!(
        async {
            // Deduplicate sequence IDs to avoid redundant fetches.
            // TODO: add `get_sequences_batch` to StorageBackend for a true single-query batch.
            let mut seen = std::collections::HashSet::new();
            let mut map = std::collections::HashMap::new();
            for sid in sequence_ids {
                if !seen.insert(sid) {
                    continue;
                }
                match state.storage.get_sequence(sid).await {
                    Ok(Some(seq)) => {
                        map.insert(sid, seq);
                    }
                    Ok(None) => {}
                    Err(e) => {
                        tracing::warn!(sequence_id = %sid.0, error = %e, "approvals: failed to fetch sequence");
                    }
                }
            }
            map
        },
        async {
            match state
                .storage
                .get_completed_block_ids_batch(&instance_ids)
                .await
            {
                Ok(map) => map,
                Err(e) => {
                    tracing::warn!(error = %e, "approvals: failed to batch fetch completed blocks");
                    std::collections::HashMap::new()
                }
            }
        },
    );

    let mut items = Vec::new();
    for (instance, tree) in pairs {
        let Some(seq) = sequences_map.get(&instance.sequence_id) else {
            continue;
        };

        if tree.is_empty() {
            // Flat-path instances have no execution tree nodes. Determine
            // the waiting step by finding the first step with `wait_for_input`
            // that hasn't produced a block output yet.
            let completed = completed_map.get(&instance.id).cloned().unwrap_or_default();
            for block in &seq.blocks {
                if let orch8_types::sequence::BlockDefinition::Step(step_def) = block {
                    if completed.contains(&step_def.id) {
                        continue;
                    }
                    if let Some(human_def) = &step_def.wait_for_input {
                        items.push(build_item_from_step(&instance, step_def, human_def, seq));
                        break; // flat path executes steps sequentially
                    }
                }
            }
        } else {
            for node in &tree {
                if let Some(item) = try_build_item(&instance, node, seq) {
                    items.push(item);
                }
            }
        }
    }

    let total = items.len() as u64;
    Ok(Json(ApprovalsResponse { items, total }))
}

fn default_yes_no_choices() -> Vec<HumanChoice> {
    vec![
        HumanChoice {
            label: "Yes".into(),
            value: "yes".into(),
        },
        HumanChoice {
            label: "No".into(),
            value: "no".into(),
        },
    ]
}

/// Build an `ApprovalItem` directly from a step definition (flat-path, no tree node).
fn build_item_from_step(
    instance: &orch8_types::instance::TaskInstance,
    step_def: &orch8_types::sequence::StepDef,
    human_def: &orch8_types::sequence::HumanInputDef,
    sequence: &orch8_types::sequence::SequenceDefinition,
) -> ApprovalItem {
    let choices = human_def
        .choices
        .clone()
        .unwrap_or_else(default_yes_no_choices);

    let waiting_since = instance.updated_at;
    let timeout_seconds = human_def.timeout.map(|d| d.as_secs());
    let deadline = human_def.timeout.map(|d| {
        waiting_since
            + chrono::Duration::from_std(d).unwrap_or_else(|_| chrono::Duration::days(365))
    });

    ApprovalItem {
        instance_id: instance.id,
        tenant_id: instance.tenant_id.0.clone(),
        namespace: instance.namespace.0.clone(),
        sequence_id: instance.sequence_id,
        sequence_name: sequence.name.clone(),
        block_id: step_def.id.clone(),
        prompt: human_def.prompt.clone(),
        choices,
        store_as: human_def.store_as.clone(),
        timeout_seconds,
        escalation_handler: human_def.escalation_handler.clone(),
        waiting_since,
        deadline,
        metadata: instance.metadata.clone(),
    }
}

/// Build an `ApprovalItem` if this (instance, node) pair represents a step
/// currently paused on `wait_for_input`. Returns None otherwise.
fn try_build_item(
    instance: &orch8_types::instance::TaskInstance,
    node: &orch8_types::execution::ExecutionNode,
    sequence: &orch8_types::sequence::SequenceDefinition,
) -> Option<ApprovalItem> {
    use orch8_types::execution::NodeState;

    if node.state != NodeState::Waiting {
        return None;
    }

    let step = find_step_by_id(sequence, &node.block_id)?;
    let human_def = step.wait_for_input.as_ref()?;

    let choices = human_def
        .choices
        .clone()
        .unwrap_or_else(default_yes_no_choices);

    let waiting_since = node.started_at?;
    let timeout_seconds = human_def.timeout.map(|d| d.as_secs());
    let deadline = human_def.timeout.map(|d| {
        waiting_since
            + chrono::Duration::from_std(d).unwrap_or_else(|_| chrono::Duration::days(365))
    });

    Some(ApprovalItem {
        instance_id: instance.id,
        tenant_id: instance.tenant_id.0.clone(),
        namespace: instance.namespace.0.clone(),
        sequence_id: instance.sequence_id,
        sequence_name: sequence.name.clone(),
        block_id: node.block_id.clone(),
        prompt: human_def.prompt.clone(),
        choices,
        store_as: human_def.store_as.clone(),
        timeout_seconds,
        escalation_handler: human_def.escalation_handler.clone(),
        waiting_since,
        deadline,
        metadata: instance.metadata.clone(),
    })
}

/// Depth-first search over all steps in a sequence's block tree.
fn find_step_by_id<'a>(
    seq: &'a orch8_types::sequence::SequenceDefinition,
    block_id: &orch8_types::ids::BlockId,
) -> Option<&'a orch8_types::sequence::StepDef> {
    walk_blocks(&seq.blocks, block_id)
}

fn walk_blocks<'a>(
    blocks: &'a [orch8_types::sequence::BlockDefinition],
    block_id: &orch8_types::ids::BlockId,
) -> Option<&'a orch8_types::sequence::StepDef> {
    use orch8_types::sequence::BlockDefinition;
    for block in blocks {
        let hit = match block {
            BlockDefinition::Step(s) => {
                if &s.id == block_id {
                    Some(s.as_ref())
                } else {
                    None
                }
            }
            BlockDefinition::Parallel(p) => {
                let mut found = None;
                for branch in &p.branches {
                    if let Some(s) = walk_blocks(branch, block_id) {
                        found = Some(s);
                        break;
                    }
                }
                found
            }
            BlockDefinition::Race(r) => {
                let mut found = None;
                for branch in &r.branches {
                    if let Some(s) = walk_blocks(branch, block_id) {
                        found = Some(s);
                        break;
                    }
                }
                found
            }
            BlockDefinition::Loop(l) => walk_blocks(&l.body, block_id),
            BlockDefinition::ForEach(f) => walk_blocks(&f.body, block_id),
            BlockDefinition::Router(r) => {
                let mut found = None;
                for route in &r.routes {
                    if let Some(s) = walk_blocks(&route.blocks, block_id) {
                        found = Some(s);
                        break;
                    }
                }
                if found.is_none() {
                    if let Some(default) = r.default.as_ref() {
                        found = walk_blocks(default, block_id);
                    }
                }
                found
            }
            BlockDefinition::TryCatch(t) => {
                let mut found = walk_blocks(&t.try_block, block_id);
                if found.is_none() {
                    found = walk_blocks(&t.catch_block, block_id);
                }
                if found.is_none() {
                    if let Some(finally) = t.finally_block.as_ref() {
                        found = walk_blocks(finally, block_id);
                    }
                }
                found
            }
            BlockDefinition::SubSequence(_) => None,
            BlockDefinition::ABSplit(ab) => {
                let mut found = None;
                for variant in &ab.variants {
                    if let Some(s) = walk_blocks(&variant.blocks, block_id) {
                        found = Some(s);
                        break;
                    }
                }
                found
            }
            BlockDefinition::CancellationScope(cs) => walk_blocks(&cs.blocks, block_id),
        };
        if let Some(step) = hit {
            return Some(step);
        }
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;
    use orch8_types::context::ExecutionContext;
    use orch8_types::execution::{BlockType, ExecutionNode, NodeState};
    use orch8_types::ids::{ExecutionNodeId, InstanceId, Namespace, SequenceId, TenantId};
    use orch8_types::instance::{InstanceState, Priority, TaskInstance};
    use orch8_types::sequence::{BlockDefinition, HumanInputDef, SequenceDefinition, StepDef};
    use std::time::Duration;

    fn make_step(id: &str, wait_for_input: Option<HumanInputDef>) -> StepDef {
        StepDef {
            id: BlockId(id.into()),
            handler: "human_review".into(),
            params: serde_json::Value::Null,
            delay: None,
            retry: None,
            timeout: None,
            rate_limit_key: None,
            send_window: None,
            context_access: None,
            cancellable: true,
            wait_for_input,
            queue_name: None,
            deadline: None,
            on_deadline_breach: None,
            fallback_handler: None,
            cache_key: None,
        }
    }

    fn make_sequence(step: StepDef) -> SequenceDefinition {
        SequenceDefinition {
            id: SequenceId::new(),
            tenant_id: TenantId("t1".into()),
            namespace: Namespace("default".into()),
            name: "test-seq".into(),
            version: 1,
            deprecated: false,
            blocks: vec![BlockDefinition::Step(Box::new(step))],
            interceptors: None,
            created_at: Utc::now(),
        }
    }

    fn make_instance(sequence_id: SequenceId) -> TaskInstance {
        let now = Utc::now();
        TaskInstance {
            id: InstanceId::new(),
            sequence_id,
            tenant_id: TenantId("t1".into()),
            namespace: Namespace("default".into()),
            state: InstanceState::Waiting,
            next_fire_at: None,
            priority: Priority::Normal,
            timezone: "UTC".into(),
            metadata: serde_json::json!({"k": "v"}),
            context: ExecutionContext::default(),
            concurrency_key: None,
            max_concurrency: None,
            idempotency_key: None,
            session_id: None,
            parent_instance_id: None,
            created_at: now,
            updated_at: now,
        }
    }

    fn make_node(instance_id: InstanceId, block_id: &str, state: NodeState) -> ExecutionNode {
        ExecutionNode {
            id: ExecutionNodeId::new(),
            instance_id,
            block_id: BlockId(block_id.into()),
            parent_id: None,
            block_type: BlockType::Step,
            branch_index: None,
            state,
            started_at: Some(Utc::now()),
            completed_at: None,
        }
    }

    #[test]
    fn try_build_item_yields_default_yesno_choices_when_none() {
        let human = HumanInputDef {
            prompt: "approve?".into(),
            timeout: None,
            escalation_handler: None,
            choices: None,
            store_as: None,
        };
        let step = make_step("review", Some(human));
        let seq = make_sequence(step);
        let inst = make_instance(seq.id);
        let node = make_node(inst.id, "review", NodeState::Waiting);

        let item = try_build_item(&inst, &node, &seq).expect("should build item");
        assert_eq!(item.choices.len(), 2);
        assert_eq!(item.choices[0].label, "Yes");
        assert_eq!(item.choices[0].value, "yes");
        assert_eq!(item.choices[1].label, "No");
        assert_eq!(item.choices[1].value, "no");
    }

    #[test]
    fn try_build_item_returns_none_when_step_has_no_wait_for_input() {
        let step = make_step("review", None);
        let seq = make_sequence(step);
        let inst = make_instance(seq.id);
        let node = make_node(inst.id, "review", NodeState::Waiting);

        assert!(try_build_item(&inst, &node, &seq).is_none());
    }

    #[test]
    fn try_build_item_returns_none_when_node_is_not_waiting() {
        let human = HumanInputDef {
            prompt: "approve?".into(),
            timeout: None,
            escalation_handler: None,
            choices: None,
            store_as: None,
        };
        let step = make_step("review", Some(human));
        let seq = make_sequence(step);
        let inst = make_instance(seq.id);
        let node = make_node(inst.id, "review", NodeState::Running);

        assert!(try_build_item(&inst, &node, &seq).is_none());
    }

    #[test]
    fn try_build_item_honors_author_choices_and_store_as() {
        let choices = vec![
            HumanChoice {
                label: "Approve".into(),
                value: "approve".into(),
            },
            HumanChoice {
                label: "Reject".into(),
                value: "reject".into(),
            },
            HumanChoice {
                label: "Escalate".into(),
                value: "escalate".into(),
            },
        ];
        let human = HumanInputDef {
            prompt: "choose".into(),
            timeout: None,
            escalation_handler: None,
            choices: Some(choices),
            store_as: Some("decision".into()),
        };
        let step = make_step("review", Some(human));
        let seq = make_sequence(step);
        let inst = make_instance(seq.id);
        let node = make_node(inst.id, "review", NodeState::Waiting);

        let item = try_build_item(&inst, &node, &seq).expect("should build item");
        assert_eq!(item.choices.len(), 3);
        assert_eq!(item.choices[0].value, "approve");
        assert_eq!(item.choices[1].value, "reject");
        assert_eq!(item.choices[2].value, "escalate");
        assert_eq!(item.store_as.as_deref(), Some("decision"));
    }

    #[test]
    fn try_build_item_computes_deadline_when_timeout_present() {
        let human = HumanInputDef {
            prompt: "approve?".into(),
            timeout: Some(Duration::from_secs(45)),
            escalation_handler: None,
            choices: None,
            store_as: None,
        };
        let step = make_step("review", Some(human));
        let seq = make_sequence(step);
        let inst = make_instance(seq.id);
        let node = make_node(inst.id, "review", NodeState::Waiting);
        let started_at = node.started_at.unwrap();

        let item = try_build_item(&inst, &node, &seq).expect("should build item");
        assert_eq!(item.timeout_seconds, Some(45));
        assert_eq!(
            item.deadline,
            Some(started_at + chrono::Duration::seconds(45))
        );
    }
}
