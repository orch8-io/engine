//! `POST /instances/{id}/fork` — clone an instance into a sandbox that
//! resumes from an arbitrary block.
//!
//! The snapshot model makes time travel a read + clone: a fork is a brand-new
//! instance of the same sequence whose `block_outputs` are seeded with copies
//! of the source's outputs for every top-level block *before* the fork point,
//! so the engine's completed-blocks memoization skips them and execution
//! resumes exactly at `from_block_id`. The source instance is never touched,
//! so forking is allowed from ANY source state (unlike resume-from, which
//! mutates the instance in place and requires quiescence).
//!
//! ## Artifact-backed (externalized) outputs
//!
//! Externalized output payloads are keyed by the *source* instance ID and
//! ownership-checked on read, so their references cannot be shared across
//! instances safely. We therefore copy **inline outputs only**: any pre-fork
//! top-level block whose snapshot is not fully inline (externalized payload,
//! or a sentinel as the latest row) is placed in the re-run set instead — it
//! executes again on the fork. Granularity is the top-level block: if any
//! block nested inside a composite is non-copyable the whole composite
//! re-runs, since a partially-seeded composite (some iterations copied, some
//! missing) would resume in an inconsistent state.
//!
//! Forks default to dry-run so re-running those blocks (and the post-fork
//! tail) does not re-fire production side effects.

use axum::extract::{Path, State};
use axum::http::StatusCode;
use axum::response::IntoResponse;
use axum::Json;
use chrono::Utc;
use serde::Serialize;
use utoipa::ToSchema;
use uuid::Uuid;

use orch8_types::context::RuntimeContext;
use orch8_types::ids::{BlockId, InstanceId};
use orch8_types::instance::{InstanceState, TaskInstance};

use super::lifecycle::{collect_block_ids, top_level_block_id};
use super::types::ForkRequest;
use crate::error::ApiError;
use crate::AppState;

#[derive(Serialize, ToSchema)]
pub struct ForkResponse {
    /// ID of the newly created fork.
    pub id: InstanceId,
    /// ID of the source instance.
    pub forked_from: InstanceId,
    /// Always `scheduled` — the fork fires immediately.
    pub state: String,
    /// Number of pre-fork top-level blocks whose outputs were copied (the
    /// fork will NOT re-execute these).
    pub copied_blocks: usize,
    /// Pre-fork top-level blocks whose outputs could not be copied (never
    /// executed, artifact-backed, or mid-flight sentinel) — these WILL
    /// (re-)execute on the fork, in addition to everything from
    /// `from_block_id` onward.
    pub rerun_blocks: Vec<String>,
    /// Whether the fork runs in dry-run mode.
    pub dry_run: bool,
}

#[utoipa::path(post, path = "/instances/{id}/fork", tag = "instances",
    params(("id" = Uuid, Path, description = "Source instance ID")),
    request_body = ForkRequest,
    responses(
        (status = 201, description = "Fork created and scheduled", body = ForkResponse),
        (status = 400, description = "Unknown block or invalid context patch"),
        (status = 404, description = "Instance or sequence not found"),
    )
)]
#[allow(clippy::too_many_lines)] // sequential clone steps — splitting hurts readability
pub async fn fork_instance(
    State(state): State<AppState>,
    tenant_ctx: crate::auth::OptionalTenant,
    Path(id): Path<Uuid>,
    Json(req): Json<ForkRequest>,
) -> Result<impl IntoResponse, ApiError> {
    let source_id = InstanceId::from_uuid(id);

    let source = state
        .storage
        .get_instance(source_id)
        .await
        .map_err(|e| ApiError::from_storage(e, "instance"))?
        .ok_or_else(|| ApiError::NotFound(format!("instance {id}")))?;

    crate::auth::enforce_tenant_access(&tenant_ctx, &source.tenant_id, &format!("instance {id}"))?;

    // Validate the context patch up-front (same rule as resume-from).
    let patch = match req.context {
        None => None,
        Some(serde_json::Value::Object(map)) => Some(map),
        Some(_) => {
            return Err(ApiError::InvalidArgument(
                "context patch must be a JSON object".into(),
            ));
        }
    };

    let sequence = state
        .storage
        .get_sequence(source.sequence_id)
        .await
        .map_err(|e| ApiError::from_storage(e, "sequence"))?
        .ok_or_else(|| {
            ApiError::NotFound(format!("sequence {}", source.sequence_id.into_uuid()))
        })?;

    let target_idx = sequence
        .blocks
        .iter()
        .position(|b| top_level_block_id(b).as_str() == req.from_block_id)
        .ok_or_else(|| {
            ApiError::InvalidArgument(format!(
                "block '{}' is not a top-level block of sequence {}",
                req.from_block_id,
                source.sequence_id.into_uuid()
            ))
        })?;

    // Partition the pre-fork top-level blocks into the copy set and the
    // re-run set. A block group (top-level block + everything nested in it)
    // is copyable iff it executed and every member's LATEST row is inline:
    // an externalized payload or a trailing sentinel makes the snapshot
    // unusable on another instance, so the whole group re-runs instead.
    // Older `__retry__` markers behind a real inline output do not block the
    // copy — the storage copy skips non-inline rows, which merely resets the
    // block's attempt counter on the fork.
    let source_outputs = state
        .storage
        .get_all_outputs(source_id)
        .await
        .map_err(|e| ApiError::from_storage(e, "outputs"))?;

    let mut copy_ids: Vec<BlockId> = Vec::new();
    let mut copied_blocks = 0usize;
    let mut rerun_blocks: Vec<String> = Vec::new();
    for block in &sequence.blocks[..target_idx] {
        let mut group_ids: Vec<BlockId> = Vec::new();
        collect_block_ids(block, &mut group_ids);

        let mut executed = false;
        let mut copyable = true;
        for block_id in &group_ids {
            // Rows are in created_at ASC order, so the last match is the
            // latest snapshot for this block.
            let latest = source_outputs.iter().rfind(|o| &o.block_id == block_id);
            if let Some(latest) = latest {
                executed = true;
                if latest.output_ref.is_some() {
                    // Trailing sentinel (mid-flight / error) or externalized
                    // payload — either way the snapshot cannot be cloned.
                    copyable = false;
                    break;
                }
            }
        }

        if executed && copyable {
            copy_ids.extend(group_ids);
            copied_blocks += 1;
        } else {
            rerun_blocks.push(top_level_block_id(block).as_str().to_owned());
        }
    }

    // Assemble the fork: same sequence/tenant/namespace, fresh identity and
    // runtime state, source context with the optional patch applied.
    let mut context = source.context.clone();
    if let Some(patch) = patch {
        if !context.data.is_object() {
            context.data = serde_json::json!({});
        }
        if let Some(data) = context.data.as_object_mut() {
            for (key, value) in patch {
                data.insert(key, value);
            }
        }
    }
    // The fork starts from a clean engine slate (no current step, attempt 0);
    // only the requested execution mode carries over.
    context.runtime = RuntimeContext {
        dry_run: req.dry_run,
        ..RuntimeContext::default()
    };
    context.check_size(state.max_context_bytes)?;

    // Stamp the provenance onto the fork's metadata, preserving the source's
    // own metadata keys.
    let mut metadata = match source.metadata.clone() {
        serde_json::Value::Object(map) => map,
        _ => serde_json::Map::new(),
    };
    metadata.insert(
        "forked_from".into(),
        serde_json::json!(source_id.into_uuid()),
    );
    metadata.insert(
        "forked_at_block".into(),
        serde_json::json!(req.from_block_id),
    );

    let now = Utc::now();
    let fork = TaskInstance {
        id: InstanceId::new(),
        sequence_id: source.sequence_id,
        tenant_id: source.tenant_id.clone(),
        namespace: source.namespace.clone(),
        state: InstanceState::Scheduled,
        next_fire_at: Some(now),
        priority: source.priority,
        timezone: source.timezone.clone(),
        metadata: serde_json::Value::Object(metadata),
        context,
        // A sandbox must not contend for (or dedupe against) the source's
        // production concurrency / idempotency slots.
        concurrency_key: None,
        max_concurrency: None,
        idempotency_key: None,
        session_id: None,
        parent_instance_id: None,
        budget: source.budget.clone(),
        created_at: now,
        updated_at: now,
    };

    state
        .storage
        .create_instance(&fork)
        .await
        .map_err(|e| ApiError::from_storage(e, "instance"))?;

    // Seed the fork with the source's pre-fork snapshots. Inline rows only —
    // see the module docs for why externalized references are not shared.
    let copied_rows = state
        .storage
        .copy_block_outputs(source_id, fork.id, &copy_ids)
        .await
        .map_err(|e| ApiError::from_storage(e, "block_outputs"))?;

    tracing::info!(
        source_id = %source_id,
        fork_id = %fork.id,
        forked_at_block = %req.from_block_id,
        copied_blocks = copied_blocks,
        copied_rows = copied_rows,
        rerun_blocks = rerun_blocks.len(),
        dry_run = req.dry_run,
        "instance forked"
    );

    Ok((
        StatusCode::CREATED,
        Json(ForkResponse {
            id: fork.id,
            forked_from: source_id,
            state: "scheduled".into(),
            copied_blocks,
            rerun_blocks,
            dry_run: req.dry_run,
        }),
    ))
}
