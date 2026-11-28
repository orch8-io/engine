use axum::Json;
use axum::extract::{Path, State};
use chrono::{DateTime, Utc};
use orch8_types::continuity::{ContinuityId, ExecutionEpoch, RuntimeId};
use orch8_types::filter::Pagination;
use orch8_types::ids::InstanceId;
use orch8_types::worker::{WorkerAttemptEventKind, WorkerTaskState};
use orch8_types::worker_filter::WorkerTaskFilter;
use serde::Serialize;
use utoipa::ToSchema;
use uuid::Uuid;

use crate::{AppState, auth::OptionalTenant, error::ApiError};

const LIMIT: u32 = 100;
const FORMER_OWNER_LIMIT: usize = 8;

#[derive(Debug, Serialize, ToSchema)]
pub(crate) struct RuntimeTimeline {
    instance_id: InstanceId,
    continuity_id: Option<ContinuityId>,
    truncated: bool,
    events: Vec<TimelineEvent>,
}

#[derive(Debug, Serialize, ToSchema)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub(crate) enum TimelineEvent {
    RuntimeOwner {
        at: DateTime<Utc>,
        instance_id: InstanceId,
        runtime_id: RuntimeId,
        epoch: ExecutionEpoch,
    },
    TaskQueued {
        at: DateTime<Utc>,
        task_id: Uuid,
        instance_id: InstanceId,
        block_id: String,
        /// The queue stored on this task after routing, if a queue was named.
        queue_name: Option<String>,
    },
    WorkerAttempt {
        at: DateTime<Utc>,
        task_id: Uuid,
        instance_id: InstanceId,
        block_id: String,
        claim_epoch: u64,
        worker_id: Option<String>,
        event: WorkerAttemptEventKind,
    },
    TaskTerminal {
        at: DateTime<Utc>,
        task_id: Uuid,
        instance_id: InstanceId,
        block_id: String,
        state: WorkerTaskState,
    },
}

impl TimelineEvent {
    fn at(&self) -> DateTime<Utc> {
        match self {
            Self::RuntimeOwner { at, .. }
            | Self::TaskQueued { at, .. }
            | Self::WorkerAttempt { at, .. }
            | Self::TaskTerminal { at, .. } => *at,
        }
    }
}

#[utoipa::path(get, path = "/instances/{id}/runtime-timeline", tag = "instances",
    params(("id" = Uuid, Path, description = "Physical instance UUID")),
    responses((status = 200, description = "Allowlisted worker and runtime events suitable for export", body = RuntimeTimeline))
)]
#[allow(clippy::too_many_lines)] // Assemble one bounded, chronologically ordered trace from compatible histories.
pub(crate) async fn get_timeline(
    State(state): State<AppState>,
    tenant_ctx: OptionalTenant,
    Path(id): Path<Uuid>,
) -> Result<Json<RuntimeTimeline>, ApiError> {
    let instance_id = InstanceId::from_uuid(id);
    let instance = state
        .storage
        .get_instance(instance_id)
        .await
        .map_err(|error| ApiError::from_storage(error, "instance"))?
        .ok_or_else(|| ApiError::NotFound(format!("instance {id}")))?;
    crate::auth::enforce_tenant_access(&tenant_ctx, &instance.tenant_id, "instance")?;
    let tenant = &instance.tenant_id;
    let filter = WorkerTaskFilter {
        tenant_id: Some(tenant.clone()),
        instance_id: Some(instance_id),
        ..Default::default()
    };
    let pagination = Pagination {
        offset: 0,
        limit: LIMIT,
        sort_ascending: false,
    };
    let (execution, mut tasks) = tokio::try_join!(
        state
            .storage
            .get_continuity_execution_touching_instance(tenant, instance_id),
        state.storage.list_worker_tasks(&filter, &pagination),
    )
    .map_err(|error| ApiError::from_storage(error, "runtime timeline"))?;
    let mut result = RuntimeTimeline {
        instance_id,
        continuity_id: execution.as_ref().map(|execution| execution.continuity_id),
        truncated: tasks.len() == LIMIT as usize,
        events: Vec::new(),
    };
    if let Some(continuity_id) = result.continuity_id {
        let locations = state
            .storage
            .list_continuity_locations(tenant, continuity_id, LIMIT)
            .await
            .map_err(|error| ApiError::from_storage(error, "runtime locations"))?;
        result.truncated |= locations.len() == LIMIT as usize;
        let mut visited = std::collections::HashSet::from([instance_id]);
        let former_owners: Vec<_> = locations
            .iter()
            .rev()
            .map(|location| location.instance_id)
            .filter(|owner| visited.insert(*owner))
            .collect();
        result.truncated |= former_owners.len() > FORMER_OWNER_LIMIT;
        for owner in former_owners.into_iter().take(FORMER_OWNER_LIMIT) {
            let remaining = LIMIT.saturating_sub(u32::try_from(tasks.len()).unwrap_or(LIMIT));
            if remaining == 0 {
                result.truncated = true;
                break;
            }
            let owner_filter = WorkerTaskFilter {
                instance_id: Some(owner),
                ..filter.clone()
            };
            let owner_pagination = Pagination {
                limit: remaining,
                ..pagination.clone()
            };
            let history = state
                .storage
                .list_worker_tasks(&owner_filter, &owner_pagination)
                .await
                .map_err(|error| ApiError::from_storage(error, "former owner worker tasks"))?;
            result.truncated |= history.len() == remaining as usize;
            tasks.extend(history);
        }
        result.events.extend(
            locations
                .into_iter()
                .map(|location| TimelineEvent::RuntimeOwner {
                    at: location.entered_at,
                    instance_id: location.instance_id,
                    runtime_id: location.runtime_id,
                    epoch: location.epoch,
                }),
        );
    }

    for task in tasks {
        let attempts = state
            .storage
            .list_worker_task_attempt_events(task.id, LIMIT)
            .await
            .map_err(|error| ApiError::from_storage(error, "worker attempts"))?;
        let block_id = task.block_id.to_string();
        result.events.push(TimelineEvent::TaskQueued {
            at: task.created_at,
            task_id: task.id,
            instance_id: task.instance_id,
            block_id: block_id.clone(),
            queue_name: task.queue_name,
        });
        result.truncated |= attempts.len() == LIMIT as usize;
        result.events.extend(
            attempts
                .into_iter()
                .map(|attempt| TimelineEvent::WorkerAttempt {
                    at: attempt.created_at,
                    task_id: attempt.task_id,
                    instance_id: task.instance_id,
                    block_id: block_id.clone(),
                    claim_epoch: attempt.claim_epoch,
                    worker_id: attempt.worker_id.map(|id| {
                        if Uuid::parse_str(&id).is_ok() {
                            id
                        } else {
                            // Legacy worker ids can be arbitrary strings.
                            let digest = crate::continuity::hex_sha256(id.as_bytes());
                            format!("legacy-{}", &digest[..12])
                        }
                    }),
                    event: attempt.event,
                }),
        );
        if let Some(at) = task.completed_at {
            result.events.push(TimelineEvent::TaskTerminal {
                at,
                task_id: task.id,
                instance_id: task.instance_id,
                block_id,
                state: task.state,
            });
        }
    }
    result.events.sort_by_key(TimelineEvent::at);
    Ok(Json(result))
}
