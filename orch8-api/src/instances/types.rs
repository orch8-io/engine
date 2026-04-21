//! Shared request/response types and small helpers for the instances module.

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use utoipa::ToSchema;
use uuid::Uuid;

use crate::error::ApiError;
use orch8_types::context::ExecutionContext;
use orch8_types::ids::{Namespace, SequenceId, TenantId};
use orch8_types::instance::{InstanceState, Priority};
use orch8_types::signal::SignalType;

#[derive(Deserialize, ToSchema)]
pub(crate) struct CreateInstanceRequest {
    pub(crate) sequence_id: SequenceId,
    pub(crate) tenant_id: TenantId,
    pub(crate) namespace: Namespace,
    #[serde(default)]
    pub(crate) priority: Priority,
    #[serde(default = "default_timezone")]
    pub(crate) timezone: String,
    #[serde(default)]
    pub(crate) metadata: serde_json::Value,
    #[serde(default)]
    pub(crate) context: ExecutionContext,
    pub(crate) next_fire_at: Option<DateTime<Utc>>,
    pub(crate) concurrency_key: Option<String>,
    pub(crate) max_concurrency: Option<i32>,
    pub(crate) idempotency_key: Option<String>,
}

pub(crate) fn default_timezone() -> String {
    "UTC".to_string()
}

#[derive(Deserialize, ToSchema)]
pub(crate) struct BatchCreateRequest {
    pub(crate) instances: Vec<CreateInstanceRequest>,
}

#[derive(Deserialize, ToSchema)]
pub(crate) struct UpdateStateRequest {
    pub(crate) state: InstanceState,
    pub(crate) next_fire_at: Option<DateTime<Utc>>,
}

#[derive(Deserialize, ToSchema)]
pub(crate) struct UpdateContextRequest {
    pub(crate) context: ExecutionContext,
}

#[derive(Deserialize, ToSchema)]
pub(crate) struct SendSignalRequest {
    pub(crate) signal_type: SignalType,
    #[serde(default)]
    pub(crate) payload: serde_json::Value,
}

#[derive(Deserialize)]
pub(crate) struct ListQuery {
    pub(crate) tenant_id: Option<String>,
    pub(crate) namespace: Option<String>,
    pub(crate) sequence_id: Option<Uuid>,
    pub(crate) state: Option<String>,
    #[serde(default)]
    pub(crate) offset: u64,
    #[serde(default = "default_limit")]
    pub(crate) limit: u32,
}

pub(crate) fn default_limit() -> u32 {
    100
}

#[derive(Deserialize, ToSchema)]
pub(crate) struct BulkFilter {
    pub(crate) tenant_id: Option<String>,
    pub(crate) namespace: Option<String>,
    pub(crate) sequence_id: Option<Uuid>,
    pub(crate) states: Option<Vec<InstanceState>>,
}

#[derive(Deserialize, ToSchema)]
pub(crate) struct BulkUpdateStateRequest {
    pub(crate) filter: BulkFilter,
    pub(crate) state: InstanceState,
}

#[derive(Deserialize, ToSchema)]
pub(crate) struct BulkRescheduleRequest {
    pub(crate) filter: BulkFilter,
    /// Shift `next_fire_at` by this many seconds (positive = later, negative = earlier).
    pub(crate) offset_secs: i64,
}

#[derive(Serialize, ToSchema)]
pub(crate) struct CountResponse {
    pub(crate) count: u64,
}

/// Parse comma-separated state values.
pub(crate) fn parse_states(s: &str) -> Result<Vec<InstanceState>, ApiError> {
    s.split(',')
        .map(|part| {
            let trimmed = part.trim();
            match trimmed {
                "scheduled" => Ok(InstanceState::Scheduled),
                "running" => Ok(InstanceState::Running),
                "waiting" => Ok(InstanceState::Waiting),
                "paused" => Ok(InstanceState::Paused),
                "completed" => Ok(InstanceState::Completed),
                "failed" => Ok(InstanceState::Failed),
                "cancelled" => Ok(InstanceState::Cancelled),
                other => Err(ApiError::InvalidArgument(format!(
                    "unknown instance state: {other}"
                ))),
            }
        })
        .collect()
}
