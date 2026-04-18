use chrono::{DateTime, Utc};
use uuid::Uuid;

use orch8_types::error::StorageError;
use orch8_types::execution::{BlockType, ExecutionNode, NodeState};
use orch8_types::ids::{
    BlockId, ExecutionNodeId, InstanceId, Namespace, ResourceKey, SequenceId, TenantId,
};
use orch8_types::instance::{InstanceState, TaskInstance};
use orch8_types::output::BlockOutput;
use orch8_types::sequence::SequenceDefinition;
use orch8_types::signal::Signal;
use orch8_types::worker::{WorkerTask, WorkerTaskState};

#[derive(sqlx::FromRow)]
pub(super) struct SequenceRow {
    pub id: Uuid,
    pub tenant_id: String,
    pub namespace: String,
    pub name: String,
    pub definition: serde_json::Value,
    pub version: i32,
    pub deprecated: bool,
    pub created_at: DateTime<Utc>,
}

impl SequenceRow {
    pub fn into_definition(self) -> Result<SequenceDefinition, StorageError> {
        // Support both old format (array of blocks) and new format ({blocks, interceptors}).
        let (blocks, interceptors) = if self.definition.is_array() {
            (serde_json::from_value(self.definition)?, None)
        } else {
            let blocks = serde_json::from_value(
                self.definition
                    .get("blocks")
                    .cloned()
                    .unwrap_or(serde_json::Value::Array(vec![])),
            )?;
            let interceptors = self.definition.get("interceptors").and_then(|v| {
                if v.is_null() {
                    None
                } else {
                    serde_json::from_value(v.clone()).ok()
                }
            });
            (blocks, interceptors)
        };
        Ok(SequenceDefinition {
            id: SequenceId(self.id),
            tenant_id: TenantId(self.tenant_id),
            namespace: Namespace(self.namespace),
            name: self.name,
            version: self.version,
            deprecated: self.deprecated,
            blocks,
            interceptors,
            created_at: self.created_at,
        })
    }
}

#[derive(sqlx::FromRow)]
pub(super) struct InstanceRow {
    pub id: Uuid,
    pub sequence_id: Uuid,
    pub tenant_id: String,
    pub namespace: String,
    pub state: String,
    pub next_fire_at: Option<DateTime<Utc>>,
    pub priority: i16,
    pub timezone: String,
    pub metadata: serde_json::Value,
    pub context: serde_json::Value,
    pub concurrency_key: Option<String>,
    pub max_concurrency: Option<i32>,
    pub idempotency_key: Option<String>,
    pub session_id: Option<Uuid>,
    pub parent_instance_id: Option<Uuid>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

impl InstanceRow {
    pub fn into_instance(self) -> Result<TaskInstance, StorageError> {
        let state = match self.state.as_str() {
            "scheduled" => InstanceState::Scheduled,
            "running" => InstanceState::Running,
            "waiting" => InstanceState::Waiting,
            "paused" => InstanceState::Paused,
            "completed" => InstanceState::Completed,
            "failed" => InstanceState::Failed,
            "cancelled" => InstanceState::Cancelled,
            other => {
                return Err(StorageError::Query(format!(
                    "unknown instance state: {other}"
                )))
            }
        };
        let priority = match self.priority {
            0 => orch8_types::instance::Priority::Low,
            2 => orch8_types::instance::Priority::High,
            3 => orch8_types::instance::Priority::Critical,
            _ => orch8_types::instance::Priority::Normal,
        };
        let context = serde_json::from_value(self.context)?;

        Ok(TaskInstance {
            id: InstanceId(self.id),
            sequence_id: SequenceId(self.sequence_id),
            tenant_id: TenantId(self.tenant_id),
            namespace: Namespace(self.namespace),
            state,
            next_fire_at: self.next_fire_at,
            priority,
            timezone: self.timezone,
            metadata: self.metadata,
            context,
            concurrency_key: self.concurrency_key,
            max_concurrency: self.max_concurrency,
            idempotency_key: self.idempotency_key,
            session_id: self.session_id,
            parent_instance_id: self.parent_instance_id.map(InstanceId),
            created_at: self.created_at,
            updated_at: self.updated_at,
        })
    }
}

#[derive(sqlx::FromRow)]
pub(super) struct ExecutionNodeRow {
    pub id: Uuid,
    pub instance_id: Uuid,
    pub block_id: String,
    pub parent_id: Option<Uuid>,
    pub block_type: String,
    pub branch_index: Option<i16>,
    pub state: String,
    pub started_at: Option<DateTime<Utc>>,
    pub completed_at: Option<DateTime<Utc>>,
}

impl ExecutionNodeRow {
    pub fn into_node(self) -> ExecutionNode {
        let block_type = match self.block_type.as_str() {
            "parallel" => BlockType::Parallel,
            "race" => BlockType::Race,
            "loop" => BlockType::Loop,
            "for_each" => BlockType::ForEach,
            "router" => BlockType::Router,
            "try_catch" => BlockType::TryCatch,
            "sub_sequence" => BlockType::SubSequence,
            "ab_split" => BlockType::ABSplit,
            _ => BlockType::Step, // "step" and unknown
        };
        let state = match self.state.as_str() {
            "running" => NodeState::Running,
            "waiting" => NodeState::Waiting,
            "completed" => NodeState::Completed,
            "failed" => NodeState::Failed,
            "cancelled" => NodeState::Cancelled,
            "skipped" => NodeState::Skipped,
            _ => NodeState::Pending, // "pending" and unknown
        };
        ExecutionNode {
            id: ExecutionNodeId(self.id),
            instance_id: InstanceId(self.instance_id),
            block_id: BlockId(self.block_id),
            parent_id: self.parent_id.map(ExecutionNodeId),
            block_type,
            branch_index: self.branch_index,
            state,
            started_at: self.started_at,
            completed_at: self.completed_at,
        }
    }
}

#[derive(sqlx::FromRow)]
pub(super) struct BlockOutputRow {
    pub id: Uuid,
    pub instance_id: Uuid,
    pub block_id: String,
    pub output: serde_json::Value,
    pub output_ref: Option<String>,
    pub output_size: i32,
    pub attempt: i16,
    pub created_at: DateTime<Utc>,
}

impl BlockOutputRow {
    pub fn into_output(self) -> BlockOutput {
        BlockOutput {
            id: self.id,
            instance_id: InstanceId(self.instance_id),
            block_id: BlockId(self.block_id),
            output: self.output,
            output_ref: self.output_ref,
            output_size: self.output_size,
            attempt: self.attempt,
            created_at: self.created_at,
        }
    }
}

#[derive(sqlx::FromRow)]
pub(super) struct SignalRow {
    pub id: Uuid,
    pub instance_id: Uuid,
    pub signal_type: String,
    pub payload: serde_json::Value,
    pub delivered: bool,
    pub created_at: DateTime<Utc>,
    pub delivered_at: Option<DateTime<Utc>>,
}

impl SignalRow {
    pub fn into_signal(self) -> Signal {
        let signal_type = match self.signal_type.as_str() {
            "pause" => orch8_types::signal::SignalType::Pause,
            "resume" => orch8_types::signal::SignalType::Resume,
            "cancel" => orch8_types::signal::SignalType::Cancel,
            "update_context" => orch8_types::signal::SignalType::UpdateContext,
            other => orch8_types::signal::SignalType::Custom(other.to_string()),
        };
        Signal {
            id: self.id,
            instance_id: InstanceId(self.instance_id),
            signal_type,
            payload: self.payload,
            delivered: self.delivered,
            created_at: self.created_at,
            delivered_at: self.delivered_at,
        }
    }
}

#[derive(sqlx::FromRow)]
pub(super) struct CronRow {
    pub id: Uuid,
    pub tenant_id: String,
    pub namespace: String,
    pub sequence_id: Uuid,
    pub cron_expr: String,
    pub timezone: String,
    pub enabled: bool,
    pub metadata: serde_json::Value,
    pub last_triggered_at: Option<DateTime<Utc>>,
    pub next_fire_at: Option<DateTime<Utc>>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

impl CronRow {
    pub fn into_schedule(self) -> orch8_types::cron::CronSchedule {
        orch8_types::cron::CronSchedule {
            id: self.id,
            tenant_id: TenantId(self.tenant_id),
            namespace: Namespace(self.namespace),
            sequence_id: SequenceId(self.sequence_id),
            cron_expr: self.cron_expr,
            timezone: self.timezone,
            enabled: self.enabled,
            metadata: self.metadata,
            last_triggered_at: self.last_triggered_at,
            next_fire_at: self.next_fire_at,
            created_at: self.created_at,
            updated_at: self.updated_at,
        }
    }
}

#[derive(sqlx::FromRow)]
pub(super) struct WorkerTaskRow {
    pub id: Uuid,
    pub instance_id: Uuid,
    pub block_id: String,
    pub handler_name: String,
    pub queue_name: Option<String>,
    pub params: serde_json::Value,
    pub context: serde_json::Value,
    pub attempt: i16,
    pub timeout_ms: Option<i64>,
    pub state: String,
    pub worker_id: Option<String>,
    pub claimed_at: Option<DateTime<Utc>>,
    pub heartbeat_at: Option<DateTime<Utc>>,
    pub completed_at: Option<DateTime<Utc>>,
    pub output: Option<serde_json::Value>,
    pub error_message: Option<String>,
    pub error_retryable: Option<bool>,
    pub created_at: DateTime<Utc>,
}

impl WorkerTaskRow {
    pub fn into_task(self) -> WorkerTask {
        let state = match self.state.as_str() {
            "claimed" => WorkerTaskState::Claimed,
            "completed" => WorkerTaskState::Completed,
            "failed" => WorkerTaskState::Failed,
            _ => WorkerTaskState::Pending,
        };
        WorkerTask {
            id: self.id,
            instance_id: InstanceId(self.instance_id),
            block_id: BlockId(self.block_id),
            handler_name: self.handler_name,
            queue_name: self.queue_name,
            params: self.params,
            context: self.context,
            attempt: self.attempt,
            timeout_ms: self.timeout_ms,
            state,
            worker_id: self.worker_id,
            claimed_at: self.claimed_at,
            heartbeat_at: self.heartbeat_at,
            completed_at: self.completed_at,
            output: self.output,
            error_message: self.error_message,
            error_retryable: self.error_retryable,
            created_at: self.created_at,
        }
    }
}

#[derive(sqlx::FromRow)]
pub(super) struct ResourcePoolRow {
    pub id: uuid::Uuid,
    pub tenant_id: String,
    pub name: String,
    pub strategy: String,
    pub round_robin_index: i32,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

impl ResourcePoolRow {
    pub fn into_pool(self) -> orch8_types::pool::ResourcePool {
        orch8_types::pool::ResourcePool {
            id: self.id,
            tenant_id: TenantId(self.tenant_id),
            name: self.name,
            strategy: match self.strategy.as_str() {
                "weighted" => orch8_types::pool::RotationStrategy::Weighted,
                "random" => orch8_types::pool::RotationStrategy::Random,
                _ => orch8_types::pool::RotationStrategy::RoundRobin,
            },
            round_robin_index: self.round_robin_index as u32,
            created_at: self.created_at,
            updated_at: self.updated_at,
        }
    }
}

#[derive(sqlx::FromRow)]
pub(super) struct PoolResourceRow {
    pub id: uuid::Uuid,
    pub pool_id: uuid::Uuid,
    pub resource_key: String,
    pub name: String,
    pub weight: i32,
    pub enabled: bool,
    pub daily_cap: i32,
    pub daily_usage: i32,
    pub daily_usage_date: Option<chrono::NaiveDate>,
    pub warmup_start: Option<chrono::NaiveDate>,
    pub warmup_days: i32,
    pub warmup_start_cap: i32,
    pub created_at: DateTime<Utc>,
}

impl PoolResourceRow {
    pub fn into_resource(self) -> orch8_types::pool::PoolResource {
        orch8_types::pool::PoolResource {
            id: self.id,
            pool_id: self.pool_id,
            resource_key: ResourceKey(self.resource_key),
            name: self.name,
            weight: self.weight as u32,
            enabled: self.enabled,
            daily_cap: self.daily_cap as u32,
            daily_usage: self.daily_usage as u32,
            daily_usage_date: self.daily_usage_date,
            warmup_start: self.warmup_start,
            warmup_days: self.warmup_days as u32,
            warmup_start_cap: self.warmup_start_cap as u32,
            created_at: self.created_at,
        }
    }
}

#[derive(sqlx::FromRow)]
pub(super) struct AuditLogRow {
    pub id: Uuid,
    pub instance_id: Uuid,
    pub tenant_id: String,
    pub event_type: String,
    pub from_state: Option<String>,
    pub to_state: Option<String>,
    pub block_id: Option<String>,
    pub details: serde_json::Value,
    pub created_at: DateTime<Utc>,
}

impl AuditLogRow {
    pub fn into_entry(self) -> orch8_types::audit::AuditLogEntry {
        orch8_types::audit::AuditLogEntry {
            id: self.id,
            instance_id: InstanceId(self.instance_id),
            tenant_id: TenantId(self.tenant_id),
            event_type: self.event_type,
            from_state: self.from_state,
            to_state: self.to_state,
            block_id: self.block_id,
            details: self.details,
            created_at: self.created_at,
        }
    }
}

#[derive(sqlx::FromRow)]
pub(super) struct SessionRow {
    pub id: Uuid,
    pub tenant_id: String,
    pub session_key: String,
    pub data: serde_json::Value,
    pub state: String,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
    pub expires_at: Option<DateTime<Utc>>,
}

impl SessionRow {
    pub fn into_session(self) -> orch8_types::session::Session {
        let state = match self.state.as_str() {
            "completed" => orch8_types::session::SessionState::Completed,
            "expired" => orch8_types::session::SessionState::Expired,
            "paused" => orch8_types::session::SessionState::Paused,
            _ => orch8_types::session::SessionState::Active,
        };
        orch8_types::session::Session {
            id: self.id,
            tenant_id: TenantId(self.tenant_id),
            session_key: self.session_key,
            data: self.data,
            state,
            created_at: self.created_at,
            updated_at: self.updated_at,
            expires_at: self.expires_at,
        }
    }
}

#[derive(sqlx::FromRow)]
pub(super) struct ClusterNodeRow {
    pub id: Uuid,
    pub name: String,
    pub status: String,
    pub registered_at: DateTime<Utc>,
    pub last_heartbeat_at: DateTime<Utc>,
    pub drain: bool,
}

impl ClusterNodeRow {
    pub fn into_node(self) -> orch8_types::cluster::ClusterNode {
        use orch8_types::cluster::NodeStatus;
        let status = match self.status.as_str() {
            "draining" => NodeStatus::Draining,
            "stopped" => NodeStatus::Stopped,
            _ => NodeStatus::Active,
        };
        orch8_types::cluster::ClusterNode {
            id: self.id,
            name: self.name,
            status,
            registered_at: self.registered_at,
            last_heartbeat_at: self.last_heartbeat_at,
            drain: self.drain,
        }
    }
}
