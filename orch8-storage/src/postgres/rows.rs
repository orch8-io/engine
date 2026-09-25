use std::str::FromStr;

use chrono::{DateTime, Utc};
use uuid::Uuid;

use orch8_types::error::StorageError;
use orch8_types::execution::{BlockType, ExecutionNode, NodeState};
use orch8_types::ids::{
    BlockId, ExecutionNodeId, InstanceId, Namespace, ResourceKey, SequenceId, TenantId,
};
use orch8_types::instance::{InstanceState, Priority, TaskInstance};
use orch8_types::output::BlockOutput;
use orch8_types::sequence::SequenceDefinition;
use orch8_types::session::SessionState;
use orch8_types::signal::{Signal, SignalType};
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
    pub status: String,
    pub created_at: DateTime<Utc>,
}

impl SequenceRow {
    pub fn into_definition(self) -> Result<SequenceDefinition, StorageError> {
        // Support both old format (array of blocks) and new format
        // ({blocks, interceptors, input_schema}).
        let (blocks, interceptors, input_schema, sla, on_failure, on_cancel) =
            if self.definition.is_array() {
                (
                    serde_json::from_value(self.definition)?,
                    None,
                    None,
                    None,
                    None,
                    None,
                )
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
                let input_schema = self
                    .definition
                    .get("input_schema")
                    .filter(|v| !v.is_null())
                    .cloned();
                let sla = self.definition.get("sla").and_then(|v| {
                    if v.is_null() {
                        None
                    } else {
                        serde_json::from_value(v.clone()).ok()
                    }
                });
                let parse_blocks = |key: &str| {
                    self.definition.get(key).and_then(|v| {
                        if v.is_null() {
                            None
                        } else {
                            serde_json::from_value(v.clone()).ok()
                        }
                    })
                };
                let on_failure = parse_blocks("on_failure");
                let on_cancel = parse_blocks("on_cancel");
                (
                    blocks,
                    interceptors,
                    input_schema,
                    sla,
                    on_failure,
                    on_cancel,
                )
            };
        Ok(SequenceDefinition {
            schema: None,
            schema_version: orch8_types::sequence::SEQUENCE_SCHEMA_VERSION,
            id: SequenceId::from_uuid(self.id),
            tenant_id: TenantId::unchecked(self.tenant_id),
            namespace: Namespace::new(self.namespace),
            name: self.name,
            version: self.version,
            deprecated: self.deprecated,
            status: self.status.parse().map_err(|e| {
                StorageError::Query(format!("invalid sequence status '{}': {e}", self.status))
            })?,
            blocks,
            interceptors,
            input_schema,
            sla,
            on_failure,
            on_cancel,
            created_at: self.created_at,
        })
    }
}

/// An integer column that may be stored as `INT4` or `INT8`.
///
/// `task_instances.max_concurrency` is `INTEGER` on every released schema.
/// An unreleased migration briefly widened it to `BIGINT` (dropped again:
/// the rewrite took an ACCESS EXCLUSIVE lock on the hottest table and broke
/// rolling deploys, since the old fleet decodes `i32`). Databases that
/// applied it keep the wide column, so decode either width.
#[derive(Debug, Clone, Copy)]
pub(super) struct PgAnyInt(pub i64);

impl sqlx::Type<sqlx::Postgres> for PgAnyInt {
    fn type_info() -> sqlx::postgres::PgTypeInfo {
        <i32 as sqlx::Type<sqlx::Postgres>>::type_info()
    }

    fn compatible(ty: &sqlx::postgres::PgTypeInfo) -> bool {
        <i32 as sqlx::Type<sqlx::Postgres>>::compatible(ty)
            || <i64 as sqlx::Type<sqlx::Postgres>>::compatible(ty)
    }
}

impl<'r> sqlx::Decode<'r, sqlx::Postgres> for PgAnyInt {
    fn decode(value: sqlx::postgres::PgValueRef<'r>) -> Result<Self, sqlx::error::BoxDynError> {
        use sqlx::ValueRef;
        if <i32 as sqlx::Type<sqlx::Postgres>>::compatible(&value.type_info()) {
            <i32 as sqlx::Decode<sqlx::Postgres>>::decode(value).map(|v| Self(i64::from(v)))
        } else {
            <i64 as sqlx::Decode<sqlx::Postgres>>::decode(value).map(Self)
        }
    }
}

/// Bind value for `task_instances.max_concurrency` (`INTEGER`). A limit above
/// `i32::MAX` is indistinguishable from "unlimited", so it saturates instead
/// of wrapping negative (the old `as i32` cast).
pub(super) fn max_concurrency_bind(v: Option<u32>) -> Option<i32> {
    v.map(|v| i32::try_from(v).unwrap_or(i32::MAX))
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
    pub max_concurrency: Option<PgAnyInt>,
    pub idempotency_key: Option<String>,
    pub session_id: Option<Uuid>,
    pub parent_instance_id: Option<Uuid>,
    pub budget: Option<serde_json::Value>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

impl InstanceRow {
    pub fn into_instance(self) -> Result<TaskInstance, StorageError> {
        let state = InstanceState::from_str(&self.state).map_err(StorageError::Query)?;
        let priority = Priority::try_from(self.priority).map_err(StorageError::Query)?;
        let context = serde_json::from_value(self.context)?;
        let budget = self.budget.map(serde_json::from_value).transpose()?;

        Ok(TaskInstance {
            id: InstanceId::from_uuid(self.id),
            sequence_id: SequenceId::from_uuid(self.sequence_id),
            tenant_id: TenantId::unchecked(self.tenant_id),
            namespace: Namespace::new(self.namespace),
            state,
            next_fire_at: self.next_fire_at,
            priority,
            timezone: self.timezone,
            metadata: self.metadata,
            context,
            concurrency_key: self.concurrency_key,
            max_concurrency: self
                .max_concurrency
                .map(|PgAnyInt(v)| {
                    u32::try_from(v).map_err(|_| {
                        StorageError::Query(format!("invalid max_concurrency value: {v}"))
                    })
                })
                .transpose()?,
            idempotency_key: self.idempotency_key,
            session_id: self.session_id,
            parent_instance_id: self.parent_instance_id.map(InstanceId::from_uuid),
            budget,
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
    // Corrupt block_type/state must surface as an error, not silently
    // resurrect the row as Step/Pending -- a corrupt Completed node masked
    // as Pending gets re-executed by the scheduler, duplicating side
    // effects. Matches SQLite's strict decode (sqlite/helpers.rs).
    pub fn into_node(self) -> Result<ExecutionNode, StorageError> {
        let block_type = BlockType::from_str(&self.block_type).map_err(StorageError::Query)?;
        let state = NodeState::from_str(&self.state).map_err(StorageError::Query)?;
        Ok(ExecutionNode {
            id: ExecutionNodeId::from_uuid(self.id),
            instance_id: InstanceId::from_uuid(self.instance_id),
            block_id: BlockId::new(self.block_id),
            parent_id: self.parent_id.map(ExecutionNodeId::from_uuid),
            block_type,
            branch_index: self.branch_index,
            state,
            started_at: self.started_at,
            completed_at: self.completed_at,
        })
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
    pub fn into_output(self) -> Result<BlockOutput, StorageError> {
        Ok(BlockOutput {
            id: self.id,
            instance_id: InstanceId::from_uuid(self.instance_id),
            block_id: BlockId::new(self.block_id),
            output: self.output,
            output_ref: self.output_ref,
            output_size: nonnegative_integer(self.output_size, "output_size")?,
            attempt: u16::try_from(self.attempt).map_err(|_| {
                StorageError::Constraint("attempt is negative in PostgreSQL".into())
            })?,
            created_at: self.created_at,
        })
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
    /// Convert a raw row into a `Signal`, surfacing an unparseable
    /// `signal_type` as `StorageError::Query` instead of panicking. A corrupt
    /// or newer-than-this-binary value in `signal_inbox.signal_type` previously
    /// brought the task down via `.unwrap()`; with this shape the caller can
    /// decide whether to skip the row or propagate the error.
    pub fn into_signal(self) -> Result<Signal, StorageError> {
        let signal_type = SignalType::from_str(&self.signal_type).map_err(|e| {
            StorageError::Query(format!(
                "unknown signal_type {:?} in signal_inbox row {}: {e}",
                self.signal_type, self.id
            ))
        })?;
        Ok(Signal {
            id: self.id,
            instance_id: InstanceId::from_uuid(self.instance_id),
            signal_type,
            payload: self.payload,
            delivered: self.delivered,
            created_at: self.created_at,
            delivered_at: self.delivered_at,
        })
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
    pub overlap_policy: String,
    pub skipped_fires: i64,
    pub last_skipped_at: Option<DateTime<Utc>>,
    pub last_triggered_at: Option<DateTime<Utc>>,
    pub next_fire_at: Option<DateTime<Utc>>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

impl CronRow {
    pub fn into_schedule(self) -> orch8_types::cron::CronSchedule {
        orch8_types::cron::CronSchedule {
            id: self.id,
            tenant_id: TenantId::unchecked(self.tenant_id),
            namespace: Namespace::new(self.namespace),
            sequence_id: SequenceId::from_uuid(self.sequence_id),
            cron_expr: self.cron_expr,
            timezone: self.timezone,
            enabled: self.enabled,
            metadata: self.metadata,
            // Unknown values fall back to `allow` (pre-policy behavior)
            // rather than failing the read — a forward-compat guard, not a
            // corruption mask: the column is constrained at write time.
            overlap_policy: self.overlap_policy.parse().unwrap_or_default(),
            skipped_fires: self.skipped_fires,
            last_skipped_at: self.last_skipped_at,
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
    pub requirements: serde_json::Value,
    pub params: serde_json::Value,
    pub context: serde_json::Value,
    pub attempt: i16,
    pub timeout_ms: Option<i64>,
    pub state: String,
    pub worker_id: Option<String>,
    pub claimed_at: Option<DateTime<Utc>>,
    pub heartbeat_at: Option<DateTime<Utc>>,
    pub claim_epoch: i64,
    pub resume_checkpoint: Option<serde_json::Value>,
    pub checkpoint_seq: i64,
    pub completed_at: Option<DateTime<Utc>>,
    pub output: Option<serde_json::Value>,
    pub error_message: Option<String>,
    pub error_retryable: Option<bool>,
    pub created_at: DateTime<Utc>,
}

impl WorkerTaskRow {
    // A corrupt `state` column must surface as an error, not silently
    // resurrect the row as Pending -- a corrupt Completed/Failed task
    // coerced back to Pending becomes claimable again, re-running work that
    // already finished (or already failed permanently).
    pub fn into_task(self) -> Result<WorkerTask, StorageError> {
        let state = WorkerTaskState::from_str(&self.state).map_err(StorageError::Query)?;
        Ok(WorkerTask {
            id: self.id,
            instance_id: InstanceId::from_uuid(self.instance_id),
            block_id: BlockId::new(self.block_id),
            handler_name: self.handler_name,
            queue_name: self.queue_name,
            requirements: serde_json::from_value(self.requirements)
                .map_err(StorageError::Serialization)?,
            params: self.params,
            context: self.context,
            attempt: self.attempt as u16,
            timeout_ms: self.timeout_ms,
            state,
            worker_id: self.worker_id,
            claimed_at: self.claimed_at,
            heartbeat_at: self.heartbeat_at,
            claim_epoch: u64::try_from(self.claim_epoch)
                .map_err(|_| StorageError::Query("negative worker claim_epoch".into()))?,
            resume_checkpoint: self.resume_checkpoint,
            checkpoint_seq: u64::try_from(self.checkpoint_seq)
                .map_err(|_| StorageError::Query("negative worker checkpoint_seq".into()))?,
            completed_at: self.completed_at,
            output: self.output,
            error_message: self.error_message,
            error_retryable: self.error_retryable,
            created_at: self.created_at,
        })
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
    pub fn into_pool(self) -> Result<orch8_types::pool::ResourcePool, StorageError> {
        use orch8_types::pool::RotationStrategy;
        Ok(orch8_types::pool::ResourcePool {
            id: self.id,
            tenant_id: TenantId::unchecked(self.tenant_id),
            name: self.name,
            strategy: RotationStrategy::from_str(&self.strategy)
                .unwrap_or(RotationStrategy::RoundRobin),
            round_robin_index: nonnegative_integer(self.round_robin_index, "round_robin_index")?,
            created_at: self.created_at,
            updated_at: self.updated_at,
        })
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
    pub fn into_resource(self) -> Result<orch8_types::pool::PoolResource, StorageError> {
        Ok(orch8_types::pool::PoolResource {
            id: self.id,
            pool_id: self.pool_id,
            resource_key: ResourceKey::new(self.resource_key),
            name: self.name,
            weight: nonnegative_integer(self.weight, "weight")?,
            enabled: self.enabled,
            daily_cap: nonnegative_integer(self.daily_cap, "daily_cap")?,
            daily_usage: nonnegative_integer(self.daily_usage, "daily_usage")?,
            daily_usage_date: self.daily_usage_date,
            warmup_start: self.warmup_start,
            warmup_days: nonnegative_integer(self.warmup_days, "warmup_days")?,
            warmup_start_cap: nonnegative_integer(self.warmup_start_cap, "warmup_start_cap")?,
            created_at: self.created_at,
        })
    }
}

fn nonnegative_integer(value: i32, field: &str) -> Result<u32, StorageError> {
    u32::try_from(value)
        .map_err(|_| StorageError::Constraint(format!("{field} is negative in PostgreSQL")))
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
            instance_id: InstanceId::from_uuid(self.instance_id),
            tenant_id: TenantId::unchecked(self.tenant_id),
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
    pub fn into_session(self) -> Result<orch8_types::session::Session, StorageError> {
        let state = SessionState::from_str(&self.state).map_err(StorageError::Query)?;
        Ok(orch8_types::session::Session {
            id: self.id,
            tenant_id: TenantId::unchecked(self.tenant_id),
            session_key: self.session_key,
            data: self.data,
            state,
            created_at: self.created_at,
            updated_at: self.updated_at,
            expires_at: self.expires_at,
        })
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
    pub drain_started_at: Option<DateTime<Utc>>,
    pub stopped_at: Option<DateTime<Utc>>,
    pub capabilities_withdrawn: bool,
    pub execution_handoff_evidence: Option<String>,
}

impl ClusterNodeRow {
    pub fn into_node(self) -> Result<orch8_types::cluster::ClusterNode, StorageError> {
        use orch8_types::cluster::NodeStatus;
        let status = NodeStatus::from_str(&self.status).map_err(StorageError::Query)?;
        Ok(orch8_types::cluster::ClusterNode {
            id: self.id,
            name: self.name,
            status,
            registered_at: self.registered_at,
            last_heartbeat_at: self.last_heartbeat_at,
            drain: self.drain,
            drain_started_at: self.drain_started_at,
            stopped_at: self.stopped_at,
            capabilities_withdrawn: self.capabilities_withdrawn,
            execution_handoff_evidence: self.execution_handoff_evidence,
        })
    }
}

#[cfg(test)]
mod integer_tests {
    use super::*;

    #[test]
    fn rejects_negative_persisted_pool_integer() {
        assert_eq!(
            nonnegative_integer(i32::MAX, "daily_cap").unwrap(),
            i32::MAX as u32
        );
        assert!(matches!(
            nonnegative_integer(-1, "daily_cap"),
            Err(StorageError::Constraint(message)) if message.contains("daily_cap")
        ));
    }

    #[test]
    fn rejects_negative_persisted_output_size_and_attempt() {
        let row = BlockOutputRow {
            id: Uuid::now_v7(),
            instance_id: Uuid::now_v7(),
            block_id: "step".into(),
            output: serde_json::Value::Null,
            output_ref: None,
            output_size: -1,
            attempt: 0,
            created_at: Utc::now(),
        };
        assert!(matches!(
            row.into_output(),
            Err(StorageError::Constraint(_))
        ));

        let row = BlockOutputRow {
            id: Uuid::now_v7(),
            instance_id: Uuid::now_v7(),
            block_id: "step".into(),
            output: serde_json::Value::Null,
            output_ref: None,
            output_size: 0,
            attempt: -1,
            created_at: Utc::now(),
        };
        assert!(matches!(
            row.into_output(),
            Err(StorageError::Constraint(_))
        ));
    }

    #[test]
    fn max_concurrency_bind_saturates_instead_of_wrapping() {
        assert_eq!(max_concurrency_bind(None), None);
        assert_eq!(max_concurrency_bind(Some(7)), Some(7));
        assert_eq!(max_concurrency_bind(Some(u32::MAX)), Some(i32::MAX));
    }

    /// STO-N7: `max_concurrency` must decode from both `INTEGER` (released
    /// schema) and `BIGINT` (databases that applied the dropped widening).
    #[tokio::test]
    async fn pg_any_int_decodes_int4_and_int8() {
        let Ok(url) = std::env::var("DATABASE_URL") else {
            eprintln!("skipping: DATABASE_URL not set");
            return;
        };
        let pool = sqlx::PgPool::connect(&url).await.unwrap();
        let (a, b, c): (PgAnyInt, PgAnyInt, Option<PgAnyInt>) =
            sqlx::query_as("SELECT 5::int4, 4294967295::int8, NULL::int4")
                .fetch_one(&pool)
                .await
                .unwrap();
        assert_eq!((a.0, b.0, c.map(|v| v.0)), (5, 4_294_967_295, None));
    }
}
