use chrono::{DateTime, Utc};
use sqlx::Row;
use uuid::Uuid;

use orch8_types::audit::AuditLogEntry;
use orch8_types::checkpoint::Checkpoint;
use orch8_types::cluster::{ClusterNode, NodeStatus};
use orch8_types::cron::CronSchedule;
use orch8_types::execution::{BlockType, ExecutionNode, NodeState};
use orch8_types::filter::InstanceFilter;
use orch8_types::ids::*;
use orch8_types::instance::{InstanceState, Priority, TaskInstance};
use orch8_types::output::BlockOutput;
use orch8_types::pool::{PoolResource, ResourcePool};
use orch8_types::session::{Session, SessionState};
use orch8_types::signal::Signal;
use orch8_types::worker::WorkerTask;

pub(super) fn ts(dt: DateTime<Utc>) -> String {
    dt.to_rfc3339()
}

pub(super) fn parse_ts(s: &str) -> DateTime<Utc> {
    DateTime::parse_from_rfc3339(s)
        .map(|dt| dt.with_timezone(&Utc))
        .unwrap_or_default()
}

pub(super) fn parse_ts_opt(s: Option<String>) -> Option<DateTime<Utc>> {
    s.map(|v| parse_ts(&v))
}

pub(super) fn parse_state(s: &str) -> InstanceState {
    match s {
        "running" => InstanceState::Running,
        "waiting" => InstanceState::Waiting,
        "paused" => InstanceState::Paused,
        "completed" => InstanceState::Completed,
        "failed" => InstanceState::Failed,
        "cancelled" => InstanceState::Cancelled,
        _ => InstanceState::Scheduled,
    }
}

pub(super) fn parse_node_state(s: &str) -> NodeState {
    match s {
        "running" => NodeState::Running,
        "waiting" => NodeState::Waiting,
        "completed" => NodeState::Completed,
        "failed" => NodeState::Failed,
        "cancelled" => NodeState::Cancelled,
        "skipped" => NodeState::Skipped,
        _ => NodeState::Pending,
    }
}

pub(super) fn parse_block_type(s: &str) -> BlockType {
    match s {
        "parallel" => BlockType::Parallel,
        "race" => BlockType::Race,
        "loop" => BlockType::Loop,
        "for_each" => BlockType::ForEach,
        "router" => BlockType::Router,
        "try_catch" => BlockType::TryCatch,
        "sub_sequence" => BlockType::SubSequence,
        "ab_split" => BlockType::ABSplit,
        _ => BlockType::Step,
    }
}

pub(super) fn parse_priority(v: i64) -> Priority {
    match v {
        0 => Priority::Low,
        2 => Priority::High,
        3 => Priority::Critical,
        _ => Priority::Normal,
    }
}

pub(super) fn row_to_instance(row: &sqlx::sqlite::SqliteRow) -> TaskInstance {
    TaskInstance {
        id: InstanceId(Uuid::parse_str(row.get::<&str, _>("id")).unwrap_or_default()),
        sequence_id: SequenceId(
            Uuid::parse_str(row.get::<&str, _>("sequence_id")).unwrap_or_default(),
        ),
        tenant_id: TenantId(row.get::<String, _>("tenant_id")),
        namespace: Namespace(row.get::<String, _>("namespace")),
        state: parse_state(row.get::<&str, _>("state")),
        next_fire_at: parse_ts_opt(row.get::<Option<String>, _>("next_fire_at")),
        priority: parse_priority(row.get::<i64, _>("priority")),
        timezone: row.get::<String, _>("timezone"),
        metadata: serde_json::from_str(row.get::<&str, _>("metadata")).unwrap_or_default(),
        context: serde_json::from_str(row.get::<&str, _>("context")).unwrap_or_default(),
        concurrency_key: row.get::<Option<String>, _>("concurrency_key"),
        max_concurrency: row.get::<Option<i32>, _>("max_concurrency"),
        idempotency_key: row.get::<Option<String>, _>("idempotency_key"),
        session_id: row
            .get::<Option<String>, _>("session_id")
            .and_then(|s| Uuid::parse_str(&s).ok()),
        parent_instance_id: row
            .get::<Option<String>, _>("parent_instance_id")
            .and_then(|s| Uuid::parse_str(&s).ok())
            .map(InstanceId),
        created_at: parse_ts(row.get::<&str, _>("created_at")),
        updated_at: parse_ts(row.get::<&str, _>("updated_at")),
    }
}

pub(super) fn row_to_node(row: &sqlx::sqlite::SqliteRow) -> ExecutionNode {
    ExecutionNode {
        id: ExecutionNodeId(Uuid::parse_str(row.get::<&str, _>("id")).unwrap_or_default()),
        instance_id: InstanceId(
            Uuid::parse_str(row.get::<&str, _>("instance_id")).unwrap_or_default(),
        ),
        block_id: BlockId(row.get::<String, _>("block_id")),
        parent_id: row
            .get::<Option<String>, _>("parent_id")
            .and_then(|s| Uuid::parse_str(&s).ok())
            .map(ExecutionNodeId),
        block_type: parse_block_type(row.get::<&str, _>("block_type")),
        branch_index: row.get::<Option<i32>, _>("branch_index").map(|v| v as i16),
        state: parse_node_state(row.get::<&str, _>("state")),
        started_at: parse_ts_opt(row.get::<Option<String>, _>("started_at")),
        completed_at: parse_ts_opt(row.get::<Option<String>, _>("completed_at")),
    }
}

pub(super) fn row_to_sequence(
    row: &sqlx::sqlite::SqliteRow,
) -> orch8_types::sequence::SequenceDefinition {
    orch8_types::sequence::SequenceDefinition {
        id: SequenceId(Uuid::parse_str(row.get::<&str, _>("id")).unwrap_or_default()),
        tenant_id: TenantId(row.get::<String, _>("tenant_id")),
        namespace: Namespace(row.get::<String, _>("namespace")),
        name: row.get::<String, _>("name"),
        version: row.get::<i32, _>("version"),
        deprecated: row.get::<i32, _>("deprecated") != 0,
        blocks: serde_json::from_str(row.get::<&str, _>("blocks")).unwrap_or_default(),
        interceptors: row
            .get::<Option<String>, _>("interceptors")
            .and_then(|s| serde_json::from_str(&s).ok()),
        created_at: parse_ts(row.get::<&str, _>("created_at")),
    }
}

pub(super) fn row_to_output(row: &sqlx::sqlite::SqliteRow) -> BlockOutput {
    BlockOutput {
        id: Uuid::parse_str(row.get::<&str, _>("id")).unwrap_or_default(),
        instance_id: InstanceId(
            Uuid::parse_str(row.get::<&str, _>("instance_id")).unwrap_or_default(),
        ),
        block_id: BlockId(row.get::<String, _>("block_id")),
        output: serde_json::from_str(row.get::<&str, _>("output")).unwrap_or_default(),
        output_ref: row.get::<Option<String>, _>("output_ref"),
        output_size: row.get::<i64, _>("output_size") as i32,
        attempt: row.get::<i64, _>("attempt") as i16,
        created_at: parse_ts(row.get::<&str, _>("created_at")),
    }
}

pub(super) fn row_to_signal(row: &sqlx::sqlite::SqliteRow) -> Signal {
    Signal {
        id: Uuid::parse_str(row.get::<&str, _>("id")).unwrap_or_default(),
        instance_id: InstanceId(
            Uuid::parse_str(row.get::<&str, _>("instance_id")).unwrap_or_default(),
        ),
        signal_type: serde_json::from_str(row.get::<&str, _>("signal_type"))
            .unwrap_or(orch8_types::signal::SignalType::Resume),
        payload: serde_json::from_str(row.get::<&str, _>("payload")).unwrap_or_default(),
        delivered: row.get::<i32, _>("delivered") != 0,
        created_at: parse_ts(row.get::<&str, _>("created_at")),
        delivered_at: None,
    }
}

pub(super) fn row_to_cron(row: &sqlx::sqlite::SqliteRow) -> CronSchedule {
    CronSchedule {
        id: Uuid::parse_str(row.get::<&str, _>("id")).unwrap_or_default(),
        tenant_id: TenantId(row.get::<String, _>("tenant_id")),
        namespace: Namespace(row.get::<String, _>("namespace")),
        sequence_id: SequenceId(
            Uuid::parse_str(row.get::<&str, _>("sequence_id")).unwrap_or_default(),
        ),
        cron_expr: row.get::<String, _>("cron_expr"),
        timezone: row.get::<String, _>("timezone"),
        enabled: row.get::<i32, _>("enabled") != 0,
        metadata: serde_json::from_str(row.get::<&str, _>("metadata")).unwrap_or_default(),
        next_fire_at: parse_ts_opt(row.get::<Option<String>, _>("next_fire_at")),
        last_triggered_at: parse_ts_opt(row.get::<Option<String>, _>("last_triggered_at")),
        created_at: parse_ts(row.get::<&str, _>("created_at")),
        updated_at: parse_ts(row.get::<&str, _>("updated_at")),
    }
}

pub(super) fn row_to_worker_task(row: &sqlx::sqlite::SqliteRow) -> WorkerTask {
    use orch8_types::worker::WorkerTaskState;
    let state = match row.get::<&str, _>("state") {
        "claimed" => WorkerTaskState::Claimed,
        "completed" => WorkerTaskState::Completed,
        "failed" => WorkerTaskState::Failed,
        _ => WorkerTaskState::Pending,
    };
    WorkerTask {
        id: Uuid::parse_str(row.get::<&str, _>("id")).unwrap_or_default(),
        instance_id: InstanceId(
            Uuid::parse_str(row.get::<&str, _>("instance_id")).unwrap_or_default(),
        ),
        block_id: BlockId(row.get::<String, _>("block_id")),
        handler_name: row.get::<String, _>("handler_name"),
        queue_name: row.get::<Option<String>, _>("queue_name"),
        params: serde_json::from_str(row.get::<&str, _>("params")).unwrap_or_default(),
        context: serde_json::from_str(row.get::<&str, _>("context")).unwrap_or_default(),
        attempt: row.get::<i64, _>("attempt") as i16,
        timeout_ms: row.get::<Option<i64>, _>("timeout_ms"),
        state,
        worker_id: row.get::<Option<String>, _>("worker_id"),
        claimed_at: parse_ts_opt(row.get::<Option<String>, _>("claimed_at")),
        heartbeat_at: parse_ts_opt(row.get::<Option<String>, _>("heartbeat_at")),
        completed_at: parse_ts_opt(row.get::<Option<String>, _>("completed_at")),
        output: row
            .get::<Option<String>, _>("output")
            .and_then(|s| serde_json::from_str(&s).ok()),
        error_message: row.get::<Option<String>, _>("error_message"),
        error_retryable: row.get::<Option<i32>, _>("error_retryable").map(|v| v != 0),
        created_at: parse_ts(row.get::<&str, _>("created_at")),
    }
}

pub(super) fn row_to_pool(row: &sqlx::sqlite::SqliteRow) -> ResourcePool {
    use orch8_types::pool::RotationStrategy;
    let strategy_str = row.get::<String, _>("strategy");
    let strategy = serde_json::from_str::<RotationStrategy>(&format!("\"{strategy_str}\""))
        .unwrap_or(RotationStrategy::RoundRobin);
    ResourcePool {
        id: Uuid::parse_str(row.get::<&str, _>("id")).unwrap_or_default(),
        tenant_id: TenantId(row.get::<String, _>("tenant_id")),
        name: row.get::<String, _>("name"),
        strategy,
        round_robin_index: row.get::<i64, _>("round_robin_index") as u32,
        created_at: parse_ts(row.get::<&str, _>("created_at")),
        updated_at: parse_ts(row.get::<&str, _>("updated_at")),
    }
}

pub(super) fn row_to_pool_resource(row: &sqlx::sqlite::SqliteRow) -> PoolResource {
    PoolResource {
        id: Uuid::parse_str(row.get::<&str, _>("id")).unwrap_or_default(),
        pool_id: Uuid::parse_str(row.get::<&str, _>("pool_id")).unwrap_or_default(),
        resource_key: ResourceKey(row.get::<String, _>("resource_key")),
        name: row.get::<String, _>("name"),
        weight: row.get::<i64, _>("weight") as u32,
        enabled: row.get::<i32, _>("enabled") != 0,
        daily_cap: row.get::<i64, _>("daily_cap") as u32,
        daily_usage: row.get::<i64, _>("daily_usage") as u32,
        daily_usage_date: row
            .get::<Option<String>, _>("daily_usage_date")
            .and_then(|s| chrono::NaiveDate::parse_from_str(&s, "%Y-%m-%d").ok()),
        warmup_start: row
            .get::<Option<String>, _>("warmup_start")
            .and_then(|s| chrono::NaiveDate::parse_from_str(&s, "%Y-%m-%d").ok()),
        warmup_days: row.get::<i64, _>("warmup_days") as u32,
        warmup_start_cap: row.get::<i64, _>("warmup_start_cap") as u32,
        created_at: parse_ts(row.get::<&str, _>("created_at")),
    }
}

pub(super) fn row_to_checkpoint(row: &sqlx::sqlite::SqliteRow) -> Checkpoint {
    Checkpoint {
        id: Uuid::parse_str(row.get::<&str, _>("id")).unwrap_or_default(),
        instance_id: InstanceId(
            Uuid::parse_str(row.get::<&str, _>("instance_id")).unwrap_or_default(),
        ),
        checkpoint_data: serde_json::from_str(row.get::<&str, _>("checkpoint_data"))
            .unwrap_or_default(),
        created_at: parse_ts(row.get::<&str, _>("created_at")),
    }
}

pub(super) fn row_to_session(row: &sqlx::sqlite::SqliteRow) -> Session {
    let state = match row.get::<&str, _>("state") {
        "completed" => SessionState::Completed,
        "expired" => SessionState::Expired,
        _ => SessionState::Active,
    };
    Session {
        id: Uuid::parse_str(row.get::<&str, _>("id")).unwrap_or_default(),
        tenant_id: TenantId(row.get::<String, _>("tenant_id")),
        session_key: row.get::<String, _>("session_key"),
        data: serde_json::from_str(row.get::<&str, _>("data")).unwrap_or_default(),
        state,
        created_at: parse_ts(row.get::<&str, _>("created_at")),
        updated_at: parse_ts(row.get::<&str, _>("updated_at")),
        expires_at: parse_ts_opt(row.get::<Option<String>, _>("expires_at")),
    }
}

pub(super) fn row_to_audit(row: &sqlx::sqlite::SqliteRow) -> AuditLogEntry {
    AuditLogEntry {
        id: Uuid::parse_str(row.get::<&str, _>("id")).unwrap_or_default(),
        instance_id: InstanceId(
            Uuid::parse_str(row.get::<&str, _>("instance_id")).unwrap_or_default(),
        ),
        tenant_id: TenantId(row.get::<String, _>("tenant_id")),
        event_type: row.get::<String, _>("event_type"),
        from_state: row.get::<Option<String>, _>("from_state"),
        to_state: row.get::<Option<String>, _>("to_state"),
        block_id: row.get::<Option<String>, _>("block_id"),
        details: serde_json::from_str(row.get::<&str, _>("details")).unwrap_or_default(),
        created_at: parse_ts(row.get::<&str, _>("created_at")),
    }
}

pub(super) fn row_to_cluster_node(row: &sqlx::sqlite::SqliteRow) -> ClusterNode {
    let status = match row.get::<&str, _>("status") {
        "draining" => NodeStatus::Draining,
        "stopped" => NodeStatus::Stopped,
        _ => NodeStatus::Active,
    };
    ClusterNode {
        id: Uuid::parse_str(row.get::<&str, _>("id")).unwrap_or_default(),
        name: row.get::<String, _>("name"),
        status,
        registered_at: parse_ts(row.get::<&str, _>("registered_at")),
        last_heartbeat_at: parse_ts(row.get::<&str, _>("last_heartbeat_at")),
        drain: row.get::<i32, _>("drain") != 0,
    }
}

/// Build a simple SQL filter string (no parameterized queries -- SQLite test mode only).
pub(super) fn apply_filter_sql(sql: &mut String, filter: &InstanceFilter) {
    if let Some(ref tid) = filter.tenant_id {
        sql.push_str(&format!(" AND tenant_id='{}'", tid.0.replace('\'', "''")));
    }
    if let Some(ref ns) = filter.namespace {
        sql.push_str(&format!(" AND namespace='{}'", ns.0.replace('\'', "''")));
    }
    if let Some(ref sid) = filter.sequence_id {
        sql.push_str(&format!(" AND sequence_id='{}'", sid.0));
    }
    if let Some(ref states) = filter.states {
        if !states.is_empty() {
            let state_strs: Vec<String> = states.iter().map(|s| format!("'{s}'")).collect();
            sql.push_str(&format!(" AND state IN ({})", state_strs.join(",")));
        }
    }
    if let Some(ref p) = filter.priority {
        sql.push_str(&format!(" AND priority={}", *p as i16));
    }
}
