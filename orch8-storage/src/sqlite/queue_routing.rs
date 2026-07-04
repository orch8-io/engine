use sqlx::Row;
use uuid::Uuid;

use orch8_types::error::StorageError;
use orch8_types::ids::TenantId;
use orch8_types::queue_routing::QueueRoutingRule;

use super::SqliteStorage;
use super::helpers::{parse_ts, ts};

fn row_to_rule(row: &sqlx::sqlite::SqliteRow) -> Result<QueueRoutingRule, StorageError> {
    Ok(QueueRoutingRule {
        id: Uuid::parse_str(row.get::<&str, _>("id"))
            .map_err(|e| StorageError::Query(e.to_string()))?,
        tenant_id: row.get("tenant_id"),
        handler_name: row.get("handler_name"),
        match_queue: row.get("match_queue"),
        queue_override: row.get("queue_override"),
        priority: row.get("priority"),
        enabled: row.get::<i64, _>("enabled") != 0,
        created_at: parse_ts(row.get::<&str, _>("created_at"))?,
        updated_at: parse_ts(row.get::<&str, _>("updated_at"))?,
    })
}

pub(super) async fn create(
    storage: &SqliteStorage,
    rule: &QueueRoutingRule,
) -> Result<(), StorageError> {
    sqlx::query(
        "INSERT INTO queue_routing_rules (id, tenant_id, handler_name, match_queue, queue_override, priority, enabled, created_at, updated_at) VALUES (?1,?2,?3,?4,?5,?6,?7,?8,?9)",
    )
    .bind(rule.id.to_string())
    .bind(&rule.tenant_id)
    .bind(&rule.handler_name)
    .bind(&rule.match_queue)
    .bind(&rule.queue_override)
    .bind(rule.priority)
    .bind(i64::from(rule.enabled))
    .bind(ts(rule.created_at))
    .bind(ts(rule.updated_at))
    .execute(&storage.pool)
    .await?;
    Ok(())
}

pub(super) async fn list(
    storage: &SqliteStorage,
    tenant_id: Option<&TenantId>,
    handler_name: Option<&str>,
) -> Result<Vec<QueueRoutingRule>, StorageError> {
    let mut qb = sqlx::QueryBuilder::new(
        "SELECT id, tenant_id, handler_name, match_queue, queue_override, priority, enabled, created_at, updated_at FROM queue_routing_rules WHERE 1=1",
    );
    if let Some(t) = tenant_id {
        qb.push(" AND tenant_id=").push_bind(t.as_str());
    }
    if let Some(h) = handler_name {
        qb.push(" AND handler_name=").push_bind(h);
    }
    qb.push(" ORDER BY priority DESC, created_at ASC");
    let rows = qb.build().fetch_all(&storage.pool).await?;
    rows.iter().map(row_to_rule).collect()
}

pub(super) async fn get(
    storage: &SqliteStorage,
    id: Uuid,
) -> Result<Option<QueueRoutingRule>, StorageError> {
    let row = sqlx::query(
        "SELECT id, tenant_id, handler_name, match_queue, queue_override, priority, enabled, created_at, updated_at FROM queue_routing_rules WHERE id = ?1",
    )
    .bind(id.to_string())
    .fetch_optional(&storage.pool)
    .await?;
    row.as_ref().map(row_to_rule).transpose()
}

pub(super) async fn delete(storage: &SqliteStorage, id: Uuid) -> Result<(), StorageError> {
    sqlx::query("DELETE FROM queue_routing_rules WHERE id = ?1")
        .bind(id.to_string())
        .execute(&storage.pool)
        .await?;
    Ok(())
}
