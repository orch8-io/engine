//! SQLite persistence for circuit breakers.
//!
//! Only `Open` rows are ever written by the registry — this module exposes
//! simple upsert/list/delete primitives; the state-machine logic lives in
//! `orch8_engine::circuit_breaker`.

use chrono::{DateTime, Utc};
use sqlx::Row;

use orch8_types::circuit_breaker::{BreakerState, CircuitBreakerState};
use orch8_types::error::StorageError;
use orch8_types::ids::TenantId;

use super::helpers::{parse_ts_opt, ts};
use super::SqliteStorage;

pub(super) async fn upsert(
    storage: &SqliteStorage,
    state: &CircuitBreakerState,
) -> Result<(), StorageError> {
    let opened_at: Option<String> = state.opened_at.map(ts);
    sqlx::query(
        "INSERT INTO circuit_breakers \
            (tenant_id,handler,state,failure_count,failure_threshold,cooldown_secs,opened_at) \
         VALUES (?1,?2,?3,?4,?5,?6,?7) \
         ON CONFLICT(tenant_id,handler) DO UPDATE SET \
            state=excluded.state, \
            failure_count=excluded.failure_count, \
            failure_threshold=excluded.failure_threshold, \
            cooldown_secs=excluded.cooldown_secs, \
            opened_at=excluded.opened_at",
    )
    .bind(&state.tenant_id.0)
    .bind(&state.handler)
    .bind(state.state.to_string())
    .bind(state.failure_count as i64)
    .bind(state.failure_threshold as i64)
    .bind(state.cooldown_secs as i64)
    .bind(opened_at)
    .execute(&storage.pool)
    .await?;
    Ok(())
}

pub(super) async fn list_open(
    storage: &SqliteStorage,
) -> Result<Vec<CircuitBreakerState>, StorageError> {
    let rows = sqlx::query(
        "SELECT tenant_id,handler,state,failure_count,failure_threshold,cooldown_secs,opened_at \
         FROM circuit_breakers WHERE state = 'open'",
    )
    .fetch_all(&storage.pool)
    .await?;

    rows.iter()
        .map(|row| -> Result<CircuitBreakerState, StorageError> {
            let state_str: String = row.get("state");
            let state = match state_str.as_str() {
                "open" => BreakerState::Open,
                "half_open" => BreakerState::HalfOpen,
                "closed" => BreakerState::Closed,
                other => {
                    return Err(StorageError::Query(format!(
                        "unknown breaker state '{other}'"
                    )));
                }
            };
            let opened_at: Option<DateTime<Utc>> =
                parse_ts_opt(row.get::<Option<String>, _>("opened_at"));
            Ok(CircuitBreakerState {
                tenant_id: TenantId(row.get::<String, _>("tenant_id")),
                handler: row.get::<String, _>("handler"),
                state,
                failure_count: row.get::<i64, _>("failure_count") as u32,
                failure_threshold: row.get::<i64, _>("failure_threshold") as u32,
                cooldown_secs: row.get::<i64, _>("cooldown_secs") as u64,
                opened_at,
            })
        })
        .collect()
}

pub(super) async fn delete(
    storage: &SqliteStorage,
    tenant_id: &TenantId,
    handler: &str,
) -> Result<(), StorageError> {
    sqlx::query("DELETE FROM circuit_breakers WHERE tenant_id=?1 AND handler=?2")
        .bind(&tenant_id.0)
        .bind(handler)
        .execute(&storage.pool)
        .await?;
    Ok(())
}
