//! Postgres persistence for circuit breakers. Only `Open` rows are written by
//! the registry; rehydration on boot reads them back so a crash mid-cooldown
//! does not reset every tripped breaker to `Closed`.

use chrono::{DateTime, Utc};

use orch8_types::circuit_breaker::{BreakerState, CircuitBreakerState};
use orch8_types::error::StorageError;
use orch8_types::ids::TenantId;

use super::PostgresStorage;

#[allow(clippy::cast_possible_wrap)]
pub(super) async fn upsert(
    store: &PostgresStorage,
    state: &CircuitBreakerState,
) -> Result<(), StorageError> {
    // cooldown_secs is u64 from the domain but stored as BIGINT; seconds-scale
    // values never approach i64 overflow.
    let cooldown_secs: i64 = state.cooldown_secs as i64;
    sqlx::query(
        r"INSERT INTO circuit_breakers
            (tenant_id, handler, state, failure_count, failure_threshold, cooldown_secs, opened_at)
          VALUES ($1, $2, $3, $4, $5, $6, $7)
          ON CONFLICT (tenant_id, handler) DO UPDATE SET
            state = EXCLUDED.state,
            failure_count = EXCLUDED.failure_count,
            failure_threshold = EXCLUDED.failure_threshold,
            cooldown_secs = EXCLUDED.cooldown_secs,
            opened_at = EXCLUDED.opened_at",
    )
    .bind(&state.tenant_id.0)
    .bind(&state.handler)
    .bind(state.state.to_string())
    .bind(i64::from(state.failure_count))
    .bind(i64::from(state.failure_threshold))
    .bind(cooldown_secs)
    .bind(state.opened_at)
    .execute(&store.pool)
    .await?;
    Ok(())
}

type BreakerRow = (String, String, String, i32, i32, i64, Option<DateTime<Utc>>);

pub(super) async fn list_open(
    store: &PostgresStorage,
) -> Result<Vec<CircuitBreakerState>, StorageError> {
    let rows: Vec<BreakerRow> = sqlx::query_as(
        "SELECT tenant_id, handler, state, failure_count, failure_threshold, cooldown_secs, \
                opened_at \
         FROM circuit_breakers WHERE state = 'open'",
    )
    .fetch_all(&store.pool)
    .await?;

    rows.into_iter()
        .map(
            |(tenant_id, handler, state, fc, ft, cs, opened_at)| -> Result<_, StorageError> {
                let state = match state.as_str() {
                    "open" => BreakerState::Open,
                    "half_open" => BreakerState::HalfOpen,
                    "closed" => BreakerState::Closed,
                    other => {
                        return Err(StorageError::Query(format!(
                            "unknown breaker state '{other}'"
                        )));
                    }
                };
                #[allow(clippy::cast_sign_loss)]
                Ok(CircuitBreakerState {
                    tenant_id: TenantId(tenant_id),
                    handler,
                    state,
                    failure_count: fc as u32,
                    failure_threshold: ft as u32,
                    cooldown_secs: cs as u64,
                    opened_at,
                })
            },
        )
        .collect()
}

pub(super) async fn delete(
    store: &PostgresStorage,
    tenant_id: &TenantId,
    handler: &str,
) -> Result<(), StorageError> {
    sqlx::query("DELETE FROM circuit_breakers WHERE tenant_id = $1 AND handler = $2")
        .bind(&tenant_id.0)
        .bind(handler)
        .execute(&store.pool)
        .await?;
    Ok(())
}
