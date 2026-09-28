//! Federation transport persistence (PostgreSQL): trust registry, outbound
//! call records, and the active-region fence. Rows keep a few indexed key
//! columns plus the full JSON `record`, mirroring the continuity tables.

use chrono::{DateTime, Utc};
use sqlx::Row;
use uuid::Uuid;

use orch8_types::continuity_advanced::FederationPeerId;
use orch8_types::error::StorageError;
use orch8_types::federation::{FederationCall, FederationPeerRecord, RegionFence};
use orch8_types::ids::TenantId;

use super::PostgresStorage;

fn encode<T: serde::Serialize>(value: &T) -> Result<serde_json::Value, StorageError> {
    serde_json::to_value(value).map_err(StorageError::Serialization)
}

fn decode<T: serde::de::DeserializeOwned>(value: serde_json::Value) -> Result<T, StorageError> {
    serde_json::from_value(value).map_err(StorageError::Serialization)
}

fn to_i64(value: u64, field: &str) -> Result<i64, StorageError> {
    i64::try_from(value)
        .map_err(|_| StorageError::Query(format!("{field} exceeds PostgreSQL BIGINT range")))
}

fn state_name(call: &FederationCall) -> Result<String, StorageError> {
    encode(&call.state)?
        .as_str()
        .map(ToOwned::to_owned)
        .ok_or_else(|| StorageError::Query("call state did not serialize as a string".into()))
}

pub(super) async fn upsert_peer(
    store: &PostgresStorage,
    peer: &FederationPeerRecord,
) -> Result<(), StorageError> {
    sqlx::query(
        "INSERT INTO federation_peers (tenant_id, peer_id, name, record, updated_at)
         VALUES ($1, $2, $3, $4, $5)
         ON CONFLICT (tenant_id, peer_id) DO UPDATE
         SET name = EXCLUDED.name, record = EXCLUDED.record, updated_at = EXCLUDED.updated_at",
    )
    .bind(peer.tenant_id.as_str())
    .bind(peer.peer_id.into_uuid())
    .bind(&peer.name)
    .bind(encode(peer)?)
    .bind(peer.updated_at)
    .execute(&store.pool)
    .await?;
    Ok(())
}

pub(super) async fn get_peer(
    store: &PostgresStorage,
    tenant_id: &TenantId,
    peer_id: FederationPeerId,
) -> Result<Option<FederationPeerRecord>, StorageError> {
    let row =
        sqlx::query("SELECT record FROM federation_peers WHERE tenant_id = $1 AND peer_id = $2")
            .bind(tenant_id.as_str())
            .bind(peer_id.into_uuid())
            .fetch_optional(&store.pool)
            .await?;
    row.map(|row| decode(row.get("record"))).transpose()
}

pub(super) async fn list_peers(
    store: &PostgresStorage,
    tenant_id: &TenantId,
) -> Result<Vec<FederationPeerRecord>, StorageError> {
    let rows = sqlx::query(
        "SELECT record FROM federation_peers WHERE tenant_id = $1 ORDER BY name LIMIT 1000",
    )
    .bind(tenant_id.as_str())
    .fetch_all(&store.pool)
    .await?;
    rows.into_iter()
        .map(|row| decode(row.get("record")))
        .collect()
}

pub(super) async fn delete_peer(
    store: &PostgresStorage,
    tenant_id: &TenantId,
    peer_id: FederationPeerId,
) -> Result<bool, StorageError> {
    let result = sqlx::query("DELETE FROM federation_peers WHERE tenant_id = $1 AND peer_id = $2")
        .bind(tenant_id.as_str())
        .bind(peer_id.into_uuid())
        .execute(&store.pool)
        .await?;
    Ok(result.rows_affected() == 1)
}

pub(super) async fn create_call(
    store: &PostgresStorage,
    call: &FederationCall,
) -> Result<bool, StorageError> {
    let result = sqlx::query(
        "INSERT INTO federation_calls
         (tenant_id, call_id, instance_id, state, notified, next_poll_at, version, record, created_at)
         VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
         ON CONFLICT (tenant_id, call_id) DO NOTHING",
    )
    .bind(call.tenant_id.as_str())
    .bind(call.call_id)
    .bind(call.instance_id.into_uuid())
    .bind(state_name(call)?)
    .bind(call.notified)
    .bind(call.next_poll_at)
    .bind(to_i64(call.version, "federation call version")?)
    .bind(encode(call)?)
    .bind(call.created_at)
    .execute(&store.pool)
    .await?;
    Ok(result.rows_affected() == 1)
}

pub(super) async fn get_call(
    store: &PostgresStorage,
    tenant_id: &TenantId,
    call_id: Uuid,
) -> Result<Option<FederationCall>, StorageError> {
    let row =
        sqlx::query("SELECT record FROM federation_calls WHERE tenant_id = $1 AND call_id = $2")
            .bind(tenant_id.as_str())
            .bind(call_id)
            .fetch_optional(&store.pool)
            .await?;
    row.map(|row| decode(row.get("record"))).transpose()
}

pub(super) async fn cas_call(
    store: &PostgresStorage,
    expected_version: u64,
    next: &FederationCall,
) -> Result<bool, StorageError> {
    if next.version != expected_version.saturating_add(1) {
        return Err(StorageError::Query(
            "federation call CAS requires next.version = expected + 1".into(),
        ));
    }
    let result = sqlx::query(
        "UPDATE federation_calls
         SET state = $3, notified = $4, next_poll_at = $5, version = $6, record = $7
         WHERE tenant_id = $1 AND call_id = $2 AND version = $8",
    )
    .bind(next.tenant_id.as_str())
    .bind(next.call_id)
    .bind(state_name(next)?)
    .bind(next.notified)
    .bind(next.next_poll_at)
    .bind(to_i64(next.version, "federation call version")?)
    .bind(encode(next)?)
    .bind(to_i64(expected_version, "federation call version")?)
    .execute(&store.pool)
    .await?;
    Ok(result.rows_affected() == 1)
}

pub(super) async fn list_due_calls(
    store: &PostgresStorage,
    now: DateTime<Utc>,
    limit: u32,
) -> Result<Vec<FederationCall>, StorageError> {
    let rows = sqlx::query(
        "SELECT record FROM federation_calls
         WHERE notified = FALSE AND next_poll_at <= $1
         ORDER BY next_poll_at LIMIT $2",
    )
    .bind(now)
    .bind(i64::from(limit.min(1_000)))
    .fetch_all(&store.pool)
    .await?;
    rows.into_iter()
        .map(|row| decode(row.get("record")))
        .collect()
}

pub(super) async fn get_fence(
    store: &PostgresStorage,
) -> Result<Option<RegionFence>, StorageError> {
    let row = sqlx::query("SELECT record FROM region_fence WHERE singleton = TRUE")
        .fetch_optional(&store.pool)
        .await?;
    row.map(|row| decode(row.get("record"))).transpose()
}

pub(super) async fn advance_fence(
    store: &PostgresStorage,
    expected_epoch: Option<u64>,
    next: &RegionFence,
) -> Result<bool, StorageError> {
    let result = match expected_epoch {
        None => {
            sqlx::query(
                "INSERT INTO region_fence (singleton, active_region, epoch, record, updated_at)
                 VALUES (TRUE, $1, $2, $3, $4)
                 ON CONFLICT (singleton) DO NOTHING",
            )
            .bind(&next.active_region)
            .bind(to_i64(next.epoch, "fence epoch")?)
            .bind(encode(next)?)
            .bind(next.updated_at)
            .execute(&store.pool)
            .await?
        }
        Some(expected) => {
            if next.epoch != expected.saturating_add(1) {
                return Ok(false);
            }
            sqlx::query(
                "UPDATE region_fence
                 SET active_region = $1, epoch = $2, record = $3, updated_at = $4
                 WHERE singleton = TRUE AND epoch = $5",
            )
            .bind(&next.active_region)
            .bind(to_i64(next.epoch, "fence epoch")?)
            .bind(encode(next)?)
            .bind(next.updated_at)
            .bind(to_i64(expected, "fence epoch")?)
            .execute(&store.pool)
            .await?
        }
    };
    Ok(result.rows_affected() == 1)
}
