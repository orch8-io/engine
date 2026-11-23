use bytes::Bytes;
use chrono::{DateTime, Utc};
use orch8_types::error::StorageError;
use orch8_types::ids::TenantId;
use orch8_types::plugin::{WASM_ARTIFACT_SOURCE_PREFIX, WasmArtifactDigest};

use super::{
    SqliteStorage,
    helpers::{begin_immediate, ts},
};
use crate::artifacts::require_store;

pub(super) async fn put(
    storage: &SqliteStorage,
    tenant: &str,
    digest: &WasmArtifactDigest,
    bytes: Bytes,
) -> Result<(), StorageError> {
    let artifact_store = require_store(storage.artifact_store.as_ref())?;
    let mut tx = begin_immediate(&storage.pool).await?;
    artifact_store
        .put_wasm_module(tenant, digest, bytes)
        .await?;
    sqlx::query("DELETE FROM wasm_artifact_retirements WHERE tenant_id=?1 AND digest=?2")
        .bind(tenant)
        .bind(digest.as_str())
        .execute(&mut *tx)
        .await?;
    tx.commit().await?;
    Ok(())
}

pub(super) async fn collect(
    storage: &SqliteStorage,
    tenant_id: &TenantId,
    before: DateTime<Utc>,
    limit: u32,
    apply: bool,
) -> Result<Vec<String>, StorageError> {
    let tenant = tenant_id.as_str();
    let store = require_store(storage.artifact_store.as_ref())?;
    let now = Utc::now();
    let digests = store.old_wasm_modules(tenant, now).await?;
    let mut selected = Vec::new();
    let mut operations = 0;
    for digest in digests {
        if operations == limit {
            break;
        }
        let mut tx = if apply {
            begin_immediate(&storage.pool).await?
        } else {
            storage.pool.begin().await?
        };
        if is_reachable(&mut tx, tenant, &digest).await? {
            continue;
        }
        let ready: Option<bool> = sqlx::query_scalar(
            "SELECT retired=1 OR julianday(unreferenced_at)<=julianday(?3)
             FROM wasm_artifact_retirements WHERE tenant_id=?1 AND digest=?2",
        )
        .bind(tenant)
        .bind(digest.as_str())
        .bind(ts(before))
        .fetch_optional(&mut *tx)
        .await?;
        if apply && ready.is_none() {
            sqlx::query(
                "INSERT INTO wasm_artifact_retirements(tenant_id,digest,unreferenced_at) VALUES (?1,?2,?3)",
            ).bind(tenant).bind(digest.as_str()).bind(ts(now)).execute(&mut *tx).await?;
            tx.commit().await?;
            operations += 1;
            continue;
        }
        if ready != Some(true) || !store.wasm_module_is_old(tenant, &digest, before).await? {
            continue;
        }
        if apply {
            sqlx::query(
                "UPDATE wasm_artifact_retirements SET retired=1 WHERE tenant_id=?1 AND digest=?2",
            )
            .bind(tenant)
            .bind(digest.as_str())
            .execute(&mut *tx)
            .await?;
        }
        tx.commit().await?;
        // A durable tombstone rejects new references even if object deletion
        // fails or this process stops before initiating it.
        if apply {
            let mut deletion = begin_immediate(&storage.pool).await?;
            let retired: bool = sqlx::query_scalar(
                "SELECT EXISTS(SELECT 1 FROM wasm_artifact_retirements WHERE tenant_id=?1 AND digest=?2 AND retired=1)",
            ).bind(tenant).bind(digest.as_str()).fetch_one(&mut *deletion).await?;
            if retired {
                store.delete_wasm_module(tenant, &digest).await?;
            }
            deletion.commit().await?;
            if !retired {
                continue; // An upload reinstalled it since marking.
            }
        }
        selected.push(digest.as_str().to_owned());
        operations += 1;
    }
    Ok(selected)
}

async fn is_reachable(
    connection: &mut sqlx::SqliteConnection,
    tenant: &str,
    digest: &WasmArtifactDigest,
) -> Result<bool, StorageError> {
    let source = format!("{WASM_ARTIFACT_SOURCE_PREFIX}{}", digest.as_str());
    let handler = format!("wasm://{source}");
    Ok(sqlx::query_scalar(r"
        SELECT EXISTS (
            SELECT 1 FROM plugins WHERE tenant_id=?1 AND source=?2
            UNION ALL
            SELECT 1 FROM sequences s, json_tree(json_array(json(s.blocks),json(s.on_cancel),json(s.on_failure))) v
                WHERE s.tenant_id=?1 AND v.value IN (?2,?3)
            UNION ALL
            SELECT 1 FROM task_instances i JOIN injected_blocks b ON b.instance_id=i.id, json_tree(b.blocks) v
                WHERE i.tenant_id=?1 AND v.value IN (?2,?3)
        )")
        .bind(tenant).bind(&source).bind(&handler).fetch_one(connection).await?)
}
