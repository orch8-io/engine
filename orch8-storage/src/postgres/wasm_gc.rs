use bytes::Bytes;
use chrono::{DateTime, Utc};
use orch8_types::error::StorageError;
use orch8_types::ids::TenantId;
use orch8_types::plugin::{WASM_ARTIFACT_SOURCE_PREFIX, WasmArtifactDigest};

use super::PostgresStorage;
use crate::artifacts::require_store;

async fn lock_retirements(connection: &mut sqlx::PgConnection) -> Result<(), StorageError> {
    sqlx::query("SET LOCAL lock_timeout = '2s'")
        .execute(&mut *connection)
        .await?;
    sqlx::query("LOCK TABLE wasm_artifact_retirements IN SHARE ROW EXCLUSIVE MODE")
        .execute(connection)
        .await?;
    Ok(())
}

pub(super) async fn put(
    storage: &PostgresStorage,
    tenant: &str,
    digest: &WasmArtifactDigest,
    bytes: Bytes,
) -> Result<(), StorageError> {
    let artifact_store = require_store(storage.artifact_store.as_ref())?;
    let mut tx = storage.pool.begin().await?;
    lock_retirements(&mut tx).await?;
    artifact_store
        .put_wasm_module(tenant, digest, bytes)
        .await?;
    sqlx::query("DELETE FROM wasm_artifact_retirements WHERE tenant_id=$1 AND digest=$2")
        .bind(tenant)
        .bind(digest.as_str())
        .execute(&mut *tx)
        .await?;
    tx.commit().await?;
    Ok(())
}

pub(super) async fn collect(
    storage: &PostgresStorage,
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
        let mut tx = storage.pool.begin().await?;
        if apply {
            sqlx::query("SET LOCAL lock_timeout = '2s'")
                .execute(&mut *tx)
                .await?;
            // Prevent any root insertion from committing between reachability
            // verification and publication of the retirement marker.
            sqlx::query("LOCK TABLE sequences, plugins, task_instances IN SHARE MODE")
                .execute(&mut *tx)
                .await?;
            lock_retirements(&mut tx).await?;
        }
        if is_reachable(&mut tx, tenant, &digest).await? {
            continue;
        }
        let ready: Option<bool> = sqlx::query_scalar(
            "SELECT retired OR unreferenced_at<=$3 FROM wasm_artifact_retirements WHERE tenant_id=$1 AND digest=$2",
        ).bind(tenant).bind(digest.as_str()).bind(before).fetch_optional(&mut *tx).await?;
        if apply && ready.is_none() {
            sqlx::query(
                "INSERT INTO wasm_artifact_retirements(tenant_id,digest,unreferenced_at) VALUES ($1,$2,$3)",
            ).bind(tenant).bind(digest.as_str()).bind(now).execute(&mut *tx).await?;
            tx.commit().await?;
            operations += 1;
            continue;
        }
        if ready != Some(true) || !store.wasm_module_is_old(tenant, &digest, before).await? {
            continue;
        }
        if apply {
            sqlx::query("UPDATE wasm_artifact_retirements SET retired=TRUE WHERE tenant_id=$1 AND digest=$2")
                .bind(tenant).bind(digest.as_str()).execute(&mut *tx).await?;
        }
        tx.commit().await?;
        if apply {
            let mut deletion = storage.pool.begin().await?;
            lock_retirements(&mut deletion).await?;
            let retired: bool = sqlx::query_scalar(
                "SELECT EXISTS(SELECT 1 FROM wasm_artifact_retirements WHERE tenant_id=$1 AND digest=$2 AND retired)",
            ).bind(tenant).bind(digest.as_str()).fetch_one(&mut *deletion).await?;
            if retired {
                store.delete_wasm_module(tenant, &digest).await?;
            }
            deletion.commit().await?;
            if !retired {
                continue;
            }
        }
        selected.push(digest.as_str().to_owned());
        operations += 1;
    }
    Ok(selected)
}

async fn is_reachable(
    connection: &mut sqlx::PgConnection,
    tenant: &str,
    digest: &WasmArtifactDigest,
) -> Result<bool, StorageError> {
    let source = format!("{WASM_ARTIFACT_SOURCE_PREFIX}{}", digest.as_str());
    let handler = format!("wasm://{source}");
    let addresses = serde_json::json!({"source":source,"handler":handler});
    Ok(sqlx::query_scalar(r"
        SELECT EXISTS (
            SELECT 1 FROM plugins WHERE tenant_id=$1 AND source=$2
            UNION ALL
            SELECT 1 FROM sequences WHERE tenant_id=$1
                AND jsonb_path_exists(definition, '$.** ? (@ == $source || @ == $handler)', $3)
            UNION ALL
            SELECT 1 FROM task_instances WHERE tenant_id=$1
                AND jsonb_path_exists(metadata->'_injected_blocks', '$.** ? (@ == $source || @ == $handler)', $3)
        )")
        .bind(tenant).bind(&source).bind(&addresses).fetch_one(connection).await?)
}
