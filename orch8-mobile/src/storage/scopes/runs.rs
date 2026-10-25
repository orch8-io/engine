//! Durable admission before scoped dispatch. These records cannot be picked up
//! by an unscoped scheduler and are not exposed as running engine instances.
use super::workflows::{checked_definition, read_owner};
use super::{CatalogScope, fence_catalog, invalid, require_reconciled, validate};
use crate::storage::MobileStorage;
use orch8_types::{
    error::StorageError,
    ids::{InstanceId, SequenceId},
};
use serde_json::Value;
use sha2::{Digest, Sha256};
use sqlx::{Row, sqlite::SqliteRow};

const MAX_INPUT_BYTES: usize = 1024 * 1024;

pub(crate) struct CatalogRunRequest<'a> {
    pub(crate) workflow: &'a str,
    pub(crate) version: Option<i32>,
    pub(crate) input_json: &'a str,
    pub(crate) dedup_key: Option<&'a str>,
}

/// Trusted runtime ceilings, shared by callers using this database. Cancelled
/// requests stay charged until explicitly forgotten; retirement deletes nothing.
#[derive(Clone, Copy)]
pub(crate) struct CatalogRunLimits {
    pub(crate) global_records: u32,
    pub(crate) account_records: u32,
    pub(crate) catalog_records: u32,
    pub(crate) global_input_bytes: u64,
    pub(crate) account_input_bytes: u64,
    pub(crate) catalog_input_bytes: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum CatalogRunState {
    Prepared,
    Cancelled,
}

#[derive(Debug)]
pub(crate) struct CatalogRun {
    pub(crate) id: InstanceId,
    pub(crate) account_id: String,
    pub(crate) catalog_id: String,
    pub(crate) owner_epoch: i64,
    pub(crate) admitted_manifest_version: i64,
    pub(crate) admitted_manifest_digest: String,
    pub(crate) created_at: chrono::DateTime<chrono::Utc>,
    pub(crate) sequence_id: SequenceId,
    pub(crate) workflow: String,
    pub(crate) version: i32,
    pub(crate) state: CatalogRunState,
    pub(crate) input: Value,
}

fn decode(row: &SqliteRow) -> Result<CatalogRun, StorageError> {
    let id: String = row.try_get("id")?;
    let sequence: String = row.try_get("selected_sequence_id")?;
    let input: String = row.try_get("input")?;
    let epoch: i64 = row.try_get("owner_epoch")?;
    if epoch <= 0
        || input.len() > MAX_INPUT_BYTES
        || usize::try_from(row.try_get::<i64, _>("input_bytes")?).ok() != Some(input.len())
    {
        return Err(invalid("invalid scoped run record"));
    }
    let workflow: String = row.try_get("workflow_name")?;
    validate(&workflow)?;
    let requested_version: Option<i32> = row.try_get("requested_version")?;
    let expected: String = row.try_get("request_digest")?;
    if expected != fingerprint(&workflow, requested_version, &input)? {
        return Err(invalid("scoped run request identity is corrupt"));
    }
    Ok(CatalogRun {
        id: id
            .parse()
            .map(InstanceId::from_uuid)
            .map_err(|_| invalid("invalid scoped run ID"))?,
        sequence_id: sequence
            .parse()
            .map(SequenceId::from_uuid)
            .map_err(|_| invalid("invalid scoped sequence ID"))?,
        account_id: row.try_get("account_id")?,
        catalog_id: row.try_get("catalog_id")?,
        owner_epoch: epoch,
        admitted_manifest_version: row.try_get("admitted_manifest_version")?,
        admitted_manifest_digest: row.try_get("admitted_manifest_digest")?,
        created_at: chrono::DateTime::parse_from_rfc3339(&row.try_get::<String, _>("created_at")?)
            .map_err(|_| invalid("invalid scoped run timestamp"))?
            .with_timezone(&chrono::Utc),
        workflow,
        version: row.try_get("workflow_version")?,
        state: match row.try_get::<String, _>("state")?.as_str() {
            "prepared" => CatalogRunState::Prepared,
            "cancelled" => CatalogRunState::Cancelled,
            _ => return Err(invalid("invalid scoped run state")),
        },
        input: serde_json::from_str(&input).map_err(|_| invalid("invalid scoped run input"))?,
    })
}

const USAGE_SQL: &str = "SELECT COUNT(*), COALESCE(SUM(account_id = ?1),0), COALESCE(SUM(account_id = ?1 AND catalog_id = ?2),0), COALESCE(SUM(input_bytes),0), COALESCE(SUM(CASE WHEN account_id = ?1 THEN input_bytes ELSE 0 END),0), COALESCE(SUM(CASE WHEN account_id = ?1 AND catalog_id = ?2 THEN input_bytes ELSE 0 END),0) FROM mobile_catalog_run_requests INDEXED BY idx_mobile_catalog_run_usage";

fn fingerprint(workflow: &str, version: Option<i32>, input: &str) -> Result<String, StorageError> {
    Ok(hex::encode(Sha256::digest(serde_json::to_vec(&(
        workflow, version, input,
    ))?)))
}

fn request_identity(request: &CatalogRunRequest<'_>) -> Result<(String, String), StorageError> {
    validate(request.workflow)?;
    if request.version.is_some_and(|version| version <= 0) {
        return Err(invalid("invalid workflow version"));
    }
    if let Some(key) = request.dedup_key {
        validate(key)?;
    }
    if request.input_json.len() > MAX_INPUT_BYTES {
        return Err(StorageError::QuotaExceeded(
            "scoped run input exceeds one MiB".into(),
        ));
    }
    let input: Value = serde_json::from_str(request.input_json)
        .map_err(|_| invalid("invalid scoped run input JSON"))?;
    let input = orch8_publisher::manifest::canonical_json(&input)
        .map_err(|_| invalid("invalid scoped run input JSON"))?;
    if input.len() > MAX_INPUT_BYTES {
        return Err(StorageError::QuotaExceeded(
            "canonical scoped input exceeds one MiB".into(),
        ));
    }
    // Requested selector, not the current resolved definition: a retry of
    // "latest" must keep the first version even after a catalog upgrade.
    let digest = fingerprint(request.workflow, request.version, &input)?;
    Ok((input, digest))
}

async fn ensure_capacity(
    tx: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
    scope: &CatalogScope,
    input_len: usize,
    limits: CatalogRunLimits,
) -> Result<(), StorageError> {
    let (global, account, catalog, global_bytes, account_bytes, catalog_bytes): (
        i64,
        i64,
        i64,
        i64,
        i64,
        i64,
    ) = sqlx::query_as(USAGE_SQL)
        .bind(&scope.account.id)
        .bind(&scope.id)
        .fetch_one(&mut **tx)
        .await?;
    for (count, cap) in [
        (global, limits.global_records),
        (account, limits.account_records),
        (catalog, limits.catalog_records),
    ] {
        if u64::try_from(count).unwrap_or(u64::MAX) >= u64::from(cap) {
            return Err(StorageError::QuotaExceeded(
                "scoped run record allowance exhausted".into(),
            ));
        }
    }
    for (used, cap) in [
        (global_bytes, limits.global_input_bytes),
        (account_bytes, limits.account_input_bytes),
        (catalog_bytes, limits.catalog_input_bytes),
    ] {
        if u64::try_from(used)
            .unwrap_or(u64::MAX)
            .saturating_add(u64::try_from(input_len).unwrap_or(u64::MAX))
            > cap
        {
            return Err(StorageError::QuotaExceeded(
                "scoped run input allowance exhausted".into(),
            ));
        }
    }
    Ok(())
}

impl MobileStorage {
    pub(crate) async fn prepare_catalog_run(
        &self,
        scope: &CatalogScope,
        request: CatalogRunRequest<'_>,
        limits: CatalogRunLimits,
    ) -> Result<CatalogRun, StorageError> {
        let (input, digest) = request_identity(&request)?;
        let mut tx = self.pool().begin().await?;
        fence_catalog(&mut tx, scope).await?;
        if let Some(key) = request.dedup_key
            && let Some(row)=sqlx::query("SELECT * FROM mobile_catalog_run_requests WHERE account_id = ?1 AND catalog_id = ?2 AND dedup_key = ?3")
                .bind(&scope.account.id).bind(&scope.id).bind(key).fetch_optional(&mut *tx).await? {
                let previous:String=row.try_get("request_digest")?;
                let run=decode(&row)?;
                if previous!=digest {return Err(invalid("scoped operation key belongs to another request"));}
                if run.owner_epoch!=scope.account.epoch {return Err(invalid("scoped operation key belongs to a retired execution epoch"));}
                tx.commit().await?;
                return Ok(run);
        }
        let (admitted_version, admitted_digest) = require_reconciled(&mut tx, scope).await?;
        ensure_capacity(&mut tx, scope, input.len(), limits).await?;
        let selected:Option<(String,i32)>=sqlx::query_as("SELECT w.sequence_id, w.version FROM mobile_catalog_workflows w JOIN sequences s ON s.id = w.sequence_id WHERE w.account_id = ?1 AND w.catalog_id = ?2 AND w.name = ?3 AND (?4 IS NULL OR w.version = ?4) AND w.withdrawn_at_version IS NULL AND s.deprecated = 0 ORDER BY w.version DESC LIMIT 1")
            .bind(&scope.account.id).bind(&scope.id).bind(request.workflow).bind(request.version).fetch_optional(&mut *tx).await?;
        let (selected, version) =
            selected.ok_or_else(|| invalid("catalog workflow is not available"))?;
        let sequence = selected
            .parse()
            .map(SequenceId::from_uuid)
            .map_err(|_| invalid("invalid scoped sequence ID"))?;
        checked_definition(&mut tx, scope, sequence, request.workflow, version).await?;
        let id = InstanceId::new();
        sqlx::query("INSERT INTO mobile_catalog_run_requests(id, account_id, catalog_id, owner_epoch, sequence_id, selected_sequence_id, workflow_name, workflow_version, requested_version, input_bytes, input, request_digest, dedup_key, state, created_at, admitted_manifest_version, admitted_manifest_digest) VALUES (?1,?2,?3,?4,?5,?5,?6,?7,?8,?9,?10,?11,?12,'prepared',?13,?14,?15)")
            .bind(id.to_string()).bind(&scope.account.id).bind(&scope.id).bind(scope.account.epoch).bind(&selected).bind(request.workflow).bind(version).bind(request.version).bind(i64::try_from(input.len()).map_err(|_|invalid("input byte count overflow"))?).bind(&input).bind(digest).bind(request.dedup_key).bind(chrono::Utc::now().to_rfc3339()).bind(admitted_version).bind(admitted_digest).execute(&mut *tx).await?;
        let row = sqlx::query("SELECT * FROM mobile_catalog_run_requests WHERE id = ?1")
            .bind(id.to_string())
            .fetch_one(&mut *tx)
            .await?;
        let run = decode(&row)?;
        tx.commit().await?;
        Ok(run)
    }

    pub(crate) async fn catalog_run(
        &self,
        scope: &CatalogScope,
        id: InstanceId,
    ) -> Result<Option<CatalogRun>, StorageError> {
        let mut tx = self.pool().begin().await?;
        read_owner(&mut tx, scope).await?;
        let row=sqlx::query("SELECT * FROM mobile_catalog_run_requests WHERE id = ?1 AND account_id = ?2 AND catalog_id = ?3")
            .bind(id.to_string()).bind(&scope.account.id).bind(&scope.id).fetch_optional(&mut *tx).await?;
        let result = row.as_ref().map(decode).transpose()?;
        tx.commit().await?;
        Ok(result)
    }

    pub(crate) async fn cancel_catalog_run(
        &self,
        scope: &CatalogScope,
        id: InstanceId,
    ) -> Result<bool, StorageError> {
        let mut tx = self.pool().begin().await?;
        fence_catalog(&mut tx, scope).await?;
        let rows=sqlx::query("UPDATE mobile_catalog_run_requests SET state = 'cancelled' WHERE id = ?1 AND account_id = ?2 AND catalog_id = ?3 AND NOT EXISTS(SELECT 1 FROM task_instances WHERE id = ?1)")
            .bind(id.to_string()).bind(&scope.account.id).bind(&scope.id).execute(&mut *tx).await?.rows_affected();
        tx.commit().await?;
        Ok(rows == 1)
    }

    /// Explicitly forget a cancelled, never-dispatched request. Account close
    /// never calls this. Materialized/unknown effects need a separate policy.
    pub(crate) async fn forget_cancelled_catalog_run(
        &self,
        scope: &CatalogScope,
        id: InstanceId,
    ) -> Result<bool, StorageError> {
        let mut tx = self.pool().begin().await?;
        fence_catalog(&mut tx, scope).await?;
        let rows=sqlx::query("DELETE FROM mobile_catalog_run_requests WHERE id = ?1 AND account_id = ?2 AND catalog_id = ?3 AND state = 'cancelled' AND NOT EXISTS(SELECT 1 FROM task_instances WHERE id = ?1)")
            .bind(id.to_string()).bind(&scope.account.id).bind(&scope.id).execute(&mut *tx).await?.rows_affected();
        tx.commit().await?;
        Ok(rows == 1)
    }
}

#[cfg(test)]
mod tests;

#[cfg(test)]
mod callback_tests;

#[cfg(test)]
mod coordination_tests;
