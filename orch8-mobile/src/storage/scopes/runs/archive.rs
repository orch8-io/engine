//! Explicit acknowledgement releases hot admission without forgetting execution.
//! The caller is the trusted app/backend integration, not workflow input.
use super::materialize::fingerprint_run;
use super::{
    CatalogRun, CatalogRunLimits, CatalogRunState, CatalogScope, InstanceId, MobileStorage,
    StorageError, decode, fence_catalog, invalid, validate,
};
use orch8_storage::sqlite::CATALOG_NATIVE_IDENTITY_SQL;
use orch8_types::instance::InstanceState;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use sqlx::{Row, sqlite::SqliteRow};

pub(crate) const MAX_RECEIPT_BYTES: usize = 4096;

/// Separate cold ceilings. Exhaustion refuses new archives without dropping old
/// operation keys. These trusted ceilings are shared by every database caller.
pub(crate) fn limits() -> CatalogRunLimits {
    CatalogRunLimits {
        global_records: 65_536,
        account_records: 32_768,
        catalog_records: 16_384,
        global_input_bytes: 64 * 1024 * 1024,
        account_input_bytes: 32 * 1024 * 1024,
        catalog_input_bytes: 16 * 1024 * 1024,
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct ArchiveReceipt {
    format_version: u32,
    pub(crate) instance_id: String,
    pub(crate) account_id: String,
    pub(crate) catalog_id: String,
    pub(crate) acknowledgement_digest: String,
    pub(crate) archived_at: String,
    /// None means purged before acknowledgement; never infer an outcome.
    pub(crate) outcome: Option<InstanceState>,
    compact_request_fingerprint: String,
    original_request_fingerprint: String,
    definition_fingerprint: String,
}

pub(super) fn decode_receipt(
    row: &SqliteRow,
    run: &CatalogRun,
) -> Result<ArchiveReceipt, StorageError> {
    let body: String = row.try_get("receipt_json")?;
    let digest: String = row.try_get("receipt_digest")?;
    let bytes: i64 = row.try_get("receipt_bytes")?;
    if body.len() > MAX_RECEIPT_BYTES
        || usize::try_from(bytes).ok() != Some(body.len())
        || digest != hex::encode(Sha256::digest(body.as_bytes()))
    {
        return Err(invalid("invalid catalog archive receipt"));
    }
    let receipt: ArchiveReceipt = serde_json::from_str(&body)?;
    let original: String = row.try_get("request_fingerprint")?;
    let definition: String = row.try_get("definition_fingerprint")?;
    if receipt.format_version != 1
        || receipt.instance_id != run.id.to_string()
        || receipt.account_id != run.account_id
        || receipt.catalog_id != run.catalog_id
        || receipt.compact_request_fingerprint != fingerprint_run(run)?
        || receipt.original_request_fingerprint != original
        || receipt.definition_fingerprint != definition
        || receipt.outcome.is_some_and(|state| !state.is_terminal())
        || receipt.acknowledgement_digest.len() != 64
        || chrono::DateTime::parse_from_rfc3339(&receipt.archived_at).is_err()
    {
        return Err(invalid("catalog archive identity is inconsistent"));
    }
    Ok(receipt)
}

pub(super) async fn read_receipt(
    tx: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
    run: &CatalogRun,
) -> Result<ArchiveReceipt, StorageError> {
    let row = sqlx::query("SELECT a.receipt_json,a.receipt_digest,a.receipt_bytes,m.request_fingerprint,m.definition_fingerprint FROM mobile_catalog_run_archives a JOIN mobile_catalog_run_materializations m ON m.request_id=a.request_id WHERE a.request_id=?1 AND a.account_id=?2 AND a.catalog_id=?3")
        .bind(run.id.to_string()).bind(&run.account_id).bind(&run.catalog_id).fetch_optional(&mut **tx).await?
        .ok_or_else(|| invalid("catalog archive receipt is missing"))?;
    decode_receipt(&row, run)
}

async fn ensure_capacity(
    tx: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
    scope: &CatalogScope,
    bytes: usize,
    limits: CatalogRunLimits,
) -> Result<(), StorageError> {
    let (global,account,catalog,global_bytes,account_bytes,catalog_bytes):(i64,i64,i64,i64,i64,i64) = sqlx::query_as("SELECT COUNT(*),COALESCE(SUM(account_id=?1),0),COALESCE(SUM(account_id=?1 AND catalog_id=?2),0),COALESCE(SUM(receipt_bytes),0),COALESCE(SUM(CASE WHEN account_id=?1 THEN receipt_bytes ELSE 0 END),0),COALESCE(SUM(CASE WHEN account_id=?1 AND catalog_id=?2 THEN receipt_bytes ELSE 0 END),0) FROM mobile_catalog_run_archives INDEXED BY idx_mobile_catalog_archive_usage")
        .bind(scope.account_id()).bind(scope.catalog_id()).fetch_one(&mut **tx).await?;
    if [
        (global, limits.global_records),
        (account, limits.account_records),
        (catalog, limits.catalog_records),
    ]
    .iter()
    .any(|(used, max)| u64::try_from(*used).unwrap_or(u64::MAX) >= u64::from(*max))
        || [
            (global_bytes, limits.global_input_bytes),
            (account_bytes, limits.account_input_bytes),
            (catalog_bytes, limits.catalog_input_bytes),
        ]
        .iter()
        .any(|(used, max)| {
            u64::try_from(*used)
                .unwrap_or(u64::MAX)
                .saturating_add(bytes as u64)
                > *max
        })
    {
        return Err(StorageError::QuotaExceeded(
            "catalog archive receipt allowance exhausted".into(),
        ));
    }
    Ok(())
}

impl MobileStorage {
    /// Atomic, idempotent acknowledgement. Never deletes native context/effect
    /// evidence: normal native GC owns that separate lifecycle. The returned
    /// receipt stores only a digest of the acknowledgement reference.
    pub(crate) async fn archive_catalog_run(
        &self,
        scope: &CatalogScope,
        id: InstanceId,
        acknowledgement: &str,
        limits: CatalogRunLimits,
    ) -> Result<Option<ArchiveReceipt>, StorageError> {
        validate(acknowledgement)?;
        let ack = hex::encode(Sha256::digest(acknowledgement.as_bytes()));
        let mut tx = self.pool().begin().await?;
        fence_catalog(&mut tx, scope).await?;
        let row = sqlx::query("SELECT * FROM mobile_catalog_run_requests WHERE id=?1 AND account_id=?2 AND catalog_id=?3 AND owner_epoch<=?4")
            .bind(id.to_string()).bind(scope.account_id()).bind(scope.catalog_id()).bind(scope.epoch()).fetch_optional(&mut *tx).await?;
        let Some(row) = row else { return Ok(None) };
        let mut run = decode(&row)?;
        if run.state == CatalogRunState::Archived {
            let receipt = read_receipt(&mut tx, &run).await?;
            if receipt.acknowledgement_digest != ack {
                return Err(invalid(
                    "catalog run already archived under another acknowledgement",
                ));
            }
            tx.commit().await?;
            return Ok(Some(receipt));
        }
        let materialization: Option<(String,String)> = sqlx::query_as("SELECT request_fingerprint,definition_fingerprint FROM mobile_catalog_run_materializations WHERE request_id=?1")
            .bind(id.to_string()).fetch_optional(&mut *tx).await?;
        let Some((original, definition)) = materialization else {
            return Ok(None);
        };
        if original != fingerprint_run(&run)? {
            return Err(invalid("catalog materialization identity is inconsistent"));
        }
        // Reads metadata only, never native context. A colliding/replaced row is
        // an integrity error rather than permission to archive someone else's run.
        let native = sqlx::query(sqlx::AssertSqlSafe(format!("SELECT i.state,({CATALOG_NATIVE_IDENTITY_SQL}) AS owned FROM task_instances i JOIN mobile_catalog_run_requests r ON r.id=i.id WHERE r.id=?1")))
            .bind(id.to_string()).fetch_optional(&mut *tx).await?;
        let outcome = if let Some(native) = native {
            if !native.try_get::<bool, _>("owned")? {
                return Err(invalid("catalog native identity is inconsistent"));
            }
            let state: InstanceState = native
                .try_get::<String, _>("state")?
                .parse()
                .map_err(StorageError::Query)?;
            if !state.is_terminal() {
                return Ok(None);
            }
            Some(state)
        } else {
            None
        };
        run.input = serde_json::Value::Null;
        let receipt = ArchiveReceipt {
            format_version: 1,
            instance_id: id.to_string(),
            account_id: run.account_id.clone(),
            catalog_id: run.catalog_id.clone(),
            acknowledgement_digest: ack,
            archived_at: chrono::Utc::now().to_rfc3339(),
            outcome,
            compact_request_fingerprint: fingerprint_run(&run)?,
            original_request_fingerprint: original,
            definition_fingerprint: definition,
        };
        let body = serde_json::to_string(&receipt)?;
        if body.len() > MAX_RECEIPT_BYTES {
            return Err(invalid("catalog archive receipt is too large"));
        }
        ensure_capacity(&mut tx, scope, body.len(), limits).await?;
        let receipt_bytes = i64::try_from(body.len())
            .map_err(|_| invalid("archive receipt byte count overflow"))?;
        let digest = hex::encode(Sha256::digest(body.as_bytes()));
        sqlx::query("INSERT INTO mobile_catalog_run_archives(request_id,account_id,catalog_id,receipt_json,receipt_bytes,receipt_digest) VALUES (?1,?2,?3,?4,?5,?6)")
            .bind(id.to_string()).bind(scope.account_id()).bind(scope.catalog_id()).bind(&body).bind(receipt_bytes).bind(digest).execute(&mut *tx).await?;
        // 'cancelled' is the existing internal dispatch-disabled storage tag;
        // public history distinguishes Archived, with its original outcome.
        sqlx::query("UPDATE mobile_catalog_run_requests SET state='cancelled',sequence_id=NULL,input='null',input_bytes=4,archive_receipt_id=id WHERE id=?1")
            .bind(id.to_string()).execute(&mut *tx).await?;
        tx.commit().await?;
        Ok(Some(receipt))
    }
}

#[cfg(test)]
mod tests;
