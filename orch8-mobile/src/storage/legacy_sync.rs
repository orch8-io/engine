//! Authority of the original, unscoped device sync protocol.
use super::{InstanceId, InstanceState, MobileStorage, StorageError};
use std::collections::HashSet;

/// Positive legacy authority, independent of account-name prefixes. Retained
/// receipts remain independent of native row garbage collection. Materialized
/// requests cannot be forgotten through the scoped cleanup API.
pub(super) const OWNER: &str = "i.tenant_id = 'mobile'
 AND NOT EXISTS (SELECT 1 FROM mobile_catalog_run_requests r WHERE r.id=i.id)
 AND NOT EXISTS (SELECT 1 FROM mobile_catalog_run_materializations m WHERE m.request_id=i.id)";

impl MobileStorage {
    pub(crate) async fn legacy_sync_instance_ids(
        &self,
        ids: &[String],
    ) -> Result<HashSet<String>, StorageError> {
        self.legacy_sync_ids(ids, false).await
    }

    /// Only rows verified by the guarded scanner may outlive their native task.
    /// Catalog ownership records remain an unconditional exclusion, even then.
    pub(crate) async fn retained_legacy_sync_instance_ids(
        &self,
        ids: &[String],
    ) -> Result<HashSet<String>, StorageError> {
        self.legacy_sync_ids(ids, true).await
    }

    async fn legacy_sync_ids(
        &self,
        ids: &[String],
        allow_missing: bool,
    ) -> Result<HashSet<String>, StorageError> {
        let mut allowed = HashSet::new();
        for chunk in ids.chunks(500) {
            let mut query = sqlx::QueryBuilder::new("WITH candidates(id) AS (");
            query.push_values(chunk, |mut row, id| {
                row.push_bind(id);
            });
            query.push(
                ") SELECT c.id FROM candidates c LEFT JOIN task_instances i ON i.id=c.id
                WHERE (i.tenant_id='mobile' OR (i.id IS NULL AND ",
            );
            query.push_bind(allow_missing);
            query.push(")) AND NOT EXISTS(SELECT 1 FROM mobile_catalog_run_requests r WHERE r.id=c.id)
                AND NOT EXISTS(SELECT 1 FROM mobile_catalog_run_materializations m WHERE m.request_id=c.id)");
            allowed.extend(
                query
                    .build_query_scalar::<String>()
                    .fetch_all(self.pool())
                    .await?,
            );
        }
        Ok(allowed)
    }

    /// Ownership and terminal-state exclusion are part of the write predicate;
    /// no preflight read can authorize a later row with a different owner.
    pub(crate) async fn transition_legacy_sync_instance(
        &self,
        id: InstanceId,
        target: InstanceState,
    ) -> Result<bool, StorageError> {
        let result = sqlx::query(sqlx::AssertSqlSafe(format!(
            "UPDATE task_instances AS i SET state=?2, next_fire_at=NULL, claimed_at=NULL, updated_at=?3
             WHERE i.id=?1 AND {OWNER} AND i.state IN ('scheduled','running','waiting','paused')"
        )))
        .bind(id.to_string())
        .bind(target.to_string())
        .bind(chrono::Utc::now().to_rfc3339())
        .execute(self.pool()).await?;
        Ok(result.rows_affected() == 1)
    }
}
