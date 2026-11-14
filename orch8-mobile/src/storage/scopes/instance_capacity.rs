//! Shared signed-catalog and remote-root instance accounting, including retained owners.
use super::{CatalogScope, StorageError, runs::materialize::InstanceLimits};
pub(super) async fn available(
    tx: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
    scope: &CatalogScope,
    limits: InstanceLimits,
) -> Result<bool, StorageError> {
    let (tenant, namespace) = super::workflows::owner(scope)?;
    // Ownership records keep the charge even if mutable native identity changes.
    let (global,account,catalog):(i64,i64,i64)=sqlx::query_as("SELECT COUNT(*),COALESCE(SUM(i.tenant_id=?1 OR EXISTS(SELECT 1 FROM mobile_catalog_run_requests r WHERE r.id=i.id AND r.account_id=?3) OR EXISTS(SELECT 1 FROM mobile_remote_run_bindings b WHERE b.local_run_id=i.id AND b.account_id=?3)),0),COALESCE(SUM((i.tenant_id=?1 AND i.namespace=?2) OR EXISTS(SELECT 1 FROM mobile_catalog_run_requests r WHERE r.id=i.id AND r.account_id=?3 AND r.catalog_id=?4) OR EXISTS(SELECT 1 FROM mobile_remote_run_bindings b WHERE b.local_run_id=i.id AND b.account_id=?3 AND b.catalog_id=?4)),0) FROM task_instances i WHERE i.state IN ('scheduled','running','waiting','paused')")
        .bind(tenant.as_str()).bind(namespace.as_str()).bind(scope.account_id()).bind(scope.catalog_id()).fetch_one(&mut **tx).await?;
    Ok([
        (global, limits.global),
        (account, limits.account),
        (catalog, limits.catalog),
    ]
    .iter()
    .all(|(used, max)| u64::try_from(*used).is_ok_and(|used| used < u64::from(*max))))
}
