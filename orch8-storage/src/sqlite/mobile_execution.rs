//! Additional execution fences for explicitly registered mobile account owners.
//! This is a necessary storage boundary, not scoped dispatch authorization.
//! No production API materializes scoped requests until handler ownership is ready.
//!
//! Legacy instances remain unaffected. A registered account tenant requires its
//! immutable request proof; changing tenant/catalog/definition cannot turn an
//! owned request into legacy work. PK lookups keep this out of history scans.
pub(super) const ALLOWED: &str = "CASE
    WHEN EXISTS (SELECT 1 FROM mobile_catalog_run_requests r WHERE r.id = task_instances.id)
    THEN EXISTS (
        SELECT 1 FROM mobile_catalog_run_requests r
        JOIN mobile_accounts a ON a.id = r.account_id
        WHERE r.id = task_instances.id AND a.active = 1 AND a.epoch = r.owner_epoch
          AND task_instances.tenant_id = 'mobile-account-' || a.id
          AND task_instances.namespace = 'catalog-' || r.catalog_id
          AND task_instances.sequence_id = r.selected_sequence_id
          AND r.state = 'prepared'
    )
    ELSE NOT EXISTS (
        SELECT 1 FROM mobile_accounts a
        WHERE a.id = substr(task_instances.tenant_id, 16)
          AND task_instances.tenant_id = 'mobile-account-' || a.id
    ) END";

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn ownership_predicate_uses_point_indexes() {
        let storage = super::super::SqliteStorage::in_memory().await.unwrap();
        let rows: Vec<(i64, i64, i64, String)> = sqlx::query_as(sqlx::AssertSqlSafe(format!(
            "EXPLAIN QUERY PLAN SELECT id FROM task_instances WHERE id = ?1 AND {ALLOWED}"
        )))
        .bind("candidate")
        .fetch_all(storage.pool())
        .await
        .unwrap();
        let plan = rows
            .iter()
            .map(|row| row.3.as_str())
            .collect::<Vec<_>>()
            .join("\n");
        assert!(
            !plan.contains("SCAN "),
            "owner checks must not scan historical accounts/requests: {plan}"
        );
        assert!(
            plan.contains("sqlite_autoindex_mobile_catalog_run_requests"),
            "request PK lookup missing: {plan}"
        );
        assert!(
            plan.contains("sqlite_autoindex_mobile_accounts"),
            "account PK lookup missing: {plan}"
        );
    }
}
