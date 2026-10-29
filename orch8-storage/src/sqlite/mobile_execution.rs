//! Additional execution fences for explicitly registered mobile account owners.
//! This is a necessary storage boundary, not scoped dispatch authorization.
//! No production API materializes scoped requests until handler ownership is ready.
//!
//! Non-retired legacy instances remain unaffected. A registered account tenant requires its
//! immutable request proof; changing tenant/catalog/definition cannot turn an
//! owned request into legacy work. PK lookups keep this out of history scans.
pub(super) const ALLOWED: &str = "(CASE
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
    ) END) AND NOT EXISTS (SELECT 1 FROM physical_instance_retirements retired WHERE retired.instance_id=task_instances.id) AND NOT EXISTS (SELECT 1 FROM capsule_source_cuts cut WHERE cut.source_instance_id=task_instances.id AND cut.state IN ('captured','sealed'))";

/// Both native rows must prove the same durable catalog owner, or both be
/// legacy. Source/target aliases are fixed trusted SQL, never caller input.
pub(super) fn coordination_predicate() -> String {
    format!(
        "caller.tenant_id = ?2 AND target.tenant_id = caller.tenant_id
         AND (caller.sub_tenant IS NULL OR caller.sub_tenant = target.sub_tenant)
         AND ({caller_allowed}) AND ({target_allowed})
         AND CASE WHEN EXISTS (SELECT 1 FROM mobile_catalog_run_requests r WHERE r.id = caller.id)
         THEN EXISTS (SELECT 1 FROM mobile_catalog_run_requests c
                      JOIN mobile_catalog_run_requests t
                        ON t.account_id = c.account_id AND t.catalog_id = c.catalog_id
                       AND t.owner_epoch = c.owner_epoch
                      WHERE c.id = caller.id AND t.id = target.id)
         ELSE NOT EXISTS (SELECT 1 FROM mobile_catalog_run_requests r WHERE r.id = target.id) END",
        caller_allowed = ALLOWED.replace("task_instances.", "caller."),
        target_allowed = ALLOWED.replace("task_instances.", "target."),
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn coordination_uses_point_indexes_for_both_owner_proofs() {
        let storage = super::super::SqliteStorage::in_memory().await.unwrap();
        let rows: Vec<(i64, i64, i64, String)> = sqlx::query_as(sqlx::AssertSqlSafe(format!(
            "EXPLAIN QUERY PLAN SELECT target.id FROM task_instances target
             JOIN task_instances caller ON caller.id = ?1 WHERE target.id = ?3 AND {}",
            coordination_predicate()
        )))
        .bind("caller")
        .bind("tenant")
        .bind("target")
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
            "coordination must not scan owner history: {plan}"
        );
        assert!(
            plan.contains("sqlite_autoindex_mobile_catalog_run_requests"),
            "{plan}"
        );
    }

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
