//! Legacy delegation authority is proved at admission and retained through GC.
use sqlx::{Row, SqlitePool};

/// Trusted SQL column expressions only; no caller data is interpolated.
pub(super) fn native(column: &'static str) -> String {
    format!(
        "EXISTS(SELECT 1 FROM task_instances i WHERE i.id={column} AND ({}))",
        orch8_storage::sqlite::LEGACY_MOBILE_OWNER_SQL
    )
}

/// The alias `d` is a delegation row. Catalog receipts always override an old
/// legacy marker, including after native history has been collected.
pub(super) fn record() -> String {
    format!("(d.legacy_verified=1 OR {})
        AND NOT EXISTS(SELECT 1 FROM mobile_catalog_run_requests r WHERE r.id=d.local_instance_id)
        AND NOT EXISTS(SELECT 1 FROM mobile_catalog_run_materializations m WHERE m.request_id=d.local_instance_id)
        AND NOT EXISTS(SELECT 1 FROM task_instances i WHERE i.id=d.local_instance_id AND i.tenant_id<>'mobile')", native("d.local_instance_id"))
}

pub(super) async fn init(pool: &SqlitePool) -> Result<(), sqlx::Error> {
    let mut tx = pool.begin_with("BEGIN IMMEDIATE").await?;
    let columns = sqlx::query("PRAGMA table_info(mobile_delegations)")
        .fetch_all(&mut *tx)
        .await?;
    let present = columns
        .iter()
        .any(|column| column.get::<String, _>("name") == "legacy_verified");
    if !present {
        sqlx::query("ALTER TABLE mobile_delegations ADD COLUMN legacy_verified INTEGER NOT NULL DEFAULT 0 CHECK(legacy_verified IN (0,1))")
            .execute(&mut *tx).await?;
        // A standalone delegation-table test need not have the native schema.
        let has_native: bool = sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM sqlite_master WHERE type='table' AND name='task_instances')")
            .fetch_one(&mut *tx).await?;
        if has_native {
            sqlx::query(sqlx::AssertSqlSafe(format!(
                "UPDATE mobile_delegations AS d SET legacy_verified=1 WHERE {}",
                native("d.local_instance_id")
            )))
            .execute(&mut *tx)
            .await?;
        }
    }
    tx.commit().await
}
