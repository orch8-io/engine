//! Durable scan bookmarks for the legacy delegation queues. Bookmarks are
//! scheduling positions, not ownership, authorization or exclusive work claims.
use super::owner;
use sqlx::{Row, SqlitePool, sqlite::SqliteRow};
use std::sync::LazyLock;

pub(super) struct Scan {
    name: &'static str,
    exists: String,
    pub(super) page: String,
}

// Pin the ordered partial indexes: the coarse state index otherwise wins
// on small databases and requires sorting the entire pending queue. init() and
// the storage schema install these indexes before any scan can run.
pub(super) static OPEN: LazyLock<Scan> = LazyLock::new(|| Scan {
    name: "open",
    exists: format!(
        "SELECT EXISTS(SELECT 1 FROM mobile_delegations AS d INDEXED BY idx_mobile_delegation_open_scan WHERE state IN ('prepared', 'claimed') AND {})",
        owner::record()
    ),
    page: format!(
        "SELECT d.*, delegation_id AS scan_id FROM mobile_delegations AS d INDEXED BY idx_mobile_delegation_open_scan WHERE state IN ('prepared', 'claimed') AND {} AND (created_at, delegation_id) > (?1, ?2) ORDER BY created_at, delegation_id LIMIT ?3",
        owner::record()
    ),
});
pub(super) static DISCOVERY: LazyLock<Scan> = LazyLock::new(|| Scan {
    name: "discovery",
    exists: format!(
        "SELECT EXISTS(SELECT 1 FROM worker_tasks INDEXED BY idx_mobile_delegation_discovery WHERE state = 'pending' AND awaiting_dispatch = 0 AND requirements <> '{{}}' AND {})",
        owner::native("worker_tasks.instance_id")
    ),
    page: format!(
        "SELECT id, requirements, created_at, id AS scan_id FROM worker_tasks INDEXED BY idx_mobile_delegation_discovery WHERE state = 'pending' AND awaiting_dispatch = 0 AND requirements <> '{{}}' AND {} AND (created_at, id) > (?1, ?2) ORDER BY created_at, id LIMIT ?3",
        owner::native("worker_tasks.instance_id")
    ),
});

pub(super) async fn init(pool: &SqlitePool) -> Result<(), sqlx::Error> {
    sqlx::query("CREATE TABLE IF NOT EXISTS mobile_delegation_scan_cursors (name TEXT PRIMARY KEY, created_at TEXT NOT NULL, row_id TEXT NOT NULL)")
        .execute(pool).await?;
    sqlx::query("CREATE INDEX IF NOT EXISTS idx_mobile_delegation_open_scan ON mobile_delegations(created_at, delegation_id) WHERE state IN ('prepared', 'claimed')")
        .execute(pool).await?;
    Ok(())
}

pub(super) async fn next_page(
    pool: &SqlitePool,
    scan: &Scan,
    limit: i64,
) -> Result<Vec<SqliteRow>, sqlx::Error> {
    // Idle scans do not dirty the database. A concurrent insert is picked up
    // by the next wake/pass; stale tail bookmarks wrap when work reappears.
    if !sqlx::query_scalar::<_, bool>(sqlx::AssertSqlSafe(scan.exists.as_str()))
        .fetch_one(pool)
        .await?
    {
        return Ok(Vec::new());
    }
    let mut tx = pool.begin().await?;
    // Reserve the writer before reading the bookmark across independent pools.
    sqlx::query("INSERT INTO mobile_delegation_scan_cursors(name, created_at, row_id) VALUES (?1, '', '') ON CONFLICT(name) DO UPDATE SET row_id = mobile_delegation_scan_cursors.row_id")
        .bind(scan.name).execute(&mut *tx).await?;
    let (mut created, mut id): (String, String) = sqlx::query_as(
        "SELECT created_at, row_id FROM mobile_delegation_scan_cursors WHERE name = ?1",
    )
    .bind(scan.name)
    .fetch_one(&mut *tx)
    .await?;
    let mut rows = sqlx::query(sqlx::AssertSqlSafe(scan.page.as_str()))
        .bind(&created)
        .bind(&id)
        .bind(limit)
        .fetch_all(&mut *tx)
        .await?;
    if rows.is_empty() && (!created.is_empty() || !id.is_empty()) {
        created.clear();
        id.clear();
        rows = sqlx::query(sqlx::AssertSqlSafe(scan.page.as_str()))
            .bind(&created)
            .bind(&id)
            .bind(limit)
            .fetch_all(&mut *tx)
            .await?;
    }
    if i64::try_from(rows.len()).unwrap_or(i64::MAX) < limit {
        created.clear();
        id.clear();
    } else if let Some(last) = rows.last() {
        created = last.try_get("created_at")?;
        id = last.try_get("scan_id")?;
    }
    sqlx::query(
        "UPDATE mobile_delegation_scan_cursors SET created_at = ?2, row_id = ?3 WHERE name = ?1",
    )
    .bind(scan.name)
    .bind(created)
    .bind(id)
    .execute(&mut *tx)
    .await?;
    // A crash after this commit defers the selected rows to another rotation;
    // their durable work/state is untouched. No network call holds this lock.
    tx.commit().await?;
    Ok(rows)
}
