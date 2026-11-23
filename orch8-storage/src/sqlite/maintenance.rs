use std::path::Path;
use std::time::Duration;

use orch8_types::error::StorageError;
use serde::Serialize;
use sqlx::sqlite::SqliteConnectOptions;
use sqlx::{Connection as _, SqliteConnection};
use tokio::time::{Instant, timeout_at};

/// Observed SQLite maintenance progress. WAL frame counts describe the last
/// completed checkpoint and are absent when the database has no WAL.
#[derive(Debug, Default, Serialize)]
pub struct MaintenanceReport {
    pub wal_frames: Option<u64>,
    pub checkpointed_frames: Option<u64>,
    pub reclaimed_pages: u32,
    pub deadline_reached: bool,
    pub busy: bool,
}

/// Reclaim freelist pages and attempt a passive WAL checkpoint on an existing
/// file. Run each page as a separate transaction so the wall-clock budget can
/// stop scheduling new work. An in-flight SQLite syscall can finish after the
/// deadline, so the result counts only work observed to finish.
pub async fn maintain_file(
    path: &Path,
    max_elapsed: Duration,
    max_pages: u32,
) -> Result<MaintenanceReport, StorageError> {
    let deadline = Instant::now()
        .checked_add(max_elapsed)
        .ok_or_else(|| StorageError::Query("invalid SQLite maintenance duration".into()))?;
    let mut report = MaintenanceReport::default();
    let options = SqliteConnectOptions::new()
        .filename(path)
        .busy_timeout(Duration::ZERO)
        .pragma("wal_autocheckpoint", "0");
    let Some(mut connection) = observe(
        deadline,
        &mut report,
        SqliteConnection::connect_with(&options),
    )
    .await?
    else {
        return Ok(report);
    };

    // This pragma emits one row per reclaimed freelist page. Drain the query
    // to completion before counting it so an uncommitted page is never counted.
    for _ in 0..max_pages {
        let rows = observe(
            deadline,
            &mut report,
            sqlx::query("PRAGMA main.incremental_vacuum(1)").fetch_all(&mut connection),
        )
        .await?;
        if rows.is_none_or(|rows| rows.is_empty()) {
            break;
        }
        report.reclaimed_pages += 1;
    }

    if let Some((busy, wal, checkpointed)) = observe(
        deadline,
        &mut report,
        sqlx::query_as::<_, (i64, i64, i64)>("PRAGMA main.wal_checkpoint(PASSIVE)")
            .fetch_one(&mut connection),
    )
    .await?
    {
        report.wal_frames = u64::try_from(wal).ok();
        report.checkpointed_frames = u64::try_from(checkpointed).ok();
        report.busy |= busy != 0 || wal > checkpointed;
    }
    Ok(report)
}

async fn observe<T>(
    deadline: Instant,
    report: &mut MaintenanceReport,
    future: impl Future<Output = Result<T, sqlx::Error>>,
) -> Result<Option<T>, StorageError> {
    if Instant::now() >= deadline {
        report.deadline_reached = true;
        return Ok(None);
    }
    match timeout_at(deadline, future).await {
        Err(_) => report.deadline_reached = true,
        Ok(Ok(value)) => return Ok(Some(value)),
        Ok(Err(sqlx::Error::Database(error)))
            if error
                .code()
                .and_then(|code| code.parse::<u32>().ok())
                .is_some_and(|code| matches!(code & 0xff, 5 | 6)) =>
        {
            report.busy = true;
        }
        Ok(Err(error)) => return Err(error.into()),
    }
    Ok(None)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write as _;

    #[tokio::test]
    async fn expired_budget_leaves_the_file_untouched() {
        let mut file = tempfile::NamedTempFile::new().unwrap();
        file.write_all(b"unopened").unwrap();
        let report = maintain_file(file.path(), Duration::ZERO, u32::MAX)
            .await
            .unwrap();
        assert!(report.deadline_reached);
        assert_eq!(report.reclaimed_pages, 0);
        assert_eq!(std::fs::read(file.path()).unwrap(), b"unopened");
    }
}
