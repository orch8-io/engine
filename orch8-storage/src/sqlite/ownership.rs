//! Recovery ownership is local to one persisted database, across processes.
//! The companion is stable and must never be unlinked on close.
use orch8_types::error::StorageError;
use std::{
    fs::{File, OpenOptions},
    path::PathBuf,
    sync::Arc,
};

pub(super) struct Owner {
    _file: File,
}
impl Owner {
    pub(super) fn acquire(path: &str) -> Result<(PathBuf, Arc<Self>), StorageError> {
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(path)
            .map_err(|_| {
                StorageError::Connection("cannot open mobile database ownership file".into())
            })?;
        #[cfg(unix)]
        {
            use std::os::unix::fs::MetadataExt;
            if file
                .metadata()
                .map_err(|_| {
                    StorageError::Connection("cannot inspect mobile database ownership file".into())
                })?
                .nlink()
                != 1
            {
                return Err(StorageError::Unsupported(
                    "mobile database hard links are unsupported".into(),
                ));
            }
        }
        let canonical = std::fs::canonicalize(path).map_err(|_| {
            StorageError::Connection("cannot resolve mobile database ownership file".into())
        })?;
        drop(file);
        let mut companion = canonical.clone().into_os_string();
        companion.push(".orch8-owner");
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(companion)
            .map_err(|_| {
                StorageError::Connection("cannot open mobile database ownership lock".into())
            })?;
        file.try_lock().map_err(|_| {
            StorageError::Conflict("mobile database is already owned or cannot be locked".into())
        })?;
        Ok((canonical, Arc::new(Self { _file: file })))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sqlite::SqliteStorage;
    use sqlx::Connection;

    #[tokio::test]
    async fn detached_native_connection_pins_ownership_until_physical_close() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("native.db");
        let path = path.to_str().unwrap();
        let storage = SqliteStorage::file_mobile_owned(path).await.unwrap();
        let alias = storage.pool().clone();
        let native = storage.pool().acquire().await.unwrap().detach();
        storage.close().await;
        assert!(alias.is_closed());
        assert!(SqliteStorage::file_mobile_owned(path).await.is_err());
        native.close().await.unwrap();
        let next = SqliteStorage::file_mobile_owned(path).await.unwrap();
        next.close().await;
        assert!(PathBuf::from(format!("{path}.orch8-owner")).exists());
    }

    #[tokio::test]
    async fn cancelled_close_cannot_release_a_checked_out_connections_lease() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("cancel.db");
        let path = path.to_str().unwrap();
        let storage = Arc::new(SqliteStorage::file_mobile_owned(path).await.unwrap());
        let alias = storage.pool().clone();
        let native = storage.pool().acquire().await.unwrap();
        let closing = storage.clone();
        let close = tokio::spawn(async move {
            closing.close().await;
        });
        alias.close_event().await;
        close.abort();
        assert!(close.await.unwrap_err().is_cancelled());
        let successor_observer = storage.clone();
        drop(storage);
        assert!(SqliteStorage::file_mobile_owned(path).await.is_err());
        native.close().await.unwrap();
        successor_observer.close().await;
        drop(successor_observer);
        let next = SqliteStorage::file_mobile_owned(path).await.unwrap();
        next.close().await;
    }

    #[tokio::test]
    async fn owned_mobile_paths_are_literal_files_and_unrelated_files_can_coexist() {
        let directory = tempfile::tempdir().unwrap();
        let mut stores = Vec::new();
        for name in [
            "plain.db",
            "with space.db",
            "query?mode=memory.db",
            "hash#literal.db",
        ] {
            let path = directory.path().join(name);
            let storage = SqliteStorage::file_mobile_owned(path.to_str().unwrap())
                .await
                .unwrap();
            sqlx::query("CREATE TABLE owner_path_proof(value INTEGER)")
                .execute(storage.pool())
                .await
                .unwrap();
            sqlx::query("INSERT INTO owner_path_proof VALUES(1)")
                .execute(storage.pool())
                .await
                .unwrap();
            stores.push((path, storage));
        }
        for (path, storage) in stores {
            storage.close().await;
            assert!(path.metadata().unwrap().len() > 0);
            let next = SqliteStorage::file_mobile_owned(path.to_str().unwrap())
                .await
                .unwrap();
            assert_eq!(
                sqlx::query_scalar::<_, i64>("SELECT value FROM owner_path_proof")
                    .fetch_one(next.pool())
                    .await
                    .unwrap(),
                1
            );
            next.close().await;
        }
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn symlink_aliases_share_the_lock_and_hard_links_are_refused() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("original.db");
        let storage = SqliteStorage::file_mobile_owned(path.to_str().unwrap())
            .await
            .unwrap();
        let symbolic = directory.path().join("symbolic.db");
        std::os::unix::fs::symlink(&path, &symbolic).unwrap();
        assert!(
            SqliteStorage::file_mobile_owned(symbolic.to_str().unwrap())
                .await
                .is_err()
        );
        let hard = directory.path().join("hard.db");
        std::fs::hard_link(&path, &hard).unwrap();
        assert!(matches!(
            SqliteStorage::file_mobile_owned(hard.to_str().unwrap()).await,
            Err(StorageError::Unsupported(_))
        ));
        std::fs::remove_file(hard).unwrap();
        storage.close().await;
        let next = SqliteStorage::file_mobile_owned(symbolic.to_str().unwrap())
            .await
            .unwrap();
        next.close().await;
    }

    #[tokio::test]
    async fn owned_memory_databases_are_independent_and_need_no_file_lock() {
        let first = SqliteStorage::file_mobile_owned(":memory:").await.unwrap();
        let second = SqliteStorage::file_mobile_owned(":memory:").await.unwrap();
        sqlx::query("CREATE TABLE memory_owner_proof(value INTEGER)")
            .execute(first.pool())
            .await
            .unwrap();
        let count: i64 = sqlx::query_scalar(
            "SELECT COUNT(*) FROM sqlite_master WHERE name='memory_owner_proof'",
        )
        .fetch_one(second.pool())
        .await
        .unwrap();
        assert_eq!(count, 0);
        first.close().await;
        assert_eq!(
            sqlx::query_scalar::<_, i64>("SELECT 1")
                .fetch_one(second.pool())
                .await
                .unwrap(),
            1
        );
        second.close().await;
    }

    #[test]
    fn ownership_probe_in_another_process() {
        let Ok(path) = std::env::var("ORCH8_TEST_MOBILE_OWNER_DB") else {
            return;
        };
        let expect_busy = std::env::var("ORCH8_TEST_MOBILE_OWNER_BUSY").unwrap() == "1";
        assert_eq!(Owner::acquire(&path).is_err(), expect_busy);
    }

    #[tokio::test]
    async fn ownership_is_exclusive_across_processes_and_reopens_after_close() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("process.db");
        let storage = SqliteStorage::file_mobile_owned(path.to_str().unwrap())
            .await
            .unwrap();
        for busy in [true, false] {
            if !busy {
                storage.close().await;
            }
            let status = std::process::Command::new(std::env::current_exe().unwrap())
                .args([
                    "--exact",
                    "sqlite::ownership::tests::ownership_probe_in_another_process",
                ])
                .env("ORCH8_TEST_MOBILE_OWNER_DB", &path)
                .env("ORCH8_TEST_MOBILE_OWNER_BUSY", if busy { "1" } else { "0" })
                .status()
                .unwrap();
            assert!(status.success());
        }
    }
}
