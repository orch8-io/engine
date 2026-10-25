//! Lifetime of a memory database must outlive replaceable pooled connections.
use orch8_types::error::StorageError;
use sqlx::{
    Connection as _, SqliteConnection, SqlitePool,
    sqlite::{SqliteConnectOptions, SqlitePoolOptions},
};
use std::sync::{Arc, Mutex};

/// Passive owner of a uniquely named shared-memory database. It holds no pool,
/// so retaining it from the pool's options cannot form an ownership cycle.
pub(super) struct MemoryDatabase {
    connection: Mutex<Option<SqliteConnection>>,
    closing: Mutex<Option<tokio::sync::watch::Receiver<bool>>>,
}

impl MemoryDatabase {
    pub(super) async fn close(&self) {
        let mut observer = {
            let mut closing = self
                .closing
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            if let Some(observer) = &*closing {
                observer.clone()
            } else {
                let connection = self
                    .connection
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner)
                    .take();
                let (completion, observer) = tokio::sync::watch::channel(false);
                // A pool close event and the storage close job can race. Both
                // join this physical close; cancelling either observer cannot
                // publish completion before the keeper worker has settled.
                tokio::spawn(async move {
                    if let Some(connection) = connection
                        && let Err(error) = connection.close().await
                    {
                        tracing::warn!(%error, "memory SQLite owner close failed");
                    }
                    completion.send_replace(true);
                });
                *closing = Some(observer.clone());
                observer
            }
        };
        let _ = observer.wait_for(|done| *done).await;
    }
}

/// Read only the memory/cache switches from the already SQLx-validated URI.
/// SQLx 0.9's to_url_lossy panics on its generated `file:` memory names; do
/// not serialize options or parse them again (which allocates a new name).
fn memory_settings(uri: &str) -> (bool, Option<bool>) {
    let input = uri
        .strip_prefix("sqlite://")
        .or_else(|| uri.strip_prefix("sqlite:"))
        .unwrap_or(uri);
    let (database, parameters) = input.split_once('?').unwrap_or((input, ""));
    let mut memory = database == ":memory:";
    let mut shared = memory.then_some(true);
    for (key, value) in url::form_urlencoded::parse(parameters.as_bytes()) {
        match (key.as_ref(), value.as_ref()) {
            ("mode", "memory") => {
                memory = true;
                shared = Some(true);
            }
            ("cache", "shared") => shared = Some(true),
            ("cache", "private") => shared = Some(false),
            _ => {}
        }
    }
    (memory, shared)
}

/// Parse once, then use the same memory name for keeper and pooled connections.
/// Disk constructors retain their normal pool configuration with no keeper.
pub(super) async fn connect(
    options: SqlitePoolOptions,
    mut connection_options: SqliteConnectOptions,
    uri: &str,
) -> Result<(SqlitePool, Option<Arc<MemoryDatabase>>), StorageError> {
    let (memory, shared) = memory_settings(uri);
    let filename = connection_options.get_filename();
    let literal_memory = filename == std::path::Path::new(":memory:");
    let uri_memory = filename == std::path::Path::new("file::memory:");
    let memory = memory || literal_memory || uri_memory;
    let unnamed = literal_memory || (memory && filename.as_os_str().is_empty());
    if !memory && filename.as_os_str().is_empty() {
        return Err(StorageError::Unsupported(
            "pooled SQLite requires a filename or explicit memory mode".into(),
        ));
    }
    let shared = shared.unwrap_or(unnamed);
    if unnamed {
        // Unadorned :memory: and empty filenames are private per connection,
        // even with OPEN_SHAREDCACHE. Give one anonymous engine a fresh name.
        connection_options = connection_options
            .filename(format!("file:orch8-memory-{}", uuid::Uuid::now_v7()))
            .in_memory(true)
            .shared_cache(true);
    }
    let owner = if memory {
        if !shared {
            return Err(StorageError::Unsupported(
                "pooled memory SQLite requires a shared cache".into(),
            ));
        }
        Some(Arc::new(MemoryDatabase {
            connection: Mutex::new(Some(
                SqliteConnection::connect_with(&connection_options)
                    .await
                    .map_err(|error| StorageError::Connection(error.to_string()))?,
            )),
            closing: Mutex::new(None),
        }))
    } else {
        None
    };
    let options = if let Some(owner) = &owner {
        let owner = Arc::clone(owner);
        options.after_connect(move |_connection, _metadata| {
            let owner = Arc::clone(&owner);
            Box::pin(async move {
                // The closure itself preserves database lifetime for retained
                // pool aliases even after the storage wrapper is dropped.
                drop(owner);
                Ok(())
            })
        })
    } else {
        options
    };
    let pool = options
        .connect_with(connection_options)
        .await
        .map_err(|error| StorageError::Connection(error.to_string()))?;
    if let Some(owner) = &owner {
        let owner = Arc::downgrade(owner);
        let closed = pool.close_event();
        // CloseEvent owns only its event listener, not a Pool clone. A raw
        // alias's close also retires the keeper; dropping the last alias cannot
        // be prevented by this watcher. Storage::close joins the same receipt.
        tokio::spawn(async move {
            closed.await;
            if let Some(owner) = owner.upgrade() {
                owner.close().await;
            }
        });
    }
    Ok((pool, owner))
}

#[cfg(test)]
mod tests {
    use super::super::SqliteStorage;
    use crate::{
        InstanceStore, SequenceStore,
        testing::fixtures::{InstanceBuilder, SequenceBuilder},
    };

    async fn seed(storage: &SqliteStorage) -> orch8_types::ids::InstanceId {
        let sequence = SequenceBuilder::new("memory-owner")
            .step("work", "noop")
            .build();
        storage.create_sequence(&sequence).await.unwrap();
        let row = InstanceBuilder::for_sequence(&sequence)
            .data(serde_json::json!({"private":"MEMORY_CANARY"}))
            .build();
        storage.create_instance(&row).await.unwrap();
        row.id
    }

    async fn retire_all_connections(pool: &sqlx::SqlitePool) {
        let mut connections = Vec::new();
        for _ in 0..pool.options().get_max_connections() {
            connections.push(pool.acquire().await.unwrap());
        }
        for connection in connections {
            connection.close().await.unwrap();
        }
    }

    async fn survives_replacement(storage: SqliteStorage) {
        let id = seed(&storage).await;
        for _ in 0..3 {
            retire_all_connections(storage.pool()).await;
            let row = storage.get_instance(id).await.unwrap().unwrap();
            assert_eq!(row.context.data["private"], "MEMORY_CANARY");
        }
        storage.close().await;
    }

    #[tokio::test]
    async fn in_memory_retains_rows_after_every_pooled_connection_is_replaced() {
        survives_replacement(SqliteStorage::in_memory().await.unwrap()).await;
    }

    #[tokio::test]
    async fn file_memory_retains_rows_after_every_pooled_connection_is_replaced() {
        survives_replacement(SqliteStorage::file(":memory:").await.unwrap()).await;
    }

    #[tokio::test]
    async fn mobile_memory_retains_rows_after_every_pooled_connection_is_replaced() {
        survives_replacement(SqliteStorage::file_mobile(":memory:").await.unwrap()).await;
    }

    #[tokio::test]
    async fn owned_mobile_memory_retains_rows_after_every_pooled_connection_is_replaced() {
        survives_replacement(SqliteStorage::file_mobile_owned(":memory:").await.unwrap()).await;
    }

    #[tokio::test]
    async fn a_retained_pool_alias_keeps_memory_rows_after_the_storage_wrapper_drops() {
        let storage = SqliteStorage::in_memory().await.unwrap();
        let id = seed(&storage).await;
        let pool = storage.pool().clone();
        drop(storage);
        retire_all_connections(&pool).await;
        let stored: Option<String> =
            sqlx::query_scalar("SELECT id FROM task_instances WHERE id=?1")
                .bind(id.to_string())
                .fetch_optional(&pool)
                .await
                .unwrap();
        assert_eq!(stored, Some(id.to_string()));
        pool.close().await;
    }

    #[tokio::test]
    async fn anonymous_memory_uri_forms_keep_one_database_across_replacement() {
        for path in ["//:memory:", "%3Amemory%3A", "?mode=memory"] {
            survives_replacement(SqliteStorage::file(path).await.unwrap()).await;
            survives_replacement(SqliteStorage::file_mobile(path).await.unwrap()).await;
        }
    }

    #[tokio::test]
    async fn named_and_percent_encoded_memory_modes_keep_their_database() {
        for query in [
            "mode=memory&cache=shared",
            "%6dode=memory&cache=shared",
            "cache=private&mode=memory",
        ] {
            let path = format!("file:orch8-memory-test-{}?{query}", uuid::Uuid::now_v7());
            survives_replacement(SqliteStorage::file(&path).await.unwrap()).await;
        }
    }

    #[tokio::test]
    async fn sqlite_special_memory_uri_is_shared_only_when_explicitly_configured() {
        survives_replacement(
            SqliteStorage::file("file::memory:?cache=shared")
                .await
                .unwrap(),
        )
        .await;
        assert!(matches!(
            SqliteStorage::file("file::memory:").await,
            Err(orch8_types::error::StorageError::Unsupported(_))
        ));
    }

    #[tokio::test]
    async fn explicit_private_cache_and_unnamed_temporary_files_are_refused() {
        let path = format!(
            "file:private-{}?mode=memory&cache=private",
            uuid::Uuid::now_v7()
        );
        for path in [path.as_str(), ":memory:?cache=private", ""] {
            assert!(matches!(
                SqliteStorage::file(path).await,
                Err(orch8_types::error::StorageError::Unsupported(_))
            ));
            assert!(matches!(
                SqliteStorage::file_mobile(path).await,
                Err(orch8_types::error::StorageError::Unsupported(_))
            ));
        }
    }

    #[tokio::test]
    async fn independent_anonymous_databases_remain_isolated_after_recycling_and_close() {
        let first = SqliteStorage::in_memory().await.unwrap();
        let second = SqliteStorage::in_memory().await.unwrap();
        let a = seed(&first).await;
        let b = seed(&second).await;
        assert!(first.get_instance(b).await.unwrap().is_none());
        assert!(second.get_instance(a).await.unwrap().is_none());
        retire_all_connections(first.pool()).await;
        assert!(first.get_instance(a).await.unwrap().is_some());
        first.close().await;
        retire_all_connections(second.pool()).await;
        assert!(second.get_instance(b).await.unwrap().is_some());
        assert!(second.get_instance(a).await.unwrap().is_none());
        second.close().await;
    }

    #[tokio::test]
    async fn explicit_named_memory_aliases_preserve_sqlite_sharing_semantics() {
        let path = format!(
            "file:shared-{}?mode=memory&cache=shared",
            uuid::Uuid::now_v7()
        );
        let first = SqliteStorage::file(&path).await.unwrap();
        let second = SqliteStorage::file(&path).await.unwrap();
        let id = seed(&first).await;
        assert!(second.get_instance(id).await.unwrap().is_some());
        first.close().await;
        retire_all_connections(second.pool()).await;
        assert!(second.get_instance(id).await.unwrap().is_some());
        second.close().await;
    }

    #[tokio::test]
    async fn cancelled_storage_close_still_drains_and_releases_memory_with_closed_aliases() {
        use sqlx::Connection as _;
        use std::{sync::Arc, time::Duration};
        let storage = Arc::new(SqliteStorage::in_memory().await.unwrap());
        seed(&storage).await;
        let alias = storage.pool().clone();
        let options = alias.connect_options();
        let owner = storage.memory_database.as_ref().unwrap().clone();
        let held = alias.acquire().await.unwrap();
        let observer = tokio::spawn({
            let storage = storage.clone();
            async move { storage.close().await }
        });
        tokio::time::timeout(Duration::from_secs(3), async {
            while !alias.is_closed() {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        observer.abort();
        let _ = observer.await;
        held.close().await.unwrap();
        tokio::time::timeout(Duration::from_secs(3), storage.close())
            .await
            .unwrap();
        assert!(owner.connection.lock().unwrap().is_none());
        assert!(alias.acquire().await.is_err());
        // No pooled connection or passive owner may retain the old database.
        let mut probe = sqlx::SqliteConnection::connect_with(&options)
            .await
            .unwrap();
        let tables: i64 =
            sqlx::query_scalar("SELECT COUNT(*) FROM sqlite_master WHERE name='task_instances'")
                .fetch_one(&mut probe)
                .await
                .unwrap();
        assert_eq!(tables, 0);
        probe.close().await.unwrap();
    }

    #[tokio::test]
    async fn a_raw_pool_close_releases_its_keeper_without_retaining_the_pool() {
        use std::time::Duration;
        let storage = SqliteStorage::in_memory().await.unwrap();
        seed(&storage).await;
        let owner = storage.memory_database.as_ref().unwrap().clone();
        let pool = storage.pool().clone();
        drop(storage);
        pool.close().await;
        tokio::time::timeout(Duration::from_secs(3), async {
            loop {
                let started = owner.closing.lock().unwrap().is_some();
                if started {
                    break;
                }
                tokio::task::yield_now().await;
            }
            owner.close().await;
        })
        .await
        .unwrap();
        assert!(owner.connection.lock().unwrap().is_none());
        let weak = std::sync::Arc::downgrade(&owner);
        drop(owner);
        drop(pool);
        tokio::time::timeout(Duration::from_secs(3), async {
            while weak.upgrade().is_some() {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
    }
}
