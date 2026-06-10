use std::path::{Path, PathBuf};
use std::sync::Arc;

use orch8_storage::postgres::PostgresStorage;
use orch8_storage::sqlite::SqliteStorage;
use orch8_storage::StorageBackend;

use crate::error::Error;

/// Storage backend selection for [`crate::EngineBuilder::storage`].
///
/// Construct with [`Storage::sqlite`], [`Storage::sqlite_in_memory`] or
/// [`Storage::postgres`]. The connection is opened — and the schema applied —
/// when [`crate::EngineBuilder::build`] runs.
#[derive(Debug, Clone)]
pub struct Storage(pub(crate) StorageKind);

#[derive(Debug, Clone)]
pub(crate) enum StorageKind {
    SqliteFile(PathBuf),
    SqliteInMemory,
    Postgres(String),
}

impl Storage {
    /// File-backed `SQLite` at `path` (WAL mode). The file and the bundled
    /// schema are created on first use — durable across process restarts.
    pub fn sqlite(path: impl AsRef<Path>) -> Self {
        Self(StorageKind::SqliteFile(path.as_ref().to_path_buf()))
    }

    /// In-memory `SQLite`. All state is lost when the engine is dropped —
    /// intended for tests and ephemeral workloads.
    #[must_use]
    pub fn sqlite_in_memory() -> Self {
        Self(StorageKind::SqliteInMemory)
    }

    /// `PostgreSQL` at `url` (e.g. `postgres://user:pass@host/db`). Migrations
    /// are applied on build, mirroring `orch8-server` startup.
    pub fn postgres(url: impl Into<String>) -> Self {
        Self(StorageKind::Postgres(url.into()))
    }

    /// Open the backend and make sure its schema is in place.
    pub(crate) async fn connect(self) -> Result<Arc<dyn StorageBackend>, Error> {
        match self.0 {
            StorageKind::SqliteFile(path) => {
                let path = path.to_string_lossy().into_owned();
                let storage = SqliteStorage::file(&path).await?;
                Ok(Arc::new(storage))
            }
            StorageKind::SqliteInMemory => {
                let storage = SqliteStorage::in_memory().await?;
                Ok(Arc::new(storage))
            }
            StorageKind::Postgres(url) => {
                // Same pool sizing default as the server's DatabaseConfig.
                let storage = PostgresStorage::new(&url, 64, None).await?;
                storage.run_migrations().await?;
                Ok(Arc::new(storage))
            }
        }
    }
}
