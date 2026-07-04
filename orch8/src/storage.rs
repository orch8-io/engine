use std::path::{Path, PathBuf};
use std::sync::Arc;

use orch8_storage::StorageBackend;
use orch8_storage::postgres::PostgresStorage;
use orch8_storage::sqlite::SqliteStorage;

use crate::error::Error;

/// Storage backend selection for [`crate::EngineBuilder::storage`].
///
/// Construct with [`Storage::sqlite`], [`Storage::sqlite_in_memory`] or
/// [`Storage::postgres`]. The connection is opened — and the schema applied —
/// when [`crate::EngineBuilder::build`] runs.
#[derive(Clone)]
pub struct Storage(pub(crate) StorageKind);

impl std::fmt::Debug for Storage {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match &self.0 {
            StorageKind::SqliteFile(path) => f.debug_tuple("SqliteFile").field(path).finish(),
            StorageKind::SqliteInMemory => f.write_str("SqliteInMemory"),
            StorageKind::Postgres(url) => {
                f.write_str("Postgres(")?;
                f.write_str(&redacted_connection_url(url))?;
                f.write_str(")")
            }
        }
    }
}

#[derive(Clone)]
pub(crate) enum StorageKind {
    SqliteFile(PathBuf),
    SqliteInMemory,
    Postgres(String),
}

impl std::fmt::Debug for StorageKind {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            StorageKind::SqliteFile(path) => f.debug_tuple("SqliteFile").field(path).finish(),
            StorageKind::SqliteInMemory => f.write_str("SqliteInMemory"),
            StorageKind::Postgres(url) => {
                f.write_str("Postgres(")?;
                f.write_str(&redacted_connection_url(url))?;
                f.write_str(")")
            }
        }
    }
}

/// Strip password from a Postgres connection URL for logging/Debug output.
fn redacted_connection_url(url: &str) -> String {
    match url::Url::parse(url) {
        Ok(mut parsed) => {
            if parsed.password().is_some() {
                let _ = parsed.set_password(None);
            }
            parsed.to_string()
        }
        Err(_) => "<invalid-url>".to_string(),
    }
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn postgres_debug_redacts_password() {
        let storage = Storage::postgres("postgres://user:secret@host/db");
        let debug = format!("{storage:?}");
        assert!(
            !debug.contains("secret"),
            "password must not appear in Debug: {debug}"
        );
        assert!(
            debug.contains("user@host"),
            "user/host should still appear: {debug}"
        );
    }

    #[test]
    fn sqlite_debug_does_not_leak_path() {
        // Sqlite path is not a secret, but we verify Debug is well-formed.
        let storage = Storage::sqlite("/tmp/orch8.db");
        let debug = format!("{storage:?}");
        assert!(
            debug.contains("/tmp/orch8.db"),
            "path should appear: {debug}"
        );
    }
}
