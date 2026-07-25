use std::path::{Path, PathBuf};
use std::sync::Arc;

use orch8_storage::StorageBackend;
use orch8_storage::artifacts::ObjectArtifactStore;
use orch8_storage::postgres::PostgresStorage;
use orch8_storage::sqlite::SqliteStorage;

use crate::error::Error;

/// Storage backend selection for [`crate::EngineBuilder::storage`].
///
/// Construct with [`Storage::sqlite`], [`Storage::sqlite_in_memory`] or
/// [`Storage::postgres`]. The connection is opened — and the schema applied —
/// when [`crate::EngineBuilder::build`] runs.
#[derive(Clone)]
pub struct Storage {
    kind: StorageKind,
    artifacts: Option<ArtifactStorage>,
}

impl std::fmt::Debug for Storage {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Storage")
            .field("kind", &self.kind)
            .field("artifacts", &self.artifacts)
            .finish()
    }
}

#[derive(Clone)]
pub(crate) enum StorageKind {
    SqliteFile(PathBuf),
    SqliteInMemory,
    Postgres(String),
}

#[derive(Debug, Clone)]
enum ArtifactStorage {
    Local(PathBuf),
    Memory,
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
        Self {
            kind: StorageKind::SqliteFile(path.as_ref().to_path_buf()),
            artifacts: None,
        }
    }

    /// In-memory `SQLite`. All state is lost when the engine is dropped —
    /// intended for tests and ephemeral workloads.
    #[must_use]
    pub fn sqlite_in_memory() -> Self {
        Self {
            kind: StorageKind::SqliteInMemory,
            artifacts: None,
        }
    }

    /// `PostgreSQL` at `url` (e.g. `postgres://user:pass@host/db`). Migrations
    /// are applied on build, mirroring `orch8-server` startup.
    pub fn postgres(url: impl Into<String>) -> Self {
        Self {
            kind: StorageKind::Postgres(url.into()),
            artifacts: None,
        }
    }

    /// Attach a durable local artifact directory for capsules and large
    /// outputs. Recommended for embedded production runtimes.
    #[must_use]
    pub fn artifacts_local(mut self, path: impl AsRef<Path>) -> Self {
        self.artifacts = Some(ArtifactStorage::Local(path.as_ref().to_path_buf()));
        self
    }

    /// Attach an ephemeral in-memory artifact store. Tests/dev only: capsule
    /// payloads disappear when the process exits.
    #[must_use]
    pub fn artifacts_in_memory(mut self) -> Self {
        self.artifacts = Some(ArtifactStorage::Memory);
        self
    }

    /// Open the backend and make sure its schema is in place.
    pub(crate) async fn connect(self) -> Result<Arc<dyn StorageBackend>, Error> {
        let artifact_store = match self.artifacts {
            Some(ArtifactStorage::Local(path)) => {
                let path = path.to_str().ok_or_else(|| {
                    orch8_types::error::StorageError::Unsupported(
                        "artifact directory must be valid UTF-8".into(),
                    )
                })?;
                Some(Arc::new(ObjectArtifactStore::local(path)?))
            }
            Some(ArtifactStorage::Memory) => Some(Arc::new(ObjectArtifactStore::memory())),
            None => None,
        };
        match self.kind {
            StorageKind::SqliteFile(path) => {
                let path = path.to_string_lossy().into_owned();
                let mut storage = SqliteStorage::file(&path).await?;
                if let Some(store) = artifact_store {
                    storage = storage.with_artifact_store(store);
                }
                Ok(Arc::new(storage))
            }
            StorageKind::SqliteInMemory => {
                let mut storage = SqliteStorage::in_memory().await?;
                if let Some(store) = artifact_store {
                    storage = storage.with_artifact_store(store);
                }
                Ok(Arc::new(storage))
            }
            StorageKind::Postgres(url) => {
                // Same pool sizing default as the server's DatabaseConfig.
                let mut storage = PostgresStorage::new(&url, 64, None).await?;
                if let Some(store) = artifact_store {
                    storage = storage.with_artifact_store(store);
                }
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
