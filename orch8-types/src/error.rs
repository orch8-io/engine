/// Storage-level errors. Every `StorageBackend` method returns these.
#[derive(Debug, thiserror::Error)]
pub enum StorageError {
    #[error("connection failed: {0}")]
    Connection(String),

    #[error("query failed: {0}")]
    Query(String),

    #[error("not found: {entity} with id {id}")]
    NotFound { entity: &'static str, id: String },

    #[error("conflict: {0}")]
    Conflict(String),

    /// The operation targets an entity that is in a terminal state and cannot
    /// accept further writes (e.g. signal enqueue to a Completed / Failed /
    /// Cancelled instance). Distinct from [`Self::Conflict`] — which covers
    /// idempotency-key duplicates, unique-constraint violations, etc. — so
    /// handlers can map terminal-target to a dedicated `Permanent` without
    /// overloading the generic conflict path.
    #[error("terminal target: {entity} {id} is in a terminal state")]
    TerminalTarget { entity: String, id: String },

    #[error("migration failed: {0}")]
    Migration(String),

    #[error("serialization error: {0}")]
    Serialization(#[from] serde_json::Error),

    #[error("pool exhausted")]
    PoolExhausted,
}

impl From<sqlx::Error> for StorageError {
    fn from(err: sqlx::Error) -> Self {
        match err {
            sqlx::Error::PoolTimedOut => Self::PoolExhausted,
            sqlx::Error::RowNotFound => Self::NotFound {
                entity: "row",
                id: String::new(),
            },
            sqlx::Error::Database(ref db_err) => {
                // Unique-constraint violations should surface as `Conflict`
                // so the API layer maps them to 409 instead of swallowing
                // them as a generic 500. `sqlx::Error::Database` wraps a
                // backend-specific error; its `kind()` normalises the most
                // common ones (unique / foreign-key / check / not-null)
                // across Postgres + SQLite.
                match db_err.kind() {
                    sqlx::error::ErrorKind::UniqueViolation => {
                        Self::Conflict(db_err.message().to_string())
                    }
                    _ => Self::Query(err.to_string()),
                }
            }
            other => Self::Query(other.to_string()),
        }
    }
}

impl From<sqlx::migrate::MigrateError> for StorageError {
    fn from(err: sqlx::migrate::MigrateError) -> Self {
        Self::Migration(err.to_string())
    }
}

/// Step-level errors returned by user-provided handlers.
#[derive(Debug, thiserror::Error)]
pub enum StepError {
    #[error("retryable: {message}")]
    Retryable {
        message: String,
        details: Option<serde_json::Value>,
    },

    #[error("permanent: {message}")]
    Permanent {
        message: String,
        details: Option<serde_json::Value>,
    },
}
