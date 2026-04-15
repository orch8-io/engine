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
