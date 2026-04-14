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

#[cfg(test)]
mod tests {
    use super::*;

    // --- StorageError Display ---

    #[test]
    fn storage_error_connection_display() {
        let err = StorageError::Connection("host unreachable".into());
        assert_eq!(err.to_string(), "connection failed: host unreachable");
    }

    #[test]
    fn storage_error_query_display() {
        let err = StorageError::Query("syntax error".into());
        assert_eq!(err.to_string(), "query failed: syntax error");
    }

    #[test]
    fn storage_error_not_found_display() {
        let err = StorageError::NotFound {
            entity: "instance",
            id: "abc-123".into(),
        };
        assert_eq!(err.to_string(), "not found: instance with id abc-123");
    }

    #[test]
    fn storage_error_conflict_display() {
        let err = StorageError::Conflict("duplicate key".into());
        assert_eq!(err.to_string(), "conflict: duplicate key");
    }

    #[test]
    fn storage_error_terminal_target_display() {
        let err = StorageError::TerminalTarget {
            entity: "instance".into(),
            id: "xyz".into(),
        };
        assert_eq!(
            err.to_string(),
            "terminal target: instance xyz is in a terminal state"
        );
    }

    #[test]
    fn storage_error_migration_display() {
        let err = StorageError::Migration("version mismatch".into());
        assert_eq!(err.to_string(), "migration failed: version mismatch");
    }

    #[test]
    fn storage_error_pool_exhausted_display() {
        let err = StorageError::PoolExhausted;
        assert_eq!(err.to_string(), "pool exhausted");
    }

    #[test]
    fn storage_error_serialization_from_serde() {
        let serde_err = serde_json::from_str::<serde_json::Value>("{bad").unwrap_err();
        let err: StorageError = serde_err.into();
        assert!(matches!(err, StorageError::Serialization(_)));
        assert!(err.to_string().starts_with("serialization error:"));
    }

    // --- StepError Display ---

    #[test]
    fn step_error_retryable_display() {
        let err = StepError::Retryable {
            message: "timeout".into(),
            details: None,
        };
        assert_eq!(err.to_string(), "retryable: timeout");
    }

    #[test]
    fn step_error_permanent_display() {
        let err = StepError::Permanent {
            message: "bad input".into(),
            details: Some(serde_json::json!({"field": "name"})),
        };
        assert_eq!(err.to_string(), "permanent: bad input");
    }

    #[test]
    fn step_error_retryable_with_details() {
        let details = serde_json::json!({"retry_after": 30});
        let err = StepError::Retryable {
            message: "rate limited".into(),
            details: Some(details.clone()),
        };
        match err {
            StepError::Retryable {
                message,
                details: d,
            } => {
                assert_eq!(message, "rate limited");
                assert_eq!(d.unwrap(), details);
            }
            StepError::Permanent { .. } => panic!("expected Retryable"),
        }
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
