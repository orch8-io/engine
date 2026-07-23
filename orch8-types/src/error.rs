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

    /// An operation this backend does not support, or a capability that is not
    /// configured (e.g. artifact storage with no artifact backend wired in).
    /// **Permanent** — retrying will not help; callers must map this to a
    /// non-retryable error.
    #[error("unsupported: {0}")]
    Unsupported(String),

    /// A transient failure in an external storage backend (object store /
    /// artifact backend): network error, throttling, temporary unavailability.
    /// **Retryable** — distinct from [`Self::Unsupported`] so handlers don't
    /// retry forever on a permanent misconfiguration, nor fail fast on a blip.
    #[error("backend error: {0}")]
    Backend(String),
}

impl StorageError {
    /// `true` when this error is a *transient* infrastructure failure —
    /// connection loss, connection-pool exhaustion, or a transient
    /// external-backend blip (e.g. a database failover or an object-store
    /// throttle).
    ///
    /// This is the single source of truth for storage-error retryability:
    /// the scheduler's per-instance safety net (`EngineError::is_transient_storage`)
    /// and the step-level mapper used by the coordination handlers both
    /// consult it, so a variant cannot be rescheduled in one place and
    /// failed/DLQ'd in another. Logic errors (`Query`, `Serialization`,
    /// `Conflict`, `Unsupported`, …) are NOT transient — retrying them loops
    /// forever.
    #[must_use]
    pub fn is_transient(&self) -> bool {
        matches!(
            self,
            Self::Connection(_) | Self::PoolExhausted | Self::Backend(_)
        )
    }
}

impl From<sqlx::Error> for StorageError {
    fn from(err: sqlx::Error) -> Self {
        match err {
            sqlx::Error::PoolTimedOut => Self::PoolExhausted,
            // Transport-level failures — socket I/O, TLS handshake, pool
            // closed mid-operation — are transient connection problems, not
            // query errors: mapping them to `Query` would classify them as
            // permanent and fail healthy instances on a momentary blip.
            sqlx::Error::Io(_) | sqlx::Error::Tls(_) | sqlx::Error::PoolClosed => {
                Self::Connection(err.to_string())
            }
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
                    // Deadlocks (SQLSTATE 40P01), serialization failures
                    // (40001), and SQLite lock contention (BUSY/LOCKED) are
                    // routine under scheduler concurrency and are *transient*:
                    // mapping them to `Query` would classify them as permanent
                    // and fail/DLQ healthy instances instead of rescheduling.
                    _ if is_transient_db_code(db_err.code().as_deref()) => {
                        Self::Connection(err.to_string())
                    }
                    _ => Self::Query(err.to_string()),
                }
            }
            other => Self::Query(other.to_string()),
        }
    }
}

/// `true` for backend error codes that indicate a transient, retryable
/// conflict rather than a logic error: Postgres serialization failures
/// (`40001`) and deadlocks (`40P01`), and `SQLite` `SQLITE_BUSY`/`SQLITE_LOCKED`
/// (primary codes 5/6 and their extended variants — sqlx-sqlite reports the
/// extended result code).
fn is_transient_db_code(code: Option<&str>) -> bool {
    matches!(
        code,
        Some("40001" | "40P01" | "5" | "6" | "261" | "262" | "517" | "518")
    )
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

    // --- StorageError::is_transient / sqlx mapping ---

    /// Transport-level sqlx failures must surface as `Connection` (transient),
    /// not `Query` (permanent) — otherwise a momentary network/TLS blip or a
    /// closed pool fails/DLQs healthy instances.
    #[test]
    fn sqlx_io_error_maps_to_transient_connection() {
        let err: StorageError = sqlx::Error::Io(std::io::Error::new(
            std::io::ErrorKind::ConnectionReset,
            "connection reset",
        ))
        .into();
        assert!(matches!(err, StorageError::Connection(_)));
        assert!(err.is_transient());
    }

    #[test]
    fn sqlx_tls_error_maps_to_transient_connection() {
        let err: StorageError = sqlx::Error::Tls("handshake failed".into()).into();
        assert!(matches!(err, StorageError::Connection(_)));
        assert!(err.is_transient());
    }

    #[test]
    fn sqlx_pool_closed_maps_to_transient_connection() {
        let err: StorageError = sqlx::Error::PoolClosed.into();
        assert!(matches!(err, StorageError::Connection(_)));
        assert!(err.is_transient());
    }

    #[test]
    fn sqlx_pool_timed_out_maps_to_transient_pool_exhausted() {
        let err: StorageError = sqlx::Error::PoolTimedOut.into();
        assert!(matches!(err, StorageError::PoolExhausted));
        assert!(err.is_transient());
    }

    /// Deadlocks (40P01), serialization failures (40001), and `SQLite` lock
    /// contention (BUSY/LOCKED, incl. extended codes) must classify as
    /// transient so the scheduler reschedules instead of DLQ-ing.
    #[test]
    fn transient_db_error_codes_are_recognised() {
        for code in ["40001", "40P01", "5", "6", "261", "262", "517", "518"] {
            assert!(
                is_transient_db_code(Some(code)),
                "{code} must be transient"
            );
        }
        for code in ["23505", "22001", "42601", "999"] {
            assert!(
                !is_transient_db_code(Some(code)),
                "{code} must not be transient"
            );
        }
        assert!(!is_transient_db_code(None));
    }

    /// Every variant's retryability classification, pinned in one table —
    /// this is the source of truth both engine classifiers must agree with.
    /// Keep it in sync when adding variants.
    #[test]
    fn storage_error_is_transient_table() {
        let serde_err = serde_json::from_str::<serde_json::Value>("{bad").unwrap_err();
        let cases: Vec<(&str, StorageError, bool)> = vec![
            ("Connection", StorageError::Connection("x".into()), true),
            ("Query", StorageError::Query("x".into()), false),
            (
                "NotFound",
                StorageError::NotFound {
                    entity: "instance",
                    id: "x".into(),
                },
                false,
            ),
            ("Conflict", StorageError::Conflict("x".into()), false),
            (
                "TerminalTarget",
                StorageError::TerminalTarget {
                    entity: "instance".into(),
                    id: "x".into(),
                },
                false,
            ),
            ("Migration", StorageError::Migration("x".into()), false),
            (
                "Serialization",
                StorageError::Serialization(serde_err),
                false,
            ),
            ("PoolExhausted", StorageError::PoolExhausted, true),
            ("Unsupported", StorageError::Unsupported("x".into()), false),
            ("Backend", StorageError::Backend("x".into()), true),
        ];
        assert_eq!(
            cases.len(),
            10,
            "table must cover every StorageError variant"
        );
        for (name, err, expected) in cases {
            assert_eq!(err.is_transient(), expected, "{name} misclassified");
        }
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
