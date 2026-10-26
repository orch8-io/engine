/// Adds an initialization checksum unique to the bidirectional callback error
/// contract. New bindings must refuse older development libraries whose flat
/// callback errors cannot be lifted back into Rust.
#[uniffi::export]
pub fn mobile_callback_contract_v2() -> u32 {
    2
}

/// Error type exposed to mobile hosts via `UniFFI`. The `detail` field avoids
/// colliding with Kotlin Throwable.message in generated structured exceptions.
#[derive(Debug, uniffi::Error, thiserror::Error)]
pub enum MobileError {
    #[error("engine error: {detail}")]
    Engine { detail: String },
    #[error("storage error: {detail}")]
    Storage { detail: String },
    #[error("invalid input: {detail}")]
    InvalidInput { detail: String },
    #[error("not found: {detail}")]
    NotFound { detail: String },
    #[error("resource limit exceeded: {detail}")]
    ResourceLimit { detail: String },
    #[error("already exists: {detail}")]
    AlreadyExists { detail: String },
    #[error("{detail}")]
    Shutdown { detail: String },
}

impl MobileError {
    pub(crate) fn shutdown() -> Self {
        Self::Shutdown {
            detail: "shutdown in progress".into(),
        }
    }
}

impl From<orch8_types::error::StorageError> for MobileError {
    fn from(e: orch8_types::error::StorageError) -> Self {
        Self::Storage {
            detail: e.to_string(),
        }
    }
}

impl From<orch8_engine::error::EngineError> for MobileError {
    fn from(e: orch8_engine::error::EngineError) -> Self {
        Self::Engine {
            detail: e.to_string(),
        }
    }
}

impl From<serde_json::Error> for MobileError {
    fn from(e: serde_json::Error) -> Self {
        Self::InvalidInput {
            detail: e.to_string(),
        }
    }
}

impl From<SyncError> for MobileError {
    fn from(e: SyncError) -> Self {
        Self::Engine {
            detail: e.to_string(),
        }
    }
}

/// Sync-specific error type.
#[derive(Debug, uniffi::Error, thiserror::Error)]
#[uniffi(flat_error)]
pub enum SyncError {
    #[error("network error: {message}")]
    Network { message: String },
    #[error("signature verification failed: {message}")]
    SignatureInvalid { message: String },
    #[error("invalid manifest: {message}")]
    InvalidManifest { message: String },
}

/// Handler error returned by host-registered step handlers.
#[derive(Debug, uniffi::Error, thiserror::Error)]
pub enum HandlerError {
    #[error("handler failed (retryable): {detail}")]
    Retryable { detail: String },
    #[error("handler failed (permanent): {detail}")]
    Permanent { detail: String },
}

#[cfg(test)]
mod callback_error_tests {
    use super::{HandlerError, MobileError};

    #[test]
    fn foreign_handler_errors_lift_with_classification_and_message() {
        // This is the same Lift boundary used for a foreign callback's error.
        // Flat errors panic here even though Rust->host lowering succeeds.
        for permanent in [false, true] {
            let error = if permanent {
                HandlerError::Permanent {
                    detail: "host permanent".into(),
                }
            } else {
                HandlerError::Retryable {
                    detail: "host retry".into(),
                }
            };
            let buffer = <HandlerError as uniffi::Lower<crate::UniFfiTag>>::lower(error);
            let lifted =
                <HandlerError as uniffi::Lift<crate::UniFfiTag>>::try_lift(buffer).unwrap();
            match lifted {
                HandlerError::Permanent { detail: message } => {
                    assert!(permanent);
                    assert_eq!(message, "host permanent");
                }
                HandlerError::Retryable { detail: message } => {
                    assert!(!permanent);
                    assert_eq!(message, "host retry");
                }
            }
        }
    }

    #[test]
    fn foreign_token_errors_lift_including_shutdown_detail() {
        for error in [
            MobileError::Engine {
                detail: "refresh refused".into(),
            },
            MobileError::Shutdown {
                detail: "engine retired".into(),
            },
        ] {
            let expected = error.to_string();
            let buffer = <MobileError as uniffi::Lower<crate::UniFfiTag>>::lower(error);
            let lifted = <MobileError as uniffi::Lift<crate::UniFfiTag>>::try_lift(buffer).unwrap();
            assert_eq!(lifted.to_string(), expected);
        }
        assert_eq!(MobileError::shutdown().to_string(), "shutdown in progress");
    }
}

/// Callback interface for refreshable authentication tokens.
#[uniffi::export(with_foreign)]
pub trait TokenProvider: Send + Sync {
    /// Return a cached bootstrap token for synchronous provider installation.
    /// Keep this prompt; reservation-based asynchronous installation does not
    /// invoke this getter. Perform network work in refresh or an async wrapper.
    fn current_token(&self) -> String;
    /// Obtain a token for unavailable initial authorization or after a 401.
    /// Runs on a native blocking thread; it may outlive the caller's timeout.
    fn refresh_token(&self) -> Result<String, MobileError>;
}
