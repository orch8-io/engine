/// Error type exposed to mobile hosts via `UniFFI`.
#[derive(Debug, uniffi::Error, thiserror::Error)]
#[uniffi(flat_error)]
pub enum MobileError {
    #[error("engine error: {message}")]
    Engine { message: String },
    #[error("storage error: {message}")]
    Storage { message: String },
    #[error("invalid input: {message}")]
    InvalidInput { message: String },
    #[error("not found: {message}")]
    NotFound { message: String },
    #[error("resource limit exceeded: {message}")]
    ResourceLimit { message: String },
    #[error("already exists: {message}")]
    AlreadyExists { message: String },
    #[error("shutdown in progress")]
    Shutdown,
}

impl From<orch8_types::error::StorageError> for MobileError {
    fn from(e: orch8_types::error::StorageError) -> Self {
        Self::Storage {
            message: e.to_string(),
        }
    }
}

impl From<orch8_engine::error::EngineError> for MobileError {
    fn from(e: orch8_engine::error::EngineError) -> Self {
        Self::Engine {
            message: e.to_string(),
        }
    }
}

impl From<serde_json::Error> for MobileError {
    fn from(e: serde_json::Error) -> Self {
        Self::InvalidInput {
            message: e.to_string(),
        }
    }
}

impl From<SyncError> for MobileError {
    fn from(e: SyncError) -> Self {
        Self::Engine {
            message: e.to_string(),
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
#[uniffi(flat_error)]
pub enum HandlerError {
    #[error("handler failed (retryable): {message}")]
    Retryable { message: String },
    #[error("handler failed (permanent): {message}")]
    Permanent { message: String },
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
