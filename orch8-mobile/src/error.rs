/// Error type exposed to mobile hosts via `UniFFI`.
#[derive(Debug, uniffi::Error, thiserror::Error)]
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
    #[error("handler failed (retryable): {message}")]
    Retryable { message: String },
    #[error("handler failed (permanent): {message}")]
    Permanent { message: String },
}

/// Callback interface for refreshable authentication tokens.
#[uniffi::export(with_foreign)]
pub trait TokenProvider: Send + Sync {
    /// Return the current auth token. Called before each sync request.
    fn current_token(&self) -> String;
    /// Called when a 401/403 is received to obtain a fresh token.
    fn refresh_token(&self) -> Result<String, MobileError>;
}
