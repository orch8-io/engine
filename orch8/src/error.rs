/// Unified error type for the embeddable engine.
///
/// Wraps the internal storage / engine error types so embedders only deal
/// with a single error enum. Marked `#[non_exhaustive]` — new variants may
/// be added before 1.0.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum Error {
    /// Builder/configuration problem (missing storage, invalid tenant, ...).
    #[error("configuration error: {0}")]
    Config(String),

    /// A sequence definition failed structural validation.
    #[error("invalid sequence: {0}")]
    InvalidSequence(String),

    /// The requested entity does not exist.
    #[error("not found: {0}")]
    NotFound(String),

    /// The target instance is in a terminal state (completed / failed /
    /// cancelled) and cannot accept the operation (e.g. a signal).
    #[error("instance {0} is in a terminal state")]
    TerminalInstance(String),

    /// Error from the storage backend.
    #[error(transparent)]
    Storage(orch8_types::error::StorageError),

    /// Error from the scheduling engine.
    #[error(transparent)]
    Engine(Box<orch8_engine::error::EngineError>),
}

impl From<orch8_types::error::StorageError> for Error {
    fn from(e: orch8_types::error::StorageError) -> Self {
        Self::Storage(e)
    }
}

impl From<orch8_engine::error::EngineError> for Error {
    fn from(e: orch8_engine::error::EngineError) -> Self {
        Self::Engine(Box::new(e))
    }
}
