use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};

use orch8_engine::error::EngineError;
use orch8_types::context::ContextTooLarge;
use orch8_types::error::StorageError;

/// API-level errors mapped to HTTP status codes.
#[derive(Debug, thiserror::Error)]
pub enum ApiError {
    #[error("not found: {0}")]
    NotFound(String),

    #[error("invalid argument: {0}")]
    InvalidArgument(String),

    #[error("already exists: {0}")]
    AlreadyExists(String),

    #[error("unauthorized")]
    Unauthorized,

    #[error("forbidden: {0}")]
    Forbidden(String),

    #[error("internal: {0}")]
    Internal(String),

    #[error("unavailable: {0}")]
    Unavailable(String),

    #[error("payload too large: {0}")]
    PayloadTooLarge(String),
}

impl ApiError {
    pub fn from_storage(err: StorageError, entity: &str) -> Self {
        match err {
            StorageError::NotFound { entity: e, id } => Self::NotFound(format!("{e} {id}")),
            StorageError::Conflict(msg) => Self::AlreadyExists(msg),
            // Terminal-state targets are precondition failures, not "already
            // exists" — surface as 409 Conflict with a clear message so HTTP
            // clients can distinguish "duplicate" from "wrong state".
            StorageError::TerminalTarget { entity: e, id } => {
                Self::AlreadyExists(format!("{e} {id} is in a terminal state"))
            }
            StorageError::Connection(msg) => Self::Unavailable(msg),
            StorageError::PoolExhausted => Self::Unavailable("pool exhausted".into()),
            other => Self::Internal(format!("{entity}: {other}")),
        }
    }
}

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        let status = match &self {
            ApiError::NotFound(_) => StatusCode::NOT_FOUND,
            ApiError::Unauthorized => StatusCode::UNAUTHORIZED,
            ApiError::Forbidden(_) => StatusCode::FORBIDDEN,
            ApiError::InvalidArgument(_) => StatusCode::BAD_REQUEST,
            ApiError::AlreadyExists(_) => StatusCode::CONFLICT,
            ApiError::Internal(_) => StatusCode::INTERNAL_SERVER_ERROR,
            ApiError::Unavailable(_) => StatusCode::SERVICE_UNAVAILABLE,
            ApiError::PayloadTooLarge(_) => StatusCode::PAYLOAD_TOO_LARGE,
        };
        let body = match &self {
            ApiError::Internal(msg) => {
                tracing::error!(error = %msg, "internal server error");
                serde_json::json!({ "error": "internal server error" })
            }
            _ => serde_json::json!({ "error": self.to_string() }),
        };
        (status, axum::Json(body)).into_response()
    }
}

impl From<StorageError> for ApiError {
    fn from(err: StorageError) -> Self {
        Self::from_storage(err, "resource")
    }
}

impl From<ContextTooLarge> for ApiError {
    fn from(err: ContextTooLarge) -> Self {
        Self::PayloadTooLarge(err.to_string())
    }
}

impl From<EngineError> for ApiError {
    fn from(err: EngineError) -> Self {
        match err {
            EngineError::Storage(StorageError::NotFound { entity, id }) => {
                ApiError::NotFound(format!("{entity} {id}"))
            }
            EngineError::Storage(StorageError::Conflict(msg)) => ApiError::AlreadyExists(msg),
            EngineError::Storage(StorageError::TerminalTarget { entity, id }) => {
                ApiError::AlreadyExists(format!("{entity} {id} is in a terminal state"))
            }
            EngineError::InvalidTransition { .. } => ApiError::InvalidArgument(err.to_string()),
            EngineError::HandlerNotFound(h) => ApiError::NotFound(format!("handler: {h}")),
            EngineError::ShuttingDown => ApiError::Unavailable("shutdown in progress".into()),
            other => ApiError::Internal(other.to_string()),
        }
    }
}
