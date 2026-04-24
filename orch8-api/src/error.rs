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

    #[error("conflict: {0}")]
    Conflict(String),

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
            Self::NotFound(_) => StatusCode::NOT_FOUND,
            Self::Unauthorized => StatusCode::UNAUTHORIZED,
            Self::Forbidden(_) => StatusCode::FORBIDDEN,
            Self::InvalidArgument(_) => StatusCode::BAD_REQUEST,
            Self::AlreadyExists(_) | Self::Conflict(_) => StatusCode::CONFLICT,
            Self::Internal(_) => StatusCode::INTERNAL_SERVER_ERROR,
            Self::Unavailable(_) => StatusCode::SERVICE_UNAVAILABLE,
            Self::PayloadTooLarge(_) => StatusCode::PAYLOAD_TOO_LARGE,
        };
        let body = match &self {
            Self::Internal(msg) => {
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
                Self::NotFound(format!("{entity} {id}"))
            }
            EngineError::Storage(StorageError::Conflict(msg)) => Self::AlreadyExists(msg),
            EngineError::Storage(StorageError::TerminalTarget { entity, id }) => {
                Self::AlreadyExists(format!("{entity} {id} is in a terminal state"))
            }
            EngineError::InvalidTransition { .. } => Self::InvalidArgument(err.to_string()),
            EngineError::HandlerNotFound(h) => Self::NotFound(format!("handler: {h}")),
            EngineError::ShuttingDown => Self::Unavailable("shutdown in progress".into()),
            other => Self::Internal(other.to_string()),
        }
    }
}

#[cfg(test)]
mod tests {
    //! Error mapping unit tests (#281-285 from `TEST_PLAN.md`).
    //!
    //! Pins HTTP status mapping — regressions here would silently change
    //! API contract for clients that switch on status codes.
    use super::*;
    use axum::http::StatusCode;

    fn status_of(err: ApiError) -> StatusCode {
        err.into_response().status()
    }

    #[test]
    fn storage_not_found_maps_to_404() {
        // #281
        let err: ApiError = StorageError::NotFound {
            entity: "instance",
            id: "abc".into(),
        }
        .into();
        assert!(matches!(err, ApiError::NotFound(_)));
        assert_eq!(status_of(err), StatusCode::NOT_FOUND);
    }

    #[test]
    fn storage_conflict_maps_to_409_already_exists() {
        // #282
        let err: ApiError = StorageError::Conflict("dup".into()).into();
        assert!(matches!(err, ApiError::AlreadyExists(_)));
        assert_eq!(status_of(err), StatusCode::CONFLICT);
    }

    #[test]
    fn api_error_conflict_maps_to_409() {
        // #283
        assert_eq!(
            status_of(ApiError::Conflict("bad state".into())),
            StatusCode::CONFLICT
        );
    }

    #[test]
    fn payload_too_large_maps_to_413() {
        // #284
        assert_eq!(
            status_of(ApiError::PayloadTooLarge("context > max".into())),
            StatusCode::PAYLOAD_TOO_LARGE
        );
    }

    #[test]
    fn unauthorized_maps_to_401() {
        assert_eq!(status_of(ApiError::Unauthorized), StatusCode::UNAUTHORIZED);
    }

    #[test]
    fn forbidden_maps_to_403() {
        assert_eq!(
            status_of(ApiError::Forbidden("cross-tenant".into())),
            StatusCode::FORBIDDEN
        );
    }

    #[test]
    fn invalid_argument_maps_to_400() {
        assert_eq!(
            status_of(ApiError::InvalidArgument("bad".into())),
            StatusCode::BAD_REQUEST
        );
    }

    #[tokio::test]
    async fn internal_response_redacts_message() {
        // The body for Internal errors must not leak the underlying message
        // (we log it at `error` level server-side instead). Redaction is
        // part of the API contract — tests here stop a regression where
        // someone "helpfully" starts returning error details to clients.
        let err = ApiError::Internal("db driver panic: stack trace".into());
        let resp = err.into_response();
        assert_eq!(resp.status(), StatusCode::INTERNAL_SERVER_ERROR);
        let body = axum::body::to_bytes(resp.into_body(), 1024)
            .await
            .expect("body ok");
        let text = std::str::from_utf8(&body).unwrap();
        assert!(
            !text.contains("stack trace"),
            "internal error body must not leak details: {text}"
        );
        assert!(text.contains("internal server error"));
    }

    #[test]
    fn storage_terminal_target_maps_to_conflict_status() {
        let err: ApiError = StorageError::TerminalTarget {
            entity: "instance".into(),
            id: "xyz".into(),
        }
        .into();
        // TerminalTarget is surfaced as `AlreadyExists` → 409 (CONFLICT), so
        // HTTP clients can distinguish "duplicate create" from "wrong state"
        // via the error message while keeping the status code consistent.
        assert_eq!(status_of(err), StatusCode::CONFLICT);
    }

    #[test]
    fn storage_connection_maps_to_503_unavailable() {
        let err: ApiError = StorageError::Connection("timeout".into()).into();
        assert_eq!(status_of(err), StatusCode::SERVICE_UNAVAILABLE);
    }
}
