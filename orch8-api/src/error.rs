use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};

use orch8_engine::error::EngineError;
use orch8_types::context::ContextTooLarge;
use orch8_types::error::StorageError;

#[derive(Debug, serde::Serialize, utoipa::ToSchema)]
pub struct ErrorDetail {
    pub code: &'static str,
    pub message: String,
    /// Stable public error code (`ORCH8-V005`) when the failure is a
    /// catalogued validation error; see `docs/ERRORS.md`.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error_code: Option<&'static str>,
    /// `https://orch8.io/docs/errors#<error_code>`.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub docs_url: Option<String>,
    pub request_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub details: Option<serde_json::Value>,
}

#[derive(Debug, serde::Serialize, utoipa::ToSchema)]
pub struct ErrorEnvelope {
    pub error: ErrorDetail,
}

/// API-level errors mapped to HTTP status codes.
#[derive(Debug, thiserror::Error)]
pub enum ApiError {
    #[error("not found: {0}")]
    NotFound(String),

    #[error("invalid argument: {0}")]
    InvalidArgument(String),

    /// A catalogued validation failure: HTTP 400 like `InvalidArgument`,
    /// plus a stable `error_code` / `docs_url` in the body. `key` is an
    /// `orch8_types::error_catalog` key such as `DUPLICATE_BLOCK_ID`.
    #[error("invalid argument: {message}")]
    Validation { key: &'static str, message: String },

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

    #[error("unprocessable entity: {0}")]
    UnprocessableEntity(String),

    #[error("bad gateway: {0}")]
    BadGateway(String),

    #[error("rate limit exceeded: {0}")]
    RateLimited(String),
}

impl ApiError {
    #[must_use]
    pub const fn code(&self) -> &'static str {
        match self {
            Self::NotFound(_) => "not_found",
            Self::InvalidArgument(_) | Self::Validation { .. } => "invalid_argument",
            Self::AlreadyExists(_) => "already_exists",
            Self::Conflict(_) => "conflict",
            Self::Unauthorized => "unauthorized",
            Self::Forbidden(_) => "forbidden",
            Self::Internal(_) => "internal",
            Self::Unavailable(_) => "unavailable",
            Self::PayloadTooLarge(_) => "payload_too_large",
            Self::UnprocessableEntity(_) => "unprocessable_entity",
            Self::BadGateway(_) => "bad_gateway",
            Self::RateLimited(_) => "rate_limited",
        }
    }

    /// A catalogued validation error (400 with `error_code` + `docs_url`).
    pub fn validation(key: &'static str, message: impl Into<String>) -> Self {
        Self::Validation {
            key,
            message: message.into(),
        }
    }

    /// The catalogued error-code entry, if this error has one.
    #[must_use]
    pub fn catalog_entry(&self) -> Option<&'static orch8_types::error_catalog::ErrorCodeEntry> {
        match self {
            Self::Validation { key, .. } => orch8_types::error_catalog::lookup(key),
            _ => None,
        }
    }

    pub fn from_storage(err: StorageError, entity: &str) -> Self {
        match err {
            StorageError::NotFound { entity: e, id } => Self::NotFound(format!("{e} {id}")),
            StorageError::Conflict(msg) => Self::AlreadyExists(msg),
            StorageError::QuotaExceeded(msg) => Self::RateLimited(msg),
            // Terminal-state targets are precondition failures, not "already
            // exists" — surface as 409 Conflict with a clear message so HTTP
            // clients can distinguish "duplicate" from "wrong state".
            StorageError::TerminalTarget { entity: e, id } => {
                Self::AlreadyExists(format!("{e} {id} is in a terminal state"))
            }
            // Connection drops and transient external-backend (object store)
            // failures are both 503 — retryable, not a server bug. The raw
            // driver/DSN text can name hosts, schemas, and topology, so it is
            // logged server-side and the client sees only a generic message.
            StorageError::Connection(msg) | StorageError::Backend(msg) => {
                tracing::warn!(entity, detail = %msg, "storage backend unavailable");
                Self::Unavailable("storage backend temporarily unavailable".into())
            }
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
            Self::InvalidArgument(_) | Self::Validation { .. } => StatusCode::BAD_REQUEST,
            Self::AlreadyExists(_) | Self::Conflict(_) => StatusCode::CONFLICT,
            Self::Internal(_) => StatusCode::INTERNAL_SERVER_ERROR,
            Self::Unavailable(_) => StatusCode::SERVICE_UNAVAILABLE,
            Self::PayloadTooLarge(_) => StatusCode::PAYLOAD_TOO_LARGE,
            Self::UnprocessableEntity(_) => StatusCode::UNPROCESSABLE_ENTITY,
            Self::BadGateway(_) => StatusCode::BAD_GATEWAY,
            Self::RateLimited(_) => StatusCode::TOO_MANY_REQUESTS,
        };
        let message = match &self {
            Self::Internal(msg) => {
                tracing::error!(error = %msg, "internal server error");
                "internal server error".to_string()
            }
            _ => self.to_string(),
        };
        let entry = self.catalog_entry();
        let body = ErrorEnvelope {
            error: ErrorDetail {
                code: self.code(),
                message,
                error_code: entry.map(|e| e.code),
                docs_url: entry.map(orch8_types::error_catalog::ErrorCodeEntry::docs_url),
                request_id: None,
                details: None,
            },
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
            EngineError::Storage(error) => Self::from(error),
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

    #[tokio::test]
    async fn error_body_has_stable_machine_code() {
        let response = ApiError::AlreadyExists("duplicate sequence".into()).into_response();
        let body = axum::body::to_bytes(response.into_body(), 4096)
            .await
            .unwrap();
        let value: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(value["error"]["code"], "already_exists");
        assert_eq!(
            value["error"]["message"],
            "already exists: duplicate sequence"
        );
        assert!(value["error"]["request_id"].is_null());
    }

    #[tokio::test]
    async fn validation_errors_carry_stable_code_and_docs_url() {
        let err = ApiError::validation("DUPLICATE_BLOCK_ID", "duplicate block id: a");
        let response = err.into_response();
        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
        let body = axum::body::to_bytes(response.into_body(), 4096)
            .await
            .unwrap();
        let value: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(value["error"]["code"], "invalid_argument");
        assert_eq!(value["error"]["error_code"], "ORCH8-V005");
        assert_eq!(
            value["error"]["docs_url"],
            "https://orch8.io/docs/errors#ORCH8-V005"
        );
        assert_eq!(
            value["error"]["message"],
            "invalid argument: duplicate block id: a"
        );
        // Uncatalogued errors keep the old body shape.
        let response = ApiError::InvalidArgument("x".into()).into_response();
        let body = axum::body::to_bytes(response.into_body(), 4096)
            .await
            .unwrap();
        let value: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert!(value["error"].get("error_code").is_none());
    }

    #[test]
    fn storage_quota_maps_to_429() {
        assert_eq!(
            status_of(StorageError::QuotaExceeded("plan exhausted".into()).into()),
            StatusCode::TOO_MANY_REQUESTS
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

    #[test]
    fn context_too_large_maps_to_payload_too_large() {
        let err: ApiError = ContextTooLarge {
            max: 1024,
            actual: 2048,
        }
        .into();
        assert_eq!(status_of(err), StatusCode::PAYLOAD_TOO_LARGE);
    }

    #[test]
    fn engine_error_step_failed_maps_to_internal() {
        let err: ApiError = EngineError::StepFailed {
            instance_id: orch8_types::ids::InstanceId::new(),
            block_id: orch8_types::ids::BlockId::new("b"),
            message: "boom".into(),
            retryable: false,
            details: None,
        }
        .into();
        assert_eq!(status_of(err), StatusCode::INTERNAL_SERVER_ERROR);
    }

    #[test]
    fn engine_error_storage_maps_to_internal() {
        let err: ApiError = EngineError::Storage(StorageError::Query("bad sql".into())).into();
        assert_eq!(status_of(err), StatusCode::INTERNAL_SERVER_ERROR);
    }

    #[tokio::test]
    async fn wrapped_storage_failures_preserve_retry_status_and_redaction() {
        for (storage, status, code, message) in [
            (
                StorageError::Connection("private database address".into()),
                StatusCode::SERVICE_UNAVAILABLE,
                "unavailable",
                "unavailable: storage backend temporarily unavailable",
            ),
            (
                StorageError::Backend("private object store address".into()),
                StatusCode::SERVICE_UNAVAILABLE,
                "unavailable",
                "unavailable: storage backend temporarily unavailable",
            ),
            (
                StorageError::PoolExhausted,
                StatusCode::SERVICE_UNAVAILABLE,
                "unavailable",
                "unavailable: pool exhausted",
            ),
            (
                StorageError::QuotaExceeded("plan exhausted".into()),
                StatusCode::TOO_MANY_REQUESTS,
                "rate_limited",
                "rate limit exceeded: plan exhausted",
            ),
        ] {
            let response = ApiError::from(EngineError::Storage(storage)).into_response();
            assert_eq!(response.status(), status);
            let bytes = axum::body::to_bytes(response.into_body(), 4096)
                .await
                .unwrap();
            let body: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
            assert_eq!(body["error"]["code"], code);
            assert_eq!(body["error"]["message"], message);
        }
    }

    #[test]
    fn not_found_maps_to_404() {
        assert_eq!(
            status_of(ApiError::NotFound("instance xyz".into())),
            StatusCode::NOT_FOUND
        );
    }

    #[test]
    fn unavailable_maps_to_503() {
        assert_eq!(
            status_of(ApiError::Unavailable("downstream".into())),
            StatusCode::SERVICE_UNAVAILABLE
        );
    }
}
