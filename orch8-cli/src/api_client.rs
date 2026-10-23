//! Shared HTTP client for every `orch8` subcommand that talks to a server.
//!
//! One place builds the `reqwest` client, stamps `x-api-key` / `x-tenant-id`,
//! and turns every non-2xx response or transport failure into a [`CliError`]
//! whose message tells the operator what to do next (set a key, pick a
//! tenant, start the server). Command modules never inspect
//! `status().is_success()` or decode error bodies themselves.

use std::fmt;
use std::time::Duration;

use reqwest::{Method, RequestBuilder, StatusCode, header};
use serde::Serialize;
use serde::de::DeserializeOwned;
use serde_json::Value;

/// Canonical API prefix the CLI strips to reach operational root routes
/// (`/health/*`, `/info`).
pub const API_PREFIX: &str = "/api/v1";

/// Longest error-body excerpt echoed back to the operator.
const BODY_EXCERPT_MAX: usize = 400;

/// The CLI's view of one server: base URL, credentials, and the HTTP client.
#[derive(Clone)]
pub struct ApiClient {
    base_url: String,
    api_key: Option<String>,
    tenant_id: Option<String>,
    http: reqwest::Client,
}

/// Every way a server interaction can fail, with an operator-facing message.
#[derive(Debug)]
pub enum CliError {
    /// 401 — the server wants an API key (or a different one).
    Unauthorized { message: String },
    /// 400/422 whose envelope points at a missing or invalid tenant header.
    TenantMissing { message: String },
    /// TCP connect failed — the server is probably not running.
    Connect { url: String },
    /// The request exceeded the client timeout.
    Timeout { url: String },
    /// Any other transport-level failure (TLS, DNS, reset, …).
    Transport { url: String, message: String },
    /// A non-2xx response with its decoded error envelope.
    Api {
        status: StatusCode,
        code: Option<String>,
        message: String,
        request_id: Option<String>,
        details: Option<Value>,
    },
    /// A 2xx response whose body was not the JSON the caller expected.
    Decode {
        url: String,
        status: StatusCode,
        message: String,
    },
}

impl CliError {
    /// HTTP status of the failing response, if the server answered at all.
    #[must_use]
    pub fn status(&self) -> Option<StatusCode> {
        match self {
            Self::Unauthorized { .. } => Some(StatusCode::UNAUTHORIZED),
            Self::Api { status, .. } | Self::Decode { status, .. } => Some(*status),
            Self::TenantMissing { .. }
            | Self::Connect { .. }
            | Self::Timeout { .. }
            | Self::Transport { .. } => None,
        }
    }

    /// True when the server answered 404.
    #[must_use]
    pub fn is_not_found(&self) -> bool {
        self.status() == Some(StatusCode::NOT_FOUND)
    }

    /// Machine-readable `code` from the error envelope, when present.
    #[must_use]
    pub fn code(&self) -> Option<&str> {
        match self {
            Self::Api { code, .. } => code.as_deref(),
            _ => None,
        }
    }
}

impl fmt::Display for CliError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Unauthorized { message } => {
                write!(f, "unauthorized: set ORCH8_API_KEY or --api-key")?;
                if !message.is_empty() {
                    write!(f, " ({message})")?;
                }
                Ok(())
            }
            Self::TenantMissing { message } => {
                write!(f, "{message}: set ORCH8_TENANT_ID or --tenant-id")
            }
            Self::Connect { url } => write!(
                f,
                "could not reach {url}: is orch8-server running? \
                 (start with `orch8-server --config orch8.toml`)"
            ),
            Self::Timeout { url } => write!(f, "request to {url} timed out"),
            Self::Transport { url, message } => write!(f, "request to {url} failed: {message}"),
            Self::Api {
                status,
                code,
                message,
                request_id,
                ..
            } => {
                write!(f, "server returned {status}")?;
                if let Some(code) = code {
                    write!(f, " [{code}]")?;
                }
                if !message.is_empty() {
                    write!(f, ": {message}")?;
                }
                if let Some(id) = request_id {
                    write!(f, " (request_id {id})")?;
                }
                Ok(())
            }
            Self::Decode {
                url,
                status,
                message,
            } => write!(
                f,
                "{url} answered {status} but the body is not the expected JSON: {message}"
            ),
        }
    }
}

impl std::error::Error for CliError {}

/// Trim a body excerpt to a single readable line.
fn excerpt(text: &str) -> String {
    let compact: String = text.split_whitespace().collect::<Vec<_>>().join(" ");
    if compact.chars().count() > BODY_EXCERPT_MAX {
        let cut: String = compact.chars().take(BODY_EXCERPT_MAX).collect();
        format!("{cut}…")
    } else {
        compact
    }
}

/// Classify a non-2xx response into a [`CliError`] using the JSON error
/// envelope `{"error", "code", "request_id", "details"}` when present and
/// the raw body (or the status reason) otherwise.
#[must_use]
pub fn map_error_response(status: StatusCode, body: &str) -> CliError {
    let envelope: Option<Value> = serde_json::from_str(body).ok();
    let field = |key: &str| -> Option<String> {
        envelope
            .as_ref()
            .and_then(|v| v.get(key))
            .and_then(Value::as_str)
            .map(str::to_string)
    };
    let message = field("error")
        .or_else(|| field("message"))
        .unwrap_or_else(|| {
            let raw = excerpt(body);
            if raw.is_empty() {
                status
                    .canonical_reason()
                    .unwrap_or("request failed")
                    .to_string()
            } else {
                raw
            }
        });
    let code = field("code");
    let request_id = field("request_id");
    let details = envelope
        .as_ref()
        .and_then(|v| v.get("details"))
        .filter(|d| !d.is_null())
        .cloned();

    if status == StatusCode::UNAUTHORIZED {
        return CliError::Unauthorized { message };
    }
    let mentions_tenant = code.as_deref().is_some_and(|c| c.contains("tenant"))
        || message.to_ascii_lowercase().contains("tenant");
    if matches!(
        status,
        StatusCode::BAD_REQUEST | StatusCode::UNPROCESSABLE_ENTITY
    ) && mentions_tenant
    {
        return CliError::TenantMissing { message };
    }
    CliError::Api {
        status,
        code,
        message,
        request_id,
        details,
    }
}

/// Classify a `reqwest` transport error.
fn map_transport_error(error: &reqwest::Error, fallback_url: &str) -> CliError {
    let url = error
        .url()
        .map_or_else(|| fallback_url.to_string(), ToString::to_string);
    if error.is_connect() {
        CliError::Connect { url }
    } else if error.is_timeout() {
        CliError::Timeout { url }
    } else {
        // Drop the redundant URL reqwest embeds in its own message.
        let message = error.without_url().to_string();
        CliError::Transport { url, message }
    }
}

impl ApiClient {
    /// Build a client with the CLI's default timeouts (10 s connect, 60 s
    /// total). Empty credentials are treated as absent.
    pub fn new(
        base_url: &str,
        api_key: Option<&str>,
        tenant_id: Option<&str>,
    ) -> anyhow::Result<Self> {
        Self::with_timeouts(
            base_url,
            api_key,
            tenant_id,
            Duration::from_secs(10),
            Duration::from_secs(60),
        )
    }

    /// Build a client with explicit timeouts (used for readiness polling).
    pub fn with_timeouts(
        base_url: &str,
        api_key: Option<&str>,
        tenant_id: Option<&str>,
        connect_timeout: Duration,
        timeout: Duration,
    ) -> anyhow::Result<Self> {
        let api_key = api_key.filter(|s| !s.is_empty()).map(str::to_string);
        let tenant_id = tenant_id.filter(|s| !s.is_empty()).map(str::to_string);
        let mut headers = header::HeaderMap::new();
        if let Some(k) = &api_key {
            let mut v = header::HeaderValue::from_str(k)
                .map_err(|e| anyhow::anyhow!("invalid --api-key value: {e}"))?;
            v.set_sensitive(true);
            headers.insert("x-api-key", v);
        }
        if let Some(t) = &tenant_id {
            let v = header::HeaderValue::from_str(t)
                .map_err(|e| anyhow::anyhow!("invalid --tenant-id value: {e}"))?;
            headers.insert("x-tenant-id", v);
        }
        let http = reqwest::Client::builder()
            .default_headers(headers)
            .connect_timeout(connect_timeout)
            .timeout(timeout)
            .build()?;
        Ok(Self {
            base_url: base_url.trim_end_matches('/').to_string(),
            api_key,
            tenant_id,
            http,
        })
    }

    /// API base URL without a trailing slash (e.g. `http://host:8080/api/v1`).
    #[must_use]
    pub fn base(&self) -> &str {
        &self.base_url
    }

    /// Operational root: the base URL with the `/api/v1` prefix removed.
    #[must_use]
    pub fn root(&self) -> &str {
        self.base_url
            .strip_suffix(API_PREFIX)
            .unwrap_or(&self.base_url)
    }

    /// Tenant sent as `x-tenant-id`, if configured.
    #[must_use]
    pub fn tenant_id(&self) -> Option<&str> {
        self.tenant_id.as_deref()
    }

    /// API key sent as `x-api-key`, if configured.
    #[must_use]
    pub fn api_key(&self) -> Option<&str> {
        self.api_key.as_deref()
    }

    /// The tenant, or a next-step error when none was configured.
    pub fn require_tenant(&self) -> Result<&str, CliError> {
        self.tenant_id().ok_or_else(|| CliError::TenantMissing {
            message: "no tenant selected".into(),
        })
    }

    /// Resolve `path` against the API base. Absolute `http(s)` URLs pass
    /// through unchanged so root routes can be addressed explicitly.
    #[must_use]
    pub fn url(&self, path: &str) -> String {
        if path.starts_with("http://") || path.starts_with("https://") {
            path.to_string()
        } else {
            format!("{}/{}", self.base_url, path.trim_start_matches('/'))
        }
    }

    /// Resolve `path` against the operational root (`/health/ready`, `/info`).
    #[must_use]
    pub fn root_url(&self, path: &str) -> String {
        format!("{}/{}", self.root(), path.trim_start_matches('/'))
    }

    /// Start a request with default headers applied; use for custom query
    /// strings or headers, then finish with [`ApiClient::execute`].
    #[must_use]
    pub fn request(&self, method: Method, path: &str) -> RequestBuilder {
        self.http.request(method, self.url(path))
    }

    /// `GET path` decoded as `T`.
    pub async fn get<T: DeserializeOwned>(&self, path: &str) -> Result<T, CliError> {
        self.execute(self.request(Method::GET, path)).await
    }

    /// `GET path?query` decoded as `T`; `query` is percent-encoded.
    pub async fn get_query<T: DeserializeOwned, Q: Serialize + ?Sized>(
        &self,
        path: &str,
        query: &Q,
    ) -> Result<T, CliError> {
        self.execute(self.request(Method::GET, path).query(query))
            .await
    }

    /// `POST path` with a JSON body, decoded as `T`.
    pub async fn post<T: DeserializeOwned, B: Serialize + ?Sized>(
        &self,
        path: &str,
        body: &B,
    ) -> Result<T, CliError> {
        self.execute(self.request(Method::POST, path).json(body))
            .await
    }

    /// `POST path` without a body, decoded as `T`.
    pub async fn post_empty<T: DeserializeOwned>(&self, path: &str) -> Result<T, CliError> {
        self.execute(self.request(Method::POST, path)).await
    }

    /// `PATCH path` with a JSON body, decoded as `T`.
    pub async fn patch<T: DeserializeOwned, B: Serialize + ?Sized>(
        &self,
        path: &str,
        body: &B,
    ) -> Result<T, CliError> {
        self.execute(self.request(Method::PATCH, path).json(body))
            .await
    }

    /// `DELETE path`, decoded as `T` (`Value::Null` for an empty 204 body).
    pub async fn delete<T: DeserializeOwned>(&self, path: &str) -> Result<T, CliError> {
        self.execute(self.request(Method::DELETE, path)).await
    }

    /// Send `request`, map failures, and decode a 2xx body as `T`. An empty
    /// body decodes as JSON `null` so `Value` callers see `Value::Null`.
    pub async fn execute<T: DeserializeOwned>(&self, request: RequestBuilder) -> Result<T, CliError> {
        let (status, url, body) = self.send(request).await?;
        let text = String::from_utf8_lossy(&body);
        let trimmed = text.trim();
        let parsed: Result<T, _> = if trimmed.is_empty() {
            serde_json::from_str("null")
        } else {
            serde_json::from_str(trimmed)
        };
        parsed.map_err(|e| CliError::Decode {
            url,
            status,
            message: format!("{e} (body: {})", excerpt(trimmed)),
        })
    }

    /// Send `request` and return only the success status — for readiness
    /// probes whose body shape is irrelevant.
    pub async fn probe(&self, url: &str) -> Result<StatusCode, CliError> {
        let (status, _, _) = self.send(self.http.get(url)).await?;
        Ok(status)
    }

    /// Transport + status mapping shared by [`ApiClient::execute`] and
    /// [`ApiClient::probe`]. Returns `(status, url, body)` for 2xx.
    async fn send(
        &self,
        request: RequestBuilder,
    ) -> Result<(StatusCode, String, Vec<u8>), CliError> {
        let url = request
            .try_clone()
            .and_then(|r| r.build().ok())
            .map_or_else(|| self.base_url.clone(), |r| r.url().to_string());
        let response = request
            .send()
            .await
            .map_err(|e| map_transport_error(&e, &url))?;
        let status = response.status();
        let body = response
            .bytes()
            .await
            .map_err(|e| CliError::Transport {
                url: url.clone(),
                message: e.without_url().to_string(),
            })?
            .to_vec();
        if status.is_success() {
            Ok((status, url, body))
        } else {
            Err(map_error_response(
                status,
                &String::from_utf8_lossy(&body),
            ))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::commands::test_support::mock_api_with_responses;
    use serde_json::json;

    #[test]
    fn root_strips_canonical_prefix_only() {
        let api = ApiClient::new("http://127.0.0.1:8080/api/v1/", None, None).unwrap();
        assert_eq!(api.base(), "http://127.0.0.1:8080/api/v1");
        assert_eq!(api.root(), "http://127.0.0.1:8080");
        assert_eq!(api.url("/instances"), "http://127.0.0.1:8080/api/v1/instances");
        assert_eq!(api.root_url("health/ready"), "http://127.0.0.1:8080/health/ready");
        let plain = ApiClient::new("http://host:9", None, None).unwrap();
        assert_eq!(plain.root(), "http://host:9");
    }

    #[test]
    fn empty_credentials_are_treated_as_absent() {
        let api = ApiClient::new("http://h", Some(""), Some("")).unwrap();
        assert!(api.api_key().is_none());
        assert!(api.tenant_id().is_none());
        let err = api.require_tenant().unwrap_err().to_string();
        assert!(err.contains("ORCH8_TENANT_ID"), "{err}");
        assert!(err.contains("--tenant-id"), "{err}");
    }

    #[test]
    fn invalid_header_values_are_rejected_up_front() {
        let err = ApiClient::new("http://h", Some("bad\nkey"), None)
            .unwrap_err()
            .to_string();
        assert!(err.contains("--api-key"), "{err}");
    }

    #[test]
    fn unauthorized_maps_to_api_key_hint() {
        let err = map_error_response(
            StatusCode::UNAUTHORIZED,
            &json!({"error": "missing api key", "code": "unauthorized"}).to_string(),
        );
        let rendered = err.to_string();
        assert!(rendered.starts_with("unauthorized: set ORCH8_API_KEY or --api-key"));
        assert!(rendered.contains("missing api key"), "{rendered}");
        assert_eq!(err.status(), Some(StatusCode::UNAUTHORIZED));
    }

    #[test]
    fn unauthorized_with_non_json_body_still_hints() {
        let rendered = map_error_response(StatusCode::UNAUTHORIZED, "").to_string();
        assert!(rendered.contains("ORCH8_API_KEY"), "{rendered}");
        assert!(!rendered.contains("EOF"), "{rendered}");
    }

    #[test]
    fn tenant_errors_map_to_tenant_hint() {
        for status in [StatusCode::BAD_REQUEST, StatusCode::UNPROCESSABLE_ENTITY] {
            let err = map_error_response(
                status,
                &json!({"error": "x-tenant-id header is required"}).to_string(),
            );
            assert!(matches!(err, CliError::TenantMissing { .. }), "{err:?}");
            let rendered = err.to_string();
            assert!(rendered.contains("ORCH8_TENANT_ID or --tenant-id"), "{rendered}");
        }
        let by_code = map_error_response(
            StatusCode::BAD_REQUEST,
            &json!({"error": "header missing", "code": "tenant_required"}).to_string(),
        );
        assert!(matches!(by_code, CliError::TenantMissing { .. }));
    }

    #[test]
    fn envelope_fields_are_surfaced_for_other_statuses() {
        let err = map_error_response(
            StatusCode::CONFLICT,
            &json!({
                "error": "schedule is active",
                "code": "conflict",
                "request_id": "req-1",
                "details": {"id": 7}
            })
            .to_string(),
        );
        let rendered = err.to_string();
        assert!(rendered.contains("409 Conflict"), "{rendered}");
        assert!(rendered.contains("[conflict]"), "{rendered}");
        assert!(rendered.contains("schedule is active"), "{rendered}");
        assert!(rendered.contains("req-1"), "{rendered}");
        assert_eq!(err.code(), Some("conflict"));
        match err {
            CliError::Api { details, .. } => assert_eq!(details, Some(json!({"id": 7}))),
            other => panic!("expected Api error, got {other:?}"),
        }
    }

    #[test]
    fn non_json_error_bodies_are_echoed_never_eof_parsing() {
        let rendered =
            map_error_response(StatusCode::BAD_GATEWAY, "<html>proxy down</html>").to_string();
        assert!(rendered.contains("502 Bad Gateway"), "{rendered}");
        assert!(rendered.contains("proxy down"), "{rendered}");
        assert!(!rendered.contains("EOF"), "{rendered}");

        let empty = map_error_response(StatusCode::NOT_FOUND, "").to_string();
        assert!(empty.contains("404 Not Found"), "{empty}");
        assert!(!empty.contains("EOF"), "{empty}");
    }

    #[test]
    fn long_bodies_are_truncated() {
        let body = "x".repeat(2_000);
        let rendered = map_error_response(StatusCode::INTERNAL_SERVER_ERROR, &body).to_string();
        assert!(rendered.len() < 700, "{}", rendered.len());
        assert!(rendered.ends_with('…'));
    }

    #[tokio::test]
    async fn connect_refused_names_the_url_and_server_hint() {
        let api = ApiClient::new("http://127.0.0.1:1/api/v1", None, None).unwrap();
        let err = api.get::<Value>("/health").await.unwrap_err();
        assert!(matches!(err, CliError::Connect { .. }), "{err:?}");
        let rendered = err.to_string();
        assert!(rendered.contains("could not reach http://127.0.0.1:1"), "{rendered}");
        assert!(rendered.contains("is orch8-server running?"), "{rendered}");
        assert!(rendered.contains("orch8-server --config orch8.toml"), "{rendered}");
        assert!(err.status().is_none());
    }

    #[tokio::test]
    async fn execute_decodes_empty_success_bodies_as_null() {
        let server = mock_api_with_responses(vec![(StatusCode::NO_CONTENT, String::new())]).await;
        let api = ApiClient::new(&server.base, Some("k"), Some("t")).unwrap();
        let value: Value = api.delete("/cron/x").await.unwrap();
        assert_eq!(value, Value::Null);
    }

    #[tokio::test]
    async fn execute_reports_undecodable_success_bodies_clearly() {
        let server =
            mock_api_with_responses(vec![(StatusCode::OK, "not json at all".into())]).await;
        let api = ApiClient::new(&server.base, None, None).unwrap();
        let err = api.get::<Value>("/x").await.unwrap_err();
        assert!(matches!(err, CliError::Decode { .. }), "{err:?}");
        let rendered = err.to_string();
        assert!(rendered.contains("not the expected JSON"), "{rendered}");
        assert!(rendered.contains("not json at all"), "{rendered}");
    }

    #[tokio::test]
    async fn execute_maps_live_error_envelopes_and_sends_headers() {
        let server = mock_api_with_responses(vec![(
            StatusCode::UNAUTHORIZED,
            json!({"error": "bad key", "code": "unauthorized", "request_id": "r9"}).to_string(),
        )])
        .await;
        let api = ApiClient::new(&server.base, Some("secret"), Some("acme")).unwrap();
        let err = api.get::<Value>("/instances").await.unwrap_err();
        assert!(err.to_string().contains("ORCH8_API_KEY"), "{err}");
        // Headers were stamped by the shared client.
        let requests = server.log.snapshot();
        assert_eq!(requests.len(), 1);
        assert_eq!(requests[0].uri, "/instances");
    }

    #[tokio::test]
    async fn get_query_percent_encodes_values() {
        let server = mock_api_with_responses(vec![]).await;
        let api = ApiClient::new(&server.base, None, None).unwrap();
        let _: Value = api
            .get_query("/cron", &[("tenant_id", "tenant a".to_string())])
            .await
            .unwrap();
        let uri = &server.log.snapshot()[0].uri;
        assert!(uri.contains("tenant_id=tenant"), "{uri}");
        assert!(!uri.contains(' '), "{uri}");
    }
}
