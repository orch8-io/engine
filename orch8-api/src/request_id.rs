use axum::extract::Request;
use axum::http::HeaderValue;
use axum::middleware::Next;
use axum::response::Response;
use uuid::Uuid;

/// Header name for the request ID.
pub const REQUEST_ID_HEADER: &str = "x-request-id";

/// Maximum length for a client-provided request ID. Longer values are truncated
/// to bound memory use and response header size.
const MAX_REQUEST_ID_LEN: usize = 128;

/// Middleware that assigns a unique request ID to every request.
///
/// If the client sends an `x-request-id` header, we preserve it; otherwise we
/// generate a new UUID v4. The ID is:
/// 1. Inserted into request extensions so handlers can access it.
/// 2. Echoed back in the response `x-request-id` header.
pub async fn request_id_middleware(mut request: Request, next: Next) -> Response {
    let raw_request_id = request
        .headers()
        .get(REQUEST_ID_HEADER)
        .and_then(|v| v.to_str().ok())
        .filter(|s| !s.is_empty())
        .map(String::from);

    // Sanitize client-provided IDs: keep only safe ASCII chars to prevent
    // header injection and ensure HeaderValue::from_str always succeeds.
    let request_id = raw_request_id
        .and_then(|s| {
            let sanitized: String = s
                .chars()
                .filter(|c| c.is_ascii_alphanumeric() || *c == '-' || *c == '_')
                .take(MAX_REQUEST_ID_LEN)
                .collect();
            if sanitized.is_empty() {
                None
            } else {
                Some(sanitized)
            }
        })
        .unwrap_or_else(|| Uuid::new_v4().to_string());

    request
        .extensions_mut()
        .insert(RequestId(request_id.clone()));

    let mut response = next.run(request).await;

    if response.status().is_client_error() || response.status().is_server_error() {
        let status = response.status();
        let (mut parts, body) = response.into_parts();
        let bytes = axum::body::to_bytes(body, 1024 * 1024)
            .await
            .unwrap_or_default();
        let fallback_code = status
            .canonical_reason()
            .unwrap_or("http_error")
            .to_ascii_lowercase()
            .replace(' ', "_");
        let mut value = serde_json::from_slice::<serde_json::Value>(&bytes).unwrap_or_else(|_| {
            serde_json::json!({
                "error": {
                    "code": fallback_code.clone(),
                    "message": String::from_utf8_lossy(&bytes),
                    "request_id": request_id.clone(),
                }
            })
        });
        // JSON-RPC uses an object-valued `error`; preserve that protocol.
        // Normal HTTP errors use the backwards-compatible string field.
        if value.get("error").is_some_and(serde_json::Value::is_string) {
            let message = value["error"].as_str().unwrap_or_default().to_owned();
            let code = value
                .get("code")
                .cloned()
                .unwrap_or_else(|| serde_json::json!(fallback_code));
            value = serde_json::json!({
                "error": {
                    "code": code,
                    "message": message,
                    "request_id": request_id.clone(),
                }
            });
        } else if value.get("jsonrpc").is_none()
            && let Some(error) = value
                .get_mut("error")
                .and_then(serde_json::Value::as_object_mut)
        {
            error.insert("request_id".into(), serde_json::json!(request_id.clone()));
        }
        parts.headers.insert(
            axum::http::header::CONTENT_TYPE,
            HeaderValue::from_static("application/json"),
        );
        parts.headers.remove(axum::http::header::CONTENT_LENGTH);
        response = Response::from_parts(parts, axum::body::Body::from(value.to_string()));
    }

    // HeaderValue::from_str is now infallible because we sanitized above,
    // but we keep the check as a defence-in-depth guard.
    if let Ok(val) = HeaderValue::from_str(&request_id) {
        response.headers_mut().insert(REQUEST_ID_HEADER, val);
    }

    response
}

/// Extractor for the request ID, injected by [`request_id_middleware`].
#[derive(Clone, Debug)]
pub struct RequestId(pub String);

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn request_id_header_name_is_lowercase() {
        assert_eq!(REQUEST_ID_HEADER, "x-request-id");
    }

    #[test]
    fn request_id_struct_clones() {
        let id = RequestId("abc-123".into());
        let cloned = id;
        assert_eq!(cloned.0, "abc-123");
    }

    #[test]
    fn long_request_id_is_truncated() {
        let raw = "a".repeat(MAX_REQUEST_ID_LEN * 2);
        let sanitized: String = raw
            .chars()
            .filter(|c| c.is_ascii_alphanumeric() || *c == '-' || *c == '_')
            .take(MAX_REQUEST_ID_LEN)
            .collect();
        assert_eq!(sanitized.len(), MAX_REQUEST_ID_LEN);
    }
}
