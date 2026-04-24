use axum::extract::Request;
use axum::http::HeaderValue;
use axum::middleware::Next;
use axum::response::Response;
use uuid::Uuid;

/// Header name for the request ID.
pub const REQUEST_ID_HEADER: &str = "x-request-id";

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
}
