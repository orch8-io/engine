use axum::{extract::Request, http::StatusCode, middleware::Next, response::Response};

pub async fn api_key_middleware(
    expected_key: String,
    request: Request,
    next: Next,
) -> Result<Response, StatusCode> {
    if expected_key.is_empty() {
        return Ok(next.run(request).await);
    }

    let provided = request
        .headers()
        .get("x-api-key")
        .and_then(|v| v.to_str().ok());

    match provided {
        Some(key) if key == expected_key => Ok(next.run(request).await),
        _ => Err(StatusCode::UNAUTHORIZED),
    }
}
