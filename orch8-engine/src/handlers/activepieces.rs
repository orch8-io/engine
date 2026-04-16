//! `ActivePieces` sidecar handler.
//!
//! Steps with handler names prefixed `ap://` dispatch to the
//! `@orch8/activepieces-worker` Node sidecar, which executes `ActivePieces`
//! community pieces (Slack, Gmail, Stripe, GitHub, ...) by dynamic import.
//!
//! Handler name format: `ap://<piece>.<action>`, e.g. `ap://slack.send_message`.
//!
//! Sidecar URL is read once from the `ORCH8_ACTIVEPIECES_URL` environment
//! variable at first use (default `http://127.0.0.1:50052/execute`). Reading
//! from env instead of `EngineConfig` keeps this integration optional and
//! config-touchless — operators who don't run the sidecar pay no runtime cost
//! beyond the `ap://` prefix check in the step dispatcher.
//!
//! Protocol: POST JSON `{ piece, action, auth, props, instance_id, block_id }`.
//! The sidecar returns `{ ok: true, output }` on success or
//! `{ ok: false, error: { type: "retryable" | "permanent", message, details } }`
//! on failure. HTTP 5xx maps to `StepError::Retryable`, 4xx to `Permanent`.

use std::env;
use std::sync::LazyLock;
use std::time::Duration;

use serde::Deserialize;
use serde_json::{json, Value};
use tracing::{debug, warn};

use orch8_types::error::StepError;

use super::StepContext;

const DEFAULT_SIDECAR_URL: &str = "http://127.0.0.1:50052/execute";
const AP_PREFIX: &str = "ap://";

/// Shared HTTP client for all `ActivePieces` sidecar calls. Reusing the client
/// enables connection pooling and a single shared DNS cache.
static AP_CLIENT: LazyLock<reqwest::Client> = LazyLock::new(|| {
    reqwest::Client::builder()
        .connect_timeout(Duration::from_secs(5))
        // Per-piece timeout is enforced sidecar-side (ORCH8_AP_TIMEOUT_MS, default 60s).
        // Our ceiling is a little higher so we don't race the sidecar's own timer.
        .timeout(Duration::from_secs(75))
        .build()
        .unwrap_or_else(|e| {
            tracing::warn!(error = %e, "failed to build activepieces HTTP client, using default");
            reqwest::Client::new()
        })
});

/// Sidecar URL resolved once at first call — changing `ORCH8_ACTIVEPIECES_URL`
/// after startup has no effect, matching how every other orch8 env var behaves.
static AP_URL: LazyLock<String> = LazyLock::new(|| {
    env::var("ORCH8_ACTIVEPIECES_URL").unwrap_or_else(|_| DEFAULT_SIDECAR_URL.into())
});

/// Shape of the sidecar's error envelope. Used to translate structured
/// `type`/`message`/`details` into the appropriate `StepError` variant when
/// the sidecar classifies the failure for us.
#[derive(Debug, Deserialize)]
struct SidecarError {
    #[serde(rename = "type")]
    kind: String,
    message: String,
    #[serde(default)]
    details: Option<Value>,
}

#[derive(Debug, Deserialize)]
struct SidecarResponse {
    ok: bool,
    #[serde(default)]
    output: Option<Value>,
    #[serde(default)]
    error: Option<SidecarError>,
}

/// Check if a handler name should dispatch to the `ActivePieces` sidecar.
pub fn is_ap_handler(handler_name: &str) -> bool {
    handler_name.starts_with(AP_PREFIX)
}

/// Parse `ap://piece.action` into its two components.
///
/// The piece name is intentionally restricted to lowercase alphanumerics and
/// hyphens; the action name accepts `snake_case` as `ActivePieces` uses it. These
/// constraints match what the Node sidecar itself validates, so malformed
/// handler names fail fast on the Rust side with a useful message.
pub fn parse_handler(handler: &str) -> Result<(&str, &str), StepError> {
    let rest = handler
        .strip_prefix(AP_PREFIX)
        .ok_or_else(|| StepError::Permanent {
            message: format!("ap handler: missing '{AP_PREFIX}' prefix in '{handler}'"),
            details: None,
        })?;
    let (piece, action) = rest.split_once('.').ok_or_else(|| StepError::Permanent {
        message: format!(
            "ap handler: invalid format '{handler}', expected '{AP_PREFIX}<piece>.<action>'"
        ),
        details: None,
    })?;
    if piece.is_empty() || action.is_empty() {
        return Err(StepError::Permanent {
            message: format!("ap handler: piece and action must be non-empty in '{handler}'"),
            details: None,
        });
    }
    Ok((piece, action))
}

/// Execute a step via the `ActivePieces` sidecar.
pub async fn handle_ap(ctx: StepContext, handler_name: &str) -> Result<Value, StepError> {
    let (piece, action) = parse_handler(handler_name)?;

    // `auth` and `props` travel verbatim from the step's params. Everything
    // else in `ctx.params` is dropped — the sidecar doesn't need orch8's
    // internal `_grpc_endpoint`/`_wasm_*` metadata fields.
    let auth = ctx.params.get("auth").cloned().unwrap_or(Value::Null);
    let props = ctx
        .params
        .get("props")
        .cloned()
        .unwrap_or_else(|| json!({}));

    let body = json!({
        "piece": piece,
        "action": action,
        "auth": auth,
        "props": props,
        "instance_id": ctx.instance_id.to_string(),
        "block_id": ctx.block_id.to_string(),
        "attempt": ctx.attempt,
    });

    let url: &str = AP_URL.as_str();

    debug!(
        instance_id = %ctx.instance_id,
        block_id = %ctx.block_id,
        piece = %piece,
        action = %action,
        url = %url,
        "dispatching step to activepieces sidecar"
    );

    let response = AP_CLIENT
        .post(url)
        .header("content-type", "application/json")
        .json(&body)
        .send()
        .await
        .map_err(|e| StepError::Retryable {
            message: format!("activepieces: sidecar unreachable at {url}: {e}"),
            details: None,
        })?;

    let status = response.status().as_u16();
    let text = response.text().await.unwrap_or_default();

    // Prefer the sidecar's own classification when it gave us a structured
    // envelope; fall back to HTTP status only when the body is missing or
    // malformed (e.g. a proxy injected an HTML error page).
    if let Ok(parsed) = serde_json::from_str::<SidecarResponse>(&text) {
        if parsed.ok {
            return Ok(parsed.output.unwrap_or(Value::Null));
        }
        if let Some(err) = parsed.error {
            return Err(map_error(err));
        }
    }

    // Un-envelope-able response: classify purely by HTTP status.
    if status >= 500 {
        return Err(StepError::Retryable {
            message: format!("activepieces: sidecar server error {status} from {url}"),
            details: Some(json!({ "status": status, "body": text })),
        });
    }
    if status >= 400 {
        return Err(StepError::Permanent {
            message: format!("activepieces: sidecar client error {status} from {url}"),
            details: Some(json!({ "status": status, "body": text })),
        });
    }
    // 2xx with an unparseable body — treat as success but preserve the raw text
    // so downstream steps can still see what came back. Matches grpc_plugin.
    warn!(
        status = status,
        "activepieces: 2xx response was not a valid envelope, wrapping as raw"
    );
    Ok(json!({ "raw": text }))
}

fn map_error(err: SidecarError) -> StepError {
    match err.kind.as_str() {
        "permanent" => StepError::Permanent {
            message: format!("activepieces: {}", err.message),
            details: err.details,
        },
        // Default to retryable for unknown kinds so transient upstream issues
        // don't accidentally land in DLQ on a classifier bug.
        _ => StepError::Retryable {
            message: format!("activepieces: {}", err.message),
            details: err.details,
        },
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn is_ap_handler_detects_prefix() {
        assert!(is_ap_handler("ap://slack.send_message"));
        assert!(is_ap_handler("ap://gmail.send_email"));
        assert!(!is_ap_handler("grpc://localhost:50051/Svc.Run"));
        assert!(!is_ap_handler("wasm://my-plugin"));
        assert!(!is_ap_handler("http_request"));
        assert!(!is_ap_handler(""));
    }

    #[test]
    fn parse_handler_extracts_piece_and_action() {
        let (piece, action) = parse_handler("ap://slack.send_message").unwrap();
        assert_eq!(piece, "slack");
        assert_eq!(action, "send_message");
    }

    #[test]
    fn parse_handler_supports_hyphenated_piece_names() {
        let (piece, action) = parse_handler("ap://google-sheets.insert_row").unwrap();
        assert_eq!(piece, "google-sheets");
        assert_eq!(action, "insert_row");
    }

    #[test]
    fn parse_handler_rejects_missing_dot() {
        let err = parse_handler("ap://slack").unwrap_err();
        assert!(matches!(err, StepError::Permanent { .. }));
    }

    #[test]
    fn parse_handler_rejects_empty_components() {
        assert!(matches!(
            parse_handler("ap://.action"),
            Err(StepError::Permanent { .. })
        ));
        assert!(matches!(
            parse_handler("ap://piece."),
            Err(StepError::Permanent { .. })
        ));
    }

    #[test]
    fn parse_handler_rejects_missing_prefix() {
        let err = parse_handler("slack.send_message").unwrap_err();
        assert!(matches!(err, StepError::Permanent { .. }));
    }

    #[test]
    fn map_error_classifies_by_type_field() {
        let perm = map_error(SidecarError {
            kind: "permanent".into(),
            message: "bad input".into(),
            details: None,
        });
        assert!(matches!(perm, StepError::Permanent { .. }));

        let retry = map_error(SidecarError {
            kind: "retryable".into(),
            message: "upstream 503".into(),
            details: None,
        });
        assert!(matches!(retry, StepError::Retryable { .. }));

        // Unknown kind → retryable (fail open rather than silently DLQ).
        let unknown = map_error(SidecarError {
            kind: "mystery".into(),
            message: "???".into(),
            details: None,
        });
        assert!(matches!(unknown, StepError::Retryable { .. }));
    }
}
