//! Built-in `human_review` handler — prepare a review request and optionally notify.
//!
//! Designed to be paired with `wait_for_input` on the step definition. The typical
//! pattern is:
//!
//! ```json
//! {
//!   "id": "review_step",
//!   "handler": "human_review",
//!   "params": {
//!     "review_data": "{{steps.llm_step.message.content}}",
//!     "instructions": "Check this LLM output for accuracy",
//!     "reviewer": "team-lead",
//!     "notify_url": "https://hooks.slack.com/..."
//!   },
//!   "wait_for_input": {
//!     "signal_name": "review_decision",
//!     "timeout_secs": 3600,
//!     "on_timeout": { "handler": "notify_escalation" }
//!   }
//! }
//! ```
//!
//! ## Flow
//! 1. Engine reaches this step → `wait_for_input` pauses the instance
//! 2. `human_review` handler runs, sends notification, returns review context
//! 3. Human submits review via `POST /instances/{id}/signals`
//! 4. Instance resumes with review decision in context
//!
//! ## Params
//!
//! | Field | Type | Default | Description |
//! |-------|------|---------|-------------|
//! | `review_data` | any | `null` | Data to review (LLM output, generated content, etc.) |
//! | `instructions` | string | `""` | Instructions for the reviewer |
//! | `reviewer` | string | `"unassigned"` | Reviewer identifier |
//! | `notify_url` | string | — | Webhook URL to notify about pending review |
//! | `notify_headers` | object | `{}` | Extra headers for notification webhook |

use serde_json::{json, Value};
use tracing::{debug, warn};

use orch8_types::error::StepError;

use super::StepContext;

pub async fn handle_human_review(ctx: StepContext) -> Result<Value, StepError> {
    let review_data = ctx.params.get("review_data").cloned().unwrap_or(Value::Null);
    let instructions = ctx
        .params
        .get("instructions")
        .and_then(Value::as_str)
        .unwrap_or("");
    let reviewer = ctx
        .params
        .get("reviewer")
        .and_then(Value::as_str)
        .unwrap_or("unassigned");

    debug!(
        instance_id = %ctx.instance_id,
        block_id = %ctx.block_id,
        reviewer = %reviewer,
        "human_review: pending"
    );

    // Send notification if configured.
    if let Some(notify_url) = ctx.params.get("notify_url").and_then(Value::as_str) {
        if !super::builtin::is_url_safe(notify_url) {
            return Err(StepError::Permanent {
                message: "blocked: URL targets a private/internal network address".into(),
                details: None,
            });
        }

        let payload = json!({
            "type": "human_review_pending",
            "instance_id": ctx.instance_id.0.to_string(),
            "block_id": ctx.block_id.0,
            "reviewer": reviewer,
            "instructions": instructions,
            "review_data": review_data,
        });

        let client = super::llm::http_client();
        let mut req = client
            .post(notify_url)
            .header("Content-Type", "application/json")
            .timeout(std::time::Duration::from_secs(10))
            .json(&payload);

        if let Some(headers) = ctx.params.get("notify_headers").and_then(Value::as_object) {
            for (k, v) in headers {
                if let Some(val) = v.as_str() {
                    req = req.header(k.as_str(), val);
                }
            }
        }

        if let Err(e) = req.send().await {
            warn!(
                url = %notify_url,
                error = %e,
                "human_review: notification failed (non-blocking)"
            );
        }
    }

    Ok(json!({
        "type": "human_review",
        "status": "pending",
        "reviewer": reviewer,
        "instructions": instructions,
        "review_data": review_data,
        "instance_id": ctx.instance_id.0.to_string(),
        "block_id": ctx.block_id.0,
    }))
}

#[cfg(test)]
mod tests {
    use super::*;
    use orch8_types::context::ExecutionContext;
    use orch8_types::ids::{BlockId, InstanceId};

    #[tokio::test]
    async fn returns_review_context() {
        let ctx = StepContext {
            instance_id: InstanceId::new(),
            block_id: BlockId("review".into()),
            params: json!({
                "review_data": {"text": "LLM generated this"},
                "instructions": "Check for accuracy",
                "reviewer": "alice",
            }),
            context: ExecutionContext::default(),
            attempt: 0,
        };
        let result = handle_human_review(ctx).await.unwrap();
        assert_eq!(result["type"], "human_review");
        assert_eq!(result["status"], "pending");
        assert_eq!(result["reviewer"], "alice");
        assert_eq!(result["instructions"], "Check for accuracy");
    }

    #[tokio::test]
    async fn defaults_when_minimal_params() {
        let ctx = StepContext {
            instance_id: InstanceId::new(),
            block_id: BlockId("r".into()),
            params: json!({}),
            context: ExecutionContext::default(),
            attempt: 0,
        };
        let result = handle_human_review(ctx).await.unwrap();
        assert_eq!(result["reviewer"], "unassigned");
        assert_eq!(result["status"], "pending");
    }
}
