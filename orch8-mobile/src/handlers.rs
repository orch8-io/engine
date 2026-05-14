use std::sync::Arc;
use std::time::Duration;

use serde_json::Value;
use tracing::warn;

use orch8_engine::handlers::{HandlerRegistry, StepContext};
use orch8_types::error::StepError;

use crate::error::HandlerError;

/// Callback interface for host-registered step handlers.
/// Implementations live in Swift/Kotlin and are called by the engine during tick execution.
#[uniffi::export(with_foreign)]
pub trait StepHandler: Send + Sync {
    fn execute(&self, step_name: String, input: String) -> Result<String, HandlerError>;
}

/// Callback interface for engine lifecycle events.
/// The host app receives notifications when instances complete, fail, or have pending steps.
#[uniffi::export(with_foreign)]
pub trait EngineListener: Send + Sync {
    fn on_instance_completed(&self, instance_id: String, output: String);
    fn on_instance_failed(&self, instance_id: String, error: String);
    fn on_step_pending(&self, instance_id: String, step_name: String, handler: String);
}

/// Register a foreign `StepHandler` into the engine's `HandlerRegistry`.
/// Bridges the `UniFFI` callback interface to the engine's async handler signature.
/// If the handler does not respond within `timeout`, the step returns a retryable
/// error (which causes the engine to transition the instance to Waiting).
pub(crate) fn register_foreign_handler(
    registry: &mut HandlerRegistry,
    name: &str,
    handler: Arc<dyn StepHandler>,
    timeout: Duration,
) {
    let handler_name = name.to_string();
    registry.register(name, move |ctx: StepContext| {
        let handler = Arc::clone(&handler);
        let name = handler_name.clone();
        async move {
            let input = serde_json::to_string(&ctx.params).unwrap_or_else(|_| "{}".to_string());

            let handler_clone = Arc::clone(&handler);
            let name_clone = name.clone();
            let input_clone = input.clone();

            let result = tokio::time::timeout(timeout, async {
                tokio::task::spawn_blocking(move || handler_clone.execute(name_clone, input_clone))
                    .await
                    .map_err(|e| HandlerError::Permanent {
                        message: format!("handler task panicked: {e}"),
                    })?
            })
            .await;

            match result {
                Ok(Ok(output_json)) => {
                    let value: Value = serde_json::from_str(&output_json).unwrap_or_else(|_| {
                        warn!(output = %output_json, "handler returned non-JSON string, wrapping");
                        Value::String(output_json)
                    });
                    Ok(value)
                }
                Ok(Err(HandlerError::Retryable { message })) => Err(StepError::Retryable {
                    message,
                    details: None,
                }),
                Ok(Err(HandlerError::Permanent { message })) => Err(StepError::Permanent {
                    message,
                    details: None,
                }),
                Err(_elapsed) => {
                    warn!(handler = %name, "handler timed out, step will transition to Waiting");
                    Err(StepError::Retryable {
                        message: format!(
                            "handler '{name}' timed out after {}ms",
                            timeout.as_millis()
                        ),
                        details: None,
                    })
                }
            }
        }
    });
}
