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

            let name_for_call = name.clone();

            let result = tokio::time::timeout(timeout, async move {
                tokio::task::spawn_blocking(move || handler.execute(name_for_call, input))
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

#[cfg(test)]
mod tests {
    use std::sync::Mutex;

    use orch8_storage::sqlite::SqliteStorage;
    use orch8_types::context::ExecutionContext;
    use orch8_types::ids::{BlockId, InstanceId, TenantId};

    use super::*;

    struct RecordingHandler {
        result: Result<String, HandlerError>,
        calls: Arc<Mutex<Vec<(String, String)>>>,
    }

    impl StepHandler for RecordingHandler {
        fn execute(&self, step_name: String, input: String) -> Result<String, HandlerError> {
            self.calls.lock().unwrap().push((step_name, input));
            match &self.result {
                Ok(output) => Ok(output.clone()),
                Err(HandlerError::Retryable { message }) => Err(HandlerError::Retryable {
                    message: message.clone(),
                }),
                Err(HandlerError::Permanent { message }) => Err(HandlerError::Permanent {
                    message: message.clone(),
                }),
            }
        }
    }

    struct PanickingHandler;

    impl StepHandler for PanickingHandler {
        fn execute(&self, _step_name: String, _input: String) -> Result<String, HandlerError> {
            panic!("foreign callback panic")
        }
    }

    struct SlowHandler;

    impl StepHandler for SlowHandler {
        fn execute(&self, _step_name: String, _input: String) -> Result<String, HandlerError> {
            std::thread::sleep(Duration::from_millis(100));
            Ok("{}".into())
        }
    }

    async fn context(params: Value) -> StepContext {
        StepContext {
            instance_id: InstanceId::new(),
            tenant_id: TenantId::unchecked("mobile"),
            block_id: BlockId::new("foreign-step"),
            params,
            context: Arc::new(ExecutionContext::default()),
            attempt: 1,
            storage: Arc::new(SqliteStorage::in_memory().await.unwrap()),
            wait_for_input: None,
        }
    }

    async fn invoke(handler: Arc<dyn StepHandler>, timeout: Duration) -> Result<Value, StepError> {
        let mut registry = HandlerRegistry::new();
        register_foreign_handler(&mut registry, "foreign", handler, timeout);
        registry.get("foreign").unwrap()(context(serde_json::json!({ "n": 7 })).await).await
    }

    #[tokio::test]
    async fn foreign_handler_receives_name_and_json_params() {
        let calls = Arc::new(Mutex::new(Vec::new()));
        let output = invoke(
            Arc::new(RecordingHandler {
                result: Ok(r#"{"accepted":true}"#.into()),
                calls: Arc::clone(&calls),
            }),
            Duration::from_secs(1),
        )
        .await
        .unwrap();

        assert_eq!(output, serde_json::json!({ "accepted": true }));
        assert_eq!(
            calls.lock().unwrap().as_slice(),
            &[("foreign".into(), r#"{"n":7}"#.into())]
        );
    }

    #[tokio::test]
    async fn non_json_foreign_output_is_preserved_as_a_string() {
        let output = invoke(
            Arc::new(RecordingHandler {
                result: Ok("plain text".into()),
                calls: Arc::new(Mutex::new(Vec::new())),
            }),
            Duration::from_secs(1),
        )
        .await
        .unwrap();

        assert_eq!(output, Value::String("plain text".into()));
    }

    #[tokio::test]
    async fn retryable_foreign_error_preserves_its_class_and_message() {
        let result = invoke(
            Arc::new(RecordingHandler {
                result: Err(HandlerError::Retryable {
                    message: "try later".into(),
                }),
                calls: Arc::new(Mutex::new(Vec::new())),
            }),
            Duration::from_secs(1),
        )
        .await;

        assert!(matches!(
            result,
            Err(StepError::Retryable { message, details: None }) if message == "try later"
        ));
    }

    #[tokio::test]
    async fn permanent_foreign_error_preserves_its_class_and_message() {
        let result = invoke(
            Arc::new(RecordingHandler {
                result: Err(HandlerError::Permanent {
                    message: "invalid request".into(),
                }),
                calls: Arc::new(Mutex::new(Vec::new())),
            }),
            Duration::from_secs(1),
        )
        .await;

        assert!(matches!(
            result,
            Err(StepError::Permanent { message, details: None }) if message == "invalid request"
        ));
    }

    #[tokio::test]
    async fn panicking_foreign_callback_becomes_a_permanent_step_error() {
        let result = invoke(Arc::new(PanickingHandler), Duration::from_secs(1)).await;

        assert!(matches!(
            result,
            Err(StepError::Permanent { message, details: None })
                if message.contains("handler task panicked")
        ));
    }

    #[tokio::test]
    async fn timed_out_foreign_callback_becomes_a_retryable_step_error() {
        let result = invoke(Arc::new(SlowHandler), Duration::from_millis(5)).await;

        assert!(matches!(
            result,
            Err(StepError::Retryable { message, details: None })
                if message.contains("handler 'foreign' timed out after 5ms")
        ));
    }
}
