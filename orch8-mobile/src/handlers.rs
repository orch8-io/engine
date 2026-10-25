use std::sync::Arc;
use std::time::Duration;

use serde_json::Value;
use tracing::warn;

use orch8_engine::handlers::{HandlerRegistry, StepContext};
use orch8_types::error::StepError;

use crate::error::HandlerError;
use crate::stragglers::{Stragglers, device_timeout_error};

mod invocation;
use invocation::{NativeClaim, NativeInvocation};

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
/// error (which causes the engine to transition the instance to Waiting) marked
/// as a device timeout. The native call cannot be interrupted, so it is tracked
/// in `stragglers` until it returns (see [`crate::stragglers`]). Outer
/// cancellation also quarantines a live native call; queued calls are aborted.
pub(crate) fn register_foreign_handler(
    registry: &mut HandlerRegistry,
    name: &str,
    handler: Arc<dyn StepHandler>,
    timeout: Duration,
    stragglers: Arc<Stragglers>,
) {
    let handler_name = name.to_string();
    registry.register(name, move |ctx: StepContext| {
        let handler = Arc::clone(&handler);
        let name = handler_name.clone();
        let stragglers = Arc::clone(&stragglers);
        async move {
            // MC-06: a timed-out native call cannot be interrupted. A retry
            // dispatched by the local scheduler must not run concurrently with
            // that straggler (double side effects), so refuse to start and let
            // the engine's retry policy come back after it has returned.
            if stragglers.blocks(&name) {
                return Err(StepError::Retryable {
                    message: format!(
                        "handler '{name}' is still running from a previous unfinished attempt"
                    ),
                    details: None,
                });
            }
            let input = serde_json::to_string(&ctx.params).unwrap_or_else(|_| "{}".to_string());

            let claim = ctx.claim_token.map(|token| NativeClaim {
                storage: Arc::clone(&ctx.storage),
                instance_id: ctx.instance_id,
                tenant_id: ctx.tenant_id.clone(),
                token,
            });
            let mut call = NativeInvocation::spawn(handler, name.clone(), input, stragglers, claim);

            let result = match tokio::time::timeout(timeout, call.result()).await {
                Ok(joined) => joined.map_err(|e| StepError::Permanent {
                    message: format!("handler task panicked: {e}"),
                    details: None,
                }),
                Err(_elapsed) => {
                    warn!(handler = %name, "handler timed out; tracking any live native call");
                    return Err(device_timeout_error(format!(
                        "handler '{name}' timed out after {}ms",
                        timeout.as_millis()
                    )));
                }
            };

            match result {
                Ok(Ok(output_json)) => {
                    let value: Value = serde_json::from_str(&output_json).unwrap_or_else(|_| {
                        warn!(output = %output_json, "handler returned non-JSON string, wrapping");
                        Value::String(output_json)
                    });
                    Ok(value)
                }
                Ok(Err(error)) | Err(error) => Err(error),
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
            claim_token: None,
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
        register_foreign_handler(
            &mut registry,
            "foreign",
            handler,
            timeout,
            Arc::new(Stragglers::default()),
        );
        registry.get("foreign").unwrap()(context(serde_json::json!({ "n": 7 })).await).await
    }

    /// Blocks until `release` is set, so the test controls exactly how long
    /// the "native call" outlives its timeout (no wall-clock races).
    struct GatedHandler {
        calls: Arc<std::sync::atomic::AtomicU32>,
        release: Arc<std::sync::atomic::AtomicBool>,
    }

    impl StepHandler for GatedHandler {
        fn execute(&self, _step_name: String, _input: String) -> Result<String, HandlerError> {
            use std::sync::atomic::Ordering::SeqCst;
            self.calls.fetch_add(1, SeqCst);
            for _ in 0..1000 {
                if self.release.load(SeqCst) {
                    break;
                }
                std::thread::sleep(Duration::from_millis(5));
            }
            Ok("{}".into())
        }
    }

    #[tokio::test]
    async fn retry_is_refused_while_a_timed_out_call_is_still_running_mc06() {
        use std::sync::atomic::Ordering::SeqCst;
        let calls = Arc::new(std::sync::atomic::AtomicU32::new(0));
        let release = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let mut registry = HandlerRegistry::new();
        register_foreign_handler(
            &mut registry,
            "foreign",
            Arc::new(GatedHandler {
                calls: Arc::clone(&calls),
                release: Arc::clone(&release),
            }),
            Duration::from_millis(50),
            Arc::new(Stragglers::default()),
        );
        let run = registry.get("foreign").unwrap();
        let (c1, c2, c3) = (
            context(serde_json::json!({})).await,
            context(serde_json::json!({})).await,
            context(serde_json::json!({})).await,
        );

        let first = run(c1).await.unwrap_err();
        assert!(crate::stragglers::is_device_timeout(&first));

        // The scheduler retries immediately: the native call is still busy.
        let retry = run(c2).await.unwrap_err();
        assert!(
            matches!(retry, StepError::Retryable { .. })
                && !crate::stragglers::is_device_timeout(&retry),
            "{retry:?}"
        );
        assert_eq!(
            calls.load(SeqCst),
            1,
            "the straggler must not be invoked a second time concurrently"
        );

        // Once the straggler returns, the handler is runnable again.
        release.store(true, SeqCst);
        tokio::time::sleep(Duration::from_millis(300)).await;
        let _ = run(c3).await;
        assert_eq!(calls.load(SeqCst), 2);
    }

    struct CancellationGate {
        started: tokio::sync::Notify,
        released: (Mutex<bool>, std::sync::Condvar),
        panic_on_release: bool,
    }

    impl CancellationGate {
        fn release(&self) {
            *self.released.0.lock().unwrap() = true;
            self.released.1.notify_all();
        }
    }

    // Always unblock the native thread, including when a regression assertion
    // panics. Runtime teardown must never hang the test process.
    struct ReleaseOnDrop(Arc<CancellationGate>);
    impl Drop for ReleaseOnDrop {
        fn drop(&mut self) {
            self.0.release();
        }
    }

    impl StepHandler for CancellationGate {
        fn execute(&self, _name: String, _input: String) -> Result<String, HandlerError> {
            self.started.notify_one();
            let mut released = self.released.0.lock().unwrap();
            while !*released {
                released = self.released.1.wait(released).unwrap();
            }
            drop(released);
            assert!(
                !self.panic_on_release,
                "native callback panic after cancellation"
            );
            Ok("{}".into())
        }
    }

    async fn check_cancelled_foreign_future(panic_on_release: bool) {
        let gate = Arc::new(CancellationGate {
            started: tokio::sync::Notify::new(),
            released: (Mutex::new(false), std::sync::Condvar::new()),
            panic_on_release,
        });
        let _release = ReleaseOnDrop(Arc::clone(&gate));
        let stragglers = Arc::new(Stragglers::default());
        let mut registry = HandlerRegistry::new();
        register_foreign_handler(
            &mut registry,
            "foreign",
            gate.clone(),
            Duration::from_secs(60),
            Arc::clone(&stragglers),
        );
        let handler = registry.get("foreign").unwrap();
        let first = tokio::spawn(handler(context(serde_json::json!({})).await));
        tokio::time::timeout(Duration::from_secs(2), gate.started.notified())
            .await
            .unwrap();
        first.abort();
        assert!(first.await.unwrap_err().is_cancelled());
        assert!(
            stragglers.blocks("foreign"),
            "outer cancellation must quarantine the live native call"
        );
        let retry = handler(context(serde_json::json!({})).await)
            .await
            .unwrap_err();
        assert!(matches!(retry, StepError::Retryable { .. }));
        assert!(!crate::stragglers::is_device_timeout(&retry));
        gate.release();
        tokio::time::timeout(Duration::from_secs(2), async {
            while stragglers.blocks("foreign") {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        let next = handler(context(serde_json::json!({})).await).await;
        if panic_on_release {
            assert!(matches!(next, Err(StepError::Permanent { .. })));
        } else {
            assert!(next.is_ok());
        }
    }

    #[tokio::test]
    async fn cancelling_foreign_future_tracks_native_call_until_it_returns() {
        check_cancelled_foreign_future(false).await;
    }

    #[tokio::test]
    async fn cancelled_native_call_clears_quarantine_even_if_it_panics() {
        check_cancelled_foreign_future(true).await;
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

        let error = result.unwrap_err();
        assert!(crate::stragglers::is_device_timeout(&error));
        assert!(matches!(
            &error,
            StepError::Retryable { message, .. }
                if message.contains("handler 'foreign' timed out after 5ms")
        ));
    }

    #[tokio::test]
    async fn timed_out_foreign_callback_is_tracked_until_it_returns() {
        let stragglers = Arc::new(Stragglers::default());
        let mut registry = HandlerRegistry::new();
        register_foreign_handler(
            &mut registry,
            "foreign",
            Arc::new(SlowHandler),
            Duration::from_millis(5),
            Arc::clone(&stragglers),
        );
        let result = registry.get("foreign").unwrap()(context(serde_json::json!({})).await).await;
        assert!(result.is_err());
        assert!(
            stragglers.blocks("foreign"),
            "the native call is still running"
        );
        tokio::time::sleep(Duration::from_millis(300)).await;
        assert!(
            !stragglers.blocks("foreign"),
            "released once the call returned"
        );
    }
}

#[cfg(test)]
mod claim_entry_tests;
