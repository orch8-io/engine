use std::sync::Arc;
use std::time::Duration;

use serde_json::Value;
use tokio::sync::Semaphore;
use tokio_util::sync::CancellationToken;
use tracing::warn;

use orch8_engine::handlers::{HandlerRegistry, StepContext};
use orch8_types::error::StepError;

use crate::error::HandlerError;
use crate::stragglers::{Stragglers, device_timeout_error};

mod invocation;
mod pool;
use invocation::{InvocationPermit, NativeClaim, NativeInvocation, NativeOwnership};
// Internal scoped dispatcher; public scoped execution is still gated.
#[allow(dead_code)]
pub(crate) mod scoped;

const MAX_PROCESS_LEGACY_CALLS: usize = 32;

fn process_legacy_slots() -> Arc<Semaphore> {
    static SLOTS: std::sync::OnceLock<Arc<Semaphore>> = std::sync::OnceLock::new();
    Arc::clone(SLOTS.get_or_init(|| Arc::new(Semaphore::new(MAX_PROCESS_LEGACY_CALLS))))
}

// A polling hint only. Preparation still reserves authoritatively after the
// response, because another owner may take the last slot during network I/O.
pub(crate) fn legacy_capacity_available() -> bool {
    process_legacy_slots().available_permits() != 0
}

#[cfg(test)]
pub(crate) fn occupy_legacy_capacity_for_test() -> tokio::sync::OwnedSemaphorePermit {
    process_legacy_slots()
        .try_acquire_many_owned(u32::try_from(MAX_PROCESS_LEGACY_CALLS).unwrap())
        .unwrap()
}

struct Callback {
    handler: Arc<dyn StepHandler>,
    name: String,
    timeout: Duration,
    stragglers: Arc<Stragglers>,
    tracking_key: String,
    permits: Option<scoped::NativePermit>,
    legacy_capacity: Option<Arc<Semaphore>>,
    binding: Option<Arc<scoped::Binding>>,
    stop: CancellationToken,
    remote_lease: Option<orch8_engine::remote_worker::lease::LeaseAuthority>,
}

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

#[cfg(test)]
pub(crate) fn register_foreign_handler(
    registry: &mut HandlerRegistry,
    name: &str,
    handler: Arc<dyn StepHandler>,
    timeout: Duration,
    stragglers: Arc<Stragglers>,
) {
    register_owned_foreign_handler(
        registry,
        name,
        handler,
        timeout,
        stragglers,
        CancellationToken::new(),
    );
}

/// Register a foreign `StepHandler` into the engine's `HandlerRegistry`.
/// Bridges the `UniFFI` callback interface to the engine's async handler signature.
/// If the handler does not respond within `timeout`, the step returns a retryable
/// error (which causes the engine to transition the instance to Waiting) marked
/// as a device timeout. The native call cannot be interrupted, so it is tracked
/// in `stragglers` until it returns (see [`crate::stragglers`]). Outer
/// cancellation also quarantines a live native call; queued calls are aborted.
/// Local runtime shutdown refuses entry even while a durable claim is valid.
pub(crate) fn register_owned_foreign_handler(
    registry: &mut HandlerRegistry,
    name: &str,
    handler: Arc<dyn StepHandler>,
    timeout: Duration,
    stragglers: Arc<Stragglers>,
    stop: CancellationToken,
) {
    register_with_capacity(
        registry,
        name,
        handler,
        timeout,
        stragglers,
        stop,
        process_legacy_slots(),
    );
}

#[cfg(test)]
pub(crate) fn register_foreign_handler_with_capacity(
    registry: &mut HandlerRegistry,
    name: &str,
    handler: Arc<dyn StepHandler>,
    timeout: Duration,
    stragglers: Arc<Stragglers>,
    capacity: Arc<Semaphore>,
) {
    register_with_capacity(
        registry,
        name,
        handler,
        timeout,
        stragglers,
        CancellationToken::new(),
        capacity,
    );
}

fn register_with_capacity(
    registry: &mut HandlerRegistry,
    name: &str,
    handler: Arc<dyn StepHandler>,
    timeout: Duration,
    stragglers: Arc<Stragglers>,
    stop: CancellationToken,
    capacity: Arc<Semaphore>,
) {
    let handler_name = name.to_string();
    registry.register_lease_aware_prepared_with_deferred_effect_admission(
        name,
        move |ctx: StepContext, remote_lease| {
            prepare_foreign_handler(
                ctx,
                Callback {
                    handler: Arc::clone(&handler),
                    name: handler_name.clone(),
                    timeout,
                    stragglers: Arc::clone(&stragglers),
                    tracking_key: handler_name.clone(),
                    permits: None,
                    legacy_capacity: Some(capacity.clone()),
                    binding: None,
                    stop: stop.clone(),
                    remote_lease,
                },
            )
        },
    );
}

fn callback_busy(ctx: &StepContext, message: String) -> StepError {
    ctx.effect_admission.as_ref().map_or_else(
        || StepError::Retryable {
            message,
            details: None,
        },
        |admission| {
            admission.reject_preflight(orch8_engine::error::EngineError::AdmissionBusy {
                instance_id: ctx.instance_id,
            })
        },
    )
}

// Declaration order retains admission through foreign Arc destruction even
// when a worker cancels an unpolled prepared invocation.
struct PreparedCallback {
    callback: Callback,
    permits: Option<InvocationPermit>,
}

async fn invoke_foreign_handler(ctx: StepContext, callback: Callback) -> Result<Value, StepError> {
    prepare_foreign_handler(ctx, callback)?.await
}

fn prepare_foreign_handler(
    ctx: StepContext,
    mut callback: Callback,
) -> Result<impl std::future::Future<Output = Result<Value, StepError>> + Send, StepError> {
    if callback.stop.is_cancelled()
        || callback
            .remote_lease
            .as_ref()
            .is_some_and(|lease| lease.check().is_err())
    {
        return Err(StepError::ClaimLost);
    }
    let permits = if ctx.is_dry_run() {
        None
    } else {
        if callback.stragglers.blocks(&callback.tracking_key) {
            return Err(callback_busy(
                &ctx,
                format!(
                    "handler '{}' is still running from a previous unfinished attempt",
                    callback.name
                ),
            ));
        }
        match (callback.permits.take(), callback.legacy_capacity.take()) {
            (Some(scoped), None) => Some(InvocationPermit::Scoped { _permit: scoped }),
            (None, Some(capacity)) => Some(InvocationPermit::Legacy {
                _permit: capacity.try_acquire_owned().map_err(|_| {
                    callback_busy(&ctx, "native callback capacity is occupied".into())
                })?,
            }),
            _ => return Err(StepError::ClaimLost),
        }
    };
    Ok(invoke_reserved_handler(
        ctx,
        PreparedCallback { callback, permits },
    ))
}

async fn invoke_reserved_handler(
    ctx: StepContext,
    prepared: PreparedCallback,
) -> Result<Value, StepError> {
    let PreparedCallback { callback, permits } = prepared;
    let Callback {
        handler,
        name,
        timeout,
        stragglers,
        tracking_key,
        binding,
        stop,
        remote_lease,
        ..
    } = callback;
    if stop.is_cancelled() {
        return Err(StepError::ClaimLost);
    }
    if ctx.is_dry_run() {
        return Ok(
            serde_json::json!({ "dry_run": true, "handler": name, "would": "invoke registered handler" }),
        );
    }
    let input = serde_json::to_string(&ctx.params).unwrap_or_else(|_| "{}".to_string());

    let claim = ctx.claim_token.map(|token| NativeClaim {
        lifetime: binding.as_ref().map(|binding| binding.lifetime.clone()),
        storage: Arc::clone(&ctx.storage),
        instance_id: ctx.instance_id,
        tenant_id: ctx.tenant_id.clone(),
        token,
    });
    let mut call = NativeInvocation::spawn(
        handler,
        name.clone(),
        input,
        stragglers,
        NativeOwnership {
            claim,
            effect_admission: ctx.effect_admission,
            tracking_key,
            _permits: permits,
            binding,
            stop,
            remote_lease,
        },
    );

    let result = match tokio::time::timeout(timeout, call.result()).await {
        Ok(joined) => joined.map_err(|_error| StepError::Permanent {
            message: "handler task panicked".into(),
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
                warn!("handler returned non-JSON string, wrapping");
                Value::String(output_json)
            });
            Ok(value)
        }
        Ok(Err(error)) | Err(error) => Err(error),
    }
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
                Err(HandlerError::Retryable { detail: message }) => Err(HandlerError::Retryable {
                    detail: message.clone(),
                }),
                Err(HandlerError::Permanent { detail: message }) => Err(HandlerError::Permanent {
                    detail: message.clone(),
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
            effect_admission: None,
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

    #[derive(Clone)]
    struct CapturedEvents(Arc<Mutex<String>>);

    impl tracing::Subscriber for CapturedEvents {
        fn enabled(&self, _: &tracing::Metadata<'_>) -> bool {
            true
        }
        fn new_span(&self, _: &tracing::span::Attributes<'_>) -> tracing::span::Id {
            tracing::span::Id::from_u64(1)
        }
        fn record(&self, _: &tracing::span::Id, _: &tracing::span::Record<'_>) {}
        fn record_follows_from(&self, _: &tracing::span::Id, _: &tracing::span::Id) {}
        fn enter(&self, _: &tracing::span::Id) {}
        fn exit(&self, _: &tracing::span::Id) {}
        fn event(&self, event: &tracing::Event<'_>) {
            struct Fields<'a>(&'a mut String);
            impl tracing::field::Visit for Fields<'_> {
                fn record_debug(
                    &mut self,
                    field: &tracing::field::Field,
                    value: &dyn std::fmt::Debug,
                ) {
                    use std::fmt::Write;
                    writeln!(self.0, "{}={value:?}", field.name()).unwrap();
                }
            }
            event.record(&mut Fields(&mut self.0.lock().unwrap()));
        }
    }

    #[tokio::test]
    async fn non_json_native_output_is_not_copied_into_diagnostics() {
        use tracing::instrument::WithSubscriber;
        let canary = "PRIVATE_NATIVE_CALLBACK_OUTPUT_CANARY";
        let events = Arc::new(Mutex::new(String::new()));
        let dispatch = tracing::Dispatch::new(CapturedEvents(Arc::clone(&events)));
        for _ in 0..2 {
            let output = invoke(
                Arc::new(RecordingHandler {
                    result: Ok(canary.into()),
                    calls: Arc::new(Mutex::new(Vec::new())),
                }),
                Duration::from_secs(1),
            )
            .with_subscriber(dispatch.clone())
            .await
            .unwrap();
            assert_eq!(output, Value::String(canary.into()));
            if !events.lock().unwrap().is_empty() {
                break;
            }
            // Concurrent first callsite registration without a subscriber can
            // cache `never`. Match the engine's span-capture regression: rebuild
            // after that registration, then run a separate mock invocation.
            tracing::dispatcher::with_default(&dispatch, tracing::callsite::rebuild_interest_cache);
        }
        let events = events.lock().unwrap();
        assert!(
            events.contains("handler returned non-JSON string, wrapping"),
            "warning missing from captured events: {events}"
        );
        assert!(
            !events.contains(canary),
            "native payload leaked into diagnostics"
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
                    detail: "try later".into(),
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
                    detail: "invalid request".into(),
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

    #[tokio::test]
    async fn returned_panic_error_does_not_include_the_host_panic_payload() {
        struct Secret;
        impl StepHandler for Secret {
            fn execute(&self, _: String, _: String) -> Result<String, HandlerError> {
                panic!("NATIVE_PANIC_PRIVATE_CANARY");
            }
        }
        let result = invoke(Arc::new(Secret), Duration::from_secs(5)).await;
        let Err(StepError::Permanent { message, .. }) = result else {
            panic!("expected permanent panic error");
        };
        assert!(message.contains("handler task panicked"));
        assert!(!message.contains("NATIVE_PANIC_PRIVATE_CANARY"));
    }
}

#[cfg(test)]
mod claim_entry_tests;

#[cfg(test)]
mod process_capacity_tests;

#[cfg(test)]
mod lease_entry_tests;
