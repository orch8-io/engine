//! Coverage tests for the portable agent-runtime preset.
//!
//! Count contract: 7 independently named unit tests.

use super::*;
use orch8_types::ids::InstanceId;

#[test]
fn coverage_agent_001_builders_generate_distinct_runtime_ids() {
    let a = AgentRuntime::builder(Storage::sqlite_in_memory());
    let b = AgentRuntime::builder(Storage::sqlite_in_memory());
    assert_ne!(a.runtime_id, b.runtime_id);
}

#[test]
fn coverage_agent_002_runtime_id_override_is_retained() {
    let id = RuntimeId::new();
    let builder = AgentRuntime::builder(Storage::sqlite_in_memory()).runtime_id(id);
    assert_eq!(builder.runtime_id, id);
}

#[test]
fn coverage_agent_003_builder_chaining_preserves_the_identity() {
    let id = RuntimeId::new();
    let builder = AgentRuntime::builder(Storage::sqlite_in_memory())
        .runtime_id(id)
        .tenant("tenant_1")
        .tick_interval(Duration::from_millis(25))
        .clock(SharedClock::default())
        .handler("lookup", |_ctx: StepContext| async move {
            Ok(serde_json::json!({"ok": true}))
        })
        .effect_handler("charge", |_ctx: EffectContext| async move {
            Ok(serde_json::json!({"provider_receipt_id": "p-1"}))
        });
    assert_eq!(builder.runtime_id, id);
}

#[tokio::test]
async fn coverage_agent_004_build_preserves_the_overridden_identity() {
    let id = RuntimeId::new();
    let runtime = AgentRuntime::builder(Storage::sqlite_in_memory())
        .runtime_id(id)
        .build()
        .await
        .expect("runtime builds");
    assert_eq!(runtime.runtime_id(), id);
    runtime.shutdown().await;
}

#[tokio::test]
async fn coverage_agent_005_generated_identity_survives_build() {
    let builder = AgentRuntime::builder(Storage::sqlite_in_memory());
    let expected = builder.runtime_id;
    let runtime = builder.build().await.expect("runtime builds");
    assert_eq!(runtime.runtime_id(), expected);
    runtime.shutdown().await;
}

#[tokio::test]
async fn coverage_agent_006_runtime_derefs_to_the_engine() {
    fn engine_surface(engine: &Engine) -> &Engine {
        engine
    }
    let runtime = AgentRuntime::builder(Storage::sqlite_in_memory())
        .build()
        .await
        .expect("runtime builds");
    // Deref coercion: the runtime is usable anywhere an `&Engine` is expected.
    let engine = engine_surface(&runtime);
    assert!(std::ptr::eq(engine, &*runtime));
    runtime.shutdown().await;
}

#[tokio::test]
async fn coverage_agent_007_into_engine_returns_a_functional_engine() {
    let runtime = AgentRuntime::builder(Storage::sqlite_in_memory())
        .build()
        .await
        .expect("runtime builds");
    let engine = runtime.into_engine();
    let error = engine
        .get_instance(InstanceId::new())
        .await
        .expect_err("unknown instance");
    assert!(matches!(error, Error::NotFound(_)), "{error:?}");
    engine.shutdown().await;
}
