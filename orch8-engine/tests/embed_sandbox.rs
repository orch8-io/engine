//! Execution-time enforcement of the embed sandbox: sequences owned by an
//! embedded end customer (sub-tenant) are confined even when they were stored
//! before the write-time check existed (or bypassed it).

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use serde_json::{Value, json};

use orch8_engine::embed_sandbox::SandboxPolicy;
use orch8_engine::handlers::{HandlerRegistry, builtin::register_builtins};
use orch8_engine::testing::{TestEngine, TestEngineBuilder};
use orch8_storage::testing::fixtures::{
    InstanceBuilder, SequenceBuilder, step_block, step_with_params,
};
use orch8_types::instance::{InstanceState, TaskInstance};
use orch8_types::sequence::{BlockDefinition, SequenceDefinition};

const SUB: &str = "acme";

/// A `http_request` stand-in that counts how often it really ran.
fn engine(calls: &Arc<AtomicUsize>, policy: Option<SandboxPolicy>) -> TestEngineBuilder {
    let mut registry = HandlerRegistry::new();
    register_builtins(&mut registry);
    if let Some(policy) = policy {
        registry = registry.with_embed_sandbox(policy);
    }
    let counter = Arc::clone(calls);
    registry.register("http_request", move |_ctx| {
        let counter = Arc::clone(&counter);
        Box::pin(async move {
            counter.fetch_add(1, Ordering::SeqCst);
            Ok(json!({"sent": true}))
        })
    });
    TestEngine::builder().registry(registry)
}

fn owned(name: &str, blocks: Vec<BlockDefinition>) -> SequenceDefinition {
    let mut seq = SequenceBuilder::new("t").name(name).blocks(blocks).build();
    seq.sub_tenant = Some(SUB.into());
    seq
}

fn vendor(name: &str, blocks: Vec<BlockDefinition>) -> SequenceDefinition {
    SequenceBuilder::new("t").name(name).blocks(blocks).build()
}

fn instance(
    seq: &SequenceDefinition,
    now: chrono::DateTime<chrono::Utc>,
    data: Value,
) -> TaskInstance {
    let mut inst = InstanceBuilder::for_sequence(seq)
        .due_at(now)
        .data(data)
        .build();
    inst.sub_tenant = Some(SUB.into());
    inst
}

#[tokio::test]
async fn stored_owned_sequence_with_forbidden_handler_fails_without_executing() {
    let calls = Arc::new(AtomicUsize::new(0));
    let eng = engine(&calls, None).build().await;
    let seq = owned("legacy", vec![step_block("send", "http_request")]);
    eng.storage.create_sequence(&seq).await.unwrap();
    let id = eng.create(&instance(&seq, eng.now(), json!({}))).await;

    let done = eng.drive(id).await;
    assert_eq!(done.state, InstanceState::Failed);
    assert_eq!(calls.load(Ordering::SeqCst), 0, "the handler must not run");
}

#[tokio::test]
async fn owned_sequence_with_allowlisted_handlers_runs() {
    let calls = Arc::new(AtomicUsize::new(0));
    let eng = engine(&calls, None).build().await;
    let seq = owned(
        "fine",
        vec![step_block("a", "noop"), step_block("b", "transform")],
    );
    eng.storage.create_sequence(&seq).await.unwrap();
    let id = eng.create(&instance(&seq, eng.now(), json!({}))).await;
    assert_eq!(eng.drive(id).await.state, InstanceState::Completed);
}

#[tokio::test]
async fn vendor_configured_allowlist_admits_extra_handler() {
    let calls = Arc::new(AtomicUsize::new(0));
    let policy = SandboxPolicy::from_config_list("noop,http_request");
    let eng = engine(&calls, Some(policy)).build().await;
    let seq = owned("custom", vec![step_block("send", "http_request")]);
    eng.storage.create_sequence(&seq).await.unwrap();
    let id = eng.create(&instance(&seq, eng.now(), json!({}))).await;
    assert_eq!(eng.drive(id).await.state, InstanceState::Completed);
    assert_eq!(calls.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn credential_reference_minted_by_a_template_is_never_resolved() {
    let calls = Arc::new(AtomicUsize::new(0));
    let eng = engine(&calls, None).build().await;
    let seq = owned(
        "sneaky",
        vec![BlockDefinition::Step(Box::new(step_with_params(
            "a",
            "noop",
            json!({"token": "{{ context.data.ref }}"}),
        )))],
    );
    eng.storage.create_sequence(&seq).await.unwrap();
    let id = eng
        .create(&instance(
            &seq,
            eng.now(),
            json!({"ref": "credentials://prod_key"}),
        ))
        .await;
    assert_eq!(eng.drive(id).await.state, InstanceState::Failed);
}

#[tokio::test]
async fn tenant_level_sequence_is_unrestricted_even_in_a_sub_tenant_run() {
    let calls = Arc::new(AtomicUsize::new(0));
    let eng = engine(&calls, None).build().await;
    let seq = vendor("gallery-flow", vec![step_block("send", "http_request")]);
    eng.storage.create_sequence(&seq).await.unwrap();
    let id = eng.create(&instance(&seq, eng.now(), json!({}))).await;
    assert_eq!(eng.drive(id).await.state, InstanceState::Completed);
    assert_eq!(calls.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn forbidden_cleanup_hook_handler_is_skipped() {
    let calls = Arc::new(AtomicUsize::new(0));
    let eng = engine(&calls, None).build().await;
    let mut seq = owned("hooked", vec![step_block("boom", "fail")]);
    seq.on_failure = Some(vec![step_block("exfil", "http_request")]);
    eng.storage.create_sequence(&seq).await.unwrap();
    let id = eng.create(&instance(&seq, eng.now(), json!({}))).await;
    assert_eq!(eng.drive(id).await.state, InstanceState::Failed);
    for _ in 0..3 {
        eng.tick().await;
    }
    assert_eq!(calls.load(Ordering::SeqCst), 0);
}

fn sub_sequence(target: &str) -> BlockDefinition {
    serde_json::from_value(json!({
        "type": "sub_sequence", "id": "ss", "sequence_name": target
    }))
    .unwrap()
}

#[tokio::test]
async fn sub_sequence_into_vendor_or_other_sub_tenant_is_denied() {
    for (child_name, child_owner) in [("vendor-child", None), ("other-child", Some("globex"))] {
        let calls = Arc::new(AtomicUsize::new(0));
        let eng = engine(&calls, None).build().await;
        let mut child = vendor(child_name, vec![step_block("send", "http_request")]);
        child.sub_tenant = child_owner.map(str::to_owned);
        eng.storage.create_sequence(&child).await.unwrap();
        let parent = owned("parent", vec![sub_sequence(child_name)]);
        eng.storage.create_sequence(&parent).await.unwrap();
        let id = eng.create(&instance(&parent, eng.now(), json!({}))).await;

        assert_eq!(
            eng.drive(id).await.state,
            InstanceState::Failed,
            "{child_name}"
        );
        assert!(
            eng.storage
                .get_child_instances(id)
                .await
                .unwrap()
                .is_empty(),
            "{child_name}: no child instance may be spawned"
        );
        assert_eq!(calls.load(Ordering::SeqCst), 0);
    }
}

#[tokio::test]
async fn sub_sequence_into_own_sub_tenant_sequence_runs() {
    let calls = Arc::new(AtomicUsize::new(0));
    let eng = engine(&calls, None).build().await;
    let child = owned("mine", vec![step_block("a", "noop")]);
    eng.storage.create_sequence(&child).await.unwrap();
    let parent = owned("parent", vec![sub_sequence("mine")]);
    eng.storage.create_sequence(&parent).await.unwrap();
    let id = eng.create(&instance(&parent, eng.now(), json!({}))).await;
    // Tick until the parent completes (the child runs in between).
    for _ in 0..60 {
        if eng.instance(id).await.state.is_terminal() {
            break;
        }
        eng.advance(std::time::Duration::from_secs(5));
        eng.tick().await;
    }
    assert_eq!(eng.instance(id).await.state, InstanceState::Completed);
    let children = eng.storage.get_child_instances(id).await.unwrap();
    assert_eq!(children.len(), 1);
    assert_eq!(children[0].sub_tenant.as_deref(), Some(SUB));
}

#[tokio::test]
async fn vendor_sequence_may_still_start_sub_sequences_freely() {
    let calls = Arc::new(AtomicUsize::new(0));
    let eng = engine(&calls, None).build().await;
    let child = vendor("vendor-child", vec![step_block("a", "noop")]);
    eng.storage.create_sequence(&child).await.unwrap();
    let parent = vendor("vendor-parent", vec![sub_sequence("vendor-child")]);
    eng.storage.create_sequence(&parent).await.unwrap();
    let id = eng.create(&instance(&parent, eng.now(), json!({}))).await;
    for _ in 0..60 {
        if eng.instance(id).await.state.is_terminal() {
            break;
        }
        eng.advance(std::time::Duration::from_secs(5));
        eng.tick().await;
    }
    assert_eq!(eng.instance(id).await.state, InstanceState::Completed);
}
