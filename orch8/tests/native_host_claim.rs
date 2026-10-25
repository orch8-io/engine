//! Exercise the actual shared Node/Python adapter across its storage waits.
#[allow(dead_code)]
#[path = "../../packages/native-common/durable.rs"]
mod durable;

use orch8::{StepContext, StepError};
use orch8_storage::{
    StorageBackend,
    testing::{
        faulty::FaultStorage,
        fixtures::{InstanceBuilder, SequenceBuilder},
    },
};
use orch8_types::{context::ExecutionContext, ids::BlockId, instance::InstanceState};
use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};

async fn claimed_context(storage: Arc<dyn StorageBackend>) -> StepContext {
    let seq = SequenceBuilder::new("native-host-claim")
        .step("work", "host")
        .build();
    storage.create_sequence(&seq).await.unwrap();
    let instance = InstanceBuilder::for_sequence(&seq).due_now().build();
    storage.create_instance(&instance).await.unwrap();
    let instance = storage
        .claim_due_instances(chrono::Utc::now() + chrono::Duration::seconds(1), 1, 0)
        .await
        .unwrap()
        .pop()
        .unwrap();
    StepContext {
        effect_admission: None,
        claim_token: instance.claimed_at,
        instance_id: instance.id,
        tenant_id: instance.tenant_id,
        block_id: BlockId::new("work"),
        params: serde_json::json!({"private":"HOST_INPUT_CANARY"}),
        context: Arc::new(ExecutionContext::default()),
        attempt: 1,
        storage,
        wait_for_input: None,
    }
}

fn callback(calls: Arc<AtomicUsize>) -> durable::Invoke {
    Arc::new(move |_| {
        calls.fetch_add(1, Ordering::SeqCst);
        Box::pin(async { Ok(serde_json::json!({"ok":true}).to_string()) })
    })
}

orch8_storage::backend_test!(async fn stale_native_host_context_never_loads_or_dispatches(b) {
    let mut ctx = claimed_context(b.storage.clone()).await;
    b.storage.update_instance_state(ctx.instance_id, InstanceState::Waiting, None).await.unwrap();
    let (storage, faults) = FaultStorage::wrap(b.storage.clone());
    ctx.storage = storage;
    let calls = Arc::new(AtomicUsize::new(0));
    let result = durable::host_handler(callback(calls.clone()))(ctx).await;
    assert_eq!(calls.load(Ordering::SeqCst), 0, "stale adapter entered host callback");
    assert_eq!(faults.calls("get_all_outputs"), 0);
    assert!(matches!(result, Err(StepError::ClaimLost)));
});

orch8_storage::backend_test!(async fn native_host_claim_loss_during_output_loading_prevents_dispatch(b) {
    let mut ctx = claimed_context(b.storage.clone()).await;
    let id = ctx.instance_id;
    let (storage, faults) = FaultStorage::wrap(b.storage.clone());
    ctx.storage = storage;
    let pause = faults.on("get_all_outputs").times(1).pause();
    let calls = Arc::new(AtomicUsize::new(0));
    let call = durable::host_handler(callback(calls.clone()))(ctx);
    let task = tokio::spawn(call);
    tokio::time::timeout(std::time::Duration::from_secs(5), pause.entered()).await.unwrap();
    b.storage.update_instance_state(id, InstanceState::Waiting, None).await.unwrap();
    pause.release();
    let result = task.await.unwrap();
    assert_eq!(calls.load(Ordering::SeqCst), 0, "output-loading wait authorized stale host work");
    assert!(matches!(result, Err(StepError::ClaimLost)));
});

orch8_storage::backend_test!(async fn native_host_failed_claim_read_prevents_dispatch_and_recovers(b) {
    let mut ctx = claimed_context(b.storage.clone()).await;
    let (storage, faults) = FaultStorage::wrap(b.storage.clone());
    ctx.storage = storage;
    let calls = Arc::new(AtomicUsize::new(0));
    let handler = durable::host_handler(callback(calls.clone()));
    faults.on("verify_instance_claim").times(1).fail_transient();
    assert!(matches!(handler(ctx.clone()).await, Err(StepError::Retryable { .. })));
    assert_eq!(faults.calls("get_all_outputs"), 0);
    assert_eq!(calls.load(Ordering::SeqCst), 0);
    assert_eq!(handler(ctx).await.unwrap(), serde_json::json!(true));
    assert_eq!(calls.load(Ordering::SeqCst), 1);
});

orch8_storage::backend_test!(async fn native_host_valid_and_trusted_unclaimed_calls_keep_their_envelope(b) {
    let mut ctx = claimed_context(b.storage.clone()).await;
    let id = ctx.instance_id;
    let expected_id = id.to_string();
    let calls = Arc::new(AtomicUsize::new(0));
    let entered = calls.clone();
    let handler = durable::host_handler(Arc::new(move |request| {
        let request: serde_json::Value = serde_json::from_str(&request).unwrap();
        assert_eq!(request["instance_id"], expected_id);
        assert_eq!(request["params"]["private"], "HOST_INPUT_CANARY");
        assert_eq!(request["step_id"], "work");
        assert_eq!(request["attempt"], 1);
        assert_eq!(request["outputs"], serde_json::json!({}));
        entered.fetch_add(1, Ordering::SeqCst);
        Box::pin(async { Ok(serde_json::json!({"ok":{"accepted":true}}).to_string()) })
    }));
    assert_eq!(handler(ctx.clone()).await.unwrap()["accepted"], true);
    b.storage.update_instance_state(id, InstanceState::Waiting, None).await.unwrap();
    ctx.claim_token = None;
    assert_eq!(handler(ctx).await.unwrap()["accepted"], true);
    assert_eq!(calls.load(Ordering::SeqCst), 2);
});

orch8_storage::backend_test!(async fn retiring_native_owner_during_request_loading_refuses_factory_entry(b) {
    use std::sync::atomic::AtomicBool;
    let mut ctx = claimed_context(b.storage.clone()).await;
    let id = ctx.instance_id;
    let tenant = ctx.tenant_id.clone();
    let token = ctx.claim_token.unwrap();
    let (storage, faults) = FaultStorage::wrap(b.storage.clone());
    ctx.storage = storage;
    let pause = faults.on("get_all_outputs").times(1).pause();
    let calls = Arc::new(AtomicUsize::new(0));
    let closed = Arc::new(AtomicBool::new(false));
    let call = durable::owned_host_handler(callback(calls.clone()), closed.clone())(ctx);
    let task = tokio::spawn(call);
    tokio::time::timeout(std::time::Duration::from_secs(5), pause.entered()).await.unwrap();
    closed.store(true, Ordering::Release);
    assert!(b.storage.verify_instance_claim(id, &tenant, token).await.unwrap(),
        "local runtime retirement is independent of the durable instance claim");
    pause.release();
    assert!(matches!(task.await.unwrap(), Err(StepError::ClaimLost)));
    assert_eq!(calls.load(Ordering::SeqCst), 0);
});
