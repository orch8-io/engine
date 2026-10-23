//! A satisfied event gate must return every matched payload or fail explicitly.
use orch8_engine::{
    event_correlation::{envelope, ingest, parse_wait_params, register_wait},
    handlers::{StepContext, wait_for_event::handle_wait_for_event},
    testing::step_context,
};
use orch8_storage::testing::{
    faulty::FaultStorage,
    fixtures::{InstanceBuilder, SequenceBuilder},
};
use orch8_types::{error::StepError, event_correlation::WaitStatus, instance::InstanceState};
use serde_json::json;
use std::sync::Arc;

async fn satisfied_context(missing_payload: bool) -> StepContext {
    let mut ctx = step_context(json!({"events":"ready","correlation_key":"key"})).await;
    let sequence = SequenceBuilder::new("t")
        .step("step", "wait_for_event")
        .build();
    ctx.storage.create_sequence(&sequence).await.unwrap();
    let instance = InstanceBuilder::for_sequence(&sequence)
        .state(InstanceState::Waiting)
        .build();
    ctx.storage.create_instance(&instance).await.unwrap();
    ctx.instance_id = instance.id;
    let mut wait = parse_wait_params(
        &ctx.params,
        ctx.tenant_id.as_str(),
        instance.id.into_uuid(),
        ctx.block_id.as_str(),
    )
    .unwrap();
    if missing_payload {
        wait.status = WaitStatus::Satisfied;
        wait.matched_names = vec!["ready".into()];
        wait.matched_event_ids = vec![uuid::Uuid::now_v7()];
        ctx.storage.upsert_event_wait(&wait).await.unwrap();
    } else {
        register_wait(&ctx.storage, wait).await.unwrap();
        let event = envelope(
            ctx.tenant_id.as_str(),
            "ready",
            "producer",
            "key",
            json!({"fact":true}),
        );
        assert!(ingest(&ctx.storage, event).await.unwrap().satisfied);
    }
    ctx
}

#[tokio::test]
async fn transient_matched_payload_read_cannot_commit_a_partial_success() {
    let mut ctx = satisfied_context(false).await;
    let (storage, faults) = FaultStorage::wrap(Arc::clone(&ctx.storage));
    faults.on("get_event").times(1).fail_transient();
    ctx.storage = storage;
    let error = handle_wait_for_event(ctx)
        .await
        .expect_err("matched payload outage became a successful empty output");
    assert!(matches!(error, StepError::Retryable { .. }));
}

#[tokio::test]
async fn missing_matched_payload_cannot_be_silently_omitted() {
    let ctx = satisfied_context(true).await;
    let error = handle_wait_for_event(ctx)
        .await
        .expect_err("missing matched event became a successful partial output");
    assert!(matches!(error, StepError::Permanent { .. }));
}

#[tokio::test]
async fn permanent_matched_payload_read_error_is_not_retried() {
    let mut ctx = satisfied_context(false).await;
    let (storage, faults) = FaultStorage::wrap(Arc::clone(&ctx.storage));
    faults.on("get_event").times(1).fail_permanent();
    ctx.storage = storage;
    let error = handle_wait_for_event(ctx).await.unwrap_err();
    assert!(matches!(error, StepError::Permanent { .. }));
}

#[tokio::test]
async fn retry_after_payload_outage_returns_the_complete_match() {
    let ctx = satisfied_context(false).await;
    let retry = ctx.clone();
    let mut first = ctx;
    let (storage, faults) = FaultStorage::wrap(Arc::clone(&first.storage));
    faults.on("get_event").times(1).fail_transient();
    first.storage = storage;
    assert!(matches!(
        handle_wait_for_event(first).await,
        Err(StepError::Retryable { .. })
    ));
    let output = handle_wait_for_event(retry).await.unwrap();
    assert_eq!(output["satisfied"], true);
    assert_eq!(output["all"].as_array().unwrap().len(), 1);
    assert_eq!(output["events"]["ready"]["payload"], json!({"fact":true}));
}
