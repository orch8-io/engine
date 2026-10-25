//! Invalid or already-finished native runs must not schedule other work.
#[allow(dead_code)]
#[path = "../../packages/native-common/durable.rs"]
mod durable;

use serde_json::{Value, json};
use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};
use std::time::Duration;

async fn fixture() -> (Arc<durable::DurableEngine>, Arc<AtomicUsize>) {
    let engine = Arc::new(durable::DurableEngine::new(":memory:"));
    let calls = Arc::new(AtomicUsize::new(0));
    let entered = calls.clone();
    engine
        .register(
            "host",
            Arc::new(move |_| {
                entered.fetch_add(1, Ordering::SeqCst);
                Box::pin(async { Ok(json!({"ok":true}).to_string()) })
            }),
        )
        .unwrap();
    engine
        .deploy(
            &json!({"name":"work","blocks":[{"type":"step","id":"work","handler":"host"}]})
                .to_string(),
        )
        .await
        .unwrap();
    (engine, calls)
}

#[tokio::test]
async fn missing_run_target_does_not_dispatch_queued_work() {
    let (engine, calls) = fixture().await;
    let pending = engine.start("work", None, None, None).await.unwrap();
    let result = engine
        .run(
            &uuid::Uuid::now_v7().to_string(),
            Some(Duration::from_secs(1)),
        )
        .await;
    let snapshot: Value = serde_json::from_str(&engine.get(&pending).await.unwrap()).unwrap();
    engine.close().await;
    assert!(result.is_err());
    assert_eq!(
        calls.load(Ordering::SeqCst),
        0,
        "missing target dispatched unrelated work"
    );
    assert_eq!(snapshot["state"], "scheduled");
}

#[tokio::test]
async fn terminal_run_target_does_not_dispatch_new_work() {
    let (engine, calls) = fixture().await;
    let done = engine.start("work", None, None, None).await.unwrap();
    engine
        .run(&done, Some(Duration::from_secs(1)))
        .await
        .unwrap();
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    let pending = engine.start("work", None, None, None).await.unwrap();
    let result: Value = serde_json::from_str(
        &engine
            .run(&done, Some(Duration::from_secs(1)))
            .await
            .unwrap(),
    )
    .unwrap();
    let snapshot: Value = serde_json::from_str(&engine.get(&pending).await.unwrap()).unwrap();
    engine.close().await;
    assert_eq!(result["state"], "completed");
    assert_eq!(
        calls.load(Ordering::SeqCst),
        1,
        "terminal target dispatched unrelated work"
    );
    assert_eq!(snapshot["state"], "scheduled");
}

#[tokio::test]
async fn zero_run_budget_returns_without_dispatch() {
    let (engine, calls) = fixture().await;
    let pending = engine.start("work", None, None, None).await.unwrap();
    let result: Value =
        serde_json::from_str(&engine.run(&pending, Some(Duration::ZERO)).await.unwrap()).unwrap();
    engine.close().await;
    assert_eq!(
        calls.load(Ordering::SeqCst),
        0,
        "exhausted budget dispatched a host callback"
    );
    assert_eq!(result["state"], "scheduled");
}

#[tokio::test]
async fn malformed_run_target_does_not_freeze_registration() {
    let engine = durable::DurableEngine::new(":memory:");
    assert!(engine.run("malformed", None).await.is_err());
    let registered = engine.register(
        "host",
        Arc::new(|_| Box::pin(async { Ok(json!({"ok":true}).to_string()) })),
    );
    engine.close().await;
    assert!(
        registered.is_ok(),
        "malformed target initialized and froze the native engine"
    );
}

#[tokio::test]
async fn unrepresentable_run_budget_is_an_error_before_initialization() {
    let engine = Arc::new(durable::DurableEngine::new(":memory:"));
    let caller = engine.clone();
    let result = tokio::spawn(async move {
        caller
            .run(&uuid::Uuid::now_v7().to_string(), Some(Duration::MAX))
            .await
    })
    .await;
    let registered = engine.register(
        "host",
        Arc::new(|_| Box::pin(async { Ok(json!({"ok":true}).to_string()) })),
    );
    engine.close().await;
    assert!(
        matches!(result, Ok(Err(_))),
        "unrepresentable timeout must return an error, not panic: {result:?}"
    );
    assert!(
        registered.is_ok(),
        "invalid budget initialized and froze the native engine"
    );
}

#[tokio::test]
async fn concurrent_native_runs_do_not_duplicate_the_target_handler() {
    let (engine, calls) = fixture().await;
    let pending = engine.start("work", None, None, None).await.unwrap();
    let (left, right) = tokio::join!(
        engine.run(&pending, Some(Duration::from_secs(2))),
        engine.run(&pending, Some(Duration::from_secs(2))),
    );
    engine.close().await;
    for result in [left, right] {
        let result: Value = serde_json::from_str(&result.unwrap()).unwrap();
        assert_eq!(result["state"], "completed");
    }
    assert_eq!(calls.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn native_run_still_processes_resume_for_a_waiting_target() {
    let engine = durable::DurableEngine::new(":memory:");
    engine
        .deploy(
            &json!({"name":"gate","blocks":[{"type":"step","id":"gate","handler":"human_review",
        "wait_for_input":{"prompt":"Continue?","choices":[{"label":"Yes","value":"yes"}]}}]})
            .to_string(),
        )
        .await
        .unwrap();
    let id = engine.start("gate", None, None, None).await.unwrap();
    let waiting: Value =
        serde_json::from_str(&engine.run(&id, Some(Duration::from_secs(2))).await.unwrap())
            .unwrap();
    assert_eq!(waiting["state"], "waiting");
    engine
        .signal(&id, "human_input:gate", Some(r#"{"value":"yes"}"#))
        .await
        .unwrap();
    let resumed: Value =
        serde_json::from_str(&engine.run(&id, Some(Duration::from_secs(2))).await.unwrap())
            .unwrap();
    engine.close().await;
    assert_eq!(resumed["state"], "completed");
}
