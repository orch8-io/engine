//! A logical native run must not own the lifetime of its scheduler pass.
#[allow(dead_code)]
#[path = "../../packages/native-common/durable.rs"]
mod durable;

use serde_json::{Value, json};
use std::{
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};
use tokio::sync::{Notify, watch};

struct Gate {
    entered: Notify,
    release: watch::Sender<bool>,
}
impl Gate {
    fn new() -> Arc<Self> {
        Arc::new(Self {
            entered: Notify::new(),
            release: watch::channel(false).0,
        })
    }
    async fn wait(&self) {
        let mut release = self.release.subscribe();
        release.wait_for(|released| *released).await.unwrap();
    }
    fn release(&self) {
        self.release.send_replace(true);
    }
}

async fn fixture() -> (Arc<durable::DurableEngine>, Arc<Gate>, Arc<AtomicUsize>) {
    fixture_at(":memory:").await
}

async fn fixture_at(path: &str) -> (Arc<durable::DurableEngine>, Arc<Gate>, Arc<AtomicUsize>) {
    fixture_with_engine(Arc::new(durable::DurableEngine::new(path))).await
}

async fn limited_fixture() -> (Arc<durable::DurableEngine>, Arc<Gate>, Arc<AtomicUsize>) {
    fixture_with_engine(Arc::new(durable::DurableEngine::with_pass_limit(
        ":memory:", 1,
    )))
    .await
}

async fn fixture_with_engine(
    engine: Arc<durable::DurableEngine>,
) -> (Arc<durable::DurableEngine>, Arc<Gate>, Arc<AtomicUsize>) {
    let gate = Gate::new();
    let calls = Arc::new(AtomicUsize::new(0));
    let entered = calls.clone();
    let held = gate.clone();
    engine
        .register(
            "host",
            Arc::new(move |_| {
                entered.fetch_add(1, Ordering::SeqCst);
                held.entered.notify_one();
                let held = held.clone();
                Box::pin(async move {
                    held.wait().await;
                    Ok(json!({"ok":true}).to_string())
                })
            }),
        )
        .unwrap();
    engine.deploy(&json!({"name":"held","blocks":[{"type":"step","id":"work","handler":"host","timeout":10000}]}).to_string()).await.unwrap();
    (engine, gate, calls)
}

#[tokio::test]
async fn run_deadline_returns_before_the_started_callback_finishes() {
    let (engine, gate, calls) = fixture().await;
    let id = engine.start("held", None, None, None).await.unwrap();
    let caller = engine.clone();
    let target = id.clone();
    let mut run =
        tokio::spawn(async move { caller.run(&target, Some(Duration::from_secs(1))).await });
    tokio::time::timeout(Duration::from_secs(5), gate.entered.notified())
        .await
        .unwrap();
    let observed = tokio::time::timeout(Duration::from_secs(3), &mut run).await;
    let returned_before_release = observed.is_ok();
    gate.release();
    let snapshot = match observed {
        Ok(value) => value.unwrap(),
        Err(_) => run.await.unwrap(),
    }
    .unwrap();
    let snapshot: Value = serde_json::from_str(&snapshot).unwrap();
    let completed: Value =
        serde_json::from_str(&engine.run(&id, Some(Duration::from_secs(2))).await.unwrap())
            .unwrap();
    engine.close().await;
    assert!(
        returned_before_release,
        "run deadline waited for its in-flight callback"
    );
    assert_ne!(snapshot["state"], "completed");
    assert_eq!(completed["state"], "completed");
    assert_eq!(calls.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn cancelling_a_run_keeps_one_pass_owned_across_later_callers() {
    let (engine, gate, calls) = limited_fixture().await;
    let first = engine.start("held", None, None, None).await.unwrap();
    let caller = engine.clone();
    let target = first.clone();
    let run = tokio::spawn(async move { caller.run(&target, None).await });
    tokio::time::timeout(Duration::from_secs(5), gate.entered.notified())
        .await
        .unwrap();
    run.abort();
    assert!(run.await.unwrap_err().is_cancelled());
    // This row did not exist when the first pass claimed its batch.
    let other = engine.start("held", None, None, None).await.unwrap();
    let caller = engine.clone();
    let target = other.clone();
    let mut later =
        tokio::spawn(async move { caller.run(&target, Some(Duration::from_secs(1))).await });
    let observed = tokio::time::timeout(Duration::from_secs(3), &mut later).await;
    let returned_before_release = observed.is_ok();
    let calls_while_busy = calls.load(Ordering::SeqCst);
    gate.release();
    let snapshot = match observed {
        Ok(value) => value.unwrap(),
        Err(_) => later.await.unwrap(),
    }
    .unwrap();
    let snapshot: Value = serde_json::from_str(&snapshot).unwrap();
    engine
        .run(&first, Some(Duration::from_secs(2)))
        .await
        .unwrap();
    let recovered: Value = serde_json::from_str(
        &engine
            .run(&other, Some(Duration::from_secs(2)))
            .await
            .unwrap(),
    )
    .unwrap();
    engine.close().await;
    assert!(
        returned_before_release,
        "later caller could not end its logical wait"
    );
    assert_eq!(
        calls_while_busy, 1,
        "cancelling a waiter admitted another scheduler pass"
    );
    assert_eq!(snapshot["state"], "scheduled");
    assert_eq!(recovered["state"], "completed");
    assert_eq!(calls.load(Ordering::SeqCst), 2);
}

#[tokio::test]
async fn close_wakes_an_unbounded_run_without_waiting_for_the_callback() {
    let (engine, gate, _) = fixture().await;
    let id = engine.start("held", None, None, None).await.unwrap();
    let caller = engine.clone();
    let mut run = tokio::spawn(async move { caller.run(&id, None).await });
    tokio::time::timeout(Duration::from_secs(5), gate.entered.notified())
        .await
        .unwrap();
    engine.close().await;
    let observed = tokio::time::timeout(Duration::from_secs(1), &mut run).await;
    let returned_before_release = observed.is_ok();
    gate.release();
    let result = match observed {
        Ok(value) => value.unwrap(),
        Err(_) => run.await.unwrap(),
    };
    assert!(
        returned_before_release,
        "close left its logical run waiter blocked"
    );
    assert!(result.is_err());
}

#[tokio::test]
async fn repeated_short_waiters_do_not_replace_a_live_pass() {
    let (engine, gate, calls) = limited_fixture().await;
    let first = engine.start("held", None, None, None).await.unwrap();
    let caller = engine.clone();
    let target = first.clone();
    let original = tokio::spawn(async move { caller.run(&target, None).await });
    tokio::time::timeout(Duration::from_secs(5), gate.entered.notified())
        .await
        .unwrap();
    let other = engine.start("held", None, None, None).await.unwrap();
    let observed = tokio::time::timeout(Duration::from_secs(2), async {
        let mut scheduled = true;
        for _ in 0..10 {
            let result = engine
                .run(&other, Some(Duration::from_millis(20)))
                .await
                .unwrap();
            let result: Value = serde_json::from_str(&result).unwrap();
            scheduled &= result["state"] == "scheduled";
        }
        scheduled
    })
    .await;
    let calls_while_busy = calls.load(Ordering::SeqCst);
    let original_still_waiting = !original.is_finished();
    gate.release();
    let completed: Value = serde_json::from_str(&original.await.unwrap().unwrap()).unwrap();
    let recovered: Value = serde_json::from_str(
        &engine
            .run(&other, Some(Duration::from_secs(2)))
            .await
            .unwrap(),
    )
    .unwrap();
    engine.close().await;
    assert!(
        matches!(observed, Ok(true)),
        "short logical waits did not settle independently"
    );
    assert!(
        original_still_waiting,
        "a short waiter terminated the original logical run"
    );
    assert_eq!(
        calls_while_busy, 1,
        "timed-out waiters replaced a physically live pass"
    );
    assert_eq!(completed["state"], "completed");
    assert_eq!(recovered["state"], "completed");
    assert_eq!(calls.load(Ordering::SeqCst), 2);
}

#[tokio::test]
async fn closing_a_timed_out_run_preserves_ambiguous_effect_evidence_on_reopen() {
    let root =
        std::env::temp_dir().join(format!("orch8-owned-pass-reopen-{}", uuid::Uuid::now_v7()));
    std::fs::create_dir(&root).unwrap();
    let path = root.join("engine.db");
    let (engine, gate, calls) = fixture_at(&path.to_string_lossy()).await;
    let id = engine.start("held", None, None, None).await.unwrap();
    let caller = engine.clone();
    let target = id.clone();
    let run = tokio::spawn(async move { caller.run(&target, Some(Duration::from_secs(1))).await });
    tokio::time::timeout(Duration::from_secs(5), gate.entered.notified())
        .await
        .unwrap();
    let observed = run.await.unwrap().unwrap();
    let observed: Value = serde_json::from_str(&observed).unwrap();
    engine.close().await;
    gate.release();
    drop(engine);
    let reopened = durable::DurableEngine::new(path.to_string_lossy());
    let entered = calls.clone();
    reopened
        .register(
            "host",
            Arc::new(move |_| {
                entered.fetch_add(1, Ordering::SeqCst);
                Box::pin(async { Ok(json!({"ok":true}).to_string()) })
            }),
        )
        .unwrap();
    reopened.deploy(&json!({"name":"held","blocks":[{"type":"step","id":"work","handler":"host","timeout":10000}]}).to_string()).await.unwrap();
    let recovered: Value = serde_json::from_str(
        &reopened
            .run(&id, Some(Duration::from_secs(2)))
            .await
            .unwrap(),
    )
    .unwrap();
    reopened.close().await;
    drop(reopened);
    std::fs::remove_dir_all(root).unwrap();
    assert_ne!(observed["state"], "completed");
    assert_eq!(
        calls.load(Ordering::SeqCst),
        1,
        "reopen redispatched an unsettled host effect"
    );
    assert_eq!(recovered["state"], "failed");
    assert!(
        recovered["outputs"]["work"]["message"]
            .as_str()
            .unwrap()
            .contains("automatic redispatch is blocked")
    );
}

async fn mixed_fixture() -> (Arc<durable::DurableEngine>, Arc<Gate>, Arc<Gate>) {
    let engine = Arc::new(durable::DurableEngine::new(":memory:"));
    let slow = Gate::new();
    let fast = Gate::new();
    for (name, gate) in [("slow", slow.clone()), ("fast", fast.clone())] {
        engine
            .register(
                name,
                Arc::new(move |_| {
                    gate.entered.notify_one();
                    let gate = gate.clone();
                    Box::pin(async move {
                        gate.wait().await;
                        Ok(json!({"ok":true}).to_string())
                    })
                }),
            )
            .unwrap();
    }
    for name in ["slow", "fast"] {
        engine.deploy(&json!({"name":name,"blocks":[{"type":"step","id":"work","handler":name,"timeout":10000}]}).to_string()).await.unwrap();
    }
    (engine, slow, fast)
}

#[tokio::test]
async fn newly_scheduled_fast_target_uses_available_capacity() {
    let (engine, slow, fast) = mixed_fixture().await;
    let first = engine.start("slow", None, None, None).await.unwrap();
    let caller = engine.clone();
    let original = tokio::spawn(async move { caller.run(&first, None).await });
    tokio::time::timeout(Duration::from_secs(5), slow.entered.notified())
        .await
        .unwrap();
    fast.release();
    let other = engine.start("fast", None, None, None).await.unwrap();
    let caller = engine.clone();
    let mut later = tokio::spawn(async move { caller.run(&other, None).await });
    let observed = tokio::time::timeout(Duration::from_secs(2), &mut later).await;
    let progressed_before_slow_release = observed.is_ok();
    slow.release();
    let result = match observed {
        Ok(result) => result.unwrap(),
        Err(_) => later.await.unwrap(),
    }
    .unwrap();
    original.await.unwrap().unwrap();
    engine.close().await;
    let result: Value = serde_json::from_str(&result).unwrap();
    assert!(
        progressed_before_slow_release,
        "one slow pass monopolized available engine capacity"
    );
    assert_eq!(result["state"], "completed");
}

#[tokio::test]
async fn fast_target_can_settle_while_its_batch_still_contains_slow_work() {
    let (engine, slow, fast) = mixed_fixture().await;
    let first = engine.start("slow", None, None, None).await.unwrap();
    let other = engine.start("fast", None, None, None).await.unwrap();
    let caller = engine.clone();
    let original = tokio::spawn(async move { caller.run(&first, None).await });
    tokio::time::timeout(Duration::from_secs(5), slow.entered.notified())
        .await
        .unwrap();
    tokio::time::timeout(Duration::from_secs(5), fast.entered.notified())
        .await
        .unwrap();
    let caller = engine.clone();
    let mut later = tokio::spawn(async move { caller.run(&other, None).await });
    fast.release();
    let observed = tokio::time::timeout(Duration::from_secs(2), &mut later).await;
    let progressed_before_slow_release = observed.is_ok();
    slow.release();
    let result = match observed {
        Ok(result) => result.unwrap(),
        Err(_) => later.await.unwrap(),
    }
    .unwrap();
    original.await.unwrap().unwrap();
    engine.close().await;
    let result: Value = serde_json::from_str(&result).unwrap();
    assert!(
        progressed_before_slow_release,
        "target completion waited for an unrelated batch member"
    );
    assert_eq!(result["state"], "completed");
}
