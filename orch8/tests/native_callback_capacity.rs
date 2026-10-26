//! Logical step timeouts must not return physical foreign-call admission.
#[allow(dead_code)]
#[path = "../../packages/native-common/durable.rs"]
mod durable;
#[path = "../../packages/native-common/runtime.rs"]
mod host_runtime;

use orch8_storage::{ContinuityStore, InstanceStore};
use orch8_types::{continuity::EffectState, ids::TenantId};
use serde_json::{Value, json};
use std::{
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};
use tokio::sync::watch;

struct Directory(std::path::PathBuf);
impl Directory {
    fn new() -> Self {
        let path = std::env::temp_dir().join(format!("orch8-physical-{}", uuid::Uuid::now_v7()));
        std::fs::create_dir(&path).unwrap();
        Self(path)
    }
}
impl Drop for Directory {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

#[tokio::test]
async fn native_current_snapshots_skip_obsolete_payloads_and_keep_retry_diagnostics() {
    use orch8_storage::{OutputStore, testing::backends::RawPool};
    let directory = Directory::new();
    let path = directory
        .0
        .join("snapshots.db")
        .to_string_lossy()
        .into_owned();
    let engine = durable::DurableEngine::new(&path);
    engine
        .deploy(
            &json!({"name":"snapshot","blocks":[{"type":"step","id":"work","handler":"noop"}]})
                .to_string(),
        )
        .await
        .unwrap();
    let id = engine
        .start("snapshot", Some("{\"seed\":42}"), None, None)
        .await
        .unwrap();
    let instance_id = orch8::InstanceId::from_uuid(uuid::Uuid::parse_str(&id).unwrap());
    let reader = orch8_storage::sqlite::SqliteStorage::file(&path)
        .await
        .unwrap();
    let at = chrono::Utc::now();
    let make = |n, block: &str, reference: Option<&str>, value| orch8_types::output::BlockOutput {
        id: uuid::Uuid::from_u128(n),
        instance_id,
        block_id: orch8_types::ids::BlockId::new(block),
        output: value,
        output_ref: reference.map(str::to_owned),
        output_size: 30,
        attempt: 0,
        created_at: at + chrono::Duration::microseconds(i64::try_from(n).unwrap()),
    };
    let rows = [
        make(1, "work", None, json!({"obsolete":true})),
        make(
            2,
            "work",
            Some("__retry__"),
            json!({"error":"retry diagnostic","attempt":2}),
        ),
        make(
            3,
            "work",
            Some("__in_progress__"),
            json!({"_sentinel":true}),
        ),
        make(4, "_interceptor:trace", None, json!({"internal":true})),
        make(5, "_sentinel", None, json!({"business":true})),
        make(
            6,
            "retry_only",
            Some("__retry__"),
            json!({"error":"first retry"}),
        ),
    ];
    for row in &rows {
        reader.save_block_output(row).await.unwrap();
    }
    let raw = RawPool::Sqlite(reader.pool().clone());
    for n in [1, 3, 4] {
        raw.set_text(
            "block_outputs",
            "output",
            "id",
            &uuid::Uuid::from_u128(n).to_string(),
            "{damaged obsolete JSON",
        )
        .await;
    }
    assert!(
        reader.get_all_outputs(instance_id).await.is_err(),
        "the fixture must fail if obsolete payloads are decoded"
    );
    let expected = json!({"work":{"error":"retry diagnostic","attempt":2},"_sentinel":{"business":true},"retry_only":{"error":"first retry"}});
    let current: Value = serde_json::from_str(&engine.get(&id).await.unwrap()).unwrap();
    let deadline: Value =
        serde_json::from_str(&engine.run(&id, Some(Duration::ZERO)).await.unwrap()).unwrap();
    assert_eq!(current["outputs"], expected);
    assert_eq!(deadline["outputs"], expected);
    assert_eq!(current["data"], json!({"seed":42}));
    assert_eq!(current["state"], "scheduled");
    assert_eq!(deadline["state"], "scheduled");
    raw.set_text(
        "block_outputs",
        "output",
        "id",
        &uuid::Uuid::from_u128(2).to_string(),
        "{damaged current diagnostic",
    )
    .await;
    assert!(
        engine.get(&id).await.is_err(),
        "selected corrupt diagnostics must fail closed"
    );
    engine.close().await;
    reader.close().await;
}

struct Fixture {
    engine: Arc<durable::DurableEngine>,
    budget: Arc<durable::callbacks::Budget>,
    calls: Arc<AtomicUsize>,
    release: watch::Sender<bool>,
}
impl Drop for Fixture {
    fn drop(&mut self) {
        self.release.send_replace(true);
    }
}
impl Fixture {
    async fn new(path: &str, budget: Arc<durable::callbacks::Budget>) -> Self {
        let engine = Arc::new(durable::DurableEngine::with_callback_budget(
            path,
            budget.clone(),
        ));
        let calls = Arc::new(AtomicUsize::new(0));
        let (release, waiting) = watch::channel(false);
        let entered = calls.clone();
        engine
            .register(
                "host",
                Arc::new(move |invocation| {
                    let mut waiting = waiting.clone();
                    let entered = entered.clone();
                    Box::pin(async move {
                        // Model the actual Node bridge: only this owned task knows when
                        // the foreign promise settles, independently of its waiter.
                        tokio::spawn(async move {
                            entered.fetch_add(1, Ordering::SeqCst);
                            let _ = waiting.wait_for(|released| *released).await;
                            drop(invocation);
                            Ok(json!({"ok":true}).to_string())
                        })
                        .await
                        .map_err(|error| error.to_string())?
                    })
                }),
            )
            .unwrap();
        engine.deploy(&json!({"name":"held","blocks":[{"type":"step","id":"work","handler":"host","timeout":50}]}).to_string()).await.unwrap();
        Self {
            engine,
            budget,
            calls,
            release,
        }
    }
    async fn start_and_run(&self, timeout: Duration) -> (String, Value) {
        let id = self.engine.start("held", None, None, None).await.unwrap();
        let snapshot = self.engine.run(&id, Some(timeout)).await.unwrap();
        (id, serde_json::from_str(&snapshot).unwrap())
    }
    async fn settle(&self, available: usize) {
        self.release.send_replace(true);
        tokio::time::timeout(Duration::from_secs(3), async {
            while self.budget.available() != available {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
    }
}

#[tokio::test]
async fn timeout_keeps_capacity_and_excess_work_preserves_its_prepared_attempt() {
    let directory = Directory::new();
    let path = directory
        .0
        .join("physical.db")
        .to_string_lossy()
        .into_owned();
    let fixture = Fixture::new(&path, durable::callbacks::Budget::new(2)).await;
    let mut timed_out = Vec::new();
    for _ in 0..2 {
        let (id, result) = fixture.start_and_run(Duration::from_secs(2)).await;
        assert_eq!(result["state"], "failed");
        timed_out.push(id);
    }
    assert_eq!(fixture.budget.available(), 0);
    let (pending, result) = fixture.start_and_run(Duration::from_millis(100)).await;
    assert!(matches!(
        result["state"].as_str(),
        Some("scheduled" | "running")
    ));
    assert_eq!(result["outputs"], json!({}));
    assert_eq!(fixture.calls.load(Ordering::SeqCst), 2);
    let reader = orch8_storage::sqlite::SqliteStorage::file(&path)
        .await
        .unwrap();
    let id = orch8::InstanceId::from_uuid(uuid::Uuid::parse_str(&pending).unwrap());
    // The observer deadline may return while its owned scheduler pass is
    // still rejecting admission. Observe the durable transition rather than
    // requiring a slow runner to finish that pass within 100 milliseconds.
    let before = tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            let instance = reader.get_instance(id).await.unwrap().unwrap();
            assert!(
                !instance.state.is_terminal(),
                "excess work must stay deferred"
            );
            let receipts = reader
                .list_instance_effect_receipts(&TenantId::new("default").unwrap(), id, 8)
                .await
                .unwrap();
            if instance.state == orch8::InstanceState::Scheduled && receipts.len() == 1 {
                break receipts;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("excess work did not settle into durable pre-dispatch deferral");
    assert_eq!(fixture.calls.load(Ordering::SeqCst), 2);
    assert_eq!(before.len(), 1);
    assert_eq!(before[0].state, EffectState::Prepared);
    assert_eq!(before[0].attempt, 0);
    fixture.settle(2).await;
    let result: Value = serde_json::from_str(
        &fixture
            .engine
            .run(&pending, Some(Duration::from_secs(3)))
            .await
            .unwrap(),
    )
    .unwrap();
    assert_eq!(result["state"], "completed");
    let after = reader
        .list_instance_effect_receipts(&TenantId::new("default").unwrap(), id, 8)
        .await
        .unwrap();
    assert_eq!(after.len(), 1);
    assert_eq!(after[0].id, before[0].id);
    assert_eq!(after[0].attempt, before[0].attempt);
    assert_eq!(after[0].state, EffectState::Committed);
    for old in timed_out {
        assert_eq!(
            reader
                .get_instance(orch8::InstanceId::from_uuid(
                    uuid::Uuid::parse_str(&old).unwrap()
                ))
                .await
                .unwrap()
                .unwrap()
                .state,
            orch8::InstanceState::Failed
        );
    }
    assert_eq!(fixture.calls.load(Ordering::SeqCst), 3);
    fixture.engine.close().await;
    reader.close().await;
}

#[tokio::test]
async fn closing_and_replacing_an_engine_cannot_reset_physical_capacity() {
    let budget = durable::callbacks::Budget::new(1);
    let first = Fixture::new(":memory:", budget.clone()).await;
    assert_eq!(
        first.start_and_run(Duration::from_secs(2)).await.1["state"],
        "failed"
    );
    first.engine.close().await;
    let second = Fixture::new(":memory:", budget.clone()).await;
    let (id, result) = second.start_and_run(Duration::from_millis(100)).await;
    assert_eq!(result["state"], "scheduled");
    assert_eq!(second.calls.load(Ordering::SeqCst), 0);
    first.settle(1).await;
    second.release.send_replace(true);
    let result: Value = serde_json::from_str(
        &second
            .engine
            .run(&id, Some(Duration::from_secs(3)))
            .await
            .unwrap(),
    )
    .unwrap();
    assert_eq!(result["state"], "completed");
    assert_eq!(second.calls.load(Ordering::SeqCst), 1);
    second.engine.close().await;
}

#[test]
fn blocking_callback_keeps_its_permit_after_its_source_runtime_is_destroyed() {
    let budget = durable::callbacks::Budget::new(1);
    let source = host_runtime::HostRuntime::new(
        tokio::runtime::Builder::new_multi_thread()
            .worker_threads(1)
            .enable_all()
            .build()
            .unwrap(),
    );
    let (release, held) = std::sync::mpsc::channel();
    let (entered, entry) = std::sync::mpsc::channel();
    let (finished, finish) = std::sync::mpsc::channel();
    let owner = budget.clone();
    source.block_on(async move {
        let invocation = durable::Invocation::new("{}".into(), owner.reserve().unwrap());
        let work = tokio::task::spawn_blocking(move || {
            entered.send(()).unwrap();
            let _ = held.recv_timeout(Duration::from_secs(10));
            drop(invocation);
            finished.send(()).unwrap();
        });
        drop(work);
    });
    entry.recv_timeout(Duration::from_secs(3)).unwrap();
    drop(source);
    let retained = budget.available() == 0;
    release.send(()).unwrap();
    finish.recv_timeout(Duration::from_secs(3)).unwrap();
    assert!(
        retained,
        "runtime retirement released the live blocking callback's permit"
    );
    assert_eq!(budget.available(), 1);
}

#[tokio::test]
async fn native_outputs_hide_internal_rows_without_interpreting_business_payload_keys() {
    let engine = durable::DurableEngine::new(":memory:");
    engine
        .register(
            "host",
            Arc::new(|invocation| {
                Box::pin(async move {
                    let request: Value = serde_json::from_str(&invocation).unwrap();
                    let output = if request["step_id"] == "first" {
                        json!({"_sentinel":true,"business":7})
                    } else {
                        request["outputs"].clone()
                    };
                    drop(invocation);
                    Ok(json!({"ok":output}).to_string())
                })
            }),
        )
        .unwrap();
    engine
        .deploy(
            &json!({"name":"outputs","blocks":[
                {"type":"step","id":"first","handler":"host"},
                {"type":"step","id":"second","handler":"host"}
            ]})
            .to_string(),
        )
        .await
        .unwrap();
    let id = engine.start("outputs", None, None, None).await.unwrap();
    let result: Value =
        serde_json::from_str(&engine.run(&id, Some(Duration::from_secs(3))).await.unwrap())
            .unwrap();
    assert_eq!(result["state"], "completed");
    assert_eq!(
        result["outputs"]["first"],
        json!({"_sentinel":true,"business":7})
    );
    assert_eq!(
        result["outputs"]["second"],
        json!({"first":{"_sentinel":true,"business":7}})
    );
    engine.close().await;
}
