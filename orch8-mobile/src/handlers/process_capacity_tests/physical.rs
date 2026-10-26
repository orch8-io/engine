use super::*;
use std::sync::{Condvar, Mutex, atomic::AtomicBool};

#[derive(Default)]
struct Gate {
    calls: AtomicUsize,
    released: Mutex<bool>,
    changed: Condvar,
}
impl Gate {
    fn enter(&self) {
        self.calls.fetch_add(1, Ordering::SeqCst);
        let mut released = self.released.lock().unwrap();
        while !*released {
            released = self.changed.wait(released).unwrap();
        }
    }
    fn release(&self) {
        *self.released.lock().unwrap() = true;
        self.changed.notify_all();
    }
}
impl StepHandler for Gate {
    fn execute(&self, _: String, _: String) -> Result<String, HandlerError> {
        self.enter();
        Ok("{}".into())
    }
}
struct Release(Arc<Gate>);
impl Drop for Release {
    fn drop(&mut self) {
        self.0.release();
    }
}

async fn wait_for(mut ready: impl FnMut() -> bool) {
    tokio::time::timeout(Duration::from_secs(5), async {
        while !ready() {
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
    })
    .await
    .expect("physical handler did not reach its gate");
}

async fn context() -> StepContext {
    StepContext {
        effect_admission: None,
        claim_token: None,
        instance_id: orch8_types::ids::InstanceId::new(),
        tenant_id: orch8_types::ids::TenantId::unchecked("mobile"),
        block_id: orch8_types::ids::BlockId::new("native"),
        params: serde_json::json!({}),
        context: Arc::new(orch8_types::context::ExecutionContext::default()),
        attempt: 1,
        storage: Arc::new(
            orch8_storage::sqlite::SqliteStorage::in_memory()
                .await
                .unwrap(),
        ),
        wait_for_input: None,
    }
}

fn registry(
    handler: Arc<dyn StepHandler>,
    stop: CancellationToken,
    capacity: Option<Arc<Semaphore>>,
) -> HandlerRegistry {
    let mut registry = HandlerRegistry::new();
    let quarantine = Arc::new(Stragglers::default());
    if let Some(capacity) = capacity {
        register_with_capacity(
            &mut registry,
            "native",
            handler,
            Duration::from_secs(60),
            quarantine,
            stop,
            capacity,
        );
    } else {
        register_owned_foreign_handler(
            &mut registry,
            "native",
            handler,
            Duration::from_secs(60),
            quarantine,
            stop,
        );
    }
    registry
}

async fn retired_registries(limit: usize, capacity: Option<Arc<Semaphore>>) {
    let slots = capacity.clone().unwrap_or_else(process_legacy_slots);
    let gate = Arc::new(Gate::default());
    let _release = Release(gate.clone());
    let ctx = context().await;
    for index in 0..limit {
        let stop = CancellationToken::new();
        let owner = registry(gate.clone(), stop.clone(), capacity.clone());
        let task = tokio::spawn(owner.get("native").unwrap()(ctx.clone()));
        wait_for(|| gate.calls.load(Ordering::SeqCst) == index + 1).await;
        task.abort();
        assert!(task.await.unwrap_err().is_cancelled());
        stop.cancel();
        drop(owner);
    }
    let calls = Arc::new(AtomicUsize::new(0));
    let successor = registry(
        Arc::new(Counter(calls.clone())),
        CancellationToken::new(),
        capacity,
    );
    let refused = successor.get("native").unwrap()(ctx.clone()).await;
    let calls_before_return = calls.load(Ordering::SeqCst);
    let mut dry = ctx.clone();
    Arc::make_mut(&mut dry.context).runtime.dry_run = true;
    let dry_result = successor.get("native").unwrap()(dry).await;
    gate.release();
    wait_for(|| slots.available_permits() == limit).await;
    successor.get("native").unwrap()(ctx).await.unwrap();
    assert!(matches!(refused, Err(StepError::Retryable { .. })));
    assert_eq!(
        calls_before_return, 0,
        "retired callbacks must retain process admission"
    );
    assert_eq!(
        dry_result.unwrap()["dry_run"],
        true,
        "dry run requires no physical callback slot"
    );
    assert_eq!(calls.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn retired_registries_cannot_reset_live_physical_capacity() {
    retired_registries(2, Some(Arc::new(Semaphore::new(2)))).await;
}

#[test]
fn default_legacy_registries_share_one_process_gate() {
    const CHILD: &str = "ORCH8_LEGACY_CAPACITY_PROBE_CHILD";
    if std::env::var_os(CHILD).is_some() {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .max_blocking_threads(MAX_PROCESS_LEGACY_CALLS + 1)
            .build()
            .unwrap();
        runtime.block_on(retired_registries(MAX_PROCESS_LEGACY_CALLS, None));
        println!("ORCH8_LEGACY_CAPACITY_VERIFIED");
        return;
    }
    let result = std::process::Command::new(std::env::current_exe().unwrap())
        .args(["--exact", "handlers::process_capacity_tests::physical::default_legacy_registries_share_one_process_gate", "--nocapture"])
        .env(CHILD, "1").output().unwrap();
    assert!(
        result.status.success()
            && String::from_utf8_lossy(&result.stdout).contains("ORCH8_LEGACY_CAPACITY_VERIFIED"),
        "default legacy capacity probe failed:\n{}\n{}",
        String::from_utf8_lossy(&result.stdout),
        String::from_utf8_lossy(&result.stderr)
    );
}

struct Retiring {
    body: Arc<Gate>,
    dropping: Arc<AtomicBool>,
    drop_gate: Mutex<std::sync::mpsc::Receiver<()>>,
    panic_in_callback: bool,
    panic_in_drop: bool,
}
impl StepHandler for Retiring {
    fn execute(&self, _: String, _: String) -> Result<String, HandlerError> {
        self.body.enter();
        assert!(!self.panic_in_callback, "expected handler panic");
        Ok("{}".into())
    }
}
impl Drop for Retiring {
    fn drop(&mut self) {
        self.dropping.store(true, Ordering::SeqCst);
        let released = self.drop_gate.lock().unwrap().recv().is_ok();
        assert!(
            !self.panic_in_drop || !released,
            "expected handler destructor panic"
        );
    }
}

#[tokio::test]
async fn process_permit_survives_foreign_destruction_and_panic_cleanup() {
    for mode in 0..3 {
        let slots = Arc::new(Semaphore::new(1));
        let body = Arc::new(Gate::default());
        let _release = Release(body.clone());
        let dropping = Arc::new(AtomicBool::new(false));
        let (release_drop, drop_gate) = std::sync::mpsc::channel();
        let stop = CancellationToken::new();
        let owner = registry(
            Arc::new(Retiring {
                body: body.clone(),
                dropping: dropping.clone(),
                drop_gate: Mutex::new(drop_gate),
                panic_in_callback: mode == 2,
                panic_in_drop: mode == 1,
            }),
            stop.clone(),
            Some(slots.clone()),
        );
        let ctx = context().await;
        let task = tokio::spawn(owner.get("native").unwrap()(ctx.clone()));
        wait_for(|| body.calls.load(Ordering::SeqCst) == 1).await;
        task.abort();
        assert!(task.await.unwrap_err().is_cancelled());
        stop.cancel();
        drop(owner);
        body.release();
        wait_for(|| dropping.load(Ordering::SeqCst)).await;
        let calls = Arc::new(AtomicUsize::new(0));
        let successor = registry(
            Arc::new(Counter(calls.clone())),
            CancellationToken::new(),
            Some(slots.clone()),
        );
        let refused = successor.get("native").unwrap()(ctx.clone()).await;
        let before = calls.load(Ordering::SeqCst);
        release_drop.send(()).unwrap();
        wait_for(|| slots.available_permits() == 1).await;
        successor.get("native").unwrap()(ctx).await.unwrap();
        assert!(matches!(refused, Err(StepError::Retryable { .. })));
        assert_eq!(
            before, 0,
            "foreign handler destruction retains physical admission"
        );
        assert_eq!(calls.load(Ordering::SeqCst), 1);
    }
}

#[tokio::test]
async fn unpolled_preparation_holds_capacity_through_foreign_destruction() {
    for panic_in_drop in [false, true] {
        let slots = Arc::new(Semaphore::new(1));
        let body = Arc::new(Gate::default());
        let dropping = Arc::new(AtomicBool::new(false));
        let (release_drop, drop_gate) = std::sync::mpsc::channel();
        let owner = registry(
            Arc::new(Retiring {
                body: body.clone(),
                dropping: dropping.clone(),
                drop_gate: Mutex::new(drop_gate),
                panic_in_callback: false,
                panic_in_drop,
            }),
            CancellationToken::new(),
            Some(slots.clone()),
        );
        let ctx = context().await;
        let prepared = owner
            .prepare_native_handler("native", ctx.clone())
            .unwrap()
            .unwrap();
        drop(owner);
        let disposing = std::thread::spawn(move || drop(prepared));
        wait_for(|| dropping.load(Ordering::SeqCst)).await;
        let calls = Arc::new(AtomicUsize::new(0));
        let successor = registry(
            Arc::new(Counter(calls.clone())),
            CancellationToken::new(),
            Some(slots.clone()),
        );
        let refused = successor
            .prepare_native_handler("native", ctx.clone())
            .unwrap()
            .is_err();
        let held = slots.available_permits();
        release_drop.send(()).unwrap();
        let panicked = disposing.join().is_err();
        wait_for(|| slots.available_permits() == 1).await;
        assert!(refused);
        assert_eq!(held, 0);
        assert_eq!(
            body.calls.load(Ordering::SeqCst),
            0,
            "preparation and cancellation must not enter host code"
        );
        assert_eq!(panicked, panic_in_drop);
        successor.get("native").unwrap()(ctx).await.unwrap();
        assert_eq!(calls.load(Ordering::SeqCst), 1);
    }
}
