use super::*;
use std::sync::Condvar;

#[derive(Default)]
struct HostGate {
    entered: AtomicBool,
    released: Mutex<bool>,
    wake: Condvar,
}

impl StepHandler for HostGate {
    fn execute(&self, _step: String, _input: String) -> Result<String, crate::HandlerError> {
        self.wait();
        Ok("{}".into())
    }
}

impl HostGate {
    fn wait(&self) {
        self.entered.store(true, Ordering::Release);
        let mut released = self.released.lock().unwrap();
        while !*released {
            released = self.wake.wait(released).unwrap();
        }
    }
}

struct ReleaseHost(Arc<HostGate>);

impl Drop for ReleaseHost {
    fn drop(&mut self) {
        *self.0.released.lock().unwrap() = true;
        self.0.wake.notify_all();
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn independent_engine_cannot_overlap_a_physically_running_remote_task() {
    independent_engine_overlap(false).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn independent_engine_cannot_overlap_a_business_retry_with_a_new_task_id() {
    independent_engine_overlap(true).await;
}

async fn independent_engine_overlap(business_retry: bool) {
    let server = spawn_control_plane(route_with(Vec::new(), 200, 200)).await;
    let mut first = harness(&server, ok_handler(Duration::ZERO)).await;
    let second = prepared_admission::prepared_harness(&server, Arc::new(Semaphore::new(2))).await;
    let gate = Arc::new(HostGate::default());
    let release = ReleaseHost(gate.clone());
    let mut registry = HandlerRegistry::new();
    register_foreign_handler(
        &mut registry,
        "scan",
        gate.clone(),
        Duration::from_secs(30),
        first.worker.deps.stragglers.clone(),
    );
    Arc::get_mut(&mut first.worker).unwrap().deps.registry = Arc::new(registry);
    let id = uuid::Uuid::new_v4();
    let original = task_json(id);
    let task: RemoteTask = serde_json::from_value(original.clone()).unwrap();
    let worker = first.worker.clone();
    let running = tokio::spawn(async move { worker.execute(task, Duration::from_secs(1)).await });
    wait_for(|| gate.entered.load(Ordering::Acquire)).await;
    running.abort();
    assert!(running.await.unwrap_err().is_cancelled());
    wait_for(|| first.worker.deps.stragglers.blocks("scan")).await;
    let next_id = if business_retry {
        uuid::Uuid::new_v4()
    } else {
        id
    };
    let mut successor = original;
    successor["id"] = json!(next_id);
    successor["claim_epoch"] = json!(if business_retry { 1 } else { 8 });
    if business_retry {
        successor["attempt"] = json!(2);
        successor["effect_id"] = json!("eff-next-attempt");
    }
    let mut retry = successor.clone();
    retry["claim_epoch"] = json!(if business_retry { 2 } else { 9 });
    second
        .worker
        .execute(
            serde_json::from_value(successor).unwrap(),
            Duration::from_secs(1),
        )
        .await;
    assert!(
        second.inputs.lock().unwrap().is_empty(),
        "a second engine entered the same invocation while its predecessor was physically running"
    );
    assert_eq!(prepared_admission::starts(&second).await, 0);
    assert_eq!(server.count(&format!("{id}/complete")), 0);
    assert_eq!(
        server.bodies(&format!("{next_id}/release"))[0]["started"],
        false
    );
    let unrelated = uuid::Uuid::new_v4();
    second
        .worker
        .execute(
            serde_json::from_value(task_json(unrelated)).unwrap(),
            Duration::from_secs(1),
        )
        .await;
    assert_eq!(server.count(&format!("{unrelated}/complete")), 1);
    drop(release);
    wait_for(|| !first.worker.deps.stragglers.blocks("scan")).await;
    second
        .worker
        .execute(
            serde_json::from_value(retry).unwrap(),
            Duration::from_secs(1),
        )
        .await;
    assert_eq!(server.count(&format!("{next_id}/complete")), 1);
    assert_eq!(second.inputs.lock().unwrap().len(), 2);
}

struct RetiringHandler {
    invoke: Arc<HostGate>,
    retire: Arc<HostGate>,
}

impl StepHandler for RetiringHandler {
    fn execute(&self, step: String, input: String) -> Result<String, crate::HandlerError> {
        self.invoke.execute(step, input)
    }
}

impl Drop for RetiringHandler {
    fn drop(&mut self) {
        if self.invoke.entered.load(Ordering::Acquire) {
            self.retire.wait();
        }
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn process_invocation_reservation_survives_engine_drop_and_foreign_callback_destruction() {
    let server = spawn_control_plane(route_with(Vec::new(), 200, 200)).await;
    let mut first = harness(&server, ok_handler(Duration::ZERO)).await;
    let second = prepared_admission::prepared_harness(&server, Arc::new(Semaphore::new(2))).await;
    let invoke = Arc::new(HostGate::default());
    let retire = Arc::new(HostGate::default());
    let release_invoke = ReleaseHost(invoke.clone());
    let release_retire = ReleaseHost(retire.clone());
    let mut registry = HandlerRegistry::new();
    register_foreign_handler(
        &mut registry,
        "scan",
        Arc::new(RetiringHandler {
            invoke: invoke.clone(),
            retire: retire.clone(),
        }),
        Duration::from_secs(30),
        first.worker.deps.stragglers.clone(),
    );
    Arc::get_mut(&mut first.worker).unwrap().deps.registry = Arc::new(registry);
    let id = uuid::Uuid::new_v4();
    let original = task_json(id);
    let task = serde_json::from_value(original.clone()).unwrap();
    let worker = first.worker.clone();
    let running = tokio::spawn(async move { worker.execute(task, Duration::from_secs(1)).await });
    wait_for(|| invoke.entered.load(Ordering::Acquire)).await;
    running.abort();
    assert!(running.await.unwrap_err().is_cancelled());
    let stragglers = first.worker.deps.stragglers.clone();
    drop(first);
    drop(release_invoke);
    wait_for(|| retire.entered.load(Ordering::Acquire)).await;
    let next_id = uuid::Uuid::new_v4();
    let mut successor = original;
    successor["id"] = json!(next_id);
    successor["attempt"] = json!(2);
    successor["effect_id"] = json!("eff-next-attempt");
    successor["claim_epoch"] = json!(1);
    let mut retry = successor.clone();
    retry["claim_epoch"] = json!(2);
    second
        .worker
        .execute(
            serde_json::from_value(successor).unwrap(),
            Duration::from_secs(1),
        )
        .await;
    assert!(second.inputs.lock().unwrap().is_empty());
    assert_eq!(prepared_admission::starts(&second).await, 0);
    drop(release_retire);
    wait_for(|| !stragglers.blocks("scan")).await;
    second
        .worker
        .execute(
            serde_json::from_value(retry).unwrap(),
            Duration::from_secs(1),
        )
        .await;
    assert_eq!(second.inputs.lock().unwrap().len(), 1);
    assert_eq!(server.count(&format!("{next_id}/complete")), 1);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn physical_retry_contention_backs_off_polling_and_recovers_after_return() {
    let original = task_json(uuid::Uuid::new_v4());
    let next_id = uuid::Uuid::new_v4();
    let mut next = original.clone();
    next["id"] = json!(next_id);
    next["attempt"] = json!(2);
    next["effect_id"] = json!("eff-next-attempt");
    let done = Arc::new(AtomicBool::new(false));
    let completed = done.clone();
    let epochs = AtomicU64::new(1);
    let server = spawn_control_plane(Arc::new(move |_, path, _| {
        if path.ends_with("/workers/tasks/poll") {
            let tasks = if completed.load(Ordering::Acquire) {
                Vec::new()
            } else {
                let mut task = next.clone();
                task["claim_epoch"] = json!(epochs.fetch_add(1, Ordering::Relaxed));
                vec![task]
            };
            return (
                200,
                json!({"tasks":tasks,"lease_secs":3,"heartbeat_interval_secs":1,"poll_after_ms":0}),
            );
        }
        if path.ends_with(&format!("{next_id}/complete")) {
            completed.store(true, Ordering::Release);
        }
        (200, json!({}))
    }))
    .await;
    let mut first = harness(&server, ok_handler(Duration::ZERO)).await;
    let second = harness_with(&server, ok_handler(Duration::ZERO), 1).await;
    let gate = Arc::new(HostGate::default());
    let release = ReleaseHost(gate.clone());
    let mut registry = HandlerRegistry::new();
    register_foreign_handler(
        &mut registry,
        "scan",
        gate.clone(),
        Duration::from_secs(30),
        first.worker.deps.stragglers.clone(),
    );
    Arc::get_mut(&mut first.worker).unwrap().deps.registry = Arc::new(registry);
    let worker = first.worker.clone();
    let task = serde_json::from_value(original).unwrap();
    let running = tokio::spawn(async move { worker.execute(task, Duration::from_secs(1)).await });
    wait_for(|| gate.entered.load(Ordering::Acquire)).await;
    running.abort();
    assert!(running.await.unwrap_err().is_cancelled());
    second.worker.spawn(&tokio::runtime::Handle::current());
    wait_for(|| server.count(&format!("{next_id}/release")) >= 1).await;
    tokio::time::sleep(Duration::from_millis(200)).await;
    let polls = server.count("/workers/tasks/poll");
    assert!(second.inputs.lock().unwrap().is_empty());
    assert!(
        polls <= 2,
        "physical contention caused a hot poll/release loop: {polls}"
    );
    drop(release);
    wait_for(|| done.load(Ordering::Acquire)).await;
    second.worker.stop();
    wait_for(|| second.worker.stats().in_flight == 0).await;
    assert_eq!(second.inputs.lock().unwrap().len(), 1);
    assert_eq!(server.count(&format!("{next_id}/fail")), 0);
}
