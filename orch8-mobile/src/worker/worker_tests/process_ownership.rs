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
    let task: RemoteTask = serde_json::from_value(task_json(id)).unwrap();
    let worker = first.worker.clone();
    let running = tokio::spawn(async move { worker.execute(task, Duration::from_secs(1)).await });
    wait_for(|| gate.entered.load(Ordering::Acquire)).await;
    running.abort();
    assert!(running.await.unwrap_err().is_cancelled());
    wait_for(|| first.worker.deps.stragglers.blocks("scan")).await;
    let mut successor = task_json(id);
    successor["claim_epoch"] = json!(8);
    second
        .worker
        .execute(
            serde_json::from_value(successor).unwrap(),
            Duration::from_secs(1),
        )
        .await;
    assert!(
        second.inputs.lock().unwrap().is_empty(),
        "a second engine entered the same remote task while its predecessor was physically running"
    );
    assert_eq!(prepared_admission::starts(&second).await, 0);
    assert_eq!(server.count(&format!("{id}/complete")), 0);
    assert_eq!(server.bodies(&format!("{id}/release"))[0]["started"], false);
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
    let mut retry = task_json(id);
    retry["claim_epoch"] = json!(9);
    second
        .worker
        .execute(
            serde_json::from_value(retry).unwrap(),
            Duration::from_secs(1),
        )
        .await;
    assert_eq!(server.count(&format!("{id}/complete")), 1);
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
async fn process_task_reservation_survives_engine_drop_and_foreign_callback_destruction() {
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
    let task = serde_json::from_value(task_json(id)).unwrap();
    let worker = first.worker.clone();
    let running = tokio::spawn(async move { worker.execute(task, Duration::from_secs(1)).await });
    wait_for(|| invoke.entered.load(Ordering::Acquire)).await;
    running.abort();
    assert!(running.await.unwrap_err().is_cancelled());
    let stragglers = first.worker.deps.stragglers.clone();
    drop(first);
    drop(release_invoke);
    wait_for(|| retire.entered.load(Ordering::Acquire)).await;
    let mut successor = task_json(id);
    successor["claim_epoch"] = json!(8);
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
    let mut retry = task_json(id);
    retry["claim_epoch"] = json!(9);
    second
        .worker
        .execute(
            serde_json::from_value(retry).unwrap(),
            Duration::from_secs(1),
        )
        .await;
    assert_eq!(second.inputs.lock().unwrap().len(), 1);
    assert_eq!(server.count(&format!("{id}/complete")), 1);
}
