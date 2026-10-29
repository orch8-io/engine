use super::*;

async fn prepared_harness(server: &MockControlPlane, slots: Arc<Semaphore>) -> Harness {
    let mut h = harness(server, ok_handler(Duration::ZERO)).await;
    let mut registry = HandlerRegistry::new();
    crate::handlers::register_foreign_handler_with_capacity(
        &mut registry,
        "scan",
        Arc::new(Recording {
            inputs: h.inputs.clone(),
            delay: Duration::ZERO,
            result: Ok(r#"{"scanned":true}"#.into()),
        }),
        Duration::from_secs(10),
        h.worker.deps.stragglers.clone(),
        slots,
    );
    Arc::get_mut(&mut h.worker).unwrap().deps.registry = Arc::new(registry);
    install_start_audit(&h).await;
    h
}

async fn install_start_audit(h: &Harness) {
    sqlx::query("CREATE TABLE native_start_audit (task_id TEXT)")
        .execute(&h.pool)
        .await
        .unwrap();
    sqlx::query("CREATE TRIGGER observe_native_start AFTER UPDATE OF started ON mobile_worker_claims WHEN NEW.started=1 BEGIN INSERT INTO native_start_audit VALUES(NEW.task_id); END").execute(&h.pool).await.unwrap();
}

async fn starts(h: &Harness) -> i64 {
    sqlx::query_scalar("SELECT COUNT(*) FROM native_start_audit")
        .fetch_one(&h.pool)
        .await
        .unwrap()
}

#[tokio::test]
async fn native_capacity_refusal_is_unstarted_and_recovers_without_business_failure() {
    let server = spawn_control_plane(route_with(Vec::new(), 200, 200)).await;
    let slots = Arc::new(Semaphore::new(1));
    let occupied = slots.clone().try_acquire_owned().unwrap();
    let h = prepared_harness(&server, slots.clone()).await;
    let id = uuid::Uuid::new_v4();
    h.worker
        .execute(
            serde_json::from_value(task_json(id)).unwrap(),
            Duration::from_secs(1),
        )
        .await;
    assert_eq!(
        starts(&h).await,
        0,
        "reservation refusal must precede journal start"
    );
    assert_eq!(server.bodies(&format!("{id}/release"))[0]["started"], false);
    assert_eq!(server.count(&format!("{id}/fail")), 0);
    assert_eq!(server.count(&format!("{id}/complete")), 0);
    assert!(h.inputs.lock().unwrap().is_empty());
    assert_eq!(h.store.count().await.unwrap(), 0);
    drop(occupied);
    let mut retry = task_json(id);
    retry["claim_epoch"] = json!(8);
    h.worker
        .execute(
            serde_json::from_value(retry).unwrap(),
            Duration::from_secs(1),
        )
        .await;
    assert_eq!(server.count(&format!("{id}/complete")), 1);
    assert_eq!(server.count(&format!("{id}/fail")), 0);
    assert_eq!(starts(&h).await, 1);
    assert_eq!(h.inputs.lock().unwrap().len(), 1);
    assert_eq!(slots.available_permits(), 1);
    assert_eq!(h.worker.stats().released, 1);
}

#[tokio::test]
async fn rejected_capacity_release_retains_only_unstarted_recovery_evidence() {
    let server = spawn_control_plane(route_with(Vec::new(), 200, 400)).await;
    let slots = Arc::new(Semaphore::new(1));
    let _occupied = slots.clone().try_acquire_owned().unwrap();
    let h = prepared_harness(&server, slots).await;
    let id = uuid::Uuid::new_v4();
    let task: RemoteTask = serde_json::from_value(task_json(id)).unwrap();
    let key = ClaimKey::for_task(&task, &h.client.worker_id(), h.client.api_base());
    h.worker.execute(task, Duration::from_secs(1)).await;
    let row = h.store.get(&key).await.unwrap().unwrap();
    assert!(!row.started);
    assert!(row.outcome.is_none());
    assert_eq!(starts(&h).await, 0);
    assert!(h.inputs.lock().unwrap().is_empty());
    assert_eq!(server.count(&format!("{id}/fail")), 0);
    assert_eq!(
        server.count(&format!("{id}/release")),
        1,
        "the rejected release must actually be attempted"
    );
    assert!(
        server
            .bodies(&format!("{id}/release"))
            .iter()
            .all(|body| body["started"] == false)
    );
}

#[tokio::test]
async fn journal_start_failure_drops_prepared_slot_without_entering_host() {
    let server = spawn_control_plane(route_with(Vec::new(), 200, 200)).await;
    let slots = Arc::new(Semaphore::new(1));
    let h = prepared_harness(&server, slots.clone()).await;
    sqlx::query("CREATE TRIGGER reject_native_start BEFORE UPDATE OF started ON mobile_worker_claims WHEN NEW.started=1 BEGIN SELECT RAISE(FAIL, 'injected start failure'); END").execute(&h.pool).await.unwrap();
    let id = uuid::Uuid::new_v4();
    h.worker
        .execute(
            serde_json::from_value(task_json(id)).unwrap(),
            Duration::from_secs(1),
        )
        .await;
    assert_eq!(server.bodies(&format!("{id}/release"))[0]["started"], false);
    assert_eq!(server.count(&format!("{id}/fail")), 0);
    assert_eq!(starts(&h).await, 0);
    assert!(h.inputs.lock().unwrap().is_empty());
    assert_eq!(slots.available_permits(), 1);
    assert_eq!(h.store.count().await.unwrap(), 0);
}

#[tokio::test]
async fn cancellation_before_journal_acquisition_drops_unpolled_native_reservation() {
    let server = spawn_control_plane(route_with(Vec::new(), 200, 200)).await;
    let slots = Arc::new(Semaphore::new(1));
    let h = prepared_harness(&server, slots.clone()).await;
    let id = uuid::Uuid::new_v4();
    let task: RemoteTask = serde_json::from_value(task_json(id)).unwrap();
    let key = ClaimKey::for_task(&task, &h.client.worker_id(), h.client.api_base());
    assert!(
        h.store
            .insert(&task, &h.client.worker_id(), h.client.api_base())
            .await
            .unwrap()
    );
    // Exhaust the pool, rather than delaying a submitted SQLite write. The
    // mark-start query cannot run after cancellation when a connection returns.
    let mut held = Vec::new();
    for _ in 0..h.pool.options().get_max_connections() {
        held.push(h.pool.acquire().await.unwrap());
    }
    let worker = h.worker.clone();
    let request_key = key.clone();
    let request = tokio::spawn(async move {
        let mut started = false;
        let mut outcome = None;
        let lease = ConfirmedLease::acquire(Some(30), Duration::from_millis(100), || async {
            LeaseResponse::Accepted
        })
        .await
        .unwrap();
        let authority = lease.authority();
        lease
            .run(
                Duration::from_millis(100),
                worker.execute_confirmed(
                    &task,
                    &request_key,
                    &mut started,
                    &mut outcome,
                    authority,
                ),
                || async { LeaseResponse::Accepted },
            )
            .await
            .unwrap()
    });
    wait_for(|| slots.available_permits() == 0).await;
    assert!(h.inputs.lock().unwrap().is_empty());
    request.abort();
    assert!(request.await.unwrap_err().is_cancelled());
    assert_eq!(slots.available_permits(), 1);
    drop(held);
    assert_eq!(starts(&h).await, 0);
    let row = h.store.get(&key).await.unwrap().unwrap();
    assert!(!row.started);
    assert!(row.outcome.is_none());
    assert_eq!(server.count(&format!("{id}/complete")), 0);
    assert_eq!(server.count(&format!("{id}/fail")), 0);
}

#[tokio::test]
async fn default_process_capacity_avoids_poll_churn_and_fences_capacity_lost_during_poll() {
    const CHILD: &str = "ORCH8_WORKER_CAPACITY_PROBE_CHILD";
    if std::env::var_os(CHILD).is_none() {
        let result = std::process::Command::new(std::env::current_exe().unwrap())
            .args(["--exact", "worker::worker_tests::prepared_admission::default_process_capacity_avoids_poll_churn_and_fences_capacity_lost_during_poll", "--nocapture"])
            .env(CHILD, "1").output().unwrap();
        assert!(
            result.status.success()
                && String::from_utf8_lossy(&result.stdout)
                    .contains("ORCH8_WORKER_CAPACITY_VERIFIED"),
            "worker process capacity probe failed:\n{}\n{}",
            String::from_utf8_lossy(&result.stdout),
            String::from_utf8_lossy(&result.stderr)
        );
        return;
    }
    let id = uuid::Uuid::new_v4();
    let poll_gate = Arc::new(Semaphore::new(0));
    let server = crate::test_support::spawn_control_plane_gated(
        route_with(vec![task_json(id)], 200, 200),
        Some(("/workers/tasks/poll".into(), poll_gate.clone())),
    )
    .await;
    let h = harness(&server, ok_handler(Duration::ZERO)).await;
    install_start_audit(&h).await;
    let occupied = crate::handlers::occupy_legacy_capacity_for_test();
    let mut cursor = 0;
    assert_eq!(h.worker.poll_round(&mut cursor).await.unwrap().0, 0);
    assert_eq!(
        server.count("/workers/tasks/poll"),
        0,
        "exhaustion must not churn claims"
    );
    drop(occupied);
    let worker = h.worker.clone();
    let polling = tokio::spawn(async move { worker.poll_round(&mut cursor).await });
    wait_for(|| server.count("/workers/tasks/poll") == 1).await;
    let occupied = crate::handlers::occupy_legacy_capacity_for_test();
    poll_gate.add_permits(1);
    assert_eq!(polling.await.unwrap().unwrap().0, 1);
    wait_for(|| server.count(&format!("{id}/release")) == 1).await;
    wait_for(|| h.worker.stats().in_flight == 0).await;
    assert_eq!(
        starts(&h).await,
        0,
        "a free-slot hint cannot authorize dispatch"
    );
    assert_eq!(server.bodies(&format!("{id}/release"))[0]["started"], false);
    assert_eq!(server.count(&format!("{id}/fail")), 0);
    assert!(h.inputs.lock().unwrap().is_empty());
    drop(occupied);
    let mut retry = task_json(id);
    retry["claim_epoch"] = json!(8);
    h.worker
        .execute(
            serde_json::from_value(retry).unwrap(),
            Duration::from_secs(1),
        )
        .await;
    assert_eq!(server.count(&format!("{id}/complete")), 1);
    assert_eq!(starts(&h).await, 1);
    assert_eq!(h.inputs.lock().unwrap().len(), 1);
    println!("ORCH8_WORKER_CAPACITY_VERIFIED");
}

#[test]
fn queued_worker_callback_cannot_enter_after_lease_expiry_while_runtime_is_stalled() {
    use std::future::Future as _;
    let runtime = tokio::runtime::Builder::new_current_thread()
        .max_blocking_threads(1)
        .enable_all()
        .build()
        .unwrap();
    runtime.block_on(async {
        let server=spawn_control_plane(route_with(Vec::new(),200,200)).await;
        let slots=Arc::new(Semaphore::new(1));let h=prepared_harness(&server,slots.clone()).await;
        let (entered_tx,entered_rx)=std::sync::mpsc::channel();let (release_tx,release_rx)=std::sync::mpsc::channel();
        let blocker=tokio::task::spawn_blocking(move||{entered_tx.send(()).unwrap();let _=release_rx.recv_timeout(Duration::from_secs(10));});
        entered_rx.recv_timeout(Duration::from_secs(5)).unwrap();
        let id=uuid::Uuid::new_v4();let mut task=task_json(id);task["lease_secs"]=json!(2);
        let task:RemoteTask=serde_json::from_value(task).unwrap();
        let mut execution=Box::pin(h.worker.execute(task,Duration::from_millis(100)));
        let started=async {loop {if starts(&h).await==1{break}tokio::time::sleep(Duration::from_millis(1)).await;}};
        tokio::select!{biased; result=tokio::time::timeout(Duration::from_secs(5),started)=>{result.unwrap();}, ()=&mut execution=>panic!("worker ended before native reservation")}
        // Consume the committed journal response and queue the physical call.
        for _ in 0..10 {
            std::future::poll_fn(|cx|{assert!(execution.as_mut().poll(cx).is_pending());std::task::Poll::Ready(())}).await;
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
        assert_eq!(starts(&h).await,1);assert_eq!(slots.available_permits(),0);
        // Neither the supervisor nor timer driver can progress in this window.
        std::thread::sleep(Duration::from_millis(2100));
        release_tx.send(()).unwrap();
        std::thread::sleep(Duration::from_millis(100));
        assert!(h.inputs.lock().unwrap().is_empty(),"expired queued callback entered the host");
        blocker.await.unwrap();
        tokio::time::timeout(Duration::from_secs(5),execution).await.unwrap();
        assert_eq!(server.count(&format!("{id}/complete")),0);assert_eq!(server.count(&format!("{id}/fail")),0);
        assert_eq!(slots.available_permits(),1);
    });
    runtime.shutdown_timeout(Duration::from_secs(1));
}
