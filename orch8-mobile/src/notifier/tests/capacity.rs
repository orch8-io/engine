use super::*;

async fn wait_for_listener(mut ready: impl FnMut() -> bool) {
    tokio::time::timeout(std::time::Duration::from_secs(5), async {
        while !ready() {
            tokio::time::sleep(std::time::Duration::from_millis(1)).await;
        }
    })
    .await
    .expect("listener did not reach its gate");
}

#[tokio::test]
async fn closed_notifiers_cannot_reset_process_capacity_or_consume_undelivered_events() {
    let slots = Arc::new(Semaphore::new(MAX_IN_FLIGHT_CALLBACKS));
    assert_retired_listener_capacity(MAX_IN_FLIGHT_CALLBACKS, || {
        let mut notifier = MobileNotifier::new();
        notifier.process_callback_slots = slots.clone();
        notifier
    })
    .await;
}

async fn assert_retired_listener_capacity(limit: usize, make_owner: impl Fn() -> MobileNotifier) {
    let (storage, mobile, _) = setup_storage_with_terminal_instance(InstanceState::Completed).await;
    let held = Arc::new(GatedListener {
        ids: StdMutex::new(Vec::new()),
        released: (StdMutex::new(false), std::sync::Condvar::new()),
    });
    let _release = ReleaseListener(held.clone());
    for index in 0..limit {
        let owner = make_owner();
        owner.set_listener(held.clone());
        owner.fire_terminal_events(&storage, &mobile).await;
        wait_for_listener(|| held.ids.lock().unwrap().len() == index + 1).await;
        drop(owner.close());
        drop(owner);
    }
    let (storage, mobile, expected) = notification_backlog().await;
    let successor = make_owner();
    let listener = Arc::new(GatedListener {
        ids: StdMutex::new(Vec::new()),
        released: (StdMutex::new(true), std::sync::Condvar::new()),
    });
    successor.set_listener(listener.clone());
    let cache = Arc::new(SequenceCache::new(16, std::time::Duration::from_secs(60)));
    successor
        .fire_step_pending_events(&storage, &mobile, &cache)
        .await;
    let cleanup = successor.fire_terminal_events(&storage, &mobile).await;
    let seen_before_return =
        successor.terminal_seen.lock().await.len() + successor.waiting_seen.lock().await.len();
    let local_after_refusal = successor.callback_slots.available_permits();
    // Unblock host code even on the broken baseline, before assertions.
    held.release();
    wait_for_listener(|| successor.process_callback_slots.available_permits() == limit).await;
    tokio::time::timeout(std::time::Duration::from_secs(10), async {
        loop {
            successor.fire_terminal_events(&storage, &mobile).await;
            successor
                .fire_step_pending_events(&storage, &mobile, &cache)
                .await;
            if listener.ids.lock().unwrap().len() == expected.len() {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("capacity return must allow undelivered events to drain");
    successor.fire_terminal_events(&storage, &mobile).await;
    successor
        .fire_step_pending_events(&storage, &mobile, &cache)
        .await;
    let delivered = listener.ids.lock().unwrap().clone();
    assert_eq!(
        seen_before_return, 0,
        "retired owners must still occupy process capacity"
    );
    assert_eq!(
        local_after_refusal, MAX_IN_FLIGHT_CALLBACKS,
        "process refusal must return local admission"
    );
    assert_eq!(
        cleanup.len(),
        6,
        "storage cleanup must proceed without callback capacity"
    );
    assert_eq!(
        delivered.len(),
        expected.len(),
        "capacity retries must not duplicate callbacks"
    );
    assert_eq!(delivered.into_iter().collect::<HashSet<_>>(), expected);
}

#[test]
fn default_notifiers_share_process_capacity_across_close() {
    const CHILD: &str = "ORCH8_LISTENER_CAPACITY_PROBE_CHILD";
    if std::env::var_os(CHILD).is_some() {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .max_blocking_threads(MAX_PROCESS_CALLBACKS + 1)
            .build()
            .unwrap();
        runtime.block_on(assert_retired_listener_capacity(
            MAX_PROCESS_CALLBACKS,
            MobileNotifier::new,
        ));
        println!("ORCH8_LISTENER_CAPACITY_VERIFIED");
        return;
    }
    let result = std::process::Command::new(std::env::current_exe().unwrap())
        .args([
            "--exact",
            "notifier::tests::capacity::default_notifiers_share_process_capacity_across_close",
            "--nocapture",
        ])
        .env(CHILD, "1")
        .output()
        .unwrap();
    assert!(
        result.status.success()
            && String::from_utf8_lossy(&result.stdout).contains("ORCH8_LISTENER_CAPACITY_VERIFIED"),
        "default listener capacity probe failed:\n{}\n{}",
        String::from_utf8_lossy(&result.stdout),
        String::from_utf8_lossy(&result.stderr)
    );
}

struct DroppingListener {
    body: Arc<GatedListener>,
    dropping: Arc<std::sync::atomic::AtomicBool>,
    drop_gate: StdMutex<std::sync::mpsc::Receiver<()>>,
    panic_in_callback: bool,
    panic_in_drop: bool,
}
impl EngineListener for DroppingListener {
    fn on_instance_completed(&self, id: String, _output: String) {
        self.body.record(id);
        assert!(!self.panic_in_callback, "expected listener callback panic");
    }
    fn on_instance_failed(&self, _id: String, _error: String) {
        unreachable!();
    }
    fn on_step_pending(&self, _id: String, _step: String, _handler: String) {
        unreachable!();
    }
}
impl Drop for DroppingListener {
    fn drop(&mut self) {
        self.dropping
            .store(true, std::sync::atomic::Ordering::SeqCst);
        let explicitly_released = self.drop_gate.lock().unwrap().recv().is_ok();
        assert!(
            !self.panic_in_drop || !explicitly_released,
            "expected listener destructor panic"
        );
    }
}

#[tokio::test]
async fn process_admission_survives_foreign_destruction_and_returns_on_panic() {
    for mode in 0..3 {
        let slots = Arc::new(Semaphore::new(1));
        let make_owner = || {
            let mut owner = MobileNotifier::new();
            owner.process_callback_slots = slots.clone();
            owner
        };
        let (storage, mobile, _) =
            setup_storage_with_terminal_instance(InstanceState::Completed).await;
        let body = Arc::new(GatedListener {
            ids: StdMutex::new(Vec::new()),
            released: (StdMutex::new(false), std::sync::Condvar::new()),
        });
        let _release = ReleaseListener(body.clone());
        let dropping = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let (release_drop, drop_gate) = std::sync::mpsc::channel();
        let owner = make_owner();
        owner.set_listener(Arc::new(DroppingListener {
            body: body.clone(),
            dropping: dropping.clone(),
            drop_gate: StdMutex::new(drop_gate),
            panic_in_callback: mode == 2,
            panic_in_drop: mode == 1,
        }));
        owner.fire_terminal_events(&storage, &mobile).await;
        wait_for_listener(|| body.ids.lock().unwrap().len() == 1).await;
        drop(owner.close());
        drop(owner);
        body.release();
        wait_for_listener(|| dropping.load(std::sync::atomic::Ordering::SeqCst)).await;
        let successor = make_owner();
        let listener = Arc::new(GatedListener {
            ids: StdMutex::new(Vec::new()),
            released: (StdMutex::new(true), std::sync::Condvar::new()),
        });
        successor.set_listener(listener.clone());
        successor.fire_terminal_events(&storage, &mobile).await;
        let consumed = successor.terminal_seen.lock().await.len();
        let held = slots.available_permits();
        release_drop.send(()).unwrap();
        wait_for_listener(|| slots.available_permits() == 1).await;
        successor.fire_terminal_events(&storage, &mobile).await;
        wait_for_listener(|| listener.ids.lock().unwrap().len() == 1).await;
        assert_eq!(
            consumed, 0,
            "callback return must not bypass foreign destruction"
        );
        assert_eq!(
            held, 0,
            "foreign destruction retains physical process admission"
        );
        wait_for_listener(|| {
            successor.callback_slots.available_permits() == MAX_IN_FLIGHT_CALLBACKS
        })
        .await;
        assert_eq!(
            successor.callback_slots.available_permits(),
            MAX_IN_FLIGHT_CALLBACKS
        );
    }
}
