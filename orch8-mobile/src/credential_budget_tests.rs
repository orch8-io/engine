use super::*;
use std::sync::Mutex;
use std::sync::atomic::AtomicUsize;
use std::sync::mpsc::{Receiver, Sender, channel};

const LIMIT: usize = 2;

struct Probe {
    calls: Arc<AtomicUsize>,
}

impl TokenProvider for Probe {
    fn current_token(&self) -> String {
        "dst_cached".into()
    }
    fn refresh_token(&self) -> Result<String, crate::MobileError> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        Ok("dst_fresh".into())
    }
}

fn install_probe(credential: &Credential, calls: &Arc<AtomicUsize>) -> String {
    credential.set_provider(Arc::new(Probe {
        calls: Arc::clone(calls),
    }));
    credential.snapshot().generation
}

async fn wait_until(mut ready: impl FnMut() -> bool) {
    let deadline = std::time::Instant::now() + Duration::from_secs(5);
    while !ready() {
        assert!(
            std::time::Instant::now() < deadline,
            "host callback did not reach its gate"
        );
        tokio::time::sleep(Duration::from_millis(1)).await;
    }
}

struct HeldCallback {
    entered: Arc<AtomicUsize>,
    finished: Arc<AtomicUsize>,
    gate: Mutex<Receiver<()>>,
}

impl TokenProvider for HeldCallback {
    fn current_token(&self) -> String {
        "dst_cached".into()
    }
    fn refresh_token(&self) -> Result<String, crate::MobileError> {
        self.entered.fetch_add(1, Ordering::SeqCst);
        let _ = self.gate.lock().unwrap().recv();
        self.finished.fetch_add(1, Ordering::SeqCst);
        Ok("dst_held_fresh".into())
    }
}

async fn recover(credential: &Credential, generation: &str) -> bool {
    let deadline = std::time::Instant::now() + Duration::from_secs(5);
    while std::time::Instant::now() < deadline {
        if credential.refresh_generation(generation).await {
            return true;
        }
        tokio::time::sleep(Duration::from_millis(1)).await;
    }
    false
}

#[tokio::test]
async fn retired_refreshes_share_one_physical_budget_and_recover_after_return() {
    for timeout in [false, true] {
        let credential = Credential::new(String::new());
        credential.set_refresh_timeout(Duration::from_secs(10));
        let entered = Arc::new(AtomicUsize::new(0));
        let finished = Arc::new(AtomicUsize::new(0));
        let mut releases: Vec<Sender<()>> = Vec::new();
        for index in 0..LIMIT {
            let (release, gate) = channel();
            releases.push(release);
            credential.set_provider(Arc::new(HeldCallback {
                entered: Arc::clone(&entered),
                finished: Arc::clone(&finished),
                gate: Mutex::new(gate),
            }));
            let generation = credential.snapshot().generation;
            let owner = Arc::clone(&credential);
            let request = tokio::spawn(async move { owner.refresh_generation(&generation).await });
            wait_until(|| entered.load(Ordering::SeqCst) == index + 1).await;
            if timeout {
                // Freeze only after physical entry; startup scheduling cannot
                // turn this into a queued-callback timeout by accident.
                tokio::time::pause();
                tokio::time::advance(Duration::from_secs(11)).await;
                let result = request.await.unwrap();
                tokio::time::resume();
                assert!(!result);
            } else {
                install_probe(&credential, &Arc::new(AtomicUsize::new(0)));
                assert!(!request.await.unwrap());
            }
        }
        let calls = Arc::new(AtomicUsize::new(0));
        let mut admitted = 0;
        for _ in 0..128 {
            let generation = install_probe(&credential, &calls);
            admitted += usize::from(credential.refresh_generation(&generation).await);
        }
        // Saturated refresh capacity must not block business calls that
        // already have a usable credential from the latest installation.
        let server = crate::test_support::spawn_control_plane(Arc::new(|_, _, _| {
            (200, serde_json::json!({}))
        }))
        .await;
        let http = reqwest::Client::builder()
            .timeout(Duration::from_secs(5))
            .build()
            .unwrap();
        let cached_response = credential
            .send(|token| http.post(&server.base).header("x-api-key", token))
            .await;
        let client = crate::node::NodeClient::new_unchecked(
            server.base.clone(),
            Arc::clone(&credential),
            "device".into(),
            orch8_types::continuity::RuntimeId::new(),
            crate::node::Advertisement {
                handlers: vec!["scan".into()],
                caps: crate::node::NodeCapabilities::default(),
                draining: false,
            },
        );
        let heartbeat = client.heartbeat(uuid::Uuid::new_v4(), 1).await;
        let cached_headers = server.header_values("", "x-api-key");
        let registry_size = credential.state.read().unwrap().physical_owners.len();
        let exhausted_calls = calls.load(Ordering::SeqCst);
        let generation = credential.snapshot().generation;
        releases.remove(0).send(()).unwrap();
        let recovered = recover(&credential, &generation).await;
        for release in releases {
            release.send(()).unwrap();
        }
        wait_until(|| finished.load(Ordering::SeqCst) == LIMIT).await;
        assert_eq!(cached_response.unwrap().status(), 200);
        assert_eq!(heartbeat, crate::node::LeaseResponse::Accepted);
        assert_eq!(cached_headers, vec!["dst_cached"; 2]);
        assert_eq!(
            admitted, 0,
            "provider replacement must not bypass the physical-call cap"
        );
        assert_eq!(
            exhausted_calls, 0,
            "exhausted admission must not invoke or queue host work"
        );
        assert!(
            registry_size <= LIMIT + 2,
            "identity metadata must remain bounded during rejected rotations"
        );
        assert!(
            recovered,
            "capacity refusal must not poison refresh backoff or admission"
        );
        assert_eq!(calls.load(Ordering::SeqCst), 1);
        assert_eq!(credential.current(), "dst_fresh");
    }
}

struct HeldDestructor {
    credential: Weak<Credential>,
    entered: Arc<AtomicUsize>,
    body_gate: Mutex<Receiver<()>>,
    dropping: Arc<AtomicUsize>,
    dropped: Arc<AtomicUsize>,
    drop_gate: Mutex<Receiver<()>>,
    panic_after_release: bool,
}

impl TokenProvider for HeldDestructor {
    fn current_token(&self) -> String {
        "dst_cached".into()
    }
    fn refresh_token(&self) -> Result<String, crate::MobileError> {
        self.entered.fetch_add(1, Ordering::SeqCst);
        let _ = self.body_gate.lock().unwrap().recv();
        Ok("dst_retired".into())
    }
}

impl Drop for HeldDestructor {
    fn drop(&mut self) {
        // Foreign destruction may re-enter the engine and must not happen
        // under a credential lock, even while physical capacity is held.
        if let Some(credential) = self.credential.upgrade() {
            let _ = credential.current();
        }
        self.dropping.fetch_add(1, Ordering::SeqCst);
        // Sender-drop is failure cleanup, not the deliberate panic signal.
        let explicitly_released = self.drop_gate.lock().unwrap().recv().is_ok();
        self.dropped.fetch_add(1, Ordering::SeqCst);
        assert!(
            !self.panic_after_release || !explicitly_released,
            "expected provider destructor panic"
        );
    }
}

#[tokio::test]
async fn physical_budget_remains_held_through_foreign_provider_destruction() {
    let credential = Credential::new(String::new());
    credential.set_refresh_timeout(Duration::from_secs(10));
    let entered = Arc::new(AtomicUsize::new(0));
    let dropping = Arc::new(AtomicUsize::new(0));
    let dropped = Arc::new(AtomicUsize::new(0));
    let mut drop_releases = Vec::new();
    for index in 0..LIMIT {
        let (body_release, body_gate) = channel();
        let (drop_release, drop_gate) = channel();
        drop_releases.push(drop_release);
        credential.set_provider(Arc::new(HeldDestructor {
            credential: Arc::downgrade(&credential),
            entered: Arc::clone(&entered),
            body_gate: Mutex::new(body_gate),
            dropping: Arc::clone(&dropping),
            dropped: Arc::clone(&dropped),
            drop_gate: Mutex::new(drop_gate),
            panic_after_release: false,
        }));
        let generation = credential.snapshot().generation;
        let owner = Arc::clone(&credential);
        let request = tokio::spawn(async move { owner.refresh_generation(&generation).await });
        wait_until(|| entered.load(Ordering::SeqCst) == index + 1).await;
        install_probe(&credential, &Arc::new(AtomicUsize::new(0)));
        assert!(!request.await.unwrap());
        body_release.send(()).unwrap();
        wait_until(|| dropping.load(Ordering::SeqCst) == index + 1).await;
    }
    let calls = Arc::new(AtomicUsize::new(0));
    let generation = install_probe(&credential, &calls);
    let admitted = credential.refresh_generation(&generation).await;
    let exhausted_calls = calls.load(Ordering::SeqCst);
    for release in drop_releases {
        release.send(()).unwrap();
    }
    wait_until(|| dropped.load(Ordering::SeqCst) == LIMIT).await;
    let recovered = recover(&credential, &generation).await;
    assert!(
        !admitted,
        "returned callback bodies must not free capacity while foreign destruction still runs"
    );
    assert_eq!(exhausted_calls, 0);
    assert!(recovered);
    assert_eq!(calls.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn callback_and_foreign_destructor_panics_return_physical_capacity() {
    struct Panics;
    impl TokenProvider for Panics {
        fn current_token(&self) -> String {
            "dst_cached".into()
        }
        fn refresh_token(&self) -> Result<String, crate::MobileError> {
            panic!("expected callback panic")
        }
    }
    let credential = Credential::new(String::new());
    for _ in 0..LIMIT + 2 {
        credential.set_provider(Arc::new(Panics));
        assert!(
            !credential
                .refresh_generation(&credential.snapshot().generation)
                .await
        );
    }
    let entered = Arc::new(AtomicUsize::new(0));
    let dropping = Arc::new(AtomicUsize::new(0));
    let dropped = Arc::new(AtomicUsize::new(0));
    let (body_release, body_gate) = channel();
    let (drop_release, drop_gate) = channel();
    credential.set_provider(Arc::new(HeldDestructor {
        credential: Arc::downgrade(&credential),
        entered: Arc::clone(&entered),
        body_gate: Mutex::new(body_gate),
        dropping: Arc::clone(&dropping),
        dropped: Arc::clone(&dropped),
        drop_gate: Mutex::new(drop_gate),
        panic_after_release: true,
    }));
    let generation = credential.snapshot().generation;
    let owner = Arc::clone(&credential);
    let request = tokio::spawn(async move { owner.refresh_generation(&generation).await });
    wait_until(|| entered.load(Ordering::SeqCst) == 1).await;
    let calls = Arc::new(AtomicUsize::new(0));
    let generation = install_probe(&credential, &calls);
    assert!(!request.await.unwrap());
    body_release.send(()).unwrap();
    wait_until(|| dropping.load(Ordering::SeqCst) == 1).await;
    let held = credential.physical_token_slots.available_permits();
    drop_release.send(()).unwrap();
    wait_until(|| credential.physical_token_slots.available_permits() == LIMIT).await;
    assert_eq!(
        held,
        LIMIT - 1,
        "foreign destruction retains the permit while blocked"
    );
    assert_eq!(dropped.load(Ordering::SeqCst), 1);
    assert!(credential.refresh_generation(&generation).await);
    assert_eq!(calls.load(Ordering::SeqCst), 1);
}

#[test]
fn timeout_of_a_queued_refresh_returns_its_slot_without_later_host_invocation() {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .max_blocking_threads(1)
        .build()
        .unwrap();
    let (occupied, ready) = channel();
    let (release, gate) = channel();
    let blocker = runtime.spawn_blocking(move || {
        occupied.send(()).unwrap();
        let _ = gate.recv();
    });
    ready.recv_timeout(Duration::from_secs(5)).unwrap();
    runtime.block_on(async {
        let credential = Credential::new(String::new());
        credential.set_refresh_timeout(Duration::from_secs(10));
        let calls = Arc::new(AtomicUsize::new(0));
        let generation = install_probe(&credential, &calls);
        let owner = Arc::clone(&credential);
        let request = tokio::spawn(async move { owner.refresh_generation(&generation).await });
        wait_until(|| credential.physical_token_slots.available_permits() == LIMIT - 1).await;
        tokio::time::pause();
        tokio::time::advance(Duration::from_secs(11)).await;
        let result = request.await.unwrap();
        tokio::time::resume();
        release.send(()).unwrap();
        blocker.await.unwrap();
        wait_until(|| credential.physical_token_slots.available_permits() == LIMIT).await;
        assert!(!result);
        assert_eq!(
            calls.load(Ordering::SeqCst),
            0,
            "timed-out queued work must not invoke foreign code later"
        );
        let generation = install_probe(&credential, &calls);
        assert!(credential.refresh_generation(&generation).await);
        assert_eq!(calls.load(Ordering::SeqCst), 1);
        assert_eq!(credential.physical_token_slots.available_permits(), LIMIT);
    });
}

#[test]
fn saturated_refreshes_leave_the_smallest_mobile_runtime_able_to_run_other_work() {
    let runtime = crate::runtime::MobileRuntime::new(0).unwrap();
    runtime.block_on(async {
        let credential = Credential::new(String::new());
        credential.set_refresh_timeout(Duration::from_secs(30));
        let entered = Arc::new(AtomicUsize::new(0));
        let finished = Arc::new(AtomicUsize::new(0));
        let mut releases = Vec::new();
        let mut started = 0;
        // The minimum production blocking pool has four threads. Admission
        // must refuse refresh work before all of them become occupied.
        for index in 0..4 {
            let (release, gate) = channel();
            releases.push(release);
            credential.set_provider(Arc::new(HeldCallback {
                entered: Arc::clone(&entered),
                finished: Arc::clone(&finished),
                gate: Mutex::new(gate),
            }));
            let generation = credential.snapshot().generation;
            let owner = Arc::clone(&credential);
            let mut request =
                tokio::spawn(async move { owner.refresh_generation(&generation).await });
            tokio::select! {
                result = &mut request => {
                    assert!(!result.unwrap());
                    break;
                }
                () = wait_until(|| entered.load(Ordering::SeqCst) == index + 1) => {
                    started += 1;
                    install_probe(&credential, &Arc::new(AtomicUsize::new(0)));
                    assert!(!request.await.unwrap());
                }
            }
        }
        let mut other_work = runtime.handle().spawn_blocking(|| 42);
        let progress = tokio::time::timeout(Duration::from_secs(5), &mut other_work).await;
        for release in releases {
            let _ = release.send(());
        }
        wait_until(|| finished.load(Ordering::SeqCst) == started).await;
        if progress.is_err() {
            other_work.await.unwrap();
        }
        assert_eq!(
            progress.ok().and_then(Result::ok),
            Some(42),
            "retired refreshes must leave production executor headroom"
        );
        assert!(started <= LIMIT);
    });
}
