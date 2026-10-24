use super::*;
use base64::Engine as _;
use std::sync::atomic::AtomicUsize;
use std::sync::mpsc;

fn engine(timeout: u64) -> (Arc<MobileEngine>, tempfile::TempDir) {
    let directory = tempfile::tempdir().unwrap();
    let config = MobileEngineConfig {
        root_public_key: base64::engine::general_purpose::STANDARD.encode(
            ed25519_dalek::SigningKey::from_bytes(&[7; 32])
                .verifying_key()
                .as_bytes(),
        ),
        operation_timeout_ms: timeout,
        max_concurrent_steps: 1,
        ..MobileEngineConfig::default()
    };
    let engine = MobileEngine::new(
        directory
            .path()
            .join("manifest.db")
            .to_string_lossy()
            .into_owned(),
        config,
    )
    .unwrap();
    (engine, directory)
}

struct BlockedToken {
    entered: mpsc::Sender<()>,
    release: StdMutex<mpsc::Receiver<()>>,
}
impl TokenProvider for BlockedToken {
    fn current_token(&self) -> String {
        self.entered.send(()).unwrap();
        let _ = self.release.lock().unwrap().recv();
        "dst_late".into()
    }
    fn refresh_token(&self) -> Result<String, MobileError> {
        Ok("dst_unused".into())
    }
}

fn interrupted_manifest(shutdown: bool) {
    let (engine, _directory) = engine(if shutdown { 30_000 } else { 1_000 });
    let (entered, started) = mpsc::channel();
    let (release, blocked) = mpsc::channel();
    let (finished, result) = mpsc::channel();
    let syncing = Arc::clone(&engine);
    let caller = std::thread::spawn(move || {
        let outcome = syncing.sync(
            "https://manifest.example.invalid/manifest.json".into(),
            Some(Arc::new(BlockedToken {
                entered,
                release: StdMutex::new(blocked),
            })),
        );
        let _ = finished.send(outcome);
    });
    let began = started.recv_timeout(Duration::from_secs(5)).is_ok();
    if shutdown {
        engine.shutdown();
    }
    let interrupted = result.recv_timeout(Duration::from_secs(5)).ok();
    // Always release the physical call before asserting, including on the
    // broken baseline, so a failing test cannot leave a blocked runtime alive.
    let _ = release.send(());
    caller.join().unwrap();
    engine.shutdown();
    assert!(began, "host getter should enter");
    let outcome =
        interrupted.expect("timeout/shutdown must return before the host getter finishes");
    if shutdown {
        assert!(matches!(outcome, Err(MobileError::Shutdown)));
    } else {
        assert!(
            matches!(outcome, Err(MobileError::Engine { message }) if message.contains("timed out"))
        );
    }
}

#[test]
fn manifest_timeout_does_not_wait_for_a_non_cooperative_host_getter() {
    interrupted_manifest(false);
}

#[test]
fn manifest_shutdown_does_not_wait_for_a_non_cooperative_host_getter() {
    interrupted_manifest(true);
}

fn wait_until(predicate: impl Fn() -> bool) {
    let deadline = std::time::Instant::now() + Duration::from_secs(5);
    while !predicate() {
        assert!(
            std::time::Instant::now() < deadline,
            "callback state did not settle"
        );
        std::thread::sleep(Duration::from_millis(1));
    }
}

struct CountedToken {
    calls: Arc<AtomicUsize>,
    refreshes: Arc<AtomicUsize>,
    token: String,
}
impl TokenProvider for CountedToken {
    fn current_token(&self) -> String {
        self.calls.fetch_add(1, Ordering::SeqCst);
        self.token.clone()
    }
    fn refresh_token(&self) -> Result<String, MobileError> {
        self.refreshes.fetch_add(1, Ordering::SeqCst);
        Ok("dst_node_refreshed".into())
    }
}

fn counted(calls: &Arc<AtomicUsize>, token: &str) -> Arc<CountedToken> {
    Arc::new(CountedToken {
        calls: Arc::clone(calls),
        refreshes: Arc::new(AtomicUsize::new(0)),
        token: token.into(),
    })
}

#[test]
fn timed_out_manifest_getters_retain_shared_admission_without_queueing() {
    let (engine, _directory) = engine(1_000);
    let (entered, started) = mpsc::channel();
    let mut releases: Vec<mpsc::Sender<()>> = Vec::new();
    for _ in 0..2 {
        let (release, blocked) = mpsc::channel();
        releases.push(release);
        let provider = Arc::new(BlockedToken {
            entered: entered.clone(),
            release: StdMutex::new(blocked),
        });
        let syncing = Arc::clone(&engine);
        let (finished, result) = mpsc::channel();
        let caller = std::thread::spawn(move || {
            let _ = finished.send(syncing.sync(
                "https://manifest.example.invalid/manifest.json".into(),
                Some(provider),
            ));
        });
        let began = started.recv_timeout(Duration::from_secs(5)).is_ok();
        let result = result.recv_timeout(Duration::from_secs(5)).ok();
        if result.is_none() {
            releases.clear();
        }
        caller.join().unwrap();
        assert!(began);
        assert!(
            matches!(result, Some(Err(MobileError::Engine { message })) if message.contains("timed out"))
        );
    }
    assert_eq!(engine.credential.available_token_slots(), 0);
    let calls = Arc::new(AtomicUsize::new(0));
    for _ in 0..128 {
        let result = engine.sync(
            "https://manifest.example.invalid/manifest.json".into(),
            Some(counted(&calls, "dst_refused")),
        );
        assert!(matches!(result, Err(MobileError::ResourceLimit { .. })));
    }
    assert_eq!(calls.load(Ordering::SeqCst), 0);
    let node = counted(&calls, "dst_node");
    engine.set_token_provider(node.clone());
    let snapshot = engine.credential.request_snapshot().unwrap();
    let refreshed = engine
        .runtime
        .block_on(engine.credential.refresh_generation(&snapshot.generation));
    assert!(
        !refreshed,
        "manifest callbacks and node refresh share physical headroom"
    );
    assert_eq!(node.refreshes.load(Ordering::SeqCst), 0);
    drop(releases);
    wait_until(|| engine.credential.available_token_slots() == 2);
    let token = engine
        .run_with_timeout(
            engine
                .credential
                .current_manifest_token(counted(&calls, "dst_recovered"), &engine.cancel),
        )
        .unwrap();
    assert_eq!(token, "dst_recovered");
    engine.shutdown();
}

struct DropGateToken {
    entered: mpsc::Sender<()>,
    release: StdMutex<mpsc::Receiver<()>>,
}
impl TokenProvider for DropGateToken {
    fn current_token(&self) -> String {
        "dst_ready".into()
    }
    fn refresh_token(&self) -> Result<String, MobileError> {
        Ok("dst_unused".into())
    }
}
impl Drop for DropGateToken {
    fn drop(&mut self) {
        let _ = self.entered.send(());
        let _ = self.release.lock().unwrap().recv();
    }
}

#[test]
fn manifest_admission_survives_foreign_provider_destruction() {
    let (engine, _directory) = engine(1_000);
    let (entered, dropping) = mpsc::channel();
    let (release, blocked) = mpsc::channel();
    let syncing = Arc::clone(&engine);
    let (finished, result) = mpsc::channel();
    let caller = std::thread::spawn(move || {
        let _ = finished.send(syncing.sync(
            "https://manifest.example.invalid/manifest.json".into(),
            Some(Arc::new(DropGateToken {
                entered,
                release: StdMutex::new(blocked),
            })),
        ));
    });
    let began = dropping.recv_timeout(Duration::from_secs(5)).is_ok();
    let result = result.recv_timeout(Duration::from_secs(5)).ok();
    let held = engine.credential.available_token_slots();
    let _ = release.send(());
    caller.join().unwrap();
    wait_until(|| engine.credential.available_token_slots() == 2);
    engine.shutdown();
    assert!(began);
    assert!(
        matches!(result, Some(Err(MobileError::Engine { message })) if message.contains("timed out"))
    );
    assert_eq!(held, 1, "foreign destruction must retain physical capacity");
}

fn queued_manifest_getter(shutdown: bool) {
    let (engine, _directory) = engine(if shutdown { 30_000 } else { 1_000 });
    let mut releases: Vec<mpsc::Sender<()>> = Vec::new();
    let (entered, started) = mpsc::channel();
    for _ in 0..5 {
        let (release, blocked) = mpsc::channel();
        releases.push(release);
        let entered = entered.clone();
        drop(engine.runtime.handle().spawn_blocking(move || {
            let _ = entered.send(());
            let _ = blocked.recv();
        }));
    }
    for _ in 0..5 {
        started.recv_timeout(Duration::from_secs(5)).unwrap();
    }
    let calls = Arc::new(AtomicUsize::new(0));
    let provider = counted(&calls, "dst_should_not_run");
    let syncing = Arc::clone(&engine);
    let (finished, result) = mpsc::channel();
    let caller = std::thread::spawn(move || {
        let _ = finished.send(syncing.sync(
            "https://manifest.example.invalid/manifest.json".into(),
            Some(provider),
        ));
    });
    wait_until(|| engine.credential.available_token_slots() == 1);
    if shutdown {
        engine.shutdown();
    }
    let interrupted = result.recv_timeout(Duration::from_secs(5)).ok();
    drop(releases);
    caller.join().unwrap();
    wait_until(|| engine.credential.available_token_slots() == 2);
    engine.shutdown();
    if shutdown {
        assert!(matches!(interrupted, Some(Err(MobileError::Shutdown))));
    } else {
        assert!(
            matches!(interrupted, Some(Err(MobileError::Engine { message })) if message.contains("timed out"))
        );
    }
    assert_eq!(calls.load(Ordering::SeqCst), 0);
}

#[test]
fn queued_manifest_getter_is_retired_before_host_entry_on_shutdown() {
    queued_manifest_getter(true);
}

#[test]
fn queued_manifest_getter_is_retired_before_host_entry_on_timeout() {
    queued_manifest_getter(false);
}

struct BlockedRefresh {
    entered: mpsc::Sender<()>,
    release: StdMutex<mpsc::Receiver<()>>,
}
impl TokenProvider for BlockedRefresh {
    fn current_token(&self) -> String {
        "dst_node".into()
    }
    fn refresh_token(&self) -> Result<String, MobileError> {
        let _ = self.entered.send(());
        let _ = self.release.lock().unwrap().recv();
        Ok("dst_late_refresh".into())
    }
}

#[test]
fn retired_node_refreshes_limit_manifest_callback_admission() {
    let (engine, _directory) = engine(1_000);
    let (entered, started) = mpsc::channel();
    let mut releases = Vec::new();
    for _ in 0..2 {
        let (release, blocked) = mpsc::channel();
        releases.push(release);
        engine.set_token_provider(Arc::new(BlockedRefresh {
            entered: entered.clone(),
            release: StdMutex::new(blocked),
        }));
        let generation = engine.credential.request_snapshot().unwrap().generation;
        let refreshing = Arc::clone(&engine);
        let (finished, result) = mpsc::channel();
        let caller = std::thread::spawn(move || {
            let outcome = refreshing.run_with_timeout(async {
                Ok(refreshing.credential.refresh_generation(&generation).await)
            });
            let _ = finished.send(outcome);
        });
        let began = started.recv_timeout(Duration::from_secs(5)).is_ok();
        let result = result.recv_timeout(Duration::from_secs(5)).ok();
        if result.is_none() {
            releases.clear();
        }
        caller.join().unwrap();
        assert!(began);
        assert!(
            matches!(result, Some(Err(MobileError::Engine { message })) if message.contains("timed out"))
        );
    }
    let calls = Arc::new(AtomicUsize::new(0));
    let result = engine.sync(
        "https://manifest.example.invalid/manifest.json".into(),
        Some(counted(&calls, "dst_refused")),
    );
    assert!(matches!(result, Err(MobileError::ResourceLimit { .. })));
    assert_eq!(calls.load(Ordering::SeqCst), 0);
    drop(releases);
    wait_until(|| engine.credential.available_token_slots() == 2);
    engine.shutdown();
}

struct LifetimeToken {
    blocked: BlockedToken,
    dropped: Arc<AtomicBool>,
}
impl TokenProvider for LifetimeToken {
    fn current_token(&self) -> String {
        self.blocked.current_token()
    }
    fn refresh_token(&self) -> Result<String, MobileError> {
        self.blocked.refresh_token()
    }
}
impl Drop for LifetimeToken {
    fn drop(&mut self) {
        self.dropped.store(true, Ordering::Release);
    }
}

#[test]
fn engine_can_retire_while_manifest_callback_keeps_its_physical_resources() {
    let (engine, _directory) = engine(1_000);
    let weak = Arc::downgrade(&engine);
    let dropped = Arc::new(AtomicBool::new(false));
    let (entered, started) = mpsc::channel();
    let (release, blocked) = mpsc::channel();
    let provider = Arc::new(LifetimeToken {
        blocked: BlockedToken {
            entered,
            release: StdMutex::new(blocked),
        },
        dropped: Arc::clone(&dropped),
    });
    let syncing = Arc::clone(&engine);
    let (finished, result) = mpsc::channel();
    let caller = std::thread::spawn(move || {
        let _ = finished.send(syncing.sync(
            "https://manifest.example.invalid/manifest.json".into(),
            Some(provider),
        ));
    });
    let began = started.recv_timeout(Duration::from_secs(5)).is_ok();
    let outcome = result.recv_timeout(Duration::from_secs(5)).ok();
    if outcome.is_none() {
        let _ = release.send(());
    }
    caller.join().unwrap();
    let held = !dropped.load(Ordering::Acquire);
    let (finished, retired) = mpsc::channel();
    let retirement_thread = std::thread::spawn(move || {
        drop(engine);
        let _ = finished.send(());
    });
    let retired_without_waiting = retired.recv_timeout(Duration::from_secs(5)).is_ok();
    let engine_gone = weak.upgrade().is_none();
    let _ = release.send(());
    retirement_thread.join().unwrap();
    wait_until(|| dropped.load(Ordering::Acquire));
    assert!(began && held && retired_without_waiting && engine_gone);
    assert!(
        matches!(outcome, Some(Err(MobileError::Engine { message })) if message.contains("timed out"))
    );
}

#[path = "manifest_callback_transport_tests.rs"]
mod transport_tests;
