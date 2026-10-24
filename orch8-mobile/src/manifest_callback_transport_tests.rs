use super::*;

struct NetworkProbe {
    addr: std::net::SocketAddr,
    connections: Arc<AtomicUsize>,
    stop: Arc<AtomicBool>,
    thread: Option<std::thread::JoinHandle<()>>,
}
impl NetworkProbe {
    fn new() -> Self {
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();
        listener.set_nonblocking(true).unwrap();
        let connections = Arc::new(AtomicUsize::new(0));
        let seen = Arc::clone(&connections);
        let stop = Arc::new(AtomicBool::new(false));
        let stopping = Arc::clone(&stop);
        let thread = std::thread::spawn(move || {
            while !stopping.load(Ordering::Acquire) {
                match listener.accept() {
                    Ok((socket, _)) => {
                        seen.fetch_add(1, Ordering::SeqCst);
                        drop(socket);
                    }
                    Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                        std::thread::sleep(Duration::from_millis(1));
                    }
                    Err(error) => panic!("test listener failed: {error}"),
                }
            }
        });
        Self {
            addr,
            connections,
            stop,
            thread: Some(thread),
        }
    }

    fn install(&self, engine: &MobileEngine) {
        engine.runtime.block_on(async {
            let client = reqwest::Client::builder()
                .no_proxy()
                .https_only(true)
                .resolve("manifest.example.invalid", self.addr)
                .build()
                .unwrap();
            engine
                .sync_orchestrator
                .lock()
                .await
                .as_ref()
                .unwrap()
                .set_http_client_for_test(client);
        });
    }
}
impl Drop for NetworkProbe {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::Release);
        self.thread.take().unwrap().join().unwrap();
    }
}

#[test]
fn late_manifest_token_cannot_dispatch_network_after_timeout_or_shutdown() {
    for shutdown in [false, true] {
        let (engine, _directory) = engine(if shutdown { 30_000 } else { 1_000 });
        let probe = NetworkProbe::new();
        probe.install(&engine);
        let (entered, started) = mpsc::channel();
        let (release, blocked) = mpsc::channel();
        let syncing = Arc::clone(&engine);
        let (finished, result) = mpsc::channel();
        let caller = std::thread::spawn(move || {
            let _ = finished.send(syncing.sync(
                "https://manifest.example.invalid/manifest.json".into(),
                Some(Arc::new(BlockedToken {
                    entered,
                    release: StdMutex::new(blocked),
                })),
            ));
        });
        let began = started.recv_timeout(Duration::from_secs(5)).is_ok();
        if shutdown {
            engine.shutdown();
        }
        let interrupted = result.recv_timeout(Duration::from_secs(5)).ok();
        let _ = release.send(());
        caller.join().unwrap();
        wait_until(|| engine.credential.available_token_slots() == 2);
        assert!(began);
        assert!(
            interrupted.is_some(),
            "caller must finish while callback is physically held"
        );
        assert_eq!(
            probe.connections.load(Ordering::SeqCst),
            0,
            "late host result must not dispatch a transport connection"
        );
        engine.shutdown();
    }
}

#[test]
fn blank_manifest_token_fails_before_transport_and_probe_detects_normal_dispatch() {
    let (engine, _directory) = engine(500);
    let probe = NetworkProbe::new();
    probe.install(&engine);
    let calls = Arc::new(AtomicUsize::new(0));
    for token in ["", " ", "\t\r\n"] {
        let result = engine.sync(
            "https://manifest.example.invalid/manifest.json".into(),
            Some(counted(&calls, token)),
        );
        assert!(matches!(result, Err(MobileError::InvalidInput { .. })));
    }
    assert_eq!(calls.load(Ordering::SeqCst), 3);
    assert_eq!(probe.connections.load(Ordering::SeqCst), 0);
    // The same production sync path, with a usable token, must reach this
    // listener. No TLS handshake is served, so no remote service is required.
    let _ = engine.sync(
        "https://manifest.example.invalid/manifest.json".into(),
        Some(counted(&calls, "dst_valid")),
    );
    wait_until(|| probe.connections.load(Ordering::SeqCst) > 0);
    engine.shutdown();
}

struct PanickingToken;
impl TokenProvider for PanickingToken {
    fn current_token(&self) -> String {
        panic!("private host callback detail")
    }
    fn refresh_token(&self) -> Result<String, MobileError> {
        unreachable!()
    }
}

#[test]
fn manifest_callback_panic_is_an_error_and_releases_admission() {
    let (engine, _directory) = engine(5_000);
    let probe = NetworkProbe::new();
    probe.install(&engine);
    let result = engine.sync(
        "https://manifest.example.invalid/manifest.json".into(),
        Some(Arc::new(PanickingToken)),
    );
    assert!(
        matches!(result, Err(MobileError::Engine { message }) if message == "manifest token callback failed")
    );
    wait_until(|| engine.credential.available_token_slots() == 2);
    assert_eq!(probe.connections.load(Ordering::SeqCst), 0);
    engine.shutdown();
}
