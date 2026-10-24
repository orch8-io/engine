use super::*;
use std::sync::Mutex;
use std::sync::atomic::AtomicU32;

struct Rotating {
    next: Mutex<Vec<String>>,
    refreshes: AtomicU32,
}

impl TokenProvider for Rotating {
    fn current_token(&self) -> String {
        "dst_first".into()
    }
    fn refresh_token(&self) -> Result<String, crate::MobileError> {
        self.refreshes.fetch_add(1, Ordering::SeqCst);
        self.next
            .lock()
            .unwrap()
            .pop()
            .ok_or(crate::MobileError::Engine {
                message: "no more tokens".into(),
            })
    }
}

#[tokio::test]
async fn refresh_is_single_flight_per_stale_token() {
    let credential = Credential::new(String::new());
    assert!(!credential.is_configured());
    let provider = Arc::new(Rotating {
        next: Mutex::new(vec!["dst_second".into()]),
        refreshes: AtomicU32::new(0),
    });
    credential.set_provider(provider.clone());
    assert!(credential.is_configured());
    assert!(credential.is_device_session());
    assert_eq!(credential.current(), "dst_first");
    let stale = credential.snapshot().generation;

    assert!(credential.refresh_after_unauthorized(&stale).await);
    assert_eq!(credential.current(), "dst_second");
    // A second request that also saw the stale token does not refresh
    // again: the token already moved on.
    assert!(credential.refresh_after_unauthorized(&stale).await);
    assert_eq!(provider.refreshes.load(Ordering::SeqCst), 1);
    // The provider has nothing newer: no retry.
    assert!(
        !credential
            .refresh_after_unauthorized(&credential.snapshot().generation)
            .await
    );
}

struct Scripted {
    refreshes: AtomicU32,
    hang: bool,
}

impl TokenProvider for Scripted {
    fn current_token(&self) -> String {
        "dst_first".into()
    }
    fn refresh_token(&self) -> Result<String, crate::MobileError> {
        self.refreshes.fetch_add(1, Ordering::SeqCst);
        if self.hang {
            std::thread::sleep(Duration::from_millis(400));
        }
        // The backend keeps handing back the rejected token.
        Ok("dst_first".into())
    }
}

#[test]
fn mc12_refresh_backoff_doubles_and_caps() {
    assert_eq!(refresh_backoff(1), Duration::from_secs(5));
    assert_eq!(refresh_backoff(2), Duration::from_secs(10));
    assert_eq!(refresh_backoff(3), Duration::from_secs(20));
    assert_eq!(refresh_backoff(50), REFRESH_BACKOFF_MAX);
}

#[tokio::test]
async fn mc12_same_token_refresh_is_negative_cached_until_backoff_elapses() {
    let credential = Credential::new(String::new());
    let provider = Arc::new(Scripted {
        refreshes: AtomicU32::new(0),
        hang: false,
    });
    credential.set_provider(provider.clone());

    let stale = credential.snapshot().generation;
    // Every 401 sees the same rejected token and the provider returns it.
    for _ in 0..5 {
        assert!(!credential.refresh_after_unauthorized(&stale).await);
    }
    assert_eq!(
        provider.refreshes.load(Ordering::SeqCst),
        1,
        "the host backend must not be hit on every 401"
    );

    // Backoff elapses.
    credential
        .state
        .write()
        .unwrap()
        .refresh_failure
        .as_mut()
        .unwrap()
        .retry_at = Instant::now();
    assert!(!credential.refresh_after_unauthorized(&stale).await);
    assert_eq!(
        provider.refreshes.load(Ordering::SeqCst),
        2,
        "retried after backoff"
    );
}

#[tokio::test]
async fn mc12_a_hung_provider_times_out_instead_of_holding_the_lock() {
    let credential = Credential::new(String::new());
    credential.set_refresh_timeout(Duration::from_millis(50));
    let provider = Arc::new(Scripted {
        refreshes: AtomicU32::new(0),
        hang: true,
    });
    credential.set_provider(provider);
    let stale = credential.snapshot().generation;

    let started = std::time::Instant::now();
    assert!(!credential.refresh_after_unauthorized(&stale).await);
    assert!(
        started.elapsed() < Duration::from_millis(350),
        "must give up at the timeout, not wait for the provider"
    );
}

#[tokio::test]
async fn a_static_key_without_provider_never_refreshes() {
    let credential = Credential::new("sk_static".into());
    assert!(credential.is_configured());
    assert!(!credential.is_device_session());
    assert!(
        !credential
            .refresh_after_unauthorized(&credential.snapshot().generation)
            .await
    );
    assert_eq!(credential.current(), "sk_static");
    assert_eq!(format!("{credential:?}"), "Credential(<redacted>)");
}

struct Fixed {
    token: &'static str,
    fresh: &'static str,
    refreshes: AtomicU32,
}
impl TokenProvider for Fixed {
    fn current_token(&self) -> String {
        self.token.into()
    }
    fn refresh_token(&self) -> Result<String, crate::MobileError> {
        self.refreshes.fetch_add(1, Ordering::SeqCst);
        Ok(self.fresh.into())
    }
}

struct GatedRefresh {
    entered: Arc<AtomicBool>,
    finished: Arc<AtomicBool>,
    gate: Mutex<std::sync::mpsc::Receiver<()>>,
}
impl TokenProvider for GatedRefresh {
    fn current_token(&self) -> String {
        "dst_old".into()
    }
    fn refresh_token(&self) -> Result<String, crate::MobileError> {
        self.entered.store(true, Ordering::SeqCst);
        let _ = self
            .gate
            .lock()
            .unwrap()
            .recv_timeout(Duration::from_secs(3));
        self.finished.store(true, Ordering::SeqCst);
        Ok("dst_old_refreshed".into())
    }
}

async fn wait_for_credential(mut predicate: impl FnMut() -> bool) {
    for _ in 0..400 {
        if predicate() {
            return;
        }
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    panic!("credential condition not met");
}

#[test]
fn empty_replacement_provider_clears_the_bootstrap_credential() {
    let credential = Credential::new("sk_operator_bootstrap".into());
    credential.set_provider(Arc::new(Fixed {
        token: "",
        fresh: "dst_new",
        refreshes: AtomicU32::new(0),
    }));
    assert_eq!(
        credential.current(),
        "",
        "an empty replacement must not retain the old operator key"
    );
    assert!(credential.is_configured());
}

#[tokio::test]
async fn replacing_provider_revokes_pending_refresh_and_prevents_late_publication() {
    let server =
        crate::test_support::spawn_control_plane(Arc::new(|_, _, _| (401, serde_json::json!({}))))
            .await;
    let credential = Credential::new(String::new());
    let entered = Arc::new(AtomicBool::new(false));
    let finished = Arc::new(AtomicBool::new(false));
    let (release, gate) = std::sync::mpsc::channel();
    credential.set_provider(Arc::new(GatedRefresh {
        entered: Arc::clone(&entered),
        finished: Arc::clone(&finished),
        gate: Mutex::new(gate),
    }));
    let owner = Arc::clone(&credential);
    let base = server.base.clone();
    let mut request = tokio::spawn(async move {
        let http = reqwest::Client::new();
        owner
            .send(|token| {
                http.post(&base)
                    .header("x-api-key", token)
                    .json(&serde_json::json!({"private":"old"}))
            })
            .await
            .unwrap()
    });
    wait_for_credential(|| entered.load(Ordering::SeqCst)).await;
    credential.set_provider(Arc::new(Fixed {
        token: "dst_new",
        fresh: "dst_new_fresh",
        refreshes: AtomicU32::new(0),
    }));
    let cancelled_promptly = tokio::time::timeout(Duration::from_millis(250), &mut request).await;
    let returned_early = cancelled_promptly.is_ok();
    release.send(()).unwrap();
    if cancelled_promptly.is_err() {
        request.await.unwrap();
    }
    wait_for_credential(|| finished.load(Ordering::SeqCst)).await;
    assert!(
        returned_early,
        "replacement must not wait for the old blocking callback"
    );
    assert_eq!(
        credential.current(),
        "dst_new",
        "a superseded refresh cannot publish into the replacement provider"
    );
    assert_eq!(server.requests.lock().unwrap().len(), 1);
}

#[tokio::test]
async fn delayed_unauthorized_request_is_not_replayed_under_a_replacement_provider() {
    let gate = Arc::new(tokio::sync::Semaphore::new(0));
    let calls = Arc::new(AtomicU32::new(0));
    let count = Arc::clone(&calls);
    let server = crate::test_support::spawn_control_plane_gated(
        Arc::new(move |_, _, _| {
            (
                if count.fetch_add(1, Ordering::SeqCst) == 0 {
                    401
                } else {
                    200
                },
                serde_json::json!({}),
            )
        }),
        Some(("/private".into(), Arc::clone(&gate))),
    )
    .await;
    let credential = Credential::new(String::new());
    credential.set_provider(Arc::new(Fixed {
        token: "dst_account_a",
        fresh: "dst_a_fresh",
        refreshes: AtomicU32::new(0),
    }));
    let owner = Arc::clone(&credential);
    let url = format!("{}/private", server.base);
    let tokens = Arc::new(Mutex::new(Vec::new()));
    let observed = Arc::clone(&tokens);
    let request = tokio::spawn(async move {
        let http = reqwest::Client::new();
        owner
            .send(|token| {
                observed.lock().unwrap().push(token.to_owned());
                http.post(&url)
                    .header("x-api-key", token)
                    .json(&serde_json::json!({"private":"account-a"}))
            })
            .await
            .unwrap()
    });
    wait_for_credential(|| server.count("/private") == 1).await;
    credential.set_provider(Arc::new(Fixed {
        token: "dst_account_b",
        fresh: "dst_b_fresh",
        refreshes: AtomicU32::new(0),
    }));
    gate.add_permits(1);
    let response = request.await.unwrap();
    assert_eq!(
        response.status(),
        401,
        "the old request must remain rejected instead of adopting replacement authorization"
    );
    assert_eq!(*tokens.lock().unwrap(), vec!["dst_account_a"]);
    assert_eq!(server.count("/private"), 1);
}

#[tokio::test]
async fn reinstalling_the_same_token_does_not_inherit_an_old_providers_backoff() {
    let server = crate::test_support::spawn_control_plane(Arc::new(|_, _, body| {
        (
            if body["token"] == "dst_fresh" {
                200
            } else {
                401
            },
            serde_json::json!({}),
        )
    }))
    .await;
    let credential = Credential::new(String::new());
    credential.set_provider(Arc::new(Fixed {
        token: "dst_same",
        fresh: "dst_same",
        refreshes: AtomicU32::new(0),
    }));
    let http = reqwest::Client::new();
    assert_eq!(
        credential
            .send(|token| http
                .post(&server.base)
                .json(&serde_json::json!({"token":token})))
            .await
            .unwrap()
            .status(),
        401
    );
    let replacement = Arc::new(Fixed {
        token: "dst_same",
        fresh: "dst_fresh",
        refreshes: AtomicU32::new(0),
    });
    credential.set_provider(replacement.clone());
    let response = credential
        .send(|token| {
            http.post(&server.base)
                .json(&serde_json::json!({"token":token}))
        })
        .await
        .unwrap();
    assert_eq!(
        response.status(),
        200,
        "a new provider must get its own refresh attempt even when its initial token has the same bytes"
    );
    assert_eq!(replacement.refreshes.load(Ordering::SeqCst), 1);
}

#[test]
fn a_slow_older_provider_installation_cannot_replace_a_newer_installation() {
    struct SlowCurrent {
        entered: std::sync::mpsc::Sender<()>,
        gate: Mutex<std::sync::mpsc::Receiver<()>>,
    }
    impl TokenProvider for SlowCurrent {
        fn current_token(&self) -> String {
            self.entered.send(()).unwrap();
            let _ = self
                .gate
                .lock()
                .unwrap()
                .recv_timeout(Duration::from_secs(3));
            "dst_older_install".into()
        }
        fn refresh_token(&self) -> Result<String, crate::MobileError> {
            Ok("dst_older_refresh".into())
        }
    }
    let credential = Credential::new(String::new());
    let (entered, ready) = std::sync::mpsc::channel();
    let (release, gate) = std::sync::mpsc::channel();
    let owner = Arc::clone(&credential);
    let old = std::thread::spawn(move || {
        owner.set_provider(Arc::new(SlowCurrent {
            entered,
            gate: Mutex::new(gate),
        }));
    });
    ready.recv_timeout(Duration::from_secs(2)).unwrap();
    credential.set_provider(Arc::new(Fixed {
        token: "dst_newer_install",
        fresh: "dst_newer_refresh",
        refreshes: AtomicU32::new(0),
    }));
    release.send(()).unwrap();
    old.join().unwrap();
    assert_eq!(credential.current(), "dst_newer_install");
}

#[tokio::test]
async fn shared_auth_lease_request_stays_with_its_original_provider() {
    let gate = Arc::new(tokio::sync::Semaphore::new(0));
    let calls = Arc::new(AtomicU32::new(0));
    let count = Arc::clone(&calls);
    let server = crate::test_support::spawn_control_plane_gated(
        Arc::new(move |_, _, _| {
            (
                if count.fetch_add(1, Ordering::SeqCst) == 0 {
                    401
                } else {
                    200
                },
                serde_json::json!({}),
            )
        }),
        Some(("/complete".into(), Arc::clone(&gate))),
    )
    .await;
    let credential = Credential::new(String::new());
    credential.set_provider(Arc::new(Fixed {
        token: "dst_account_a",
        fresh: "dst_a_fresh",
        refreshes: AtomicU32::new(0),
    }));
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
    let id = uuid::Uuid::new_v4();
    let request = tokio::spawn(async move {
        client
            .complete(id, 7, &serde_json::json!({"private":"account-a"}))
            .await
    });
    wait_for_credential(|| server.count("/complete") == 1).await;
    credential.set_provider(Arc::new(Fixed {
        token: "dst_account_b",
        fresh: "dst_b_fresh",
        refreshes: AtomicU32::new(0),
    }));
    gate.add_permits(1);
    assert_eq!(request.await.unwrap(), crate::node::LeaseResponse::Rejected);
    assert_eq!(
        server.header_values("/complete", "x-api-key"),
        vec!["dst_account_a"]
    );
    assert_eq!(server.count("/complete"), 1);
}

#[tokio::test]
async fn a_suspended_old_request_does_not_hold_the_new_providers_refresh_lock() {
    let server = crate::test_support::spawn_control_plane(Arc::new(|_, _, body| {
        (
            if body["token"] == "dst_new_fresh" {
                200
            } else {
                401
            },
            serde_json::json!({}),
        )
    }))
    .await;
    let credential = Credential::new(String::new());
    let entered = Arc::new(AtomicBool::new(false));
    let finished = Arc::new(AtomicBool::new(false));
    let (release, gate) = std::sync::mpsc::channel();
    credential.set_provider(Arc::new(GatedRefresh {
        entered: Arc::clone(&entered),
        finished: Arc::clone(&finished),
        gate: Mutex::new(gate),
    }));
    let http = reqwest::Client::new();
    let mut old = Box::pin(credential.send(|token| {
        http.post(&server.base)
            .json(&serde_json::json!({"token":token}))
    }));
    tokio::select! {
        answer = &mut old => panic!("old request finished before its callback: {answer:?}"),
        () = wait_for_credential(|| entered.load(Ordering::SeqCst)) => {}
    }
    // Keep the old future allocated but stop polling it. Cancellation must
    // not require that future to release a mutex shared with its successor.
    let replacement = Arc::new(Fixed {
        token: "dst_new",
        fresh: "dst_new_fresh",
        refreshes: AtomicU32::new(0),
    });
    credential.set_provider(replacement.clone());
    let next = tokio::time::timeout(
        Duration::from_millis(250),
        credential.send(|token| {
            http.post(&server.base)
                .json(&serde_json::json!({"token":token}))
        }),
    )
    .await;
    let status = next
        .ok()
        .and_then(Result::ok)
        .map(|response| response.status().as_u16());
    drop(old);
    release.send(()).unwrap();
    wait_for_credential(|| finished.load(Ordering::SeqCst)).await;
    assert_eq!(
        status,
        Some(200),
        "a new provider needs an independent single-flight domain"
    );
    assert_eq!(replacement.refreshes.load(Ordering::SeqCst), 1);
    assert_eq!(credential.current(), "dst_new_fresh");
}

#[tokio::test]
async fn a_refresh_started_during_installation_fences_the_older_current_token_callback() {
    struct SlowInitial {
        entered: Arc<AtomicBool>,
        gate: Mutex<std::sync::mpsc::Receiver<()>>,
    }
    impl TokenProvider for SlowInitial {
        fn current_token(&self) -> String {
            self.entered.store(true, Ordering::SeqCst);
            let _ = self
                .gate
                .lock()
                .unwrap()
                .recv_timeout(Duration::from_secs(3));
            "dst_initial_stale".into()
        }
        fn refresh_token(&self) -> Result<String, crate::MobileError> {
            Ok("dst_refresh_fresh".into())
        }
    }
    let server = crate::test_support::spawn_control_plane(Arc::new(|_, _, body| {
        (
            if body["token"] == "dst_refresh_fresh" {
                200
            } else {
                401
            },
            serde_json::json!({}),
        )
    }))
    .await;
    let credential = Credential::new("sk_bootstrap".into());
    let entered = Arc::new(AtomicBool::new(false));
    let (release, gate) = std::sync::mpsc::channel();
    let owner = Arc::clone(&credential);
    let provider = Arc::new(SlowInitial {
        entered: Arc::clone(&entered),
        gate: Mutex::new(gate),
    });
    let installing = std::thread::spawn(move || owner.set_provider(provider));
    wait_for_credential(|| entered.load(Ordering::SeqCst)).await;
    let http = reqwest::Client::new();
    let response = credential
        .send(|token| {
            http.post(&server.base)
                .header("x-api-key", token)
                .json(&serde_json::json!({"token":token}))
        })
        .await
        .unwrap();
    release.send(()).unwrap();
    installing.join().unwrap();
    assert_eq!(response.status(), 200);
    assert_eq!(credential.current(), "dst_refresh_fresh");
    assert_eq!(
        server.header_values("", "x-api-key"),
        vec!["", "dst_refresh_fresh"]
    );
}

#[tokio::test]
async fn same_provider_refresh_still_retries_lease_payloads_once() {
    let calls = Arc::new(AtomicU32::new(0));
    let count = Arc::clone(&calls);
    let server = crate::test_support::spawn_control_plane(Arc::new(move |_, _, _| {
        (
            if count.fetch_add(1, Ordering::SeqCst) == 0 {
                401
            } else {
                200
            },
            serde_json::json!({}),
        )
    }))
    .await;
    let credential = Credential::new(String::new());
    let provider = Arc::new(Fixed {
        token: "dst_current",
        fresh: "dst_refreshed",
        refreshes: AtomicU32::new(0),
    });
    credential.set_provider(provider.clone());
    let client = crate::node::NodeClient::new_unchecked(
        server.base.clone(),
        credential,
        "device".into(),
        orch8_types::continuity::RuntimeId::new(),
        crate::node::Advertisement {
            handlers: vec!["scan".into()],
            caps: crate::node::NodeCapabilities::default(),
            draining: false,
        },
    );
    let id = uuid::Uuid::new_v4();
    assert_eq!(
        client
            .complete(id, 7, &serde_json::json!({"receipt":"known"}))
            .await,
        crate::node::LeaseResponse::Accepted
    );
    assert_eq!(
        server.header_values("/complete", "x-api-key"),
        vec!["dst_current", "dst_refreshed"]
    );
    assert_eq!(server.bodies("/complete")[0], server.bodies("/complete")[1]);
    assert_eq!(provider.refreshes.load(Ordering::SeqCst), 1);
}

struct BlockingRefresh {
    calls: AtomicU32,
    finished: AtomicU32,
    gate: Mutex<std::sync::mpsc::Receiver<()>>,
}
impl TokenProvider for BlockingRefresh {
    fn current_token(&self) -> String {
        "dst_old".into()
    }
    fn refresh_token(&self) -> Result<String, crate::MobileError> {
        let call = self.calls.fetch_add(1, Ordering::SeqCst);
        let _ = self
            .gate
            .lock()
            .unwrap()
            .recv_timeout(Duration::from_secs(3));
        self.finished.fetch_add(1, Ordering::SeqCst);
        Ok(if call == 0 {
            "dst_old"
        } else {
            "dst_recovered"
        }
        .into())
    }
}

#[tokio::test]
async fn timed_out_refresh_remains_physically_single_flight_until_return() {
    for reinstall in [false, true] {
        let server = crate::test_support::spawn_control_plane(Arc::new(|_, _, body| {
            (
                if body["token"] == "dst_recovered" {
                    200
                } else {
                    401
                },
                serde_json::json!({}),
            )
        }))
        .await;
        let credential = Credential::new(String::new());
        credential.set_refresh_timeout(Duration::from_millis(50));
        let (release, gate) = std::sync::mpsc::channel();
        let provider = Arc::new(BlockingRefresh {
            calls: AtomicU32::new(0),
            finished: AtomicU32::new(0),
            gate: Mutex::new(gate),
        });
        credential.set_provider(provider.clone());
        let http = reqwest::Client::new();
        assert_eq!(
            credential
                .send(|token| http
                    .post(&server.base)
                    .json(&serde_json::json!({"token":token})))
                .await
                .unwrap()
                .status(),
            401
        );
        if reinstall {
            credential.set_provider(Arc::new(Fixed {
                token: "dst_other",
                fresh: "dst_other_fresh",
                refreshes: AtomicU32::new(0),
            }));
            credential.set_provider(provider.clone());
        } else {
            credential
                .state
                .write()
                .unwrap()
                .refresh_failure
                .as_mut()
                .unwrap()
                .retry_at = Instant::now();
        }
        assert_eq!(
            credential
                .send(|token| http
                    .post(&server.base)
                    .json(&serde_json::json!({"token":token})))
                .await
                .unwrap()
                .status(),
            401
        );
        let started = provider.calls.load(Ordering::SeqCst);
        release.send(()).unwrap();
        release.send(()).unwrap();
        wait_for_credential(|| {
            provider.finished.load(Ordering::SeqCst) == started
                && !credential
                    .state
                    .read()
                    .unwrap()
                    .physical_refresh
                    .load(Ordering::Acquire)
        })
        .await;
        assert_eq!(
            started, 1,
            "a timeout must not admit a second physical callback for the same provider"
        );
        if let Some(failure) = credential.state.write().unwrap().refresh_failure.as_mut() {
            failure.retry_at = Instant::now();
        }
        assert_eq!(
            credential
                .send(|token| http
                    .post(&server.base)
                    .json(&serde_json::json!({"token":token})))
                .await
                .unwrap()
                .status(),
            200
        );
        assert_eq!(provider.calls.load(Ordering::SeqCst), 2);
    }
}

#[test]
fn timed_out_queued_refresh_never_invokes_the_host_after_capacity_returns() {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .max_blocking_threads(1)
        .build()
        .unwrap();
    let (occupied, ready) = std::sync::mpsc::channel();
    let (release, gate) = std::sync::mpsc::channel();
    let blocker = runtime.spawn_blocking(move || {
        occupied.send(()).unwrap();
        let _ = gate.recv_timeout(Duration::from_secs(3));
    });
    ready.recv_timeout(Duration::from_secs(2)).unwrap();
    runtime.block_on(async {
        let server = crate::test_support::spawn_control_plane(Arc::new(|_, _, _| {
            (401, serde_json::json!({}))
        }))
        .await;
        let credential = Credential::new(String::new());
        credential.set_refresh_timeout(Duration::from_millis(20));
        let provider = Arc::new(Fixed {
            token: "dst_old",
            fresh: "dst_fresh",
            refreshes: AtomicU32::new(0),
        });
        credential.set_provider(provider.clone());
        let http = reqwest::Client::new();
        assert_eq!(
            credential
                .send(|token| http.post(&server.base).header("x-api-key", token))
                .await
                .unwrap()
                .status(),
            401
        );
        release.send(()).unwrap();
        blocker.await.unwrap();
        wait_for_credential(|| {
            !credential
                .state
                .read()
                .unwrap()
                .physical_refresh
                .load(Ordering::Acquire)
        })
        .await;
        assert_eq!(
            provider.refreshes.load(Ordering::SeqCst),
            0,
            "a queued callback cancelled at timeout must never run later"
        );
    });
}

#[test]
fn retired_provider_destructors_can_reenter_credential_reads() {
    struct Reentrant {
        credential: std::sync::Weak<Credential>,
        responsive: Arc<AtomicBool>,
    }
    impl TokenProvider for Reentrant {
        fn current_token(&self) -> String {
            "dst_retired".into()
        }
        fn refresh_token(&self) -> Result<String, crate::MobileError> {
            Ok("dst_refreshed".into())
        }
    }
    impl Drop for Reentrant {
        fn drop(&mut self) {
            let credential = self.credential.upgrade().unwrap();
            let (completed, wait) = std::sync::mpsc::channel();
            std::thread::spawn(move || {
                let _ = credential.current();
                let _ = completed.send(());
            });
            self.responsive.store(
                wait.recv_timeout(Duration::from_secs(1)).is_ok(),
                Ordering::SeqCst,
            );
        }
    }
    let credential = Credential::new(String::new());
    let responsive = Arc::new(AtomicBool::new(false));
    credential.set_provider(Arc::new(Reentrant {
        credential: Arc::downgrade(&credential),
        responsive: Arc::clone(&responsive),
    }));
    credential.set_provider(Arc::new(Fixed {
        token: "dst_new",
        fresh: "dst_new_fresh",
        refreshes: AtomicU32::new(0),
    }));
    assert!(
        responsive.load(Ordering::SeqCst),
        "foreign destruction must happen outside the credential lock"
    );
    assert_eq!(credential.current(), "dst_new");
}
