use super::*;
use crate::{node::claim_payloads::tests::payloads, storage::scopes::peers::WorkerPeerDescriptor};
use orch8_storage::sqlite::SqliteStorage;
use serde_json::json;
use std::sync::{Condvar, Mutex};
use tokio::sync::Notify;

pub(super) struct State {
    pub(super) bad: &'static str,
    pub(super) seen: Vec<(String, String)>,
    pub(super) hold: &'static str,
}
pub(super) struct Fixture {
    pub(super) reader: Arc<ScopedPayloadReader>,
    pub(super) task: RemoteTask,
    pub(super) state: Arc<Mutex<State>>,
    pub(super) entered: Arc<Notify>,
    pub(super) release: Arc<Notify>,
    server: tokio::task::JoinHandle<()>,
}
impl Drop for Fixture {
    fn drop(&mut self) {
        self.server.abort();
    }
}
async fn request(stream: &mut tokio::net::TcpStream) -> Option<(String, String, Vec<u8>)> {
    use tokio::io::AsyncReadExt;
    let mut bytes = Vec::new();
    let mut buffer = [0u8; 1024];
    let header_end = loop {
        let count = stream.read(&mut buffer).await.unwrap();
        if count == 0 {
            break 0;
        }
        bytes.extend_from_slice(&buffer[..count]);
        assert!(bytes.len() < 8192);
        if let Some(at) = bytes.windows(4).position(|s| s == b"\r\n\r\n") {
            break at + 4;
        }
    };
    if header_end == 0 {
        return None;
    }
    let headers = String::from_utf8(bytes[..header_end].to_vec()).unwrap();
    let path = headers.split_whitespace().nth(1).unwrap().to_owned();
    let len = headers
        .lines()
        .find_map(|h| {
            h.to_lowercase()
                .strip_prefix("content-length:")
                .and_then(|s| s.trim().parse::<usize>().ok())
        })
        .unwrap_or(0);
    while bytes.len() < header_end + len {
        let count = stream.read(&mut buffer).await.unwrap();
        assert!(count > 0);
        bytes.extend_from_slice(&buffer[..count]);
        assert!(bytes.len() < 8192);
    }
    let token = headers
        .lines()
        .find_map(|h| h.strip_prefix("x-api-key: "))
        .unwrap_or("")
        .to_owned();
    Some((path, token, bytes[header_end..header_end + len].to_vec()))
}

fn corrupt(body: &mut serde_json::Value, bad: &str, identity: bool, status: &mut u16) {
    if identity {
        match bad {
            "worker" => body["runtime_id"] = json!(uuid::Uuid::new_v4()),
            "tenant" => body["tenant_id"] = json!("other"),
            "device" => body["device_id"] = json!("other"),
            "nonce" => body["nonce"] = json!(uuid::Uuid::new_v4()),
            "expired" => {
                body["expires_at"] = json!(chrono::Utc::now() - chrono::Duration::seconds(1));
            }
            "status" => {
                *status = 403;
                *body = json!({"secret":"PRIVATE_IDENTITY_BODY"});
            }
            _ => {}
        }
    } else if bad == "input" {
        body["payload_base64"] = json!("PRIVATE_BAD_PAYLOAD");
    }
}

fn check_heartbeat(
    path: &str,
    bytes: &[u8],
    scope_copy: &orch8_types::delegation_definition::wire::DefinitionReadScope,
) {
    assert_eq!(
        path,
        format!("/api/v1/workers/tasks/{}/heartbeat", scope_copy.task_id)
    );
    let mutation: serde_json::Value = serde_json::from_slice(bytes).unwrap();
    assert_eq!(mutation["worker_id"], json!(scope_copy.worker_id));
    assert_eq!(
        mutation["claim_epoch"].as_u64(),
        Some(scope_copy.claim_epoch)
    );
}
async fn respond(stream: &mut tokio::net::TcpStream, status: u16, body: &serde_json::Value) {
    use tokio::io::AsyncWriteExt;
    let body = serde_json::to_vec(body).unwrap();
    let head = format!(
        "HTTP/1.1 {status} Response\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
        body.len()
    );
    if stream.write_all(head.as_bytes()).await.is_ok() {
        let _ = stream.write_all(&body).await;
    }
    let _ = stream.shutdown().await;
}

pub(super) async fn fixture() -> Fixture {
    let storage = Arc::new(MobileStorage::new(Arc::new(
        SqliteStorage::in_memory().await.unwrap(),
    )));
    let account = storage
        .open_account_scope("alice", "issuer", "alice")
        .await
        .unwrap();
    let scope = storage
        .open_catalog_scope(&account, "work", "source", "authority")
        .await
        .unwrap();
    let runtime = orch8_types::continuity::RuntimeId::new();
    let (task, definition, input) = payloads(&runtime.to_string());
    let tenant = task.delegation.as_ref().unwrap().scope.tenant_id.as_str();
    let peer = storage
        .bind_worker_peer(
            &scope,
            WorkerPeerDescriptor {
                peer_key: "backend",
                api_base: "https://engine.example/api/v1",
                remote_tenant: tenant,
                runtime_id: runtime,
            },
        )
        .await
        .unwrap();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let base = format!("http://{}/api/v1/", listener.local_addr().unwrap());
    let mut reader = ScopedPayloadReader::new(storage, peer, "device".into()).unwrap();
    let owned = Arc::get_mut(&mut reader).unwrap();
    owned.base = base;
    owned.budget = Arc::new(Semaphore::new(1));
    let state = Arc::new(Mutex::new(State {
        bad: "",
        seen: Vec::new(),
        hold: "",
    }));
    let received = state.clone();
    let entered = Arc::new(Notify::new());
    let release = Arc::new(Notify::new());
    let entering = entered.clone();
    let releasing = release.clone();
    let scope_copy = task.delegation.as_ref().unwrap().scope.clone();
    let server = tokio::spawn(async move {
        loop {
            let (mut stream, _) = listener.accept().await.unwrap();
            let Some((path, token, bytes)) = request(&mut stream).await else {
                continue;
            };
            let identity = path.ends_with("/workers/identity");
            let heartbeat = path.ends_with("/heartbeat");
            if heartbeat {
                check_heartbeat(&path, &bytes, &scope_copy);
            }
            let (bad, hold) = {
                let mut state = received.lock().unwrap();
                state.seen.push((path.clone(), token));
                (state.bad, state.hold)
            };
            if (hold == "identity" && identity)
                || (hold == "payload" && !identity && !heartbeat)
                || (hold == "heartbeat" && heartbeat)
            {
                entering.notify_one();
                releasing.notified().await;
            }
            let mut status = 200;
            let mut body = if identity {
                let challenge: WorkerIdentityRequest = serde_json::from_slice(&bytes).unwrap();
                let now = chrono::Utc::now();
                json!({"version":1,"nonce":challenge.nonce,"tenant_id":scope_copy.tenant_id,"runtime_id":runtime,"kind":"mobile","device_id":"device","issued_at":now,"expires_at":now+chrono::Duration::seconds(60)})
            } else if heartbeat {
                if bad == "heartbeat-denied" {
                    status = 401;
                }
                if bad == "heartbeat-lost" {
                    status = 409;
                }
                json!({})
            } else if path.contains("/definition?") {
                serde_json::to_value(&definition).unwrap()
            } else {
                serde_json::to_value(&input).unwrap()
            };
            corrupt(&mut body, bad, identity, &mut status);
            respond(&mut stream, status, &body).await;
        }
    });
    Fixture {
        reader,
        task,
        state,
        entered,
        release,
        server,
    }
}
#[tokio::test]
async fn each_payload_uses_a_fresh_proof_and_the_exact_same_token() {
    let f = fixture().await;
    f.reader
        .definition(&f.task, "dst_alice", CancellationToken::new())
        .await
        .unwrap();
    let input = f
        .reader
        .input(&f.task, "dst_next", CancellationToken::new())
        .await
        .unwrap();
    let context = f.task.delegation.as_ref().unwrap();
    let verified = input
        .verify(
            &context.scope,
            &context.request_sha256,
            &context.input,
            InputLimits::default(),
        )
        .unwrap();
    assert_eq!(verified.value()["wide"].as_u64(), Some(u64::MAX));
    {
        let seen = &f.state.lock().unwrap().seen;
        assert_eq!(seen.len(), 4);
        assert_eq!(seen[0].1, "dst_alice");
        assert_eq!(seen[1].1, "dst_alice");
        assert_eq!(seen[2].1, "dst_next");
        assert_eq!(seen[3].1, "dst_next");
        assert!(seen[3].0.contains("claim_epoch=9007199254740993"));
    }
    assert!(
        f.reader
            .storage
            .check_worker_peer_device(&f.reader.peer, "another-device")
            .await
            .is_err()
    );
}
#[tokio::test]
async fn wrong_runtime_tenant_device_nonce_expiry_or_role_never_fetches_payload() {
    for bad in ["worker", "tenant", "device", "nonce", "expired", "status"] {
        let f = fixture().await;
        f.state.lock().unwrap().bad = bad;
        let error = f
            .reader
            .input(&f.task, "dst_other", CancellationToken::new())
            .await
            .unwrap_err();
        assert!(!format!("{error:?}").contains("PRIVATE"));
        assert_eq!(f.state.lock().unwrap().seen.len(), 1);
    }
}
#[tokio::test]
async fn stale_scope_bad_claim_and_legacy_credentials_make_no_request() {
    let f = fixture().await;
    assert!(
        f.reader
            .input(&f.task, "operator-key", CancellationToken::new())
            .await
            .is_err()
    );
    let mut wrong = f.task.clone();
    wrong.delegation.as_mut().unwrap().scope.tenant_id =
        orch8_types::ids::TenantId::new("foreign").unwrap();
    assert!(
        f.reader
            .input(&wrong, "dst_alice", CancellationToken::new())
            .await
            .is_err()
    );
    let account = f
        .reader
        .storage
        .open_account_scope("alice", "issuer", "alice")
        .await
        .unwrap();
    f.reader
        .storage
        .retire_account_scope(&account)
        .await
        .unwrap();
    assert!(
        f.reader
            .input(&f.task, "dst_alice", CancellationToken::new())
            .await
            .is_err()
    );
    assert!(f.state.lock().unwrap().seen.is_empty());
}
async fn retire_during(stage: &'static str) {
    let f = fixture().await;
    f.state.lock().unwrap().hold = stage;
    let reader = f.reader.clone();
    let task = f.task.clone();
    let reading = tokio::spawn(async move {
        reader
            .input(&task, "dst_alice", CancellationToken::new())
            .await
    });
    tokio::time::timeout(Duration::from_secs(5), f.entered.notified())
        .await
        .unwrap();
    let owner = f
        .reader
        .storage
        .open_account_scope("alice", "issuer", "alice")
        .await
        .unwrap();
    f.reader.storage.retire_account_scope(&owner).await.unwrap();
    f.release.notify_one();
    assert!(reading.await.unwrap().is_err());
    let expected = if stage == "identity" { 1 } else { 2 };
    assert_eq!(f.state.lock().unwrap().seen.len(), expected);
}
#[tokio::test]
async fn retirement_during_identity_prevents_private_get() {
    retire_during("identity").await;
}
#[tokio::test]
async fn retirement_during_body_prevents_publication() {
    retire_during("payload").await;
}
#[tokio::test]
async fn cancelled_verification_keeps_physical_read_capacity_until_it_exits() {
    struct Release(Arc<(Mutex<bool>, Condvar)>);
    impl Drop for Release {
        fn drop(&mut self) {
            let (lock, cv) = &*self.0;
            *lock.lock().unwrap() = true;
            cv.notify_all();
        }
    }
    let mut f = fixture().await;
    let entered = Arc::new(Notify::new());
    let gate = Arc::new((Mutex::new(false), Condvar::new()));
    let _release_on_panic = Release(gate.clone());
    let entry = entered.clone();
    let held = gate.clone();
    Arc::get_mut(&mut f.reader).unwrap().verify_hook = Some(Arc::new(move || {
        entry.notify_one();
        let (lock, cv) = &*held;
        let mut open = lock.lock().unwrap();
        while !*open {
            open = cv.wait(open).unwrap();
        }
    }));
    let reader = f.reader.clone();
    let task = f.task.clone();
    let reading = tokio::spawn(async move {
        reader
            .input(&task, "dst_alice", CancellationToken::new())
            .await
    });
    tokio::time::timeout(Duration::from_secs(5), entered.notified())
        .await
        .unwrap();
    reading.abort();
    assert!(reading.await.unwrap_err().is_cancelled());
    assert!(matches!(
        f.reader
            .input(&f.task, "dst_alice", CancellationToken::new())
            .await,
        Err(MobileError::ResourceLimit { .. })
    ));
    {
        let (lock, cv) = &*gate;
        *lock.lock().unwrap() = true;
        cv.notify_all();
    }
    tokio::time::timeout(Duration::from_secs(5), async {
        while f.reader.budget.available_permits() != 1 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn bad_payloads_are_redacted_and_never_replace_verified_identity() {
    let f = fixture().await;
    f.state.lock().unwrap().bad = "input";
    let error = f
        .reader
        .input(&f.task, "dst_alice", CancellationToken::new())
        .await
        .unwrap_err();
    assert!(!format!("{error:?}").contains("PRIVATE_BAD_PAYLOAD"));
    assert_eq!(f.state.lock().unwrap().seen.len(), 2);
}

#[tokio::test]
async fn preparation_cold_and_cached_paths_keep_one_logical_run_and_fresh_identity() {
    let f = fixture().await;
    let first = f
        .reader
        .prepare(&f.task, "dst_alice", CancellationToken::new())
        .await
        .unwrap();
    assert_eq!(first.input.value()["wide"].as_u64(), Some(u64::MAX));
    assert_eq!(f.state.lock().unwrap().seen.len(), 5);
    wait_for_preparation_capacity(&f).await;
    let mut replacement = f.task.clone();
    replacement.claim_epoch += 1;
    replacement.delegation.as_mut().unwrap().scope.claim_epoch = replacement.claim_epoch;
    let second = f
        .reader
        .prepare(&replacement, "dst_refreshed", CancellationToken::new())
        .await
        .unwrap();
    assert_eq!(first.run.local_run_id, second.run.local_run_id);
    assert_eq!(first.run.definitions, second.run.definitions);
    assert_eq!(second.input.scope().claim_epoch, replacement.claim_epoch);
    let seen = f.state.lock().unwrap().seen.clone();
    assert_eq!(
        seen.len(),
        6,
        "cached graph/input still require exactly one fresh proof"
    );
    assert!(seen[5].0.ends_with("/workers/identity"));
    assert_eq!(seen[5].1, "dst_refreshed");
    let native: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM task_instances")
        .fetch_one(f.reader.storage.pool())
        .await
        .unwrap();
    assert_eq!(native, 0, "preparation is not native admission");
}

struct PreparationGate {
    state: Arc<(Mutex<bool>, Condvar)>,
    entered: Arc<Notify>,
}
impl PreparationGate {
    fn new() -> Self {
        Self {
            state: Arc::new((Mutex::new(false), Condvar::new())),
            entered: Arc::new(Notify::new()),
        }
    }
    fn hook(&self) -> Arc<dyn Fn() + Send + Sync> {
        let gate = self.state.clone();
        let entering = self.entered.clone();
        Arc::new(move || {
            entering.notify_one();
            let (lock, cv) = &*gate;
            let mut open = lock.lock().unwrap();
            while !*open {
                open = cv.wait(open).unwrap();
            }
        })
    }
    fn install(reader: &mut Arc<ScopedPayloadReader>) -> Self {
        let gate = Self::new();
        Arc::get_mut(reader).unwrap().prepare_hook = Some(gate.hook());
        gate
    }
    fn initialization(reader: &mut Arc<ScopedPayloadReader>) -> Self {
        let gate = Self::new();
        Arc::get_mut(reader).unwrap().initialize_hook = Some(gate.hook());
        gate
    }
    fn release(&self) {
        let (lock, cv) = &*self.state;
        *lock.lock().unwrap() = true;
        cv.notify_all();
    }
    async fn wait(&self) {
        tokio::time::timeout(Duration::from_secs(5), self.entered.notified())
            .await
            .unwrap();
    }
}
impl Drop for PreparationGate {
    fn drop(&mut self) {
        self.release();
    }
}

#[tokio::test]
async fn cancelled_cached_preparation_keeps_shared_capacity_and_does_not_block_executor() {
    let mut f = fixture().await;
    f.reader
        .prepare(&f.task, "dst_alice", CancellationToken::new())
        .await
        .unwrap();
    wait_for_preparation_capacity(&f).await;
    let gate = PreparationGate::install(&mut f.reader);
    let reader = f.reader.clone();
    let task = f.task.clone();
    let reading = tokio::spawn(async move {
        reader
            .prepare(&task, "dst_alice", CancellationToken::new())
            .await
    });
    gate.wait().await;
    // This test uses a current-thread executor: its timer cannot advance if the
    // cache preparation hook accidentally runs on that executor.
    tokio::time::sleep(Duration::from_millis(1)).await;
    reading.abort();
    assert!(reading.await.unwrap_err().is_cancelled());
    assert!(matches!(
        f.reader
            .prepare(&f.task, "dst_alice", CancellationToken::new())
            .await,
        Err(MobileError::ResourceLimit { .. })
    ));
    assert!(matches!(
        f.reader
            .input(&f.task, "dst_alice", CancellationToken::new())
            .await,
        Err(MobileError::ResourceLimit { .. })
    ));
    gate.release();
    tokio::time::timeout(Duration::from_secs(5), async {
        while f.reader.budget.available_permits() != 1 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    assert_eq!(f.state.lock().unwrap().seen.len(), 6);
}

#[tokio::test]
async fn account_retirement_during_cached_preparation_prevents_result_publication() {
    let mut f = fixture().await;
    f.reader
        .prepare(&f.task, "dst_alice", CancellationToken::new())
        .await
        .unwrap();
    wait_for_preparation_capacity(&f).await;
    let gate = PreparationGate::install(&mut f.reader);
    let reader = f.reader.clone();
    let task = f.task.clone();
    let reading = tokio::spawn(async move {
        reader
            .prepare(&task, "dst_alice", CancellationToken::new())
            .await
    });
    gate.wait().await;
    let account = f
        .reader
        .storage
        .open_account_scope("alice", "issuer", "alice")
        .await
        .unwrap();
    f.reader
        .storage
        .retire_account_scope(&account)
        .await
        .unwrap();
    gate.release();
    assert!(reading.await.unwrap().is_err());
    assert_eq!(f.state.lock().unwrap().seen.len(), 6);
    wait_for_preparation_capacity(&f).await;
}

#[tokio::test]
async fn failed_cache_preparation_cannot_leak_private_rows_or_capacity() {
    let f = fixture().await;
    f.reader
        .prepare(&f.task, "dst_alice", CancellationToken::new())
        .await
        .unwrap();
    wait_for_preparation_capacity(&f).await;
    sqlx::query("DROP TRIGGER mobile_remote_input_update")
        .execute(f.reader.storage.pool())
        .await
        .unwrap();
    sqlx::query("UPDATE mobile_remote_run_inputs SET reference_json='PRIVATE_CORRUPT_REFERENCE'")
        .execute(f.reader.storage.pool())
        .await
        .unwrap();
    let error = f
        .reader
        .prepare(&f.task, "dst_alice", CancellationToken::new())
        .await
        .unwrap_err();
    assert!(!format!("{error:?}").contains("PRIVATE_CORRUPT_REFERENCE"));
    wait_for_preparation_capacity(&f).await;
    assert_eq!(
        f.state.lock().unwrap().seen.len(),
        6,
        "corruption must not trigger a repair download"
    );
}

#[tokio::test]
async fn a_permit_from_an_unrelated_budget_cannot_bypass_reader_capacity() {
    let f = fixture().await;
    let other = Arc::new(Semaphore::new(1));
    let mut reader = (*f.reader).clone();
    reader.reserved = Some(Permit::acquire(&other).unwrap());
    assert!(
        Arc::new(reader)
            .input(&f.task, "dst_alice", CancellationToken::new())
            .await
            .is_err()
    );
    assert!(f.state.lock().unwrap().seen.is_empty());
}

#[test]
fn preparation_and_initialization_finish_with_only_one_tokio_blocking_worker() {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .max_blocking_threads(1)
        .build()
        .unwrap();
    let result = runtime.block_on(async {
        let f = fixture().await;
        tokio::time::timeout(
            Duration::from_secs(10),
            f.reader.initialize(
                &f.task,
                "dst_alice",
                CancellationToken::new(),
                initialization_limits(),
            ),
        )
        .await
    });
    runtime.shutdown_timeout(Duration::from_secs(1));
    assert!(result.unwrap().is_ok());
}

async fn wait_for_preparation_capacity(f: &Fixture) {
    tokio::time::timeout(Duration::from_secs(5), async {
        while f.reader.budget.available_permits() != 1 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
}
#[tokio::test]
async fn panicking_preparation_thread_releases_its_physical_reservation() {
    let mut f = fixture().await;
    Arc::get_mut(&mut f.reader).unwrap().prepare_hook =
        Some(Arc::new(|| panic!("preparation fault injection")));
    assert!(
        f.reader
            .prepare(&f.task, "dst_alice", CancellationToken::new())
            .await
            .is_err()
    );
    wait_for_preparation_capacity(&f).await;
    assert_eq!(f.state.lock().unwrap().seen.len(), 1);
}

fn initialization_limits() -> crate::storage::scopes::runs::materialize::InstanceLimits {
    crate::storage::scopes::runs::materialize::InstanceLimits {
        global: 16,
        account: 8,
        catalog: 4,
    }
}
async fn native_instances(f: &Fixture) -> i64 {
    sqlx::query_scalar("SELECT COUNT(*) FROM task_instances")
        .fetch_one(f.reader.storage.pool())
        .await
        .unwrap()
}
#[tokio::test]
async fn owned_initialization_is_paused_and_reuses_its_root_after_a_fresh_probe() {
    let f = fixture().await;
    let first = f
        .reader
        .initialize(
            &f.task,
            "dst_alice",
            CancellationToken::new(),
            initialization_limits(),
        )
        .await
        .unwrap();
    assert_eq!(
        first.instance.state,
        orch8_types::instance::InstanceState::Paused
    );
    assert_eq!(first.instance.id, first.run.local_run_id);
    assert_eq!(first.instance.context.data["wide"].as_u64(), Some(u64::MAX));
    wait_for_preparation_capacity(&f).await;
    let next = f
        .reader
        .initialize(
            &f.task,
            "dst_refreshed",
            CancellationToken::new(),
            initialization_limits(),
        )
        .await
        .unwrap();
    assert_eq!(first.instance.id, next.instance.id);
    assert_eq!(native_instances(&f).await, 1);
    assert_eq!(f.state.lock().unwrap().seen.len(), 6);
}

#[tokio::test]
async fn nonzero_mailbox_attempt_cannot_fetch_or_initialize_and_does_not_poison_retry() {
    let mut f = fixture().await;
    for attempt in [1, 2, u32::MAX] {
        f.task.attempt = attempt;
        assert!(
            f.reader
                .initialize(
                    &f.task,
                    "dst_alice",
                    CancellationToken::new(),
                    initialization_limits(),
                )
                .await
                .is_err(),
            "nonzero mailbox business attempt initialized a root"
        );
        assert!(f.state.lock().unwrap().seen.is_empty());
        assert_eq!(native_instances(&f).await, 0);
        assert_eq!(f.reader.budget.available_permits(), 1);
    }
    f.task.attempt = 0;
    assert!(
        f.reader
            .initialize(
                &f.task,
                "dst_alice",
                CancellationToken::new(),
                initialization_limits(),
            )
            .await
            .is_ok()
    );
    assert_eq!(native_instances(&f).await, 1);
}
#[tokio::test]
async fn cancelled_initialization_keeps_capacity_until_physical_preparation_returns() {
    let mut f = fixture().await;
    let gate = PreparationGate::initialization(&mut f.reader);
    let reader = f.reader.clone();
    let task = f.task.clone();
    let initializing = tokio::spawn(async move {
        reader
            .initialize(
                &task,
                "dst_alice",
                CancellationToken::new(),
                initialization_limits(),
            )
            .await
    });
    gate.wait().await;
    assert_eq!(native_instances(&f).await, 0);
    initializing.abort();
    assert!(initializing.await.unwrap_err().is_cancelled());
    assert!(matches!(
        f.reader
            .initialize(
                &f.task,
                "dst_alice",
                CancellationToken::new(),
                initialization_limits()
            )
            .await,
        Err(MobileError::ResourceLimit { .. })
    ));
    gate.release();
    wait_for_preparation_capacity(&f).await;
    assert_eq!(native_instances(&f).await, 0);
}
#[tokio::test]
async fn retiring_the_account_after_preparation_prevents_initialization() {
    let mut f = fixture().await;
    let gate = PreparationGate::initialization(&mut f.reader);
    let reader = f.reader.clone();
    let task = f.task.clone();
    let initializing = tokio::spawn(async move {
        reader
            .initialize(
                &task,
                "dst_alice",
                CancellationToken::new(),
                initialization_limits(),
            )
            .await
    });
    gate.wait().await;
    let account = f
        .reader
        .storage
        .open_account_scope("alice", "issuer", "alice")
        .await
        .unwrap();
    f.reader
        .storage
        .retire_account_scope(&account)
        .await
        .unwrap();
    gate.release();
    assert!(initializing.await.unwrap().is_err());
    assert_eq!(native_instances(&f).await, 0);
}
#[tokio::test]
async fn initialization_failure_is_redacted_and_leaves_no_partial_root() {
    let f = fixture().await;
    sqlx::query("CREATE TRIGGER fail_root BEFORE INSERT ON mobile_remote_run_materializations BEGIN SELECT RAISE(ABORT,'PRIVATE_NATIVE_FAILURE'); END").execute(f.reader.storage.pool()).await.unwrap();
    let error = f
        .reader
        .initialize(
            &f.task,
            "dst_alice",
            CancellationToken::new(),
            initialization_limits(),
        )
        .await
        .unwrap_err();
    assert!(!format!("{error:?}").contains("PRIVATE_NATIVE_FAILURE"));
    assert_eq!(native_instances(&f).await, 0);
    wait_for_preparation_capacity(&f).await;
    sqlx::query("DROP TRIGGER fail_root")
        .execute(f.reader.storage.pool())
        .await
        .unwrap();
    assert!(
        f.reader
            .initialize(
                &f.task,
                "dst_alice",
                CancellationToken::new(),
                initialization_limits()
            )
            .await
            .is_ok()
    );
}
