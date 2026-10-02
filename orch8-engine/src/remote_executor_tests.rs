use std::sync::Mutex;

use serde_json::json;

use super::*;

/// In-memory lease transport: hands out queued tasks once, records every
/// mutation.
#[derive(Default)]
struct FakeTransport {
    queue: Mutex<Vec<RemoteTask>>,
    calls: Mutex<Vec<(String, uuid::Uuid, Value)>>,
    advertisements: Mutex<Vec<RuntimeCapabilities>>,
}

impl FakeTransport {
    fn push(&self, task: RemoteTask) {
        self.queue.lock().unwrap().push(task);
    }

    fn calls(&self, verb: &str) -> Vec<(uuid::Uuid, Value)> {
        self.calls
            .lock()
            .unwrap()
            .iter()
            .filter(|(v, _, _)| v == verb)
            .map(|(_, id, body)| (*id, body.clone()))
            .collect()
    }

    fn record(&self, verb: &str, task: &ClaimedTask, body: Value) {
        self.calls
            .lock()
            .unwrap()
            .push((verb.to_string(), task.task.id, body));
    }
}

#[async_trait]
impl LeaseTransport for FakeTransport {
    fn name(&self) -> &'static str {
        "fake"
    }

    async fn advertise(&self, capabilities: &RuntimeCapabilities) -> Result<(), String> {
        self.advertisements
            .lock()
            .unwrap()
            .push(capabilities.clone());
        Ok(())
    }

    async fn claim(
        &self,
        handlers: &[String],
        capacity: u32,
        _capabilities: &RuntimeCapabilities,
    ) -> Result<Claimed, String> {
        let mut queue = self.queue.lock().unwrap();
        let mut tasks = Vec::new();
        while tasks.len() < capacity as usize {
            let Some(index) = queue
                .iter()
                .position(|task| handlers.contains(&task.handler_name))
            else {
                break;
            };
            tasks.push(ClaimedTask {
                task: queue.remove(index),
                session: 0,
            });
        }
        Ok(Claimed {
            tasks,
            poll_after: None,
            heartbeat: Some(Duration::from_millis(50)),
        })
    }

    async fn heartbeat(&self, task: &ClaimedTask) -> LeaseResponse {
        self.record("heartbeat", task, Value::Null);
        LeaseResponse::Accepted
    }

    async fn complete(&self, task: &ClaimedTask, output: &Value) -> LeaseResponse {
        self.record("complete", task, output.clone());
        LeaseResponse::Accepted
    }

    async fn fail(&self, task: &ClaimedTask, message: &str, retryable: bool) -> LeaseResponse {
        self.record(
            "fail",
            task,
            json!({"message": message, "retryable": retryable}),
        );
        LeaseResponse::Accepted
    }

    async fn release(&self, task: &ClaimedTask, started: bool) -> LeaseResponse {
        self.record("release", task, json!({"started": started}));
        LeaseResponse::Accepted
    }
}

/// Vault double: seals into a map keyed by `(instance, ref_key)`.
#[derive(Default)]
struct MemoryVault {
    objects: Mutex<HashMap<(InstanceId, String), Value>>,
}

#[async_trait]
impl ExternalPayloadVault for MemoryVault {
    async fn seal(
        &self,
        instance_id: InstanceId,
        ref_key: &str,
        value: &Value,
    ) -> Result<Value, orch8_types::error::StorageError> {
        self.objects
            .lock()
            .unwrap()
            .insert((instance_id, ref_key.to_string()), value.clone());
        Ok(
            json!({ VAULT_REF_KEY: { "v": 1, "object": format!("obj/{ref_key}"), "kid": "k", "dek": "d", "alg": "A256GCM" } }),
        )
    }

    async fn open(
        &self,
        instance_id: InstanceId,
        ref_key: &str,
        _reference: &Value,
    ) -> Result<Value, orch8_types::error::StorageError> {
        self.objects
            .lock()
            .unwrap()
            .get(&(instance_id, ref_key.to_string()))
            .cloned()
            .ok_or_else(|| orch8_types::error::StorageError::Backend("no such object".into()))
    }
}

#[allow(clippy::needless_pass_by_value)]
fn task(handler: &str, params: Value) -> RemoteTask {
    serde_json::from_value(json!({
        "id": uuid::Uuid::now_v7(), "instance_id": uuid::Uuid::now_v7(),
        "block_id": "b1", "handler_name": handler, "params": params,
        "context": {"data": {}}, "attempt": 1, "claim_epoch": 7,
    }))
    .unwrap()
}

async fn executor(
    transport: Arc<FakeTransport>,
    credentials: LocalCredentials,
    vault: Option<Arc<dyn ExternalPayloadVault>>,
    drain_timeout: Duration,
) -> Arc<RemoteExecutor> {
    let identity = ExecutorIdentity {
        runtime_id: replica_runtime_id(uuid::Uuid::now_v7(), "acme-dc1-host"),
        host: "acme-dc1-host".into(),
        tenant_id: "acme".into(),
        labels: BTreeMap::from([("residency".into(), "eu".into())]),
        regions: vec!["eu-west-1".into()],
        handlers: vec!["transform".into(), "sleep".into(), "fail".into()],
        credentials: credentials.names(),
    };
    let names = identity.handlers.clone();
    RemoteExecutor::new(RemoteExecutorParts {
        identity,
        settings: RemoteExecutorSettings {
            poll_interval: Duration::from_millis(20),
            drain_timeout,
            externalize_bytes: 16,
            ..RemoteExecutorSettings::default()
        },
        transport,
        registry: executor_registry(&names),
        credentials,
        vault,
        scratch: Arc::new(
            orch8_storage::sqlite::SqliteStorage::in_memory()
                .await
                .unwrap(),
        ),
    })
    .unwrap()
}

async fn wait_until(what: &str, mut probe: impl FnMut() -> bool) {
    for _ in 0..400 {
        if probe() {
            return;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    panic!("timed out waiting for {what}");
}

#[test]
fn replica_runtime_ids_are_stable_and_distinct_per_replica() {
    let token = uuid::Uuid::now_v7();
    assert_eq!(
        replica_runtime_id(token, "acme-a"),
        replica_runtime_id(token, "acme-a")
    );
    assert_ne!(
        replica_runtime_id(token, "acme-a"),
        replica_runtime_id(token, "acme-b")
    );
    assert_ne!(
        replica_runtime_id(token, "acme-a"),
        replica_runtime_id(uuid::Uuid::now_v7(), "acme-a")
    );
}

#[test]
fn registry_serves_only_remote_executable_builtins() {
    let all = executor_registry(&[]);
    assert!(all.contains("http_request"));
    assert!(all.contains("transform"));
    assert!(!all.contains("set_state"));
    assert!(!all.contains("send_signal"));
    let one = executor_registry(&["http_request".to_string()]);
    assert!(one.contains("http_request"));
    assert!(!one.contains("transform"));
}

#[test]
fn capabilities_carry_placement_facts_and_credential_names_only() {
    let identity = ExecutorIdentity {
        runtime_id: RuntimeId::new(),
        host: "acme-dc1-host".into(),
        tenant_id: "acme".into(),
        labels: BTreeMap::from([("residency".into(), "eu".into())]),
        regions: vec!["eu-west-1".into()],
        handlers: vec!["http_request".into()],
        credentials: vec!["stripe".into()],
    };
    let caps = identity.capabilities(true);
    assert!(caps.draining);
    assert_eq!(caps.kind, RuntimeKind::Server);
    assert_eq!(caps.labels.get("residency").map(String::as_str), Some("eu"));
    assert_eq!(caps.hardware, vec!["host:acme-dc1-host".to_string()]);
    assert_eq!(caps.credentials, vec!["stripe".to_string()]);
    assert!(caps.expires_at - caps.observed_at <= chrono::Duration::minutes(5));
    assert_eq!(identity.worker_id(), identity.runtime_id.to_string());
}

#[tokio::test]
async fn runs_tasks_with_local_credentials_and_reports_outcomes() {
    let transport = Arc::new(FakeTransport::default());
    let ok = task(
        "transform",
        json!({"token": "credentials://api/token", "n": 1}),
    );
    let missing = task("transform", json!({"token": "credentials://absent"}));
    let failing = task("fail", json!({"message": "boom"}));
    for t in [ok.clone(), missing.clone(), failing.clone()] {
        transport.push(t);
    }
    let credentials = LocalCredentials::from_vars(
        None,
        [(
            "ORCH8_CREDENTIAL_api".to_string(),
            r#"{"token":"tok-local"}"#.to_string(),
        )],
    );
    let exec = executor(
        Arc::clone(&transport),
        credentials,
        None,
        Duration::from_secs(5),
    )
    .await;
    let shutdown = CancellationToken::new();
    let handle = tokio::spawn(Arc::clone(&exec).run(shutdown.clone()));
    wait_until("three outcomes", || {
        transport.calls("complete").len() + transport.calls("fail").len() == 3
    })
    .await;
    shutdown.cancel();
    let stats = handle.await.unwrap();

    let completes = transport.calls("complete");
    assert_eq!(completes.len(), 1);
    assert_eq!(completes[0].0, ok.id);
    assert_eq!(
        completes[0].1["token"], "tok-local",
        "resolved on the executor"
    );
    let fails = transport.calls("fail");
    let missing_fail = fails.iter().find(|(id, _)| *id == missing.id).unwrap();
    assert_eq!(missing_fail.1["retryable"], false);
    assert!(
        missing_fail.1["message"]
            .as_str()
            .unwrap()
            .contains("no local credential 'absent'")
    );
    assert!(fails.iter().any(|(id, _)| *id == failing.id));
    assert_eq!(stats.claimed, 3);
    assert_eq!(stats.completed, 1);
    assert_eq!(stats.failed, 2);
    let ads = transport.advertisements.lock().unwrap();
    assert!(!ads.first().unwrap().draining);
    assert!(ads.last().unwrap().draining, "drain withdraws capability");
    assert_eq!(ads[0].credentials, vec!["api".to_string()]);
}

#[tokio::test]
async fn drain_releases_tasks_that_outlive_the_window() {
    let transport = Arc::new(FakeTransport::default());
    let slow = task("sleep", json!({"duration_ms": 60_000}));
    transport.push(slow.clone());
    let exec = executor(
        Arc::clone(&transport),
        LocalCredentials::default(),
        None,
        Duration::from_millis(200),
    )
    .await;
    let shutdown = CancellationToken::new();
    let handle = tokio::spawn(Arc::clone(&exec).run(shutdown.clone()));
    wait_until("the slow task to start", || exec.in_flight() == 1).await;
    wait_until("a heartbeat", || !transport.calls("heartbeat").is_empty()).await;
    shutdown.cancel();
    let stats = handle.await.unwrap();
    let releases = transport.calls("release");
    assert_eq!(releases.len(), 1);
    assert_eq!(releases[0].0, slow.id);
    assert_eq!(releases[0].1["started"], true);
    assert!(transport.calls("complete").is_empty());
    assert_eq!(stats.released, 1);
}

#[tokio::test]
async fn byok_seals_large_output_fields_and_opens_them_for_later_steps() {
    let vault = Arc::new(MemoryVault::default());
    let instance = InstanceId::new();
    let output = json!({"status": 200, "body": {"rows": ["a", "b", "c", "d", "e"]}});
    let sealed = seal_output(vault.as_ref(), instance, "fetch", output.clone(), 16)
        .await
        .unwrap();
    assert_eq!(sealed["status"], 200, "small fields stay readable");
    assert!(is_vault_reference(&sealed["body"]));
    assert_eq!(
        sealed["body"][VAULT_REF_KEY][VAULT_REF_KEY_FIELD],
        "remote-output/fetch/body"
    );
    assert!(!sealed.to_string().contains("rows"), "no plaintext");

    let mut next_params = json!({"input": sealed["body"].clone(), "x": 1});
    open_vault_references(vault.as_ref(), instance, &mut next_params)
        .await
        .unwrap();
    assert_eq!(next_params["input"], output["body"]);

    // A reference from another instance does not open.
    let mut foreign = json!({"input": sealed["body"].clone()});
    assert!(
        open_vault_references(vault.as_ref(), InstanceId::new(), &mut foreign)
            .await
            .is_err()
    );

    // Everything sealed with threshold 0; scalars sealed whole when large.
    let all = seal_output(vault.as_ref(), instance, "b", json!({"a": 1}), 0)
        .await
        .unwrap();
    assert!(is_vault_reference(&all["a"]));
    let scalar = seal_output(vault.as_ref(), instance, "c", json!("x".repeat(64)), 16)
        .await
        .unwrap();
    assert!(is_vault_reference(&scalar));
}

#[tokio::test]
async fn executor_with_vault_reports_references_not_plaintext() {
    let transport = Arc::new(FakeTransport::default());
    let t = task(
        "transform",
        json!({"secret_report": "confidential-payload-0123456789", "ok": true}),
    );
    transport.push(t.clone());
    let vault: Arc<dyn ExternalPayloadVault> = Arc::new(MemoryVault::default());
    let exec = executor(
        Arc::clone(&transport),
        LocalCredentials::default(),
        Some(vault),
        Duration::from_secs(5),
    )
    .await;
    let shutdown = CancellationToken::new();
    let handle = tokio::spawn(Arc::clone(&exec).run(shutdown.clone()));
    wait_until("completion", || !transport.calls("complete").is_empty()).await;
    shutdown.cancel();
    handle.await.unwrap();
    let (_, output) = &transport.calls("complete")[0];
    assert!(!output.to_string().contains("confidential-payload"));
    assert!(is_vault_reference(&output["secret_report"]));
    assert_eq!(output["ok"], true);
}
