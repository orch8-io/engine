//! Device-mesh delegation from **phone-local** parents (Feature 29).
//!
//! The parent workflow runs on the phone's own `MobileEngine` (its local
//! `SQLite` scheduler), not on the server. A step placed on a desktop with
//! `$runtime` is delegated through the server mailbox by the phone's
//! delegation pump — registration of the local parent's continuity
//! identity, a destination-bound grant, a delegation claim — while the local
//! instance stays parked; a desktop node claims the mailbox task, runs the
//! sub-sequence on its own embedded engine, and reports; the phone reads the
//! outcome and resumes the local step exactly once.
//!
//! Every scenario runs on `SQLite` and, with `DATABASE_URL`, on Postgres,
//! and injects disconnects on both links (a TCP proxy per device that
//! refuses and severs connections), duplicate deliveries on both sides, and
//! app kills while parked.
#![allow(clippy::too_many_lines)]

use std::sync::Arc;
use std::time::Duration;

use orch8_e2e::{
    Backend, Cloud, Delivery, Desktop, Link, Phone, PhoneAuth, PhoneDb, backends, wait_for,
};
use orch8_mobile::{DelegationStatus, InstanceStateKind};
use orch8_types::continuity::EffectState;
use serde_json::{Value, json};
use uuid::Uuid;

const LONG: Duration = Duration::from_secs(45);

/// Phone handler: produces the photo the delegation works on.
struct Capture;

impl orch8_mobile::StepHandler for Capture {
    fn execute(
        &self,
        _step_name: String,
        _input: String,
    ) -> Result<String, orch8_mobile::HandlerError> {
        Ok(json!({"photo": {"id": "photo-7", "bytes": 2048}}).to_string())
    }
}

/// Phone handler: records what the step after the delegation received.
struct Note(Arc<std::sync::Mutex<Vec<Value>>>);

impl orch8_mobile::StepHandler for Note {
    fn execute(
        &self,
        _step_name: String,
        input: String,
    ) -> Result<String, orch8_mobile::HandlerError> {
        let params: Value = serde_json::from_str(&input).unwrap_or(Value::Null);
        self.0.lock().unwrap().push(params.clone());
        Ok(json!({"noted": params["labels"]}).to_string())
    }
}

/// The phone side of a scenario: engine, its database, and its link.
struct PhoneNode {
    phone: Phone,
    notes: Arc<std::sync::Mutex<Vec<Value>>>,
    _dir: tempfile::TempDir,
}

fn open_phone(
    db_path: &str,
    device_id: &str,
    key: &PhoneAuth,
    base: &str,
    notes: &Arc<std::sync::Mutex<Vec<Value>>>,
) -> Phone {
    let phone = Phone::open(
        db_path,
        device_id,
        key,
        base,
        "phone_capture",
        Arc::new(Capture),
    );
    phone
        .engine
        .register_handler("phone_note".into(), Arc::new(Note(Arc::clone(notes))))
        .unwrap();
    phone
}

fn new_phone(cloud: &Cloud, base: &str, key: &PhoneAuth) -> PhoneNode {
    let dir = tempfile::tempdir().unwrap();
    let notes = Arc::default();
    let phone = open_phone(
        &dir.path().join("phone.db").to_string_lossy(),
        &format!("phone-{}", Uuid::now_v7().simple()),
        key,
        base,
        &notes,
    );
    phone.join(200);
    phone.start_delegation(&cloud.tenant);
    PhoneNode {
        phone,
        notes,
        _dir: dir,
    }
}

/// A desktop node behind its own link, serving `desktop_classify`.
struct DesktopNode {
    rt: tokio::runtime::Runtime,
    desktop: Desktop,
    link: Link,
    _dir: tempfile::TempDir,
}

const SERVES: [&str; 1] = ["desktop_classify"];

fn new_desktop(cloud: &Cloud) -> DesktopNode {
    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
        .unwrap();
    let link = Link::start(cloud);
    let key = cloud.mint_key(&["worker", "operator"]);
    let dir = tempfile::tempdir().unwrap();
    let desktop = rt.block_on(Desktop::start(
        &key,
        &dir.path().join("desktop.db").to_string_lossy(),
        |builder| {
            builder.handler("desktop_classify", |ctx: orch8::StepContext| async move {
                // An isolated delegated step sees its params; a delegated
                // sub-sequence sees its explicit input in the context.
                let photo = ctx.params["photo"]["id"]
                    .as_str()
                    .or_else(|| ctx.context.data["photo"]["id"].as_str())
                    .unwrap_or_default()
                    .to_owned();
                Ok(json!({"labels": ["receipt", "grocery"], "photo": photo}))
            })
        },
    ));
    let node = DesktopNode {
        rt,
        desktop,
        link,
        _dir: dir,
    };
    assert_eq!(node.register(cloud), Delivery::Accepted);
    node
}

impl DesktopNode {
    fn base(&self) -> String {
        self.link.base.clone()
    }

    fn register(&self, cloud: &Cloud) -> Delivery {
        self.rt
            .block_on(self.desktop.register(&self.base(), &cloud.tenant, &SERVES))
    }

    fn poll(&self) -> Result<Vec<Value>, Delivery> {
        self.rt.block_on(self.desktop.poll(&self.base(), &SERVES))
    }

    fn call(&self, task: &Value, verb: &str, extra: Value) -> Delivery {
        self.rt
            .block_on(self.desktop.lease_call(&self.base(), task, verb, extra))
    }

    /// Fetch the task's sub-sequence and run it on the desktop's engine.
    fn run(&self, task: &Value) -> Value {
        let sequence_id = task["params"]["sub_sequence_id"].as_str().unwrap();
        let (status, sequence) = self
            .rt
            .block_on(
                self.desktop
                    .get(&self.base(), &format!("/sequences/{sequence_id}")),
            )
            .unwrap();
        assert_eq!(status, 200, "{sequence}");
        self.rt
            .block_on(self.desktop.run_delegation(sequence, task))
    }

    /// Claim, run and report the next mailbox task (no fault injection).
    fn serve_one(&self) -> Value {
        let task = wait_for("a mailbox task for the desktop", LONG, || {
            self.poll().ok().and_then(|tasks| tasks.into_iter().next())
        });
        let output = self.run(&task);
        assert_eq!(
            self.call(&task, "complete", json!({"output": output})),
            Delivery::Accepted
        );
        task
    }

    fn runs(&self) -> usize {
        self.desktop.runs.lock().unwrap().len()
    }
}

/// The phone authenticates with device sessions from the app backend — no
/// stored key, no operator capability — granting its two local handlers.
fn phone_key(cloud: &Cloud) -> PhoneAuth {
    PhoneAuth::DeviceSession(cloud.device_sessions(&["phone_capture", "phone_note"], 3_600))
}

/// A server-side sub-sequence the desktop runs.
fn classify_sequence(cloud: &Cloud) -> Uuid {
    cloud.create_sequence(
        "classify",
        &json!([{"type": "step", "id": "classify", "handler": "desktop_classify",
                 "params": {}, "cancellable": true}]),
    )
}

/// The phone-local parent: capture → classify (delegated sub-sequence,
/// placed on `desktop`) → note (local, consumes the desktop's labels).
fn local_parent(phone: &Phone, name: &str, sub_sequence: Uuid, desktop: Uuid) {
    phone.load_local_sequence(
        name,
        &json!([
            {"type": "step", "id": "capture", "handler": "phone_capture", "params": {},
             "cancellable": true},
            {"type": "step", "id": "classify", "handler": "orch8.delegation", "cancellable": true,
             "retry": {"max_attempts": 3, "initial_backoff": 10, "max_backoff": 10},
             "params": {
                "sequence_id": sub_sequence,
                "input": {"photo": {"id": "{{outputs.capture.photo.id}}"}},
                "$runtime": {"runtime_id": desktop},
             }},
            {"type": "step", "id": "note", "handler": "phone_note", "cancellable": true,
             "params": {"labels": "{{outputs.classify.outputs.classify.labels}}"}},
        ]),
    );
}

/// Exactly-once on both sides for a completed phone-local delegation.
fn assert_delegated_once(
    cloud: &Cloud,
    node: &PhoneNode,
    db: &PhoneDb,
    local: &str,
    desktop: &DesktopNode,
    delegation_id: &str,
) {
    let backend = cloud.backend;
    node.phone
        .wait_local_state(local, InstanceStateKind::Completed, LONG);

    // Phone: the parked step resumed once, with the desktop's output, and
    // its local receipt committed once; the next step consumed it once.
    let outputs = db.block_outputs(local, "classify");
    assert_eq!(outputs.len(), 1, "{backend}: one local output: {outputs:?}");
    assert_eq!(
        outputs[0]["outputs"]["classify"]["labels"],
        json!(["receipt", "grocery"]),
        "{backend}"
    );
    assert_eq!(
        outputs[0]["outputs"]["classify"]["photo"], "photo-7",
        "{backend}: the desktop got the explicit input"
    );
    assert_eq!(
        db.receipt_states(local, "classify")
            .last()
            .map(String::as_str),
        Some("committed"),
        "{backend}"
    );
    assert_eq!(
        db.receipt_states(local, "classify")
            .iter()
            .filter(|state| *state == "committed")
            .count(),
        1,
        "{backend}"
    );
    let notes = node.notes.lock().unwrap().clone();
    assert_eq!(notes.len(), 1, "{backend}: {notes:?}");
    assert_eq!(
        notes[0]["labels"],
        json!(["receipt", "grocery"]),
        "{backend}"
    );
    let data: Value = serde_json::from_str(
        &node
            .phone
            .engine
            .get_instance(local.to_owned())
            .unwrap()
            .context,
    )
    .unwrap();
    assert_eq!(
        data["delegations"][delegation_id]["status"], "completed",
        "{backend}: {data}"
    );
    assert_eq!(
        data["delegations"][delegation_id]["runtime_id"],
        desktop.desktop.runtime_id.to_string(),
        "{backend}"
    );

    // Server: the proxy holds one integrated result and one committed
    // receipt; the desktop ran the sub-sequence once.
    let proxy: Uuid = delegation_id.parse().unwrap();
    let block = format!("delegation-{delegation_id}");
    assert_eq!(cloud.block_outputs(proxy, &block).len(), 1, "{backend}");
    assert_eq!(
        cloud
            .block_receipts(proxy, &block)
            .iter()
            .map(|receipt| receipt.state)
            .collect::<Vec<_>>(),
        [EffectState::Committed],
        "{backend}"
    );
    assert_eq!(desktop.runs(), 1, "{backend}: one desktop run");
    let (status, delegation) = cloud.root(
        "GET",
        &format!(
            "/continuity/delegations/{delegation_id}?tenant_id={}",
            cloud.tenant
        ),
        None,
    );
    assert_eq!(status, 200, "{backend}: {delegation}");
    assert_eq!(delegation["status"], "completed", "{backend}");
    assert_eq!(
        delegation["delegation"]["source_runtime_id"],
        node.phone.runtime_id(),
        "{backend}"
    );
    assert_eq!(
        delegation["parent_instance_id"], local,
        "{backend}: anchored on the phone-local parent"
    );
    let continuity = delegation["delegation"]["parent_continuity_id"]
        .as_str()
        .unwrap();
    let (_, provenance) = cloud.root(
        "GET",
        &format!(
            "/continuity/executions/{continuity}/provenance?tenant_id={}",
            cloud.tenant
        ),
        None,
    );
    let kinds: Vec<&str> = provenance
        .as_array()
        .or_else(|| provenance["items"].as_array())
        .map(|entries| {
            entries
                .iter()
                .filter_map(|entry| entry["kind"].as_str())
                .collect()
        })
        .unwrap_or_default();
    assert!(
        kinds.contains(&"execution_registered_by_runtime") && kinds.contains(&"device_delegation"),
        "{backend}: {provenance}"
    );
}

/// (h) A phone-local parent delegates a sub-sequence to a desktop. Both
/// sides lose the network repeatedly — the phone before the delegation is
/// placed and again while parked, the desktop before claiming, mid-run and
/// while reporting — and results are delivered twice on both sides. The
/// local step resumes exactly once.
#[test]
fn h_phone_local_parent_delegates_across_disconnects() {
    for backend in backends() {
        run_disconnects(&backend);
    }
}

fn run_disconnects(backend: &Backend) {
    let cloud = Cloud::start(backend, |_| {});
    let backend = cloud.backend;
    let phone_link = Link::start(&cloud);
    let key = phone_key(&cloud);
    let node = new_phone(&cloud, &phone_link.base, &key);
    let desktop = new_desktop(&cloud);
    let sub_sequence = classify_sequence(&cloud);
    local_parent(
        &node.phone,
        "photo-flow",
        sub_sequence,
        desktop.desktop.runtime_id,
    );
    let db = PhoneDb::open(&node.phone.db_path);

    // Phone disconnect 1: the step is parked locally, but cannot be placed.
    phone_link.set_online(false);
    desktop.link.set_online(false);
    let local = node.phone.start_local("photo-flow", &json!({}));
    node.phone
        .wait_local_state(&local, InstanceStateKind::Waiting, LONG);
    let parked = node.phone.wait_delegation("preparing", LONG);
    assert_eq!(parked.local_instance_id, local, "{backend}");
    assert_eq!(parked.block_id.as_deref(), Some("classify"), "{backend}");
    std::thread::sleep(Duration::from_millis(600));
    assert_eq!(
        node.phone.engine.list_delegations().unwrap()[0].state,
        "preparing",
        "{backend}: nothing is placed while the phone is offline"
    );

    // Reconnect: the delegation lands in the (offline) desktop's mailbox.
    phone_link.set_online(true);
    let delegated = node.phone.wait_delegation("delegated", LONG);
    let delegation_id = delegated.delegation_id.clone();
    let proxy: Uuid = delegation_id.parse().unwrap();
    let mailbox = wait_for("the mailbox task", LONG, || {
        cloud.tasks(proxy).into_iter().next()
    });
    assert_eq!(
        mailbox
            .requirements
            .runtime_id
            .map(orch8_types::continuity::RuntimeId::into_uuid),
        Some(desktop.desktop.runtime_id),
        "{backend}: targeted at the desktop"
    );
    assert_eq!(
        node.phone.local_state(&local),
        InstanceStateKind::Waiting,
        "{backend}"
    );

    // Phone disconnect 2: parked and offline while the desktop works.
    phone_link.set_online(false);

    // Desktop disconnect 1: offline before claiming.
    assert_eq!(desktop.poll().unwrap_err(), Delivery::Unreachable);
    desktop.link.set_online(true);
    assert_eq!(desktop.register(&cloud), Delivery::Accepted);
    let task = desktop.poll().unwrap().into_iter().next().expect("claim");
    assert_eq!(task["id"], mailbox.id.to_string(), "{backend}");

    // Desktop disconnect 2 (mid-run): heartbeats fail, the run goes on.
    desktop.link.set_online(false);
    assert_eq!(
        desktop.call(&task, "heartbeat", json!({})),
        Delivery::Unreachable
    );
    desktop.link.set_online(true);
    let output = {
        // The sub-sequence was fetched while online; run it offline.
        let sequence_id = task["params"]["sub_sequence_id"].as_str().unwrap();
        let (status, sequence) = desktop
            .rt
            .block_on(
                desktop
                    .desktop
                    .get(&desktop.base(), &format!("/sequences/{sequence_id}")),
            )
            .unwrap();
        assert_eq!(status, 200);
        desktop.link.set_online(false);
        desktop
            .rt
            .block_on(desktop.desktop.run_delegation(sequence, &task))
    };
    desktop.link.set_online(true);
    assert_eq!(
        desktop.call(&task, "heartbeat", json!({})),
        Delivery::Accepted
    );

    // Desktop disconnect 3 (reporting), then a duplicate report.
    desktop.link.set_online(false);
    let complete = json!({"output": output});
    assert_eq!(
        desktop.call(&task, "complete", complete.clone()),
        Delivery::Unreachable
    );
    desktop.link.set_online(true);
    assert_eq!(
        desktop.call(&task, "complete", complete.clone()),
        Delivery::Accepted
    );
    assert_eq!(
        desktop.call(&task, "complete", complete),
        Delivery::Accepted,
        "{backend}: the duplicate report is idempotent"
    );

    // The server has the outcome; the offline phone has not resumed.
    let (_, read) = cloud.root(
        "GET",
        &format!(
            "/continuity/delegations/{delegation_id}?tenant_id={}",
            cloud.tenant
        ),
        None,
    );
    assert_eq!(read["status"], "completed", "{backend}: {read}");
    std::thread::sleep(Duration::from_millis(600));
    assert_eq!(
        node.phone.local_state(&local),
        InstanceStateKind::Waiting,
        "{backend}: an offline phone keeps the step parked"
    );

    // Reconnect: the phone reads the outcome and resumes.
    phone_link.set_online(true);
    assert_delegated_once(&cloud, &node, &db, &local, &desktop, &delegation_id);

    // Duplicate delivery on the phone: the journal forgets it applied the
    // result; the next pass reads it again but the resume is fenced.
    db.replay_delegation(&delegation_id);
    node.phone.engine.on_push_received();
    node.phone.wait_delegation("completed", LONG);
    std::thread::sleep(Duration::from_millis(500));
    assert_delegated_once(&cloud, &node, &db, &local, &desktop, &delegation_id);
    let stats = node.phone.engine.delegation_stats();
    assert_eq!(
        (stats.delegated, stats.resumed),
        (1, 1),
        "{backend}: resumed exactly once: {stats:?}"
    );

    node.phone.engine.shutdown();
    desktop.rt.block_on(desktop.desktop.engine.shutdown());
}

/// (i) The app is killed twice: once before the delegation could be placed
/// (offline), once while parked on a placed delegation. Each relaunch picks
/// the journal up; the desktop reports while the app is dead, and the
/// relaunched phone resumes the step exactly once.
#[test]
fn i_phone_killed_while_parked_resumes_once() {
    for backend in backends() {
        run_kills(&backend);
    }
}

fn run_kills(backend: &Backend) {
    let cloud = Cloud::start(backend, |_| {});
    let backend = cloud.backend;
    let phone_link = Link::start(&cloud);
    let key = phone_key(&cloud);
    let desktop = new_desktop(&cloud);
    let sub_sequence = classify_sequence(&cloud);
    let dir = tempfile::tempdir().unwrap();
    let db_path = dir.path().join("phone.db").to_string_lossy().into_owned();
    let device = format!("phone-{}", Uuid::now_v7().simple());
    let notes: Arc<std::sync::Mutex<Vec<Value>>> = Arc::default();
    let launch = || {
        let phone = open_phone(&db_path, &device, &key, &phone_link.base, &notes);
        phone.join(200);
        phone.start_delegation(&cloud.tenant);
        phone.engine.resume();
        phone
    };

    // Launch 1: offline, the step parks but cannot be placed. Kill.
    let phone = launch();
    local_parent(
        &phone,
        "photo-flow",
        sub_sequence,
        desktop.desktop.runtime_id,
    );
    phone_link.set_online(false);
    let local = phone.start_local("photo-flow", &json!({}));
    phone.wait_local_state(&local, InstanceStateKind::Waiting, LONG);
    phone.wait_delegation("preparing", LONG);
    drop(phone);

    // Launch 2: online, the journal is picked up and placed. Kill while
    // parked.
    phone_link.set_online(true);
    let phone = launch();
    let delegated = phone.wait_delegation("delegated", LONG);
    assert_eq!(
        phone.local_state(&local),
        InstanceStateKind::Waiting,
        "{backend}"
    );
    let delegation_id = delegated.delegation_id;
    drop(phone);

    // The desktop claims, runs and reports while the app is dead.
    desktop.serve_one();
    let (_, read) = cloud.root(
        "GET",
        &format!(
            "/continuity/delegations/{delegation_id}?tenant_id={}",
            cloud.tenant
        ),
        None,
    );
    assert_eq!(read["status"], "completed", "{backend}: {read}");

    // Launch 3: the parked step resumes from the journal, exactly once.
    let phone = launch();
    let node = PhoneNode {
        phone,
        notes: Arc::clone(&notes),
        _dir: tempfile::tempdir().unwrap(),
    };
    let db = PhoneDb::open(&db_path);
    assert_delegated_once(&cloud, &node, &db, &local, &desktop, &delegation_id);
    assert_eq!(
        node.phone.engine.list_delegations().unwrap().len(),
        1,
        "{backend}: one delegation across three launches"
    );

    node.phone.engine.shutdown();
    desktop.rt.block_on(desktop.desktop.engine.shutdown());
    drop(dir);
}

/// (j) An isolated step placed by kind (`runtime_kinds: ["desktop"]`) is
/// delegated as a one-step sub-sequence to a live desktop chosen by the
/// phone, and resumes with that step's own output; a failed delegation
/// follows the local retry policy with a fresh delegation; the explicit
/// `delegate` API reports its outcome through `delegation_status`.
#[test]
fn j_isolated_step_by_kind_retry_and_explicit_delegate() {
    for backend in backends() {
        run_isolated_step(&backend);
    }
}

fn run_isolated_step(backend: &Backend) {
    let cloud = Cloud::start(backend, |_| {});
    let backend = cloud.backend;
    let key = phone_key(&cloud);
    let node = new_phone(&cloud, &cloud.v1(), &key);
    let desktop = new_desktop(&cloud);
    let db = PhoneDb::open(&node.phone.db_path);
    node.phone.load_local_sequence(
        "scan-flow",
        &json!([
            {"type": "step", "id": "capture", "handler": "phone_capture", "params": {},
             "cancellable": true},
            {"type": "step", "id": "ocr", "handler": "desktop_classify", "cancellable": true,
             "retry": {"max_attempts": 2, "initial_backoff": 10, "max_backoff": 10},
             "params": {"photo": {"id": "{{outputs.capture.photo.id}}"},
                        "$runtime": {"runtime_kinds": ["desktop"]}}},
            {"type": "step", "id": "note", "handler": "phone_note", "cancellable": true,
             "params": {"labels": "{{outputs.ocr.labels}}"}},
        ]),
    );
    let local = node.phone.start_local("scan-flow", &json!({}));

    // Attempt 1: the desktop fails it; the delegation integrates a failure
    // and the local step retries under a fresh delegation.
    let failed = wait_for("the first mailbox task", LONG, || {
        desktop
            .poll()
            .ok()
            .and_then(|tasks| tasks.into_iter().next())
    });
    assert_eq!(
        desktop.call(
            &failed,
            "fail",
            json!({"message": "GPU busy", "retryable": true})
        ),
        Delivery::Accepted
    );
    let task = desktop.serve_one();
    assert_ne!(task["id"], failed["id"], "{backend}: a fresh delegation");
    node.phone
        .wait_local_state(&local, InstanceStateKind::Completed, LONG);

    // The pump journals the outcome *after* it resumes the local parent (so
    // a crash in between replays the resume rather than losing it): the
    // instance can complete a beat before its delegation row reads
    // `completed`. Wait for the journal instead of racing it.
    let journal_states = |journal: &[DelegationStatus]| {
        journal
            .iter()
            .map(|delegation| delegation.state.clone())
            .collect::<Vec<_>>()
    };
    let journal = wait_for("the delegation journal to settle", LONG, || {
        let journal = node.phone.engine.list_delegations().unwrap();
        (journal_states(&journal) == ["failed", "completed"]).then_some(journal)
    });
    assert!(
        journal[0]
            .error
            .as_deref()
            .is_some_and(|error| error.contains("GPU busy")),
        "{backend}: {journal:?}"
    );
    let destination = desktop.desktop.runtime_id.to_string();
    assert!(
        journal
            .iter()
            .all(|delegation| delegation.destination_runtime_id.as_deref()
                == Some(destination.as_str())),
        "{backend}: the desktop was chosen by kind"
    );
    let outputs = db.block_outputs(&local, "ocr");
    assert_eq!(outputs.len(), 1, "{backend}: {outputs:?}");
    assert_eq!(
        outputs[0],
        json!({"labels": ["receipt", "grocery"], "photo": "photo-7"}),
        "{backend}: the step resumed with its own output"
    );
    assert_eq!(
        db.receipt_states(&local, "ocr"),
        ["unknown", "committed"],
        "{backend}: the failed attempt's effect is unknown, the retry committed"
    );
    assert_eq!(
        node.notes.lock().unwrap()[0]["labels"],
        json!(["receipt", "grocery"]),
        "{backend}"
    );
    assert_eq!(desktop.runs(), 1, "{backend}");

    // Explicit API: delegate a sub-sequence on behalf of the local instance.
    let sub_sequence = classify_sequence(&cloud);
    let id = node
        .phone
        .engine
        .delegate(orch8_mobile::DelegateRequest {
            instance_id: local.clone(),
            destination_runtime_id: destination.clone(),
            sub_sequence_id: sub_sequence.to_string(),
            input_json: json!({"photo": {"id": "photo-9"}}).to_string(),
        })
        .unwrap();
    wait_for("the explicit delegation to be placed", LONG, || {
        let status = node.phone.engine.delegation_status(id.clone()).unwrap();
        (status.state == "delegated").then_some(())
    });
    desktop.serve_one();
    let status = wait_for("the explicit outcome", LONG, || {
        let status = node.phone.engine.delegation_status(id.clone()).unwrap();
        (status.state == "completed").then_some(status)
    });
    let output: Value = serde_json::from_str(status.output_json.as_deref().unwrap()).unwrap();
    assert_eq!(
        output["outputs"]["classify"]["photo"], "photo-9",
        "{backend}: {output}"
    );
    assert_eq!(
        status.destination_runtime_id.as_deref(),
        Some(destination.as_str())
    );
    assert!(
        node.phone
            .engine
            .delegation_status(Uuid::now_v7().to_string())
            .is_err(),
        "{backend}: unknown delegations are NotFound"
    );

    // Tenant isolation: another tenant's credential cannot read the
    // delegation, even naming this tenant.
    let other = format!("e2e-other-{}", Uuid::now_v7().simple());
    let (status, minted) = cloud.root(
        "POST",
        "/api-keys",
        Some(&json!({"tenant_id": other, "name": "other", "capabilities": ["operator"]})),
    );
    assert_eq!(status, 201, "{backend}: {minted}");
    let foreign = minted["secret"].as_str().unwrap();
    for tenant in [other.as_str(), cloud.tenant.as_str()] {
        let (status, body) = cloud.call(
            foreign,
            "GET",
            &format!("/continuity/delegations/{id}?tenant_id={tenant}"),
            None,
        );
        assert!(
            status == 403 || status == 404,
            "{backend}: foreign read of a delegation: {status} {body}"
        );
    }

    // A runtime-hosted execution cannot claim a server-hosted instance, and
    // needs a live registration of its hosting runtime.
    let server_instance = cloud.create_instance(classify_sequence(&cloud), &json!({}));
    for (instance, runtime) in [
        (server_instance, node.phone.runtime_id()),
        (Uuid::now_v7(), Uuid::now_v7().to_string()),
    ] {
        let (status, body) = cloud.root(
            "POST",
            "/continuity/executions",
            Some(&json!({"tenant_id": cloud.tenant, "instance_id": instance,
                         "runtime_id": runtime, "hosted_by_runtime": true})),
        );
        assert_eq!(status, 409, "{backend}: {body}");
    }

    node.phone.engine.shutdown();
    desktop.rt.block_on(desktop.desktop.engine.shutdown());
}
