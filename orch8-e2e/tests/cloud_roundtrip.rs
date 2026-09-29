//! Cloud → phone → cloud, end to end, with no simulated device.
//!
//! Each scenario runs the real control plane (API router with API-key auth,
//! scheduler, lease reaper) on a loopback port — over `SQLite`, and over
//! Postgres when `DATABASE_URL` is set — and a real
//! `orch8_mobile::MobileEngine` with its own `SQLite` file that joins the
//! mesh with `register_node` + `start_worker` using a tenant API key.
//!
//! The sequence is `prepare` (server) → `sign` (placed on the phone with
//! `$runtime.runtime_id`, run by a native `StepHandler`) → `finish`
//! (server). Every scenario asserts exactly-once effect semantics from the
//! effect receipts and the block outputs: one committed receipt per step,
//! whose id is the `effect_id` the handler that produced the output saw.
#![allow(clippy::too_many_lines)]

use std::sync::{Arc, Mutex};
use std::time::Duration;

use orch8_e2e::{
    Backend, Cloud, Delivery, Desktop, Gate, Ledger, Link, Phone, PhoneAuth, SignHandler, backends,
    ledger_effects, ledger_len, wait_for,
};
use orch8_types::continuity::EffectState;
use orch8_types::ids::{InstanceId, TenantId};
use orch8_types::worker::{WorkerAttemptEventKind, WorkerTaskState};
use serde_json::{Value, json};
use uuid::Uuid;

const LONG: Duration = Duration::from_secs(45);

type Calls = Arc<Mutex<Vec<(String, Value)>>>;

/// A control plane serving the two server-side steps.
fn cloud(backend: &Backend) -> (Cloud, Calls) {
    let calls: Calls = Arc::default();
    let (prepare_calls, finish_calls) = (Arc::clone(&calls), Arc::clone(&calls));
    let cloud = Cloud::start(backend, move |registry| {
        registry.register("cloud_prepare", move |ctx| {
            let calls = Arc::clone(&prepare_calls);
            async move {
                calls
                    .lock()
                    .unwrap()
                    .push(("prepare".into(), ctx.params.clone()));
                Ok(json!({"prepared": {"order": format!("o-{}", ctx.instance_id)}}))
            }
        });
        registry.register("cloud_finish", move |ctx| {
            let calls = Arc::clone(&finish_calls);
            async move {
                calls
                    .lock()
                    .unwrap()
                    .push(("finish".into(), ctx.params.clone()));
                Ok(json!({"finished": {"signature": ctx.params["signature"]}}))
            }
        });
    });
    (cloud, calls)
}

fn calls_of(calls: &Calls, name: &str) -> usize {
    calls
        .lock()
        .unwrap()
        .iter()
        .filter(|(call, _)| call == name)
        .count()
}

/// `prepare` → `sign` (on the phone) → `finish`.
fn roundtrip_blocks(phone_runtime: &str, sign_attempts: Option<u32>) -> Value {
    let mut sign = json!({
        "type": "step", "id": "sign", "handler": "phone_sign", "cancellable": true,
        "params": {
            "doc": "{{outputs.prepare.prepared.order}}",
            "$runtime": {"runtime_id": phone_runtime},
        },
    });
    if let Some(max_attempts) = sign_attempts {
        sign["retry"] = json!({
            "max_attempts": max_attempts, "initial_backoff": 10, "max_backoff": 10,
        });
    }
    json!([
        {"type": "step", "id": "prepare", "handler": "cloud_prepare", "params": {}, "cancellable": true},
        sign,
        {"type": "step", "id": "finish", "handler": "cloud_finish", "cancellable": true,
         "params": {"signature": "{{outputs.sign.signature}}"}},
    ])
}

struct Device {
    phone: Phone,
    ledger: Ledger,
    gate: Arc<Gate>,
    dir: tempfile::TempDir,
}

/// A fresh phone authenticating with device sessions from the app backend
/// (granting `phone_sign`), reaching the cloud at `api_base`.
fn phone(cloud: &Cloud, api_base: &str, gate: Arc<Gate>) -> Device {
    let auth = PhoneAuth::DeviceSession(cloud.device_sessions(&["phone_sign"], 3_600));
    phone_with(api_base, gate, &auth)
}

fn phone_with(api_base: &str, gate: Arc<Gate>, key: &PhoneAuth) -> Device {
    let dir = tempfile::tempdir().unwrap();
    let ledger: Ledger = Arc::default();
    let phone = Phone::open(
        &dir.path().join("phone.db").to_string_lossy(),
        &format!("phone-{}", Uuid::now_v7().simple()),
        key,
        api_base,
        "phone_sign",
        Arc::new(SignHandler {
            ledger: Arc::clone(&ledger),
            gate: Arc::clone(&gate),
        }),
    );
    Device {
        phone,
        ledger,
        gate,
        dir,
    }
}

/// Exactly-once for the whole round trip:
/// * the instance completed, and its steps ran in order;
/// * each step has exactly one output and exactly one committed receipt;
/// * `sign`'s receipts follow `sign_receipts` (attempt order) and the
///   committed one is the effect id the handler that produced the output
///   saw — and saw exactly once;
/// * `finish` consumed that very signature.
fn assert_exactly_once(
    cloud: &Cloud,
    instance: Uuid,
    ledger: &Ledger,
    sign_receipts: &[EffectState],
) {
    let backend = cloud.backend;
    assert_eq!(cloud.instance(instance)["state"], "completed", "{backend}");

    let outputs = cloud.outputs(instance);
    let first = |block: &str| {
        outputs
            .iter()
            .position(|output| output["block_id"] == block && output["output_ref"] != "__retry__")
            .unwrap_or_else(|| panic!("{backend}: no output for {block}: {outputs:?}"))
    };
    assert!(
        first("prepare") < first("sign") && first("sign") < first("finish"),
        "{backend}: steps ran in order: {outputs:?}"
    );
    for block in ["prepare", "sign", "finish"] {
        assert_eq!(
            cloud.block_outputs(instance, block).len(),
            1,
            "{backend}: exactly one {block} output"
        );
        let committed = cloud
            .block_receipts(instance, block)
            .into_iter()
            .filter(|receipt| receipt.state == EffectState::Committed)
            .count();
        assert_eq!(
            committed, 1,
            "{backend}: exactly one committed {block} receipt"
        );
    }

    let receipts = cloud.block_receipts(instance, "sign");
    assert_eq!(
        receipts
            .iter()
            .map(|receipt| receipt.state)
            .collect::<Vec<_>>(),
        sign_receipts,
        "{backend}: sign receipts by attempt"
    );
    let committed = receipts
        .iter()
        .find(|receipt| receipt.state == EffectState::Committed)
        .unwrap();
    let sign = &cloud.block_outputs(instance, "sign")[0]["output"];
    assert_eq!(
        sign["signed_effect_id"],
        committed.id.to_string(),
        "{backend}: the committed receipt is the effect the output came from"
    );
    assert_eq!(
        ledger_effects(ledger)
            .iter()
            .filter(|effect| **effect == committed.id.to_string())
            .count(),
        1,
        "{backend}: the committed effect ran exactly once on the phone"
    );
    assert_eq!(
        sign["doc"],
        format!("o-{instance}"),
        "{backend}: sign saw prepare's output"
    );
    let finish = &cloud.block_outputs(instance, "finish")[0]["output"];
    assert_eq!(
        finish["finished"]["signature"], sign["signature"],
        "{backend}: finish consumed the phone's signature"
    );
}

fn sign_task(cloud: &Cloud, instance: Uuid) -> Option<orch8_types::worker::WorkerTask> {
    cloud
        .tasks(instance)
        .into_iter()
        .filter(|task| task.block_id.as_str() == "sign")
        .max_by_key(|task| task.attempt)
}

/// How the phone of a happy-path run authenticates.
#[derive(Clone, Copy)]
enum Auth {
    /// Device sessions valid for this many seconds.
    Session(u32),
    /// A stored `worker`+`device` API key (legacy).
    LegacyKey,
}

/// (a) Happy path, with a device session.
#[test]
fn a_happy_path_runs_cloud_phone_cloud_exactly_once() {
    for backend in backends() {
        run_happy_path(&backend, Auth::Session(3_600));
    }
}

/// (a, legacy) The same round trip with a stored `worker`+`device` API key
/// in `sync_api_key`: configs that predate device sessions keep working.
#[test]
fn a_legacy_api_key_phone_still_runs_the_round_trip() {
    for backend in backends() {
        run_happy_path(&backend, Auth::LegacyKey);
    }
}

/// (a, refresh) Device sessions that expire every two seconds: the SDK
/// answers each `401` by asking the host's token provider for a fresh one
/// and retrying, so the round trip completes across several expiries.
#[test]
fn a_expired_device_sessions_are_refreshed_through_the_token_provider() {
    for backend in backends() {
        run_happy_path(&backend, Auth::Session(2));
    }
}

fn run_happy_path(backend: &Backend, auth: Auth) {
    let (cloud, calls) = cloud(backend);
    let device = match auth {
        Auth::LegacyKey => {
            let key = PhoneAuth::ApiKey(cloud.mint_key(&["worker", "device"]));
            phone_with(&cloud.v1(), Gate::open(), &key)
        }
        Auth::Session(ttl) => {
            let key = PhoneAuth::DeviceSession(cloud.device_sessions(&["phone_sign"], ttl));
            phone_with(&cloud.v1(), Gate::open(), &key)
        }
    };
    device.phone.join(200);
    if let Auth::Session(ttl) = auth
        && ttl < 10
    {
        // Let the first session lapse while the worker idles.
        std::thread::sleep(Duration::from_secs(u64::from(ttl) + 1));
    }
    let runtime = device.phone.runtime_id();

    let sequence = cloud.create_sequence("roundtrip", &roundtrip_blocks(&runtime, None));
    let instance = cloud.create_instance(sequence, &json!({}));
    cloud.wait_state(instance, "completed", LONG);

    assert_exactly_once(&cloud, instance, &device.ledger, &[EffectState::Committed]);
    let backend = cloud.backend;
    let seen = device.ledger.lock().unwrap()[0].clone();
    let task = sign_task(&cloud, instance).unwrap();
    assert_eq!(task.state, WorkerTaskState::Completed, "{backend}");
    assert_eq!(
        seen["__orch8"]["effect_id"],
        task.effect_id.unwrap().to_string(),
        "{backend}: the handler saw the effect id stored at dispatch"
    );
    assert_eq!(seen["__orch8"]["runtime_id"], runtime, "{backend}");
    assert_eq!(seen["__orch8"]["task_id"], task.id.to_string(), "{backend}");
    assert_eq!(
        task.worker_id.as_deref(),
        Some(runtime.as_str()),
        "{backend}"
    );
    assert_eq!(
        task.claimed_runtime_kind,
        Some(orch8_types::continuity::RuntimeKind::Mobile),
        "{backend}"
    );
    assert_eq!(calls_of(&calls, "prepare"), 1, "{backend}");
    assert_eq!(calls_of(&calls, "finish"), 1, "{backend}");
    let stats = device.phone.engine.worker_stats();
    assert_eq!(
        (stats.claimed, stats.completed, stats.lost),
        (1, 1, 0),
        "{backend}"
    );
    if let (Auth::Session(ttl), PhoneAuth::DeviceSession(sessions)) = (auth, &device.phone.auth)
        && ttl < 10
    {
        let minted = sessions.minted.load(std::sync::atomic::Ordering::SeqCst);
        assert!(
            minted >= 2,
            "{backend}: expired sessions were refreshed ({minted} minted)"
        );
    }
    device.phone.engine.shutdown();
}

/// (b) Phone offline when `sign` is dispatched: the task waits in the
/// phone's mailbox; a push wake while offline cannot claim it; once the
/// network is back a push wake (not the 60 s idle poll) delivers it.
#[test]
fn b_offline_phone_is_woken_by_push_and_completes() {
    for backend in backends() {
        let (cloud, _) = cloud(&backend);
        let link = Link::start(&cloud);
        let device = phone(&cloud, &link.base, Gate::open());
        // Idle poll far beyond the test: only wakes make it poll again.
        device.phone.join(60_000);
        let runtime = device.phone.runtime_id();
        std::thread::sleep(Duration::from_millis(300)); // initial (empty) poll

        link.set_online(false);
        let sequence = cloud.create_sequence("offline", &roundtrip_blocks(&runtime, None));
        let instance = cloud.create_instance(sequence, &json!({}));
        let task = wait_for("sign in the mailbox", LONG, || {
            sign_task(&cloud, instance).filter(|task| task.state == WorkerTaskState::Pending)
        });
        cloud.wait_state(instance, "waiting", LONG);
        let backend = cloud.backend;
        assert_eq!(
            task.requirements.runtime_id.map(|id| id.to_string()),
            Some(runtime.clone()),
            "{backend}: targeted at this phone"
        );

        let wake = json!({"task_id": task.id, "runtime_id": runtime, "reason": "task_available"})
            .to_string();
        // Offline: the wake makes the worker try, and fail, to poll.
        assert!(device.phone.engine.on_push_wake(wake.clone()), "{backend}");
        std::thread::sleep(Duration::from_millis(800));
        link.set_online(true);
        std::thread::sleep(Duration::from_millis(800));
        assert_eq!(
            cloud.task(task.id).unwrap().state,
            WorkerTaskState::Pending,
            "{backend}: nothing claims it until the phone is told to poll"
        );
        assert_eq!(ledger_len(&device.ledger), 0, "{backend}");

        // A wake for another runtime is ignored; this phone's wake delivers.
        let foreign = json!({"runtime_id": Uuid::now_v7(), "reason": "task_available"}).to_string();
        assert!(!device.phone.engine.on_push_wake(foreign), "{backend}");
        assert!(device.phone.engine.on_push_wake(wake), "{backend}");
        cloud.wait_state(instance, "completed", Duration::from_secs(20));
        assert_exactly_once(&cloud, instance, &device.ledger, &[EffectState::Committed]);
        device.phone.engine.shutdown();
    }
}

/// (c) The app is killed while `sign`'s handler runs. The reopened engine's
/// orphan drain releases the claim as started, so the server marks the
/// receipt unknown and follows the retry policy: a new attempt (new effect
/// id) runs once and commits. The killed attempt's effect never commits.
#[test]
fn c_kill_mid_step_drains_the_orphan_and_retries_once() {
    for backend in backends() {
        let (cloud, _) = cloud(&backend);
        let device = phone(&cloud, &cloud.v1(), Gate::closed());
        device.phone.join(200);
        let runtime = device.phone.runtime_id();
        let sequence = cloud.create_sequence("kill", &roundtrip_blocks(&runtime, Some(2)));
        let instance = cloud.create_instance(sequence, &json!({}));
        device.gate.wait_arrivals(1, LONG);
        let first = sign_task(&cloud, instance).unwrap();
        let backend = cloud.backend;
        assert_eq!(first.state, WorkerTaskState::Claimed, "{backend}");

        // Kill: drop the engine while the native handler is blocked. The
        // gate opens only after the runtime began shutting down, so the
        // handler's result has nowhere to go (as after an OS kill).
        let gate = Arc::clone(&device.gate);
        let opener = std::thread::spawn(move || {
            std::thread::sleep(Duration::from_millis(400));
            gate.release();
        });
        let Device {
            phone,
            ledger,
            gate,
            dir,
        } = device;
        let (db_path, device_id, key) = (
            phone.db_path.clone(),
            phone.device_id.clone(),
            phone.auth.clone(),
        );
        drop(phone);
        opener.join().unwrap();

        std::thread::sleep(Duration::from_secs(1));
        let stranded = cloud.task(first.id).unwrap();
        assert_eq!(
            stranded.state,
            WorkerTaskState::Claimed,
            "{backend}: a killed app reports nothing"
        );
        assert_eq!(
            cloud.block_receipts(instance, "sign")[0].state,
            EffectState::Dispatched,
            "{backend}"
        );
        assert_eq!(ledger_len(&ledger), 1, "{backend}");

        // Relaunch on the same database: join → orphan drain.
        let device = Device {
            phone: Phone::open(
                &db_path,
                &device_id,
                &key,
                &cloud.v1(),
                "phone_sign",
                Arc::new(SignHandler {
                    ledger: Arc::clone(&ledger),
                    gate: Arc::clone(&gate),
                }),
            ),
            ledger,
            gate,
            dir,
        };
        device.phone.join(200);
        cloud.wait_state(instance, "completed", LONG);

        assert_exactly_once(
            &cloud,
            instance,
            &device.ledger,
            &[EffectState::Unknown, EffectState::Committed],
        );
        assert!(
            cloud.task(first.id).is_none(),
            "{backend}: superseded by the retry"
        );
        let events = cloud.attempt_events(first.id);
        assert!(
            events
                .iter()
                .any(|event| event.event == WorkerAttemptEventKind::Reclaimed
                    && event
                        .reason
                        .as_deref()
                        .is_some_and(|reason| reason.contains("after starting"))),
            "{backend}: released as started by the orphan drain: {events:?}"
        );
        let effects = ledger_effects(&device.ledger);
        assert_eq!(effects.len(), 2, "{backend}");
        assert_ne!(
            effects[0], effects[1],
            "{backend}: the retry has a new effect id"
        );
        device.phone.engine.shutdown();
    }
}

/// (d) The phone stalls past its lease; the real reaper reclaims the task
/// (receipt unknown → retry). The stalled handler's late completion is
/// rejected, and the retry runs once and commits.
#[test]
fn d_lease_loss_rejects_the_late_completion_and_retries_once() {
    for backend in backends() {
        let (cloud, _) = cloud(&backend);
        let device = phone(&cloud, &cloud.v1(), Gate::closed());
        device.phone.join(200);
        let runtime = device.phone.runtime_id();
        let sequence = cloud.create_sequence("lease", &roundtrip_blocks(&runtime, Some(2)));
        let instance = cloud.create_instance(sequence, &json!({}));
        device.gate.wait_arrivals(1, LONG);
        let first = sign_task(&cloud, instance).unwrap();
        let backend = cloud.backend;
        assert_eq!(first.lease_secs, Some(120), "{backend}: mobile lease");

        // Stall past the lease (the lease clock is moved, not the phone).
        cloud.expire_lease(first.id);
        let retry = wait_for("the reaper's retry", LONG, || {
            sign_task(&cloud, instance).filter(|task| task.attempt > first.attempt)
        });
        assert!(cloud.task(first.id).is_none(), "{backend}");
        assert_eq!(
            cloud.block_receipts(instance, "sign")[0].state,
            EffectState::Unknown,
            "{backend}: never blindly requeued"
        );
        assert_eq!(retry.state, WorkerTaskState::Pending, "{backend}");

        // The stalled handler finishes; its completion is rejected.
        device.gate.release();
        cloud.wait_state(instance, "completed", LONG);
        assert_eq!(
            device.phone.engine.worker_stats().lost,
            1,
            "{backend}: the late completion lost the lease"
        );
        let (status, _) = cloud.call(
            &device.phone.credential(),
            "POST",
            &format!("/workers/tasks/{}/complete", first.id),
            Some(&json!({"worker_id": runtime, "claim_epoch": first.claim_epoch, "output": {}})),
        );
        assert_eq!(
            status, 404,
            "{backend}: a task superseded by its retry is gone"
        );
        assert_exactly_once(
            &cloud,
            instance,
            &device.ledger,
            &[EffectState::Unknown, EffectState::Committed],
        );
        let reclaimed = cloud.attempt_events(first.id);
        assert!(
            reclaimed
                .iter()
                .any(|event| event.event == WorkerAttemptEventKind::Reclaimed),
            "{backend}: {reclaimed:?}"
        );
        assert_eq!(
            ledger_len(&device.ledger),
            2,
            "{backend}: late run + one retry"
        );
        device.phone.engine.shutdown();
    }
}

/// (e) While `sign` is claimed by the phone the execution cannot be handed
/// off: the preview reports the in-flight effect, the handoff is refused,
/// and a capsule export is refused for the in-flight worker task. The
/// phone's completion still lands and ownership is unchanged.
#[test]
fn e_handoff_is_refused_while_the_phone_holds_the_step() {
    for backend in backends() {
        let (cloud, _) = cloud(&backend);
        let device = phone(&cloud, &cloud.v1(), Gate::closed());
        device.phone.join(200);
        let runtime = device.phone.runtime_id();
        let sequence = cloud.create_sequence("handoff", &roundtrip_blocks(&runtime, None));
        let instance = cloud.create_instance(sequence, &json!({}));
        device.gate.wait_arrivals(1, LONG);
        cloud.wait_state(instance, "waiting", LONG);
        let backend = cloud.backend;

        let tenant = TenantId::unchecked(&cloud.tenant);
        let execution = cloud
            .block_on(
                cloud
                    .storage
                    .get_continuity_execution_by_instance(&tenant, InstanceId::from_uuid(instance)),
            )
            .unwrap()
            .expect("side-effecting steps enroll the instance");

        // A registered desktop to hand off to.
        let destination = Uuid::now_v7();
        let now = chrono::Utc::now();
        let (status, body) = cloud.root(
            "POST",
            "/runtimes/register",
            Some(&json!({"tenant_id": cloud.tenant, "capabilities": {
                "runtime_id": destination, "kind": "desktop", "trust": "registered",
                "handlers": ["phone_sign", "cloud_finish"], "offline_capable": true,
                "observed_at": now.to_rfc3339(),
                "expires_at": (now + chrono::Duration::seconds(240)).to_rfc3339(),
            }})),
        );
        assert!(status == 200 || status == 201, "{backend}: {body}");

        let (status, preview) = cloud.root(
            "POST",
            &format!(
                "/continuity/executions/{}/handoff-preview",
                execution.continuity_id
            ),
            Some(&json!({"tenant_id": cloud.tenant, "destination_runtime_id": destination})),
        );
        assert_eq!(status, 200, "{backend}: {preview}");
        assert_eq!(
            preview["compatible"], false,
            "{backend}: in-flight effect: {preview}"
        );
        assert!(
            !preview["unresolved_effects"].as_array().unwrap().is_empty(),
            "{backend}: {preview}"
        );
        let (status, refused) = cloud.root(
            "POST",
            "/continuity/handoffs",
            Some(&json!({
                "tenant_id": cloud.tenant, "continuity_id": execution.continuity_id,
                "destination_runtime_id": destination,
                "placement_decision_id": preview["placement_decision"]["id"],
                "preview_sha256": preview["preview_sha256"],
            })),
        );
        assert_eq!(status, 409, "{backend}: handoff refused: {refused}");

        let exported = cloud.block_on(orch8_engine::capsule::export_paused_capsule_manifest(
            cloud.storage.as_ref(),
            orch8_engine::capsule::CapsuleExportRequest {
                continuity: execution.clone(),
                destination_runtime_id: Some(orch8_types::continuity::RuntimeId::from_uuid(
                    destination,
                )),
                requirements: orch8_types::continuity::CapsuleRequirements::default(),
                expires_at: chrono::Utc::now() + chrono::Duration::minutes(5),
                signing_key_id: "e2e".into(),
                encryption_key_id: "e2e".into(),
            },
            &orch8_types::encryption::FieldEncryptor::from_bytes(&[7; 32]),
        ));
        assert!(
            matches!(
                exported,
                Err(orch8_engine::capsule::CapsuleServiceError::WorkerTasksInFlight(1))
            ),
            "{backend}: capsule export refused: {exported:?}"
        );

        device.gate.release();
        cloud.wait_state(instance, "completed", LONG);
        assert_exactly_once(&cloud, instance, &device.ledger, &[EffectState::Committed]);
        let after = cloud
            .block_on(
                cloud
                    .storage
                    .get_continuity_execution(&tenant, execution.continuity_id),
            )
            .unwrap()
            .unwrap();
        assert_eq!(
            after.epoch, execution.epoch,
            "{backend}: no handoff happened"
        );
        assert_eq!(
            after.owner_runtime_id, execution.owner_runtime_id,
            "{backend}"
        );
        assert_eq!(
            after.current_instance_id, execution.current_instance_id,
            "{backend}"
        );
        device.phone.engine.shutdown();
    }
}

// ---------------------------------------------------------------------------
// (f) Device-mesh delegation (Feature 29)
// ---------------------------------------------------------------------------

/// Native handler for the parent's `delegate` step: the phone delegates the
/// photo classification to a desktop through the control plane with its
/// own credential (grant, then destination-bound delegation claim).
struct DelegateHandler {
    plan: Arc<Mutex<Value>>,
    base: String,
    key: String,
    tenant: String,
}

impl orch8_mobile::StepHandler for DelegateHandler {
    fn execute(
        &self,
        _step_name: String,
        input: String,
    ) -> Result<String, orch8_mobile::HandlerError> {
        let params: Value = serde_json::from_str(&input).unwrap_or(Value::Null);
        let plan = self.plan.lock().unwrap().clone();
        let (base, key, tenant) = (self.base.clone(), self.key.clone(), self.tenant.clone());
        // The native call runs on a blocking thread; the app's own HTTP
        // stack is modelled with a private runtime on a plain thread.
        std::thread::spawn(move || {
            let rt = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .unwrap();
            rt.block_on(async move {
                let http = reqwest::Client::new();
                let post = |path: &str, body: Value| {
                    http.post(format!("{base}{path}"))
                        .header("x-api-key", &key)
                        .json(&body)
                        .send()
                };
                let grant: Value = post(
                    "/continuity/grants",
                    json!({
                        "tenant_id": tenant, "continuity_id": plan["continuity_id"],
                        "destination_runtime_id": plan["desktop"],
                        "allowed_actions": ["accept"], "ttl_seconds": 300,
                    }),
                )
                .await
                .map_err(|error| error.to_string())?
                .json()
                .await
                .map_err(|error| error.to_string())?;
                let delegation_id = Uuid::now_v7();
                let response = post(
                    "/continuity/delegations/claim",
                    json!({
                        "tenant_id": tenant,
                        "delegation": {
                            "id": delegation_id, "tenant_id": tenant,
                            "parent_continuity_id": plan["continuity_id"],
                            "parent_epoch": plan["epoch"],
                            "source_runtime_id": plan["phone"],
                            "destination_runtime_id": plan["desktop"],
                            "sub_sequence_id": plan["sub_sequence_id"],
                            "grant_id": grant["signed_grant"]["grant"]["id"],
                            "expires_at": (chrono::Utc::now() + chrono::Duration::minutes(4)).to_rfc3339(),
                        },
                        "signed_grant": grant["signed_grant"], "token": grant["token"],
                        "input": {"photo": {"id": params["photo_id"]}},
                    }),
                )
                .await
                .map_err(|error| error.to_string())?;
                let status = response.status();
                let claimed: Value = response.json().await.map_err(|error| error.to_string())?;
                if !status.is_success() {
                    return Err(format!("delegation claim {status}: {claimed}"));
                }
                Ok(json!({"delegation": {
                    "delegation_id": delegation_id,
                    "mailbox_task_id": claimed["mailbox_task_id"],
                }})
                .to_string())
            })
        })
        .join()
        .unwrap()
        .map_err(|message| orch8_mobile::HandlerError::Permanent { message })
    }
}

struct Capture;

impl orch8_mobile::StepHandler for Capture {
    fn execute(
        &self,
        _step_name: String,
        _input: String,
    ) -> Result<String, orch8_mobile::HandlerError> {
        Ok(json!({"photo": {"id": "photo-1", "bytes": 4096}}).to_string())
    }
}

fn delegation_cloud(backend: &Backend) -> Cloud {
    Cloud::start(backend, |registry| {
        // The parent waits for the integrated delegation result
        // (`context.data.delegations.<id>`), re-checking on each retry.
        registry.register("cloud_await_delegation", |ctx| async move {
            let id = ctx.context.data["delegation"]["delegation_id"]
                .as_str()
                .unwrap_or_default()
                .to_owned();
            let result = ctx.context.data["delegations"][id.as_str()].clone();
            if result.is_null() {
                return Err(orch8_types::error::StepError::Retryable {
                    message: "delegation result not integrated yet".into(),
                    details: None,
                });
            }
            Ok(json!({"delegation_result": result}))
        });
        registry.register("cloud_summarize", |ctx| async move {
            let id = ctx.context.data["delegation"]["delegation_id"]
                .as_str()
                .unwrap_or_default()
                .to_owned();
            let result = &ctx.context.data["delegations"][id.as_str()];
            Ok(json!({"summary": {
                "labels": result["output"]["outputs"]["classify"]["labels"],
                "classified_by": result["runtime_id"],
            }}))
        });
    })
}

/// (f) A phone-owned parent delegates a capability-specific sub-sequence to
/// a desktop node through the server mailbox; the desktop disconnects and
/// reconnects repeatedly (before claiming, mid-run, and while delivering
/// its result, plus a duplicate delivery) and the result is integrated into
/// the parent exactly once.
#[test]
fn f_phone_delegates_to_a_desktop_across_disconnects() {
    for backend in backends() {
        let cloud = delegation_cloud(&backend);
        let backend_name = cloud.backend;

        // Phone: owns the parent execution, runs capture + delegate.
        let phone_link = Link::start(&cloud);
        let dir = tempfile::tempdir().unwrap();
        let phone_auth = PhoneAuth::DeviceSession(
            cloud.device_sessions(&["phone_capture", "phone_delegate"], 3_600),
        );
        let plan: Arc<Mutex<Value>> = Arc::default();
        let phone = Phone::open(
            &dir.path().join("phone.db").to_string_lossy(),
            &format!("phone-{}", Uuid::now_v7().simple()),
            &phone_auth,
            &phone_link.base,
            "phone_capture",
            Arc::new(Capture),
        );
        phone
            .engine
            .register_handler(
                "phone_delegate".into(),
                Arc::new(DelegateHandler {
                    plan: Arc::clone(&plan),
                    base: phone_link.base.clone(),
                    // The app calls the delegation API with the phone's own
                    // device session (it owns the parent execution).
                    key: phone.credential(),
                    tenant: cloud.tenant.clone(),
                }),
            )
            .unwrap();
        phone.join(200);
        let phone_runtime = phone.runtime_id();

        // Desktop: embedded engine + lease protocol behind its own link.
        let desktop_rt = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2)
            .enable_all()
            .build()
            .unwrap();
        let desktop_link = Link::start(&cloud);
        let desktop_key = cloud.mint_key(&["worker", "operator"]);
        let desktop_dir = tempfile::tempdir().unwrap();
        let desktop = desktop_rt.block_on(Desktop::start(
            &desktop_key,
            &desktop_dir.path().join("desktop.db").to_string_lossy(),
            |builder| {
                builder.handler("desktop_classify", |ctx: orch8::StepContext| async move {
                    Ok(json!({"labels": ["receipt", "grocery"],
                              "photo": ctx.context.data["photo"]["id"]}))
                })
            },
        ));
        let serves = ["desktop_classify"];
        let base = desktop_link.base.clone();
        assert_eq!(
            desktop_rt.block_on(desktop.register(&base, &cloud.tenant, &serves)),
            Delivery::Accepted,
            "{backend_name}"
        );

        let sub_sequence = cloud.create_sequence(
            "classify",
            &json!([{"type": "step", "id": "classify", "handler": "desktop_classify",
                     "params": {}, "cancellable": true}]),
        );
        let placed = |handler: &str, params: Value| {
            let mut params = params;
            params["$runtime"] = json!({"runtime_id": phone_runtime});
            json!({"type": "step", "id": handler.trim_start_matches("phone_"), "handler": handler,
                   "params": params, "cancellable": true})
        };
        let parent_sequence = cloud.create_sequence(
            "parent",
            &json!([
                placed("phone_capture", json!({})),
                placed("phone_delegate", json!({"photo_id": "{{context.data.photo.id}}"})),
                {"type": "step", "id": "await", "handler": "cloud_await_delegation",
                 "params": {}, "cancellable": true,
                 "retry": {"max_attempts": 600, "initial_backoff": 100, "max_backoff": 100,
                           "backoff_multiplier": 1.0}},
                {"type": "step", "id": "summarize", "handler": "cloud_summarize",
                 "params": {}, "cancellable": true},
            ]),
        );

        // The desktop drops off the network before anything is delegated.
        desktop_link.set_online(false);

        // Parent: first fire deferred so the phone can own it before any
        // step runs.
        let (status, created) = cloud.root(
            "POST",
            "/instances",
            Some(&json!({
                "sequence_id": parent_sequence, "tenant_id": cloud.tenant, "namespace": "e2e",
                "context": {"data": {}, "config": {}, "audit": []},
                "next_fire_at": (chrono::Utc::now() + chrono::Duration::seconds(2)).to_rfc3339(),
            })),
        );
        assert_eq!(status, 201, "{backend_name}: {created}");
        let parent: Uuid = created["id"].as_str().unwrap().parse().unwrap();
        let (status, execution) = cloud.root(
            "POST",
            "/continuity/executions",
            Some(&json!({"tenant_id": cloud.tenant, "instance_id": parent,
                         "runtime_id": phone_runtime})),
        );
        assert_eq!(status, 201, "{backend_name}: {execution}");
        *plan.lock().unwrap() = json!({
            "continuity_id": execution["continuity_id"], "epoch": execution["epoch"],
            "phone": phone_runtime, "desktop": desktop.runtime_id,
            "sub_sequence_id": sub_sequence,
        });

        // The phone captures and delegates; the result is only a mailbox
        // task targeted at the (offline) desktop.
        let mailbox = wait_for("the delegation mailbox task", LONG, || {
            cloud
                .tasks(parent)
                .into_iter()
                .find(|task| task.handler_name == orch8_engine::delegation::DELEGATION_HANDLER)
        });
        assert_eq!(mailbox.state, WorkerTaskState::Pending, "{backend_name}");
        assert_eq!(
            mailbox
                .requirements
                .runtime_id
                .map(orch8_types::continuity::RuntimeId::into_uuid),
            Some(desktop.runtime_id),
            "{backend_name}"
        );

        // Disconnect 1: the offline desktop cannot reach its mailbox.
        let offline = desktop_rt.block_on(desktop.poll(&base, &serves));
        assert_eq!(
            offline.unwrap_err(),
            Delivery::Unreachable,
            "{backend_name}"
        );
        std::thread::sleep(Duration::from_millis(500));
        assert_eq!(
            cloud.task(mailbox.id).unwrap().state,
            WorkerTaskState::Pending,
            "{backend_name}: the mailbox waits for its destination"
        );

        // Reconnect: re-advertise, claim, fetch the sub-sequence.
        desktop_link.set_online(true);
        assert_eq!(
            desktop_rt.block_on(desktop.register(&base, &cloud.tenant, &serves)),
            Delivery::Accepted
        );
        let task = desktop_rt
            .block_on(desktop.poll(&base, &serves))
            .unwrap()
            .into_iter()
            .next()
            .expect("the desktop claims its mailbox task");
        assert_eq!(task["id"], mailbox.id.to_string(), "{backend_name}");
        let (status, sequence) = desktop_rt
            .block_on(desktop.get(&base, &format!("/sequences/{sub_sequence}")))
            .unwrap();
        assert_eq!(status, 200, "{backend_name}: {sequence}");

        // Disconnect 2 (mid-run): heartbeats fail, the local run goes on.
        desktop_link.set_online(false);
        assert_eq!(
            desktop_rt.block_on(desktop.lease_call(&base, &task, "heartbeat", json!({}))),
            Delivery::Unreachable
        );
        let output = desktop_rt.block_on(desktop.run_delegation(sequence, &task));
        assert_eq!(output["state"], "completed", "{backend_name}: {output}");
        desktop_link.set_online(true);
        assert_eq!(
            desktop_rt.block_on(desktop.lease_call(&base, &task, "heartbeat", json!({}))),
            Delivery::Accepted,
            "{backend_name}: the lease survived the short disconnect"
        );

        // Disconnect 3 (delivering the result), then a duplicate delivery.
        desktop_link.set_online(false);
        let complete = json!({"output": output});
        assert_eq!(
            desktop_rt.block_on(desktop.lease_call(&base, &task, "complete", complete.clone())),
            Delivery::Unreachable
        );
        desktop_link.set_online(true);
        assert_eq!(
            desktop_rt.block_on(desktop.lease_call(&base, &task, "complete", complete.clone())),
            Delivery::Accepted
        );
        assert_eq!(
            desktop_rt.block_on(desktop.lease_call(&base, &task, "complete", complete)),
            Delivery::Accepted,
            "{backend_name}: a re-delivered completion is idempotent"
        );

        cloud.wait_state(parent, "completed", Duration::from_secs(60));

        // Integrated exactly once, into the phone-owned parent.
        let delegation_id =
            cloud.block_outputs(parent, "delegate")[0]["output"]["delegation"]["delegation_id"]
                .as_str()
                .unwrap()
                .to_owned();
        let block = format!("delegation-{delegation_id}");
        assert_eq!(
            cloud.block_outputs(parent, &block).len(),
            1,
            "{backend_name}: integrated once despite the duplicate delivery"
        );
        let receipts = cloud.block_receipts(parent, &block);
        assert_eq!(
            receipts
                .iter()
                .map(|receipt| receipt.state)
                .collect::<Vec<_>>(),
            [EffectState::Committed],
            "{backend_name}"
        );
        assert_eq!(
            desktop.runs.lock().unwrap().len(),
            1,
            "{backend_name}: the sub-sequence ran once on the desktop"
        );
        let data = &cloud.instance(parent)["context"]["data"];
        let result = &data["delegations"][delegation_id.as_str()];
        assert_eq!(result["status"], "completed", "{backend_name}: {data}");
        assert_eq!(
            result["runtime_id"],
            desktop.runtime_id.to_string(),
            "{backend_name}"
        );
        let summary = &cloud.block_outputs(parent, "summarize")[0]["output"]["summary"];
        assert_eq!(
            summary["labels"],
            json!(["receipt", "grocery"]),
            "{backend_name}: the parent used the desktop's result: {summary}"
        );
        assert_eq!(
            summary["classified_by"],
            desktop.runtime_id.to_string(),
            "{backend_name}"
        );
        assert_eq!(
            result["output"]["outputs"]["classify"]["photo"], "photo-1",
            "{backend_name}: the desktop got the delegation's explicit input"
        );
        let events = cloud.attempt_events(mailbox.id);
        let count = |kind| events.iter().filter(|event| event.event == kind).count();
        assert_eq!(count(WorkerAttemptEventKind::Claimed), 1, "{backend_name}");
        assert_eq!(
            count(WorkerAttemptEventKind::Completed),
            1,
            "{backend_name}"
        );

        // The phone still owns the parent; the delegation left provenance.
        let continuity_id = execution["continuity_id"].as_str().unwrap();
        let (_, owned) = cloud.root(
            "GET",
            &format!(
                "/continuity/executions/{continuity_id}?tenant_id={}",
                cloud.tenant
            ),
            None,
        );
        assert_eq!(owned["owner_runtime_id"], phone_runtime, "{backend_name}");
        assert_eq!(owned["epoch"], execution["epoch"], "{backend_name}");
        let (_, provenance) = cloud.root(
            "GET",
            &format!(
                "/continuity/executions/{continuity_id}/provenance?tenant_id={}",
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
            kinds.contains(&"device_delegation"),
            "{backend_name}: {provenance}"
        );

        phone.engine.shutdown();
        desktop_rt.block_on(desktop.engine.shutdown());
    }
}
