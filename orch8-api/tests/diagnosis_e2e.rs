//! End-to-end coverage for `GET /instances/{id}/diagnosis` (the Stuck
//! Instance Doctor) against the full axum router + in-memory `SQLite`.
//!
//! Evidence is seeded two ways: through the public API (sequences,
//! instances) and directly through `srv.storage` (state transitions,
//! worker tasks, registrations, version pins, circuit breakers, signals,
//! event waits, parent/child instances) so every diagnosis code is
//! reachable exactly the way production reaches it.

use chrono::{DateTime, Duration, Utc};
use orch8_api::test_harness::{TestServer, spawn_test_server};
use orch8_storage::{AdminStore, InstanceStore, SignalStore, WorkerStore};
use orch8_types::circuit_breaker::{BreakerState, CircuitBreakerState};
use orch8_types::context::ExecutionContext;
use orch8_types::event_correlation::{EventWait, JoinMode, WaitStatus};
use orch8_types::ids::{BlockId, InstanceId, Namespace, SequenceId, TenantId};
use orch8_types::instance::{InstanceState, Priority, TaskInstance};
use orch8_types::signal::{Signal, SignalType};
use orch8_types::worker::{WorkerRegistration, WorkerTask, WorkerTaskState, WorkerVersionPin};
use reqwest::StatusCode;
use serde_json::{Value, json};
use uuid::Uuid;

const TENANT: &str = "t1";

// ---------------------------------------------------------------------------
// Harness helpers
// ---------------------------------------------------------------------------

struct Env {
    srv: TestServer,
    client: reqwest::Client,
    base: String,
}

async fn setup() -> Env {
    let srv = spawn_test_server().await;
    let base = srv.v1_url();
    Env {
        srv,
        client: reqwest::Client::new(),
        base,
    }
}

impl Env {
    async fn create_sequence(&self, blocks: Value) -> String {
        let id = Uuid::now_v7().to_string();
        let body = json!({
            "id": id,
            "tenant_id": TENANT,
            "namespace": "default",
            "name": format!("diag-e2e-{id}"),
            "version": 1,
            "blocks": blocks,
            "created_at": Utc::now().to_rfc3339(),
        });
        let resp = self
            .client
            .post(format!("{}/sequences", self.base))
            .header("X-Tenant-Id", TENANT)
            .json(&body)
            .send()
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::CREATED);
        id
    }

    /// A one-step sequence with no human gate.
    async fn simple_sequence(&self) -> String {
        self.create_sequence(json!([
            {"type": "step", "id": "a", "handler": "noop", "params": {}}
        ]))
        .await
    }

    /// A sequence whose first step waits for human input (block id `gate`).
    async fn gated_sequence(&self) -> String {
        self.create_sequence(json!([
            {"type": "step", "id": "gate", "handler": "noop", "params": {},
             "wait_for_input": {"prompt": "approve?"}}
        ]))
        .await
    }

    /// Create an instance through the public API (`tenant_id`, `sequence_id` and
    /// namespace are the required body fields).
    async fn create_instance(&self, seq_id: &str, extra: Value) -> String {
        let mut body = json!({
            "tenant_id": TENANT,
            "sequence_id": seq_id,
            "namespace": "default",
        });
        if let (Some(obj), Some(extra_obj)) = (body.as_object_mut(), extra.as_object()) {
            for (k, v) in extra_obj {
                obj.insert(k.clone(), v.clone());
            }
        }
        let resp = self
            .client
            .post(format!("{}/instances", self.base))
            .header("X-Tenant-Id", TENANT)
            .json(&body)
            .send()
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::CREATED);
        let v: Value = resp.json().await.unwrap();
        v["id"].as_str().unwrap().to_string()
    }

    /// Seed a fully hand-built instance row directly through storage —
    /// needed for old `updated_at`, parent/child links, and past timers.
    async fn seed_instance(&self, inst: &TaskInstance) {
        self.srv.storage.create_instance(inst).await.unwrap();
    }

    async fn set_state(&self, id: &str, state: InstanceState, fire_at: Option<DateTime<Utc>>) {
        self.srv
            .storage
            .update_instance_state(iid(id), state, fire_at)
            .await
            .unwrap();
    }

    async fn get_diagnosis_as(&self, id: &str, tenant: Option<&str>) -> (StatusCode, Value) {
        let mut req = self
            .client
            .get(format!("{}/instances/{id}/diagnosis", self.base));
        if let Some(t) = tenant {
            req = req.header("X-Tenant-Id", t);
        }
        let resp = req.send().await.unwrap();
        let status = resp.status();
        let body = resp.json().await.unwrap_or(Value::Null);
        (status, body)
    }

    async fn diagnose(&self, id: &str) -> Value {
        let (status, body) = self.get_diagnosis_as(id, Some(TENANT)).await;
        assert_eq!(status, StatusCode::OK, "{body}");
        body
    }
}

fn iid(id: &str) -> InstanceId {
    InstanceId::from_uuid(Uuid::parse_str(id).unwrap())
}

fn mk_instance(seq_id: &str, state: InstanceState) -> TaskInstance {
    let now = Utc::now();
    TaskInstance {
        claimed_at: None,
        sub_tenant: None,
        id: InstanceId::new(),
        sequence_id: SequenceId::from_uuid(Uuid::parse_str(seq_id).unwrap()),
        tenant_id: TenantId::unchecked(TENANT),
        namespace: Namespace::new("default"),
        state,
        next_fire_at: None,
        priority: Priority::Normal,
        timezone: "UTC".into(),
        metadata: json!({}),
        context: ExecutionContext::default(),
        concurrency_key: None,
        max_concurrency: None,
        idempotency_key: None,
        session_id: None,
        parent_instance_id: None,
        budget: None,
        created_at: now - Duration::minutes(5),
        updated_at: now - Duration::seconds(5),
    }
}

fn mk_task(instance_id: &str, block: &str, handler: &str, state: WorkerTaskState) -> WorkerTask {
    WorkerTask {
        id: Uuid::now_v7(),
        instance_id: iid(instance_id),
        block_id: BlockId::new(block),
        handler_name: handler.to_string(),
        queue_name: None,
        requirements: orch8_types::continuity::CapsuleRequirements::default(),
        params: json!({}),
        context: json!({}),
        attempt: 1,
        timeout_ms: None,
        state,
        worker_id: Some("w-1".into()),
        claimed_at: Some(Utc::now() - Duration::seconds(5)),
        heartbeat_at: Some(Utc::now() - Duration::seconds(5)),
        claim_epoch: 1,
        resume_checkpoint: None,
        checkpoint_seq: 0,
        completed_at: None,
        output: None,
        error_message: None,
        error_retryable: None,
        created_at: Utc::now() - Duration::seconds(5),
        effect_id: None,
        continuity_epoch: None,
        heartbeat_timeout_secs: None,
        wait_for_callback: false,
        lease_secs: None,
        carries_credentials: false,
        claimed_runtime_kind: None,
    }
}

fn mk_registration(
    handler: &str,
    version: Option<&str>,
    tenant: Option<&str>,
) -> WorkerRegistration {
    WorkerRegistration {
        worker_id: "w-1".into(),
        handler_name: handler.to_string(),
        queue_name: None,
        version: version.map(ToString::to_string),
        tenant_id: tenant.map(ToString::to_string),
        last_seen_at: Utc::now(),
        available_poll_slots: None,
    }
}

fn mk_breaker(tenant: &str, handler: &str, state: BreakerState) -> CircuitBreakerState {
    CircuitBreakerState {
        tenant_id: TenantId::unchecked(tenant),
        handler: handler.into(),
        state,
        failure_count: 6,
        failure_threshold: 5,
        cooldown_secs: 300,
        opened_at: Some(Utc::now() - Duration::seconds(10)),
    }
}

fn mk_signal(instance_id: &str, age_secs: i64, delivered: bool) -> Signal {
    Signal {
        id: Uuid::now_v7(),
        instance_id: iid(instance_id),
        signal_type: SignalType::Resume,
        payload: json!({}),
        delivered,
        created_at: Utc::now() - Duration::seconds(age_secs),
        delivered_at: None,
    }
}

fn mk_event_wait(instance_id: &str, block: &str, status: WaitStatus) -> EventWait {
    EventWait {
        id: Uuid::now_v7(),
        tenant_id: TENANT.into(),
        instance_id: Uuid::parse_str(instance_id).unwrap(),
        block_id: block.into(),
        event_names: vec!["payment_received".into(), "inventory_reserved".into()],
        correlation_key: "order-42".into(),
        join_mode: JoinMode::All,
        status,
        matched_names: vec!["inventory_reserved".into()],
        matched_event_ids: vec![Uuid::now_v7()],
        created_at: Utc::now(),
    }
}

fn codes(report: &Value) -> Vec<String> {
    report["diagnoses"]
        .as_array()
        .unwrap()
        .iter()
        .map(|d| d["code"].as_str().unwrap().to_string())
        .collect()
}

fn find<'r>(report: &'r Value, code: &str) -> &'r Value {
    report["diagnoses"]
        .as_array()
        .unwrap()
        .iter()
        .find(|d| d["code"] == code)
        .unwrap_or_else(|| panic!("expected {code}, got {:?}", codes(report)))
}

// ===========================================================================
// Routing, auth, 404s
// ===========================================================================

#[tokio::test]
async fn unknown_instance_returns_404() {
    let env = setup().await;
    let (status, _) = env
        .get_diagnosis_as(&Uuid::now_v7().to_string(), Some(TENANT))
        .await;
    assert_eq!(status, StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn malformed_instance_id_is_a_client_error() {
    let env = setup().await;
    let (status, _) = env.get_diagnosis_as("not-a-uuid", Some(TENANT)).await;
    assert!(status.is_client_error(), "{status}");
}

#[tokio::test]
async fn other_tenant_cannot_read_diagnosis() {
    let env = setup().await;
    let seq = env.simple_sequence().await;
    let inst = env.create_instance(&seq, json!({})).await;
    let (status, _) = env.get_diagnosis_as(&inst, Some("intruder")).await;
    assert_eq!(status, StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn owning_tenant_reads_diagnosis_ok() {
    let env = setup().await;
    let seq = env.simple_sequence().await;
    let inst = env.create_instance(&seq, json!({})).await;
    let (status, body) = env.get_diagnosis_as(&inst, Some(TENANT)).await;
    assert_eq!(status, StatusCode::OK, "{body}");
}

#[tokio::test]
async fn admin_without_tenant_header_reads_diagnosis_ok() {
    // The harness disables API-key auth, marking requests as admin.
    let env = setup().await;
    let seq = env.simple_sequence().await;
    let inst = env.create_instance(&seq, json!({})).await;
    let (status, _) = env.get_diagnosis_as(&inst, None).await;
    assert_eq!(status, StatusCode::OK);
}

#[tokio::test]
async fn report_has_top_level_shape() {
    let env = setup().await;
    let seq = env.simple_sequence().await;
    let inst = env.create_instance(&seq, json!({})).await;
    let report = env.diagnose(&inst).await;
    assert_eq!(report["instance_id"].as_str().unwrap(), inst);
    assert!(report["state"].is_string());
    assert!(report["generated_at"].is_string());
    assert!(report["diagnoses"].is_array());
    assert!(!report["diagnoses"].as_array().unwrap().is_empty());
}

#[tokio::test]
async fn diagnosis_entries_flatten_finding_fields() {
    let env = setup().await;
    let seq = env.simple_sequence().await;
    let fire_at = Utc::now() + Duration::hours(6);
    let inst = env
        .create_instance(&seq, json!({"next_fire_at": fire_at.to_rfc3339()}))
        .await;
    let report = env.diagnose(&inst).await;
    let d = find(&report, "WAITING_UNTIL");
    // category/health sit next to the flattened Finding fields.
    assert_eq!(d["category"], "direct_evidence");
    assert_eq!(d["health"], "expected");
    assert_eq!(d["confidence"], "certain");
    assert_eq!(d["severity"], "info");
    assert!(d["summary"].as_str().unwrap().contains("timer"));
    assert!(d["observed_at"].is_string());
    // Not nested under a "finding" key.
    assert!(d.get("finding").is_none());
}

// ===========================================================================
// Terminal states
// ===========================================================================

#[tokio::test]
async fn completed_instance_reports_terminal_state_only() {
    let env = setup().await;
    let seq = env.simple_sequence().await;
    let inst = env.create_instance(&seq, json!({})).await;
    env.set_state(&inst, InstanceState::Running, None).await;
    env.set_state(&inst, InstanceState::Completed, None).await;
    let report = env.diagnose(&inst).await;
    assert_eq!(codes(&report), vec!["TERMINAL_STATE"]);
    assert_eq!(report["state"], "completed");
}

#[tokio::test]
async fn cancelled_instance_reports_terminal_state_only() {
    let env = setup().await;
    let seq = env.simple_sequence().await;
    let inst = env.create_instance(&seq, json!({})).await;
    env.set_state(&inst, InstanceState::Cancelled, None).await;
    let report = env.diagnose(&inst).await;
    assert_eq!(codes(&report), vec!["TERMINAL_STATE"]);
    assert_eq!(report["state"], "cancelled");
}

#[tokio::test]
async fn failed_instance_offers_dlq_retry_remediation() {
    let env = setup().await;
    let seq = env.simple_sequence().await;
    let inst = env.create_instance(&seq, json!({})).await;
    env.set_state(&inst, InstanceState::Running, None).await;
    env.set_state(&inst, InstanceState::Failed, None).await;
    let report = env.diagnose(&inst).await;
    let d = find(&report, "TERMINAL_STATE");
    let rem = &d["remediation"][0];
    assert_eq!(rem["side_effect_risk"], true);
    let cmd = rem["command"].as_str().unwrap();
    assert!(cmd.contains("instance retry"), "{cmd}");
    assert!(cmd.contains(&inst), "{cmd}");
}

#[tokio::test]
async fn completed_terminal_has_no_remediation() {
    let env = setup().await;
    let seq = env.simple_sequence().await;
    let inst = env.create_instance(&seq, json!({})).await;
    env.set_state(&inst, InstanceState::Running, None).await;
    env.set_state(&inst, InstanceState::Completed, None).await;
    let report = env.diagnose(&inst).await;
    let d = find(&report, "TERMINAL_STATE");
    assert!(d.get("remediation").is_none(), "{d}");
}

#[tokio::test]
async fn terminal_diagnosis_is_certain_direct_evidence() {
    let env = setup().await;
    let seq = env.simple_sequence().await;
    let inst = env.create_instance(&seq, json!({})).await;
    env.set_state(&inst, InstanceState::Cancelled, None).await;
    let report = env.diagnose(&inst).await;
    let d = find(&report, "TERMINAL_STATE");
    assert_eq!(d["category"], "direct_evidence");
    assert_eq!(d["confidence"], "certain");
    assert_eq!(d["health"], "expected");
}

// ===========================================================================
// WAITING_UNTIL
// ===========================================================================

#[tokio::test]
async fn future_timer_is_ranked_first_as_waiting_until() {
    let env = setup().await;
    let seq = env.simple_sequence().await;
    let fire_at = Utc::now() + Duration::hours(3);
    let inst = env
        .create_instance(&seq, json!({"next_fire_at": fire_at.to_rfc3339()}))
        .await;
    let report = env.diagnose(&inst).await;
    assert_eq!(report["diagnoses"][0]["code"], "WAITING_UNTIL");
}

#[tokio::test]
async fn waiting_until_carries_next_fire_at_evidence() {
    let env = setup().await;
    let seq = env.simple_sequence().await;
    let fire_at = Utc::now() + Duration::hours(3);
    let inst = env
        .create_instance(&seq, json!({"next_fire_at": fire_at.to_rfc3339()}))
        .await;
    let report = env.diagnose(&inst).await;
    let d = find(&report, "WAITING_UNTIL");
    assert_eq!(d["evidence"][0]["label"], "next_fire_at");
}

#[tokio::test]
async fn waiting_instance_with_future_timer_has_no_external_event_guess() {
    let env = setup().await;
    let seq = env.simple_sequence().await;
    let inst = env.create_instance(&seq, json!({})).await;
    env.set_state(&inst, InstanceState::Running, None).await;
    env.set_state(
        &inst,
        InstanceState::Waiting,
        Some(Utc::now() + Duration::hours(1)),
    )
    .await;
    let report = env.diagnose(&inst).await;
    let cs = codes(&report);
    assert!(cs.contains(&"WAITING_UNTIL".to_string()), "{cs:?}");
    assert!(
        !cs.contains(&"WAITING_EXTERNAL_EVENT".to_string()),
        "{cs:?}"
    );
}

// ===========================================================================
// Paused / budget paused
// ===========================================================================

#[tokio::test]
async fn operator_paused_instance_reports_paused_with_resume_command() {
    let env = setup().await;
    let seq = env.simple_sequence().await;
    let inst = env.create_instance(&seq, json!({})).await;
    env.set_state(&inst, InstanceState::Paused, None).await;
    let report = env.diagnose(&inst).await;
    let d = find(&report, "PAUSED");
    let cmd = d["remediation"][0]["command"].as_str().unwrap();
    assert!(cmd.contains("resume"), "{cmd}");
    assert!(cmd.contains(&inst), "{cmd}");
}

#[tokio::test]
async fn budget_paused_metadata_switches_diagnosis_to_budget_paused() {
    let env = setup().await;
    let seq = env.simple_sequence().await;
    let inst = env.create_instance(&seq, json!({})).await;
    env.srv
        .storage
        .merge_instance_metadata(iid(&inst), &json!({"paused_reason": "budget_exceeded"}))
        .await
        .unwrap();
    env.set_state(&inst, InstanceState::Paused, None).await;
    let report = env.diagnose(&inst).await;
    let cs = codes(&report);
    assert!(cs.contains(&"BUDGET_PAUSED".to_string()), "{cs:?}");
    assert!(!cs.contains(&"PAUSED".to_string()), "{cs:?}");
    let d = find(&report, "BUDGET_PAUSED");
    assert_eq!(d["severity"], "warning");
}

#[tokio::test]
async fn budget_paused_breach_metadata_becomes_evidence() {
    let env = setup().await;
    let seq = env.simple_sequence().await;
    let inst = env.create_instance(&seq, json!({})).await;
    env.srv
        .storage
        .merge_instance_metadata(
            iid(&inst),
            &json!({
                "paused_reason": "budget_exceeded",
                "budget_breach": {"limit": "max_steps", "limit_value": 10, "actual": 11}
            }),
        )
        .await
        .unwrap();
    env.set_state(&inst, InstanceState::Paused, None).await;
    let report = env.diagnose(&inst).await;
    let d = find(&report, "BUDGET_PAUSED");
    assert_eq!(d["evidence"][0]["label"], "budget_breach");
    assert!(
        d["evidence"][0]["summary"]
            .as_str()
            .unwrap()
            .contains("max_steps")
    );
}

#[tokio::test]
async fn paused_instance_with_future_timer_reports_pause_not_timer() {
    let env = setup().await;
    let seq = env.simple_sequence().await;
    let inst = env.create_instance(&seq, json!({})).await;
    env.set_state(
        &inst,
        InstanceState::Paused,
        Some(Utc::now() + Duration::hours(2)),
    )
    .await;
    let report = env.diagnose(&inst).await;
    let cs = codes(&report);
    assert!(cs.contains(&"PAUSED".to_string()), "{cs:?}");
    assert!(!cs.contains(&"WAITING_UNTIL".to_string()), "{cs:?}");
}

// ===========================================================================
// Worker tasks: pending matrix
// ===========================================================================

/// Seed a Waiting instance plus one pending worker task for `handler`.
async fn waiting_with_pending_task(env: &Env, handler: &str) -> String {
    let seq = env.simple_sequence().await;
    let inst = env.create_instance(&seq, json!({})).await;
    env.set_state(&inst, InstanceState::Running, None).await;
    env.set_state(&inst, InstanceState::Waiting, None).await;
    env.srv
        .storage
        .create_worker_task(&mk_task(&inst, "blk-1", handler, WorkerTaskState::Pending))
        .await
        .unwrap();
    inst
}

#[tokio::test]
async fn pending_task_without_any_worker_is_no_compatible_worker() {
    let env = setup().await;
    let inst = waiting_with_pending_task(&env, "charge_card").await;
    let report = env.diagnose(&inst).await;
    let d = find(&report, "NO_COMPATIBLE_WORKER");
    assert_eq!(d["category"], "direct_evidence");
    assert_eq!(d["severity"], "error");
    assert!(d["summary"].as_str().unwrap().contains("charge_card"));
}

#[tokio::test]
async fn diagnosis_finds_a_live_task_behind_other_runs_and_completed_history() {
    let env = setup().await;
    let seq = env.simple_sequence().await;
    let selected = env.create_instance(&seq, json!({})).await;
    let neighbor = env.create_instance(&seq, json!({})).await;
    env.set_state(&selected, InstanceState::Waiting, None).await;
    let now = Utc::now();
    let mut blocker = mk_task(
        &selected,
        "selected",
        "selected_handler",
        WorkerTaskState::Pending,
    );
    blocker.created_at = now - Duration::hours(2);
    env.srv.storage.create_worker_task(&blocker).await.unwrap();

    for index in 0..101 {
        let mut completed = mk_task(
            &selected,
            &format!("finished-{index}"),
            "history_handler",
            WorkerTaskState::Completed,
        );
        completed.created_at = now - Duration::minutes(10);
        env.srv
            .storage
            .create_worker_task(&completed)
            .await
            .unwrap();
        let mut unrelated = mk_task(
            &neighbor,
            &format!("queued-{index}"),
            "other_handler",
            WorkerTaskState::Pending,
        );
        unrelated.created_at = now - Duration::minutes(5);
        env.srv
            .storage
            .create_worker_task(&unrelated)
            .await
            .unwrap();
    }

    let report = env.diagnose(&selected).await;
    let finding = find(&report, "NO_COMPATIBLE_WORKER");
    assert!(
        finding["summary"]
            .as_str()
            .unwrap()
            .contains("selected_handler")
    );
    assert_eq!(finding["evidence"][0]["label"], "worker_task_target");
    assert_eq!(
        finding["evidence"][0]["data"],
        json!({
            "instance_id": selected,
            "tenant_id": TENANT,
            "task_id": blocker.id,
            "block_id": "selected",
        })
    );
    assert!(!report.to_string().contains("other_handler"));
    assert!(
        !codes(&report)
            .iter()
            .any(|code| code == "WAITING_EXTERNAL_EVENT")
    );

    env.set_state(&selected, InstanceState::Failed, None).await;
    for (block, age) in [("prior-failure", 50), ("latest-failure", 30)] {
        let mut failure = mk_task(&selected, block, "failed_handler", WorkerTaskState::Failed);
        failure.created_at = now - Duration::minutes(age);
        failure.error_message = Some("worker call failed".into());
        env.srv.storage.create_worker_task(&failure).await.unwrap();
    }
    let explanation: Value = env
        .client
        .get(format!("{}/instances/{selected}/explain", env.base))
        .header("X-Tenant-Id", TENANT)
        .send()
        .await
        .unwrap()
        .error_for_status()
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(explanation["failure"]["block_id"], "latest-failure");
    assert_eq!(explanation["failure"]["handler"], "failed_handler");
}

#[tokio::test]
async fn diagnosis_reports_an_incomplete_sample_when_more_live_tasks_exist() {
    let env = setup().await;
    let seq = env.simple_sequence().await;
    let inst = env.create_instance(&seq, json!({})).await;
    env.set_state(&inst, InstanceState::Waiting, None).await;
    let first = Utc::now() - Duration::minutes(10);
    for index in 0..101 {
        let mut task = mk_task(
            &inst,
            &format!("queued-{index:03}"),
            "worker_handler",
            WorkerTaskState::Pending,
        );
        task.created_at = first + Duration::seconds(index);
        env.srv.storage.create_worker_task(&task).await.unwrap();
    }
    let report = env.diagnose(&inst).await;
    assert_eq!(
        codes(&report)
            .iter()
            .filter(|code| *code == "NO_COMPATIBLE_WORKER")
            .count(),
        100
    );
    assert!(
        find(&report, "EVIDENCE_INCOMPLETE")["summary"]
            .as_str()
            .unwrap()
            .contains("worker_tasks beyond the diagnostic limit")
    );
    let report = report.to_string();
    assert!(report.contains("queued-000"));
    assert!(!report.contains("queued-100"));
}

#[tokio::test]
async fn pending_task_with_live_worker_expects_pickup() {
    let env = setup().await;
    let inst = waiting_with_pending_task(&env, "charge_card").await;
    env.srv
        .storage
        .upsert_worker_registration(&mk_registration("charge_card", None, None))
        .await
        .unwrap();
    let report = env.diagnose(&inst).await;
    assert!(codes(&report).contains(&"WAITING_WORKER_PICKUP".to_string()));
}

#[tokio::test]
async fn old_pending_task_with_live_worker_flags_not_claiming() {
    let env = setup().await;
    let seq = env.simple_sequence().await;
    let inst = env.create_instance(&seq, json!({})).await;
    env.set_state(&inst, InstanceState::Waiting, None).await;
    let mut task = mk_task(&inst, "blk-1", "charge_card", WorkerTaskState::Pending);
    task.created_at = Utc::now() - Duration::minutes(10);
    env.srv.storage.create_worker_task(&task).await.unwrap();
    env.srv
        .storage
        .upsert_worker_registration(&mk_registration("charge_card", None, None))
        .await
        .unwrap();
    let report = env.diagnose(&inst).await;
    let d = find(&report, "WORKER_NOT_CLAIMING");
    assert!(d["summary"].as_str().unwrap().contains("charge_card"));
    assert_eq!(d["evidence"][0]["label"], "task_created_at");
}

#[tokio::test]
async fn worker_registered_for_other_tenant_does_not_count() {
    let env = setup().await;
    let inst = waiting_with_pending_task(&env, "charge_card").await;
    env.srv
        .storage
        .upsert_worker_registration(&mk_registration("charge_card", None, Some("t2")))
        .await
        .unwrap();
    let report = env.diagnose(&inst).await;
    assert!(codes(&report).contains(&"NO_COMPATIBLE_WORKER".to_string()));
}

#[tokio::test]
async fn worker_registered_with_empty_tenant_counts_for_everyone() {
    let env = setup().await;
    let inst = waiting_with_pending_task(&env, "charge_card").await;
    env.srv
        .storage
        .upsert_worker_registration(&mk_registration("charge_card", None, Some("")))
        .await
        .unwrap();
    let report = env.diagnose(&inst).await;
    assert!(codes(&report).contains(&"WAITING_WORKER_PICKUP".to_string()));
}

#[tokio::test]
async fn worker_registered_for_same_tenant_counts() {
    let env = setup().await;
    let inst = waiting_with_pending_task(&env, "charge_card").await;
    env.srv
        .storage
        .upsert_worker_registration(&mk_registration("charge_card", None, Some(TENANT)))
        .await
        .unwrap();
    let report = env.diagnose(&inst).await;
    assert!(codes(&report).contains(&"WAITING_WORKER_PICKUP".to_string()));
}

#[tokio::test]
async fn version_pin_above_live_worker_blocks_pickup() {
    let env = setup().await;
    let inst = waiting_with_pending_task(&env, "charge_card").await;
    env.srv
        .storage
        .upsert_worker_registration(&mk_registration("charge_card", Some("1.0.0"), None))
        .await
        .unwrap();
    env.srv
        .storage
        .upsert_worker_version_pin(&WorkerVersionPin {
            tenant_id: TENANT.into(),
            handler_name: "charge_card".into(),
            min_version: "2.0.0".into(),
            created_at: Utc::now(),
            updated_at: Utc::now(),
        })
        .await
        .unwrap();
    let report = env.diagnose(&inst).await;
    let d = find(&report, "WORKER_BELOW_VERSION_PIN");
    assert_eq!(d["evidence"][0]["label"], "live_workers");
    assert!(
        d["evidence"][0]["summary"]
            .as_str()
            .unwrap()
            .contains("1.0.0")
    );
}

#[tokio::test]
async fn satisfied_version_pin_allows_pickup() {
    let env = setup().await;
    let inst = waiting_with_pending_task(&env, "charge_card").await;
    env.srv
        .storage
        .upsert_worker_registration(&mk_registration("charge_card", Some("2.1.0"), None))
        .await
        .unwrap();
    env.srv
        .storage
        .upsert_worker_version_pin(&WorkerVersionPin {
            tenant_id: TENANT.into(),
            handler_name: "charge_card".into(),
            min_version: "2.0.0".into(),
            created_at: Utc::now(),
            updated_at: Utc::now(),
        })
        .await
        .unwrap();
    let report = env.diagnose(&inst).await;
    let cs = codes(&report);
    assert!(cs.contains(&"WAITING_WORKER_PICKUP".to_string()), "{cs:?}");
    assert!(
        !cs.contains(&"WORKER_BELOW_VERSION_PIN".to_string()),
        "{cs:?}"
    );
}

#[tokio::test]
async fn version_pin_of_other_tenant_is_ignored() {
    let env = setup().await;
    let inst = waiting_with_pending_task(&env, "charge_card").await;
    env.srv
        .storage
        .upsert_worker_registration(&mk_registration("charge_card", Some("1.0.0"), None))
        .await
        .unwrap();
    env.srv
        .storage
        .upsert_worker_version_pin(&WorkerVersionPin {
            tenant_id: "t2".into(),
            handler_name: "charge_card".into(),
            min_version: "9.0.0".into(),
            created_at: Utc::now(),
            updated_at: Utc::now(),
        })
        .await
        .unwrap();
    let report = env.diagnose(&inst).await;
    let cs = codes(&report);
    assert!(
        !cs.contains(&"WORKER_BELOW_VERSION_PIN".to_string()),
        "{cs:?}"
    );
    assert!(cs.contains(&"WAITING_WORKER_PICKUP".to_string()), "{cs:?}");
}

#[tokio::test]
async fn registration_outside_liveness_window_is_invisible() {
    // The collector only fetches registrations seen within the last 120s.
    let env = setup().await;
    let inst = waiting_with_pending_task(&env, "charge_card").await;
    let mut reg = mk_registration("charge_card", None, None);
    reg.last_seen_at = Utc::now() - Duration::minutes(10);
    env.srv
        .storage
        .upsert_worker_registration(&reg)
        .await
        .unwrap();
    let report = env.diagnose(&inst).await;
    assert!(codes(&report).contains(&"NO_COMPATIBLE_WORKER".to_string()));
}

// ===========================================================================
// Worker tasks: claimed matrix
// ===========================================================================

#[tokio::test]
async fn freshly_claimed_task_is_not_stale() {
    let env = setup().await;
    let seq = env.simple_sequence().await;
    let inst = env.create_instance(&seq, json!({})).await;
    env.set_state(&inst, InstanceState::Waiting, None).await;
    env.srv
        .storage
        .create_worker_task(&mk_task(&inst, "blk-1", "h", WorkerTaskState::Claimed))
        .await
        .unwrap();
    let report = env.diagnose(&inst).await;
    assert!(!codes(&report).contains(&"STALE_WORKER_CLAIM".to_string()));
}

#[tokio::test]
async fn stale_heartbeat_reports_stale_worker_claim() {
    let env = setup().await;
    let seq = env.simple_sequence().await;
    let inst = env.create_instance(&seq, json!({})).await;
    env.set_state(&inst, InstanceState::Waiting, None).await;
    let mut task = mk_task(&inst, "blk-1", "h", WorkerTaskState::Claimed);
    task.claimed_at = Some(Utc::now() - Duration::minutes(15));
    task.heartbeat_at = Some(Utc::now() - Duration::minutes(15));
    env.srv.storage.create_worker_task(&task).await.unwrap();
    let report = env.diagnose(&inst).await;
    let d = find(&report, "STALE_WORKER_CLAIM");
    assert_eq!(d["severity"], "error");
    assert_eq!(d["confidence"], "high");
    assert_eq!(d["remediation"][0]["side_effect_risk"], true);
}

#[tokio::test]
async fn missing_heartbeat_falls_back_to_claimed_at() {
    let env = setup().await;
    let seq = env.simple_sequence().await;
    let inst = env.create_instance(&seq, json!({})).await;
    env.set_state(&inst, InstanceState::Waiting, None).await;
    let mut task = mk_task(&inst, "blk-1", "h", WorkerTaskState::Claimed);
    task.heartbeat_at = None;
    task.claimed_at = Some(Utc::now() - Duration::minutes(20));
    env.srv.storage.create_worker_task(&task).await.unwrap();
    let report = env.diagnose(&inst).await;
    assert!(codes(&report).contains(&"STALE_WORKER_CLAIM".to_string()));
}

#[tokio::test]
async fn completed_worker_task_produces_no_worker_diagnosis() {
    let env = setup().await;
    let seq = env.simple_sequence().await;
    let inst = env.create_instance(&seq, json!({})).await;
    env.set_state(&inst, InstanceState::Waiting, None).await;
    let mut task = mk_task(&inst, "blk-1", "h", WorkerTaskState::Completed);
    task.heartbeat_at = Some(Utc::now() - Duration::minutes(30));
    env.srv.storage.create_worker_task(&task).await.unwrap();
    let report = env.diagnose(&inst).await;
    let cs = codes(&report);
    for code in [
        "STALE_WORKER_CLAIM",
        "NO_COMPATIBLE_WORKER",
        "WAITING_WORKER_PICKUP",
        "WORKER_NOT_CLAIMING",
    ] {
        assert!(!cs.contains(&code.to_string()), "{code}: {cs:?}");
    }
}

// ===========================================================================
// Circuit breakers
// ===========================================================================

#[tokio::test]
async fn open_breaker_for_involved_handler_is_direct_evidence() {
    let env = setup().await;
    let inst = waiting_with_pending_task(&env, "flaky_api").await;
    env.srv
        .storage
        .upsert_worker_registration(&mk_registration("flaky_api", None, None))
        .await
        .unwrap();
    env.srv
        .storage
        .upsert_circuit_breaker(&mk_breaker(TENANT, "flaky_api", BreakerState::Open))
        .await
        .unwrap();
    let report = env.diagnose(&inst).await;
    let d = find(&report, "OPEN_CIRCUIT_BREAKER");
    assert_eq!(d["category"], "direct_evidence");
    assert_eq!(d["confidence"], "high");
    assert_eq!(d["health"], "degraded");
    let target = d["evidence"]
        .as_array()
        .unwrap()
        .iter()
        .find(|item| item["label"] == "circuit_breaker_target")
        .unwrap();
    assert_eq!(
        target["data"],
        serde_json::json!({"instance_id": inst, "tenant_id": TENANT, "handler": "flaky_api"})
    );
}

#[tokio::test]
async fn open_breaker_for_uninvolved_handler_ranks_last_as_health_warning() {
    let env = setup().await;
    let seq = env.simple_sequence().await;
    let inst = env.create_instance(&seq, json!({})).await;
    env.set_state(&inst, InstanceState::Waiting, None).await;
    env.srv
        .storage
        .upsert_circuit_breaker(&mk_breaker(TENANT, "unrelated", BreakerState::Open))
        .await
        .unwrap();
    let report = env.diagnose(&inst).await;
    let d = find(&report, "OPEN_CIRCUIT_BREAKER");
    assert_eq!(d["category"], "health_warning");
    assert_eq!(d["confidence"], "low");
    // Ranked below the probable cause (WAITING_EXTERNAL_EVENT).
    let cs = codes(&report);
    let ext = cs
        .iter()
        .position(|c| c == "WAITING_EXTERNAL_EVENT")
        .unwrap();
    let brk = cs.iter().position(|c| c == "OPEN_CIRCUIT_BREAKER").unwrap();
    assert!(ext < brk, "{cs:?}");
}

#[tokio::test]
async fn non_open_breaker_rows_are_not_reported() {
    let env = setup().await;
    let seq = env.simple_sequence().await;
    let inst = env.create_instance(&seq, json!({})).await;
    env.set_state(&inst, InstanceState::Waiting, None).await;
    env.srv
        .storage
        .upsert_circuit_breaker(&mk_breaker(TENANT, "h", BreakerState::Closed))
        .await
        .unwrap();
    let report = env.diagnose(&inst).await;
    assert!(!codes(&report).contains(&"OPEN_CIRCUIT_BREAKER".to_string()));
}

#[tokio::test]
async fn open_breaker_of_other_tenant_is_not_reported() {
    let env = setup().await;
    let seq = env.simple_sequence().await;
    let inst = env.create_instance(&seq, json!({})).await;
    env.set_state(&inst, InstanceState::Waiting, None).await;
    env.srv
        .storage
        .upsert_circuit_breaker(&mk_breaker("someone-else", "h", BreakerState::Open))
        .await
        .unwrap();
    let report = env.diagnose(&inst).await;
    assert!(!codes(&report).contains(&"OPEN_CIRCUIT_BREAKER".to_string()));
}

#[tokio::test]
async fn open_breaker_reports_cooldown_countdown() {
    let env = setup().await;
    let seq = env.simple_sequence().await;
    let inst = env.create_instance(&seq, json!({})).await;
    env.set_state(&inst, InstanceState::Waiting, None).await;
    env.srv
        .storage
        .upsert_circuit_breaker(&mk_breaker(TENANT, "h", BreakerState::Open))
        .await
        .unwrap();
    let report = env.diagnose(&inst).await;
    let d = find(&report, "OPEN_CIRCUIT_BREAKER");
    let summary = d["summary"].as_str().unwrap();
    assert!(summary.contains("retries resume in"), "{summary}");
    assert_eq!(d["evidence"][0]["label"], "opened_at");
}

// ===========================================================================
// Children (parent_instance_id set at creation, read via get_child_instances)
// ===========================================================================

#[tokio::test]
async fn waiting_parent_with_live_child_reports_waiting_child() {
    let env = setup().await;
    let seq = env.simple_sequence().await;
    let parent = env.create_instance(&seq, json!({})).await;
    env.set_state(&parent, InstanceState::Waiting, None).await;
    let mut child = mk_instance(&seq, InstanceState::Running);
    child.parent_instance_id = Some(iid(&parent));
    env.seed_instance(&child).await;
    let report = env.diagnose(&parent).await;
    let d = find(&report, "WAITING_CHILD");
    assert_eq!(d["category"], "direct_evidence");
    assert_eq!(d["health"], "expected");
}

#[tokio::test]
async fn waiting_child_evidence_names_the_child_id() {
    let env = setup().await;
    let seq = env.simple_sequence().await;
    let parent = env.create_instance(&seq, json!({})).await;
    env.set_state(&parent, InstanceState::Waiting, None).await;
    let mut child = mk_instance(&seq, InstanceState::Scheduled);
    child.parent_instance_id = Some(iid(&parent));
    let child_id = child.id.to_string();
    env.seed_instance(&child).await;
    let report = env.diagnose(&parent).await;
    let d = find(&report, "WAITING_CHILD");
    assert!(
        d["evidence"][0]["summary"]
            .as_str()
            .unwrap()
            .contains(&child_id),
        "{d}"
    );
}

#[tokio::test]
async fn all_children_terminal_but_parent_waiting_is_inconsistent() {
    let env = setup().await;
    let seq = env.simple_sequence().await;
    let parent = env.create_instance(&seq, json!({})).await;
    env.set_state(&parent, InstanceState::Waiting, None).await;
    let mut child = mk_instance(&seq, InstanceState::Completed);
    child.parent_instance_id = Some(iid(&parent));
    env.seed_instance(&child).await;
    let report = env.diagnose(&parent).await;
    let d = find(&report, "CHILDREN_DONE_PARENT_WAITING");
    assert_eq!(d["health"], "inconsistent");
    assert_eq!(d["category"], "probable_cause");
}

#[tokio::test]
async fn non_waiting_parent_gets_no_child_diagnosis() {
    let env = setup().await;
    let seq = env.simple_sequence().await;
    let parent = env.create_instance(&seq, json!({})).await;
    env.set_state(&parent, InstanceState::Running, None).await;
    let mut child = mk_instance(&seq, InstanceState::Running);
    child.parent_instance_id = Some(iid(&parent));
    env.seed_instance(&child).await;
    let report = env.diagnose(&parent).await;
    let cs = codes(&report);
    assert!(!cs.contains(&"WAITING_CHILD".to_string()), "{cs:?}");
    assert!(
        !cs.contains(&"CHILDREN_DONE_PARENT_WAITING".to_string()),
        "{cs:?}"
    );
}

// ===========================================================================
// Signals
// ===========================================================================

#[tokio::test]
async fn old_undelivered_signal_reports_signals_not_consumed() {
    let env = setup().await;
    let seq = env.simple_sequence().await;
    let inst = env.create_instance(&seq, json!({})).await;
    env.set_state(&inst, InstanceState::Waiting, None).await;
    env.srv
        .storage
        .enqueue_signal(&mk_signal(&inst, 300, false))
        .await
        .unwrap();
    let report = env.diagnose(&inst).await;
    let d = find(&report, "SIGNALS_NOT_CONSUMED");
    assert_eq!(d["category"], "probable_cause");
    assert_eq!(d["health"], "degraded");
}

#[tokio::test]
async fn fresh_signal_is_not_flagged() {
    let env = setup().await;
    let seq = env.simple_sequence().await;
    let inst = env.create_instance(&seq, json!({})).await;
    env.set_state(&inst, InstanceState::Waiting, None).await;
    env.srv
        .storage
        .enqueue_signal(&mk_signal(&inst, 5, false))
        .await
        .unwrap();
    let report = env.diagnose(&inst).await;
    assert!(!codes(&report).contains(&"SIGNALS_NOT_CONSUMED".to_string()));
}

#[tokio::test]
async fn delivered_signal_is_not_pending_and_not_flagged() {
    let env = setup().await;
    let seq = env.simple_sequence().await;
    let inst = env.create_instance(&seq, json!({})).await;
    env.set_state(&inst, InstanceState::Waiting, None).await;
    env.srv
        .storage
        .enqueue_signal(&mk_signal(&inst, 600, true))
        .await
        .unwrap();
    let report = env.diagnose(&inst).await;
    assert!(!codes(&report).contains(&"SIGNALS_NOT_CONSUMED".to_string()));
}

// ===========================================================================
// State inconsistencies
// ===========================================================================

#[tokio::test]
async fn long_silent_running_instance_reports_stale_running_state() {
    let env = setup().await;
    let seq = env.simple_sequence().await;
    let mut inst = mk_instance(&seq, InstanceState::Running);
    inst.updated_at = Utc::now() - Duration::minutes(30);
    let id = inst.id.to_string();
    env.seed_instance(&inst).await;
    let report = env.diagnose(&id).await;
    let d = find(&report, "STALE_RUNNING_STATE");
    assert_eq!(d["health"], "inconsistent");
    assert_eq!(d["evidence"][0]["label"], "updated_at");
}

#[tokio::test]
async fn recently_updated_running_instance_has_no_blockers() {
    let env = setup().await;
    let seq = env.simple_sequence().await;
    let inst = env.create_instance(&seq, json!({})).await;
    env.set_state(&inst, InstanceState::Running, None).await;
    let report = env.diagnose(&inst).await;
    assert_eq!(codes(&report), vec!["NO_BLOCKER_FOUND".to_string()]);
}

#[tokio::test]
async fn long_overdue_scheduled_instance_reports_scheduler_lag() {
    let env = setup().await;
    let seq = env.simple_sequence().await;
    let mut inst = mk_instance(&seq, InstanceState::Scheduled);
    inst.next_fire_at = Some(Utc::now() - Duration::minutes(10));
    let id = inst.id.to_string();
    env.seed_instance(&inst).await;
    let report = env.diagnose(&id).await;
    let d = find(&report, "SCHEDULER_LAG");
    assert_eq!(d["severity"], "error");
    assert_eq!(d["evidence"][0]["label"], "next_fire_at");
}

#[tokio::test]
async fn recently_due_scheduled_instance_is_not_lagging() {
    let env = setup().await;
    let seq = env.simple_sequence().await;
    let mut inst = mk_instance(&seq, InstanceState::Scheduled);
    inst.next_fire_at = Some(Utc::now() - Duration::seconds(10));
    let id = inst.id.to_string();
    env.seed_instance(&inst).await;
    let report = env.diagnose(&id).await;
    assert!(!codes(&report).contains(&"SCHEDULER_LAG".to_string()));
}

// ===========================================================================
// Approvals and event waits
// ===========================================================================

#[tokio::test]
async fn waiting_gated_instance_reports_pending_approval() {
    let env = setup().await;
    let seq = env.gated_sequence().await;
    let inst = env.create_instance(&seq, json!({})).await;
    env.set_state(&inst, InstanceState::Waiting, None).await;
    let report = env.diagnose(&inst).await;
    let d = find(&report, "PENDING_APPROVAL");
    assert_eq!(d["category"], "direct_evidence");
    assert_eq!(d["confidence"], "certain");
    let cmd = d["remediation"][0]["command"].as_str().unwrap();
    assert!(cmd.contains("human_input:gate"), "{cmd}");
    assert!(cmd.contains(&inst), "{cmd}");
}

#[tokio::test]
async fn approval_suppresses_external_event_guess() {
    let env = setup().await;
    let seq = env.gated_sequence().await;
    let inst = env.create_instance(&seq, json!({})).await;
    env.set_state(&inst, InstanceState::Waiting, None).await;
    let report = env.diagnose(&inst).await;
    assert!(!codes(&report).contains(&"WAITING_EXTERNAL_EVENT".to_string()));
}

#[tokio::test]
async fn non_waiting_gated_instance_reports_no_approval() {
    let env = setup().await;
    let seq = env.gated_sequence().await;
    let inst = env.create_instance(&seq, json!({})).await;
    env.set_state(&inst, InstanceState::Running, None).await;
    let report = env.diagnose(&inst).await;
    assert!(!codes(&report).contains(&"PENDING_APPROVAL".to_string()));
}

#[tokio::test]
async fn partially_matched_wait_reports_missing_names_and_stored_event() {
    let env = setup().await;
    let seq = env.gated_sequence().await;
    let inst = env.create_instance(&seq, json!({})).await;
    env.set_state(&inst, InstanceState::Waiting, None).await;
    let mut wait = mk_event_wait(&inst, "gate", WaitStatus::Waiting);
    wait.matched_names.clear();
    wait.matched_event_ids.clear();
    env.srv.storage.upsert_event_wait(&wait).await.unwrap();
    let response = env
        .client
        .post(format!("{}/events", env.base))
        .header("X-Tenant-Id", TENANT)
        .header("X-Orch8-Include-Stored-Event-Id", "true")
        .json(&json!({
            "tenant_id": TENANT,
            "event_name": "inventory_reserved",
            "producer_event_id": "reserve-42",
            "correlation_key": "order-42",
            "payload": {"warehouse": "private-fixture"},
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::CREATED);
    let outcome: Value = response.json().await.unwrap();
    assert_eq!(outcome["matched_wait"], json!(wait.id));
    assert_eq!(outcome["satisfied"], false);
    let event = outcome["stored_event_id"].as_str().unwrap();
    let report = env.diagnose(&inst).await;
    let d = find(&report, "WAITING_EVENT");
    let summary = d["summary"].as_str().unwrap();
    assert!(summary.contains("payment_received"), "{summary}");
    assert!(
        summary.contains("already matched: inventory_reserved"),
        "{summary}"
    );
    assert!(summary.contains("order-42"), "{summary}");
    assert_eq!(d["evidence"][0]["label"], "correlation_key");
    assert_eq!(d["evidence"][1]["label"], "matched_inbox_events");
    assert_eq!(
        d["evidence"][1]["data"],
        json!({"instance_id": inst, "tenant_id": TENANT, "event_ids": [event], "total": 1})
    );
    assert!(!report.to_string().contains("private-fixture"));

    let url = format!("{}/events/{event}", env.base);
    let stored: Value = env
        .client
        .get(&url)
        .header("X-Tenant-Id", TENANT)
        .send()
        .await
        .unwrap()
        .error_for_status()
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(stored["id"], event);
    assert_eq!(stored["event_name"], "inventory_reserved");
    assert_eq!(stored["status"], "consumed");
    assert_eq!(stored["consumed_by"], inst);
    assert_eq!(
        env.client
            .get(&url)
            .header("X-Tenant-Id", "other")
            .send()
            .await
            .unwrap()
            .status(),
        StatusCode::NOT_FOUND
    );
}

#[tokio::test]
async fn event_wait_takes_precedence_over_pending_approval() {
    let env = setup().await;
    let seq = env.gated_sequence().await;
    let inst = env.create_instance(&seq, json!({})).await;
    env.set_state(&inst, InstanceState::Waiting, None).await;
    env.srv
        .storage
        .upsert_event_wait(&mk_event_wait(&inst, "gate", WaitStatus::Waiting))
        .await
        .unwrap();
    let report = env.diagnose(&inst).await;
    let cs = codes(&report);
    assert!(cs.contains(&"WAITING_EVENT".to_string()), "{cs:?}");
    assert!(!cs.contains(&"PENDING_APPROVAL".to_string()), "{cs:?}");
}

#[tokio::test]
async fn missing_registration_for_a_declared_event_wait_is_incomplete() {
    let env = setup().await;
    let seq = env
        .create_sequence(json!([{
            "type": "step",
            "id": "await_order",
            "handler": "wait_for_event",
            "params": {"events": ["order_received"], "correlation_key": "order-42", "join": "all"},
            "wait_for_input": {"prompt": "Waiting for order"},
        }]))
        .await;
    let inst = env.create_instance(&seq, json!({})).await;
    env.set_state(&inst, InstanceState::Waiting, None).await;
    let report = env.diagnose(&inst).await;
    assert!(
        find(&report, "EVIDENCE_INCOMPLETE")["summary"]
            .as_str()
            .unwrap()
            .contains("event_waits")
    );
    assert!(!codes(&report).iter().any(|code| code == "PENDING_APPROVAL"));
}

#[tokio::test]
async fn failed_wait_lookup_withholds_the_approval_until_a_verified_read() {
    use orch8_api::test_harness::{TestServerOptions, spawn_test_server_with};
    use orch8_storage::testing::faulty::FaultStorage;

    let env = setup().await;
    let seq = env.gated_sequence().await;
    let inst = env.create_instance(&seq, json!({})).await;
    env.set_state(&inst, InstanceState::Waiting, None).await;
    let (storage, faults) = FaultStorage::wrap(env.srv.storage.clone());
    faults.on("get_event_wait").times(1).fail_transient();
    let server = spawn_test_server_with(storage, TestServerOptions::default()).await;
    let read = || async {
        env.client
            .get(format!("{}/instances/{inst}/diagnosis", server.v1_url()))
            .header("X-Tenant-Id", TENANT)
            .send()
            .await
            .unwrap()
            .error_for_status()
            .unwrap()
            .json::<Value>()
            .await
            .unwrap()
    };
    let incomplete = read().await;
    assert_eq!(faults.injected("get_event_wait"), 1);
    assert!(
        find(&incomplete, "EVIDENCE_INCOMPLETE")["summary"]
            .as_str()
            .unwrap()
            .contains("event_waits")
    );
    assert!(
        !codes(&incomplete)
            .iter()
            .any(|code| code == "PENDING_APPROVAL")
    );
    let verified = read().await;
    assert!(
        find(&verified, "PENDING_APPROVAL")["summary"]
            .as_str()
            .unwrap()
            .contains("gate")
    );
    assert!(
        !codes(&verified)
            .iter()
            .any(|code| code == "EVIDENCE_INCOMPLETE")
    );
}

#[tokio::test]
async fn satisfied_event_wait_is_not_reported_as_waiting_event() {
    let env = setup().await;
    let seq = env.gated_sequence().await;
    let inst = env.create_instance(&seq, json!({})).await;
    env.set_state(&inst, InstanceState::Waiting, None).await;
    env.srv
        .storage
        .upsert_event_wait(&mk_event_wait(&inst, "gate", WaitStatus::Satisfied))
        .await
        .unwrap();
    let report = env.diagnose(&inst).await;
    assert!(!codes(&report).contains(&"WAITING_EVENT".to_string()));
}

#[tokio::test]
async fn waiting_event_remediation_points_at_events_endpoint() {
    let env = setup().await;
    let seq = env.gated_sequence().await;
    let inst = env.create_instance(&seq, json!({})).await;
    env.set_state(&inst, InstanceState::Waiting, None).await;
    env.srv
        .storage
        .upsert_event_wait(&mk_event_wait(&inst, "gate", WaitStatus::Waiting))
        .await
        .unwrap();
    let report = env.diagnose(&inst).await;
    let d = find(&report, "WAITING_EVENT");
    let rem = d["remediation"][0]["summary"].as_str().unwrap();
    assert!(rem.contains("POST /events"), "{rem}");
    assert!(rem.contains("order-42"), "{rem}");
}

// ===========================================================================
// External-event guess, missing sequence, ranking
// ===========================================================================

#[tokio::test]
async fn unexplained_waiting_instance_suggests_external_event() {
    let env = setup().await;
    let seq = env.simple_sequence().await;
    let inst = env.create_instance(&seq, json!({})).await;
    env.set_state(&inst, InstanceState::Waiting, None).await;
    let report = env.diagnose(&inst).await;
    let d = find(&report, "WAITING_EXTERNAL_EVENT");
    assert_eq!(d["category"], "probable_cause");
    assert_eq!(d["confidence"], "medium");
    let cmd = d["remediation"][0]["command"].as_str().unwrap();
    assert!(cmd.contains(&inst), "{cmd}");
}

#[tokio::test]
async fn open_worker_task_suppresses_external_event_guess() {
    let env = setup().await;
    let inst = waiting_with_pending_task(&env, "h").await;
    let report = env.diagnose(&inst).await;
    assert!(!codes(&report).contains(&"WAITING_EXTERNAL_EVENT".to_string()));
}

#[tokio::test]
async fn instance_pointing_at_deleted_sequence_is_critical() {
    let env = setup().await;
    // Reference a sequence id that was never created.
    let ghost_seq = Uuid::now_v7().to_string();
    let inst = mk_instance(&ghost_seq, InstanceState::Scheduled);
    let id = inst.id.to_string();
    env.seed_instance(&inst).await;
    let report = env.diagnose(&id).await;
    assert_eq!(report["diagnoses"][0]["code"], "SEQUENCE_MISSING");
    let d = find(&report, "SEQUENCE_MISSING");
    assert_eq!(d["severity"], "critical");
    assert_eq!(d["health"], "inconsistent");
    assert!(d["summary"].as_str().unwrap().contains(&ghost_seq));
}

#[tokio::test]
async fn missing_sequence_also_reports_incomplete_approval_evidence() {
    // With no sequence, approvals cannot be collected — the doctor says so
    // explicitly instead of silently passing.
    let env = setup().await;
    let inst = mk_instance(&Uuid::now_v7().to_string(), InstanceState::Scheduled);
    let id = inst.id.to_string();
    env.seed_instance(&inst).await;
    let report = env.diagnose(&id).await;
    let d = find(&report, "EVIDENCE_INCOMPLETE");
    assert!(d["summary"].as_str().unwrap().contains("approvals"), "{d}");
}

#[tokio::test]
async fn scheduled_instance_without_timer_reports_no_blocker_found() {
    let env = setup().await;
    let seq = env.simple_sequence().await;
    let inst = mk_instance(&seq, InstanceState::Scheduled);
    let id = inst.id.to_string();
    env.seed_instance(&inst).await;
    let report = env.diagnose(&id).await;
    assert_eq!(codes(&report), vec!["NO_BLOCKER_FOUND".to_string()]);
    let d = find(&report, "NO_BLOCKER_FOUND");
    assert_eq!(d["category"], "health_warning");
    assert_eq!(d["confidence"], "low");
}

#[tokio::test]
async fn direct_evidence_outranks_probable_cause_end_to_end() {
    // Stale claim (direct/high) + stale signal (probable/medium): the claim
    // must be ranked first.
    let env = setup().await;
    let seq = env.simple_sequence().await;
    let inst = env.create_instance(&seq, json!({})).await;
    env.set_state(&inst, InstanceState::Waiting, None).await;
    let mut task = mk_task(&inst, "blk-1", "h", WorkerTaskState::Claimed);
    task.heartbeat_at = Some(Utc::now() - Duration::minutes(15));
    env.srv.storage.create_worker_task(&task).await.unwrap();
    env.srv
        .storage
        .enqueue_signal(&mk_signal(&inst, 300, false))
        .await
        .unwrap();
    let report = env.diagnose(&inst).await;
    assert_eq!(report["diagnoses"][0]["code"], "STALE_WORKER_CLAIM");
    let cs = codes(&report);
    assert!(cs.contains(&"SIGNALS_NOT_CONSUMED".to_string()), "{cs:?}");
}

#[tokio::test]
async fn report_state_and_generated_at_reflect_diagnosis_time() {
    let env = setup().await;
    let seq = env.simple_sequence().await;
    let inst = env.create_instance(&seq, json!({})).await;
    env.set_state(&inst, InstanceState::Waiting, None).await;
    let before = Utc::now();
    let report = env.diagnose(&inst).await;
    assert_eq!(report["state"], "waiting");
    let generated: DateTime<Utc> = report["generated_at"].as_str().unwrap().parse().unwrap();
    assert!(generated >= before - Duration::seconds(5));
    assert!(generated <= Utc::now() + Duration::seconds(5));
}
