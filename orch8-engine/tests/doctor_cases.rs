//! Exhaustive case coverage for the Stuck Instance Doctor rules
//! (`orch8_engine::doctor`) and the ranking model
//! (`orch8_types::diagnosis`).
//!
//! Everything here is pure: a hand-built [`InstanceDiagnosticContext`] plus a
//! fixed `now`, so every boundary (heartbeat staleness, pending-task alarm,
//! scheduler lag, signal age) is tested exactly at, just below, and just above
//! its threshold.

use chrono::{DateTime, Duration, TimeZone, Utc};

use orch8_engine::doctor::{
    InstanceDiagnosticContext, PENDING_TASK_ALARM_SECS, SCHEDULER_LAG_SECS, STALE_HEARTBEAT_SECS,
    STALE_RUNNING_SECS, diagnose,
};
use orch8_types::circuit_breaker::{BreakerState, CircuitBreakerState};
use orch8_types::context::ExecutionContext;
use orch8_types::diagnosis::{
    Diagnosis, DiagnosisCategory, DiagnosisHealth, InstanceDiagnosisReport, rank_diagnoses,
};
use orch8_types::event_correlation::{EventWait, JoinMode, WaitStatus};
use orch8_types::finding::{Confidence, Finding, FindingSeverity};
use orch8_types::ids::{BlockId, InstanceId, Namespace, SequenceId, TenantId};
use orch8_types::instance::{InstanceState, Priority, TaskInstance};
use orch8_types::signal::{Signal, SignalType};
use orch8_types::worker::{WorkerRegistration, WorkerTask, WorkerTaskState, WorkerVersionPin};
use serde_json::json;

// ---------------------------------------------------------------------------
// Fixtures
// ---------------------------------------------------------------------------

fn t0() -> DateTime<Utc> {
    Utc.with_ymd_and_hms(2026, 7, 11, 12, 0, 0).unwrap()
}

fn instance(state: InstanceState) -> TaskInstance {
    TaskInstance {
        id: InstanceId::new(),
        sequence_id: SequenceId::new(),
        tenant_id: TenantId::unchecked("t1"),
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
        created_at: t0() - Duration::minutes(5),
        updated_at: t0() - Duration::seconds(5),
    }
}

/// A context where every evidence section was collected (and empty).
fn full_ctx(inst: TaskInstance) -> InstanceDiagnosticContext {
    InstanceDiagnosticContext {
        instance: inst,
        sequence_exists: Some(true),
        pending_signals: Some(vec![]),
        worker_tasks: Some(vec![]),
        worker_registrations: Some(vec![]),
        version_pins: Some(vec![]),
        open_breakers: Some(vec![]),
        children: Some(vec![]),
        pending_approval_blocks: Some(vec![]),
        event_waits: Some(vec![]),
    }
}

fn task_named(state: WorkerTaskState, handler: &str, block: &str, age_secs: i64) -> WorkerTask {
    WorkerTask {
        id: uuid::Uuid::now_v7(),
        instance_id: InstanceId::new(),
        block_id: BlockId::new(block),
        handler_name: handler.to_string(),
        queue_name: None,
        params: json!({}),
        context: json!({}),
        attempt: 1,
        timeout_ms: None,
        state,
        worker_id: Some("w-1".into()),
        claimed_at: Some(t0() - Duration::seconds(age_secs)),
        heartbeat_at: Some(t0() - Duration::seconds(age_secs)),
        completed_at: None,
        output: None,
        error_message: None,
        error_retryable: None,
        created_at: t0() - Duration::seconds(age_secs),
    }
}

fn task(state: WorkerTaskState, handler: &str, age_secs: i64) -> WorkerTask {
    task_named(state, handler, "blk", age_secs)
}

fn registration(handler: &str, version: Option<&str>) -> WorkerRegistration {
    WorkerRegistration {
        worker_id: "w-1".into(),
        handler_name: handler.to_string(),
        queue_name: None,
        version: version.map(ToString::to_string),
        tenant_id: None,
        last_seen_at: t0(),
    }
}

fn registration_for_tenant(
    handler: &str,
    version: Option<&str>,
    tenant: Option<&str>,
) -> WorkerRegistration {
    WorkerRegistration {
        tenant_id: tenant.map(ToString::to_string),
        ..registration(handler, version)
    }
}

fn pin(tenant: &str, handler: &str, min_version: &str) -> WorkerVersionPin {
    WorkerVersionPin {
        tenant_id: tenant.into(),
        handler_name: handler.into(),
        min_version: min_version.into(),
        created_at: t0(),
        updated_at: t0(),
    }
}

fn breaker(tenant: &str, handler: &str, state: BreakerState) -> CircuitBreakerState {
    CircuitBreakerState {
        tenant_id: TenantId::unchecked(tenant),
        handler: handler.into(),
        state,
        failure_count: 7,
        failure_threshold: 5,
        cooldown_secs: 300,
        opened_at: Some(t0() - Duration::seconds(60)),
    }
}

fn signal(age_secs: i64) -> Signal {
    Signal {
        id: uuid::Uuid::now_v7(),
        instance_id: InstanceId::new(),
        signal_type: SignalType::Resume,
        payload: json!({}),
        delivered: false,
        created_at: t0() - Duration::seconds(age_secs),
        delivered_at: None,
    }
}

fn wait(block: &str, status: WaitStatus, names: &[&str], matched: &[&str]) -> EventWait {
    EventWait {
        id: uuid::Uuid::now_v7(),
        tenant_id: "t1".into(),
        instance_id: uuid::Uuid::now_v7(),
        block_id: block.into(),
        event_names: names.iter().map(ToString::to_string).collect(),
        correlation_key: "corr-1".into(),
        join_mode: JoinMode::All,
        status,
        matched_names: matched.iter().map(ToString::to_string).collect(),
        matched_event_ids: vec![],
        created_at: t0(),
    }
}

fn codes(report: &InstanceDiagnosisReport) -> Vec<&str> {
    report
        .diagnoses
        .iter()
        .map(|d| d.finding.code.as_str())
        .collect()
}

fn find<'r>(report: &'r InstanceDiagnosisReport, code: &str) -> &'r Diagnosis {
    report
        .diagnoses
        .iter()
        .find(|d| d.finding.code == code)
        .unwrap_or_else(|| panic!("expected diagnosis {code}, got {:?}", codes(report)))
}

fn diag_of(code: &str, category: DiagnosisCategory, confidence: Confidence) -> Diagnosis {
    Diagnosis {
        category,
        health: DiagnosisHealth::Expected,
        finding: Finding::new(code, FindingSeverity::Info, "s", confidence, t0()),
    }
}

// ===========================================================================
// Terminal states
// ===========================================================================

#[test]
fn completed_reports_only_terminal_state() {
    let report = diagnose(&full_ctx(instance(InstanceState::Completed)), t0());
    assert_eq!(codes(&report), vec!["TERMINAL_STATE"]);
    assert_eq!(report.state, "completed");
}

#[test]
fn failed_reports_only_terminal_state() {
    let report = diagnose(&full_ctx(instance(InstanceState::Failed)), t0());
    assert_eq!(codes(&report), vec!["TERMINAL_STATE"]);
    assert_eq!(report.state, "failed");
}

#[test]
fn cancelled_reports_only_terminal_state() {
    let report = diagnose(&full_ctx(instance(InstanceState::Cancelled)), t0());
    assert_eq!(codes(&report), vec!["TERMINAL_STATE"]);
    assert_eq!(report.state, "cancelled");
}

#[test]
fn failed_offers_retry_remediation_with_side_effect_risk() {
    let inst = instance(InstanceState::Failed);
    let id = inst.id;
    let report = diagnose(&full_ctx(inst), t0());
    let d = &report.diagnoses[0];
    assert_eq!(d.finding.remediation.len(), 1);
    let r = &d.finding.remediation[0];
    assert!(r.side_effect_risk);
    assert!(r.summary.contains("retry"), "{}", r.summary);
    let cmd = r.command.as_deref().unwrap();
    assert!(cmd.contains("instance retry"), "{cmd}");
    assert!(cmd.contains(&id.to_string()), "{cmd}");
}

#[test]
fn completed_terminal_has_no_remediation() {
    let report = diagnose(&full_ctx(instance(InstanceState::Completed)), t0());
    assert!(report.diagnoses[0].finding.remediation.is_empty());
}

#[test]
fn cancelled_terminal_has_no_remediation() {
    let report = diagnose(&full_ctx(instance(InstanceState::Cancelled)), t0());
    assert!(report.diagnoses[0].finding.remediation.is_empty());
}

#[test]
fn terminal_diagnosis_is_direct_evidence_expected_certain_info() {
    let report = diagnose(&full_ctx(instance(InstanceState::Completed)), t0());
    let d = &report.diagnoses[0];
    assert_eq!(d.category, DiagnosisCategory::DirectEvidence);
    assert_eq!(d.health, DiagnosisHealth::Expected);
    assert_eq!(d.finding.confidence, Confidence::Certain);
    assert_eq!(d.finding.severity, FindingSeverity::Info);
}

#[test]
fn terminal_short_circuits_all_other_rules() {
    // Even with a missing sequence, stale tasks, breakers, stale signals,
    // and children present, a terminal instance reports only TERMINAL_STATE.
    let mut ctx = full_ctx(instance(InstanceState::Failed));
    ctx.sequence_exists = Some(false);
    ctx.worker_tasks = Some(vec![task(WorkerTaskState::Claimed, "h", 900)]);
    ctx.open_breakers = Some(vec![breaker("t1", "h", BreakerState::Open)]);
    ctx.pending_signals = Some(vec![signal(600)]);
    ctx.children = Some(vec![instance(InstanceState::Running)]);
    let report = diagnose(&ctx, t0());
    assert_eq!(codes(&report), vec!["TERMINAL_STATE"]);
}

#[test]
fn terminal_skips_evidence_incomplete_note() {
    // Nothing collected at all — but terminal instances never report
    // EVIDENCE_INCOMPLETE.
    let ctx = InstanceDiagnosticContext::new(instance(InstanceState::Completed));
    let report = diagnose(&ctx, t0());
    assert_eq!(codes(&report), vec!["TERMINAL_STATE"]);
}

#[test]
fn terminal_summary_names_the_state() {
    let report = diagnose(&full_ctx(instance(InstanceState::Cancelled)), t0());
    assert!(
        report.diagnoses[0].finding.summary.contains("cancelled"),
        "{}",
        report.diagnoses[0].finding.summary
    );
}

#[test]
fn report_carries_instance_id_state_and_generated_at() {
    let inst = instance(InstanceState::Waiting);
    let id = inst.id.into_uuid();
    let report = diagnose(&full_ctx(inst), t0());
    assert_eq!(report.instance_id, id);
    assert_eq!(report.state, "waiting");
    assert_eq!(report.generated_at, t0());
}

// ===========================================================================
// WAITING_UNTIL boundaries
// ===========================================================================

#[test]
fn timer_firing_exactly_now_is_not_waiting_until() {
    let mut inst = instance(InstanceState::Scheduled);
    inst.next_fire_at = Some(t0()); // fire_at > now is required
    let report = diagnose(&full_ctx(inst), t0());
    assert!(!codes(&report).contains(&"WAITING_UNTIL"), "{:?}", codes(&report));
}

#[test]
fn timer_one_second_in_future_is_waiting_until() {
    let mut inst = instance(InstanceState::Scheduled);
    inst.next_fire_at = Some(t0() + Duration::seconds(1));
    let report = diagnose(&full_ctx(inst), t0());
    let d = find(&report, "WAITING_UNTIL");
    assert_eq!(d.finding.confidence, Confidence::Certain);
    assert!(d.finding.summary.contains("1s"), "{}", d.finding.summary);
}

#[test]
fn timer_far_in_future_humanizes_days() {
    let mut inst = instance(InstanceState::Scheduled);
    inst.next_fire_at = Some(t0() + Duration::days(3) + Duration::hours(4));
    let report = diagnose(&full_ctx(inst), t0());
    let d = find(&report, "WAITING_UNTIL");
    assert!(d.finding.summary.contains("3d 4h"), "{}", d.finding.summary);
}

#[test]
fn paused_state_suppresses_waiting_until() {
    let mut inst = instance(InstanceState::Paused);
    inst.next_fire_at = Some(t0() + Duration::hours(1));
    let report = diagnose(&full_ctx(inst), t0());
    assert!(!codes(&report).contains(&"WAITING_UNTIL"));
    assert_eq!(report.diagnoses[0].finding.code, "PAUSED");
}

#[test]
fn budget_paused_with_future_timer_also_suppresses_waiting_until() {
    let mut inst = instance(InstanceState::Paused);
    inst.metadata = json!({"paused_reason": "budget_exceeded"});
    inst.next_fire_at = Some(t0() + Duration::hours(1));
    let report = diagnose(&full_ctx(inst), t0());
    assert!(!codes(&report).contains(&"WAITING_UNTIL"));
    assert_eq!(report.diagnoses[0].finding.code, "BUDGET_PAUSED");
}

#[test]
fn waiting_until_evidence_is_next_fire_at_rfc3339() {
    let fire_at = t0() + Duration::hours(2);
    let mut inst = instance(InstanceState::Waiting);
    inst.next_fire_at = Some(fire_at);
    let report = diagnose(&full_ctx(inst), t0());
    let d = find(&report, "WAITING_UNTIL");
    assert_eq!(d.finding.evidence[0].label, "next_fire_at");
    assert_eq!(d.finding.evidence[0].summary, fire_at.to_rfc3339());
    assert_eq!(d.finding.evidence[0].observed_at, Some(t0()));
}

#[test]
fn waiting_state_future_timer_reports_waiting_until_and_no_external_event() {
    let mut inst = instance(InstanceState::Waiting);
    inst.next_fire_at = Some(t0() + Duration::minutes(10));
    let report = diagnose(&full_ctx(inst), t0());
    let cs = codes(&report);
    assert!(cs.contains(&"WAITING_UNTIL"), "{cs:?}");
    assert!(!cs.contains(&"WAITING_EXTERNAL_EVENT"), "{cs:?}");
}

#[test]
fn waiting_until_is_direct_evidence_expected() {
    let mut inst = instance(InstanceState::Scheduled);
    inst.next_fire_at = Some(t0() + Duration::hours(1));
    let report = diagnose(&full_ctx(inst), t0());
    let d = find(&report, "WAITING_UNTIL");
    assert_eq!(d.category, DiagnosisCategory::DirectEvidence);
    assert_eq!(d.health, DiagnosisHealth::Expected);
}

// ===========================================================================
// BUDGET_PAUSED / PAUSED
// ===========================================================================

#[test]
fn budget_paused_with_breach_metadata_attaches_evidence() {
    let mut inst = instance(InstanceState::Paused);
    inst.metadata = json!({
        "paused_reason": "budget_exceeded",
        "budget_breach": {"limit": "max_total_tokens", "limit_value": 500, "actual": 612}
    });
    let report = diagnose(&full_ctx(inst), t0());
    let d = find(&report, "BUDGET_PAUSED");
    assert_eq!(d.finding.evidence.len(), 1);
    assert_eq!(d.finding.evidence[0].label, "budget_breach");
    assert!(d.finding.evidence[0].summary.contains("max_total_tokens"));
    assert!(d.finding.evidence[0].summary.contains("612"));
}

#[test]
fn budget_paused_without_breach_metadata_has_no_evidence() {
    let mut inst = instance(InstanceState::Paused);
    inst.metadata = json!({"paused_reason": "budget_exceeded"});
    let report = diagnose(&full_ctx(inst), t0());
    let d = find(&report, "BUDGET_PAUSED");
    assert!(d.finding.evidence.is_empty());
}

#[test]
fn budget_paused_is_warning_certain_expected() {
    let mut inst = instance(InstanceState::Paused);
    inst.metadata = json!({"paused_reason": "budget_exceeded"});
    let report = diagnose(&full_ctx(inst), t0());
    let d = find(&report, "BUDGET_PAUSED");
    assert_eq!(d.finding.severity, FindingSeverity::Warning);
    assert_eq!(d.finding.confidence, Confidence::Certain);
    assert_eq!(d.health, DiagnosisHealth::Expected);
    assert_eq!(d.category, DiagnosisCategory::DirectEvidence);
}

#[test]
fn budget_paused_remediation_resumes_the_instance() {
    let mut inst = instance(InstanceState::Paused);
    inst.metadata = json!({"paused_reason": "budget_exceeded"});
    let id = inst.id;
    let report = diagnose(&full_ctx(inst), t0());
    let d = find(&report, "BUDGET_PAUSED");
    let cmd = d.finding.remediation[0].command.as_deref().unwrap();
    assert!(cmd.contains("resume"), "{cmd}");
    assert!(cmd.contains(&id.to_string()), "{cmd}");
}

#[test]
fn plain_paused_reported_with_resume_command() {
    let inst = instance(InstanceState::Paused);
    let id = inst.id;
    let report = diagnose(&full_ctx(inst), t0());
    let d = find(&report, "PAUSED");
    assert_eq!(d.finding.severity, FindingSeverity::Info);
    let cmd = d.finding.remediation[0].command.as_deref().unwrap();
    assert!(cmd.contains("resume"), "{cmd}");
    assert!(cmd.contains(&id.to_string()), "{cmd}");
}

#[test]
fn paused_with_unrelated_reason_string_is_plain_paused() {
    let mut inst = instance(InstanceState::Paused);
    inst.metadata = json!({"paused_reason": "operator_request"});
    let report = diagnose(&full_ctx(inst), t0());
    let cs = codes(&report);
    assert!(cs.contains(&"PAUSED"), "{cs:?}");
    assert!(!cs.contains(&"BUDGET_PAUSED"), "{cs:?}");
}

#[test]
fn paused_with_non_string_reason_is_plain_paused() {
    let mut inst = instance(InstanceState::Paused);
    inst.metadata = json!({"paused_reason": 42});
    let report = diagnose(&full_ctx(inst), t0());
    let cs = codes(&report);
    assert!(cs.contains(&"PAUSED"), "{cs:?}");
    assert!(!cs.contains(&"BUDGET_PAUSED"), "{cs:?}");
}

#[test]
fn budget_metadata_on_non_paused_state_reports_nothing_paused() {
    let mut inst = instance(InstanceState::Running);
    inst.metadata = json!({"paused_reason": "budget_exceeded"});
    let report = diagnose(&full_ctx(inst), t0());
    let cs = codes(&report);
    assert!(!cs.contains(&"PAUSED"), "{cs:?}");
    assert!(!cs.contains(&"BUDGET_PAUSED"), "{cs:?}");
}

// ===========================================================================
// Worker tasks: Pending matrix
// ===========================================================================

#[test]
fn pending_task_with_uncollected_registrations_degrades_to_low_confidence() {
    let mut ctx = full_ctx(instance(InstanceState::Waiting));
    ctx.worker_tasks = Some(vec![task(WorkerTaskState::Pending, "charge_card", 10)]);
    ctx.worker_registrations = None;
    let report = diagnose(&ctx, t0());
    let d = find(&report, "WORKER_TASK_PENDING");
    assert_eq!(d.category, DiagnosisCategory::ProbableCause);
    assert_eq!(d.health, DiagnosisHealth::Degraded);
    assert_eq!(d.finding.confidence, Confidence::Low);
    assert!(d.finding.summary.contains("liveness could not be checked"));
}

#[test]
fn worker_task_pending_names_block_and_handler() {
    let mut ctx = full_ctx(instance(InstanceState::Waiting));
    ctx.worker_tasks = Some(vec![task_named(
        WorkerTaskState::Pending,
        "charge_card",
        "step_pay",
        10,
    )]);
    ctx.worker_registrations = None;
    let report = diagnose(&ctx, t0());
    let d = find(&report, "WORKER_TASK_PENDING");
    assert!(d.finding.summary.contains("step_pay"));
    assert!(d.finding.summary.contains("charge_card"));
}

#[test]
fn pending_task_with_no_registrations_is_no_compatible_worker() {
    let mut ctx = full_ctx(instance(InstanceState::Waiting));
    ctx.worker_tasks = Some(vec![task(WorkerTaskState::Pending, "charge_card", 10)]);
    let report = diagnose(&ctx, t0());
    let d = find(&report, "NO_COMPATIBLE_WORKER");
    assert_eq!(d.category, DiagnosisCategory::DirectEvidence);
    assert_eq!(d.health, DiagnosisHealth::Degraded);
    assert_eq!(d.finding.severity, FindingSeverity::Error);
    assert_eq!(d.finding.confidence, Confidence::High);
}

#[test]
fn pending_task_with_other_handler_registration_is_no_compatible_worker() {
    let mut ctx = full_ctx(instance(InstanceState::Waiting));
    ctx.worker_tasks = Some(vec![task(WorkerTaskState::Pending, "charge_card", 10)]);
    ctx.worker_registrations = Some(vec![registration("send_email", None)]);
    let report = diagnose(&ctx, t0());
    assert!(codes(&report).contains(&"NO_COMPATIBLE_WORKER"));
}

#[test]
fn no_compatible_worker_defaults_queue_name_to_default() {
    let mut ctx = full_ctx(instance(InstanceState::Waiting));
    ctx.worker_tasks = Some(vec![task(WorkerTaskState::Pending, "charge_card", 10)]);
    let report = diagnose(&ctx, t0());
    let d = find(&report, "NO_COMPATIBLE_WORKER");
    assert!(d.finding.summary.contains("queue: default"), "{}", d.finding.summary);
}

#[test]
fn no_compatible_worker_reports_custom_queue_name() {
    let mut ctx = full_ctx(instance(InstanceState::Waiting));
    let mut t = task(WorkerTaskState::Pending, "charge_card", 10);
    t.queue_name = Some("payments".into());
    ctx.worker_tasks = Some(vec![t]);
    let report = diagnose(&ctx, t0());
    let d = find(&report, "NO_COMPATIBLE_WORKER");
    assert!(d.finding.summary.contains("queue: payments"), "{}", d.finding.summary);
}

#[test]
fn no_compatible_worker_suggests_starting_a_worker() {
    let mut ctx = full_ctx(instance(InstanceState::Waiting));
    ctx.worker_tasks = Some(vec![task(WorkerTaskState::Pending, "charge_card", 10)]);
    let report = diagnose(&ctx, t0());
    let d = find(&report, "NO_COMPATIBLE_WORKER");
    assert!(d.finding.remediation[0].summary.contains("start a worker"));
}

#[test]
fn registration_scoped_to_other_tenant_does_not_match() {
    let mut ctx = full_ctx(instance(InstanceState::Waiting));
    ctx.worker_tasks = Some(vec![task(WorkerTaskState::Pending, "charge_card", 10)]);
    ctx.worker_registrations = Some(vec![registration_for_tenant(
        "charge_card",
        None,
        Some("t2"),
    )]);
    let report = diagnose(&ctx, t0());
    assert!(codes(&report).contains(&"NO_COMPATIBLE_WORKER"));
}

#[test]
fn registration_with_empty_tenant_matches_any_tenant() {
    let mut ctx = full_ctx(instance(InstanceState::Waiting));
    ctx.worker_tasks = Some(vec![task(WorkerTaskState::Pending, "charge_card", 10)]);
    ctx.worker_registrations = Some(vec![registration_for_tenant(
        "charge_card",
        None,
        Some(""),
    )]);
    let report = diagnose(&ctx, t0());
    assert!(codes(&report).contains(&"WAITING_WORKER_PICKUP"));
}

#[test]
fn registration_scoped_to_same_tenant_matches() {
    let mut ctx = full_ctx(instance(InstanceState::Waiting));
    ctx.worker_tasks = Some(vec![task(WorkerTaskState::Pending, "charge_card", 10)]);
    ctx.worker_registrations = Some(vec![registration_for_tenant(
        "charge_card",
        None,
        Some("t1"),
    )]);
    let report = diagnose(&ctx, t0());
    assert!(codes(&report).contains(&"WAITING_WORKER_PICKUP"));
}

#[test]
fn registration_with_null_tenant_matches_any_tenant() {
    let mut ctx = full_ctx(instance(InstanceState::Waiting));
    ctx.worker_tasks = Some(vec![task(WorkerTaskState::Pending, "charge_card", 10)]);
    ctx.worker_registrations = Some(vec![registration("charge_card", None)]);
    let report = diagnose(&ctx, t0());
    assert!(codes(&report).contains(&"WAITING_WORKER_PICKUP"));
}

#[test]
fn version_pin_unsatisfied_reports_below_pin_with_seen_workers() {
    let mut ctx = full_ctx(instance(InstanceState::Waiting));
    ctx.worker_tasks = Some(vec![task(WorkerTaskState::Pending, "charge_card", 10)]);
    ctx.worker_registrations = Some(vec![registration("charge_card", Some("1.0.0"))]);
    ctx.version_pins = Some(vec![pin("t1", "charge_card", "2.0.0")]);
    let report = diagnose(&ctx, t0());
    let d = find(&report, "WORKER_BELOW_VERSION_PIN");
    assert_eq!(d.category, DiagnosisCategory::DirectEvidence);
    assert_eq!(d.health, DiagnosisHealth::Degraded);
    assert_eq!(d.finding.severity, FindingSeverity::Error);
    assert_eq!(d.finding.confidence, Confidence::High);
    assert_eq!(d.finding.evidence[0].label, "live_workers");
    assert!(d.finding.evidence[0].summary.contains("w-1@1.0.0"));
    assert!(d.finding.summary.contains(">= 2.0.0"));
}

#[test]
fn version_pin_satisfied_exactly_allows_pickup() {
    let mut ctx = full_ctx(instance(InstanceState::Waiting));
    ctx.worker_tasks = Some(vec![task(WorkerTaskState::Pending, "charge_card", 10)]);
    ctx.worker_registrations = Some(vec![registration("charge_card", Some("2.0.0"))]);
    ctx.version_pins = Some(vec![pin("t1", "charge_card", "2.0.0")]);
    let report = diagnose(&ctx, t0());
    let cs = codes(&report);
    assert!(cs.contains(&"WAITING_WORKER_PICKUP"), "{cs:?}");
    assert!(!cs.contains(&"WORKER_BELOW_VERSION_PIN"), "{cs:?}");
}

#[test]
fn version_pin_satisfied_by_higher_version_allows_pickup() {
    let mut ctx = full_ctx(instance(InstanceState::Waiting));
    ctx.worker_tasks = Some(vec![task(WorkerTaskState::Pending, "charge_card", 10)]);
    ctx.worker_registrations = Some(vec![registration("charge_card", Some("2.10.1"))]);
    ctx.version_pins = Some(vec![pin("t1", "charge_card", "2.9.0")]);
    let report = diagnose(&ctx, t0());
    assert!(!codes(&report).contains(&"WORKER_BELOW_VERSION_PIN"));
}

#[test]
fn version_pin_for_other_tenant_is_ignored() {
    let mut ctx = full_ctx(instance(InstanceState::Waiting));
    ctx.worker_tasks = Some(vec![task(WorkerTaskState::Pending, "charge_card", 10)]);
    ctx.worker_registrations = Some(vec![registration("charge_card", Some("1.0.0"))]);
    ctx.version_pins = Some(vec![pin("t2", "charge_card", "9.0.0")]);
    let report = diagnose(&ctx, t0());
    let cs = codes(&report);
    assert!(!cs.contains(&"WORKER_BELOW_VERSION_PIN"), "{cs:?}");
    assert!(cs.contains(&"WAITING_WORKER_PICKUP"), "{cs:?}");
}

#[test]
fn version_pin_for_other_handler_is_ignored() {
    let mut ctx = full_ctx(instance(InstanceState::Waiting));
    ctx.worker_tasks = Some(vec![task(WorkerTaskState::Pending, "charge_card", 10)]);
    ctx.worker_registrations = Some(vec![registration("charge_card", Some("1.0.0"))]);
    ctx.version_pins = Some(vec![pin("t1", "send_email", "9.0.0")]);
    let report = diagnose(&ctx, t0());
    assert!(!codes(&report).contains(&"WORKER_BELOW_VERSION_PIN"));
}

#[test]
fn unversioned_worker_never_satisfies_a_pin() {
    let mut ctx = full_ctx(instance(InstanceState::Waiting));
    ctx.worker_tasks = Some(vec![task(WorkerTaskState::Pending, "charge_card", 10)]);
    ctx.worker_registrations = Some(vec![registration("charge_card", None)]);
    ctx.version_pins = Some(vec![pin("t1", "charge_card", "1.0.0")]);
    let report = diagnose(&ctx, t0());
    let d = find(&report, "WORKER_BELOW_VERSION_PIN");
    assert!(d.finding.evidence[0].summary.contains("unversioned"));
}

#[test]
fn pin_satisfied_by_any_one_of_multiple_workers() {
    let mut ctx = full_ctx(instance(InstanceState::Waiting));
    ctx.worker_tasks = Some(vec![task(WorkerTaskState::Pending, "charge_card", 10)]);
    let old = registration("charge_card", Some("1.0.0"));
    let mut new = registration("charge_card", Some("3.0.0"));
    new.worker_id = "w-2".into();
    ctx.worker_registrations = Some(vec![old, new]);
    ctx.version_pins = Some(vec![pin("t1", "charge_card", "2.0.0")]);
    let report = diagnose(&ctx, t0());
    let cs = codes(&report);
    assert!(!cs.contains(&"WORKER_BELOW_VERSION_PIN"), "{cs:?}");
    assert!(cs.contains(&"WAITING_WORKER_PICKUP"), "{cs:?}");
}

#[test]
fn uncollected_version_pins_do_not_block_pickup() {
    let mut ctx = full_ctx(instance(InstanceState::Waiting));
    ctx.worker_tasks = Some(vec![task(WorkerTaskState::Pending, "charge_card", 10)]);
    ctx.worker_registrations = Some(vec![registration("charge_card", None)]);
    ctx.version_pins = None;
    let report = diagnose(&ctx, t0());
    assert!(codes(&report).contains(&"WAITING_WORKER_PICKUP"));
}

#[test]
fn pending_task_at_exactly_alarm_age_is_still_pickup() {
    let mut ctx = full_ctx(instance(InstanceState::Waiting));
    ctx.worker_tasks = Some(vec![task(
        WorkerTaskState::Pending,
        "charge_card",
        PENDING_TASK_ALARM_SECS,
    )]);
    ctx.worker_registrations = Some(vec![registration("charge_card", None)]);
    let report = diagnose(&ctx, t0());
    let cs = codes(&report);
    assert!(cs.contains(&"WAITING_WORKER_PICKUP"), "{cs:?}");
    assert!(!cs.contains(&"WORKER_NOT_CLAIMING"), "{cs:?}");
}

#[test]
fn pending_task_one_second_past_alarm_is_worker_not_claiming() {
    let mut ctx = full_ctx(instance(InstanceState::Waiting));
    ctx.worker_tasks = Some(vec![task(
        WorkerTaskState::Pending,
        "charge_card",
        PENDING_TASK_ALARM_SECS + 1,
    )]);
    ctx.worker_registrations = Some(vec![registration("charge_card", None)]);
    let report = diagnose(&ctx, t0());
    let d = find(&report, "WORKER_NOT_CLAIMING");
    assert_eq!(d.category, DiagnosisCategory::DirectEvidence);
    assert_eq!(d.health, DiagnosisHealth::Degraded);
    assert_eq!(d.finding.confidence, Confidence::Medium);
}

#[test]
fn worker_not_claiming_reports_queue_and_age() {
    let mut ctx = full_ctx(instance(InstanceState::Waiting));
    let mut t = task(WorkerTaskState::Pending, "charge_card", 600);
    t.queue_name = Some("priority".into());
    ctx.worker_tasks = Some(vec![t]);
    ctx.worker_registrations = Some(vec![registration("charge_card", None)]);
    let report = diagnose(&ctx, t0());
    let d = find(&report, "WORKER_NOT_CLAIMING");
    assert!(d.finding.summary.contains("10m 0s"), "{}", d.finding.summary);
    assert!(d.finding.summary.contains("priority"), "{}", d.finding.summary);
    assert_eq!(d.finding.evidence[0].label, "task_created_at");
}

#[test]
fn waiting_worker_pickup_is_probable_cause_expected_medium() {
    let mut ctx = full_ctx(instance(InstanceState::Waiting));
    ctx.worker_tasks = Some(vec![task(WorkerTaskState::Pending, "charge_card", 10)]);
    ctx.worker_registrations = Some(vec![registration("charge_card", None)]);
    let report = diagnose(&ctx, t0());
    let d = find(&report, "WAITING_WORKER_PICKUP");
    assert_eq!(d.category, DiagnosisCategory::ProbableCause);
    assert_eq!(d.health, DiagnosisHealth::Expected);
    assert_eq!(d.finding.confidence, Confidence::Medium);
    assert_eq!(d.finding.severity, FindingSeverity::Info);
}

#[test]
fn multiple_pending_tasks_each_get_a_diagnosis() {
    let mut ctx = full_ctx(instance(InstanceState::Waiting));
    ctx.worker_tasks = Some(vec![
        task_named(WorkerTaskState::Pending, "charge_card", "b1", 10),
        task_named(WorkerTaskState::Pending, "send_email", "b2", 10),
    ]);
    ctx.worker_registrations = Some(vec![registration("charge_card", None)]);
    let report = diagnose(&ctx, t0());
    let cs = codes(&report);
    assert!(cs.contains(&"WAITING_WORKER_PICKUP"), "{cs:?}");
    assert!(cs.contains(&"NO_COMPATIBLE_WORKER"), "{cs:?}");
}

// ===========================================================================
// Worker tasks: Claimed matrix
// ===========================================================================

#[test]
fn fresh_claim_produces_no_diagnosis() {
    let mut ctx = full_ctx(instance(InstanceState::Waiting));
    ctx.worker_tasks = Some(vec![task(WorkerTaskState::Claimed, "h", 10)]);
    let report = diagnose(&ctx, t0());
    assert!(!codes(&report).contains(&"STALE_WORKER_CLAIM"));
}

#[test]
fn heartbeat_at_exactly_stale_boundary_is_not_stale() {
    let mut ctx = full_ctx(instance(InstanceState::Waiting));
    ctx.worker_tasks = Some(vec![task(
        WorkerTaskState::Claimed,
        "h",
        STALE_HEARTBEAT_SECS,
    )]);
    let report = diagnose(&ctx, t0());
    assert!(!codes(&report).contains(&"STALE_WORKER_CLAIM"));
}

#[test]
fn heartbeat_one_second_past_boundary_is_stale() {
    let mut ctx = full_ctx(instance(InstanceState::Waiting));
    ctx.worker_tasks = Some(vec![task(
        WorkerTaskState::Claimed,
        "h",
        STALE_HEARTBEAT_SECS + 1,
    )]);
    let report = diagnose(&ctx, t0());
    let d = find(&report, "STALE_WORKER_CLAIM");
    assert_eq!(d.finding.severity, FindingSeverity::Error);
    assert_eq!(d.finding.confidence, Confidence::High);
}

#[test]
fn missing_heartbeat_falls_back_to_claimed_at() {
    let mut ctx = full_ctx(instance(InstanceState::Waiting));
    let mut t = task(WorkerTaskState::Claimed, "h", 10);
    t.heartbeat_at = None;
    t.claimed_at = Some(t0() - Duration::seconds(900));
    ctx.worker_tasks = Some(vec![t]);
    let report = diagnose(&ctx, t0());
    let d = find(&report, "STALE_WORKER_CLAIM");
    assert_eq!(
        d.finding.evidence[0].summary,
        (t0() - Duration::seconds(900)).to_rfc3339()
    );
}

#[test]
fn missing_heartbeat_with_fresh_claimed_at_is_not_stale() {
    let mut ctx = full_ctx(instance(InstanceState::Waiting));
    let mut t = task(WorkerTaskState::Claimed, "h", 900);
    t.heartbeat_at = None;
    t.claimed_at = Some(t0() - Duration::seconds(10));
    ctx.worker_tasks = Some(vec![t]);
    let report = diagnose(&ctx, t0());
    assert!(!codes(&report).contains(&"STALE_WORKER_CLAIM"));
}

#[test]
fn claimed_with_no_heartbeat_and_no_claimed_at_produces_no_diagnosis() {
    let mut ctx = full_ctx(instance(InstanceState::Waiting));
    let mut t = task(WorkerTaskState::Claimed, "h", 900);
    t.heartbeat_at = None;
    t.claimed_at = None;
    ctx.worker_tasks = Some(vec![t]);
    let report = diagnose(&ctx, t0());
    assert!(!codes(&report).contains(&"STALE_WORKER_CLAIM"));
}

#[test]
fn stale_claim_names_the_worker_and_age() {
    let mut ctx = full_ctx(instance(InstanceState::Waiting));
    let mut t = task(WorkerTaskState::Claimed, "h", 900);
    t.worker_id = Some("worker-42".into());
    ctx.worker_tasks = Some(vec![t]);
    let report = diagnose(&ctx, t0());
    let d = find(&report, "STALE_WORKER_CLAIM");
    assert!(d.finding.summary.contains("worker-42"), "{}", d.finding.summary);
    assert!(d.finding.summary.contains("15m 0s"), "{}", d.finding.summary);
    assert!(d.finding.remediation[0].side_effect_risk);
}

#[test]
fn stale_claim_with_unknown_worker_prints_placeholder() {
    let mut ctx = full_ctx(instance(InstanceState::Waiting));
    let mut t = task(WorkerTaskState::Claimed, "h", 900);
    t.worker_id = None;
    ctx.worker_tasks = Some(vec![t]);
    let report = diagnose(&ctx, t0());
    let d = find(&report, "STALE_WORKER_CLAIM");
    assert!(d.finding.summary.contains("'?'"), "{}", d.finding.summary);
}

#[test]
fn completed_worker_task_is_ignored() {
    let mut ctx = full_ctx(instance(InstanceState::Waiting));
    ctx.worker_tasks = Some(vec![task(WorkerTaskState::Completed, "h", 900)]);
    let report = diagnose(&ctx, t0());
    let cs = codes(&report);
    for code in [
        "STALE_WORKER_CLAIM",
        "NO_COMPATIBLE_WORKER",
        "WAITING_WORKER_PICKUP",
        "WORKER_NOT_CLAIMING",
        "WORKER_TASK_PENDING",
    ] {
        assert!(!cs.contains(&code), "{code} unexpectedly present: {cs:?}");
    }
}

#[test]
fn failed_worker_task_is_ignored() {
    let mut ctx = full_ctx(instance(InstanceState::Waiting));
    ctx.worker_tasks = Some(vec![task(WorkerTaskState::Failed, "h", 900)]);
    let report = diagnose(&ctx, t0());
    let cs = codes(&report);
    assert!(!cs.contains(&"STALE_WORKER_CLAIM"), "{cs:?}");
    assert!(!cs.contains(&"NO_COMPATIBLE_WORKER"), "{cs:?}");
}

// ===========================================================================
// Circuit breaker matrix
// ===========================================================================

#[test]
fn open_breaker_for_involved_handler_is_direct_evidence_high() {
    let mut ctx = full_ctx(instance(InstanceState::Waiting));
    ctx.worker_tasks = Some(vec![task(WorkerTaskState::Pending, "flaky", 10)]);
    ctx.worker_registrations = Some(vec![registration("flaky", None)]);
    ctx.open_breakers = Some(vec![breaker("t1", "flaky", BreakerState::Open)]);
    let report = diagnose(&ctx, t0());
    let d = find(&report, "OPEN_CIRCUIT_BREAKER");
    assert_eq!(d.category, DiagnosisCategory::DirectEvidence);
    assert_eq!(d.finding.confidence, Confidence::High);
    assert_eq!(d.health, DiagnosisHealth::Degraded);
}

#[test]
fn open_breaker_for_uninvolved_handler_is_health_warning_low() {
    let mut ctx = full_ctx(instance(InstanceState::Waiting));
    ctx.open_breakers = Some(vec![breaker("t1", "unrelated", BreakerState::Open)]);
    let report = diagnose(&ctx, t0());
    let d = find(&report, "OPEN_CIRCUIT_BREAKER");
    assert_eq!(d.category, DiagnosisCategory::HealthWarning);
    assert_eq!(d.finding.confidence, Confidence::Low);
    assert_eq!(d.health, DiagnosisHealth::Degraded);
}

#[test]
fn closed_breaker_is_ignored() {
    let mut ctx = full_ctx(instance(InstanceState::Waiting));
    ctx.open_breakers = Some(vec![breaker("t1", "h", BreakerState::Closed)]);
    let report = diagnose(&ctx, t0());
    assert!(!codes(&report).contains(&"OPEN_CIRCUIT_BREAKER"));
}

#[test]
fn half_open_breaker_is_ignored() {
    let mut ctx = full_ctx(instance(InstanceState::Waiting));
    ctx.open_breakers = Some(vec![breaker("t1", "h", BreakerState::HalfOpen)]);
    let report = diagnose(&ctx, t0());
    assert!(!codes(&report).contains(&"OPEN_CIRCUIT_BREAKER"));
}

#[test]
fn open_breaker_for_other_tenant_is_ignored() {
    let mut ctx = full_ctx(instance(InstanceState::Waiting));
    ctx.open_breakers = Some(vec![breaker("t2", "h", BreakerState::Open)]);
    let report = diagnose(&ctx, t0());
    assert!(!codes(&report).contains(&"OPEN_CIRCUIT_BREAKER"));
}

#[test]
fn breaker_cooldown_remaining_is_humanized_in_summary() {
    let mut ctx = full_ctx(instance(InstanceState::Waiting));
    let mut b = breaker("t1", "h", BreakerState::Open);
    b.opened_at = Some(t0() - Duration::seconds(60));
    b.cooldown_secs = 300; // 240s left => "4m 0s"
    ctx.open_breakers = Some(vec![b]);
    let report = diagnose(&ctx, t0());
    let d = find(&report, "OPEN_CIRCUIT_BREAKER");
    assert!(
        d.finding.summary.contains("retries resume in 4m 0s"),
        "{}",
        d.finding.summary
    );
}

#[test]
fn breaker_with_elapsed_cooldown_omits_resume_text() {
    let mut ctx = full_ctx(instance(InstanceState::Waiting));
    let mut b = breaker("t1", "h", BreakerState::Open);
    b.opened_at = Some(t0() - Duration::seconds(400)); // past the 300s cooldown
    ctx.open_breakers = Some(vec![b]);
    let report = diagnose(&ctx, t0());
    let d = find(&report, "OPEN_CIRCUIT_BREAKER");
    assert!(!d.finding.summary.contains("resume in"), "{}", d.finding.summary);
}

#[test]
fn breaker_without_opened_at_has_no_evidence_and_no_resume_text() {
    let mut ctx = full_ctx(instance(InstanceState::Waiting));
    let mut b = breaker("t1", "h", BreakerState::Open);
    b.opened_at = None;
    ctx.open_breakers = Some(vec![b]);
    let report = diagnose(&ctx, t0());
    let d = find(&report, "OPEN_CIRCUIT_BREAKER");
    assert!(d.finding.evidence.is_empty());
    assert!(!d.finding.summary.contains("resume in"));
}

#[test]
fn breaker_summary_reports_failure_count() {
    let mut ctx = full_ctx(instance(InstanceState::Waiting));
    ctx.open_breakers = Some(vec![breaker("t1", "flaky", BreakerState::Open)]);
    let report = diagnose(&ctx, t0());
    let d = find(&report, "OPEN_CIRCUIT_BREAKER");
    assert!(
        d.finding.summary.contains("7 consecutive failures"),
        "{}",
        d.finding.summary
    );
    assert!(d.finding.summary.contains("flaky"));
}

#[test]
fn breaker_involvement_counts_terminal_tasks_too() {
    // A completed task for the breaker's handler still marks the breaker as
    // directly involved (the handler participated in this instance).
    let mut ctx = full_ctx(instance(InstanceState::Waiting));
    ctx.worker_tasks = Some(vec![task(WorkerTaskState::Completed, "flaky", 10)]);
    ctx.open_breakers = Some(vec![breaker("t1", "flaky", BreakerState::Open)]);
    let report = diagnose(&ctx, t0());
    let d = find(&report, "OPEN_CIRCUIT_BREAKER");
    assert_eq!(d.category, DiagnosisCategory::DirectEvidence);
    assert_eq!(d.finding.confidence, Confidence::High);
}

#[test]
fn breaker_with_uncollected_worker_tasks_is_treated_as_uninvolved() {
    let mut ctx = full_ctx(instance(InstanceState::Waiting));
    ctx.worker_tasks = None;
    ctx.open_breakers = Some(vec![breaker("t1", "flaky", BreakerState::Open)]);
    let report = diagnose(&ctx, t0());
    let d = find(&report, "OPEN_CIRCUIT_BREAKER");
    assert_eq!(d.category, DiagnosisCategory::HealthWarning);
    assert_eq!(d.finding.confidence, Confidence::Low);
}

#[test]
fn multiple_open_breakers_reported_individually() {
    let mut ctx = full_ctx(instance(InstanceState::Waiting));
    ctx.open_breakers = Some(vec![
        breaker("t1", "a", BreakerState::Open),
        breaker("t1", "b", BreakerState::Open),
        breaker("t2", "c", BreakerState::Open),
    ]);
    let report = diagnose(&ctx, t0());
    let n = report
        .diagnoses
        .iter()
        .filter(|d| d.finding.code == "OPEN_CIRCUIT_BREAKER")
        .count();
    assert_eq!(n, 2, "{:?}", codes(&report));
}

// ===========================================================================
// Children
// ===========================================================================

#[test]
fn waiting_parent_with_live_child_reports_waiting_child() {
    let mut ctx = full_ctx(instance(InstanceState::Waiting));
    ctx.children = Some(vec![instance(InstanceState::Running)]);
    let report = diagnose(&ctx, t0());
    let d = find(&report, "WAITING_CHILD");
    assert_eq!(d.category, DiagnosisCategory::DirectEvidence);
    assert_eq!(d.health, DiagnosisHealth::Expected);
    assert_eq!(d.finding.confidence, Confidence::High);
    assert!(d.finding.summary.contains("1 child"), "{}", d.finding.summary);
}

#[test]
fn waiting_child_evidence_lists_child_ids() {
    let mut ctx = full_ctx(instance(InstanceState::Waiting));
    let c1 = instance(InstanceState::Running);
    let c2 = instance(InstanceState::Scheduled);
    let (id1, id2) = (c1.id.to_string(), c2.id.to_string());
    ctx.children = Some(vec![c1, c2]);
    let report = diagnose(&ctx, t0());
    let d = find(&report, "WAITING_CHILD");
    assert_eq!(d.finding.evidence[0].label, "children");
    assert!(d.finding.evidence[0].summary.contains(&id1));
    assert!(d.finding.evidence[0].summary.contains(&id2));
    assert!(d.finding.summary.contains("2 child"), "{}", d.finding.summary);
}

#[test]
fn mixed_children_counts_only_live_ones() {
    let mut ctx = full_ctx(instance(InstanceState::Waiting));
    ctx.children = Some(vec![
        instance(InstanceState::Completed),
        instance(InstanceState::Running),
        instance(InstanceState::Failed),
    ]);
    let report = diagnose(&ctx, t0());
    let d = find(&report, "WAITING_CHILD");
    assert!(d.finding.summary.contains("1 child"), "{}", d.finding.summary);
    assert!(!codes(&report).contains(&"CHILDREN_DONE_PARENT_WAITING"));
}

#[test]
fn all_terminal_children_with_waiting_parent_is_inconsistent() {
    let mut ctx = full_ctx(instance(InstanceState::Waiting));
    ctx.children = Some(vec![
        instance(InstanceState::Completed),
        instance(InstanceState::Cancelled),
        instance(InstanceState::Failed),
    ]);
    let report = diagnose(&ctx, t0());
    let d = find(&report, "CHILDREN_DONE_PARENT_WAITING");
    assert_eq!(d.category, DiagnosisCategory::ProbableCause);
    assert_eq!(d.health, DiagnosisHealth::Inconsistent);
    assert_eq!(d.finding.confidence, Confidence::Medium);
    assert_eq!(d.finding.severity, FindingSeverity::Warning);
}

#[test]
fn empty_children_list_produces_no_child_diagnosis() {
    let report = diagnose(&full_ctx(instance(InstanceState::Waiting)), t0());
    let cs = codes(&report);
    assert!(!cs.contains(&"WAITING_CHILD"), "{cs:?}");
    assert!(!cs.contains(&"CHILDREN_DONE_PARENT_WAITING"), "{cs:?}");
}

#[test]
fn non_waiting_parent_with_live_children_reports_nothing() {
    let mut ctx = full_ctx(instance(InstanceState::Running));
    ctx.children = Some(vec![instance(InstanceState::Running)]);
    let report = diagnose(&ctx, t0());
    let cs = codes(&report);
    assert!(!cs.contains(&"WAITING_CHILD"), "{cs:?}");
    assert!(!cs.contains(&"CHILDREN_DONE_PARENT_WAITING"), "{cs:?}");
}

#[test]
fn non_waiting_parent_with_terminal_children_reports_nothing() {
    let mut ctx = full_ctx(instance(InstanceState::Scheduled));
    ctx.children = Some(vec![instance(InstanceState::Completed)]);
    let report = diagnose(&ctx, t0());
    let cs = codes(&report);
    assert!(!cs.contains(&"WAITING_CHILD"), "{cs:?}");
    assert!(!cs.contains(&"CHILDREN_DONE_PARENT_WAITING"), "{cs:?}");
}

// ===========================================================================
// SIGNALS_NOT_CONSUMED (60s age boundary)
// ===========================================================================

#[test]
fn signal_aged_exactly_sixty_seconds_is_not_stale() {
    let mut ctx = full_ctx(instance(InstanceState::Waiting));
    ctx.pending_signals = Some(vec![signal(60)]);
    let report = diagnose(&ctx, t0());
    assert!(!codes(&report).contains(&"SIGNALS_NOT_CONSUMED"));
}

#[test]
fn signal_aged_sixty_one_seconds_is_stale() {
    let mut ctx = full_ctx(instance(InstanceState::Waiting));
    ctx.pending_signals = Some(vec![signal(61)]);
    let report = diagnose(&ctx, t0());
    let d = find(&report, "SIGNALS_NOT_CONSUMED");
    assert_eq!(d.category, DiagnosisCategory::ProbableCause);
    assert_eq!(d.health, DiagnosisHealth::Degraded);
    assert_eq!(d.finding.confidence, Confidence::Medium);
    assert_eq!(d.finding.severity, FindingSeverity::Warning);
}

#[test]
fn stale_signal_count_appears_in_summary() {
    let mut ctx = full_ctx(instance(InstanceState::Waiting));
    ctx.pending_signals = Some(vec![signal(120), signal(600), signal(3600)]);
    let report = diagnose(&ctx, t0());
    let d = find(&report, "SIGNALS_NOT_CONSUMED");
    assert!(d.finding.summary.contains("3 signal(s)"), "{}", d.finding.summary);
}

#[test]
fn mixed_signal_ages_count_only_stale_ones() {
    let mut ctx = full_ctx(instance(InstanceState::Waiting));
    ctx.pending_signals = Some(vec![signal(10), signal(59), signal(300)]);
    let report = diagnose(&ctx, t0());
    let d = find(&report, "SIGNALS_NOT_CONSUMED");
    assert!(d.finding.summary.contains("1 signal(s)"), "{}", d.finding.summary);
}

#[test]
fn only_fresh_signals_produce_no_diagnosis() {
    let mut ctx = full_ctx(instance(InstanceState::Waiting));
    ctx.pending_signals = Some(vec![signal(1), signal(30)]);
    let report = diagnose(&ctx, t0());
    assert!(!codes(&report).contains(&"SIGNALS_NOT_CONSUMED"));
}

// ===========================================================================
// STALE_RUNNING_STATE boundary
// ===========================================================================

#[test]
fn running_updated_exactly_at_boundary_is_not_stale() {
    let mut inst = instance(InstanceState::Running);
    inst.updated_at = t0() - Duration::seconds(STALE_RUNNING_SECS);
    let report = diagnose(&full_ctx(inst), t0());
    assert!(!codes(&report).contains(&"STALE_RUNNING_STATE"));
}

#[test]
fn running_updated_one_second_past_boundary_is_stale() {
    let mut inst = instance(InstanceState::Running);
    inst.updated_at = t0() - Duration::seconds(STALE_RUNNING_SECS + 1);
    let report = diagnose(&full_ctx(inst), t0());
    let d = find(&report, "STALE_RUNNING_STATE");
    assert_eq!(d.category, DiagnosisCategory::ProbableCause);
    assert_eq!(d.health, DiagnosisHealth::Inconsistent);
    assert_eq!(d.finding.severity, FindingSeverity::Error);
    assert_eq!(d.finding.confidence, Confidence::Medium);
}

#[test]
fn stale_running_evidence_is_updated_at() {
    let mut inst = instance(InstanceState::Running);
    let updated = t0() - Duration::minutes(30);
    inst.updated_at = updated;
    let report = diagnose(&full_ctx(inst), t0());
    let d = find(&report, "STALE_RUNNING_STATE");
    assert_eq!(d.finding.evidence[0].label, "updated_at");
    assert_eq!(d.finding.evidence[0].summary, updated.to_rfc3339());
    assert!(d.finding.summary.contains("30m"), "{}", d.finding.summary);
}

#[test]
fn stale_update_on_non_running_state_is_not_stale_running() {
    let mut inst = instance(InstanceState::Waiting);
    inst.updated_at = t0() - Duration::hours(2);
    let report = diagnose(&full_ctx(inst), t0());
    assert!(!codes(&report).contains(&"STALE_RUNNING_STATE"));
}

// ===========================================================================
// SCHEDULER_LAG boundary
// ===========================================================================

#[test]
fn scheduled_overdue_exactly_at_boundary_is_not_lag() {
    let mut inst = instance(InstanceState::Scheduled);
    inst.next_fire_at = Some(t0() - Duration::seconds(SCHEDULER_LAG_SECS));
    let report = diagnose(&full_ctx(inst), t0());
    assert!(!codes(&report).contains(&"SCHEDULER_LAG"));
}

#[test]
fn scheduled_overdue_one_second_past_boundary_is_lag() {
    let mut inst = instance(InstanceState::Scheduled);
    inst.next_fire_at = Some(t0() - Duration::seconds(SCHEDULER_LAG_SECS + 1));
    let report = diagnose(&full_ctx(inst), t0());
    let d = find(&report, "SCHEDULER_LAG");
    assert_eq!(d.category, DiagnosisCategory::ProbableCause);
    assert_eq!(d.health, DiagnosisHealth::Degraded);
    assert_eq!(d.finding.severity, FindingSeverity::Error);
}

#[test]
fn scheduler_lag_evidence_is_next_fire_at() {
    let fire_at = t0() - Duration::minutes(10);
    let mut inst = instance(InstanceState::Scheduled);
    inst.next_fire_at = Some(fire_at);
    let report = diagnose(&full_ctx(inst), t0());
    let d = find(&report, "SCHEDULER_LAG");
    assert_eq!(d.finding.evidence[0].label, "next_fire_at");
    assert_eq!(d.finding.evidence[0].summary, fire_at.to_rfc3339());
    assert!(d.finding.summary.contains("10m"), "{}", d.finding.summary);
}

#[test]
fn overdue_waiting_instance_is_not_scheduler_lag() {
    let mut inst = instance(InstanceState::Waiting);
    inst.next_fire_at = Some(t0() - Duration::hours(1));
    let report = diagnose(&full_ctx(inst), t0());
    assert!(!codes(&report).contains(&"SCHEDULER_LAG"));
}

#[test]
fn scheduled_without_timer_is_not_scheduler_lag() {
    let report = diagnose(&full_ctx(instance(InstanceState::Scheduled)), t0());
    assert!(!codes(&report).contains(&"SCHEDULER_LAG"));
}

// ===========================================================================
// WAITING_EVENT
// ===========================================================================

#[test]
fn waiting_event_reports_only_missing_names() {
    let mut ctx = full_ctx(instance(InstanceState::Waiting));
    ctx.event_waits = Some(vec![wait(
        "gate",
        WaitStatus::Waiting,
        &["paid", "shipped"],
        &["paid"],
    )]);
    let report = diagnose(&ctx, t0());
    let d = find(&report, "WAITING_EVENT");
    assert!(d.finding.summary.contains("[shipped]"), "{}", d.finding.summary);
    assert!(d.finding.summary.contains("already matched: paid"));
    assert_eq!(d.finding.confidence, Confidence::Certain);
    assert_eq!(d.category, DiagnosisCategory::DirectEvidence);
}

#[test]
fn waiting_event_without_matches_omits_matched_annotation() {
    let mut ctx = full_ctx(instance(InstanceState::Waiting));
    ctx.event_waits = Some(vec![wait("gate", WaitStatus::Waiting, &["paid"], &[])]);
    let report = diagnose(&ctx, t0());
    let d = find(&report, "WAITING_EVENT");
    assert!(!d.finding.summary.contains("already matched"), "{}", d.finding.summary);
    assert!(d.finding.summary.contains("[paid]"));
}

#[test]
fn waiting_event_evidence_and_remediation_carry_correlation_key() {
    let mut ctx = full_ctx(instance(InstanceState::Waiting));
    ctx.event_waits = Some(vec![wait("gate", WaitStatus::Waiting, &["paid"], &[])]);
    let report = diagnose(&ctx, t0());
    let d = find(&report, "WAITING_EVENT");
    assert_eq!(d.finding.evidence[0].label, "correlation_key");
    assert_eq!(d.finding.evidence[0].summary, "corr-1");
    assert!(d.finding.remediation[0].summary.contains("corr-1"));
    assert!(d.finding.remediation[0].summary.contains("POST /events"));
}

#[test]
fn satisfied_event_wait_is_not_reported() {
    let mut ctx = full_ctx(instance(InstanceState::Waiting));
    ctx.event_waits = Some(vec![wait("gate", WaitStatus::Satisfied, &["paid"], &["paid"])]);
    let report = diagnose(&ctx, t0());
    assert!(!codes(&report).contains(&"WAITING_EVENT"));
}

#[test]
fn cancelled_event_wait_is_not_reported() {
    let mut ctx = full_ctx(instance(InstanceState::Waiting));
    ctx.event_waits = Some(vec![wait("gate", WaitStatus::Cancelled, &["paid"], &[])]);
    let report = diagnose(&ctx, t0());
    assert!(!codes(&report).contains(&"WAITING_EVENT"));
}

#[test]
fn event_wait_takes_precedence_over_pending_approval_for_same_block() {
    let mut ctx = full_ctx(instance(InstanceState::Waiting));
    ctx.pending_approval_blocks = Some(vec!["gate".into()]);
    ctx.event_waits = Some(vec![wait("gate", WaitStatus::Waiting, &["paid"], &[])]);
    let report = diagnose(&ctx, t0());
    let cs = codes(&report);
    assert!(cs.contains(&"WAITING_EVENT"), "{cs:?}");
    assert!(!cs.contains(&"PENDING_APPROVAL"), "{cs:?}");
}

#[test]
fn non_waiting_event_wait_still_masks_approval_for_its_block() {
    // Documented behavior: any wait registration for a block (even a
    // satisfied one) claims the block, so it is not reported as an approval.
    let mut ctx = full_ctx(instance(InstanceState::Waiting));
    ctx.pending_approval_blocks = Some(vec!["gate".into()]);
    ctx.event_waits = Some(vec![wait("gate", WaitStatus::Satisfied, &["paid"], &["paid"])]);
    let report = diagnose(&ctx, t0());
    let cs = codes(&report);
    assert!(!cs.contains(&"WAITING_EVENT"), "{cs:?}");
    assert!(!cs.contains(&"PENDING_APPROVAL"), "{cs:?}");
}

#[test]
fn approval_for_different_block_survives_event_wait() {
    let mut ctx = full_ctx(instance(InstanceState::Waiting));
    ctx.pending_approval_blocks = Some(vec!["review".into()]);
    ctx.event_waits = Some(vec![wait("gate", WaitStatus::Waiting, &["paid"], &[])]);
    let report = diagnose(&ctx, t0());
    let cs = codes(&report);
    assert!(cs.contains(&"WAITING_EVENT"), "{cs:?}");
    assert!(cs.contains(&"PENDING_APPROVAL"), "{cs:?}");
}

#[test]
fn multiple_waiting_event_waits_each_reported() {
    let mut ctx = full_ctx(instance(InstanceState::Waiting));
    ctx.event_waits = Some(vec![
        wait("g1", WaitStatus::Waiting, &["a"], &[]),
        wait("g2", WaitStatus::Waiting, &["b"], &[]),
    ]);
    let report = diagnose(&ctx, t0());
    let n = report
        .diagnoses
        .iter()
        .filter(|d| d.finding.code == "WAITING_EVENT")
        .count();
    assert_eq!(n, 2);
}

// ===========================================================================
// PENDING_APPROVAL
// ===========================================================================

#[test]
fn pending_approval_command_targets_block_and_instance() {
    let mut ctx = full_ctx(instance(InstanceState::Waiting));
    let id = ctx.instance.id;
    ctx.pending_approval_blocks = Some(vec!["approve_payment".into()]);
    let report = diagnose(&ctx, t0());
    let d = find(&report, "PENDING_APPROVAL");
    let cmd = d.finding.remediation[0].command.as_deref().unwrap();
    assert!(cmd.contains("human_input:approve_payment"), "{cmd}");
    assert!(cmd.contains(&id.to_string()), "{cmd}");
    assert_eq!(d.finding.confidence, Confidence::Certain);
}

#[test]
fn multiple_pending_approvals_each_reported() {
    let mut ctx = full_ctx(instance(InstanceState::Waiting));
    ctx.pending_approval_blocks = Some(vec!["a1".into(), "a2".into()]);
    let report = diagnose(&ctx, t0());
    let n = report
        .diagnoses
        .iter()
        .filter(|d| d.finding.code == "PENDING_APPROVAL")
        .count();
    assert_eq!(n, 2);
}

#[test]
fn pending_approval_is_reported_regardless_of_instance_state() {
    // The rule trusts the collector: if it says a block waits for input,
    // the diagnosis is emitted even for non-Waiting instances.
    let mut ctx = full_ctx(instance(InstanceState::Running));
    ctx.pending_approval_blocks = Some(vec!["gate".into()]);
    let report = diagnose(&ctx, t0());
    assert!(codes(&report).contains(&"PENDING_APPROVAL"));
}

// ===========================================================================
// WAITING_EXTERNAL_EVENT suppression rules
// ===========================================================================

#[test]
fn bare_waiting_instance_suggests_external_event() {
    let report = diagnose(&full_ctx(instance(InstanceState::Waiting)), t0());
    let d = find(&report, "WAITING_EXTERNAL_EVENT");
    assert_eq!(d.category, DiagnosisCategory::ProbableCause);
    assert_eq!(d.health, DiagnosisHealth::Expected);
    assert!(d.finding.confidence < Confidence::High);
    let cmd = d.finding.remediation[0].command.as_deref().unwrap();
    assert!(cmd.contains("custom:<name>"), "{cmd}");
}

#[test]
fn future_timer_suppresses_external_event() {
    let mut inst = instance(InstanceState::Waiting);
    inst.next_fire_at = Some(t0() + Duration::seconds(30));
    let report = diagnose(&full_ctx(inst), t0());
    assert!(!codes(&report).contains(&"WAITING_EXTERNAL_EVENT"));
}

#[test]
fn past_timer_does_not_suppress_external_event() {
    let mut inst = instance(InstanceState::Waiting);
    inst.next_fire_at = Some(t0() - Duration::seconds(30));
    let report = diagnose(&full_ctx(inst), t0());
    assert!(codes(&report).contains(&"WAITING_EXTERNAL_EVENT"));
}

#[test]
fn pending_approval_suppresses_external_event() {
    let mut ctx = full_ctx(instance(InstanceState::Waiting));
    ctx.pending_approval_blocks = Some(vec!["gate".into()]);
    let report = diagnose(&ctx, t0());
    assert!(!codes(&report).contains(&"WAITING_EXTERNAL_EVENT"));
}

#[test]
fn live_child_suppresses_external_event() {
    let mut ctx = full_ctx(instance(InstanceState::Waiting));
    ctx.children = Some(vec![instance(InstanceState::Running)]);
    let report = diagnose(&ctx, t0());
    assert!(!codes(&report).contains(&"WAITING_EXTERNAL_EVENT"));
}

#[test]
fn terminal_children_do_not_suppress_external_event() {
    let mut ctx = full_ctx(instance(InstanceState::Waiting));
    ctx.children = Some(vec![instance(InstanceState::Completed)]);
    let report = diagnose(&ctx, t0());
    let cs = codes(&report);
    assert!(cs.contains(&"WAITING_EXTERNAL_EVENT"), "{cs:?}");
    assert!(cs.contains(&"CHILDREN_DONE_PARENT_WAITING"), "{cs:?}");
}

#[test]
fn pending_worker_task_suppresses_external_event() {
    let mut ctx = full_ctx(instance(InstanceState::Waiting));
    ctx.worker_tasks = Some(vec![task(WorkerTaskState::Pending, "h", 10)]);
    let report = diagnose(&ctx, t0());
    assert!(!codes(&report).contains(&"WAITING_EXTERNAL_EVENT"));
}

#[test]
fn claimed_worker_task_suppresses_external_event() {
    let mut ctx = full_ctx(instance(InstanceState::Waiting));
    ctx.worker_tasks = Some(vec![task(WorkerTaskState::Claimed, "h", 10)]);
    let report = diagnose(&ctx, t0());
    assert!(!codes(&report).contains(&"WAITING_EXTERNAL_EVENT"));
}

#[test]
fn completed_worker_task_does_not_suppress_external_event() {
    let mut ctx = full_ctx(instance(InstanceState::Waiting));
    ctx.worker_tasks = Some(vec![task(WorkerTaskState::Completed, "h", 10)]);
    let report = diagnose(&ctx, t0());
    assert!(codes(&report).contains(&"WAITING_EXTERNAL_EVENT"));
}

#[test]
fn failed_worker_task_does_not_suppress_external_event() {
    let mut ctx = full_ctx(instance(InstanceState::Waiting));
    ctx.worker_tasks = Some(vec![task(WorkerTaskState::Failed, "h", 10)]);
    let report = diagnose(&ctx, t0());
    assert!(codes(&report).contains(&"WAITING_EXTERNAL_EVENT"));
}

#[test]
fn non_waiting_states_never_report_external_event() {
    for state in [
        InstanceState::Scheduled,
        InstanceState::Running,
        InstanceState::Paused,
    ] {
        let report = diagnose(&full_ctx(instance(state)), t0());
        assert!(
            !codes(&report).contains(&"WAITING_EXTERNAL_EVENT"),
            "{state:?}: {:?}",
            codes(&report)
        );
    }
}

// ===========================================================================
// SEQUENCE_MISSING
// ===========================================================================

#[test]
fn missing_sequence_is_critical_certain_inconsistent() {
    let mut ctx = full_ctx(instance(InstanceState::Scheduled));
    ctx.sequence_exists = Some(false);
    let report = diagnose(&ctx, t0());
    let d = find(&report, "SEQUENCE_MISSING");
    assert_eq!(d.category, DiagnosisCategory::DirectEvidence);
    assert_eq!(d.health, DiagnosisHealth::Inconsistent);
    assert_eq!(d.finding.severity, FindingSeverity::Critical);
    assert_eq!(d.finding.confidence, Confidence::Certain);
}

#[test]
fn missing_sequence_summary_names_the_sequence_id() {
    let mut ctx = full_ctx(instance(InstanceState::Scheduled));
    let seq = ctx.instance.sequence_id.into_uuid().to_string();
    ctx.sequence_exists = Some(false);
    let report = diagnose(&ctx, t0());
    let d = find(&report, "SEQUENCE_MISSING");
    assert!(d.finding.summary.contains(&seq), "{}", d.finding.summary);
}

#[test]
fn existing_sequence_produces_no_missing_diagnosis() {
    let report = diagnose(&full_ctx(instance(InstanceState::Scheduled)), t0());
    assert!(!codes(&report).contains(&"SEQUENCE_MISSING"));
}

#[test]
fn missing_sequence_on_terminal_instance_is_not_reported() {
    let mut ctx = full_ctx(instance(InstanceState::Completed));
    ctx.sequence_exists = Some(false);
    let report = diagnose(&ctx, t0());
    assert_eq!(codes(&report), vec!["TERMINAL_STATE"]);
}

// ===========================================================================
// EVIDENCE_INCOMPLETE (each uncollected section + combos)
// ===========================================================================

/// Assert EVIDENCE_INCOMPLETE mentions `expected` and none of the others.
fn assert_only_missing(ctx: &InstanceDiagnosticContext, expected: &str) {
    const ALL: &[&str] = &[
        "sequence",
        "signals",
        "worker_tasks",
        "worker_registrations",
        "circuit_breakers",
        "children",
        "approvals",
    ];
    let report = diagnose(ctx, t0());
    let d = find(&report, "EVIDENCE_INCOMPLETE");
    let inner = d
        .finding
        .summary
        .split('(')
        .nth(1)
        .and_then(|s| s.split(')').next())
        .unwrap_or_default();
    let listed: Vec<&str> = inner.split(", ").collect();
    assert_eq!(listed, vec![expected], "summary: {}", d.finding.summary);
    for section in ALL {
        if section != &expected {
            assert!(
                !listed.contains(section),
                "{section} listed unexpectedly: {}",
                d.finding.summary
            );
        }
    }
}

#[test]
fn uncollected_sequence_alone_is_reported() {
    let mut ctx = full_ctx(instance(InstanceState::Scheduled));
    ctx.sequence_exists = None;
    assert_only_missing(&ctx, "sequence");
}

#[test]
fn uncollected_signals_alone_is_reported() {
    let mut ctx = full_ctx(instance(InstanceState::Scheduled));
    ctx.pending_signals = None;
    assert_only_missing(&ctx, "signals");
}

#[test]
fn uncollected_worker_tasks_alone_is_reported() {
    let mut ctx = full_ctx(instance(InstanceState::Scheduled));
    ctx.worker_tasks = None;
    assert_only_missing(&ctx, "worker_tasks");
}

#[test]
fn uncollected_worker_registrations_alone_is_reported() {
    let mut ctx = full_ctx(instance(InstanceState::Scheduled));
    ctx.worker_registrations = None;
    assert_only_missing(&ctx, "worker_registrations");
}

#[test]
fn uncollected_circuit_breakers_alone_is_reported() {
    let mut ctx = full_ctx(instance(InstanceState::Scheduled));
    ctx.open_breakers = None;
    assert_only_missing(&ctx, "circuit_breakers");
}

#[test]
fn uncollected_children_alone_is_reported() {
    let mut ctx = full_ctx(instance(InstanceState::Scheduled));
    ctx.children = None;
    assert_only_missing(&ctx, "children");
}

#[test]
fn uncollected_approvals_alone_is_reported() {
    let mut ctx = full_ctx(instance(InstanceState::Scheduled));
    ctx.pending_approval_blocks = None;
    assert_only_missing(&ctx, "approvals");
}

#[test]
fn everything_uncollected_lists_all_seven_sections() {
    let report = diagnose(
        &InstanceDiagnosticContext::new(instance(InstanceState::Scheduled)),
        t0(),
    );
    let d = find(&report, "EVIDENCE_INCOMPLETE");
    for section in [
        "sequence",
        "signals",
        "worker_tasks",
        "worker_registrations",
        "circuit_breakers",
        "children",
        "approvals",
    ] {
        assert!(
            d.finding.summary.contains(section),
            "missing {section}: {}",
            d.finding.summary
        );
    }
}

#[test]
fn two_missing_sections_both_listed() {
    let mut ctx = full_ctx(instance(InstanceState::Scheduled));
    ctx.pending_signals = None;
    ctx.children = None;
    let report = diagnose(&ctx, t0());
    let d = find(&report, "EVIDENCE_INCOMPLETE");
    assert!(d.finding.summary.contains("signals, "));
    assert!(d.finding.summary.contains("children"));
    assert!(!d.finding.summary.contains("worker_tasks"));
}

#[test]
fn uncollected_version_pins_do_not_trigger_evidence_incomplete() {
    let mut ctx = full_ctx(instance(InstanceState::Scheduled));
    ctx.version_pins = None;
    let report = diagnose(&ctx, t0());
    assert!(!codes(&report).contains(&"EVIDENCE_INCOMPLETE"), "{:?}", codes(&report));
}

#[test]
fn uncollected_event_waits_do_not_trigger_evidence_incomplete() {
    let mut ctx = full_ctx(instance(InstanceState::Scheduled));
    ctx.event_waits = None;
    let report = diagnose(&ctx, t0());
    assert!(!codes(&report).contains(&"EVIDENCE_INCOMPLETE"), "{:?}", codes(&report));
}

#[test]
fn fully_collected_context_has_no_evidence_incomplete() {
    let report = diagnose(&full_ctx(instance(InstanceState::Waiting)), t0());
    assert!(!codes(&report).contains(&"EVIDENCE_INCOMPLETE"));
}

#[test]
fn evidence_incomplete_is_health_warning_expected_certain_info() {
    let mut ctx = full_ctx(instance(InstanceState::Scheduled));
    ctx.children = None;
    let report = diagnose(&ctx, t0());
    let d = find(&report, "EVIDENCE_INCOMPLETE");
    assert_eq!(d.category, DiagnosisCategory::HealthWarning);
    assert_eq!(d.health, DiagnosisHealth::Expected);
    assert_eq!(d.finding.confidence, Confidence::Certain);
    assert_eq!(d.finding.severity, FindingSeverity::Info);
}

// ===========================================================================
// NO_BLOCKER_FOUND
// ===========================================================================

#[test]
fn healthy_scheduled_instance_reports_no_blockers_only() {
    let report = diagnose(&full_ctx(instance(InstanceState::Scheduled)), t0());
    assert_eq!(codes(&report), vec!["NO_BLOCKER_FOUND"]);
    let d = &report.diagnoses[0];
    assert_eq!(d.category, DiagnosisCategory::HealthWarning);
    assert_eq!(d.health, DiagnosisHealth::Expected);
    assert_eq!(d.finding.confidence, Confidence::Low);
}

#[test]
fn healthy_running_instance_reports_no_blockers_only() {
    let report = diagnose(&full_ctx(instance(InstanceState::Running)), t0());
    assert_eq!(codes(&report), vec!["NO_BLOCKER_FOUND"]);
}

#[test]
fn just_due_scheduled_instance_reports_no_blockers() {
    let mut inst = instance(InstanceState::Scheduled);
    inst.next_fire_at = Some(t0() - Duration::seconds(1));
    let report = diagnose(&full_ctx(inst), t0());
    assert_eq!(codes(&report), vec!["NO_BLOCKER_FOUND"]);
}

#[test]
fn no_blocker_is_omitted_when_any_diagnosis_exists() {
    let mut inst = instance(InstanceState::Scheduled);
    inst.next_fire_at = Some(t0() + Duration::hours(1));
    let report = diagnose(&full_ctx(inst), t0());
    assert!(!codes(&report).contains(&"NO_BLOCKER_FOUND"));
}

#[test]
fn bare_context_reports_no_blocker_plus_incomplete_evidence() {
    // Rules that need evidence all degrade to nothing; EVIDENCE_INCOMPLETE
    // is added after NO_BLOCKER_FOUND and outranks it on confidence.
    let report = diagnose(
        &InstanceDiagnosticContext::new(instance(InstanceState::Scheduled)),
        t0(),
    );
    assert_eq!(codes(&report), vec!["EVIDENCE_INCOMPLETE", "NO_BLOCKER_FOUND"]);
}

// ===========================================================================
// Ranking invariants
// ===========================================================================

#[test]
fn category_order_is_direct_probable_health() {
    assert!(DiagnosisCategory::DirectEvidence < DiagnosisCategory::ProbableCause);
    assert!(DiagnosisCategory::ProbableCause < DiagnosisCategory::HealthWarning);
}

#[test]
fn rank_sorts_by_category_first() {
    let mut ds = vec![
        diag_of("H", DiagnosisCategory::HealthWarning, Confidence::Certain),
        diag_of("P", DiagnosisCategory::ProbableCause, Confidence::Certain),
        diag_of("D", DiagnosisCategory::DirectEvidence, Confidence::Low),
    ];
    rank_diagnoses(&mut ds);
    let cs: Vec<&str> = ds.iter().map(|d| d.finding.code.as_str()).collect();
    assert_eq!(cs, vec!["D", "P", "H"]);
}

#[test]
fn rank_breaks_category_ties_by_descending_confidence() {
    let mut ds = vec![
        diag_of("LOW", DiagnosisCategory::DirectEvidence, Confidence::Low),
        diag_of("CERTAIN", DiagnosisCategory::DirectEvidence, Confidence::Certain),
        diag_of("MEDIUM", DiagnosisCategory::DirectEvidence, Confidence::Medium),
        diag_of("HIGH", DiagnosisCategory::DirectEvidence, Confidence::High),
    ];
    rank_diagnoses(&mut ds);
    let cs: Vec<&str> = ds.iter().map(|d| d.finding.code.as_str()).collect();
    assert_eq!(cs, vec!["CERTAIN", "HIGH", "MEDIUM", "LOW"]);
}

#[test]
fn rank_breaks_confidence_ties_by_ascending_code() {
    let mut ds = vec![
        diag_of("ZULU", DiagnosisCategory::ProbableCause, Confidence::Medium),
        diag_of("ALPHA", DiagnosisCategory::ProbableCause, Confidence::Medium),
        diag_of("MIKE", DiagnosisCategory::ProbableCause, Confidence::Medium),
    ];
    rank_diagnoses(&mut ds);
    let cs: Vec<&str> = ds.iter().map(|d| d.finding.code.as_str()).collect();
    assert_eq!(cs, vec!["ALPHA", "MIKE", "ZULU"]);
}

#[test]
fn rank_is_invariant_under_input_permutation() {
    let base = vec![
        diag_of("A", DiagnosisCategory::DirectEvidence, Confidence::High),
        diag_of("B", DiagnosisCategory::DirectEvidence, Confidence::High),
        diag_of("C", DiagnosisCategory::ProbableCause, Confidence::Certain),
        diag_of("D", DiagnosisCategory::HealthWarning, Confidence::Low),
        diag_of("E", DiagnosisCategory::ProbableCause, Confidence::Low),
    ];
    let mut sorted = base.clone();
    rank_diagnoses(&mut sorted);
    let expect: Vec<String> = sorted.iter().map(|d| d.finding.code.clone()).collect();

    // A few fixed permutations must all converge to the same order.
    let permutations: Vec<Vec<usize>> = vec![
        vec![4, 3, 2, 1, 0],
        vec![2, 0, 4, 1, 3],
        vec![1, 4, 0, 3, 2],
    ];
    for perm in permutations {
        let mut shuffled: Vec<Diagnosis> = perm.iter().map(|&i| base[i].clone()).collect();
        rank_diagnoses(&mut shuffled);
        let got: Vec<String> = shuffled.iter().map(|d| d.finding.code.clone()).collect();
        assert_eq!(got, expect, "permutation {perm:?}");
    }
}

#[test]
fn rank_handles_empty_and_singleton_slices() {
    let mut empty: Vec<Diagnosis> = vec![];
    rank_diagnoses(&mut empty);
    assert!(empty.is_empty());

    let mut one = vec![diag_of("X", DiagnosisCategory::HealthWarning, Confidence::Low)];
    rank_diagnoses(&mut one);
    assert_eq!(one[0].finding.code, "X");
}

#[test]
fn report_ranks_direct_evidence_above_probable_cause() {
    // NO_COMPATIBLE_WORKER (direct/high) must beat SIGNALS_NOT_CONSUMED
    // (probable/medium).
    let mut ctx = full_ctx(instance(InstanceState::Waiting));
    ctx.worker_tasks = Some(vec![task(WorkerTaskState::Pending, "h", 10)]);
    ctx.pending_signals = Some(vec![signal(300)]);
    let report = diagnose(&ctx, t0());
    assert_eq!(report.diagnoses[0].finding.code, "NO_COMPATIBLE_WORKER");
    let pos_sig = codes(&report)
        .iter()
        .position(|c| *c == "SIGNALS_NOT_CONSUMED")
        .unwrap();
    assert!(pos_sig > 0);
}

#[test]
fn report_ranks_probable_cause_above_health_warning() {
    // WAITING_EXTERNAL_EVENT (probable) must beat an uninvolved
    // OPEN_CIRCUIT_BREAKER (health warning).
    let mut ctx = full_ctx(instance(InstanceState::Waiting));
    ctx.open_breakers = Some(vec![breaker("t1", "unrelated", BreakerState::Open)]);
    let report = diagnose(&ctx, t0());
    let cs = codes(&report);
    let pos_ext = cs.iter().position(|c| *c == "WAITING_EXTERNAL_EVENT").unwrap();
    let pos_brk = cs.iter().position(|c| *c == "OPEN_CIRCUIT_BREAKER").unwrap();
    assert!(pos_ext < pos_brk, "{cs:?}");
}

#[test]
fn certain_codes_within_direct_evidence_sort_alphabetically() {
    // PENDING_APPROVAL and SEQUENCE_MISSING are both Direct + Certain;
    // the code tiebreak puts PENDING_APPROVAL first.
    let mut ctx = full_ctx(instance(InstanceState::Waiting));
    ctx.sequence_exists = Some(false);
    ctx.pending_approval_blocks = Some(vec!["gate".into()]);
    let report = diagnose(&ctx, t0());
    let cs = codes(&report);
    assert_eq!(&cs[..2], &["PENDING_APPROVAL", "SEQUENCE_MISSING"], "{cs:?}");
}

#[test]
fn mega_context_produces_fully_deterministic_ranked_order() {
    let mut ctx = full_ctx(instance(InstanceState::Waiting));
    ctx.sequence_exists = Some(false); // Direct / Certain
    ctx.pending_approval_blocks = Some(vec!["gate".into()]); // Direct / Certain
    ctx.worker_tasks = Some(vec![task(WorkerTaskState::Claimed, "h", 900)]); // Direct / High
    ctx.pending_signals = Some(vec![signal(300)]); // Probable / Medium
    ctx.open_breakers = Some(vec![breaker("t1", "unrelated", BreakerState::Open)]); // Health / Low
    let report = diagnose(&ctx, t0());
    assert_eq!(
        codes(&report),
        vec![
            "PENDING_APPROVAL",
            "SEQUENCE_MISSING",
            "STALE_WORKER_CLAIM",
            "SIGNALS_NOT_CONSUMED",
            "OPEN_CIRCUIT_BREAKER",
        ]
    );
}

#[test]
fn diagnose_is_deterministic_for_a_fixed_clock() {
    let mut ctx = full_ctx(instance(InstanceState::Waiting));
    ctx.worker_tasks = Some(vec![task(WorkerTaskState::Claimed, "h", 900)]);
    ctx.pending_signals = Some(vec![signal(300)]);
    ctx.open_breakers = Some(vec![breaker("t1", "h", BreakerState::Open)]);
    let a = diagnose(&ctx, t0());
    let b = diagnose(&ctx, t0());
    assert_eq!(a, b);
}

#[test]
fn every_diagnosis_observed_at_matches_the_supplied_clock() {
    let mut ctx = full_ctx(instance(InstanceState::Waiting));
    ctx.worker_tasks = Some(vec![task(WorkerTaskState::Claimed, "h", 900)]);
    ctx.pending_signals = Some(vec![signal(300)]);
    let report = diagnose(&ctx, t0());
    assert!(!report.diagnoses.is_empty());
    for d in &report.diagnoses {
        assert_eq!(d.finding.observed_at, t0(), "{}", d.finding.code);
    }
}
