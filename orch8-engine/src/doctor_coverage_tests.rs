//! Coverage tests for the actionable execution doctor.
//!
//! Pins remediation-preview classification and state binding, `humanize`
//! boundaries, and diagnostic-rule edge semantics not covered by the inline
//! rule tests (tenant matching, breaker cooldown edges, signal staleness
//! thresholds, waiting-external suppression).
//!
//! Count contract: 31 independently named unit tests.

use chrono::TimeZone;
use orch8_types::context::ExecutionContext;
use orch8_types::ids::{BlockId, InstanceId, Namespace, SequenceId, TenantId};
use orch8_types::instance::Priority;
use serde_json::json;

use super::*;

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

fn task(state: WorkerTaskState, handler: &str, age_secs: i64) -> WorkerTask {
    WorkerTask {
        id: uuid::Uuid::now_v7(),
        instance_id: InstanceId::new(),
        block_id: BlockId::new("blk"),
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
        claim_epoch: 0,
        resume_checkpoint: None,
        checkpoint_seq: 0,
        completed_at: None,
        output: None,
        error_message: None,
        error_retryable: None,
        created_at: t0() - Duration::seconds(age_secs),
    }
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

fn breaker(
    handler: &str,
    state: BreakerState,
    cooldown_secs: u64,
    opened_at: Option<DateTime<Utc>>,
) -> CircuitBreakerState {
    CircuitBreakerState {
        tenant_id: TenantId::unchecked("t1"),
        handler: handler.into(),
        state,
        failure_count: 5,
        failure_threshold: 5,
        cooldown_secs,
        opened_at,
    }
}

fn codes(report: &InstanceDiagnosisReport) -> Vec<&str> {
    report
        .diagnoses
        .iter()
        .map(|d| d.finding.code.as_str())
        .collect()
}

#[test]
fn coverage_doctor_001_paused_instance_classifies_resume_action() {
    let report = diagnose(&full_ctx(instance(InstanceState::Paused)), t0());
    let previews = remediation_previews(&report);
    assert_eq!(previews.len(), 1);
    assert_eq!(previews[0].action, RemediationAction::ResumeInstance);
    assert_eq!(previews[0].finding_code, "PAUSED");
    assert_eq!(previews[0].expected_state, "paused");
    assert!(!previews[0].side_effect_risk);
}

#[test]
fn coverage_doctor_002_budget_pause_also_classifies_resume_action() {
    let mut inst = instance(InstanceState::Paused);
    inst.metadata = json!({"paused_reason": "budget_exceeded"});
    let report = diagnose(&full_ctx(inst), t0());
    let previews = remediation_previews(&report);
    assert_eq!(previews[0].action, RemediationAction::ResumeInstance);
    assert_eq!(previews[0].finding_code, "BUDGET_PAUSED");
}

#[test]
fn coverage_doctor_003_remediation_without_command_is_manual() {
    // WORKER_BELOW_VERSION_PIN carries a remediation with no command.
    let mut ctx = full_ctx(instance(InstanceState::Waiting));
    ctx.worker_tasks = Some(vec![task(WorkerTaskState::Pending, "charge_card", 10)]);
    ctx.worker_registrations = Some(vec![registration("charge_card", Some("1.0.0"))]);
    ctx.version_pins = Some(vec![WorkerVersionPin {
        tenant_id: "t1".into(),
        handler_name: "charge_card".into(),
        min_version: "2.0.0".into(),
        created_at: t0(),
        updated_at: t0(),
    }]);
    let report = diagnose(&ctx, t0());
    let previews = remediation_previews(&report);
    let pin = previews
        .iter()
        .find(|p| p.finding_code == "WORKER_BELOW_VERSION_PIN")
        .expect("pin preview");
    assert_eq!(pin.action, RemediationAction::Manual);
    assert!(pin.command.is_none());
}

#[test]
fn coverage_doctor_004_unrecognized_command_is_manual() {
    // PENDING_APPROVAL's command embeds a block-specific signal name, which
    // is not one of the two classified actions.
    let mut ctx = full_ctx(instance(InstanceState::Waiting));
    ctx.pending_approval_blocks = Some(vec!["approve_payment".into()]);
    let report = diagnose(&ctx, t0());
    let previews = remediation_previews(&report);
    assert_eq!(previews.len(), 1);
    assert_eq!(previews[0].action, RemediationAction::Manual);
    assert!(
        previews[0]
            .command
            .as_deref()
            .unwrap()
            .contains("custom:human_input:approve_payment")
    );
}

#[test]
fn coverage_doctor_005_preview_id_binds_instance_state_code_and_indices() {
    let report = diagnose(&full_ctx(instance(InstanceState::Failed)), t0());
    let previews = remediation_previews(&report);
    let expected = format!("{}:failed:TERMINAL_STATE:0:0", report.instance_id);
    assert_eq!(previews[0].preview_id, expected);
    assert_eq!(previews[0].remediation_index, 0);
}

#[test]
fn coverage_doctor_006_preview_indices_follow_diagnosis_ranking() {
    // Two diagnoses with one remediation each: stale claim and a pending
    // approval. Preview order must follow the ranked diagnosis order and
    // each preview id must embed its diagnosis/remediation indices.
    let mut ctx = full_ctx(instance(InstanceState::Waiting));
    ctx.worker_tasks = Some(vec![task(WorkerTaskState::Claimed, "claimed_handler", 900)]);
    ctx.pending_approval_blocks = Some(vec!["gate".into()]);
    let report = diagnose(&ctx, t0());
    assert_eq!(report.diagnoses.len(), 2);
    let previews = remediation_previews(&report);
    assert_eq!(previews.len(), 2);
    for (diagnosis_index, diagnosis) in report.diagnoses.iter().enumerate() {
        assert_eq!(
            previews[diagnosis_index].finding_code,
            diagnosis.finding.code
        );
        assert!(
            previews[diagnosis_index]
                .preview_id
                .ends_with(&format!(":{diagnosis_index}:0"))
        );
    }
    // Side-effect risk propagates per finding, independent of ranking.
    let claim = previews
        .iter()
        .find(|p| p.finding_code == "STALE_WORKER_CLAIM")
        .unwrap();
    assert!(claim.side_effect_risk);
    let approval = previews
        .iter()
        .find(|p| p.finding_code == "PENDING_APPROVAL")
        .unwrap();
    assert!(!approval.side_effect_risk);
}

#[test]
fn coverage_doctor_007_diagnosis_without_remediation_yields_no_preview() {
    let report = diagnose(&full_ctx(instance(InstanceState::Waiting)), t0());
    assert!(codes(&report).contains(&"WAITING_EXTERNAL_EVENT"));
    // WAITING_EXTERNAL_EVENT has a remediation; NO_BLOCKER_FOUND does not.
    let mut inst = instance(InstanceState::Scheduled);
    inst.next_fire_at = Some(t0() - Duration::seconds(1));
    let clean = diagnose(&full_ctx(inst), t0());
    assert_eq!(codes(&clean), vec!["NO_BLOCKER_FOUND"]);
    assert!(remediation_previews(&clean).is_empty());
}

#[test]
fn coverage_doctor_008_preview_copies_summary_and_command() {
    let report = diagnose(&full_ctx(instance(InstanceState::Failed)), t0());
    let previews = remediation_previews(&report);
    let remediation = &report.diagnoses[0].finding.remediation[0];
    assert_eq!(previews[0].summary, remediation.summary);
    assert_eq!(previews[0].command, remediation.command);
}

#[test]
fn coverage_doctor_009_cancelled_terminal_state_has_no_retry_remediation() {
    let report = diagnose(&full_ctx(instance(InstanceState::Cancelled)), t0());
    assert_eq!(codes(&report), vec!["TERMINAL_STATE"]);
    assert!(report.diagnoses[0].finding.remediation.is_empty());
    assert!(remediation_previews(&report).is_empty());
}

#[test]
fn coverage_doctor_010_pending_task_without_registrations_degrades_confidence() {
    let mut ctx = full_ctx(instance(InstanceState::Waiting));
    ctx.worker_tasks = Some(vec![task(WorkerTaskState::Pending, "charge_card", 10)]);
    ctx.worker_registrations = None;
    let report = diagnose(&ctx, t0());
    let d = report
        .diagnoses
        .iter()
        .find(|d| d.finding.code == "WORKER_TASK_PENDING")
        .expect("pending diagnosis");
    assert_eq!(d.category, DiagnosisCategory::ProbableCause);
    assert_eq!(d.health, DiagnosisHealth::Degraded);
    assert_eq!(d.finding.confidence, Confidence::Low);
}

#[test]
fn coverage_doctor_011_empty_tenant_registration_matches_any_tenant() {
    let mut ctx = full_ctx(instance(InstanceState::Waiting));
    ctx.worker_tasks = Some(vec![task(WorkerTaskState::Pending, "h", 10)]);
    let mut reg = registration("h", None);
    reg.tenant_id = Some(String::new());
    ctx.worker_registrations = Some(vec![reg]);
    let report = diagnose(&ctx, t0());
    assert!(codes(&report).contains(&"WAITING_WORKER_PICKUP"));
}

#[test]
fn coverage_doctor_012_foreign_tenant_registration_does_not_match() {
    let mut ctx = full_ctx(instance(InstanceState::Waiting));
    ctx.worker_tasks = Some(vec![task(WorkerTaskState::Pending, "h", 10)]);
    let mut reg = registration("h", None);
    reg.tenant_id = Some("tenant-z".into());
    ctx.worker_registrations = Some(vec![reg]);
    let report = diagnose(&ctx, t0());
    assert!(codes(&report).contains(&"NO_COMPATIBLE_WORKER"));
}

#[test]
fn coverage_doctor_013_same_tenant_registration_matches() {
    let mut ctx = full_ctx(instance(InstanceState::Waiting));
    ctx.worker_tasks = Some(vec![task(WorkerTaskState::Pending, "h", 10)]);
    let mut reg = registration("h", None);
    reg.tenant_id = Some("t1".into());
    ctx.worker_registrations = Some(vec![reg]);
    let report = diagnose(&ctx, t0());
    assert!(codes(&report).contains(&"WAITING_WORKER_PICKUP"));
}

#[test]
fn coverage_doctor_014_satisfied_version_pin_allows_pickup() {
    let mut ctx = full_ctx(instance(InstanceState::Waiting));
    ctx.worker_tasks = Some(vec![task(WorkerTaskState::Pending, "h", 10)]);
    ctx.worker_registrations = Some(vec![registration("h", Some("2.1.0"))]);
    ctx.version_pins = Some(vec![WorkerVersionPin {
        tenant_id: "t1".into(),
        handler_name: "h".into(),
        min_version: "2.0.0".into(),
        created_at: t0(),
        updated_at: t0(),
    }]);
    let report = diagnose(&ctx, t0());
    assert!(!codes(&report).contains(&"WORKER_BELOW_VERSION_PIN"));
    assert!(codes(&report).contains(&"WAITING_WORKER_PICKUP"));
}

#[test]
fn coverage_doctor_015_pin_for_other_tenant_is_ignored() {
    let mut ctx = full_ctx(instance(InstanceState::Waiting));
    ctx.worker_tasks = Some(vec![task(WorkerTaskState::Pending, "h", 10)]);
    ctx.worker_registrations = Some(vec![registration("h", Some("1.0.0"))]);
    ctx.version_pins = Some(vec![WorkerVersionPin {
        tenant_id: "tenant-z".into(),
        handler_name: "h".into(),
        min_version: "9.9.9".into(),
        created_at: t0(),
        updated_at: t0(),
    }]);
    let report = diagnose(&ctx, t0());
    assert!(!codes(&report).contains(&"WORKER_BELOW_VERSION_PIN"));
}

#[test]
fn coverage_doctor_016_breaker_without_opened_at_omits_cooldown_hint() {
    let mut ctx = full_ctx(instance(InstanceState::Waiting));
    ctx.open_breakers = Some(vec![breaker("flaky", BreakerState::Open, 300, None)]);
    let report = diagnose(&ctx, t0());
    let d = report
        .diagnoses
        .iter()
        .find(|d| d.finding.code == "OPEN_CIRCUIT_BREAKER")
        .expect("breaker diagnosis");
    assert!(!d.finding.summary.contains("resume in"));
    assert!(d.finding.evidence.is_empty());
}

#[test]
fn coverage_doctor_017_elapsed_cooldown_omits_resume_hint() {
    let mut ctx = full_ctx(instance(InstanceState::Waiting));
    ctx.open_breakers = Some(vec![breaker(
        "flaky",
        BreakerState::Open,
        60,
        Some(t0() - Duration::seconds(120)),
    )]);
    let report = diagnose(&ctx, t0());
    let d = report
        .diagnoses
        .iter()
        .find(|d| d.finding.code == "OPEN_CIRCUIT_BREAKER")
        .expect("breaker diagnosis");
    assert!(!d.finding.summary.contains("resume in"));
}

#[test]
fn coverage_doctor_018_clock_skewed_breaker_does_not_panic() {
    let mut ctx = full_ctx(instance(InstanceState::Waiting));
    ctx.open_breakers = Some(vec![breaker(
        "flaky",
        BreakerState::Open,
        300,
        Some(t0() + Duration::seconds(60)),
    )]);
    let report = diagnose(&ctx, t0());
    let d = report
        .diagnoses
        .iter()
        .find(|d| d.finding.code == "OPEN_CIRCUIT_BREAKER")
        .expect("breaker diagnosis");
    assert!(d.finding.summary.contains("resume in"));
}

#[test]
fn coverage_doctor_019_half_open_breaker_is_not_reported_open() {
    let mut ctx = full_ctx(instance(InstanceState::Waiting));
    ctx.open_breakers = Some(vec![breaker(
        "flaky",
        BreakerState::HalfOpen,
        300,
        Some(t0()),
    )]);
    let report = diagnose(&ctx, t0());
    assert!(!codes(&report).contains(&"OPEN_CIRCUIT_BREAKER"));
}

#[test]
fn coverage_doctor_020_uninvolved_breaker_is_low_confidence_health_warning() {
    let mut ctx = full_ctx(instance(InstanceState::Waiting));
    ctx.open_breakers = Some(vec![breaker(
        "unrelated",
        BreakerState::Open,
        300,
        Some(t0()),
    )]);
    let report = diagnose(&ctx, t0());
    let d = report
        .diagnoses
        .iter()
        .find(|d| d.finding.code == "OPEN_CIRCUIT_BREAKER")
        .expect("breaker diagnosis");
    assert_eq!(d.category, DiagnosisCategory::HealthWarning);
    assert_eq!(d.finding.confidence, Confidence::Low);
}

#[test]
fn coverage_doctor_021_fresh_signal_is_not_flagged_stale() {
    let mut ctx = full_ctx(instance(InstanceState::Waiting));
    ctx.pending_signals = Some(vec![Signal {
        id: uuid::Uuid::now_v7(),
        instance_id: ctx.instance.id,
        signal_type: orch8_types::signal::SignalType::Resume,
        payload: json!({}),
        delivered: false,
        created_at: t0() - Duration::seconds(30),
        delivered_at: None,
    }]);
    let report = diagnose(&ctx, t0());
    assert!(!codes(&report).contains(&"SIGNALS_NOT_CONSUMED"));
}

#[test]
fn coverage_doctor_022_signal_older_than_sixty_seconds_is_flagged() {
    let mut ctx = full_ctx(instance(InstanceState::Waiting));
    ctx.pending_signals = Some(vec![Signal {
        id: uuid::Uuid::now_v7(),
        instance_id: ctx.instance.id,
        signal_type: orch8_types::signal::SignalType::Resume,
        payload: json!({}),
        delivered: false,
        created_at: t0() - Duration::seconds(61),
        delivered_at: None,
    }]);
    let report = diagnose(&ctx, t0());
    assert!(codes(&report).contains(&"SIGNALS_NOT_CONSUMED"));
}

#[test]
fn coverage_doctor_023_running_with_live_children_is_not_waiting_child() {
    let mut ctx = full_ctx(instance(InstanceState::Running));
    ctx.children = Some(vec![instance(InstanceState::Running)]);
    let report = diagnose(&ctx, t0());
    assert!(!codes(&report).contains(&"WAITING_CHILD"));
    assert!(!codes(&report).contains(&"CHILDREN_DONE_PARENT_WAITING"));
}

#[test]
fn coverage_doctor_024_future_timer_suppresses_waiting_external() {
    let mut inst = instance(InstanceState::Waiting);
    inst.next_fire_at = Some(t0() + Duration::minutes(5));
    let report = diagnose(&full_ctx(inst), t0());
    assert!(codes(&report).contains(&"WAITING_UNTIL"));
    assert!(!codes(&report).contains(&"WAITING_EXTERNAL_EVENT"));
}

#[test]
fn coverage_doctor_025_pending_approval_suppresses_waiting_external() {
    let mut ctx = full_ctx(instance(InstanceState::Waiting));
    ctx.pending_approval_blocks = Some(vec!["gate".into()]);
    let report = diagnose(&ctx, t0());
    assert!(!codes(&report).contains(&"WAITING_EXTERNAL_EVENT"));
}

#[test]
fn coverage_doctor_026_open_worker_task_suppresses_waiting_external() {
    let mut ctx = full_ctx(instance(InstanceState::Waiting));
    ctx.worker_tasks = Some(vec![task(WorkerTaskState::Claimed, "h", 10)]);
    let report = diagnose(&ctx, t0());
    assert!(!codes(&report).contains(&"WAITING_EXTERNAL_EVENT"));
}

#[test]
fn coverage_doctor_027_evidence_incomplete_lists_each_missing_section() {
    let inst = instance(InstanceState::Waiting);
    let report = diagnose(&InstanceDiagnosticContext::new(inst), t0());
    let d = report
        .diagnoses
        .iter()
        .find(|d| d.finding.code == "EVIDENCE_INCOMPLETE")
        .expect("evidence warning");
    for section in [
        "sequence",
        "signals",
        "worker_tasks",
        "worker_registrations",
        "circuit_breakers",
        "children",
        "approvals",
    ] {
        assert!(d.finding.summary.contains(section), "{section}");
    }
}

#[test]
fn coverage_doctor_028_fully_collected_evidence_has_no_incomplete_warning() {
    let report = diagnose(&full_ctx(instance(InstanceState::Waiting)), t0());
    assert!(!codes(&report).contains(&"EVIDENCE_INCOMPLETE"));
}

macro_rules! humanize_case {
    ($name:ident, $secs:expr, $expected:expr) => {
        #[test]
        fn $name() {
            assert_eq!(humanize(Duration::seconds($secs)), $expected);
        }
    };
}

humanize_case!(coverage_doctor_029_humanize_minute_boundaries, 60, "1m 0s");
humanize_case!(coverage_doctor_030_humanize_day_boundary, 86_400, "1d 0h");

#[test]
fn coverage_doctor_031_signal_exactly_sixty_seconds_old_is_not_flagged() {
    // The staleness rule is a strict `age > 60s` comparison; exactly 60s is
    // still fresh.
    let mut ctx = full_ctx(instance(InstanceState::Waiting));
    ctx.pending_signals = Some(vec![Signal {
        id: uuid::Uuid::now_v7(),
        instance_id: ctx.instance.id,
        signal_type: orch8_types::signal::SignalType::Resume,
        payload: json!({}),
        delivered: false,
        created_at: t0() - Duration::seconds(60),
        delivered_at: None,
    }]);
    let report = diagnose(&ctx, t0());
    assert!(!codes(&report).contains(&"SIGNALS_NOT_CONSUMED"));
}
