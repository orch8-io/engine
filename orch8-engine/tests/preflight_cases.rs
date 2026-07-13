//! Exhaustive unit tests for sequence preflight.
//!
//! Covers every check id × every reachable status, builtin handler
//! coverage, worker tenant scoping, version-pin satisfaction, credential
//! expiry boundaries, nested composite reference collection, queue
//! consumer resolution, sub-sequence status matrices, and report
//! aggregation semantics.

use chrono::{DateTime, Duration, TimeZone, Utc};
use serde_json::{Value, json};

use orch8_engine::preflight::{
    CredentialInfo, PluginInfo, RuntimeInventory, SubSequenceInfo, run_preflight,
};
use orch8_types::preflight::{PreflightCheck, PreflightReport, PreflightStatus};
use orch8_types::queue_dispatch::{DispatchMode, QueueDispatchConfig};
use orch8_types::queue_routing::QueueRoutingRule;
use orch8_types::sequence::{BUILTIN_HANDLER_NAMES, SequenceDefinition, SequenceStatus};
use orch8_types::worker::{WorkerRegistration, WorkerVersionPin, version_satisfies};

// ---------------------------------------------------------------------------
// helpers
// ---------------------------------------------------------------------------

fn t0() -> DateTime<Utc> {
    Utc.with_ymd_and_hms(2026, 7, 11, 0, 0, 0).unwrap()
}

#[allow(clippy::needless_pass_by_value)]
fn seq_in(tenant: &str, namespace: &str, blocks: Value) -> SequenceDefinition {
    serde_json::from_value(json!({
        "id": uuid::Uuid::now_v7(),
        "tenant_id": tenant,
        "namespace": namespace,
        "name": "case-seq",
        "version": 1,
        "blocks": blocks,
        "created_at": "2026-01-01T00:00:00Z"
    }))
    .expect("valid sequence definition")
}

fn seq(blocks: Value) -> SequenceDefinition {
    seq_in("t1", "default", blocks)
}

fn noop_step(id: &str) -> Value {
    json!({"type": "step", "id": id, "handler": "noop", "params": {}})
}

fn step(id: &str, handler: &str) -> Value {
    json!({"type": "step", "id": id, "handler": handler, "params": {}})
}

fn full_inventory() -> RuntimeInventory {
    RuntimeInventory {
        worker_registrations: Some(vec![]),
        version_pins: Some(vec![]),
        credentials: Some(vec![]),
        plugins: Some(vec![]),
        queue_dispatch: Some(vec![]),
        routing_rules: Some(vec![]),
        sequences: Some(vec![]),
    }
}

fn registration(handler: &str, queue: Option<&str>, version: Option<&str>) -> WorkerRegistration {
    WorkerRegistration {
        worker_id: format!("w-{handler}"),
        handler_name: handler.to_string(),
        queue_name: queue.map(ToString::to_string),
        version: version.map(ToString::to_string),
        tenant_id: None,
        last_seen_at: t0(),
    }
}

fn pin(tenant: &str, handler: &str, min_version: &str) -> WorkerVersionPin {
    WorkerVersionPin {
        tenant_id: tenant.to_string(),
        handler_name: handler.to_string(),
        min_version: min_version.to_string(),
        created_at: t0(),
        updated_at: t0(),
    }
}

fn cred(id: &str, enabled: bool, expires_at: Option<DateTime<Utc>>) -> CredentialInfo {
    CredentialInfo {
        id: id.to_string(),
        enabled,
        expires_at,
    }
}

fn sub(name: &str, namespace: &str, version: i32, status: SequenceStatus) -> SubSequenceInfo {
    SubSequenceInfo {
        name: name.to_string(),
        namespace: namespace.to_string(),
        version,
        status,
    }
}

fn push_dispatch(tenant: &str, queue: &str) -> QueueDispatchConfig {
    QueueDispatchConfig {
        tenant_id: tenant.to_string(),
        queue_name: queue.to_string(),
        mode: DispatchMode::Push,
        push_url: Some("https://worker.example.com/tasks".to_string()),
        secret: None,
        created_at: t0(),
        updated_at: t0(),
    }
}

fn poll_dispatch(tenant: &str, queue: &str) -> QueueDispatchConfig {
    QueueDispatchConfig {
        tenant_id: tenant.to_string(),
        queue_name: queue.to_string(),
        mode: DispatchMode::Poll,
        push_url: None,
        secret: None,
        created_at: t0(),
        updated_at: t0(),
    }
}

fn redirect_rule(match_queue: Option<&str>, enabled: bool) -> QueueRoutingRule {
    QueueRoutingRule {
        id: uuid::Uuid::now_v7(),
        tenant_id: "t1".to_string(),
        handler_name: "anything".to_string(),
        match_queue: match_queue.map(ToString::to_string),
        queue_override: "elsewhere".to_string(),
        priority: 0,
        enabled,
        created_at: t0(),
        updated_at: t0(),
    }
}

fn check<'a>(report: &'a PreflightReport, id: &str) -> &'a PreflightCheck {
    report
        .checks
        .iter()
        .find(|c| c.id == id)
        .unwrap_or_else(|| panic!("missing check {id} in {report:?}"))
}

fn codes(c: &PreflightCheck) -> Vec<&str> {
    c.findings.iter().map(|f| f.code.as_str()).collect()
}

// ---------------------------------------------------------------------------
// definition_valid
// ---------------------------------------------------------------------------

#[test]
fn definition_valid_passes_for_simple_sequence() {
    let report = run_preflight(&seq(json!([noop_step("a")])), &full_inventory(), t0());
    let c = check(&report, "definition_valid");
    assert_eq!(c.status, PreflightStatus::Pass);
    assert!(c.findings.is_empty());
    assert!(c.summary.contains("validates"));
}

#[test]
fn definition_with_no_blocks_fails() {
    let report = run_preflight(&seq(json!([])), &full_inventory(), t0());
    let c = check(&report, "definition_valid");
    assert_eq!(c.status, PreflightStatus::Fail);
    assert_eq!(codes(c), vec!["INVALID_DEFINITION"]);
    assert!(c.findings[0].summary.contains("no blocks"));
}

#[test]
fn duplicate_block_ids_fail_definition_check() {
    let report = run_preflight(
        &seq(json!([noop_step("dup"), noop_step("dup")])),
        &full_inventory(),
        t0(),
    );
    let c = check(&report, "definition_valid");
    assert_eq!(c.status, PreflightStatus::Fail);
    assert_eq!(c.findings[0].code, "INVALID_DEFINITION");
    assert!(c.findings[0].summary.contains("duplicate block id"));
    assert!(c.findings[0].summary.contains("dup"));
}

#[test]
fn nested_duplicate_block_id_fails_definition_check() {
    let report = run_preflight(
        &seq(json!([
            noop_step("a"),
            {"type": "parallel", "id": "p", "branches": [[noop_step("a")]]}
        ])),
        &full_inventory(),
        t0(),
    );
    assert_eq!(
        check(&report, "definition_valid").status,
        PreflightStatus::Fail
    );
}

#[test]
fn invalid_definition_makes_overall_fail() {
    let report = run_preflight(&seq(json!([])), &full_inventory(), t0());
    assert_eq!(report.overall, PreflightStatus::Fail);
    assert!(!report.is_ready());
}

#[test]
fn definition_failure_finding_observed_at_matches_now() {
    let report = run_preflight(&seq(json!([])), &full_inventory(), t0());
    let c = check(&report, "definition_valid");
    assert_eq!(c.findings[0].observed_at, t0());
}

// ---------------------------------------------------------------------------
// lint_clean
// ---------------------------------------------------------------------------

#[test]
fn lint_clean_for_wellformed_noop() {
    let report = run_preflight(&seq(json!([noop_step("a")])), &full_inventory(), t0());
    let c = check(&report, "lint_clean");
    assert_eq!(c.status, PreflightStatus::Pass);
    assert!(c.findings.is_empty());
}

#[test]
fn http_request_missing_url_is_lint_warning() {
    let report = run_preflight(
        &seq(json!([step("fetch", "http_request")])),
        &full_inventory(),
        t0(),
    );
    let c = check(&report, "lint_clean");
    assert_eq!(c.status, PreflightStatus::Warning);
    assert_eq!(c.findings[0].code, "LINT_WARNING");
    let res = c.findings[0].affected_resource.as_ref().unwrap();
    assert_eq!(res.kind, "block");
    assert_eq!(res.id, "fetch");
}

#[test]
fn single_branch_race_is_lint_warning() {
    let report = run_preflight(
        &seq(json!([
            {"type": "race", "id": "r", "branches": [[noop_step("a")]]}
        ])),
        &full_inventory(),
        t0(),
    );
    let c = check(&report, "lint_clean");
    assert_eq!(c.status, PreflightStatus::Warning);
    assert_eq!(c.findings[0].code, "LINT_WARNING");
}

#[test]
fn empty_catch_block_is_lint_warning() {
    let report = run_preflight(
        &seq(json!([
            {"type": "try_catch", "id": "tc", "try_block": [noop_step("a")], "catch_block": []}
        ])),
        &full_inventory(),
        t0(),
    );
    assert_eq!(
        check(&report, "lint_clean").status,
        PreflightStatus::Warning
    );
}

#[test]
fn multiple_lint_warnings_accumulate_into_one_check() {
    let report = run_preflight(
        &seq(json!([
            {"type": "race", "id": "r", "branches": [[noop_step("a")]]},
            {"type": "try_catch", "id": "tc", "try_block": [noop_step("b")], "catch_block": []}
        ])),
        &full_inventory(),
        t0(),
    );
    let c = check(&report, "lint_clean");
    assert_eq!(c.status, PreflightStatus::Warning);
    assert!(c.findings.len() >= 2, "{c:?}");
    assert!(c.summary.contains("lint warning"));
}

#[test]
fn lint_warning_alone_leaves_report_ready_with_warning_overall() {
    let report = run_preflight(
        &seq(json!([
            {"type": "race", "id": "r", "branches": [[noop_step("a")]]}
        ])),
        &full_inventory(),
        t0(),
    );
    assert_eq!(report.overall, PreflightStatus::Warning);
    assert!(report.is_ready());
}

// ---------------------------------------------------------------------------
// input_schema_valid (structural, engine-level)
// ---------------------------------------------------------------------------

#[test]
fn missing_input_schema_passes() {
    let report = run_preflight(&seq(json!([noop_step("a")])), &full_inventory(), t0());
    let c = check(&report, "input_schema_valid");
    assert_eq!(c.status, PreflightStatus::Pass);
    assert!(c.summary.contains("no input schema"));
}

#[test]
fn object_input_schema_passes_structurally() {
    let mut s = seq(json!([noop_step("a")]));
    s.input_schema = Some(json!({"type": "object"}));
    let report = run_preflight(&s, &full_inventory(), t0());
    assert_eq!(
        check(&report, "input_schema_valid").status,
        PreflightStatus::Pass
    );
}

#[test]
fn engine_structural_check_accepts_semantically_bad_object_schema() {
    // The engine's check is structural only — deep JSON Schema compilation
    // is the API layer's job. An object with a bogus "type" still passes here.
    let mut s = seq(json!([noop_step("a")]));
    s.input_schema = Some(json!({"type": "definitely_not_a_type"}));
    let report = run_preflight(&s, &full_inventory(), t0());
    assert_eq!(
        check(&report, "input_schema_valid").status,
        PreflightStatus::Pass
    );
}

#[test]
fn string_input_schema_fails_and_names_the_type() {
    let mut s = seq(json!([noop_step("a")]));
    s.input_schema = Some(json!("nope"));
    let report = run_preflight(&s, &full_inventory(), t0());
    let c = check(&report, "input_schema_valid");
    assert_eq!(c.status, PreflightStatus::Fail);
    assert_eq!(c.findings[0].code, "INVALID_INPUT_SCHEMA");
    assert!(c.findings[0].summary.contains("string"));
}

#[test]
fn array_input_schema_fails_and_names_the_type() {
    let mut s = seq(json!([noop_step("a")]));
    s.input_schema = Some(json!([1, 2]));
    let report = run_preflight(&s, &full_inventory(), t0());
    let c = check(&report, "input_schema_valid");
    assert_eq!(c.status, PreflightStatus::Fail);
    assert!(c.findings[0].summary.contains("array"));
}

#[test]
fn number_input_schema_fails_and_names_the_type() {
    let mut s = seq(json!([noop_step("a")]));
    s.input_schema = Some(json!(42));
    let report = run_preflight(&s, &full_inventory(), t0());
    assert!(
        check(&report, "input_schema_valid").findings[0]
            .summary
            .contains("number")
    );
}

#[test]
fn boolean_input_schema_fails_and_names_the_type() {
    let mut s = seq(json!([noop_step("a")]));
    s.input_schema = Some(json!(true));
    let report = run_preflight(&s, &full_inventory(), t0());
    let c = check(&report, "input_schema_valid");
    assert_eq!(c.status, PreflightStatus::Fail);
    assert!(c.findings[0].summary.contains("boolean"));
}

#[test]
fn null_input_schema_fails_and_names_the_type() {
    let mut s = seq(json!([noop_step("a")]));
    s.input_schema = Some(Value::Null);
    let report = run_preflight(&s, &full_inventory(), t0());
    let c = check(&report, "input_schema_valid");
    assert_eq!(c.status, PreflightStatus::Fail);
    assert!(c.findings[0].summary.contains("null"));
}

// ---------------------------------------------------------------------------
// handlers_have_workers: builtins
// ---------------------------------------------------------------------------

#[test]
fn every_builtin_handler_passes_without_any_worker() {
    // Each builtin gets its own single-step sequence so one failure names
    // the culprit precisely.
    for handler in BUILTIN_HANDLER_NAMES {
        let report = run_preflight(
            &seq(json!([step("only", handler)])),
            &full_inventory(),
            t0(),
        );
        assert_eq!(
            check(&report, "handlers_have_workers").status,
            PreflightStatus::Pass,
            "builtin '{handler}' should not require a worker"
        );
    }
}

#[test]
fn sequence_mixing_all_builtins_passes_handler_check() {
    let blocks: Vec<Value> = BUILTIN_HANDLER_NAMES
        .iter()
        .enumerate()
        .map(|(i, h)| step(&format!("b{i}"), h))
        .collect();
    let report = run_preflight(&seq(json!(blocks)), &full_inventory(), t0());
    assert_eq!(
        check(&report, "handlers_have_workers").status,
        PreflightStatus::Pass
    );
}

#[test]
fn builtin_handler_list_is_nontrivial() {
    assert!(BUILTIN_HANDLER_NAMES.len() >= 20);
    assert!(BUILTIN_HANDLER_NAMES.contains(&"noop"));
    assert!(BUILTIN_HANDLER_NAMES.contains(&"transform"));
}

// ---------------------------------------------------------------------------
// handlers_have_workers: external handlers and workers
// ---------------------------------------------------------------------------

#[test]
fn external_handler_without_worker_fails_with_full_finding_shape() {
    let report = run_preflight(
        &seq(json!([step("pay", "charge_card")])),
        &full_inventory(),
        t0(),
    );
    let c = check(&report, "handlers_have_workers");
    assert_eq!(c.status, PreflightStatus::Fail);
    let f = &c.findings[0];
    assert_eq!(f.code, "NO_COMPATIBLE_WORKER");
    assert!(f.summary.contains("charge_card"));
    assert!(f.summary.contains("pay"));
    let res = f.affected_resource.as_ref().unwrap();
    assert_eq!(res.kind, "handler");
    assert_eq!(res.id, "charge_card");
    assert!(!f.remediation.is_empty());
    assert!(f.remediation[0].summary.contains("worker"));
}

#[test]
fn one_missing_handler_used_by_two_blocks_yields_one_finding_naming_both() {
    let report = run_preflight(
        &seq(json!([
            step("first", "custom_x"),
            step("second", "custom_x")
        ])),
        &full_inventory(),
        t0(),
    );
    let c = check(&report, "handlers_have_workers");
    assert_eq!(c.findings.len(), 1);
    assert!(c.findings[0].summary.contains("first"));
    assert!(c.findings[0].summary.contains("second"));
}

#[test]
fn two_missing_handlers_yield_two_findings() {
    let report = run_preflight(
        &seq(json!([step("a", "custom_a"), step("b", "custom_b")])),
        &full_inventory(),
        t0(),
    );
    let c = check(&report, "handlers_have_workers");
    assert_eq!(c.findings.len(), 2);
    assert!(c.summary.contains("2 handler(s)"));
}

#[test]
fn live_worker_flips_handler_check_to_pass() {
    let mut inv = full_inventory();
    inv.worker_registrations = Some(vec![registration("custom_a", None, Some("1.0.0"))]);
    let report = run_preflight(&seq(json!([step("a", "custom_a")])), &inv, t0());
    assert_eq!(
        check(&report, "handlers_have_workers").status,
        PreflightStatus::Pass
    );
}

#[test]
fn worker_for_a_different_handler_does_not_help() {
    let mut inv = full_inventory();
    inv.worker_registrations = Some(vec![registration("custom_b", None, None)]);
    let report = run_preflight(&seq(json!([step("a", "custom_a")])), &inv, t0());
    assert_eq!(
        check(&report, "handlers_have_workers").status,
        PreflightStatus::Fail
    );
}

#[test]
fn missing_worker_inventory_yields_unknown() {
    let mut inv = full_inventory();
    inv.worker_registrations = None;
    let report = run_preflight(&seq(json!([step("a", "custom_a")])), &inv, t0());
    let c = check(&report, "handlers_have_workers");
    assert_eq!(c.status, PreflightStatus::Unknown);
    assert!(c.summary.contains("not collected"));
    assert!(c.findings.is_empty());
}

#[test]
fn missing_worker_inventory_is_unknown_even_for_builtin_only_sequence() {
    // The handler check goes Unknown before looking at the refs at all.
    let mut inv = full_inventory();
    inv.worker_registrations = None;
    let report = run_preflight(&seq(json!([noop_step("a")])), &inv, t0());
    assert_eq!(
        check(&report, "handlers_have_workers").status,
        PreflightStatus::Unknown
    );
}

#[test]
fn nested_handler_in_parallel_branch_is_collected() {
    let report = run_preflight(
        &seq(json!([
            {"type": "parallel", "id": "p", "branches": [[step("deep", "nested_handler")]]}
        ])),
        &full_inventory(),
        t0(),
    );
    let c = check(&report, "handlers_have_workers");
    assert_eq!(c.status, PreflightStatus::Fail);
    assert!(c.findings[0].summary.contains("deep"));
}

// ---------------------------------------------------------------------------
// handlers_have_workers: worker tenant scoping
// ---------------------------------------------------------------------------

#[test]
fn worker_with_no_tenant_serves_any_tenant() {
    let mut inv = full_inventory();
    let mut reg = registration("custom_a", None, None);
    reg.tenant_id = None;
    inv.worker_registrations = Some(vec![reg]);
    let report = run_preflight(&seq(json!([step("a", "custom_a")])), &inv, t0());
    assert_eq!(
        check(&report, "handlers_have_workers").status,
        PreflightStatus::Pass
    );
}

#[test]
fn worker_with_empty_tenant_serves_any_tenant() {
    let mut inv = full_inventory();
    let mut reg = registration("custom_a", None, None);
    reg.tenant_id = Some(String::new());
    inv.worker_registrations = Some(vec![reg]);
    let report = run_preflight(&seq(json!([step("a", "custom_a")])), &inv, t0());
    assert_eq!(
        check(&report, "handlers_have_workers").status,
        PreflightStatus::Pass
    );
}

#[test]
fn worker_scoped_to_matching_tenant_counts() {
    let mut inv = full_inventory();
    let mut reg = registration("custom_a", None, None);
    reg.tenant_id = Some("t1".to_string());
    inv.worker_registrations = Some(vec![reg]);
    let report = run_preflight(&seq(json!([step("a", "custom_a")])), &inv, t0());
    assert_eq!(
        check(&report, "handlers_have_workers").status,
        PreflightStatus::Pass
    );
}

#[test]
fn worker_scoped_to_other_tenant_is_invisible() {
    let mut inv = full_inventory();
    let mut reg = registration("custom_a", None, None);
    reg.tenant_id = Some("someone-else".to_string());
    inv.worker_registrations = Some(vec![reg]);
    let report = run_preflight(&seq(json!([step("a", "custom_a")])), &inv, t0());
    assert_eq!(
        check(&report, "handlers_have_workers").status,
        PreflightStatus::Fail
    );
}

#[test]
fn other_tenant_worker_plus_global_worker_passes() {
    let mut inv = full_inventory();
    let mut foreign = registration("custom_a", None, None);
    foreign.tenant_id = Some("someone-else".to_string());
    foreign.worker_id = "w-foreign".to_string();
    let global = registration("custom_a", None, None);
    inv.worker_registrations = Some(vec![foreign, global]);
    let report = run_preflight(&seq(json!([step("a", "custom_a")])), &inv, t0());
    assert_eq!(
        check(&report, "handlers_have_workers").status,
        PreflightStatus::Pass
    );
}

// ---------------------------------------------------------------------------
// handlers_have_workers: version pins
// ---------------------------------------------------------------------------

fn pinned_report(worker_version: Option<&str>, min: &str) -> PreflightReport {
    let mut inv = full_inventory();
    inv.worker_registrations = Some(vec![registration("custom_a", None, worker_version)]);
    inv.version_pins = Some(vec![pin("t1", "custom_a", min)]);
    run_preflight(&seq(json!([step("a", "custom_a")])), &inv, t0())
}

#[test]
fn pin_satisfied_by_equal_version_passes() {
    let report = pinned_report(Some("1.2.0"), "1.2.0");
    assert_eq!(
        check(&report, "handlers_have_workers").status,
        PreflightStatus::Pass
    );
}

#[test]
fn pin_uses_numeric_compare_not_lexical() {
    // Lexically "1.10.0" < "1.9.0", numerically it is newer.
    let report = pinned_report(Some("1.10.0"), "1.9.0");
    assert_eq!(
        check(&report, "handlers_have_workers").status,
        PreflightStatus::Pass
    );
}

#[test]
fn pin_rejects_numerically_older_worker() {
    let report = pinned_report(Some("1.9.0"), "1.10.0");
    let c = check(&report, "handlers_have_workers");
    assert_eq!(c.status, PreflightStatus::Fail);
    let f = &c.findings[0];
    assert_eq!(f.code, "WORKER_BELOW_VERSION_PIN");
    assert!(f.summary.contains("1.10.0"));
    assert_eq!(f.evidence[0].label, "live_workers");
    assert!(f.evidence[0].summary.contains("1.9.0"));
    assert!(!f.remediation.is_empty());
}

#[test]
fn pin_pads_missing_components() {
    let report = pinned_report(Some("2"), "2.0.0");
    assert_eq!(
        check(&report, "handlers_have_workers").status,
        PreflightStatus::Pass
    );
}

#[test]
fn pin_accepts_v_prefixed_worker_version() {
    let report = pinned_report(Some("v2.1"), "2.0.0");
    assert_eq!(
        check(&report, "handlers_have_workers").status,
        PreflightStatus::Pass
    );
}

#[test]
fn unversioned_worker_never_satisfies_a_pin() {
    let report = pinned_report(None, "1.0.0");
    let c = check(&report, "handlers_have_workers");
    assert_eq!(c.status, PreflightStatus::Fail);
    assert_eq!(c.findings[0].code, "WORKER_BELOW_VERSION_PIN");
    assert!(c.findings[0].evidence[0].summary.contains("unversioned"));
}

#[test]
fn non_numeric_pin_falls_back_to_lexical_compare() {
    let report = pinned_report(Some("2024-06-01"), "2024-05-01");
    assert_eq!(
        check(&report, "handlers_have_workers").status,
        PreflightStatus::Pass
    );
    let report = pinned_report(Some("2024-04-01"), "2024-05-01");
    assert_eq!(
        check(&report, "handlers_have_workers").status,
        PreflightStatus::Fail
    );
}

#[test]
fn mixed_numeric_and_non_numeric_pin_uses_lexical_over_raw_strings() {
    // "9.0" >= "10.x" lexically even though numeric ordering would say no.
    let report = pinned_report(Some("9.0"), "10.x");
    assert_eq!(
        check(&report, "handlers_have_workers").status,
        PreflightStatus::Pass
    );
}

#[test]
fn pin_for_other_tenant_is_ignored() {
    let mut inv = full_inventory();
    inv.worker_registrations = Some(vec![registration("custom_a", None, Some("0.1.0"))]);
    inv.version_pins = Some(vec![pin("other-tenant", "custom_a", "9.9.9")]);
    let report = run_preflight(&seq(json!([step("a", "custom_a")])), &inv, t0());
    assert_eq!(
        check(&report, "handlers_have_workers").status,
        PreflightStatus::Pass
    );
}

#[test]
fn pin_for_other_handler_is_ignored() {
    let mut inv = full_inventory();
    inv.worker_registrations = Some(vec![registration("custom_a", None, Some("0.1.0"))]);
    inv.version_pins = Some(vec![pin("t1", "custom_b", "9.9.9")]);
    let report = run_preflight(&seq(json!([step("a", "custom_a")])), &inv, t0());
    assert_eq!(
        check(&report, "handlers_have_workers").status,
        PreflightStatus::Pass
    );
}

#[test]
fn any_one_of_several_live_workers_satisfying_the_pin_is_enough() {
    let mut inv = full_inventory();
    let mut old = registration("custom_a", None, Some("1.0.0"));
    old.worker_id = "w-old".to_string();
    let new = registration("custom_a", None, Some("2.0.0"));
    inv.worker_registrations = Some(vec![old, new]);
    inv.version_pins = Some(vec![pin("t1", "custom_a", "1.5.0")]);
    let report = run_preflight(&seq(json!([step("a", "custom_a")])), &inv, t0());
    assert_eq!(
        check(&report, "handlers_have_workers").status,
        PreflightStatus::Pass
    );
}

#[test]
fn missing_pin_inventory_skips_pin_enforcement() {
    let mut inv = full_inventory();
    inv.worker_registrations = Some(vec![registration("custom_a", None, Some("0.0.1"))]);
    inv.version_pins = None;
    let report = run_preflight(&seq(json!([step("a", "custom_a")])), &inv, t0());
    assert_eq!(
        check(&report, "handlers_have_workers").status,
        PreflightStatus::Pass
    );
}

#[test]
fn pin_on_builtin_handler_is_irrelevant() {
    let mut inv = full_inventory();
    inv.version_pins = Some(vec![pin("t1", "noop", "99.0.0")]);
    let report = run_preflight(&seq(json!([noop_step("a")])), &inv, t0());
    assert_eq!(
        check(&report, "handlers_have_workers").status,
        PreflightStatus::Pass
    );
}

#[test]
fn pin_on_plugin_covered_handler_is_irrelevant() {
    let mut inv = full_inventory();
    inv.plugins = Some(vec![PluginInfo {
        name: "sentiment".to_string(),
        enabled: true,
    }]);
    inv.version_pins = Some(vec![pin("t1", "sentiment", "99.0.0")]);
    let report = run_preflight(&seq(json!([step("a", "sentiment")])), &inv, t0());
    assert_eq!(
        check(&report, "handlers_have_workers").status,
        PreflightStatus::Pass
    );
}

#[test]
fn version_satisfies_matrix_direct() {
    // Numeric compare.
    assert!(version_satisfies(Some("1.10.0"), "1.9.0"));
    assert!(!version_satisfies(Some("1.9.5"), "1.10"));
    assert!(version_satisfies(Some("2"), "2.0.0"));
    // Missing worker version never satisfies.
    assert!(!version_satisfies(None, "0.0.0"));
    // Lexical fallback for non-numeric components.
    assert!(version_satisfies(Some("abd"), "abc"));
    assert!(!version_satisfies(Some("abc"), "abd"));
}

// ---------------------------------------------------------------------------
// plugins_enabled
// ---------------------------------------------------------------------------

#[test]
fn no_plugin_references_pass_with_empty_plugin_inventory() {
    let report = run_preflight(&seq(json!([noop_step("a")])), &full_inventory(), t0());
    let c = check(&report, "plugins_enabled");
    assert_eq!(c.status, PreflightStatus::Pass);
    assert!(c.findings.is_empty());
}

#[test]
fn enabled_plugin_passes_both_plugin_and_handler_checks() {
    let mut inv = full_inventory();
    inv.plugins = Some(vec![PluginInfo {
        name: "sentiment".to_string(),
        enabled: true,
    }]);
    let report = run_preflight(&seq(json!([step("x", "sentiment")])), &inv, t0());
    assert_eq!(
        check(&report, "plugins_enabled").status,
        PreflightStatus::Pass
    );
    assert_eq!(
        check(&report, "handlers_have_workers").status,
        PreflightStatus::Pass
    );
}

#[test]
fn disabled_plugin_fails_with_finding_shape() {
    let mut inv = full_inventory();
    inv.plugins = Some(vec![PluginInfo {
        name: "sentiment".to_string(),
        enabled: false,
    }]);
    let report = run_preflight(&seq(json!([step("x", "sentiment")])), &inv, t0());
    let c = check(&report, "plugins_enabled");
    assert_eq!(c.status, PreflightStatus::Fail);
    let f = &c.findings[0];
    assert_eq!(f.code, "PLUGIN_DISABLED");
    assert!(f.summary.contains('x'));
    assert!(f.summary.contains("sentiment"));
    let res = f.affected_resource.as_ref().unwrap();
    assert_eq!(res.kind, "plugin");
    assert_eq!(res.id, "sentiment");
    assert!(!f.remediation.is_empty());
}

#[test]
fn missing_plugin_inventory_yields_unknown() {
    let mut inv = full_inventory();
    inv.plugins = None;
    let report = run_preflight(&seq(json!([noop_step("a")])), &inv, t0());
    let c = check(&report, "plugins_enabled");
    assert_eq!(c.status, PreflightStatus::Unknown);
    assert!(c.summary.contains("not collected"));
}

#[test]
fn missing_plugin_inventory_does_not_cover_handlers_but_still_requires_workers() {
    // plugins = None: the handler check cannot credit plugin coverage, so an
    // external handler with no worker fails (not unknown — workers ARE known).
    let mut inv = full_inventory();
    inv.plugins = None;
    let report = run_preflight(&seq(json!([step("x", "sentiment")])), &inv, t0());
    assert_eq!(
        check(&report, "handlers_have_workers").status,
        PreflightStatus::Fail
    );
}

#[test]
fn two_blocks_on_same_disabled_plugin_yield_two_findings() {
    let mut inv = full_inventory();
    inv.plugins = Some(vec![PluginInfo {
        name: "sentiment".to_string(),
        enabled: false,
    }]);
    let report = run_preflight(
        &seq(json!([step("x", "sentiment"), step("y", "sentiment")])),
        &inv,
        t0(),
    );
    let c = check(&report, "plugins_enabled");
    assert_eq!(c.findings.len(), 2);
    assert!(c.summary.contains("2 disabled plugin reference(s)"));
}

#[test]
fn disabled_plugin_in_nested_catch_block_is_detected() {
    let mut inv = full_inventory();
    inv.plugins = Some(vec![PluginInfo {
        name: "cleanup_plugin".to_string(),
        enabled: false,
    }]);
    let report = run_preflight(
        &seq(json!([
            {"type": "try_catch", "id": "tc",
             "try_block": [noop_step("a")],
             "catch_block": [step("c", "cleanup_plugin")]}
        ])),
        &inv,
        t0(),
    );
    assert_eq!(
        check(&report, "plugins_enabled").status,
        PreflightStatus::Fail
    );
}

#[test]
fn unreferenced_disabled_plugin_is_harmless() {
    let mut inv = full_inventory();
    inv.plugins = Some(vec![PluginInfo {
        name: "unused".to_string(),
        enabled: false,
    }]);
    let report = run_preflight(&seq(json!([noop_step("a")])), &inv, t0());
    assert_eq!(
        check(&report, "plugins_enabled").status,
        PreflightStatus::Pass
    );
}

#[test]
fn disabled_plugin_shadowing_a_builtin_name_still_reports() {
    // check_plugins matches step handlers against plugin names without
    // exempting builtins: a disabled plugin named "noop" is flagged even
    // though the handler check passes via the builtin.
    let mut inv = full_inventory();
    inv.plugins = Some(vec![PluginInfo {
        name: "noop".to_string(),
        enabled: false,
    }]);
    let report = run_preflight(&seq(json!([noop_step("a")])), &inv, t0());
    assert_eq!(
        check(&report, "handlers_have_workers").status,
        PreflightStatus::Pass
    );
    assert_eq!(
        check(&report, "plugins_enabled").status,
        PreflightStatus::Fail
    );
}

// ---------------------------------------------------------------------------
// credentials_present
// ---------------------------------------------------------------------------

fn cred_seq(reference: &str) -> SequenceDefinition {
    seq(json!([
        {"type": "step", "id": "a", "handler": "noop", "params": {"k": reference}}
    ]))
}

#[test]
fn no_credential_references_pass_even_without_credential_inventory() {
    // The early return fires before the inventory is consulted.
    let mut inv = full_inventory();
    inv.credentials = None;
    let report = run_preflight(&seq(json!([noop_step("a")])), &inv, t0());
    let c = check(&report, "credentials_present");
    assert_eq!(c.status, PreflightStatus::Pass);
    assert!(c.summary.contains("no credential references"));
}

#[test]
fn credential_reference_with_missing_inventory_is_unknown() {
    let mut inv = full_inventory();
    inv.credentials = None;
    let report = run_preflight(&cred_seq("credentials://stripe"), &inv, t0());
    let c = check(&report, "credentials_present");
    assert_eq!(c.status, PreflightStatus::Unknown);
    assert!(c.summary.contains("not collected"));
}

#[test]
fn missing_credential_fails_with_remediation_command() {
    let report = run_preflight(&cred_seq("credentials://stripe"), &full_inventory(), t0());
    let c = check(&report, "credentials_present");
    assert_eq!(c.status, PreflightStatus::Fail);
    let f = &c.findings[0];
    assert_eq!(f.code, "CREDENTIAL_MISSING");
    let res = f.affected_resource.as_ref().unwrap();
    assert_eq!(res.kind, "credential");
    assert_eq!(res.id, "stripe");
    let cmd = f.remediation[0].command.as_deref().unwrap();
    assert!(cmd.contains("orch8 credential create stripe"));
}

#[test]
fn disabled_credential_fails() {
    let mut inv = full_inventory();
    inv.credentials = Some(vec![cred("stripe", false, None)]);
    let report = run_preflight(&cred_seq("credentials://stripe"), &inv, t0());
    let c = check(&report, "credentials_present");
    assert_eq!(c.status, PreflightStatus::Fail);
    assert_eq!(c.findings[0].code, "CREDENTIAL_DISABLED");
}

#[test]
fn credential_expired_one_hour_ago_fails() {
    let mut inv = full_inventory();
    inv.credentials = Some(vec![cred("stripe", true, Some(t0() - Duration::hours(1)))]);
    let report = run_preflight(&cred_seq("credentials://stripe"), &inv, t0());
    let c = check(&report, "credentials_present");
    assert_eq!(c.findings[0].code, "CREDENTIAL_EXPIRED");
    assert!(c.findings[0].summary.contains("expired at"));
    assert!(!c.findings[0].remediation.is_empty());
}

#[test]
fn credential_expiring_exactly_now_is_already_expired() {
    let mut inv = full_inventory();
    inv.credentials = Some(vec![cred("stripe", true, Some(t0()))]);
    let report = run_preflight(&cred_seq("credentials://stripe"), &inv, t0());
    assert_eq!(
        check(&report, "credentials_present").findings[0].code,
        "CREDENTIAL_EXPIRED"
    );
}

#[test]
fn credential_expired_one_second_ago_fails() {
    let mut inv = full_inventory();
    inv.credentials = Some(vec![cred(
        "stripe",
        true,
        Some(t0() - Duration::seconds(1)),
    )]);
    let report = run_preflight(&cred_seq("credentials://stripe"), &inv, t0());
    assert_eq!(
        check(&report, "credentials_present").status,
        PreflightStatus::Fail
    );
}

#[test]
fn credential_expiring_one_second_in_future_passes() {
    let mut inv = full_inventory();
    inv.credentials = Some(vec![cred(
        "stripe",
        true,
        Some(t0() + Duration::seconds(1)),
    )]);
    let report = run_preflight(&cred_seq("credentials://stripe"), &inv, t0());
    assert_eq!(
        check(&report, "credentials_present").status,
        PreflightStatus::Pass
    );
}

#[test]
fn credential_without_expiry_passes() {
    let mut inv = full_inventory();
    inv.credentials = Some(vec![cred("stripe", true, None)]);
    let report = run_preflight(&cred_seq("credentials://stripe"), &inv, t0());
    let c = check(&report, "credentials_present");
    assert_eq!(c.status, PreflightStatus::Pass);
    assert!(c.summary.contains("1 referenced credential(s)"));
}

#[test]
fn disabled_and_expired_credential_reports_disabled_only() {
    // The disabled arm matches first; expiry is not double-reported.
    let mut inv = full_inventory();
    inv.credentials = Some(vec![cred("stripe", false, Some(t0() - Duration::hours(1)))]);
    let report = run_preflight(&cred_seq("credentials://stripe"), &inv, t0());
    let c = check(&report, "credentials_present");
    assert_eq!(codes(c), vec!["CREDENTIAL_DISABLED"]);
}

#[test]
fn mixed_credential_problems_accumulate() {
    let s = seq(json!([
        {"type": "step", "id": "a", "handler": "noop", "params": {
            "k1": "credentials://absent",
            "k2": "credentials://disabled",
            "k3": "credentials://expired",
            "k4": "credentials://healthy"
        }}
    ]));
    let mut inv = full_inventory();
    inv.credentials = Some(vec![
        cred("disabled", false, None),
        cred("expired", true, Some(t0() - Duration::days(1))),
        cred("healthy", true, Some(t0() + Duration::days(30))),
    ]);
    let report = run_preflight(&s, &inv, t0());
    let c = check(&report, "credentials_present");
    assert_eq!(c.status, PreflightStatus::Fail);
    assert_eq!(c.findings.len(), 3);
    assert!(c.summary.contains("3 credential problem(s)"));
    let cs = codes(c);
    assert!(cs.contains(&"CREDENTIAL_MISSING"));
    assert!(cs.contains(&"CREDENTIAL_DISABLED"));
    assert!(cs.contains(&"CREDENTIAL_EXPIRED"));
}

#[test]
fn credential_field_suffix_is_stripped_for_lookup() {
    let mut inv = full_inventory();
    inv.credentials = Some(vec![cred("vault", true, None)]);
    let report = run_preflight(&cred_seq("credentials://vault/token"), &inv, t0());
    assert_eq!(
        check(&report, "credentials_present").status,
        PreflightStatus::Pass
    );
}

#[test]
fn missing_credential_with_field_suffix_reports_bare_id() {
    let report = run_preflight(
        &cred_seq("credentials://vault/token"),
        &full_inventory(),
        t0(),
    );
    let c = check(&report, "credentials_present");
    assert_eq!(
        c.findings[0].affected_resource.as_ref().unwrap().id,
        "vault"
    );
}

#[test]
fn same_credential_referenced_with_and_without_suffix_dedupes() {
    let s = seq(json!([
        {"type": "step", "id": "a", "handler": "noop", "params": {
            "k1": "credentials://vault",
            "k2": "credentials://vault/token"
        }}
    ]));
    let report = run_preflight(&s, &full_inventory(), t0());
    assert_eq!(check(&report, "credentials_present").findings.len(), 1);
}

#[test]
fn empty_credential_id_is_ignored() {
    let report = run_preflight(&cred_seq("credentials://"), &full_inventory(), t0());
    let c = check(&report, "credentials_present");
    assert_eq!(c.status, PreflightStatus::Pass);
    assert!(c.summary.contains("no credential references"));
}

#[test]
fn non_credential_string_params_are_not_references() {
    let report = run_preflight(
        &cred_seq("https://example.com/credentials://not-a-ref"),
        &full_inventory(),
        t0(),
    );
    // Prefix must be at the start of the string.
    assert_eq!(
        check(&report, "credentials_present").status,
        PreflightStatus::Pass
    );
}

#[test]
fn credential_ref_in_parallel_branch_is_collected() {
    let s = seq(json!([
        {"type": "parallel", "id": "p", "branches": [
            [{"type": "step", "id": "b1", "handler": "noop", "params": {"k": "credentials://par-cred"}}],
            [noop_step("b2")]
        ]}
    ]));
    let report = run_preflight(&s, &full_inventory(), t0());
    let c = check(&report, "credentials_present");
    assert_eq!(c.status, PreflightStatus::Fail);
    assert_eq!(
        c.findings[0].affected_resource.as_ref().unwrap().id,
        "par-cred"
    );
}

#[test]
fn credential_ref_in_router_route_and_default_is_collected() {
    let s = seq(json!([
        {"type": "router", "id": "r", "routes": [
            {"condition": "true", "blocks": [
                {"type": "step", "id": "b1", "handler": "noop", "params": {"k": "credentials://route-cred"}}
            ]}
        ], "default": [
            {"type": "step", "id": "b2", "handler": "noop", "params": {"k": "credentials://default-cred"}}
        ]}
    ]));
    let report = run_preflight(&s, &full_inventory(), t0());
    let c = check(&report, "credentials_present");
    assert_eq!(c.findings.len(), 2);
    let ids: Vec<&str> = c
        .findings
        .iter()
        .map(|f| f.affected_resource.as_ref().unwrap().id.as_str())
        .collect();
    assert!(ids.contains(&"route-cred"));
    assert!(ids.contains(&"default-cred"));
}

#[test]
fn credential_ref_in_loop_body_is_collected() {
    let s = seq(json!([
        {"type": "loop", "id": "l", "condition": "true", "body": [
            {"type": "step", "id": "b", "handler": "noop", "params": {"k": "credentials://loop-cred"}}
        ]}
    ]));
    let report = run_preflight(&s, &full_inventory(), t0());
    assert_eq!(
        check(&report, "credentials_present").findings[0]
            .affected_resource
            .as_ref()
            .unwrap()
            .id,
        "loop-cred"
    );
}

#[test]
fn credential_ref_in_try_catch_blocks_is_collected() {
    let s = seq(json!([
        {"type": "try_catch", "id": "tc",
         "try_block": [
            {"type": "step", "id": "t", "handler": "noop", "params": {"k": "credentials://try-cred"}}
         ],
         "catch_block": [
            {"type": "step", "id": "c", "handler": "noop", "params": {"k": "credentials://catch-cred"}}
         ]}
    ]));
    let report = run_preflight(&s, &full_inventory(), t0());
    let ids: Vec<&str> = check(&report, "credentials_present")
        .findings
        .iter()
        .map(|f| f.affected_resource.as_ref().unwrap().id.as_str())
        .collect();
    assert!(ids.contains(&"try-cred"));
    assert!(ids.contains(&"catch-cred"));
}

#[test]
fn credential_ref_in_for_each_body_is_collected() {
    let s = seq(json!([
        {"type": "for_each", "id": "fe", "collection": "items", "body": [
            {"type": "step", "id": "b", "handler": "noop", "params": {"k": "credentials://each-cred"}}
        ]}
    ]));
    let report = run_preflight(&s, &full_inventory(), t0());
    assert_eq!(
        check(&report, "credentials_present").findings[0].code,
        "CREDENTIAL_MISSING"
    );
}

#[test]
fn credential_ref_in_sub_sequence_input_is_collected() {
    let s = seq(json!([
        {"type": "sub_sequence", "id": "child", "sequence_name": "other",
         "input": {"token": "credentials://sub-cred"}}
    ]));
    let report = run_preflight(&s, &full_inventory(), t0());
    assert_eq!(
        check(&report, "credentials_present").findings[0]
            .affected_resource
            .as_ref()
            .unwrap()
            .id,
        "sub-cred"
    );
}

// ---------------------------------------------------------------------------
// queues_consumable
// ---------------------------------------------------------------------------

fn queue_seq(queue: &str) -> SequenceDefinition {
    seq(json!([
        {"type": "step", "id": "a", "handler": "noop", "params": {}, "queue_name": queue}
    ]))
}

#[test]
fn no_explicit_queues_pass_even_without_queue_inventory() {
    let report = run_preflight(
        &seq(json!([noop_step("a")])),
        &RuntimeInventory::default(),
        t0(),
    );
    let c = check(&report, "queues_consumable");
    assert_eq!(c.status, PreflightStatus::Pass);
    assert!(c.summary.contains("no explicit queue targets"));
}

#[test]
fn missing_dispatch_inventory_yields_unknown() {
    let mut inv = full_inventory();
    inv.queue_dispatch = None;
    let report = run_preflight(&queue_seq("q"), &inv, t0());
    assert_eq!(
        check(&report, "queues_consumable").status,
        PreflightStatus::Unknown
    );
}

#[test]
fn missing_registration_inventory_yields_unknown_for_queues() {
    let mut inv = full_inventory();
    inv.worker_registrations = None;
    let report = run_preflight(&queue_seq("q"), &inv, t0());
    assert_eq!(
        check(&report, "queues_consumable").status,
        PreflightStatus::Unknown
    );
}

#[test]
fn orphan_queue_warns_with_finding_shape() {
    let report = run_preflight(&queue_seq("orphan"), &full_inventory(), t0());
    let c = check(&report, "queues_consumable");
    assert_eq!(c.status, PreflightStatus::Warning);
    let f = &c.findings[0];
    assert_eq!(f.code, "QUEUE_HAS_NO_CONSUMER");
    assert!(f.summary.contains("orphan"));
    let res = f.affected_resource.as_ref().unwrap();
    assert_eq!(res.kind, "queue");
    assert_eq!(res.id, "orphan");
}

#[test]
fn queue_warning_keeps_report_ready() {
    let report = run_preflight(&queue_seq("orphan"), &full_inventory(), t0());
    assert_eq!(report.overall, PreflightStatus::Warning);
    assert!(report.is_ready());
}

#[test]
fn polling_worker_on_queue_is_a_consumer() {
    let mut inv = full_inventory();
    inv.worker_registrations = Some(vec![registration("whatever", Some("billing"), None)]);
    let report = run_preflight(&queue_seq("billing"), &inv, t0());
    assert_eq!(
        check(&report, "queues_consumable").status,
        PreflightStatus::Pass
    );
}

#[test]
fn polling_worker_on_different_queue_is_not_a_consumer() {
    let mut inv = full_inventory();
    inv.worker_registrations = Some(vec![registration("whatever", Some("other-q"), None)]);
    let report = run_preflight(&queue_seq("billing"), &inv, t0());
    assert_eq!(
        check(&report, "queues_consumable").status,
        PreflightStatus::Warning
    );
}

#[test]
fn push_dispatch_config_is_a_consumer() {
    let mut inv = full_inventory();
    inv.queue_dispatch = Some(vec![push_dispatch("t1", "push-q")]);
    let report = run_preflight(&queue_seq("push-q"), &inv, t0());
    assert_eq!(
        check(&report, "queues_consumable").status,
        PreflightStatus::Pass
    );
}

#[test]
fn poll_mode_dispatch_config_is_not_a_consumer() {
    let mut inv = full_inventory();
    inv.queue_dispatch = Some(vec![poll_dispatch("t1", "poll-q")]);
    let report = run_preflight(&queue_seq("poll-q"), &inv, t0());
    assert_eq!(
        check(&report, "queues_consumable").status,
        PreflightStatus::Warning
    );
}

#[test]
fn enabled_routing_rule_redirect_counts_as_consumer() {
    let mut inv = full_inventory();
    inv.routing_rules = Some(vec![redirect_rule(Some("redirected-q"), true)]);
    let report = run_preflight(&queue_seq("redirected-q"), &inv, t0());
    assert_eq!(
        check(&report, "queues_consumable").status,
        PreflightStatus::Pass
    );
}

#[test]
fn disabled_routing_rule_does_not_redirect() {
    let mut inv = full_inventory();
    inv.routing_rules = Some(vec![redirect_rule(Some("redirected-q"), false)]);
    let report = run_preflight(&queue_seq("redirected-q"), &inv, t0());
    assert_eq!(
        check(&report, "queues_consumable").status,
        PreflightStatus::Warning
    );
}

#[test]
fn routing_rule_without_match_queue_does_not_count_as_redirect() {
    let mut inv = full_inventory();
    inv.routing_rules = Some(vec![redirect_rule(None, true)]);
    let report = run_preflight(&queue_seq("some-q"), &inv, t0());
    assert_eq!(
        check(&report, "queues_consumable").status,
        PreflightStatus::Warning
    );
}

#[test]
fn missing_routing_rules_inventory_is_treated_as_no_redirects() {
    // routing_rules = None is NOT an unknown for this check — it just means
    // no redirect credit; workers/dispatch still decide.
    let mut inv = full_inventory();
    inv.routing_rules = None;
    let report = run_preflight(&queue_seq("some-q"), &inv, t0());
    assert_eq!(
        check(&report, "queues_consumable").status,
        PreflightStatus::Warning
    );
}

#[test]
fn multiple_queues_report_only_the_orphans() {
    let s = seq(json!([
        {"type": "step", "id": "a", "handler": "noop", "params": {}, "queue_name": "served"},
        {"type": "step", "id": "b", "handler": "noop", "params": {}, "queue_name": "orphan-1"},
        {"type": "step", "id": "c", "handler": "noop", "params": {}, "queue_name": "orphan-2"}
    ]));
    let mut inv = full_inventory();
    inv.worker_registrations = Some(vec![registration("whatever", Some("served"), None)]);
    let report = run_preflight(&s, &inv, t0());
    let c = check(&report, "queues_consumable");
    assert_eq!(c.findings.len(), 2);
    assert!(c.summary.contains("2 queue(s)"));
    let ids: Vec<&str> = c
        .findings
        .iter()
        .map(|f| f.affected_resource.as_ref().unwrap().id.as_str())
        .collect();
    assert!(ids.contains(&"orphan-1"));
    assert!(ids.contains(&"orphan-2"));
}

#[test]
fn queue_name_on_nested_step_is_collected() {
    let s = seq(json!([
        {"type": "parallel", "id": "p", "branches": [[
            {"type": "step", "id": "b", "handler": "noop", "params": {}, "queue_name": "nested-q"}
        ]]}
    ]));
    let report = run_preflight(&s, &full_inventory(), t0());
    let c = check(&report, "queues_consumable");
    assert_eq!(c.status, PreflightStatus::Warning);
    assert_eq!(
        c.findings[0].affected_resource.as_ref().unwrap().id,
        "nested-q"
    );
}

// ---------------------------------------------------------------------------
// sub_sequences_available
// ---------------------------------------------------------------------------

fn sub_seq_ref(name: &str, version: Option<i32>) -> SequenceDefinition {
    match version {
        Some(v) => seq(json!([
            {"type": "sub_sequence", "id": "child", "sequence_name": name, "version": v, "input": {}}
        ])),
        None => seq(json!([
            {"type": "sub_sequence", "id": "child", "sequence_name": name, "input": {}}
        ])),
    }
}

#[test]
fn no_sub_sequence_refs_pass_without_sequence_inventory() {
    let mut inv = full_inventory();
    inv.sequences = None;
    let report = run_preflight(&seq(json!([noop_step("a")])), &inv, t0());
    assert_eq!(
        check(&report, "sub_sequences_available").status,
        PreflightStatus::Pass
    );
}

#[test]
fn sub_sequence_ref_with_missing_inventory_is_unknown() {
    let mut inv = full_inventory();
    inv.sequences = None;
    let report = run_preflight(&sub_seq_ref("child-flow", None), &inv, t0());
    assert_eq!(
        check(&report, "sub_sequences_available").status,
        PreflightStatus::Unknown
    );
}

#[test]
fn missing_sub_sequence_fails_without_version_in_message() {
    let report = run_preflight(&sub_seq_ref("ghost", None), &full_inventory(), t0());
    let c = check(&report, "sub_sequences_available");
    assert_eq!(c.status, PreflightStatus::Fail);
    let f = &c.findings[0];
    assert_eq!(f.code, "SUB_SEQUENCE_MISSING");
    assert!(f.summary.contains("ghost"));
    assert!(f.summary.contains("default"));
    assert_eq!(f.affected_resource.as_ref().unwrap().kind, "sequence");
}

#[test]
fn missing_pinned_sub_sequence_names_the_version() {
    let report = run_preflight(&sub_seq_ref("ghost", Some(3)), &full_inventory(), t0());
    let c = check(&report, "sub_sequences_available");
    assert!(c.findings[0].summary.contains("v3"));
}

#[test]
fn production_sub_sequence_passes() {
    let mut inv = full_inventory();
    inv.sequences = Some(vec![sub("child", "default", 1, SequenceStatus::Production)]);
    let report = run_preflight(&sub_seq_ref("child", None), &inv, t0());
    assert_eq!(
        check(&report, "sub_sequences_available").status,
        PreflightStatus::Pass
    );
}

#[test]
fn staging_sub_sequence_passes() {
    let mut inv = full_inventory();
    inv.sequences = Some(vec![sub("child", "default", 1, SequenceStatus::Staging)]);
    let report = run_preflight(&sub_seq_ref("child", None), &inv, t0());
    assert_eq!(
        check(&report, "sub_sequences_available").status,
        PreflightStatus::Pass
    );
}

#[test]
fn draft_only_sub_sequence_warns() {
    let mut inv = full_inventory();
    inv.sequences = Some(vec![sub("child", "default", 1, SequenceStatus::Draft)]);
    let report = run_preflight(&sub_seq_ref("child", None), &inv, t0());
    let c = check(&report, "sub_sequences_available");
    assert_eq!(c.status, PreflightStatus::Warning);
    assert_eq!(c.findings[0].code, "SUB_SEQUENCE_DRAFT_ONLY");
}

#[test]
fn unpublished_only_sub_sequence_fails() {
    let mut inv = full_inventory();
    inv.sequences = Some(vec![sub(
        "child",
        "default",
        1,
        SequenceStatus::Unpublished,
    )]);
    let report = run_preflight(&sub_seq_ref("child", None), &inv, t0());
    let c = check(&report, "sub_sequences_available");
    assert_eq!(c.status, PreflightStatus::Fail);
    assert_eq!(c.findings[0].code, "SUB_SEQUENCE_UNPUBLISHED");
}

#[test]
fn draft_plus_production_versions_pass() {
    let mut inv = full_inventory();
    inv.sequences = Some(vec![
        sub("child", "default", 1, SequenceStatus::Draft),
        sub("child", "default", 2, SequenceStatus::Production),
    ]);
    let report = run_preflight(&sub_seq_ref("child", None), &inv, t0());
    assert_eq!(
        check(&report, "sub_sequences_available").status,
        PreflightStatus::Pass
    );
}

#[test]
fn draft_plus_unpublished_versions_warn_as_draft_only() {
    // Not all unpublished (a draft exists), but nothing usable either.
    let mut inv = full_inventory();
    inv.sequences = Some(vec![
        sub("child", "default", 1, SequenceStatus::Unpublished),
        sub("child", "default", 2, SequenceStatus::Draft),
    ]);
    let report = run_preflight(&sub_seq_ref("child", None), &inv, t0());
    let c = check(&report, "sub_sequences_available");
    assert_eq!(c.status, PreflightStatus::Warning);
    assert_eq!(c.findings[0].code, "SUB_SEQUENCE_DRAFT_ONLY");
}

#[test]
fn unpublished_plus_production_versions_pass() {
    let mut inv = full_inventory();
    inv.sequences = Some(vec![
        sub("child", "default", 1, SequenceStatus::Unpublished),
        sub("child", "default", 2, SequenceStatus::Production),
    ]);
    let report = run_preflight(&sub_seq_ref("child", None), &inv, t0());
    assert_eq!(
        check(&report, "sub_sequences_available").status,
        PreflightStatus::Pass
    );
}

#[test]
fn version_pin_filters_candidates_to_that_version_only() {
    let mut inv = full_inventory();
    inv.sequences = Some(vec![
        sub("child", "default", 1, SequenceStatus::Production),
        sub("child", "default", 2, SequenceStatus::Draft),
    ]);
    // Pinned to v2, which is draft-only → warning even though v1 is prod.
    let report = run_preflight(&sub_seq_ref("child", Some(2)), &inv, t0());
    let c = check(&report, "sub_sequences_available");
    assert_eq!(c.status, PreflightStatus::Warning);
    assert_eq!(c.findings[0].code, "SUB_SEQUENCE_DRAFT_ONLY");
}

#[test]
fn version_pin_to_nonexistent_version_fails_as_missing() {
    let mut inv = full_inventory();
    inv.sequences = Some(vec![sub("child", "default", 1, SequenceStatus::Production)]);
    let report = run_preflight(&sub_seq_ref("child", Some(7)), &inv, t0());
    assert_eq!(
        check(&report, "sub_sequences_available").findings[0].code,
        "SUB_SEQUENCE_MISSING"
    );
}

#[test]
fn unpinned_ref_matches_any_version() {
    let mut inv = full_inventory();
    inv.sequences = Some(vec![sub(
        "child",
        "default",
        42,
        SequenceStatus::Production,
    )]);
    let report = run_preflight(&sub_seq_ref("child", None), &inv, t0());
    assert_eq!(
        check(&report, "sub_sequences_available").status,
        PreflightStatus::Pass
    );
}

#[test]
fn sub_sequence_in_other_namespace_is_invisible() {
    let mut inv = full_inventory();
    inv.sequences = Some(vec![sub("child", "otherns", 1, SequenceStatus::Production)]);
    let report = run_preflight(&sub_seq_ref("child", None), &inv, t0());
    assert_eq!(
        check(&report, "sub_sequences_available").findings[0].code,
        "SUB_SEQUENCE_MISSING"
    );
}

#[test]
fn parent_namespace_scopes_sub_sequence_lookup() {
    // The same inventory resolves when the parent lives in the matching
    // namespace.
    let mut inv = full_inventory();
    inv.sequences = Some(vec![sub("child", "otherns", 1, SequenceStatus::Production)]);
    let parent = seq_in(
        "t1",
        "otherns",
        json!([
            {"type": "sub_sequence", "id": "c", "sequence_name": "child", "input": {}}
        ]),
    );
    let report = run_preflight(&parent, &inv, t0());
    assert_eq!(
        check(&report, "sub_sequences_available").status,
        PreflightStatus::Pass
    );
}

#[test]
fn missing_and_draft_refs_together_fail_with_both_codes() {
    let s = seq(json!([
        {"type": "sub_sequence", "id": "c1", "sequence_name": "ghost", "input": {}},
        {"type": "sub_sequence", "id": "c2", "sequence_name": "drafty", "input": {}}
    ]));
    let mut inv = full_inventory();
    inv.sequences = Some(vec![sub("drafty", "default", 1, SequenceStatus::Draft)]);
    let report = run_preflight(&s, &inv, t0());
    let c = check(&report, "sub_sequences_available");
    // Error outranks warning → check-level Fail.
    assert_eq!(c.status, PreflightStatus::Fail);
    let cs = codes(c);
    assert!(cs.contains(&"SUB_SEQUENCE_MISSING"));
    assert!(cs.contains(&"SUB_SEQUENCE_DRAFT_ONLY"));
}

#[test]
fn draft_only_warning_keeps_overall_warning_and_ready() {
    let mut inv = full_inventory();
    inv.sequences = Some(vec![sub("child", "default", 1, SequenceStatus::Draft)]);
    let report = run_preflight(&sub_seq_ref("child", None), &inv, t0());
    assert_eq!(report.overall, PreflightStatus::Warning);
    assert!(report.is_ready());
}

#[test]
fn sub_sequence_nested_in_parallel_branch_is_collected() {
    let s = seq(json!([
        {"type": "parallel", "id": "p", "branches": [[
            {"type": "sub_sequence", "id": "c", "sequence_name": "nested-ghost", "input": {}}
        ]]}
    ]));
    let report = run_preflight(&s, &full_inventory(), t0());
    let c = check(&report, "sub_sequences_available");
    assert_eq!(c.status, PreflightStatus::Fail);
    assert!(c.findings[0].summary.contains("nested-ghost"));
}

#[test]
fn two_sub_sequences_resolve_independently() {
    let s = seq(json!([
        {"type": "sub_sequence", "id": "c1", "sequence_name": "good", "input": {}},
        {"type": "sub_sequence", "id": "c2", "sequence_name": "ghost", "input": {}}
    ]));
    let mut inv = full_inventory();
    inv.sequences = Some(vec![sub("good", "default", 1, SequenceStatus::Production)]);
    let report = run_preflight(&s, &inv, t0());
    let c = check(&report, "sub_sequences_available");
    assert_eq!(c.findings.len(), 1);
    assert!(c.findings[0].summary.contains("ghost"));
}

// ---------------------------------------------------------------------------
// report shape and aggregation
// ---------------------------------------------------------------------------

#[test]
fn report_contains_all_eight_checks_in_stable_order() {
    let report = run_preflight(&seq(json!([noop_step("a")])), &full_inventory(), t0());
    let ids: Vec<&str> = report.checks.iter().map(|c| c.id.as_str()).collect();
    assert_eq!(
        ids,
        vec![
            "definition_valid",
            "lint_clean",
            "input_schema_valid",
            "handlers_have_workers",
            "plugins_enabled",
            "credentials_present",
            "queues_consumable",
            "sub_sequences_available",
        ]
    );
}

#[test]
fn report_copies_name_version_and_timestamp() {
    let report = run_preflight(&seq(json!([noop_step("a")])), &full_inventory(), t0());
    assert_eq!(report.sequence_name, "case-seq");
    assert_eq!(report.sequence_version, 1);
    assert_eq!(report.generated_at, t0());
}

#[test]
fn fully_clean_sequence_is_pass_and_ready() {
    let report = run_preflight(&seq(json!([noop_step("a")])), &full_inventory(), t0());
    assert_eq!(report.overall, PreflightStatus::Pass);
    assert!(report.is_ready());
    assert!(report.checks.iter().all(|c| c.findings.is_empty()));
}

#[test]
fn default_inventory_yields_unknown_overall_for_builtin_sequence() {
    // Every inventory-backed check is unknown; unknown is never success.
    let report = run_preflight(
        &seq(json!([noop_step("a")])),
        &RuntimeInventory::default(),
        t0(),
    );
    assert_eq!(report.overall, PreflightStatus::Unknown);
    assert!(!report.is_ready());
}

#[test]
fn unknown_outranks_warning_in_overall() {
    // Orphan queue (warning) + missing plugin inventory (unknown).
    let mut inv = full_inventory();
    inv.plugins = None;
    let report = run_preflight(&queue_seq("orphan"), &inv, t0());
    assert_eq!(
        check(&report, "queues_consumable").status,
        PreflightStatus::Warning
    );
    assert_eq!(
        check(&report, "plugins_enabled").status,
        PreflightStatus::Unknown
    );
    assert_eq!(report.overall, PreflightStatus::Unknown);
    assert!(!report.is_ready());
}

#[test]
fn fail_outranks_unknown_in_overall() {
    // Missing worker (fail) + missing credential inventory (unknown).
    let mut inv = full_inventory();
    inv.credentials = None;
    let s = seq(json!([
        {"type": "step", "id": "a", "handler": "custom_x", "params": {"k": "credentials://c"}}
    ]));
    let report = run_preflight(&s, &inv, t0());
    assert_eq!(
        check(&report, "handlers_have_workers").status,
        PreflightStatus::Fail
    );
    assert_eq!(
        check(&report, "credentials_present").status,
        PreflightStatus::Unknown
    );
    assert_eq!(report.overall, PreflightStatus::Fail);
}

#[test]
fn warning_outranks_pass_in_overall() {
    let report = run_preflight(&queue_seq("orphan"), &full_inventory(), t0());
    assert_eq!(report.overall, PreflightStatus::Warning);
}

#[test]
fn several_checks_can_fail_at_once() {
    let s = seq(json!([
        {"type": "step", "id": "a", "handler": "custom_x",
         "params": {"k": "credentials://absent"}},
        {"type": "sub_sequence", "id": "c", "sequence_name": "ghost", "input": {}}
    ]));
    let report = run_preflight(&s, &full_inventory(), t0());
    assert_eq!(
        check(&report, "handlers_have_workers").status,
        PreflightStatus::Fail
    );
    assert_eq!(
        check(&report, "credentials_present").status,
        PreflightStatus::Fail
    );
    assert_eq!(
        check(&report, "sub_sequences_available").status,
        PreflightStatus::Fail
    );
    assert_eq!(report.overall, PreflightStatus::Fail);
}

#[test]
fn run_preflight_is_deterministic_for_same_inputs() {
    let s = seq(json!([
        {"type": "step", "id": "a", "handler": "custom_x", "params": {"k": "credentials://c"}}
    ]));
    let inv = full_inventory();
    let r1 = run_preflight(&s, &inv, t0());
    let r2 = run_preflight(&s, &inv, t0());
    assert_eq!(r1, r2);
}

// ---------------------------------------------------------------------------
// orch8_types::preflight primitives
// ---------------------------------------------------------------------------

#[test]
fn status_ordering_is_pass_warning_unknown_fail() {
    assert!(PreflightStatus::Pass < PreflightStatus::Warning);
    assert!(PreflightStatus::Warning < PreflightStatus::Unknown);
    assert!(PreflightStatus::Unknown < PreflightStatus::Fail);
}

#[test]
fn report_new_with_empty_checks_is_pass_and_ready() {
    let report = PreflightReport::new("empty", 3, vec![], t0());
    assert_eq!(report.overall, PreflightStatus::Pass);
    assert!(report.is_ready());
    assert!(report.checks.is_empty());
    assert_eq!(report.sequence_name, "empty");
    assert_eq!(report.sequence_version, 3);
    assert_eq!(report.generated_at, t0());
}

#[test]
fn report_new_overall_is_max_of_check_statuses() {
    let mk = |s| PreflightCheck::with_status("c", s, "s", vec![]);
    let cases = [
        (vec![PreflightStatus::Pass], PreflightStatus::Pass),
        (
            vec![PreflightStatus::Pass, PreflightStatus::Warning],
            PreflightStatus::Warning,
        ),
        (
            vec![
                PreflightStatus::Warning,
                PreflightStatus::Unknown,
                PreflightStatus::Pass,
            ],
            PreflightStatus::Unknown,
        ),
        (
            vec![
                PreflightStatus::Fail,
                PreflightStatus::Unknown,
                PreflightStatus::Warning,
            ],
            PreflightStatus::Fail,
        ),
    ];
    for (statuses, expected) in cases {
        let checks = statuses.into_iter().map(mk).collect();
        assert_eq!(PreflightReport::new("s", 1, checks, t0()).overall, expected);
    }
}

#[test]
fn is_ready_semantics_per_status() {
    let mk = |s| {
        PreflightReport::new(
            "s",
            1,
            vec![PreflightCheck::with_status("c", s, "s", vec![])],
            t0(),
        )
    };
    assert!(mk(PreflightStatus::Pass).is_ready());
    assert!(mk(PreflightStatus::Warning).is_ready());
    assert!(!mk(PreflightStatus::Unknown).is_ready());
    assert!(!mk(PreflightStatus::Fail).is_ready());
}

#[test]
fn preflight_check_pass_constructor_shape() {
    let c = PreflightCheck::pass("some_id", "all good");
    assert_eq!(c.id, "some_id");
    assert_eq!(c.status, PreflightStatus::Pass);
    assert_eq!(c.summary, "all good");
    assert!(c.findings.is_empty());
}

#[test]
fn statuses_serialize_snake_case() {
    assert_eq!(
        serde_json::to_string(&PreflightStatus::Pass).unwrap(),
        "\"pass\""
    );
    assert_eq!(
        serde_json::to_string(&PreflightStatus::Warning).unwrap(),
        "\"warning\""
    );
    assert_eq!(
        serde_json::to_string(&PreflightStatus::Unknown).unwrap(),
        "\"unknown\""
    );
    assert_eq!(
        serde_json::to_string(&PreflightStatus::Fail).unwrap(),
        "\"fail\""
    );
}

#[test]
fn full_report_round_trips_through_json() {
    let s = seq(json!([
        {"type": "step", "id": "a", "handler": "custom_x", "params": {"k": "credentials://c"}},
        {"type": "sub_sequence", "id": "c2", "sequence_name": "ghost", "input": {}}
    ]));
    let report = run_preflight(&s, &full_inventory(), t0());
    let json = serde_json::to_string(&report).unwrap();
    let back: PreflightReport = serde_json::from_str(&json).unwrap();
    assert_eq!(back, report);
}

#[test]
fn clean_pass_checks_omit_findings_key_in_json() {
    let report = run_preflight(&seq(json!([noop_step("a")])), &full_inventory(), t0());
    let v = serde_json::to_value(&report).unwrap();
    for c in v["checks"].as_array().unwrap() {
        assert!(
            c.get("findings").is_none(),
            "clean check should omit findings: {c}"
        );
    }
}
