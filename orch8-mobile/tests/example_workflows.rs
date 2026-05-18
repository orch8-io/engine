use std::sync::Arc;

use orch8_mobile::{
    EngineListener, HandlerError, InstanceStateKind, MobileEngine, MobileEngineConfig, StepHandler,
};

struct JsonEchoHandler;

impl StepHandler for JsonEchoHandler {
    fn execute(&self, _step_name: String, _input: String) -> Result<String, HandlerError> {
        Ok(r#"{"ok":true}"#.to_string())
    }
}

#[derive(Default)]
struct NoopListener;

impl EngineListener for NoopListener {
    fn on_instance_completed(&self, _id: String, _output: String) {}
    fn on_instance_failed(&self, _id: String, _error: String) {}
    fn on_step_pending(&self, _id: String, _step: String, _handler: String) {}
}

fn load_workflow(name: &str) -> String {
    let path = format!(
        "{}/mobile-examples/workflows/{}.json",
        env!("CARGO_MANIFEST_DIR").replace("/engine/orch8-mobile", ""),
        name
    );
    std::fs::read_to_string(&path).unwrap_or_else(|e| panic!("failed to read {path}: {e}"))
}

fn make_engine() -> Arc<MobileEngine> {
    let dir = tempfile::tempdir().unwrap();
    let db = dir.path().join("test.db").to_string_lossy().to_string();
    std::mem::forget(dir);

    let config = MobileEngineConfig {
        tick_interval_ms: 50,
        max_concurrent_steps: 4,
        max_steps_per_instance: 1000,
        max_concurrent_instances: 10,
        max_tick_duration_ms: 5000,
        max_instance_lifetime_secs: 86400,
        max_stored_sequences: 50,
        max_sequence_size_bytes: 1_048_576,
        handler_timeout_ms: 5000,
        operation_timeout_ms: 10_000,
        telemetry_enabled: false,
        environment: "test".to_string(),
        root_public_key: String::new(),
        sdk_version: "0.1.0".to_string(),
        memory_budget_bytes: 0,
    };

    let engine = MobileEngine::new(db, config).unwrap();

    let handlers = [
        "init_profile",
        "validate_email",
        "show_terms",
        "collect_preferences",
        "setup_notifications",
        "request_approval",
        "complete_onboarding",
        "init_payment",
        "validate_amount",
        "fraud_check",
        "risk_assessment",
        "compliance_check",
        "process_payment",
        "send_receipt",
        "check_eligibility",
        "fetch_feature_config",
        "evaluate_rules",
        "request_consent",
        "verify_identity",
        "activate_feature",
        "show_banner",
    ];
    for name in handlers {
        engine
            .register_handler(name.to_string(), Arc::new(JsonEchoHandler))
            .unwrap();
    }

    engine.set_listener(Arc::new(NoopListener));

    for wf in ["onboarding-flow", "payment-verification", "feature-access"] {
        engine.load_sequence_from_json(load_workflow(wf)).unwrap();
    }

    engine
}

/// Tick until the instance reaches `target` state, panic if not reached.
fn wait_for_state(
    engine: &MobileEngine,
    instance_id: &str,
    target: InstanceStateKind,
    max_ticks: u32,
) {
    for _ in 0..max_ticks {
        let _ = engine.tick_once();
        let info = engine.get_instance(instance_id.to_string()).unwrap();
        if info.state == target {
            return;
        }
        std::thread::sleep(std::time::Duration::from_millis(20));
    }
    let info = engine.get_instance(instance_id.to_string()).unwrap();
    panic!(
        "instance {} did not reach {:?} after {max_ticks} ticks (stuck at {:?})",
        instance_id, target, info.state
    );
}

fn complete_waiting_step(engine: &MobileEngine, instance_id: &str, step: &str, value: &str) {
    let output = format!(r#"{{"value":"{value}"}}"#);
    engine
        .complete_step(instance_id.to_string(), step.to_string(), output)
        .unwrap();
}

// ---------------------------------------------------------------------------
// Loading & starting
// ---------------------------------------------------------------------------

#[test]
fn all_three_workflows_load_successfully() {
    let engine = make_engine();
    let seqs = engine.loaded_sequences().unwrap();
    let names: Vec<&str> = seqs.iter().map(|s| s.name.as_str()).collect();
    assert!(names.contains(&"onboarding-flow"));
    assert!(names.contains(&"payment-verification"));
    assert!(names.contains(&"feature-access"));
    engine.shutdown();
}

#[test]
fn all_three_workflows_can_start_concurrently() {
    let engine = make_engine();
    let id1 = engine
        .start("onboarding-flow".into(), "{}".into(), None)
        .unwrap();
    let id2 = engine
        .start("payment-verification".into(), "{}".into(), None)
        .unwrap();
    let id3 = engine
        .start("feature-access".into(), "{}".into(), None)
        .unwrap();
    assert_ne!(id1, id2);
    assert_ne!(id2, id3);
    engine.shutdown();
}

// ---------------------------------------------------------------------------
// Onboarding: accept terms → prefs → notifications → approve → complete
// ---------------------------------------------------------------------------

#[test]
fn onboarding_approve_path() {
    let engine = make_engine();
    let id = engine
        .start("onboarding-flow".into(), "{}".into(), None)
        .unwrap();

    // 1. show_terms wait
    wait_for_state(&engine, &id, InstanceStateKind::Waiting, 50);
    complete_waiting_step(&engine, &id, "show_terms", "accepted");

    // 2. collect_preferences wait (no explicit choices → defaults to yes/no)
    wait_for_state(&engine, &id, InstanceStateKind::Waiting, 50);
    complete_waiting_step(&engine, &id, "collect_preferences", "yes");

    // 3. setup_notifications wait
    wait_for_state(&engine, &id, InstanceStateKind::Waiting, 50);
    complete_waiting_step(&engine, &id, "setup_notifications", "enabled");

    // 4. admin_approval wait
    wait_for_state(&engine, &id, InstanceStateKind::Waiting, 50);
    complete_waiting_step(&engine, &id, "admin_approval", "approved");

    wait_for_state(&engine, &id, InstanceStateKind::Completed, 50);
    engine.shutdown();
}

#[test]
fn onboarding_reject_path() {
    let engine = make_engine();
    let id = engine
        .start("onboarding-flow".into(), "{}".into(), None)
        .unwrap();

    // Accept terms
    wait_for_state(&engine, &id, InstanceStateKind::Waiting, 50);
    complete_waiting_step(&engine, &id, "show_terms", "accepted");

    // Preferences
    wait_for_state(&engine, &id, InstanceStateKind::Waiting, 50);
    complete_waiting_step(&engine, &id, "collect_preferences", "yes");

    // Notifications
    wait_for_state(&engine, &id, InstanceStateKind::Waiting, 50);
    complete_waiting_step(&engine, &id, "setup_notifications", "enabled");

    // Reject at admin approval
    wait_for_state(&engine, &id, InstanceStateKind::Waiting, 50);
    complete_waiting_step(&engine, &id, "admin_approval", "rejected");

    wait_for_state(&engine, &id, InstanceStateKind::Completed, 50);
    engine.shutdown();
}

#[test]
fn onboarding_decline_terms_path() {
    let engine = make_engine();
    let id = engine
        .start("onboarding-flow".into(), "{}".into(), None)
        .unwrap();

    // Decline terms → routes to show_decline_notice → completes
    wait_for_state(&engine, &id, InstanceStateKind::Waiting, 50);
    complete_waiting_step(&engine, &id, "show_terms", "declined");

    wait_for_state(&engine, &id, InstanceStateKind::Completed, 50);
    engine.shutdown();
}

// ---------------------------------------------------------------------------
// Payment verification
// ---------------------------------------------------------------------------

#[test]
fn payment_approve_path() {
    let engine = make_engine();
    let id = engine
        .start("payment-verification".into(), "{}".into(), None)
        .unwrap();

    wait_for_state(&engine, &id, InstanceStateKind::Waiting, 50);
    complete_waiting_step(&engine, &id, "payment_approval", "approved");

    wait_for_state(&engine, &id, InstanceStateKind::Completed, 50);
    engine.shutdown();
}

#[test]
fn payment_reject_path() {
    let engine = make_engine();
    let id = engine
        .start("payment-verification".into(), "{}".into(), None)
        .unwrap();

    wait_for_state(&engine, &id, InstanceStateKind::Waiting, 50);
    complete_waiting_step(&engine, &id, "payment_approval", "rejected");

    wait_for_state(&engine, &id, InstanceStateKind::Completed, 50);
    engine.shutdown();
}

// ---------------------------------------------------------------------------
// Feature access
// ---------------------------------------------------------------------------

#[test]
fn feature_access_full_approve_path() {
    let engine = make_engine();
    let id = engine
        .start("feature-access".into(), "{}".into(), None)
        .unwrap();

    // Consent
    wait_for_state(&engine, &id, InstanceStateKind::Waiting, 50);
    complete_waiting_step(&engine, &id, "consent_request", "consented");

    // Access approval
    wait_for_state(&engine, &id, InstanceStateKind::Waiting, 50);
    complete_waiting_step(&engine, &id, "access_approval", "approved");

    wait_for_state(&engine, &id, InstanceStateKind::Completed, 50);
    engine.shutdown();
}

#[test]
fn feature_access_deny_path() {
    let engine = make_engine();
    let id = engine
        .start("feature-access".into(), "{}".into(), None)
        .unwrap();

    // Consent
    wait_for_state(&engine, &id, InstanceStateKind::Waiting, 50);
    complete_waiting_step(&engine, &id, "consent_request", "consented");

    // Deny access
    wait_for_state(&engine, &id, InstanceStateKind::Waiting, 50);
    complete_waiting_step(&engine, &id, "access_approval", "denied");

    wait_for_state(&engine, &id, InstanceStateKind::Completed, 50);
    engine.shutdown();
}

#[test]
fn feature_access_decline_consent_path() {
    let engine = make_engine();
    let id = engine
        .start("feature-access".into(), "{}".into(), None)
        .unwrap();

    // Decline consent → routes to show_limited_access_banner → completes
    wait_for_state(&engine, &id, InstanceStateKind::Waiting, 50);
    complete_waiting_step(&engine, &id, "consent_request", "declined");

    wait_for_state(&engine, &id, InstanceStateKind::Completed, 50);
    engine.shutdown();
}
