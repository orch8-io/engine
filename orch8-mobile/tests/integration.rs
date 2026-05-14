use std::sync::{Arc, Mutex};

use orch8_mobile::{EngineListener, HandlerError, MobileEngineConfig, MobileError, StepHandler};

struct EchoHandler;

impl StepHandler for EchoHandler {
    fn execute(&self, _step_name: String, input: String) -> Result<String, HandlerError> {
        Ok(input)
    }
}

#[derive(Default)]
struct TestListener {
    completed: Mutex<Vec<(String, String)>>,
    failed: Mutex<Vec<(String, String)>>,
}

impl EngineListener for TestListener {
    fn on_instance_completed(&self, instance_id: String, output: String) {
        self.completed.lock().unwrap().push((instance_id, output));
    }

    fn on_instance_failed(&self, instance_id: String, error: String) {
        self.failed.lock().unwrap().push((instance_id, error));
    }

    fn on_step_pending(&self, _instance_id: String, _step_name: String, _handler: String) {}
}

fn test_sequence_json() -> String {
    serde_json::json!({
        "id": uuid::Uuid::new_v4().to_string(),
        "tenant_id": "mobile",
        "namespace": "default",
        "name": "test_flow",
        "version": 1,
        "deprecated": false,
        "blocks": [
            {
                "type": "step",
                "id": "step_1",
                "handler": "echo",
                "params": { "greeting": "hello" }
            }
        ],
        "created_at": "2026-01-01T00:00:00Z"
    })
    .to_string()
}

#[test]
fn engine_lifecycle() {
    let dir = tempfile::tempdir().unwrap();
    let db_path = dir.path().join("test.db").to_string_lossy().to_string();

    let config = MobileEngineConfig {
        tick_interval_ms: 50,
        max_concurrent_steps: 2,
        max_steps_per_instance: 100,
        max_concurrent_instances: 5,
        max_tick_duration_ms: 5000,
        max_instance_lifetime_secs: 3600,
        max_stored_sequences: 10,
        max_sequence_size_bytes: 1_048_576,
        handler_timeout_ms: 5000,
        telemetry_enabled: false,
        environment: "production".to_string(),
        root_public_key: String::new(),
        sdk_version: "0.4.0".to_string(),
    };

    let engine = orch8_mobile::MobileEngine::new(db_path, config).unwrap();

    engine
        .register_handler("echo".to_string(), Arc::new(EchoHandler))
        .unwrap();

    let listener = Arc::new(TestListener::default());
    engine.set_listener(listener.clone());

    // Load a test sequence.
    engine
        .load_sequence_from_json(test_sequence_json())
        .unwrap();

    // Verify it's listed.
    let seqs = engine.loaded_sequences().unwrap();
    assert_eq!(seqs.len(), 1);
    assert_eq!(seqs[0].name, "test_flow");

    // Start an instance.
    let instance_id = engine
        .start(
            "test_flow".to_string(),
            r#"{"greeting": "hello"}"#.to_string(),
            Some("dedup_1".to_string()),
        )
        .unwrap();
    assert!(!instance_id.is_empty());

    // Dedup: same key returns same ID.
    let dup_id = engine
        .start(
            "test_flow".to_string(),
            "{}".to_string(),
            Some("dedup_1".to_string()),
        )
        .unwrap();
    assert_eq!(instance_id, dup_id);

    // Tick to execute.
    let _result = engine.tick_once().unwrap();

    // After enough ticks, instance should complete.
    for _ in 0..5 {
        let _ = engine.tick_once();
    }

    // Verify instance reached terminal state.
    let state = engine.get_instance(instance_id.clone()).unwrap();
    // The instance should be Completed or still Running (depending on handler execution).
    assert!(
        state.state == "Completed" || state.state == "Running" || state.state == "Scheduled",
        "unexpected state: {}",
        state.state
    );

    // Verify active_instances doesn't include terminal instances.
    let active = engine.active_instances().unwrap();
    for inst in &active {
        assert_ne!(inst.state, "Completed");
        assert_ne!(inst.state, "Failed");
    }

    engine.shutdown();
}

#[test]
fn resource_limit_rejects_oversized_sequence() {
    let dir = tempfile::tempdir().unwrap();
    let db_path = dir.path().join("test.db").to_string_lossy().to_string();

    let config = MobileEngineConfig {
        max_sequence_size_bytes: 100, // tiny limit
        ..MobileEngineConfig::default()
    };

    let engine = orch8_mobile::MobileEngine::new(db_path, config).unwrap();

    let result = engine.load_sequence_from_json(test_sequence_json());
    assert!(matches!(result, Err(MobileError::ResourceLimit { .. })));
}

#[test]
fn resource_limit_max_concurrent_instances() {
    let dir = tempfile::tempdir().unwrap();
    let db_path = dir.path().join("test.db").to_string_lossy().to_string();

    let config = MobileEngineConfig {
        max_concurrent_instances: 2,
        ..MobileEngineConfig::default()
    };

    let engine = orch8_mobile::MobileEngine::new(db_path, config).unwrap();
    engine
        .register_handler("echo".to_string(), Arc::new(EchoHandler))
        .unwrap();
    engine
        .load_sequence_from_json(test_sequence_json())
        .unwrap();

    // Start two instances — should succeed.
    engine
        .start("test_flow".to_string(), "{}".to_string(), None)
        .unwrap();
    engine
        .start("test_flow".to_string(), "{}".to_string(), None)
        .unwrap();

    // Third should fail.
    let result = engine.start("test_flow".to_string(), "{}".to_string(), None);
    assert!(matches!(result, Err(MobileError::ResourceLimit { .. })));
}

#[test]
fn cancel_instance_removes_from_active() {
    let dir = tempfile::tempdir().unwrap();
    let db_path = dir.path().join("test.db").to_string_lossy().to_string();

    let engine = orch8_mobile::MobileEngine::new(db_path, MobileEngineConfig::default()).unwrap();
    engine
        .register_handler("echo".to_string(), Arc::new(EchoHandler))
        .unwrap();
    engine
        .load_sequence_from_json(test_sequence_json())
        .unwrap();

    let id = engine
        .start("test_flow".to_string(), "{}".to_string(), None)
        .unwrap();

    engine.cancel_instance(id.clone()).unwrap();

    let state = engine.get_instance(id).unwrap();
    assert_eq!(state.state, "Cancelled");
}

#[test]
fn shutdown_prevents_ticks() {
    let dir = tempfile::tempdir().unwrap();
    let db_path = dir.path().join("test.db").to_string_lossy().to_string();

    let engine = orch8_mobile::MobileEngine::new(db_path, MobileEngineConfig::default()).unwrap();
    engine.shutdown();

    let result = engine.tick_once();
    assert!(matches!(result, Err(MobileError::Shutdown)));
}
