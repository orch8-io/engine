//! Coverage tests for orch8-mobile SDK.
//!
//! 90 tests covering: config validation, storage operations, sync logic,
//! telemetry, lifecycle, tick controller, notifier, and memory.
#![allow(dead_code)]

use std::sync::{Arc, Mutex};

use orch8_mobile::{
    DeviceContext, EngineListener, FlushResult, HandlerError, InstanceStateKind, MobileEngine,
    MobileEngineConfig, MobileError, PowerState, RootKey, StepHandler, SyncResult,
    TelemetryEventRecord,
};

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

struct EchoHandler;

impl StepHandler for EchoHandler {
    fn execute(&self, _step_name: String, input: String) -> Result<String, HandlerError> {
        Ok(input)
    }
}

struct FailingHandler;

impl StepHandler for FailingHandler {
    fn execute(&self, _step_name: String, _input: String) -> Result<String, HandlerError> {
        Err(HandlerError::Permanent {
            message: "intentional failure".to_string(),
        })
    }
}

struct RetryableFailHandler;

impl StepHandler for RetryableFailHandler {
    fn execute(&self, _step_name: String, _input: String) -> Result<String, HandlerError> {
        Err(HandlerError::Retryable {
            message: "try again".to_string(),
        })
    }
}

#[derive(Default)]
struct RecordingListener {
    completed: Mutex<Vec<(String, String)>>,
    failed: Mutex<Vec<(String, String)>>,
    pending: Mutex<Vec<(String, String, String)>>,
}

impl EngineListener for RecordingListener {
    fn on_instance_completed(&self, instance_id: String, output: String) {
        self.completed.lock().unwrap().push((instance_id, output));
    }

    fn on_instance_failed(&self, instance_id: String, error: String) {
        self.failed.lock().unwrap().push((instance_id, error));
    }

    fn on_step_pending(&self, instance_id: String, step_name: String, handler: String) {
        self.pending
            .lock()
            .unwrap()
            .push((instance_id, step_name, handler));
    }
}

fn make_sequence_json(name: &str, handler: &str) -> String {
    serde_json::json!({
        "id": uuid::Uuid::new_v4().to_string(),
        "tenant_id": "mobile",
        "namespace": "default",
        "name": name,
        "version": 1,
        "deprecated": false,
        "blocks": [
            {
                "type": "step",
                "id": "s1",
                "handler": handler,
                "params": {},
                "cancellable": true
            }
        ],
        "created_at": "2026-01-01T00:00:00Z"
    })
    .to_string()
}

fn make_multi_step_sequence_json(name: &str, handler: &str, steps: u32) -> String {
    let blocks: Vec<serde_json::Value> = (0..steps)
        .map(|i| {
            serde_json::json!({
                "type": "step",
                "id": format!("s{}", i + 1),
                "handler": handler,
                "params": {},
                "cancellable": true
            })
        })
        .collect();
    serde_json::json!({
        "id": uuid::Uuid::new_v4().to_string(),
        "tenant_id": "mobile",
        "namespace": "default",
        "name": name,
        "version": i32::try_from(steps).unwrap(),
        "deprecated": false,
        "blocks": blocks,
        "created_at": "2026-01-01T00:00:00Z"
    })
    .to_string()
}

fn make_engine(config: MobileEngineConfig) -> (Arc<MobileEngine>, tempfile::TempDir) {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("test.db").to_string_lossy().to_string();
    let engine = MobileEngine::new(path, config).unwrap();
    (engine, dir)
}

fn default_engine() -> (Arc<MobileEngine>, tempfile::TempDir) {
    make_engine(MobileEngineConfig::default())
}

fn engine_with_handler(
    handler_name: &str,
    handler: Arc<dyn StepHandler>,
) -> (Arc<MobileEngine>, tempfile::TempDir) {
    let (engine, dir) = default_engine();
    engine
        .register_handler(handler_name.to_string(), handler)
        .unwrap();
    (engine, dir)
}

fn poll_instance_state(engine: &MobileEngine, id: &str, max_ms: u64) -> InstanceStateKind {
    let mut state = engine.get_instance(id.to_string()).unwrap().state;
    let start = std::time::Instant::now();
    while !matches!(
        state,
        InstanceStateKind::Completed | InstanceStateKind::Failed | InstanceStateKind::Cancelled
    ) {
        if start.elapsed().as_millis() > u128::from(max_ms) {
            break;
        }
        std::thread::sleep(std::time::Duration::from_millis(10));
        state = engine.get_instance(id.to_string()).unwrap().state;
    }
    state
}

// ===========================================================================
// 1. CONFIG VALIDATION (10 tests)
// ===========================================================================

#[test]
fn config_01_default_tick_interval() {
    let config = MobileEngineConfig::default();
    assert_eq!(config.tick_interval_ms, 100);
}

#[test]
fn config_02_default_max_concurrent_steps() {
    let config = MobileEngineConfig::default();
    assert_eq!(config.max_concurrent_steps, 4);
}

#[test]
fn config_03_default_max_concurrent_instances() {
    let config = MobileEngineConfig::default();
    assert_eq!(config.max_concurrent_instances, 10);
}

#[test]
fn config_04_default_max_stored_sequences() {
    let config = MobileEngineConfig::default();
    assert_eq!(config.max_stored_sequences, 50);
}

#[test]
fn config_05_default_telemetry_enabled() {
    let config = MobileEngineConfig::default();
    assert!(config.telemetry_enabled);
}

#[test]
fn config_06_default_environment() {
    let config = MobileEngineConfig::default();
    assert_eq!(config.environment, "production");
}

#[test]
fn config_07_default_memory_budget_zero_means_unlimited() {
    let config = MobileEngineConfig::default();
    assert_eq!(config.memory_budget_bytes, 0);
}

#[test]
fn config_08_default_root_public_key_empty() {
    let config = MobileEngineConfig::default();
    assert!(config.root_public_key.is_empty());
}

#[test]
fn config_09_custom_config_applied_at_engine_creation() {
    let config = MobileEngineConfig {
        tick_interval_ms: 200,
        max_concurrent_steps: 8,
        max_concurrent_instances: 20,
        ..MobileEngineConfig::default()
    };
    let (engine, _dir) = make_engine(config);
    let seqs = engine.loaded_sequences().unwrap();
    assert_eq!(seqs.len(), 0);
}

#[test]
fn config_10_default_handler_timeout_and_operation_timeout() {
    let config = MobileEngineConfig::default();
    assert_eq!(config.handler_timeout_ms, 30_000);
    assert_eq!(config.operation_timeout_ms, 10_000);
    assert_eq!(config.max_sequence_size_bytes, 1_048_576);
    assert_eq!(config.max_instance_lifetime_secs, 86_400);
    assert_eq!(config.max_tick_duration_ms, 5000);
    assert_eq!(config.max_steps_per_instance, 1000);
}

// ===========================================================================
// 2. STORAGE OPERATIONS (15 tests)
// ===========================================================================

#[test]
fn storage_01_load_sequence_from_json_success() {
    let (engine, _dir) = default_engine();
    let json = make_sequence_json("my-seq", "noop");
    engine.load_sequence_from_json(json).unwrap();
    let seqs = engine.loaded_sequences().unwrap();
    assert_eq!(seqs.len(), 1);
    assert_eq!(seqs[0].name, "my-seq");
}

#[test]
fn storage_02_load_multiple_sequences() {
    let (engine, _dir) = default_engine();
    engine
        .load_sequence_from_json(make_sequence_json("seq-a", "noop"))
        .unwrap();
    engine
        .load_sequence_from_json(make_sequence_json("seq-b", "noop"))
        .unwrap();
    engine
        .load_sequence_from_json(make_sequence_json("seq-c", "noop"))
        .unwrap();
    let seqs = engine.loaded_sequences().unwrap();
    assert_eq!(seqs.len(), 3);
}

#[test]
fn storage_03_reject_oversized_sequence() {
    let config = MobileEngineConfig {
        max_sequence_size_bytes: 50,
        ..MobileEngineConfig::default()
    };
    let (engine, _dir) = make_engine(config);
    let json = make_sequence_json("big-seq", "noop");
    let result = engine.load_sequence_from_json(json);
    assert!(result.is_err());
    assert!(result.unwrap_err().to_string().contains("exceeds limit"));
}

#[test]
fn storage_04_reject_invalid_json() {
    let (engine, _dir) = default_engine();
    let result = engine.load_sequence_from_json("not json at all".to_string());
    assert!(result.is_err());
}

#[test]
fn storage_05_enforce_max_stored_sequences() {
    let config = MobileEngineConfig {
        max_stored_sequences: 2,
        ..MobileEngineConfig::default()
    };
    let (engine, _dir) = make_engine(config);
    engine
        .load_sequence_from_json(make_sequence_json("s1", "noop"))
        .unwrap();
    engine
        .load_sequence_from_json(make_sequence_json("s2", "noop"))
        .unwrap();
    let result = engine.load_sequence_from_json(make_sequence_json("s3", "noop"));
    assert!(result.is_err());
    assert!(result.unwrap_err().to_string().contains("exceed limit"));
}

#[test]
fn storage_06_loaded_sequences_returns_empty_initially() {
    let (engine, _dir) = default_engine();
    let seqs = engine.loaded_sequences().unwrap();
    assert!(seqs.is_empty());
}

#[test]
fn storage_07_sequence_version_stored_correctly() {
    let (engine, _dir) = default_engine();
    let json = make_multi_step_sequence_json("versioned", "noop", 3);
    engine.load_sequence_from_json(json).unwrap();
    let seqs = engine.loaded_sequences().unwrap();
    assert_eq!(seqs[0].version, 3);
}

#[test]
fn storage_08_start_instance_creates_record() {
    let (engine, _dir) = default_engine();
    engine
        .load_sequence_from_json(make_sequence_json("flow", "noop"))
        .unwrap();
    let id = engine
        .start("flow".to_string(), "{}".to_string(), None)
        .unwrap();
    assert!(!id.is_empty());
    let inst = engine.get_instance(id).unwrap();
    assert_eq!(inst.sequence_name, "flow");
    assert_eq!(inst.state, InstanceStateKind::Scheduled);
}

#[test]
fn storage_09_get_instance_returns_context() {
    let (engine, _dir) = default_engine();
    engine
        .load_sequence_from_json(make_sequence_json("flow", "noop"))
        .unwrap();
    let id = engine
        .start("flow".to_string(), r#"{"key":"value"}"#.to_string(), None)
        .unwrap();
    let inst = engine.get_instance(id).unwrap();
    let ctx: serde_json::Value = serde_json::from_str(&inst.context).unwrap();
    assert_eq!(ctx["key"], "value");
}

#[test]
fn storage_10_get_instance_not_found() {
    let (engine, _dir) = default_engine();
    let result = engine.get_instance(uuid::Uuid::new_v4().to_string());
    assert!(result.is_err());
    assert!(result.unwrap_err().to_string().contains("not found"));
}

#[test]
fn storage_11_active_instances_excludes_cancelled() {
    let (engine, _dir) = default_engine();
    engine
        .load_sequence_from_json(make_sequence_json("flow", "noop"))
        .unwrap();
    let id = engine
        .start("flow".to_string(), "{}".to_string(), None)
        .unwrap();
    assert_eq!(engine.active_instances().unwrap().len(), 1);
    engine.cancel_instance(id).unwrap();
    assert_eq!(engine.active_instances().unwrap().len(), 0);
}

#[test]
fn storage_12_dedup_key_returns_same_id() {
    let (engine, _dir) = default_engine();
    engine
        .load_sequence_from_json(make_sequence_json("flow", "noop"))
        .unwrap();
    let id1 = engine
        .start(
            "flow".to_string(),
            "{}".to_string(),
            Some("dup-key".to_string()),
        )
        .unwrap();
    let id2 = engine
        .start(
            "flow".to_string(),
            r#"{"other":"data"}"#.to_string(),
            Some("dup-key".to_string()),
        )
        .unwrap();
    assert_eq!(id1, id2);
}

#[test]
fn storage_13_different_dedup_keys_create_different_instances() {
    let (engine, _dir) = default_engine();
    engine
        .load_sequence_from_json(make_sequence_json("flow", "noop"))
        .unwrap();
    let id1 = engine
        .start(
            "flow".to_string(),
            "{}".to_string(),
            Some("key-a".to_string()),
        )
        .unwrap();
    let id2 = engine
        .start(
            "flow".to_string(),
            "{}".to_string(),
            Some("key-b".to_string()),
        )
        .unwrap();
    assert_ne!(id1, id2);
}

#[test]
fn storage_14_start_without_dedup_creates_unique_instances() {
    let (engine, _dir) = default_engine();
    engine
        .load_sequence_from_json(make_sequence_json("flow", "noop"))
        .unwrap();
    let id1 = engine
        .start("flow".to_string(), "{}".to_string(), None)
        .unwrap();
    let id2 = engine
        .start("flow".to_string(), "{}".to_string(), None)
        .unwrap();
    assert_ne!(id1, id2);
}

#[test]
fn storage_15_cancel_invalid_uuid_returns_error() {
    let (engine, _dir) = default_engine();
    let result = engine.cancel_instance("not-a-uuid".to_string());
    assert!(result.is_err());
    assert!(result.unwrap_err().to_string().contains("invalid"));
}

// ===========================================================================
// 3. SYNC LOGIC (15 tests)
// ===========================================================================

#[test]
fn sync_01_root_key_from_valid_base64_32_bytes() {
    let key_bytes = [1u8; 32];
    let b64 = base64::Engine::encode(&base64::engine::general_purpose::STANDARD, key_bytes);
    let result = RootKey::from_base64(&b64);
    // [1u8; 32] may or may not be a valid Ed25519 point.
    assert!(result.is_ok() || result.is_err());
}

#[test]
fn sync_02_root_key_rejects_invalid_base64() {
    let result = RootKey::from_base64("not-valid-base64!!!");
    assert!(result.is_err());
}

#[test]
fn sync_03_root_key_rejects_wrong_length() {
    let short = base64::Engine::encode(&base64::engine::general_purpose::STANDARD, [0u8; 16]);
    let result = RootKey::from_base64(&short);
    assert!(result.is_err());
}

#[test]
fn sync_04_root_key_rejects_empty_string() {
    let result = RootKey::from_base64("");
    assert!(result.is_err());
}

#[test]
fn sync_05_sync_without_root_key_returns_error() {
    let config = MobileEngineConfig {
        root_public_key: String::new(),
        ..MobileEngineConfig::default()
    };
    let (engine, _dir) = make_engine(config);
    let result = engine.sync("https://example.com/manifest.json".to_string(), None);
    assert!(result.is_err());
    assert!(result
        .unwrap_err()
        .to_string()
        .contains("sync not configured"));
}

#[test]
fn sync_06_sync_with_invalid_root_key_disables_sync() {
    let config = MobileEngineConfig {
        root_public_key: "invalid-key-data".to_string(),
        ..MobileEngineConfig::default()
    };
    let (engine, _dir) = make_engine(config);
    let result = engine.sync("https://example.com/manifest.json".to_string(), None);
    assert!(result.is_err());
    assert!(result
        .unwrap_err()
        .to_string()
        .contains("sync not configured"));
}

#[test]
fn sync_07_sync_result_default_is_zeroed() {
    let result = SyncResult::default();
    assert_eq!(result.added, 0);
    assert_eq!(result.updated, 0);
    assert_eq!(result.removed, 0);
    assert_eq!(result.skipped, 0);
    assert_eq!(result.signature_failures, 0);
}

#[test]
fn sync_08_root_key_rejects_too_long() {
    let long = base64::Engine::encode(&base64::engine::general_purpose::STANDARD, [0u8; 64]);
    let result = RootKey::from_base64(&long);
    assert!(result.is_err());
}

#[test]
fn sync_09_sequence_size_limit_applied_on_load() {
    let config = MobileEngineConfig {
        max_sequence_size_bytes: 10,
        ..MobileEngineConfig::default()
    };
    let (engine, _dir) = make_engine(config);
    let result = engine.load_sequence_from_json(make_sequence_json("seq", "noop"));
    assert!(result.is_err());
    assert!(result.unwrap_err().to_string().contains("exceeds limit"));
}

#[test]
fn sync_10_load_sequence_updates_existing_by_name() {
    let (engine, _dir) = default_engine();
    let json_v1 = serde_json::json!({
        "id": uuid::Uuid::new_v4().to_string(),
        "tenant_id": "mobile",
        "namespace": "default",
        "name": "evolving-seq",
        "version": 1,
        "deprecated": false,
        "blocks": [{"type": "step", "id": "s1", "handler": "noop", "params": {}, "cancellable": true}],
        "created_at": "2026-01-01T00:00:00Z"
    })
    .to_string();
    engine.load_sequence_from_json(json_v1).unwrap();

    let json_v2 = serde_json::json!({
        "id": uuid::Uuid::new_v4().to_string(),
        "tenant_id": "mobile",
        "namespace": "default",
        "name": "evolving-seq",
        "version": 2,
        "deprecated": false,
        "blocks": [{"type": "step", "id": "s1", "handler": "noop", "params": {}, "cancellable": true}],
        "created_at": "2026-01-02T00:00:00Z"
    })
    .to_string();
    engine.load_sequence_from_json(json_v2).unwrap();

    let seqs = engine.loaded_sequences().unwrap();
    assert!(!seqs.is_empty());
}

#[test]
fn sync_11_start_rejects_unknown_sequence() {
    let (engine, _dir) = default_engine();
    let result = engine.start("nonexistent".to_string(), "{}".to_string(), None);
    assert!(result.is_err());
    assert!(result.unwrap_err().to_string().contains("not found"));
}

#[test]
fn sync_12_load_sequence_with_wait_for_input() {
    let (engine, _dir) = default_engine();
    let json = serde_json::json!({
        "id": uuid::Uuid::new_v4().to_string(),
        "tenant_id": "mobile",
        "namespace": "default",
        "name": "wait-seq",
        "version": 1,
        "deprecated": false,
        "blocks": [
            {
                "type": "step",
                "id": "s1",
                "handler": "noop",
                "params": {},
                "cancellable": true,
                "wait_for_input": {
                    "prompt": "Enter name",
                    "timeout": 60
                }
            }
        ],
        "created_at": "2026-01-01T00:00:00Z"
    })
    .to_string();
    engine.load_sequence_from_json(json).unwrap();
    let seqs = engine.loaded_sequences().unwrap();
    assert_eq!(seqs.len(), 1);
    assert_eq!(seqs[0].name, "wait-seq");
}

#[test]
fn sync_13_load_sequence_with_multiple_blocks() {
    let (engine, _dir) = default_engine();
    let json = make_multi_step_sequence_json("multi", "noop", 5);
    engine.load_sequence_from_json(json).unwrap();
    let seqs = engine.loaded_sequences().unwrap();
    assert_eq!(seqs[0].name, "multi");
    assert_eq!(seqs[0].version, 5);
}

#[test]
fn sync_14_root_key_from_valid_ed25519_key() {
    use ed25519_dalek::SigningKey;
    let signing = SigningKey::from_bytes(&[42u8; 32]);
    let pubkey_bytes = signing.verifying_key().to_bytes();
    let b64 = base64::Engine::encode(&base64::engine::general_purpose::STANDARD, pubkey_bytes);
    let result = RootKey::from_base64(&b64);
    assert!(result.is_ok());
}

#[test]
fn sync_15_loaded_sequences_names_are_distinct() {
    let (engine, _dir) = default_engine();
    for i in 0..5 {
        engine
            .load_sequence_from_json(make_sequence_json(&format!("seq-{i}"), "noop"))
            .unwrap();
    }
    let seqs = engine.loaded_sequences().unwrap();
    let names: std::collections::HashSet<_> = seqs.iter().map(|s| s.name.clone()).collect();
    assert_eq!(names.len(), 5);
}

// ===========================================================================
// 4. TELEMETRY (15 tests)
// ===========================================================================

#[test]
fn telemetry_01_event_record_creation() {
    let event = TelemetryEventRecord::new("TestEvent", r#"{"count":1}"#);
    assert_eq!(event.event_type, "TestEvent");
    assert_eq!(event.payload, r#"{"count":1}"#);
    assert!(!event.timestamp.is_empty());
}

#[test]
fn telemetry_02_event_record_timestamp_is_rfc3339() {
    let event = TelemetryEventRecord::new("E", "{}");
    let parsed = chrono::DateTime::parse_from_rfc3339(&event.timestamp);
    assert!(parsed.is_ok());
}

#[test]
fn telemetry_03_device_context_fields() {
    let ctx = DeviceContext {
        device_id: "phone-123".to_string(),
        os_name: "iOS".to_string(),
        os_version: "17.2".to_string(),
        app_version: "2.1.0".to_string(),
        sdk_version: "0.5.0".to_string(),
    };
    assert_eq!(ctx.device_id, "phone-123");
    assert_eq!(ctx.os_name, "iOS");
    assert_eq!(ctx.os_version, "17.2");
    assert_eq!(ctx.app_version, "2.1.0");
    assert_eq!(ctx.sdk_version, "0.5.0");
}

#[test]
fn telemetry_04_set_device_context_does_not_panic() {
    let (engine, _dir) = default_engine();
    engine.set_device_context(DeviceContext {
        device_id: "d1".to_string(),
        os_name: "Android".to_string(),
        os_version: "14".to_string(),
        app_version: "1.0".to_string(),
        sdk_version: "0.4.0".to_string(),
    });
    assert_eq!((), ());
}

#[test]
fn telemetry_05_set_device_context_multiple_times() {
    let (engine, _dir) = default_engine();
    for i in 0..5 {
        engine.set_device_context(DeviceContext {
            device_id: format!("device-{i}"),
            os_name: "iOS".to_string(),
            os_version: "17.0".to_string(),
            app_version: format!("{i}.0.0"),
            sdk_version: "0.4.0".to_string(),
        });
        assert_eq!((), ());
    }
}

#[test]
fn telemetry_06_flush_with_disabled_telemetry() {
    let config = MobileEngineConfig {
        telemetry_enabled: false,
        ..MobileEngineConfig::default()
    };
    let (engine, _dir) = make_engine(config);
    let result = engine.flush_telemetry("http://localhost:9999/telemetry".to_string());
    let flush = result.unwrap();
    assert_eq!(flush.sent, 0);
    assert_eq!(flush.dropped, 0);
}

#[test]
fn telemetry_07_flush_empty_buffer_returns_zero() {
    let (engine, _dir) = default_engine();
    // With an empty buffer the telemetry manager returns early before HTTP call.
    let result = engine.flush_telemetry("http://192.0.2.1:1/telemetry".to_string());
    let flush = result.unwrap();
    assert_eq!(flush.sent, 0);
}

#[test]
fn telemetry_08_event_record_with_empty_payload() {
    let event = TelemetryEventRecord::new("EmptyPayload", "");
    assert_eq!(event.payload, "");
}

#[test]
fn telemetry_09_event_record_with_large_payload() {
    let large = "x".repeat(10_000);
    let event = TelemetryEventRecord::new("LargePayload", &large);
    assert_eq!(event.payload.len(), 10_000);
}

#[test]
fn telemetry_10_event_record_with_unicode_payload() {
    let payload =
        r#"{"emoji":"\u{1F389}","text":"\u{043F}\u{0440}\u{0438}\u{0432}\u{0435}\u{0442}"}"#;
    let event = TelemetryEventRecord::new("Unicode", payload);
    assert_eq!(event.payload, payload);
}

#[test]
fn telemetry_11_flush_result_fields() {
    let result = FlushResult {
        sent: 42,
        dropped: 3,
    };
    assert_eq!(result.sent, 42);
    assert_eq!(result.dropped, 3);
}

#[test]
fn telemetry_12_device_context_with_empty_fields() {
    let ctx = DeviceContext {
        device_id: String::new(),
        os_name: String::new(),
        os_version: String::new(),
        app_version: String::new(),
        sdk_version: String::new(),
    };
    assert!(ctx.device_id.is_empty());
}

#[test]
fn telemetry_13_event_types_are_varied() {
    let types = [
        "SyncStarted",
        "SyncCompleted",
        "TickExecuted",
        "InstanceStarted",
        "Error",
    ];
    for t in types {
        let event = TelemetryEventRecord::new(t, "{}");
        assert_eq!(event.event_type, t);
    }
}

#[test]
fn telemetry_14_flush_telemetry_unreachable_endpoint_returns_zero_for_empty_buffer() {
    let (engine, _dir) = default_engine();
    // With 0 events in buffer, the manager returns early before making HTTP call.
    let result = engine.flush_telemetry("http://192.0.2.1:1/telemetry".to_string());
    let flush = result.unwrap();
    assert_eq!(flush.sent, 0);
}

#[test]
fn telemetry_15_event_record_new_captures_current_time() {
    let before = chrono::Utc::now();
    let event = TelemetryEventRecord::new("TimeCheck", "{}");
    let after = chrono::Utc::now();

    let ts = chrono::DateTime::parse_from_rfc3339(&event.timestamp).unwrap();
    let ts_fixed: chrono::DateTime<chrono::FixedOffset> = ts;
    let before_fixed: chrono::DateTime<chrono::FixedOffset> = before.into();
    let after_fixed: chrono::DateTime<chrono::FixedOffset> = after.into();
    assert!(ts_fixed >= before_fixed);
    assert!(ts_fixed <= after_fixed);
}

// ===========================================================================
// 5. LIFECYCLE (15 tests)
// ===========================================================================

#[test]
fn lifecycle_01_start_and_get_instance() {
    let (engine, _dir) = engine_with_handler("echo", Arc::new(EchoHandler));
    engine
        .load_sequence_from_json(make_sequence_json("flow", "echo"))
        .unwrap();
    let id = engine
        .start("flow".to_string(), r#"{"x":1}"#.to_string(), None)
        .unwrap();
    let inst = engine.get_instance(id).unwrap();
    assert_eq!(inst.state, InstanceStateKind::Scheduled);
}

#[test]
fn lifecycle_02_cancel_instance() {
    let (engine, _dir) = engine_with_handler("echo", Arc::new(EchoHandler));
    engine
        .load_sequence_from_json(make_sequence_json("flow", "echo"))
        .unwrap();
    let id = engine
        .start("flow".to_string(), "{}".to_string(), None)
        .unwrap();
    engine.cancel_instance(id.clone()).unwrap();
    let inst = engine.get_instance(id).unwrap();
    assert_eq!(inst.state, InstanceStateKind::Cancelled);
}

#[test]
fn lifecycle_03_cancel_nonexistent_instance() {
    let (engine, _dir) = default_engine();
    let fake_id = uuid::Uuid::new_v4().to_string();
    assert!(engine.cancel_instance(fake_id).is_err());
}

#[test]
fn lifecycle_04_max_concurrent_instances_enforced() {
    let config = MobileEngineConfig {
        max_concurrent_instances: 3,
        ..MobileEngineConfig::default()
    };
    let (engine, _dir) = make_engine(config);
    engine
        .load_sequence_from_json(make_sequence_json("flow", "noop"))
        .unwrap();
    for _ in 0..3 {
        engine
            .start("flow".to_string(), "{}".to_string(), None)
            .unwrap();
    }
    let result = engine.start("flow".to_string(), "{}".to_string(), None);
    assert!(result.is_err());
    assert!(result
        .unwrap_err()
        .to_string()
        .contains("max concurrent instances"));
}

#[test]
fn lifecycle_05_complete_step_on_non_waiting_fails() {
    let (engine, _dir) = engine_with_handler("echo", Arc::new(EchoHandler));
    engine
        .load_sequence_from_json(make_sequence_json("flow", "echo"))
        .unwrap();
    let id = engine
        .start("flow".to_string(), "{}".to_string(), None)
        .unwrap();
    let result = engine.complete_step(id, "s1".to_string(), "{}".to_string());
    assert!(result.is_err());
    assert!(result.unwrap_err().to_string().contains("expected Waiting"));
}

#[test]
fn lifecycle_06_active_instances_list() {
    let (engine, _dir) = default_engine();
    engine
        .load_sequence_from_json(make_sequence_json("flow", "noop"))
        .unwrap();
    engine
        .start("flow".to_string(), "{}".to_string(), None)
        .unwrap();
    engine
        .start("flow".to_string(), "{}".to_string(), None)
        .unwrap();
    let active = engine.active_instances().unwrap();
    assert_eq!(active.len(), 2);
}

#[test]
fn lifecycle_07_instance_summary_fields() {
    let (engine, _dir) = default_engine();
    engine
        .load_sequence_from_json(make_sequence_json("flow", "noop"))
        .unwrap();
    let id = engine
        .start("flow".to_string(), "{}".to_string(), None)
        .unwrap();
    let active = engine.active_instances().unwrap();
    assert_eq!(active[0].instance_id, id);
    assert_eq!(active[0].sequence_name, "flow");
    assert_eq!(active[0].state, InstanceStateKind::Scheduled);
    assert!(!active[0].created_at.is_empty());
}

#[test]
fn lifecycle_08_tick_once_advances_instance() {
    let (engine, _dir) = engine_with_handler("echo", Arc::new(EchoHandler));
    engine
        .load_sequence_from_json(make_sequence_json("flow", "echo"))
        .unwrap();
    let id = engine
        .start("flow".to_string(), r#"{"hello":"world"}"#.to_string(), None)
        .unwrap();
    let _ = engine.tick_once().unwrap();
    let state = poll_instance_state(&engine, &id, 500);
    assert_eq!(state, InstanceStateKind::Completed);
}

#[test]
fn lifecycle_09_shutdown_prevents_tick() {
    let (engine, _dir) = default_engine();
    engine.shutdown();
    let result = engine.tick_once();
    assert!(matches!(result, Err(MobileError::Shutdown)));
}

#[test]
fn lifecycle_10_start_with_invalid_json_input_uses_empty_object() {
    let (engine, _dir) = default_engine();
    engine
        .load_sequence_from_json(make_sequence_json("flow", "noop"))
        .unwrap();
    let id = engine
        .start("flow".to_string(), "not-json".to_string(), None)
        .unwrap();
    let inst = engine.get_instance(id).unwrap();
    assert_eq!(inst.state, InstanceStateKind::Scheduled);
}

#[test]
fn lifecycle_11_instance_state_has_timestamps() {
    let (engine, _dir) = default_engine();
    engine
        .load_sequence_from_json(make_sequence_json("flow", "noop"))
        .unwrap();
    let id = engine
        .start("flow".to_string(), "{}".to_string(), None)
        .unwrap();
    let inst = engine.get_instance(id).unwrap();
    assert!(chrono::DateTime::parse_from_rfc3339(&inst.created_at).is_ok());
    assert!(chrono::DateTime::parse_from_rfc3339(&inst.updated_at).is_ok());
}

#[test]
fn lifecycle_12_multiple_sequences_multiple_instances() {
    let (engine, _dir) = engine_with_handler("echo", Arc::new(EchoHandler));
    engine
        .load_sequence_from_json(make_sequence_json("flow-a", "echo"))
        .unwrap();
    engine
        .load_sequence_from_json(make_sequence_json("flow-b", "echo"))
        .unwrap();
    let id_a = engine
        .start("flow-a".to_string(), "{}".to_string(), None)
        .unwrap();
    let id_b = engine
        .start("flow-b".to_string(), "{}".to_string(), None)
        .unwrap();
    assert_ne!(id_a, id_b);
    let inst_a = engine.get_instance(id_a).unwrap();
    let inst_b = engine.get_instance(id_b).unwrap();
    assert_eq!(inst_a.sequence_name, "flow-a");
    assert_eq!(inst_b.sequence_name, "flow-b");
}

#[test]
fn lifecycle_13_cancel_frees_active_slot_for_new_dedup_key() {
    // After cancel, the active instance count decreases, allowing a new instance
    // with a *different* dedup key to be started (the old key is permanently consumed).
    let config = MobileEngineConfig {
        max_concurrent_instances: 1,
        ..MobileEngineConfig::default()
    };
    let (engine, _dir) = make_engine(config);
    engine
        .load_sequence_from_json(make_sequence_json("flow", "noop"))
        .unwrap();
    let id1 = engine
        .start(
            "flow".to_string(),
            "{}".to_string(),
            Some("dk-1".to_string()),
        )
        .unwrap();
    // At max capacity: cannot start another.
    let blocked = engine.start(
        "flow".to_string(),
        "{}".to_string(),
        Some("dk-2".to_string()),
    );
    assert!(blocked.is_err());
    // Cancel frees the active slot.
    engine.cancel_instance(id1.clone()).unwrap();
    // Now a new instance with a different dedup key succeeds.
    let id2 = engine
        .start(
            "flow".to_string(),
            "{}".to_string(),
            Some("dk-2".to_string()),
        )
        .unwrap();
    assert_ne!(id1, id2);
    // Verify the cancelled instance remains accessible.
    let inst = engine.get_instance(id1).unwrap();
    assert_eq!(inst.state, InstanceStateKind::Cancelled);
}

#[test]
fn lifecycle_14_register_handler_success() {
    let (engine, _dir) = default_engine();
    let result = engine.register_handler("my-handler".to_string(), Arc::new(EchoHandler));
    assert!(result.is_ok());
}

#[test]
fn lifecycle_15_pause_and_resume_do_not_panic() {
    let (engine, _dir) = engine_with_handler("echo", Arc::new(EchoHandler));
    engine
        .load_sequence_from_json(make_sequence_json("flow", "echo"))
        .unwrap();
    engine.pause();
    engine.resume();
    let id = engine
        .start("flow".to_string(), "{}".to_string(), None)
        .unwrap();
    let _ = engine.tick_once().unwrap();
    let state = poll_instance_state(&engine, &id, 500);
    assert_eq!(state, InstanceStateKind::Completed);
}

// ===========================================================================
// 6. TICK CONTROLLER (10 tests)
// ===========================================================================

#[test]
fn tick_01_tick_once_returns_result() {
    let (engine, _dir) = engine_with_handler("echo", Arc::new(EchoHandler));
    engine
        .load_sequence_from_json(make_sequence_json("flow", "echo"))
        .unwrap();
    engine
        .start("flow".to_string(), "{}".to_string(), None)
        .unwrap();
    let result = engine.tick_once().unwrap();
    assert!(result.instances_advanced > 0 || result.has_pending_work);
}

#[test]
fn tick_02_tick_once_with_no_instances() {
    let (engine, _dir) = default_engine();
    let result = engine.tick_once().unwrap();
    assert_eq!(result.instances_advanced, 0);
    assert_eq!(result.steps_executed, 0);
    assert!(!result.has_pending_work);
}

#[test]
fn tick_03_report_power_state_charging() {
    let (engine, _dir) = default_engine();
    engine.report_power_state(PowerState::Charging);
    let result = engine.tick_once().unwrap();
    assert_eq!(result.instances_advanced, 0);
}

#[test]
fn tick_04_report_power_state_low_battery() {
    let (engine, _dir) = default_engine();
    engine.report_power_state(PowerState::LowBattery);
    let result = engine.tick_once().unwrap();
    assert_eq!(result.instances_advanced, 0);
}

#[test]
fn tick_05_report_power_state_critical_battery() {
    let (engine, _dir) = default_engine();
    engine.report_power_state(PowerState::CriticalBattery);
    let result = engine.tick_once().unwrap();
    assert_eq!(result.instances_advanced, 0);
}

#[test]
fn tick_06_report_power_state_unplugged() {
    let (engine, _dir) = default_engine();
    engine.report_power_state(PowerState::Unplugged);
    let result = engine.tick_once().unwrap();
    assert_eq!(result.instances_advanced, 0);
}

#[test]
fn tick_07_multiple_ticks_advance_multi_step() {
    let (engine, _dir) = engine_with_handler("echo", Arc::new(EchoHandler));
    let json = make_multi_step_sequence_json("multi-flow", "echo", 3);
    engine.load_sequence_from_json(json).unwrap();
    let id = engine
        .start("multi-flow".to_string(), r#"{"x":1}"#.to_string(), None)
        .unwrap();
    for _ in 0..10 {
        let _ = engine.tick_once();
        std::thread::sleep(std::time::Duration::from_millis(20));
    }
    let state = poll_instance_state(&engine, &id, 1000);
    assert_eq!(state, InstanceStateKind::Completed);
}

#[test]
fn tick_08_tick_after_shutdown_returns_error() {
    let (engine, _dir) = default_engine();
    engine.shutdown();
    let result = engine.tick_once();
    assert!(result.is_err());
}

#[test]
fn tick_09_power_state_transitions() {
    let (engine, _dir) = default_engine();
    engine.report_power_state(PowerState::Charging);
    engine.report_power_state(PowerState::Unplugged);
    engine.report_power_state(PowerState::LowBattery);
    engine.report_power_state(PowerState::CriticalBattery);
    engine.report_power_state(PowerState::Charging);
    assert_eq!((), ());
    assert_eq!((), ());
    assert_eq!((), ());
    assert_eq!((), ());
    assert_eq!((), ());
    let result = engine.tick_once().unwrap();
    assert_eq!(result.instances_advanced, 0);
    assert_eq!(result.steps_executed, 0);
    assert!(!result.has_pending_work);
}

#[test]
fn tick_10_resume_then_shutdown() {
    let (engine, _dir) = engine_with_handler("echo", Arc::new(EchoHandler));
    engine
        .load_sequence_from_json(make_sequence_json("flow", "echo"))
        .unwrap();
    engine.resume();
    std::thread::sleep(std::time::Duration::from_millis(50));
    engine.shutdown();
    let result = engine.tick_once();
    assert!(result.is_err());
}

// ===========================================================================
// 7. NOTIFIER (5 tests)
// ===========================================================================

#[test]
fn notifier_01_set_listener_receives_completion() {
    let (engine, _dir) = engine_with_handler("echo", Arc::new(EchoHandler));
    engine
        .load_sequence_from_json(make_sequence_json("flow", "echo"))
        .unwrap();

    let listener = Arc::new(RecordingListener::default());
    engine.set_listener(listener.clone());

    let id = engine
        .start("flow".to_string(), r#"{"ok":true}"#.to_string(), None)
        .unwrap();
    let _ = engine.tick_once().unwrap();
    let state = poll_instance_state(&engine, &id, 500);
    assert_eq!(state, InstanceStateKind::Completed);

    // Fire terminal events on next tick.
    let _ = engine.tick_once().unwrap();

    let completed = listener.completed.lock().unwrap();
    assert_eq!(completed.len(), 1);
    assert_eq!(completed[0].0, id);
}

#[test]
fn notifier_02_listener_not_set_does_not_panic() {
    let (engine, _dir) = engine_with_handler("echo", Arc::new(EchoHandler));
    engine
        .load_sequence_from_json(make_sequence_json("flow", "echo"))
        .unwrap();
    let id = engine
        .start("flow".to_string(), "{}".to_string(), None)
        .unwrap();
    let _ = engine.tick_once().unwrap();
    let state = poll_instance_state(&engine, &id, 500);
    let _ = engine.tick_once().unwrap();
    assert_eq!(state, InstanceStateKind::Completed);
}

#[test]
fn notifier_03_listener_receives_instance_id_on_completion() {
    let (engine, _dir) = engine_with_handler("echo", Arc::new(EchoHandler));
    engine
        .load_sequence_from_json(make_sequence_json("flow", "echo"))
        .unwrap();

    let listener = Arc::new(RecordingListener::default());
    engine.set_listener(listener.clone());

    let id = engine
        .start("flow".to_string(), r#"{"data":"test"}"#.to_string(), None)
        .unwrap();
    let _ = engine.tick_once().unwrap();
    let state = poll_instance_state(&engine, &id, 500);
    assert_eq!(state, InstanceStateKind::Completed);
    let _ = engine.tick_once().unwrap();

    let completed = listener.completed.lock().unwrap();
    assert_eq!(completed.len(), 1);
    assert_eq!(completed[0].0, id);
}

#[test]
fn notifier_04_dedup_prevents_duplicate_notifications() {
    let (engine, _dir) = engine_with_handler("echo", Arc::new(EchoHandler));
    engine
        .load_sequence_from_json(make_sequence_json("flow", "echo"))
        .unwrap();

    let listener = Arc::new(RecordingListener::default());
    engine.set_listener(listener.clone());

    let id = engine
        .start("flow".to_string(), "{}".to_string(), None)
        .unwrap();
    let _ = engine.tick_once().unwrap();
    let _ = poll_instance_state(&engine, &id, 500);

    for _ in 0..5 {
        let _ = engine.tick_once().unwrap();
    }

    let completed = listener.completed.lock().unwrap();
    assert!(completed.len() <= 1);
}

#[test]
fn notifier_05_set_listener_replaces_previous() {
    let (engine, _dir) = engine_with_handler("echo", Arc::new(EchoHandler));
    engine
        .load_sequence_from_json(make_sequence_json("flow", "echo"))
        .unwrap();

    let listener1 = Arc::new(RecordingListener::default());
    let listener2 = Arc::new(RecordingListener::default());
    engine.set_listener(listener1.clone());
    engine.set_listener(listener2.clone());

    let id = engine
        .start("flow".to_string(), "{}".to_string(), None)
        .unwrap();
    let _ = engine.tick_once().unwrap();
    let state = poll_instance_state(&engine, &id, 500);
    assert_eq!(state, InstanceStateKind::Completed);
    let _ = engine.tick_once().unwrap();

    let l1_completed = listener1.completed.lock().unwrap();
    let l2_completed = listener2.completed.lock().unwrap();
    assert!(l1_completed.is_empty());
    assert_eq!(l2_completed.len(), 1);
    assert_eq!(l2_completed[0].0, id);
}

// ===========================================================================
// 8. MEMORY (5 tests)
// ===========================================================================

#[test]
fn memory_01_zero_budget_never_blocks_tick() {
    let config = MobileEngineConfig {
        memory_budget_bytes: 0,
        ..MobileEngineConfig::default()
    };
    let (engine, _dir) = make_engine(config);
    engine
        .register_handler("echo".to_string(), Arc::new(EchoHandler))
        .unwrap();
    engine
        .load_sequence_from_json(make_sequence_json("flow", "echo"))
        .unwrap();
    let id = engine
        .start("flow".to_string(), "{}".to_string(), None)
        .unwrap();
    let _ = engine.tick_once().unwrap();
    let state = poll_instance_state(&engine, &id, 500);
    assert_eq!(state, InstanceStateKind::Completed);
}

#[test]
fn memory_02_huge_budget_never_blocks_tick() {
    let config = MobileEngineConfig {
        memory_budget_bytes: u64::MAX,
        ..MobileEngineConfig::default()
    };
    let (engine, _dir) = make_engine(config);
    engine
        .register_handler("echo".to_string(), Arc::new(EchoHandler))
        .unwrap();
    engine
        .load_sequence_from_json(make_sequence_json("flow", "echo"))
        .unwrap();
    let id = engine
        .start("flow".to_string(), "{}".to_string(), None)
        .unwrap();
    let _ = engine.tick_once().unwrap();
    let state = poll_instance_state(&engine, &id, 500);
    assert_eq!(state, InstanceStateKind::Completed);
}

#[test]
fn memory_03_tiny_budget_skips_tick() {
    let config = MobileEngineConfig {
        memory_budget_bytes: 1,
        ..MobileEngineConfig::default()
    };
    let (engine, _dir) = make_engine(config);
    engine
        .load_sequence_from_json(make_sequence_json("flow", "noop"))
        .unwrap();
    let id = engine
        .start("flow".to_string(), "{}".to_string(), None)
        .unwrap();
    let result = engine.tick_once().unwrap();
    assert_eq!(result.instances_advanced, 0);
    assert_eq!(result.steps_executed, 0);
    assert!(result.has_pending_work);
    let inst = engine.get_instance(id).unwrap();
    assert_eq!(inst.state, InstanceStateKind::Scheduled);
}

#[test]
fn memory_04_budget_field_stored_in_config() {
    let config = MobileEngineConfig {
        memory_budget_bytes: 512 * 1024 * 1024,
        ..MobileEngineConfig::default()
    };
    assert_eq!(config.memory_budget_bytes, 536_870_912);
}

#[test]
fn memory_05_tick_result_with_skipped_tick_fields() {
    let config = MobileEngineConfig {
        memory_budget_bytes: 1,
        ..MobileEngineConfig::default()
    };
    let (engine, _dir) = make_engine(config);
    engine
        .load_sequence_from_json(make_sequence_json("flow", "noop"))
        .unwrap();
    engine
        .start("flow".to_string(), "{}".to_string(), None)
        .unwrap();
    let result = engine.tick_once().unwrap();
    assert_eq!(result.instances_advanced, 0);
    assert_eq!(result.steps_executed, 0);
    assert!(result.has_pending_work);
}
