//! Extra coverage tests for orch8-types: 100 unit tests covering areas not
//! exercised by `types_coverage.rs`.

use chrono::Utc;
use serde_json::json;
use std::str::FromStr;
use std::time::Duration;
use uuid::Uuid;

use orch8_types::config::{
    ApiConfig, EngineConfig, ExternalizationMode, LoggingConfig, SchedulerConfig, SecretString,
    WebhookConfig,
};
use orch8_types::context::{ExecutionContext, RuntimeContext};
use orch8_types::ids::{BlockId, InstanceId, Namespace, SequenceId, TenantId};
use orch8_types::instance::{InstanceState, Priority, TaskInstance};
use orch8_types::sequence::{
    BlockDefinition, ContextAccess, DelaySpec, EscalationDef, FieldAccess, HumanChoice,
    HumanInputDef, RetryPolicy, SendWindow, SequenceDefinition, SequenceStatus, StepDef,
};
use orch8_types::signal::{Signal, SignalAction, SignalActionError, SignalType};

// ═══════════════════════════════════════════════════════════════════════════════
// HELPERS
// ═══════════════════════════════════════════════════════════════════════════════

fn make_step(id: &str, handler: &str) -> BlockDefinition {
    BlockDefinition::Step(Box::new(StepDef {
        id: BlockId::new(id),
        handler: handler.into(),
        params: serde_json::Value::Null,
        delay: None,
        retry: None,
        timeout: None,
        rate_limit_key: None,
        send_window: None,
        context_access: None,
        cancellable: true,
        wait_for_input: None,
        queue_name: None,
        deadline: None,
        on_deadline_breach: None,
        fallback_handler: None,
        cache_key: None,
    }))
}

fn make_seq(blocks: Vec<BlockDefinition>) -> SequenceDefinition {
    SequenceDefinition {
        id: SequenceId::new(),
        tenant_id: TenantId::unchecked("tenant-b2"),
        namespace: Namespace::new("default"),
        name: "extra-seq".into(),
        version: 1,
        deprecated: false,
        status: SequenceStatus::default(),
        blocks,
        interceptors: None,
        created_at: Utc::now(),
    }
}

#[allow(dead_code)]
fn full_step(id: &str) -> StepDef {
    StepDef {
        id: BlockId::new(id),
        handler: "http_request".into(),
        params: json!({"url": "https://example.com", "method": "POST"}),
        delay: Some(DelaySpec {
            duration: Duration::from_secs(5),
            business_days_only: true,
            jitter: Some(Duration::from_millis(500)),
            holidays: vec!["2026-12-25".into()],
            fire_at_local: Some("2026-03-08T09:00:00".into()),
            timezone: Some("America/New_York".into()),
        }),
        retry: Some(RetryPolicy {
            max_attempts: 5,
            initial_backoff: Duration::from_millis(200),
            max_backoff: Duration::from_secs(60),
            backoff_multiplier: 3.0,
        }),
        timeout: Some(Duration::from_secs(30)),
        rate_limit_key: Some("api:send".into()),
        send_window: Some(SendWindow {
            start_hour: 8,
            end_hour: 20,
            days: vec![0, 1, 2, 3, 4],
        }),
        context_access: Some(ContextAccess {
            data: FieldAccess::Fields {
                fields: vec!["user_id".into(), "order_id".into()],
            },
            config: true,
            audit: false,
            runtime: true,
        }),
        cancellable: false,
        wait_for_input: Some(HumanInputDef {
            prompt: "Please review this order".into(),
            timeout: Some(Duration::from_secs(3600)),
            escalation_handler: Some("notify_manager".into()),
            choices: Some(vec![
                HumanChoice {
                    label: "Approve".into(),
                    value: "approve".into(),
                },
                HumanChoice {
                    label: "Reject".into(),
                    value: "reject".into(),
                },
                HumanChoice {
                    label: "Escalate".into(),
                    value: "escalate".into(),
                },
            ]),
            store_as: Some("review_decision".into()),
            allow_comment: true,
        }),
        queue_name: Some("priority-queue".into()),
        deadline: Some(Duration::from_secs(7200)),
        on_deadline_breach: Some(EscalationDef {
            handler: "send_alert".into(),
            params: json!({"channel": "#ops"}),
        }),
        fallback_handler: Some("http_request_fallback".into()),
        cache_key: Some("rate_{{ data.currency }}".into()),
    }
}

// ═══════════════════════════════════════════════════════════════════════════════
// SEQUENCE DEFINITION DEEP COVERAGE (25 tests)
// ═══════════════════════════════════════════════════════════════════════════════

#[test]
fn seq_11_retry_policy_zero_attempts_rejected_by_validation() {
    let mut s = make_step("r0", "noop");
    if let BlockDefinition::Step(ref mut sd) = s {
        sd.retry = Some(RetryPolicy {
            max_attempts: 0,
            initial_backoff: Duration::from_millis(100),
            max_backoff: Duration::from_secs(1),
            backoff_multiplier: 2.0,
        });
    }
    let seq = make_seq(vec![s]);
    let err = seq.validate().unwrap_err();
    assert!(err.to_string().contains("max_attempts must be > 0"));
}

#[test]
fn seq_12_retry_policy_huge_backoff_serde() {
    let rp = RetryPolicy {
        max_attempts: 100,
        initial_backoff: Duration::from_millis(1),
        max_backoff: Duration::from_secs(86400), // 24 hours
        backoff_multiplier: 10.0,
    };
    let json = serde_json::to_string(&rp).unwrap();
    let back: RetryPolicy = serde_json::from_str(&json).unwrap();
    assert_eq!(back.max_backoff, Duration::from_secs(86400));
    assert!((back.backoff_multiplier - 10.0).abs() < f64::EPSILON);
}

#[test]
fn seq_13_retry_policy_zero_initial_backoff_valid() {
    // Zero initial_backoff is valid as long as it is <= max_backoff
    let mut s = make_step("r-zero", "noop");
    if let BlockDefinition::Step(ref mut sd) = s {
        sd.retry = Some(RetryPolicy {
            max_attempts: 3,
            initial_backoff: Duration::ZERO,
            max_backoff: Duration::from_secs(10),
            backoff_multiplier: 2.0,
        });
    }
    let seq = make_seq(vec![s]);
    assert!(seq.validate().is_ok());
}

#[test]
fn seq_14_retry_policy_equal_initial_max_backoff_valid() {
    let mut s = make_step("r-eq", "noop");
    if let BlockDefinition::Step(ref mut sd) = s {
        sd.retry = Some(RetryPolicy {
            max_attempts: 1,
            initial_backoff: Duration::from_secs(5),
            max_backoff: Duration::from_secs(5),
            backoff_multiplier: 1.0,
        });
    }
    let seq = make_seq(vec![s]);
    assert!(seq.validate().is_ok());
}

#[test]
fn seq_15_retry_policy_negative_multiplier_rejected() {
    let mut s = make_step("r-neg", "noop");
    if let BlockDefinition::Step(ref mut sd) = s {
        sd.retry = Some(RetryPolicy {
            max_attempts: 3,
            initial_backoff: Duration::from_secs(1),
            max_backoff: Duration::from_secs(30),
            backoff_multiplier: -1.0,
        });
    }
    let seq = make_seq(vec![s]);
    let err = seq.validate().unwrap_err();
    assert!(err.to_string().contains("backoff_multiplier must be > 0"));
}

#[test]
fn seq_16_human_input_with_all_fields() {
    let hid = HumanInputDef {
        prompt: "Review the contract".into(),
        timeout: Some(Duration::from_secs(7200)),
        escalation_handler: Some("notify_legal".into()),
        choices: Some(vec![
            HumanChoice {
                label: "Sign".into(),
                value: "sign".into(),
            },
            HumanChoice {
                label: "Reject".into(),
                value: "reject".into(),
            },
        ]),
        store_as: Some("contract_decision".into()),
        allow_comment: true,
    };
    assert!(hid.validate().is_ok());
    let choices = hid.effective_choices();
    assert_eq!(choices.len(), 2);
    assert_eq!(choices[0].value, "sign");
}

#[test]
fn seq_17_human_input_timeout_serde() {
    let hid = HumanInputDef {
        prompt: "Ack".into(),
        timeout: Some(Duration::from_secs(300)),
        escalation_handler: None,
        choices: None,
        store_as: None,
        allow_comment: false,
    };
    let json = serde_json::to_string(&hid).unwrap();
    let back: HumanInputDef = serde_json::from_str(&json).unwrap();
    assert_eq!(back.timeout, Some(Duration::from_millis(300_000)));
}

#[test]
fn seq_18_human_input_escalation_handler_stored() {
    let hid = HumanInputDef {
        prompt: "Check".into(),
        timeout: Some(Duration::from_secs(60)),
        escalation_handler: Some("page_oncall".into()),
        choices: None,
        store_as: None,
        allow_comment: false,
    };
    let json = serde_json::to_string(&hid).unwrap();
    let back: HumanInputDef = serde_json::from_str(&json).unwrap();
    assert_eq!(back.escalation_handler.as_deref(), Some("page_oncall"));
}

#[test]
fn seq_19_human_input_allow_comment_serialized() {
    let hid = HumanInputDef {
        prompt: "Comment test".into(),
        timeout: None,
        escalation_handler: None,
        choices: None,
        store_as: None,
        allow_comment: true,
    };
    let json = serde_json::to_string(&hid).unwrap();
    assert!(json.contains("allow_comment"));
    let back: HumanInputDef = serde_json::from_str(&json).unwrap();
    assert!(back.allow_comment);
}

#[test]
fn seq_20_human_input_allow_comment_false_not_serialized() {
    let hid = HumanInputDef {
        prompt: "No comment".into(),
        timeout: None,
        escalation_handler: None,
        choices: None,
        store_as: None,
        allow_comment: false,
    };
    let json = serde_json::to_string(&hid).unwrap();
    // skip_serializing_if = "std::ops::Not::not" skips when false
    assert!(!json.contains("allow_comment"));
}

// ═══════════════════════════════════════════════════════════════════════════════
// CONFIG DEEP COVERAGE (20 tests)
// ═══════════════════════════════════════════════════════════════════════════════

#[test]
fn cfg_32_webhook_config_empty_urls_default() {
    let json = r"{}";
    let wh: WebhookConfig = serde_json::from_str(json).unwrap();
    assert!(wh.urls.is_empty());
    assert_eq!(wh.timeout_secs, 10);
    assert_eq!(wh.max_retries, 3);
}

#[test]
fn cfg_33_webhook_config_serde_round_trip() {
    let wh = WebhookConfig {
        urls: vec!["https://hook.io/x".into()],
        timeout_secs: 15,
        max_retries: 7,
        secret: None,
    };
    let json = serde_json::to_string(&wh).unwrap();
    let back: WebhookConfig = serde_json::from_str(&json).unwrap();
    assert_eq!(back.urls[0], "https://hook.io/x");
    assert_eq!(back.timeout_secs, 15);
    assert_eq!(back.max_retries, 7);
}

#[test]
fn cfg_34_webhook_in_scheduler_config() {
    let json = r#"{
        "webhooks": {
            "urls": ["https://hook.io"],
            "timeout_secs": 20
        }
    }"#;
    let cfg: SchedulerConfig = serde_json::from_str(json).unwrap();
    assert_eq!(cfg.webhooks.urls.len(), 1);
    assert_eq!(cfg.webhooks.timeout_secs, 20);
    assert_eq!(cfg.webhooks.max_retries, 3);
}

#[test]
fn cfg_35_scheduler_config_max_steps_per_instance() {
    let json = r#"{"max_steps_per_instance": 500}"#;
    let cfg: SchedulerConfig = serde_json::from_str(json).unwrap();
    assert_eq!(cfg.max_steps_per_instance, 500);
}

#[test]
fn cfg_36_config_serde_with_all_sections() {
    let cfg = EngineConfig {
        database: orch8_types::config::DatabaseConfig {
            backend: "sqlite".into(),
            url: SecretString::from("sqlite::memory:"),
            max_connections: 4,
            run_migrations: true,
            search_path: Some("test_schema".into()),
        },
        engine: SchedulerConfig::default(),
        api: ApiConfig::default(),
        logging: LoggingConfig {
            level: "debug".into(),
            json: true,
        },
        artifacts: orch8_types::config::ArtifactConfig::default(),
        telemetry: orch8_types::config::TelemetryConfig::default(),
    };
    let json = serde_json::to_string(&cfg).unwrap();
    let back: EngineConfig = serde_json::from_str(&json).unwrap();
    assert_eq!(back.database.backend, "sqlite");
    assert!(back.database.run_migrations);
    assert_eq!(back.database.search_path.as_deref(), Some("test_schema"));
    assert_eq!(back.logging.level, "debug");
    assert!(back.logging.json);
}

#[test]
fn cfg_37_config_serde_api_section() {
    let json = r#"{
        "grpc_addr": "0.0.0.0:9051",
        "http_addr": "0.0.0.0:9080",
        "cors_origins": "https://app.example.com",
        "require_tenant_header": true,
        "max_concurrent_requests": 1000
    }"#;
    let cfg: ApiConfig = serde_json::from_str(json).unwrap();
    assert_eq!(cfg.grpc_addr, "0.0.0.0:9051");
    assert_eq!(cfg.http_addr, "0.0.0.0:9080");
    assert_eq!(cfg.cors_origins, "https://app.example.com");
    assert!(cfg.require_tenant_header);
    assert_eq!(cfg.max_concurrent_requests, 1000);
}

#[test]
fn cfg_38_config_serde_logging_defaults() {
    let json = r"{}";
    let cfg: LoggingConfig = serde_json::from_str(json).unwrap();
    assert_eq!(cfg.level, "info");
    assert!(!cfg.json);
}

#[test]
fn cfg_39_externalization_mode_round_trip_all_variants() {
    for mode in [
        ExternalizationMode::Never,
        ExternalizationMode::Threshold { bytes: 1024 },
        ExternalizationMode::AlwaysOutputs,
    ] {
        let json = serde_json::to_string(&mode).unwrap();
        let back: ExternalizationMode = serde_json::from_str(&json).unwrap();
        assert_eq!(back, mode);
    }
}

#[test]
fn cfg_40_config_validate_rejects_stale_less_than_tick() {
    let mut cfg = EngineConfig::default();
    cfg.engine.tick_interval_ms = 10000; // 10s
    cfg.engine.stale_instance_threshold_secs = 5; // 5s < 10s
    let errs = cfg.validate().unwrap_err();
    assert!(errs.iter().any(|e| e.contains("stale_instance_threshold")));
}

#[test]
fn cfg_41_config_validate_accepts_all_log_levels() {
    for level in ["trace", "debug", "info", "warn", "error"] {
        let mut cfg = EngineConfig::default();
        cfg.logging.level = level.into();
        assert!(cfg.validate().is_ok(), "level {level} should be valid");
    }
}

#[test]
fn cfg_42_config_validate_multiple_errors_collected() {
    let mut cfg = EngineConfig::default();
    cfg.engine.tick_interval_ms = 0;
    cfg.engine.batch_size = 0;
    cfg.engine.max_concurrent_steps = 0;
    cfg.database.max_connections = 0;
    cfg.database.backend = "redis".into();
    cfg.logging.level = "critical".into();
    let errs = cfg.validate().unwrap_err();
    assert!(
        errs.len() >= 5,
        "expected at least 5 errors, got {}",
        errs.len()
    );
}

#[test]
fn cfg_43_config_validate_accepts_sqlite_backend() {
    let mut cfg = EngineConfig::default();
    cfg.database.backend = "sqlite".into();
    assert!(cfg.validate().is_ok());
}

#[test]
fn cfg_44_secret_string_not_leaked_in_serialized_config() {
    let cfg = EngineConfig {
        database: orch8_types::config::DatabaseConfig {
            url: SecretString::from("postgres://user:pass@host/db"),
            ..orch8_types::config::DatabaseConfig::default()
        },
        engine: SchedulerConfig {
            encryption_key: SecretString::from("a".repeat(64)),
            ..SchedulerConfig::default()
        },
        api: ApiConfig {
            api_key: SecretString::from("super-secret-key"),
            ..ApiConfig::default()
        },
        logging: LoggingConfig::default(),
        artifacts: orch8_types::config::ArtifactConfig::default(),
        telemetry: orch8_types::config::TelemetryConfig::default(),
    };
    let json = serde_json::to_string(&cfg).unwrap();
    assert!(!json.contains("pass@host"));
    assert!(!json.contains("super-secret-key"));
    assert!(json.contains("[REDACTED]"));
}

#[test]
fn cfg_45_scheduler_config_max_instances_per_tenant_default() {
    let cfg = SchedulerConfig::default();
    assert_eq!(cfg.max_instances_per_tenant, 0);
}

// ═══════════════════════════════════════════════════════════════════════════════
// TASK INSTANCE DEEP COVERAGE (15 tests)
// ═══════════════════════════════════════════════════════════════════════════════

#[test]
fn inst_46_task_instance_serde_round_trip_all_fields() {
    let instance = TaskInstance {
        id: InstanceId::new(),
        sequence_id: SequenceId::new(),
        tenant_id: TenantId::unchecked("tenant-1"),
        namespace: Namespace::new("production"),
        state: InstanceState::Running,
        next_fire_at: Some(Utc::now()),
        priority: Priority::High,
        timezone: "Europe/London".into(),
        metadata: json!({"user": "alice", "tags": ["vip"]}),
        context: ExecutionContext {
            data: json!({"step": 3}),
            config: json!({}),
            audit: vec![],
            runtime: RuntimeContext::default(),
        },
        concurrency_key: Some("user:alice".into()),
        max_concurrency: Some(5),
        idempotency_key: Some("idem-123".into()),
        session_id: Some(Uuid::now_v7()),
        parent_instance_id: Some(InstanceId::new()),
        budget: None,
        created_at: Utc::now(),
        updated_at: Utc::now(),
    };
    let json = serde_json::to_string(&instance).unwrap();
    let back: TaskInstance = serde_json::from_str(&json).unwrap();
    assert_eq!(back.tenant_id.as_str(), "tenant-1");
    assert_eq!(back.namespace.as_str(), "production");
    assert_eq!(back.state, InstanceState::Running);
    assert_eq!(back.priority, Priority::High);
    assert_eq!(back.timezone, "Europe/London");
    assert_eq!(back.concurrency_key.as_deref(), Some("user:alice"));
    assert_eq!(back.max_concurrency, Some(5));
    assert_eq!(back.idempotency_key.as_deref(), Some("idem-123"));
    assert!(back.session_id.is_some());
    assert!(back.parent_instance_id.is_some());
}

#[test]
fn inst_47_task_instance_optional_fields_none() {
    let instance = TaskInstance {
        id: InstanceId::new(),
        sequence_id: SequenceId::new(),
        tenant_id: TenantId::unchecked("t"),
        namespace: Namespace::new("default"),
        state: InstanceState::Scheduled,
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
        created_at: Utc::now(),
        updated_at: Utc::now(),
    };
    let json = serde_json::to_string(&instance).unwrap();
    assert!(!json.contains("concurrency_key"));
    assert!(!json.contains("max_concurrency"));
    assert!(!json.contains("idempotency_key"));
    assert!(!json.contains("session_id"));
    assert!(!json.contains("parent_instance_id"));
}

#[test]
fn inst_48_task_instance_metadata_json() {
    let instance = TaskInstance {
        id: InstanceId::new(),
        sequence_id: SequenceId::new(),
        tenant_id: TenantId::unchecked("t"),
        namespace: Namespace::new("default"),
        state: InstanceState::Scheduled,
        next_fire_at: None,
        priority: Priority::Normal,
        timezone: "UTC".into(),
        metadata: json!({"campaign_id": "c-42", "source": "api", "priority_override": true}),
        context: ExecutionContext::default(),
        concurrency_key: None,
        max_concurrency: None,
        idempotency_key: None,
        session_id: None,
        parent_instance_id: None,
        budget: None,
        created_at: Utc::now(),
        updated_at: Utc::now(),
    };
    let json = serde_json::to_string(&instance).unwrap();
    let back: TaskInstance = serde_json::from_str(&json).unwrap();
    assert_eq!(back.metadata["campaign_id"], "c-42");
    assert_eq!(back.metadata["priority_override"], true);
}

#[test]
fn inst_49_task_instance_state_deserialization() {
    let json_str = r#"{"state": "waiting"}"#;
    let val: serde_json::Value = serde_json::from_str(json_str).unwrap();
    let state: InstanceState = serde_json::from_value(val["state"].clone()).unwrap();
    assert_eq!(state, InstanceState::Waiting);
}

#[test]
fn inst_50_task_instance_all_states_serde() {
    for state in [
        InstanceState::Scheduled,
        InstanceState::Running,
        InstanceState::Waiting,
        InstanceState::Paused,
        InstanceState::Completed,
        InstanceState::Failed,
        InstanceState::Cancelled,
    ] {
        let json = serde_json::to_string(&state).unwrap();
        let back: InstanceState = serde_json::from_str(&json).unwrap();
        assert_eq!(back, state);
    }
}

#[test]
fn inst_51_priority_ordering_low_lt_normal() {
    assert!(Priority::Low < Priority::Normal);
}

#[test]
fn inst_52_priority_ordering_normal_lt_high() {
    assert!(Priority::Normal < Priority::High);
}

#[test]
fn inst_53_priority_ordering_high_lt_critical() {
    assert!(Priority::High < Priority::Critical);
}

#[test]
fn inst_54_priority_full_ordering() {
    let mut priorities = vec![
        Priority::Critical,
        Priority::Low,
        Priority::High,
        Priority::Normal,
    ];
    priorities.sort();
    assert_eq!(
        priorities,
        vec![
            Priority::Low,
            Priority::Normal,
            Priority::High,
            Priority::Critical
        ]
    );
}

#[test]
fn inst_55_priority_default_is_normal() {
    assert_eq!(Priority::default(), Priority::Normal);
}

#[test]
fn inst_56_priority_try_from_i16_low() {
    assert_eq!(Priority::try_from(0i16).unwrap(), Priority::Low);
}

#[test]
fn inst_57_priority_try_from_i16_normal() {
    assert_eq!(Priority::try_from(1i16).unwrap(), Priority::Normal);
}

#[test]
fn inst_58_priority_try_from_i16_high() {
    assert_eq!(Priority::try_from(2i16).unwrap(), Priority::High);
}

#[test]
fn inst_59_priority_try_from_i16_critical() {
    assert_eq!(Priority::try_from(3i16).unwrap(), Priority::Critical);
}

#[test]
fn inst_60_priority_try_from_i16_invalid() {
    assert!(Priority::try_from(-1i16).is_err());
    assert!(Priority::try_from(4i16).is_err());
    assert!(Priority::try_from(100i16).is_err());
    let err = Priority::try_from(99i16).unwrap_err();
    assert!(err.contains("unknown priority value: 99"));
}

// ═══════════════════════════════════════════════════════════════════════════════
// SIGNAL DEEP COVERAGE (10 tests)
// ═══════════════════════════════════════════════════════════════════════════════

#[test]
fn sig_61_signal_construction_pause() {
    let sig = Signal {
        id: Uuid::now_v7(),
        instance_id: InstanceId::new(),
        signal_type: SignalType::Pause,
        payload: json!(null),
        delivered: false,
        created_at: Utc::now(),
        delivered_at: None,
    };
    assert!(!sig.delivered);
    assert!(sig.delivered_at.is_none());
    assert_eq!(sig.signal_type, SignalType::Pause);
}

#[test]
fn sig_62_signal_construction_custom() {
    let sig = Signal {
        id: Uuid::now_v7(),
        instance_id: InstanceId::new(),
        signal_type: SignalType::Custom("webhook_received".into()),
        payload: json!({"event": "payment_success", "amount": 99.99}),
        delivered: true,
        created_at: Utc::now(),
        delivered_at: Some(Utc::now()),
    };
    assert!(sig.delivered);
    assert!(sig.delivered_at.is_some());
    match &sig.signal_type {
        SignalType::Custom(name) => assert_eq!(name, "webhook_received"),
        _ => panic!("expected Custom"),
    }
}

#[test]
fn sig_63_signal_serde_round_trip() {
    let sig = Signal {
        id: Uuid::now_v7(),
        instance_id: InstanceId::new(),
        signal_type: SignalType::Resume,
        payload: json!({"reason": "manual"}),
        delivered: false,
        created_at: Utc::now(),
        delivered_at: None,
    };
    let json = serde_json::to_string(&sig).unwrap();
    let back: Signal = serde_json::from_str(&json).unwrap();
    assert_eq!(back.signal_type, SignalType::Resume);
    assert_eq!(back.payload["reason"], "manual");
}

#[test]
fn sig_64_signal_type_display_custom() {
    let st = SignalType::Custom("my_signal".into());
    assert_eq!(st.to_string(), "custom:my_signal");
}

#[test]
fn sig_65_signal_type_from_str_custom_prefix_required() {
    // Without "custom:" prefix, unknown strings produce an error
    let err = SignalType::from_str("my_signal").unwrap_err();
    assert_eq!(err.to_string(), "unknown signal type: my_signal");
}

#[test]
fn sig_66_signal_action_pause_ignores_payload() {
    let sig = Signal {
        id: Uuid::now_v7(),
        instance_id: InstanceId::new(),
        signal_type: SignalType::Pause,
        payload: json!({"irrelevant": true, "nested": {"deep": 1}}),
        delivered: false,
        created_at: Utc::now(),
        delivered_at: None,
    };
    match sig.action().unwrap() {
        SignalAction::Pause => {}
        other => panic!("expected Pause, got {other:?}"),
    }
}

#[test]
fn sig_67_signal_action_cancel_ignores_payload() {
    let sig = Signal {
        id: Uuid::now_v7(),
        instance_id: InstanceId::new(),
        signal_type: SignalType::Cancel,
        payload: json!("random string"),
        delivered: false,
        created_at: Utc::now(),
        delivered_at: None,
    };
    match sig.action().unwrap() {
        SignalAction::Cancel => {}
        other => panic!("expected Cancel, got {other:?}"),
    }
}

#[test]
fn sig_68_signal_action_update_context_valid() {
    let ctx = ExecutionContext {
        data: json!({"new_key": "new_value"}),
        config: json!({}),
        audit: vec![],
        runtime: RuntimeContext::default(),
    };
    let sig = Signal {
        id: Uuid::now_v7(),
        instance_id: InstanceId::new(),
        signal_type: SignalType::UpdateContext,
        payload: serde_json::to_value(&ctx).unwrap(),
        delivered: false,
        created_at: Utc::now(),
        delivered_at: None,
    };
    match sig.action().unwrap() {
        SignalAction::UpdateContext(decoded) => {
            assert_eq!(decoded.data["new_key"], "new_value");
        }
        other => panic!("expected UpdateContext, got {other:?}"),
    }
}

#[test]
fn sig_69_signal_action_update_context_invalid_payload() {
    let sig = Signal {
        id: Uuid::now_v7(),
        instance_id: InstanceId::new(),
        signal_type: SignalType::UpdateContext,
        payload: json!(42),
        delivered: false,
        created_at: Utc::now(),
        delivered_at: None,
    };
    let err = sig.action().unwrap_err();
    match err {
        SignalActionError::InvalidPayload { signal_type, .. } => {
            assert_eq!(signal_type, "update_context");
        }
    }
}

#[test]
fn sig_70_signal_action_custom_preserves_payload() {
    let sig = Signal {
        id: Uuid::now_v7(),
        instance_id: InstanceId::new(),
        signal_type: SignalType::Custom("payment_received".into()),
        payload: json!({"amount": 150, "currency": "USD"}),
        delivered: false,
        created_at: Utc::now(),
        delivered_at: None,
    };
    match sig.action().unwrap() {
        SignalAction::Custom { name, payload } => {
            assert_eq!(name, "payment_received");
            assert_eq!(payload["amount"], 150);
            assert_eq!(payload["currency"], "USD");
        }
        other => panic!("expected Custom, got {other:?}"),
    }
}
