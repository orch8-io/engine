//! Coverage tests for orch8-types: 90 unit tests across all major type modules.

use chrono::{NaiveDate, Utc};
use serde_json::json;
use std::time::Duration;
use uuid::Uuid;

use orch8_types::config::{
    EngineConfig, ExternalizationMode, SchedulerConfig, SecretString, WebhookConfig,
};
use orch8_types::context::{
    AuditEntry, DEFAULT_MAX_CONTEXT_BYTES, ExecutionContext, RuntimeContext,
};
use orch8_types::sequence::{
    BlockDefinition, HumanChoice, HumanInputDef, RetryPolicy, SequenceDefinition, SequenceStatus,
    StepDef,
};
use orch8_types::{BlockId, InstanceId, Namespace, SequenceId, TenantId};

// ═══════════════════════════════════════════════════════════════════════════════
// SEQUENCE DEFINITION TESTS (20 tests)
// ═══════════════════════════════════════════════════════════════════════════════

fn make_step(id: &str) -> BlockDefinition {
    BlockDefinition::Step(Box::new(StepDef {
        id: BlockId::new(id),
        handler: "noop".into(),
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
        output_schema: None,
        when: None,
    }))
}

fn make_seq(blocks: Vec<BlockDefinition>) -> SequenceDefinition {
    SequenceDefinition {
        id: SequenceId::new(),
        tenant_id: TenantId::unchecked("test-tenant"),
        namespace: Namespace::new("default"),
        name: "test-seq".into(),
        version: 1,
        deprecated: false,
        status: SequenceStatus::default(),
        blocks,
        interceptors: None,
        input_schema: None,
        sla: None,
        on_failure: None,
        on_cancel: None,
        created_at: Utc::now(),
    }
}

#[test]
fn seq_01_sequence_definition_serde_round_trip() {
    let seq = make_seq(vec![make_step("s1"), make_step("s2")]);
    let json = serde_json::to_string(&seq).unwrap();
    let back: SequenceDefinition = serde_json::from_str(&json).unwrap();
    assert_eq!(back.name, "test-seq");
    assert_eq!(back.version, 1);
    assert_eq!(back.blocks.len(), 2);
}

#[test]
fn seq_12_step_def_default_cancellable_from_json() {
    let json = r#"{
        "type": "step",
        "id": "s1",
        "handler": "http_request",
        "params": {}
    }"#;
    let block: BlockDefinition = serde_json::from_str(json).unwrap();
    if let BlockDefinition::Step(s) = block {
        assert!(s.cancellable);
        assert!(s.timeout.is_none());
        assert!(s.retry.is_none());
    } else {
        panic!("expected Step");
    }
}

#[test]
fn seq_13_retry_policy_backoff_multiplier_default() {
    let json = r#"{
        "max_attempts": 3,
        "initial_backoff": 500,
        "max_backoff": 10000
    }"#;
    let rp: RetryPolicy = serde_json::from_str(json).unwrap();
    assert!((rp.backoff_multiplier - 2.0).abs() < f64::EPSILON);
}

#[test]
fn seq_14_retry_policy_validation_via_sequence() {
    let mut s = make_step("s1");
    if let BlockDefinition::Step(ref mut sd) = s {
        sd.retry = Some(RetryPolicy {
            max_attempts: 3,
            initial_backoff: Duration::from_secs(60),
            max_backoff: Duration::from_secs(5),
            backoff_multiplier: 2.0,
            retry_if: None,
            non_retryable_codes: None,
        });
    }
    let seq = make_seq(vec![s]);
    let err = seq.validate().unwrap_err();
    assert!(err.to_string().contains("initial_backoff"));
}

#[test]
fn seq_15_retry_policy_rejects_zero_multiplier() {
    let mut s = make_step("s1");
    if let BlockDefinition::Step(ref mut sd) = s {
        sd.retry = Some(RetryPolicy {
            max_attempts: 3,
            initial_backoff: Duration::from_secs(1),
            max_backoff: Duration::from_secs(30),
            backoff_multiplier: 0.0,
            retry_if: None,
            non_retryable_codes: None,
        });
    }
    let seq = make_seq(vec![s]);
    let err = seq.validate().unwrap_err();
    assert!(err.to_string().contains("backoff_multiplier must be > 0"));
}

#[test]
fn seq_16_human_input_effective_choices_without_user_choices() {
    let hid = HumanInputDef {
        prompt: "Approve?".into(),
        timeout: None,
        escalation_handler: None,
        choices: None,
        store_as: None,
        allow_comment: false,
    };
    let choices = hid.effective_choices();
    assert_eq!(choices.len(), 2);
    assert_eq!(choices[0].value, "yes");
    assert_eq!(choices[1].value, "no");
}

#[test]
fn seq_17_human_input_effective_choices_with_user_choices() {
    let hid = HumanInputDef {
        prompt: "Pick one".into(),
        timeout: None,
        escalation_handler: None,
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
        store_as: Some("decision".into()),
        allow_comment: true,
    };
    let choices = hid.effective_choices();
    assert_eq!(choices.len(), 3);
    assert_eq!(choices[2].value, "escalate");
}

#[test]
fn seq_18_human_choice_serde_roundtrip() {
    let choice = HumanChoice {
        label: "Continue".into(),
        value: "continue".into(),
    };
    let json = serde_json::to_string(&choice).unwrap();
    let back: HumanChoice = serde_json::from_str(&json).unwrap();
    assert_eq!(back, choice);
}

#[test]
fn seq_19_sequence_status_variants_and_display() {
    assert_eq!(SequenceStatus::Draft.to_string(), "draft");
    assert_eq!(SequenceStatus::Staging.to_string(), "staging");
    assert_eq!(SequenceStatus::Production.to_string(), "production");
    assert_eq!(SequenceStatus::Unpublished.to_string(), "unpublished");
}

#[test]
fn seq_20_sequence_status_transitions() {
    assert!(SequenceStatus::Draft.can_transition_to(SequenceStatus::Staging));
    assert!(!SequenceStatus::Draft.can_transition_to(SequenceStatus::Production));
    assert!(SequenceStatus::Staging.can_transition_to(SequenceStatus::Production));
    assert!(!SequenceStatus::Unpublished.can_transition_to(SequenceStatus::Draft));
    assert!(SequenceStatus::Unpublished.valid_transitions().is_empty());
}

// ═══════════════════════════════════════════════════════════════════════════════
// CONFIG TESTS (15 tests)
// ═══════════════════════════════════════════════════════════════════════════════

#[test]
fn cfg_01_engine_config_default_values() {
    let cfg = EngineConfig::default();
    assert_eq!(cfg.database.backend, "postgres");
    assert!(cfg.database.url.is_empty());
    assert_eq!(cfg.database.max_connections, 64);
    assert_eq!(cfg.engine.tick_interval_ms, 100);
    assert_eq!(cfg.engine.batch_size, 256);
    assert_eq!(cfg.engine.max_concurrent_steps, 128);
    assert_eq!(cfg.api.http_addr, "127.0.0.1:8080");
    assert_eq!(cfg.logging.level, "info");
}

#[test]
fn cfg_02_engine_config_validate_passes_default() {
    let cfg = EngineConfig::default();
    assert!(cfg.validate().is_ok());
}

#[test]
fn cfg_03_validate_rejects_zero_tick_interval() {
    let mut cfg = EngineConfig::default();
    cfg.engine.tick_interval_ms = 0;
    let errs = cfg.validate().unwrap_err();
    assert!(errs.iter().any(|e| e.contains("tick_interval_ms")));
}

#[test]
fn cfg_04_validate_rejects_zero_batch_size() {
    let mut cfg = EngineConfig::default();
    cfg.engine.batch_size = 0;
    let errs = cfg.validate().unwrap_err();
    assert!(errs.iter().any(|e| e.contains("batch_size")));
}

#[test]
fn cfg_05_validate_rejects_zero_max_concurrent_steps() {
    let mut cfg = EngineConfig::default();
    cfg.engine.max_concurrent_steps = 0;
    let errs = cfg.validate().unwrap_err();
    assert!(errs.iter().any(|e| e.contains("max_concurrent_steps")));
}

#[test]
fn cfg_06_validate_rejects_unknown_backend() {
    let mut cfg = EngineConfig::default();
    cfg.database.backend = "mysql".into();
    let errs = cfg.validate().unwrap_err();
    assert!(errs.iter().any(|e| e.contains("mysql")));
}

#[test]
fn cfg_07_validate_rejects_invalid_log_level() {
    let mut cfg = EngineConfig::default();
    cfg.logging.level = "verbose".into();
    let errs = cfg.validate().unwrap_err();
    assert!(errs.iter().any(|e| e.contains("verbose")));
}

#[test]
fn cfg_08_validate_rejects_bad_encryption_key() {
    let mut cfg = EngineConfig::default();
    cfg.engine.encryption_key = SecretString::from("tooshort");
    let errs = cfg.validate().unwrap_err();
    assert!(errs.iter().any(|e| e.contains("encryption_key")));
}

#[test]
fn cfg_09_validate_accepts_valid_encryption_key() {
    let mut cfg = EngineConfig::default();
    cfg.engine.encryption_key =
        SecretString::from("aabbccdd11223344556677889900aabbccdd11223344556677889900aabbccdd");
    assert!(cfg.validate().is_ok());
}

#[test]
fn cfg_10_config_serde_round_trip() {
    let cfg = EngineConfig::default();
    let json = serde_json::to_string(&cfg).unwrap();
    let back: EngineConfig = serde_json::from_str(&json).unwrap();
    assert_eq!(back.database.backend, "postgres");
    assert_eq!(back.engine.tick_interval_ms, 100);
}

#[test]
fn cfg_11_config_deserialization_partial_json() {
    let json = r#"{"engine": {"tick_interval_ms": 50}}"#;
    let cfg: EngineConfig = serde_json::from_str(json).unwrap();
    assert_eq!(cfg.engine.tick_interval_ms, 50);
    // Other fields get defaults
    assert_eq!(cfg.engine.batch_size, 256);
    assert_eq!(cfg.database.backend, "postgres");
}

#[test]
fn cfg_12_validate_zero_max_connections() {
    let mut cfg = EngineConfig::default();
    cfg.database.max_connections = 0;
    let errs = cfg.validate().unwrap_err();
    assert!(errs.iter().any(|e| e.contains("max_connections")));
}

#[test]
fn cfg_13_externalization_mode_threshold_default() {
    let cfg = SchedulerConfig::default();
    assert_eq!(cfg.externalization_mode.context_threshold(), Some(65536));
    assert!(!cfg.externalization_mode.always_externalize_outputs());
}

#[test]
fn cfg_14_externalization_mode_never() {
    let mode = ExternalizationMode::Never;
    assert_eq!(mode.context_threshold(), None);
    assert!(!mode.always_externalize_outputs());
}

#[test]
fn cfg_15_webhook_config_defaults() {
    let wh = WebhookConfig::default();
    assert!(wh.urls.is_empty());
    assert_eq!(wh.timeout_secs, 10);
    assert_eq!(wh.max_retries, 3);
}

// ═══════════════════════════════════════════════════════════════════════════════
// CONTEXT TESTS (15 tests)
// ═══════════════════════════════════════════════════════════════════════════════

#[test]
fn ctx_03_context_serde_with_data() {
    let ctx = ExecutionContext {
        data: json!({"user_id": "u123", "score": 95}),
        config: json!({"timeout": 30}),
        audit: vec![],
        runtime: RuntimeContext::default(),
    };
    let json = serde_json::to_string(&ctx).unwrap();
    let back: ExecutionContext = serde_json::from_str(&json).unwrap();
    assert_eq!(back.data["user_id"], "u123");
    assert_eq!(back.config["timeout"], 30);
}

#[test]
fn ctx_04_context_filtered_all_denied() {
    let ctx = ExecutionContext {
        data: json!({"secret": "value"}),
        config: json!({"db": "pg"}),
        audit: vec![AuditEntry {
            timestamp: Utc::now(),
            event: "test".into(),
            details: json!({}),
        }],
        runtime: RuntimeContext {
            attempt: 5,
            ..RuntimeContext::default()
        },
    };
    let access = orch8_types::sequence::ContextAccess {
        data: orch8_types::sequence::FieldAccess::NONE,
        config: false,
        audit: false,
        runtime: false,
    };
    let filtered = ctx.filtered(&access);
    assert_eq!(filtered.data, json!({}));
    assert_eq!(filtered.config, json!({}));
    assert!(filtered.audit.is_empty());
    assert_eq!(filtered.runtime.attempt, 0);
}

#[test]
fn ctx_05_context_filtered_field_level_access() {
    let ctx = ExecutionContext {
        data: json!({"user_id": "u1", "email": "e@x.com", "secret": "shhh"}),
        config: json!({}),
        audit: vec![],
        runtime: RuntimeContext::default(),
    };
    let access = orch8_types::sequence::ContextAccess {
        data: orch8_types::sequence::FieldAccess::Fields {
            fields: vec!["user_id".into(), "email".into()],
        },
        config: true,
        audit: true,
        runtime: true,
    };
    let filtered = ctx.filtered(&access);
    assert_eq!(filtered.data["user_id"], "u1");
    assert_eq!(filtered.data["email"], "e@x.com");
    assert!(filtered.data.get("secret").is_none());
}

#[test]
fn ctx_06_context_check_size_passes_small() {
    let ctx = ExecutionContext {
        data: json!({"k": "v"}),
        ..ExecutionContext::default()
    };
    ctx.check_size(DEFAULT_MAX_CONTEXT_BYTES).unwrap();
}

#[test]
fn ctx_07_context_check_size_rejects_oversize() {
    let large = "x".repeat(1024);
    let ctx = ExecutionContext {
        data: json!({"blob": large}),
        ..ExecutionContext::default()
    };
    let err = ctx.check_size(128).unwrap_err();
    assert!(err.actual > 128);
    assert_eq!(err.max, 128);
}

#[test]
fn ctx_08_context_check_size_zero_disables() {
    let ctx = ExecutionContext {
        data: json!({"blob": "x".repeat(100_000)}),
        ..ExecutionContext::default()
    };
    ctx.check_size(0).unwrap();
}

#[test]
fn ctx_09_audit_entry_serde_round_trip() {
    let entry = AuditEntry {
        timestamp: Utc::now(),
        event: "step_completed".into(),
        details: json!({"block_id": "b1", "duration_ms": 42}),
    };
    let json = serde_json::to_string(&entry).unwrap();
    let back: AuditEntry = serde_json::from_str(&json).unwrap();
    assert_eq!(back.event, "step_completed");
    assert_eq!(back.details["duration_ms"], 42);
}

#[test]
fn ctx_10_context_serialized_size_grows_with_payload() {
    let empty_size = ExecutionContext::default().serialized_size();
    let ctx = ExecutionContext {
        data: json!({"payload": "a".repeat(500)}),
        ..ExecutionContext::default()
    };
    let grown_size = ctx.serialized_size();
    assert!(grown_size > empty_size + 400);
}

#[test]
fn ctx_13_context_filtered_all_allowed() {
    let ctx = ExecutionContext {
        data: json!({"a": 1}),
        config: json!({"b": 2}),
        audit: vec![AuditEntry {
            timestamp: Utc::now(),
            event: "e".into(),
            details: json!({}),
        }],
        runtime: RuntimeContext {
            attempt: 4,
            ..RuntimeContext::default()
        },
    };
    let access = orch8_types::sequence::ContextAccess {
        data: orch8_types::sequence::FieldAccess::ALL,
        config: true,
        audit: true,
        runtime: true,
    };
    let f = ctx.filtered(&access);
    assert_eq!(f.data, json!({"a": 1}));
    assert_eq!(f.config, json!({"b": 2}));
    assert_eq!(f.audit.len(), 1);
    assert_eq!(f.runtime.attempt, 4);
}

#[test]
fn ctx_14_context_too_large_error_message() {
    let ctx = ExecutionContext {
        data: json!({"blob": "x".repeat(2000)}),
        ..ExecutionContext::default()
    };
    let err = ctx.check_size(512).unwrap_err();
    let msg = err.to_string();
    assert!(msg.contains("context too large"));
    assert!(msg.contains("ORCH8_SCHEDULER__MAX_CONTEXT_BYTES"));
}

#[test]
fn ctx_15_default_max_context_bytes_value() {
    assert_eq!(DEFAULT_MAX_CONTEXT_BYTES, 256 * 1024);
}

// ═══════════════════════════════════════════════════════════════════════════════
// EXECUTION & NODE STATE TESTS (10 tests)
// ═══════════════════════════════════════════════════════════════════════════════

use orch8_types::execution::{BlockType, ExecutionNode, NodeState};

#[test]
fn exec_05_node_state_serde_round_trip() {
    for state in [
        NodeState::Pending,
        NodeState::Running,
        NodeState::Waiting,
        NodeState::Completed,
        NodeState::Failed,
        NodeState::Cancelled,
        NodeState::Skipped,
    ] {
        let json = serde_json::to_string(&state).unwrap();
        let back: NodeState = serde_json::from_str(&json).unwrap();
        assert_eq!(back, state);
    }
}

#[test]
fn exec_09_execution_node_serde_round_trip() {
    let node = ExecutionNode {
        id: orch8_types::ExecutionNodeId::new(),
        instance_id: InstanceId::new(),
        block_id: BlockId::new("b1"),
        parent_id: Some(orch8_types::ExecutionNodeId::new()),
        block_type: BlockType::Parallel,
        branch_index: Some(2),
        state: NodeState::Running,
        started_at: Some(Utc::now()),
        completed_at: None,
    };
    let json = serde_json::to_string(&node).unwrap();
    let back: ExecutionNode = serde_json::from_str(&json).unwrap();
    assert_eq!(back.block_id.as_str(), "b1");
    assert_eq!(back.state, NodeState::Running);
    assert_eq!(back.branch_index, Some(2));
    assert!(back.parent_id.is_some());
}

#[test]
fn exec_10_block_type_serde_round_trip() {
    for bt in [
        BlockType::Step,
        BlockType::Parallel,
        BlockType::Race,
        BlockType::Loop,
        BlockType::ForEach,
        BlockType::Router,
        BlockType::TryCatch,
        BlockType::SubSequence,
        BlockType::ABSplit,
        BlockType::CancellationScope,
    ] {
        let json = serde_json::to_string(&bt).unwrap();
        let back: BlockType = serde_json::from_str(&json).unwrap();
        assert_eq!(back, bt);
    }
}

// ═══════════════════════════════════════════════════════════════════════════════
// FILTER TESTS (10 tests)
// ═══════════════════════════════════════════════════════════════════════════════

use orch8_types::filter::{InstanceFilter, Pagination};

#[test]
fn flt_06_pagination_default() {
    let p = Pagination::default();
    assert_eq!(p.offset, 0);
    assert_eq!(p.limit, 100);
    assert!(!p.sort_ascending);
}

#[test]
fn flt_07_pagination_capped_under_limit() {
    let p = Pagination {
        offset: 0,
        limit: 500,
        sort_ascending: false,
    }
    .capped();
    assert_eq!(p.limit, 500);
}

#[test]
fn flt_08_pagination_capped_over_limit() {
    let p = Pagination {
        offset: 10,
        limit: 5000,
        sort_ascending: true,
    }
    .capped();
    assert_eq!(p.limit, 1000);
    assert_eq!(p.offset, 10);
    assert!(p.sort_ascending);
}

#[test]
fn flt_09_instance_filter_with_metadata() {
    let f = InstanceFilter {
        metadata_filter: Some(json!({"region": "us-east-1"})),
        ..Default::default()
    };
    assert_eq!(f.metadata_filter.as_ref().unwrap()["region"], "us-east-1");
}

#[test]
fn flt_10_instance_filter_deserialization() {
    let json = r#"{
        "tenant_id": "t1",
        "states": ["running", "scheduled"]
    }"#;
    let f: InstanceFilter = serde_json::from_str(json).unwrap();
    assert_eq!(f.tenant_id.as_ref().unwrap().as_str(), "t1");
    assert_eq!(f.states.as_ref().unwrap().len(), 2);
}

// ═══════════════════════════════════════════════════════════════════════════════
// CREDENTIAL TESTS (5 tests)
// ═══════════════════════════════════════════════════════════════════════════════

use orch8_types::credential::{CredentialDef, CredentialKind};

#[test]
fn cred_01_credential_kind_default_is_api_key() {
    assert_eq!(CredentialKind::default(), CredentialKind::ApiKey);
}

#[test]
fn cred_04_credential_def_redacts_secret() {
    let now = Utc::now();
    let def = CredentialDef {
        id: "stripe-prod".into(),
        tenant_id: "t1".into(),
        name: "Stripe Production".into(),
        kind: CredentialKind::ApiKey,
        value: SecretString::new(r#"{"token":"sk_live_super_secret"}"#.into()),
        expires_at: None,
        refresh_url: None,
        refresh_token: None,
        enabled: true,
        description: Some("Production key".into()),
        created_at: now,
        updated_at: now,
    };
    let json = serde_json::to_string(&def).unwrap();
    assert!(!json.contains("sk_live_super_secret"));
    assert!(json.contains("[REDACTED]"));
    assert_eq!(def.value.expose(), r#"{"token":"sk_live_super_secret"}"#);
}

#[test]
fn cred_05_credential_kind_from_str_loose() {
    assert_eq!(
        CredentialKind::from_str_loose("api_key"),
        Some(CredentialKind::ApiKey)
    );
    assert_eq!(
        CredentialKind::from_str_loose("oauth2"),
        Some(CredentialKind::Oauth2)
    );
    assert_eq!(
        CredentialKind::from_str_loose("basic"),
        Some(CredentialKind::Basic)
    );
    assert_eq!(CredentialKind::from_str_loose("unknown"), None);
}

// ═══════════════════════════════════════════════════════════════════════════════
// POOL & WORKER TESTS (5 tests)
// ═══════════════════════════════════════════════════════════════════════════════

use orch8_types::pool::{PoolResource, RotationStrategy};
use orch8_types::worker::WorkerTaskState;

#[test]
fn pool_01_rotation_strategy_from_str() {
    assert_eq!(
        "round_robin".parse::<RotationStrategy>().unwrap(),
        RotationStrategy::RoundRobin
    );
    assert_eq!(
        "weighted".parse::<RotationStrategy>().unwrap(),
        RotationStrategy::Weighted
    );
    assert_eq!(
        "random".parse::<RotationStrategy>().unwrap(),
        RotationStrategy::Random
    );
    assert!("fifo".parse::<RotationStrategy>().is_err());
}

#[test]
fn pool_02_pool_resource_effective_daily_cap_no_warmup() {
    let r = PoolResource {
        id: Uuid::now_v7(),
        pool_id: Uuid::now_v7(),
        resource_key: orch8_types::ids::ResourceKey::new("test"),
        name: "test".into(),
        weight: 1,
        enabled: true,
        daily_cap: 100,
        daily_usage: 0,
        daily_usage_date: None,
        warmup_start: None,
        warmup_days: 0,
        warmup_start_cap: 0,
        created_at: Utc::now(),
    };
    let today = NaiveDate::from_ymd_opt(2024, 6, 1).unwrap();
    assert_eq!(r.effective_daily_cap(today), 100);
}

#[test]
fn pool_03_pool_resource_has_capacity() {
    let today = NaiveDate::from_ymd_opt(2024, 6, 1).unwrap();
    let mut r = PoolResource {
        id: Uuid::now_v7(),
        pool_id: Uuid::now_v7(),
        resource_key: orch8_types::ids::ResourceKey::new("test"),
        name: "test".into(),
        weight: 1,
        enabled: true,
        daily_cap: 10,
        daily_usage: 9,
        daily_usage_date: Some(today),
        warmup_start: None,
        warmup_days: 0,
        warmup_start_cap: 0,
        created_at: Utc::now(),
    };
    assert!(r.has_capacity(today));
    r.daily_usage = 10;
    assert!(!r.has_capacity(today));
}

#[test]
fn pool_04_worker_task_state_from_str() {
    assert_eq!(
        "pending".parse::<WorkerTaskState>().unwrap(),
        WorkerTaskState::Pending
    );
    assert_eq!(
        "claimed".parse::<WorkerTaskState>().unwrap(),
        WorkerTaskState::Claimed
    );
    assert_eq!(
        "completed".parse::<WorkerTaskState>().unwrap(),
        WorkerTaskState::Completed
    );
    assert_eq!(
        "failed".parse::<WorkerTaskState>().unwrap(),
        WorkerTaskState::Failed
    );
    assert!("bogus".parse::<WorkerTaskState>().is_err());
}

#[test]
fn pool_05_worker_task_state_display_round_trip() {
    for state in [
        WorkerTaskState::Pending,
        WorkerTaskState::Claimed,
        WorkerTaskState::Completed,
        WorkerTaskState::Failed,
    ] {
        let s = state.to_string();
        let back: WorkerTaskState = s.parse().unwrap();
        assert_eq!(back, state);
    }
}

// ═══════════════════════════════════════════════════════════════════════════════
// ENCRYPTION TESTS (5 tests)
// ═══════════════════════════════════════════════════════════════════════════════

use orch8_types::encryption::{EncryptionError, FieldEncryptor};

fn test_key_bytes() -> [u8; 32] {
    let mut key = [0u8; 32];
    for (i, b) in key.iter_mut().enumerate() {
        *b = u8::try_from(i).unwrap();
    }
    key
}

#[test]
fn enc_01_encrypt_decrypt_round_trip() {
    let enc = FieldEncryptor::from_bytes(&test_key_bytes());
    let original = json!({"secret": "data", "num": 42});
    let encrypted = enc.encrypt_value(&original).unwrap();
    assert!(FieldEncryptor::is_encrypted(&encrypted));
    let decrypted = enc.decrypt_value(&encrypted).unwrap();
    assert_eq!(original, decrypted);
}

#[test]
fn enc_02_non_encrypted_passthrough() {
    let enc = FieldEncryptor::from_bytes(&test_key_bytes());
    let plain = json!({"not": "encrypted"});
    let result = enc.decrypt_value(&plain).unwrap();
    assert_eq!(plain, result);
}

#[test]
fn enc_03_wrong_key_fails() {
    let enc1 = FieldEncryptor::from_bytes(&test_key_bytes());
    let mut other = test_key_bytes();
    other[0] = 255;
    let enc2 = FieldEncryptor::from_bytes(&other);
    let encrypted = enc1.encrypt_value(&json!("secret")).unwrap();
    assert!(enc2.decrypt_value(&encrypted).is_err());
}

#[test]
fn enc_04_from_hex_key_valid() {
    let hex = "000102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f";
    let enc = FieldEncryptor::from_hex_key(hex).unwrap();
    let val = json!("test");
    let encrypted = enc.encrypt_value(&val).unwrap();
    let decrypted = enc.decrypt_value(&encrypted).unwrap();
    assert_eq!(val, decrypted);
}

#[test]
fn enc_05_from_hex_key_rejects_invalid_length() {
    let short = "00".repeat(16);
    let err = FieldEncryptor::from_hex_key(&short).unwrap_err();
    assert!(matches!(err, EncryptionError::InvalidKeyLength(16)));

    let long = "00".repeat(33);
    let err = FieldEncryptor::from_hex_key(&long).unwrap_err();
    assert!(matches!(err, EncryptionError::InvalidKeyLength(33)));
}

// ═══════════════════════════════════════════════════════════════════════════════
// MISC TYPES TESTS (5 tests)
// ═══════════════════════════════════════════════════════════════════════════════

use orch8_types::dedupe::DedupeScope;
use orch8_types::output::BlockOutput;
use orch8_types::rate_limit::RateLimit;
use orch8_types::rollback::RollbackPolicy;
use orch8_types::trigger::TriggerType;

#[test]
fn misc_01_dedupe_scope_parent_kind() {
    let id = InstanceId::new();
    let scope = DedupeScope::Parent(id);
    assert_eq!(scope.kind(), "parent");
    assert_eq!(scope.value(), id.to_string());
}

#[test]
fn misc_02_rate_limit_serde_round_trip() {
    let rl = RateLimit {
        id: Uuid::now_v7(),
        tenant_id: TenantId::unchecked("t1"),
        resource_key: orch8_types::ids::ResourceKey::new("api:send"),
        max_count: 50,
        window_seconds: 3600,
        current_count: 10,
        window_start: Utc::now(),
    };
    let json = serde_json::to_string(&rl).unwrap();
    let back: RateLimit = serde_json::from_str(&json).unwrap();
    assert_eq!(back.max_count, 50);
    assert_eq!(back.window_seconds, 3600);
    assert_eq!(back.current_count, 10);
}

#[test]
fn misc_03_trigger_type_display_and_default() {
    assert_eq!(TriggerType::default(), TriggerType::Webhook);
    assert_eq!(TriggerType::Webhook.to_string(), "webhook");
    assert_eq!(TriggerType::Nats.to_string(), "nats");
    assert_eq!(TriggerType::FileWatch.to_string(), "file_watch");
    assert_eq!(TriggerType::Event.to_string(), "event");
}

#[test]
fn misc_04_block_output_serde_round_trip() {
    let bo = BlockOutput {
        id: Uuid::now_v7(),
        instance_id: InstanceId::new(),
        block_id: BlockId::new("step_1"),
        output: json!({"status": "ok", "count": 5}),
        output_ref: None,
        output_size: 35,
        attempt: 1,
        created_at: Utc::now(),
    };
    let json = serde_json::to_string(&bo).unwrap();
    let back: BlockOutput = serde_json::from_str(&json).unwrap();
    assert_eq!(back.block_id.as_str(), "step_1");
    assert_eq!(back.output["status"], "ok");
    assert_eq!(back.output_size, 35);
}

#[test]
fn misc_05_rollback_policy_serde_with_defaults() {
    let json = r#"{
        "id": 1,
        "tenant_id": "t1",
        "sequence_name": "seq-a",
        "error_rate_threshold": 0.1,
        "time_window_secs": 300,
        "enabled": true,
        "created_at": "2026-01-01T00:00:00Z",
        "updated_at": "2026-01-01T00:00:00Z"
    }"#;
    let policy: RollbackPolicy = serde_json::from_str(json).unwrap();
    assert_eq!(policy.cooldown_secs, 3600);
    assert_eq!(policy.confirmation_window_secs, 60);
    assert!(policy.enabled);
    assert!((policy.error_rate_threshold - 0.1).abs() < f64::EPSILON);
}
