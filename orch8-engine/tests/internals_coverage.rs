//! 100 integration/unit tests for orch8-engine internal modules:
//! credentials, gc, cron, interceptors, `sequence_cache`, webhooks, recovery,
//! and built-in handlers (`send_signal`, `emit_event`, `query_instance`, `self_modify`).

use std::sync::Arc;
use std::time::Duration;

use chrono::Utc;
use serde_json::json;
use tokio_util::sync::CancellationToken;

use orch8_engine::credentials::resolve_in_value;
use orch8_engine::cron::{calculate_next_fire, validate_cron_expr};
use orch8_engine::gc::{run_gc_loop, run_gc_loop_with_ttl, GC_BATCH_LIMIT, GC_DEFAULT_INTERVAL};
use orch8_engine::handlers::builtin::register_builtins;
use orch8_engine::handlers::{HandlerRegistry, StepContext};
use orch8_engine::interceptors;
use orch8_engine::recovery::recover_stale_instances;
use orch8_engine::sequence_cache::SequenceCache;
use orch8_engine::webhooks::{self, instance_event, WebhookEvent};
use orch8_storage::sqlite::SqliteStorage;
use orch8_storage::{AdminStore, InstanceStore, OutputStore, SequenceStore, StorageBackend};
use orch8_types::config::SecretString;
use orch8_types::context::{ExecutionContext, RuntimeContext};
use orch8_types::credential::{CredentialDef, CredentialKind};
use orch8_types::cron::CronSchedule;
use orch8_types::error::StepError;
use orch8_types::ids::{BlockId, InstanceId, Namespace, SequenceId, TenantId};
use orch8_types::instance::{InstanceState, Priority, TaskInstance};
use orch8_types::interceptor::{InterceptorAction, InterceptorDef};
use orch8_types::sequence::{BlockDefinition, SequenceDefinition, SequenceStatus, StepDef};

// ================================================================
// HELPERS
// ================================================================

fn mk_credential(id: &str, tenant_id: &str, value: &str, enabled: bool) -> CredentialDef {
    let now = Utc::now();
    CredentialDef {
        id: id.into(),
        tenant_id: tenant_id.into(),
        name: id.into(),
        kind: CredentialKind::ApiKey,
        value: SecretString::new(value.into()),
        expires_at: None,
        refresh_url: None,
        refresh_token: None,
        enabled,
        description: None,
        created_at: now,
        updated_at: now,
    }
}

fn mk_seq(name: &str) -> SequenceDefinition {
    SequenceDefinition {
        id: SequenceId::new(),
        tenant_id: TenantId::unchecked("t"),
        namespace: Namespace::new("ns"),
        name: name.into(),
        version: 1,
        deprecated: false,
        status: SequenceStatus::default(),
        blocks: vec![BlockDefinition::Step(Box::new(StepDef {
            id: BlockId::new("s1"),
            handler: "noop".into(),
            params: json!({}),
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
        }))],
        interceptors: None,
        created_at: Utc::now(),
    }
}

fn mk_cron_schedule(expr: &str, tz: &str) -> CronSchedule {
    CronSchedule {
        id: uuid::Uuid::now_v7(),
        tenant_id: TenantId::unchecked("t"),
        namespace: Namespace::new("ns"),
        sequence_id: SequenceId::new(),
        cron_expr: expr.into(),
        timezone: tz.into(),
        enabled: true,
        metadata: json!({}),
        last_triggered_at: None,
        next_fire_at: None,
        created_at: Utc::now(),
        updated_at: Utc::now(),
    }
}

async fn seed_instance(storage: &dyn StorageBackend, id: InstanceId) {
    let now = Utc::now();
    let inst = TaskInstance {
        id,
        sequence_id: SequenceId::new(),
        tenant_id: TenantId::unchecked("t"),
        namespace: Namespace::new("ns"),
        state: InstanceState::Running,
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
        created_at: now,
        updated_at: now,
    };
    storage.create_instance(&inst).await.unwrap();
}

fn mk_handler_instance(tenant: &str, state: InstanceState) -> TaskInstance {
    let now = Utc::now();
    TaskInstance {
        id: InstanceId::new(),
        sequence_id: SequenceId::new(),
        tenant_id: TenantId::unchecked(tenant),
        namespace: Namespace::new("default"),
        state,
        next_fire_at: Some(now),
        priority: Priority::Normal,
        timezone: "UTC".into(),
        metadata: json!({}),
        context: ExecutionContext {
            data: json!({}),
            config: json!({}),
            audit: vec![],
            runtime: RuntimeContext::default(),
        },
        concurrency_key: None,
        max_concurrency: None,
        idempotency_key: None,
        session_id: None,
        parent_instance_id: None,
        created_at: now,
        updated_at: now,
    }
}

fn mk_step_ctx(
    caller: &TaskInstance,
    storage: Arc<dyn StorageBackend>,
    params: serde_json::Value,
) -> StepContext {
    StepContext {
        instance_id: caller.id,
        tenant_id: caller.tenant_id.clone(),
        block_id: BlockId::new("test_step"),
        params,
        context: ExecutionContext::default(),
        attempt: 1,
        storage,
        wait_for_input: None,
    }
}

fn make_registry() -> HandlerRegistry {
    let mut registry = HandlerRegistry::new();
    register_builtins(&mut registry);
    registry
}

async fn invoke_handler(
    registry: &HandlerRegistry,
    name: &str,
    ctx: StepContext,
) -> Result<serde_json::Value, StepError> {
    let handler = registry.get(name).expect("handler not registered");
    handler(ctx).await
}

// ================================================================
// CREDENTIAL RESOLUTION (Tests 1-15)
// ================================================================

/// Test 1: Resolve full credential value by key.
#[tokio::test]
async fn cred_01_resolve_full_value() {
    let storage = SqliteStorage::in_memory().await.unwrap();
    storage
        .create_credential(&mk_credential(
            "api-key",
            "t1",
            r#"{"token":"abc123"}"#,
            true,
        ))
        .await
        .unwrap();
    let mut val = json!({"auth": "credentials://api-key"});
    resolve_in_value(&storage, "t1", &mut val).await.unwrap();
    assert_eq!(val["auth"]["token"], "abc123");
}

/// Test 2: Resolve credential sub-field by path.
#[tokio::test]
async fn cred_02_resolve_subfield() {
    let storage = SqliteStorage::in_memory().await.unwrap();
    storage
        .create_credential(&mk_credential(
            "multi",
            "t1",
            r#"{"key":"k1","secret":"s1"}"#,
            true,
        ))
        .await
        .unwrap();
    let mut val = json!({"api_key": "credentials://multi/key"});
    resolve_in_value(&storage, "t1", &mut val).await.unwrap();
    assert_eq!(val["api_key"], "k1");
}

/// Test 3: Non-credential strings left untouched.
#[tokio::test]
async fn cred_03_non_credential_string_untouched() {
    let storage = SqliteStorage::in_memory().await.unwrap();
    let mut val = json!({"url": "https://example.com", "count": 5});
    resolve_in_value(&storage, "t1", &mut val).await.unwrap();
    assert_eq!(val["url"], "https://example.com");
    assert_eq!(val["count"], 5);
}

/// Test 4: Resolve credential in array elements.
#[tokio::test]
async fn cred_04_resolve_in_array() {
    let storage = SqliteStorage::in_memory().await.unwrap();
    storage
        .create_credential(&mk_credential("tok", "", r#""my-token""#, true))
        .await
        .unwrap();
    let mut val = json!(["credentials://tok", "plain"]);
    resolve_in_value(&storage, "t1", &mut val).await.unwrap();
    assert_eq!(val[0], "my-token");
    assert_eq!(val[1], "plain");
}

/// Test 5: Resolve raw (non-JSON) credential value as string.
#[tokio::test]
async fn cred_05_resolve_raw_string_value() {
    let storage = SqliteStorage::in_memory().await.unwrap();
    storage
        .create_credential(&mk_credential("raw-tok", "t1", "raw-value-here", true))
        .await
        .unwrap();
    let mut val = json!("credentials://raw-tok");
    resolve_in_value(&storage, "t1", &mut val).await.unwrap();
    assert_eq!(val, json!("raw-value-here"));
}

/// Test 6: Missing credential returns Permanent error.
#[tokio::test]
async fn cred_06_missing_credential_error() {
    let storage = SqliteStorage::in_memory().await.unwrap();
    let mut val = json!("credentials://nonexistent");
    let err = resolve_in_value(&storage, "t1", &mut val)
        .await
        .unwrap_err();
    assert!(matches!(err, StepError::Permanent { .. }));
}

/// Test 7: Empty credential id returns Permanent error.
#[tokio::test]
async fn cred_07_empty_id_error() {
    let storage = SqliteStorage::in_memory().await.unwrap();
    let mut val = json!("credentials://");
    let err = resolve_in_value(&storage, "t1", &mut val)
        .await
        .unwrap_err();
    assert!(matches!(err, StepError::Permanent { .. }));
}

/// Test 8: Disabled credential returns Permanent error.
#[tokio::test]
async fn cred_08_disabled_credential_error() {
    let storage = SqliteStorage::in_memory().await.unwrap();
    storage
        .create_credential(&mk_credential("disabled", "t1", r#""val""#, false))
        .await
        .unwrap();
    let mut val = json!("credentials://disabled");
    let err = resolve_in_value(&storage, "t1", &mut val)
        .await
        .unwrap_err();
    assert!(matches!(err, StepError::Permanent { .. }));
}

/// Test 9: Missing sub-field returns Permanent error.
#[tokio::test]
async fn cred_09_missing_subfield_error() {
    let storage = SqliteStorage::in_memory().await.unwrap();
    storage
        .create_credential(&mk_credential("obj", "t1", r#"{"a":"1"}"#, true))
        .await
        .unwrap();
    let mut val = json!("credentials://obj/nonexistent_field");
    let err = resolve_in_value(&storage, "t1", &mut val)
        .await
        .unwrap_err();
    assert!(matches!(err, StepError::Permanent { .. }));
}

/// Test 10: Deeply nested credential reference resolves correctly.
#[tokio::test]
async fn cred_10_deep_nested_resolve() {
    let storage = SqliteStorage::in_memory().await.unwrap();
    storage
        .create_credential(&mk_credential("deep", "t1", r#""deep-val""#, true))
        .await
        .unwrap();
    let mut val = json!({"a": {"b": {"c": "credentials://deep"}}});
    resolve_in_value(&storage, "t1", &mut val).await.unwrap();
    assert_eq!(val["a"]["b"]["c"], "deep-val");
}

/// Test 11: Same-tenant credential resolves.
#[tokio::test]
async fn cred_11_same_tenant_resolves() {
    let storage = SqliteStorage::in_memory().await.unwrap();
    storage
        .create_credential(&mk_credential("mine", "tenant-a", r#""secret""#, true))
        .await
        .unwrap();
    let mut val = json!("credentials://mine");
    resolve_in_value(&storage, "tenant-a", &mut val)
        .await
        .unwrap();
    assert_eq!(val, json!("secret"));
}

/// Test 12: Global credential (empty `tenant_id`) accessible by any tenant.
#[tokio::test]
async fn cred_12_global_credential_accessible() {
    let storage = SqliteStorage::in_memory().await.unwrap();
    storage
        .create_credential(&mk_credential("global", "", r#""g-val""#, true))
        .await
        .unwrap();
    let mut val = json!("credentials://global");
    resolve_in_value(&storage, "any-tenant", &mut val)
        .await
        .unwrap();
    assert_eq!(val, json!("g-val"));
}

/// Test 13: Cross-tenant access denied.
#[tokio::test]
async fn cred_13_cross_tenant_denied() {
    let storage = SqliteStorage::in_memory().await.unwrap();
    storage
        .create_credential(&mk_credential("private", "owner-x", r#""secret""#, true))
        .await
        .unwrap();
    let mut val = json!("credentials://private");
    let err = resolve_in_value(&storage, "other-tenant", &mut val)
        .await
        .unwrap_err();
    assert!(matches!(err, StepError::Permanent { .. }));
}

/// Test 14: Tenant with empty `tenant_id` can resolve any credential.
#[tokio::test]
async fn cred_14_empty_caller_tenant_resolves_any() {
    let storage = SqliteStorage::in_memory().await.unwrap();
    storage
        .create_credential(&mk_credential("shared", "acme", r#""v""#, true))
        .await
        .unwrap();
    let mut val = json!("credentials://shared");
    resolve_in_value(&storage, "", &mut val).await.unwrap();
    assert_eq!(val, json!("v"));
}

/// Test 15: Multiple credentials in single object all resolved.
#[tokio::test]
async fn cred_15_multiple_credentials_in_one_object() {
    let storage = SqliteStorage::in_memory().await.unwrap();
    storage
        .create_credential(&mk_credential("a", "t1", r#""val-a""#, true))
        .await
        .unwrap();
    storage
        .create_credential(&mk_credential("b", "t1", r#""val-b""#, true))
        .await
        .unwrap();
    let mut val = json!({"x": "credentials://a", "y": "credentials://b", "z": "plain"});
    resolve_in_value(&storage, "t1", &mut val).await.unwrap();
    assert_eq!(val["x"], "val-a");
    assert_eq!(val["y"], "val-b");
    assert_eq!(val["z"], "plain");
}

// ================================================================
// GARBAGE COLLECTION (Tests 16-30)
// ================================================================

#[tokio::test]
async fn gc_16_loop_exits_on_cancel() {
    let storage: Arc<dyn StorageBackend> = Arc::new(SqliteStorage::in_memory().await.unwrap());
    let cancel = CancellationToken::new();
    let handle = tokio::spawn({
        let storage = Arc::clone(&storage);
        let cancel = cancel.clone();
        async move { run_gc_loop(storage, Duration::from_millis(10), None, cancel).await }
    });
    tokio::time::sleep(Duration::from_millis(30)).await;
    cancel.cancel();
    tokio::time::timeout(Duration::from_secs(2), handle)
        .await
        .unwrap()
        .unwrap();
}

#[tokio::test]
async fn gc_17_empty_storage_noop() {
    let storage: Arc<dyn StorageBackend> = Arc::new(SqliteStorage::in_memory().await.unwrap());
    let cancel = CancellationToken::new();
    let handle = tokio::spawn({
        let storage = Arc::clone(&storage);
        let cancel = cancel.clone();
        async move { run_gc_loop(storage, Duration::from_millis(5), None, cancel).await }
    });
    tokio::time::sleep(Duration::from_millis(20)).await;
    cancel.cancel();
    tokio::time::timeout(Duration::from_secs(2), handle)
        .await
        .unwrap()
        .unwrap();
}

#[test]
fn gc_18_batch_limit_value() {
    assert_eq!(GC_BATCH_LIMIT, 1_000);
}

#[test]
fn gc_19_default_interval() {
    assert_eq!(GC_DEFAULT_INTERVAL, Duration::from_secs(300));
}

#[tokio::test]
async fn gc_20_sweeps_expired_dedupe() {
    use orch8_storage::{DedupeScope, EmitDedupeOutcome};
    let storage: Arc<dyn StorageBackend> = Arc::new(SqliteStorage::in_memory().await.unwrap());
    let parent = InstanceId::new();
    let child = InstanceId::new();
    storage
        .record_or_get_emit_dedupe(&DedupeScope::Parent(parent), "key1", child)
        .await
        .unwrap();
    tokio::time::sleep(Duration::from_millis(2_100)).await;
    let cancel = CancellationToken::new();
    let handle = {
        let storage = Arc::clone(&storage);
        let cancel = cancel.clone();
        tokio::spawn(async move {
            run_gc_loop_with_ttl(
                storage,
                Duration::from_millis(10),
                Duration::from_secs(1),
                None,
                cancel,
            )
            .await;
        })
    };
    tokio::time::sleep(Duration::from_millis(80)).await;
    cancel.cancel();
    tokio::time::timeout(Duration::from_secs(2), handle)
        .await
        .unwrap()
        .unwrap();
    let outcome = storage
        .record_or_get_emit_dedupe(&DedupeScope::Parent(parent), "key1", InstanceId::new())
        .await
        .unwrap();
    assert_eq!(outcome, EmitDedupeOutcome::Inserted);
}

#[tokio::test]
async fn gc_21_preserves_fresh_dedupe() {
    use orch8_storage::{DedupeScope, EmitDedupeOutcome};
    let storage: Arc<dyn StorageBackend> = Arc::new(SqliteStorage::in_memory().await.unwrap());
    let parent = InstanceId::new();
    let child = InstanceId::new();
    storage
        .record_or_get_emit_dedupe(&DedupeScope::Parent(parent), "k", child)
        .await
        .unwrap();
    let cancel = CancellationToken::new();
    let handle = {
        let storage = Arc::clone(&storage);
        let cancel = cancel.clone();
        tokio::spawn(async move {
            run_gc_loop_with_ttl(
                storage,
                Duration::from_millis(10),
                Duration::from_secs(3600),
                None,
                cancel,
            )
            .await;
        })
    };
    tokio::time::sleep(Duration::from_millis(50)).await;
    cancel.cancel();
    tokio::time::timeout(Duration::from_secs(2), handle)
        .await
        .unwrap()
        .unwrap();
    let outcome = storage
        .record_or_get_emit_dedupe(&DedupeScope::Parent(parent), "k", InstanceId::new())
        .await
        .unwrap();
    assert_eq!(outcome, EmitDedupeOutcome::AlreadyExists(child));
}

#[tokio::test]
async fn gc_22_multiple_ticks() {
    let storage: Arc<dyn StorageBackend> = Arc::new(SqliteStorage::in_memory().await.unwrap());
    let cancel = CancellationToken::new();
    let handle = tokio::spawn({
        let storage = Arc::clone(&storage);
        let cancel = cancel.clone();
        async move { run_gc_loop(storage, Duration::from_millis(5), None, cancel).await }
    });
    tokio::time::sleep(Duration::from_millis(50)).await;
    cancel.cancel();
    tokio::time::timeout(Duration::from_secs(2), handle)
        .await
        .unwrap()
        .unwrap();
}

#[test]
fn gc_23_emit_dedupe_default_ttl() {
    assert_eq!(
        orch8_engine::gc::EMIT_DEDUPE_DEFAULT_TTL,
        Duration::from_secs(2_592_000)
    );
}

#[tokio::test]
async fn gc_24_cancellation_prompt() {
    let storage: Arc<dyn StorageBackend> = Arc::new(SqliteStorage::in_memory().await.unwrap());
    let cancel = CancellationToken::new();
    let handle = tokio::spawn({
        let storage = Arc::clone(&storage);
        let cancel = cancel.clone();
        async move { run_gc_loop(storage, Duration::from_secs(60), None, cancel).await }
    });
    cancel.cancel();
    tokio::time::timeout(Duration::from_secs(2), handle)
        .await
        .unwrap()
        .unwrap();
}

#[tokio::test]
async fn gc_25_zero_ttl_sweeps_everything() {
    use orch8_storage::{DedupeScope, EmitDedupeOutcome};
    let storage: Arc<dyn StorageBackend> = Arc::new(SqliteStorage::in_memory().await.unwrap());
    let parent = InstanceId::new();
    storage
        .record_or_get_emit_dedupe(&DedupeScope::Parent(parent), "z", InstanceId::new())
        .await
        .unwrap();
    tokio::time::sleep(Duration::from_millis(1_100)).await;
    let cancel = CancellationToken::new();
    let handle = {
        let storage = Arc::clone(&storage);
        let cancel = cancel.clone();
        tokio::spawn(async move {
            run_gc_loop_with_ttl(
                storage,
                Duration::from_millis(10),
                Duration::from_secs(0),
                None,
                cancel,
            )
            .await;
        })
    };
    tokio::time::sleep(Duration::from_millis(60)).await;
    cancel.cancel();
    tokio::time::timeout(Duration::from_secs(2), handle)
        .await
        .unwrap()
        .unwrap();
    let outcome = storage
        .record_or_get_emit_dedupe(&DedupeScope::Parent(parent), "z", InstanceId::new())
        .await
        .unwrap();
    assert_eq!(outcome, EmitDedupeOutcome::Inserted);
}

#[tokio::test]
async fn gc_26_multiple_dedupe_rows_swept() {
    use orch8_storage::{DedupeScope, EmitDedupeOutcome};
    let storage: Arc<dyn StorageBackend> = Arc::new(SqliteStorage::in_memory().await.unwrap());
    let parent = InstanceId::new();
    for i in 0..5 {
        storage
            .record_or_get_emit_dedupe(
                &DedupeScope::Parent(parent),
                &format!("k{i}"),
                InstanceId::new(),
            )
            .await
            .unwrap();
    }
    tokio::time::sleep(Duration::from_millis(2_100)).await;
    let cancel = CancellationToken::new();
    let handle = {
        let storage = Arc::clone(&storage);
        let cancel = cancel.clone();
        tokio::spawn(async move {
            run_gc_loop_with_ttl(
                storage,
                Duration::from_millis(10),
                Duration::from_secs(1),
                None,
                cancel,
            )
            .await;
        })
    };
    tokio::time::sleep(Duration::from_millis(80)).await;
    cancel.cancel();
    tokio::time::timeout(Duration::from_secs(2), handle)
        .await
        .unwrap()
        .unwrap();
    for i in 0..5 {
        let outcome = storage
            .record_or_get_emit_dedupe(
                &DedupeScope::Parent(parent),
                &format!("k{i}"),
                InstanceId::new(),
            )
            .await
            .unwrap();
        assert_eq!(outcome, EmitDedupeOutcome::Inserted);
    }
}

#[tokio::test]
async fn gc_27_preserves_running_instances() {
    let storage: Arc<dyn StorageBackend> = Arc::new(SqliteStorage::in_memory().await.unwrap());
    let inst_id = InstanceId::new();
    seed_instance(storage.as_ref(), inst_id).await;
    let cancel = CancellationToken::new();
    let handle = tokio::spawn({
        let storage = Arc::clone(&storage);
        let cancel = cancel.clone();
        async move { run_gc_loop(storage, Duration::from_millis(5), None, cancel).await }
    });
    tokio::time::sleep(Duration::from_millis(30)).await;
    cancel.cancel();
    handle.await.unwrap();
    let inst = storage.get_instance(inst_id).await.unwrap().unwrap();
    assert_eq!(inst.state, InstanceState::Running);
}

#[tokio::test]
async fn gc_28_dedupe_tenant_scope_swept() {
    use orch8_storage::{DedupeScope, EmitDedupeOutcome};
    let storage: Arc<dyn StorageBackend> = Arc::new(SqliteStorage::in_memory().await.unwrap());
    let tenant = TenantId::unchecked("gc-tenant");
    storage
        .record_or_get_emit_dedupe(
            &DedupeScope::Tenant(tenant.clone()),
            "tkey",
            InstanceId::new(),
        )
        .await
        .unwrap();
    tokio::time::sleep(Duration::from_millis(2_100)).await;
    let cancel = CancellationToken::new();
    let handle = {
        let storage = Arc::clone(&storage);
        let cancel = cancel.clone();
        tokio::spawn(async move {
            run_gc_loop_with_ttl(
                storage,
                Duration::from_millis(10),
                Duration::from_secs(1),
                None,
                cancel,
            )
            .await;
        })
    };
    tokio::time::sleep(Duration::from_millis(80)).await;
    cancel.cancel();
    handle.await.unwrap();
    let outcome = storage
        .record_or_get_emit_dedupe(&DedupeScope::Tenant(tenant), "tkey", InstanceId::new())
        .await
        .unwrap();
    assert_eq!(outcome, EmitDedupeOutcome::Inserted);
}

#[tokio::test]
async fn gc_29_concurrent_gc_loops_dont_panic() {
    let storage: Arc<dyn StorageBackend> = Arc::new(SqliteStorage::in_memory().await.unwrap());
    let cancel = CancellationToken::new();
    let h1 = tokio::spawn({
        let s = Arc::clone(&storage);
        let c = cancel.clone();
        async move { run_gc_loop(s, Duration::from_millis(5), None, c).await }
    });
    let h2 = tokio::spawn({
        let s = Arc::clone(&storage);
        let c = cancel.clone();
        async move { run_gc_loop(s, Duration::from_millis(7), None, c).await }
    });
    tokio::time::sleep(Duration::from_millis(40)).await;
    cancel.cancel();
    h1.await.unwrap();
    h2.await.unwrap();
}

#[test]
fn gc_30_batch_limit_positive() {
    const { assert!(GC_BATCH_LIMIT > 0) }
}

// ================================================================
// CRON SCHEDULING (Tests 31-45)
// ================================================================

#[test]
fn cron_31_valid_7_field() {
    assert!(validate_cron_expr("0 0 9 * * MON-FRI *").is_ok());
}

#[test]
fn cron_32_valid_5_field() {
    assert!(validate_cron_expr("*/5 * * * *").is_ok());
}

#[test]
fn cron_33_invalid_expr() {
    assert!(validate_cron_expr("not a cron").is_err());
}

#[test]
fn cron_34_empty_expr_invalid() {
    assert!(validate_cron_expr("").is_err());
}

#[test]
fn cron_35_out_of_range() {
    assert!(validate_cron_expr("0 99 * * * * *").is_err());
}

#[test]
fn cron_36_next_fire_is_future() {
    let schedule = mk_cron_schedule("0 * * * * * *", "UTC");
    let next = calculate_next_fire(&schedule).unwrap();
    assert!(next > Utc::now());
}

#[test]
fn cron_37_every_second_within_2s() {
    let schedule = mk_cron_schedule("* * * * * * *", "UTC");
    let next = calculate_next_fire(&schedule).unwrap();
    let delta = (next - Utc::now()).num_milliseconds();
    assert!((0..=2000).contains(&delta));
}

#[test]
fn cron_38_invalid_expr_returns_none() {
    let schedule = mk_cron_schedule("invalid expr", "UTC");
    assert!(calculate_next_fire(&schedule).is_none());
}

#[test]
fn cron_39_midnight_fires_at_zero() {
    use chrono::Timelike;
    let schedule = mk_cron_schedule("0 0 0 * * * *", "UTC");
    let next = calculate_next_fire(&schedule).unwrap();
    assert_eq!(next.hour(), 0);
    assert_eq!(next.minute(), 0);
    assert_eq!(next.second(), 0);
}

#[test]
fn cron_40_hourly_within_one_hour() {
    let schedule = mk_cron_schedule("0 * * * *", "UTC");
    let next = calculate_next_fire(&schedule).unwrap();
    let delta = (next - Utc::now()).num_seconds();
    assert!((0..=3600).contains(&delta));
}

#[test]
fn cron_41_timezone_london() {
    let schedule = mk_cron_schedule("0 0 12 * * * *", "Europe/London");
    assert!(calculate_next_fire(&schedule).is_some());
}

#[test]
fn cron_42_timezone_new_york() {
    let schedule = mk_cron_schedule("0 0 9 * * MON-FRI *", "America/New_York");
    assert!(calculate_next_fire(&schedule).is_some());
}

#[test]
fn cron_43_invalid_timezone_fallback() {
    let schedule = mk_cron_schedule("0 * * * * * *", "Invalid/Zone");
    assert!(calculate_next_fire(&schedule).is_some());
}

#[test]
fn cron_44_timezone_tokyo() {
    let schedule = mk_cron_schedule("0 0 3 * * * *", "Asia/Tokyo");
    let next = calculate_next_fire(&schedule);
    assert!(next.is_some());
    assert!(next.unwrap() > Utc::now());
}

#[test]
fn cron_45_weekday_only() {
    use chrono::{Datelike, Weekday};
    let schedule = mk_cron_schedule("0 9 * * MON-FRI", "UTC");
    let next = calculate_next_fire(&schedule).unwrap();
    assert!(!matches!(next.weekday(), Weekday::Sat | Weekday::Sun));
}

// ================================================================
// INTERCEPTORS (Tests 46-60)
// ================================================================

#[tokio::test]
async fn interceptor_46_before_step_saves() {
    let storage = SqliteStorage::in_memory().await.unwrap();
    let iid = InstanceId::new();
    seed_instance(&storage, iid).await;
    let def = InterceptorDef {
        before_step: Some(InterceptorAction {
            handler: "log".into(),
            params: json!({"stage": "pre"}),
        }),
        ..Default::default()
    };
    interceptors::emit_before_step(&storage, &def, iid, &BlockId::new("step1")).await;
    let out = storage
        .get_block_output(iid, &BlockId::new("_interceptor:before:step1"))
        .await
        .unwrap();
    assert!(out.is_some());
    assert_eq!(out.unwrap().output["stage"], "pre");
}

#[tokio::test]
async fn interceptor_47_before_step_noop() {
    let storage = SqliteStorage::in_memory().await.unwrap();
    let iid = InstanceId::new();
    seed_instance(&storage, iid).await;
    interceptors::emit_before_step(
        &storage,
        &InterceptorDef::default(),
        iid,
        &BlockId::new("s"),
    )
    .await;
    assert!(storage
        .get_block_output(iid, &BlockId::new("_interceptor:before:s"))
        .await
        .unwrap()
        .is_none());
}

#[tokio::test]
async fn interceptor_48_distinct_block_ids() {
    let storage = SqliteStorage::in_memory().await.unwrap();
    let iid = InstanceId::new();
    seed_instance(&storage, iid).await;
    let def = InterceptorDef {
        before_step: Some(InterceptorAction {
            handler: "x".into(),
            params: json!({}),
        }),
        ..Default::default()
    };
    interceptors::emit_before_step(&storage, &def, iid, &BlockId::new("a")).await;
    interceptors::emit_before_step(&storage, &def, iid, &BlockId::new("b")).await;
    assert!(storage
        .get_block_output(iid, &BlockId::new("_interceptor:before:a"))
        .await
        .unwrap()
        .is_some());
    assert!(storage
        .get_block_output(iid, &BlockId::new("_interceptor:before:b"))
        .await
        .unwrap()
        .is_some());
}

#[tokio::test]
async fn interceptor_49_params_preserved() {
    let storage = SqliteStorage::in_memory().await.unwrap();
    let iid = InstanceId::new();
    seed_instance(&storage, iid).await;
    let params = json!({"key": "value", "num": 42});
    let def = InterceptorDef {
        before_step: Some(InterceptorAction {
            handler: "h".into(),
            params: params.clone(),
        }),
        ..Default::default()
    };
    interceptors::emit_before_step(&storage, &def, iid, &BlockId::new("s")).await;
    let out = storage
        .get_block_output(iid, &BlockId::new("_interceptor:before:s"))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(out.output, params);
}

#[tokio::test]
async fn interceptor_50_output_size_correct() {
    let storage = SqliteStorage::in_memory().await.unwrap();
    let iid = InstanceId::new();
    seed_instance(&storage, iid).await;
    let params = json!({"data": "test"});
    let def = InterceptorDef {
        before_step: Some(InterceptorAction {
            handler: "h".into(),
            params: params.clone(),
        }),
        ..Default::default()
    };
    interceptors::emit_before_step(&storage, &def, iid, &BlockId::new("sz")).await;
    let out = storage
        .get_block_output(iid, &BlockId::new("_interceptor:before:sz"))
        .await
        .unwrap()
        .unwrap();
    let expected = u32::try_from(serde_json::to_vec(&params).unwrap().len()).unwrap();
    assert_eq!(out.output_size, expected);
}

#[tokio::test]
async fn interceptor_51_after_step_saves() {
    let storage = SqliteStorage::in_memory().await.unwrap();
    let iid = InstanceId::new();
    seed_instance(&storage, iid).await;
    let def = InterceptorDef {
        after_step: Some(InterceptorAction {
            handler: "post".into(),
            params: json!({"done": true}),
        }),
        ..Default::default()
    };
    interceptors::emit_after_step(&storage, &def, iid, &BlockId::new("s")).await;
    let out = storage
        .get_block_output(iid, &BlockId::new("_interceptor:after:s"))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(out.output["done"], true);
}

#[tokio::test]
async fn interceptor_52_after_only() {
    let storage = SqliteStorage::in_memory().await.unwrap();
    let iid = InstanceId::new();
    seed_instance(&storage, iid).await;
    let def = InterceptorDef {
        after_step: Some(InterceptorAction {
            handler: "x".into(),
            params: json!({"only": "after"}),
        }),
        ..Default::default()
    };
    interceptors::emit_before_step(&storage, &def, iid, &BlockId::new("s")).await;
    interceptors::emit_after_step(&storage, &def, iid, &BlockId::new("s")).await;
    assert!(storage
        .get_block_output(iid, &BlockId::new("_interceptor:before:s"))
        .await
        .unwrap()
        .is_none());
    assert!(storage
        .get_block_output(iid, &BlockId::new("_interceptor:after:s"))
        .await
        .unwrap()
        .is_some());
}

#[tokio::test]
async fn interceptor_53_after_step_complex_params() {
    let storage = SqliteStorage::in_memory().await.unwrap();
    let iid = InstanceId::new();
    seed_instance(&storage, iid).await;
    let params = json!({"nested": {"a": [1,2,3]}, "flag": true});
    let def = InterceptorDef {
        after_step: Some(InterceptorAction {
            handler: "h".into(),
            params: params.clone(),
        }),
        ..Default::default()
    };
    interceptors::emit_after_step(&storage, &def, iid, &BlockId::new("cx")).await;
    let out = storage
        .get_block_output(iid, &BlockId::new("_interceptor:after:cx"))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(out.output, params);
}

#[tokio::test]
async fn interceptor_54_both_before_and_after() {
    let storage = SqliteStorage::in_memory().await.unwrap();
    let iid = InstanceId::new();
    seed_instance(&storage, iid).await;
    let def = InterceptorDef {
        before_step: Some(InterceptorAction {
            handler: "h".into(),
            params: json!({"t": "before"}),
        }),
        after_step: Some(InterceptorAction {
            handler: "h".into(),
            params: json!({"t": "after"}),
        }),
        ..Default::default()
    };
    let step = BlockId::new("s");
    interceptors::emit_before_step(&storage, &def, iid, &step).await;
    interceptors::emit_after_step(&storage, &def, iid, &step).await;
    assert_eq!(
        storage
            .get_block_output(iid, &BlockId::new("_interceptor:before:s"))
            .await
            .unwrap()
            .unwrap()
            .output["t"],
        "before"
    );
    assert_eq!(
        storage
            .get_block_output(iid, &BlockId::new("_interceptor:after:s"))
            .await
            .unwrap()
            .unwrap()
            .output["t"],
        "after"
    );
}

#[tokio::test]
async fn interceptor_55_attempt_is_zero() {
    let storage = SqliteStorage::in_memory().await.unwrap();
    let iid = InstanceId::new();
    seed_instance(&storage, iid).await;
    let def = InterceptorDef {
        after_step: Some(InterceptorAction {
            handler: "h".into(),
            params: json!({}),
        }),
        ..Default::default()
    };
    interceptors::emit_after_step(&storage, &def, iid, &BlockId::new("s")).await;
    assert_eq!(
        storage
            .get_block_output(iid, &BlockId::new("_interceptor:after:s"))
            .await
            .unwrap()
            .unwrap()
            .attempt,
        0
    );
}

#[tokio::test]
async fn interceptor_56_on_complete() {
    let storage = SqliteStorage::in_memory().await.unwrap();
    let iid = InstanceId::new();
    seed_instance(&storage, iid).await;
    let def = InterceptorDef {
        on_complete: Some(InterceptorAction {
            handler: "done".into(),
            params: json!({"completed": true}),
        }),
        ..Default::default()
    };
    interceptors::emit_on_complete(&storage, &def, iid).await;
    assert_eq!(
        storage
            .get_block_output(iid, &BlockId::new("_interceptor:on_complete"))
            .await
            .unwrap()
            .unwrap()
            .output["completed"],
        true
    );
}

#[tokio::test]
async fn interceptor_57_on_failure() {
    let storage = SqliteStorage::in_memory().await.unwrap();
    let iid = InstanceId::new();
    seed_instance(&storage, iid).await;
    let def = InterceptorDef {
        on_failure: Some(InterceptorAction {
            handler: "fail".into(),
            params: json!({"error": true}),
        }),
        ..Default::default()
    };
    interceptors::emit_on_failure(&storage, &def, iid).await;
    assert_eq!(
        storage
            .get_block_output(iid, &BlockId::new("_interceptor:on_failure"))
            .await
            .unwrap()
            .unwrap()
            .output["error"],
        true
    );
}

#[tokio::test]
async fn interceptor_58_on_signal_merges() {
    let storage = SqliteStorage::in_memory().await.unwrap();
    let iid = InstanceId::new();
    seed_instance(&storage, iid).await;
    let def = InterceptorDef {
        on_signal: Some(InterceptorAction {
            handler: "sig".into(),
            params: json!({"notify": true}),
        }),
        ..Default::default()
    };
    interceptors::emit_on_signal(&storage, &def, iid, &json!({"type": "pause"})).await;
    let out = storage
        .get_block_output(iid, &BlockId::new("_interceptor:on_signal"))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(out.output["notify"], true);
    assert_eq!(out.output["_signal"]["type"], "pause");
}

#[tokio::test]
async fn interceptor_59_on_signal_wraps_non_object() {
    let storage = SqliteStorage::in_memory().await.unwrap();
    let iid = InstanceId::new();
    seed_instance(&storage, iid).await;
    let def = InterceptorDef {
        on_signal: Some(InterceptorAction {
            handler: "sig".into(),
            params: json!("string_params"),
        }),
        ..Default::default()
    };
    interceptors::emit_on_signal(&storage, &def, iid, &json!({"type": "cancel"})).await;
    let out = storage
        .get_block_output(iid, &BlockId::new("_interceptor:on_signal"))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(out.output["_params"], "string_params");
    assert_eq!(out.output["_signal"]["type"], "cancel");
}

#[tokio::test]
async fn interceptor_60_all_noop() {
    let storage = SqliteStorage::in_memory().await.unwrap();
    let iid = InstanceId::new();
    seed_instance(&storage, iid).await;
    let def = InterceptorDef::default();
    interceptors::emit_before_step(&storage, &def, iid, &BlockId::new("s")).await;
    interceptors::emit_after_step(&storage, &def, iid, &BlockId::new("s")).await;
    interceptors::emit_on_signal(&storage, &def, iid, &json!({})).await;
    interceptors::emit_on_complete(&storage, &def, iid).await;
    interceptors::emit_on_failure(&storage, &def, iid).await;
    assert!(storage
        .get_block_output(iid, &BlockId::new("_interceptor:before:s"))
        .await
        .unwrap()
        .is_none());
    assert!(storage
        .get_block_output(iid, &BlockId::new("_interceptor:on_complete"))
        .await
        .unwrap()
        .is_none());
}

// ================================================================
// SEQUENCE CACHE (Tests 61-70)
// ================================================================

#[tokio::test]
async fn cache_61_miss_fetches() {
    let storage = SqliteStorage::in_memory().await.unwrap();
    let seq = mk_seq("flow-61");
    storage.create_sequence(&seq).await.unwrap();
    let cache = SequenceCache::new(100, Duration::from_secs(60));
    let got = cache.get_by_id(&storage, seq.id).await.unwrap();
    assert_eq!(got.name, "flow-61");
}

#[tokio::test]
async fn cache_62_hit_returns_same() {
    let storage = SqliteStorage::in_memory().await.unwrap();
    let seq = mk_seq("flow-62");
    storage.create_sequence(&seq).await.unwrap();
    let cache = SequenceCache::new(100, Duration::from_secs(60));
    let a = cache.get_by_id(&storage, seq.id).await.unwrap();
    let b = cache.get_by_id(&storage, seq.id).await.unwrap();
    assert_eq!(a.id, b.id);
}

#[tokio::test]
async fn cache_63_missing_returns_error() {
    let storage = SqliteStorage::in_memory().await.unwrap();
    let cache = SequenceCache::new(100, Duration::from_secs(60));
    let err = cache
        .get_by_id(&storage, SequenceId::new())
        .await
        .unwrap_err();
    assert!(err.to_string().contains("not found"));
}

#[tokio::test]
async fn cache_64_by_name_missing() {
    let storage = SqliteStorage::in_memory().await.unwrap();
    let cache = SequenceCache::new(100, Duration::from_secs(60));
    let got = cache
        .get_by_name(
            &storage,
            &TenantId::unchecked("t"),
            &Namespace::new("ns"),
            "missing",
            None,
        )
        .await
        .unwrap();
    assert!(got.is_none());
}

#[tokio::test]
async fn cache_65_by_name_populates_both() {
    let storage = SqliteStorage::in_memory().await.unwrap();
    let seq = mk_seq("flow-65");
    storage.create_sequence(&seq).await.unwrap();
    let cache = SequenceCache::new(100, Duration::from_secs(60));
    let _ = cache
        .get_by_name(&storage, &seq.tenant_id, &seq.namespace, "flow-65", Some(1))
        .await
        .unwrap();
    let got = cache.get_by_id(&storage, seq.id).await.unwrap();
    assert_eq!(got.id, seq.id);
}

#[tokio::test]
async fn cache_66_invalidate_by_id() {
    let storage = SqliteStorage::in_memory().await.unwrap();
    let seq = mk_seq("flow-66");
    storage.create_sequence(&seq).await.unwrap();
    let cache = SequenceCache::new(100, Duration::from_secs(60));
    let _ = cache.get_by_id(&storage, seq.id).await.unwrap();
    cache.invalidate_by_id(seq.id).await;
    let got = cache.get_by_id(&storage, seq.id).await.unwrap();
    assert_eq!(got.id, seq.id);
}

#[tokio::test]
async fn cache_67_invalidate_all() {
    let storage = SqliteStorage::in_memory().await.unwrap();
    let seq1 = mk_seq("flow-67a");
    let seq2 = mk_seq("flow-67b");
    storage.create_sequence(&seq1).await.unwrap();
    storage.create_sequence(&seq2).await.unwrap();
    let cache = SequenceCache::new(100, Duration::from_secs(60));
    let _ = cache.get_by_id(&storage, seq1.id).await.unwrap();
    let _ = cache.get_by_id(&storage, seq2.id).await.unwrap();
    cache.invalidate_all();
    assert_eq!(
        cache.get_by_id(&storage, seq1.id).await.unwrap().id,
        seq1.id
    );
    assert_eq!(
        cache.get_by_id(&storage, seq2.id).await.unwrap().id,
        seq2.id
    );
}

#[tokio::test]
async fn cache_68_ttl_expiry_retrieval() {
    let storage = SqliteStorage::in_memory().await.unwrap();
    let seq = mk_seq("flow-68");
    storage.create_sequence(&seq).await.unwrap();
    let cache = SequenceCache::new(100, Duration::from_millis(1));
    let _ = cache.get_by_id(&storage, seq.id).await.unwrap();
    tokio::time::sleep(Duration::from_millis(50)).await;
    let got = cache.get_by_id(&storage, seq.id).await.unwrap();
    assert_eq!(got.id, seq.id);
}

#[tokio::test]
async fn cache_69_capacity_eviction() {
    let storage = SqliteStorage::in_memory().await.unwrap();
    let seq1 = mk_seq("flow-69a");
    let seq2 = mk_seq("flow-69b");
    storage.create_sequence(&seq1).await.unwrap();
    storage.create_sequence(&seq2).await.unwrap();
    let cache = SequenceCache::new(1, Duration::from_secs(60));
    let _ = cache.get_by_id(&storage, seq1.id).await.unwrap();
    let _ = cache.get_by_id(&storage, seq2.id).await.unwrap();
    assert_eq!(
        cache.get_by_id(&storage, seq1.id).await.unwrap().id,
        seq1.id
    );
    assert_eq!(
        cache.get_by_id(&storage, seq2.id).await.unwrap().id,
        seq2.id
    );
}

#[tokio::test]
async fn cache_70_id_lookup_warms_name() {
    let storage = SqliteStorage::in_memory().await.unwrap();
    let seq = mk_seq("flow-70");
    storage.create_sequence(&seq).await.unwrap();
    let cache = SequenceCache::new(100, Duration::from_secs(60));
    let _ = cache.get_by_id(&storage, seq.id).await.unwrap();
    let got = cache
        .get_by_name(&storage, &seq.tenant_id, &seq.namespace, "flow-70", Some(1))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(got.id, seq.id);
}

// ================================================================
// WEBHOOKS (Tests 71-80)
// ================================================================

#[test]
fn webhook_71_instance_event_structure() {
    let id = InstanceId::new();
    let event = instance_event("instance.completed", id, json!({"result": "ok"}));
    assert_eq!(event.event_type, "instance.completed");
    assert_eq!(event.instance_id, Some(id));
    assert_eq!(event.data["result"], "ok");
    assert!(!event.timestamp.is_empty());
}

#[test]
fn webhook_72_serializes() {
    let event = WebhookEvent {
        event_type: "test".into(),
        instance_id: None,
        timestamp: "2026-01-01T00:00:00Z".into(),
        data: json!({"key": "val"}),
    };
    let json_str = serde_json::to_string(&event).unwrap();
    assert!(json_str.contains("\"event_type\":\"test\""));
    assert!(json_str.contains("\"instance_id\":null"));
}

#[test]
fn webhook_73_timestamp_rfc3339() {
    let event = instance_event("x", InstanceId::new(), json!({}));
    assert!(chrono::DateTime::parse_from_rfc3339(&event.timestamp).is_ok());
}

#[test]
fn webhook_74_event_type_preserved() {
    let event = instance_event("instance.failed", InstanceId::new(), json!({}));
    assert_eq!(event.event_type, "instance.failed");
}

#[test]
fn webhook_75_none_instance_id_serializes_as_null() {
    let event = WebhookEvent {
        event_type: "system.tick".into(),
        instance_id: None,
        timestamp: "t".into(),
        data: json!({}),
    };
    let json_str = serde_json::to_string(&event).unwrap();
    assert!(json_str.contains("\"instance_id\":null"));
}

#[test]
fn webhook_76_emit_empty_urls() {
    use orch8_types::config::WebhookConfig;
    let config = WebhookConfig {
        urls: vec![],
        timeout_secs: 5,
        max_retries: 0,
        secret: None,
    };
    let event = instance_event("test", InstanceId::new(), json!({}));
    webhooks::emit(&config, &event, &CancellationToken::new());
}

#[test]
fn webhook_77_instance_id_serialized() {
    let id = InstanceId::new();
    let event = WebhookEvent {
        event_type: "e".into(),
        instance_id: Some(id),
        timestamp: "t".into(),
        data: json!({}),
    };
    let json_str = serde_json::to_string(&event).unwrap();
    assert!(json_str.contains(&id.to_string()));
}

#[test]
fn webhook_78_cloneable() {
    let event = WebhookEvent {
        event_type: "e".into(),
        instance_id: Some(InstanceId::new()),
        timestamp: "t".into(),
        data: json!({"k": "v"}),
    };
    let cloned = event.clone();
    assert_eq!(event.event_type, cloned.event_type);
    assert_eq!(event.data, cloned.data);
}

#[test]
fn webhook_79_nested_data() {
    let payload = json!({"nested": {"a": [1, 2]}, "b": null});
    let event = instance_event("e", InstanceId::new(), payload.clone());
    assert_eq!(event.data, payload);
}

#[test]
fn webhook_80_cancelled_token_noop() {
    use orch8_types::config::WebhookConfig;
    let config = WebhookConfig {
        urls: vec![],
        timeout_secs: 1,
        max_retries: 0,
        secret: None,
    };
    let event = instance_event("x", InstanceId::new(), json!({}));
    let cancel = CancellationToken::new();
    cancel.cancel();
    webhooks::emit(&config, &event, &cancel);
}

// ================================================================
// RECOVERY (Tests 81-85)
// ================================================================

#[tokio::test]
async fn recovery_81_empty_db() {
    let storage = SqliteStorage::in_memory().await.unwrap();
    assert_eq!(recover_stale_instances(&storage, 300).await.unwrap(), 0);
}

#[tokio::test]
async fn recovery_82_zero_threshold() {
    let storage = SqliteStorage::in_memory().await.unwrap();
    assert_eq!(recover_stale_instances(&storage, 0).await.unwrap(), 0);
}

#[tokio::test]
async fn recovery_83_stale_instance_recovered() {
    let storage = SqliteStorage::in_memory().await.unwrap();
    let seq = mk_seq("recovery");
    storage.create_sequence(&seq).await.unwrap();
    let now = Utc::now();
    let inst = TaskInstance {
        id: InstanceId::new(),
        sequence_id: seq.id,
        tenant_id: TenantId::unchecked("t"),
        namespace: Namespace::new("ns"),
        state: InstanceState::Running,
        next_fire_at: Some(now - chrono::Duration::seconds(600)),
        priority: Priority::Normal,
        timezone: "UTC".into(),
        metadata: json!({}),
        context: ExecutionContext::default(),
        concurrency_key: None,
        max_concurrency: None,
        idempotency_key: None,
        session_id: None,
        parent_instance_id: None,
        created_at: now - chrono::Duration::seconds(600),
        updated_at: now - chrono::Duration::seconds(600),
    };
    storage.create_instance(&inst).await.unwrap();
    assert_eq!(recover_stale_instances(&storage, 60).await.unwrap(), 1);
    assert_eq!(
        storage.get_instance(inst.id).await.unwrap().unwrap().state,
        InstanceState::Scheduled
    );
}

#[tokio::test]
async fn recovery_84_fresh_instance_untouched() {
    let storage = SqliteStorage::in_memory().await.unwrap();
    let seq = mk_seq("fresh");
    storage.create_sequence(&seq).await.unwrap();
    let now = Utc::now();
    let inst = TaskInstance {
        id: InstanceId::new(),
        sequence_id: seq.id,
        tenant_id: TenantId::unchecked("t"),
        namespace: Namespace::new("ns"),
        state: InstanceState::Running,
        next_fire_at: Some(now),
        priority: Priority::Normal,
        timezone: "UTC".into(),
        metadata: json!({}),
        context: ExecutionContext::default(),
        concurrency_key: None,
        max_concurrency: None,
        idempotency_key: None,
        session_id: None,
        parent_instance_id: None,
        created_at: now,
        updated_at: now,
    };
    storage.create_instance(&inst).await.unwrap();
    assert_eq!(recover_stale_instances(&storage, 300).await.unwrap(), 0);
    assert_eq!(
        storage.get_instance(inst.id).await.unwrap().unwrap().state,
        InstanceState::Running
    );
}

#[tokio::test]
async fn recovery_85_multiple_stale() {
    let storage = SqliteStorage::in_memory().await.unwrap();
    let seq = mk_seq("multi-rec");
    storage.create_sequence(&seq).await.unwrap();
    let now = Utc::now();
    for _ in 0..3 {
        let inst = TaskInstance {
            id: InstanceId::new(),
            sequence_id: seq.id,
            tenant_id: TenantId::unchecked("t"),
            namespace: Namespace::new("ns"),
            state: InstanceState::Running,
            next_fire_at: Some(now - chrono::Duration::seconds(600)),
            priority: Priority::Normal,
            timezone: "UTC".into(),
            metadata: json!({}),
            context: ExecutionContext::default(),
            concurrency_key: None,
            max_concurrency: None,
            idempotency_key: None,
            session_id: None,
            parent_instance_id: None,
            created_at: now - chrono::Duration::seconds(600),
            updated_at: now - chrono::Duration::seconds(600),
        };
        storage.create_instance(&inst).await.unwrap();
    }
    assert_eq!(recover_stale_instances(&storage, 60).await.unwrap(), 3);
}

// ================================================================
// BUILT-IN HANDLERS (Tests 86-100)
// ================================================================

#[tokio::test]
async fn handler_86_send_signal_same_tenant() {
    use orch8_storage::SignalStore;
    use orch8_types::signal::SignalType;
    let registry = make_registry();
    let storage = Arc::new(SqliteStorage::in_memory().await.unwrap());
    let storage_dyn: Arc<dyn StorageBackend> = storage.clone();
    let caller = mk_handler_instance("T1", InstanceState::Running);
    let target = mk_handler_instance("T1", InstanceState::Running);
    storage.create_instance(&caller).await.unwrap();
    storage.create_instance(&target).await.unwrap();
    let ctx = mk_step_ctx(
        &caller,
        storage_dyn,
        json!({"instance_id": target.id.to_string(), "signal_type": "cancel"}),
    );
    let result = invoke_handler(&registry, "send_signal", ctx).await.unwrap();
    assert!(result.get("signal_id").is_some());
    let pending = storage.get_pending_signals(target.id).await.unwrap();
    assert_eq!(pending.len(), 1);
    assert_eq!(pending[0].signal_type, SignalType::Cancel);
}

#[tokio::test]
async fn handler_87_send_signal_cross_tenant() {
    let registry = make_registry();
    let storage = Arc::new(SqliteStorage::in_memory().await.unwrap());
    let storage_dyn: Arc<dyn StorageBackend> = storage.clone();
    let caller = mk_handler_instance("T1", InstanceState::Running);
    let target = mk_handler_instance("T2", InstanceState::Running);
    storage.create_instance(&caller).await.unwrap();
    storage.create_instance(&target).await.unwrap();
    let ctx = mk_step_ctx(
        &caller,
        storage_dyn,
        json!({"instance_id": target.id.to_string(), "signal_type": "cancel"}),
    );
    let err = invoke_handler(&registry, "send_signal", ctx)
        .await
        .unwrap_err();
    assert!(matches!(err, StepError::Permanent { .. }));
}

#[tokio::test]
async fn handler_88_send_signal_missing_target() {
    let registry = make_registry();
    let storage = Arc::new(SqliteStorage::in_memory().await.unwrap());
    let storage_dyn: Arc<dyn StorageBackend> = storage.clone();
    let caller = mk_handler_instance("T1", InstanceState::Running);
    storage.create_instance(&caller).await.unwrap();
    let ctx = mk_step_ctx(
        &caller,
        storage_dyn,
        json!({"instance_id": InstanceId::new().to_string(), "signal_type": "cancel"}),
    );
    let err = invoke_handler(&registry, "send_signal", ctx)
        .await
        .unwrap_err();
    assert!(matches!(err, StepError::Permanent { .. }));
}

#[tokio::test]
async fn handler_89_send_signal_terminal_target() {
    let registry = make_registry();
    let storage = Arc::new(SqliteStorage::in_memory().await.unwrap());
    let storage_dyn: Arc<dyn StorageBackend> = storage.clone();
    let caller = mk_handler_instance("T1", InstanceState::Running);
    let target = mk_handler_instance("T1", InstanceState::Completed);
    storage.create_instance(&caller).await.unwrap();
    storage.create_instance(&target).await.unwrap();
    let ctx = mk_step_ctx(
        &caller,
        storage_dyn,
        json!({"instance_id": target.id.to_string(), "signal_type": "cancel"}),
    );
    let err = invoke_handler(&registry, "send_signal", ctx)
        .await
        .unwrap_err();
    assert!(matches!(err, StepError::Permanent { .. }));
}

#[tokio::test]
async fn handler_90_send_signal_custom_type() {
    use orch8_storage::SignalStore;
    use orch8_types::signal::SignalType;
    let registry = make_registry();
    let storage = Arc::new(SqliteStorage::in_memory().await.unwrap());
    let storage_dyn: Arc<dyn StorageBackend> = storage.clone();
    let caller = mk_handler_instance("T1", InstanceState::Running);
    let target = mk_handler_instance("T1", InstanceState::Running);
    storage.create_instance(&caller).await.unwrap();
    storage.create_instance(&target).await.unwrap();
    let ctx = mk_step_ctx(
        &caller,
        storage_dyn,
        json!({"instance_id": target.id.to_string(), "signal_type": {"custom": "my_event"}, "payload": {"data": 42}}),
    );
    invoke_handler(&registry, "send_signal", ctx).await.unwrap();
    let pending = storage.get_pending_signals(target.id).await.unwrap();
    assert_eq!(
        pending[0].signal_type,
        SignalType::Custom("my_event".into())
    );
    assert_eq!(pending[0].payload, json!({"data": 42}));
}

#[tokio::test]
async fn handler_91_emit_event_creates_child() {
    use orch8_types::trigger::{TriggerDef, TriggerType};
    let registry = make_registry();
    let storage = Arc::new(SqliteStorage::in_memory().await.unwrap());
    let storage_dyn: Arc<dyn StorageBackend> = storage.clone();
    let caller = mk_handler_instance("T1", InstanceState::Running);
    storage.create_instance(&caller).await.unwrap();
    let seq = SequenceDefinition {
        id: SequenceId::new(),
        tenant_id: TenantId::unchecked("T1"),
        namespace: Namespace::new("default"),
        name: "child-seq".into(),
        version: 1,
        deprecated: false,
        status: SequenceStatus::default(),
        blocks: vec![],
        interceptors: None,
        created_at: Utc::now(),
    };
    storage.create_sequence(&seq).await.unwrap();
    let trigger = TriggerDef {
        slug: "on-thing".into(),
        sequence_name: "child-seq".into(),
        version: None,
        tenant_id: TenantId::unchecked("T1"),
        namespace: "default".into(),
        enabled: true,
        secret: None,
        trigger_type: TriggerType::Event,
        config: serde_json::Value::Null,
        created_at: Utc::now(),
        updated_at: Utc::now(),
    };
    storage.create_trigger(&trigger).await.unwrap();
    let ctx = mk_step_ctx(
        &caller,
        storage_dyn,
        json!({"trigger_slug": "on-thing", "data": {"x": 1}}),
    );
    let result = invoke_handler(&registry, "emit_event", ctx).await.unwrap();
    assert_eq!(result["deduped"], false);
    assert!(result.get("instance_id").is_some());
}

#[tokio::test]
async fn handler_92_emit_event_missing_trigger() {
    let registry = make_registry();
    let storage = Arc::new(SqliteStorage::in_memory().await.unwrap());
    let storage_dyn: Arc<dyn StorageBackend> = storage.clone();
    let caller = mk_handler_instance("T1", InstanceState::Running);
    storage.create_instance(&caller).await.unwrap();
    let ctx = mk_step_ctx(&caller, storage_dyn, json!({"trigger_slug": "nonexistent"}));
    let err = invoke_handler(&registry, "emit_event", ctx)
        .await
        .unwrap_err();
    assert!(matches!(err, StepError::Permanent { .. }));
}

#[tokio::test]
async fn handler_93_emit_event_missing_slug() {
    let registry = make_registry();
    let storage = Arc::new(SqliteStorage::in_memory().await.unwrap());
    let storage_dyn: Arc<dyn StorageBackend> = storage.clone();
    let caller = mk_handler_instance("T1", InstanceState::Running);
    storage.create_instance(&caller).await.unwrap();
    let ctx = mk_step_ctx(&caller, storage_dyn, json!({}));
    let err = invoke_handler(&registry, "emit_event", ctx)
        .await
        .unwrap_err();
    assert!(matches!(err, StepError::Permanent { .. }));
}

#[tokio::test]
async fn handler_94_emit_event_cross_tenant() {
    use orch8_types::trigger::{TriggerDef, TriggerType};
    let registry = make_registry();
    let storage = Arc::new(SqliteStorage::in_memory().await.unwrap());
    let storage_dyn: Arc<dyn StorageBackend> = storage.clone();
    let caller = mk_handler_instance("T1", InstanceState::Running);
    storage.create_instance(&caller).await.unwrap();
    let seq = SequenceDefinition {
        id: SequenceId::new(),
        tenant_id: TenantId::unchecked("T2"),
        namespace: Namespace::new("default"),
        name: "child-seq".into(),
        version: 1,
        deprecated: false,
        status: SequenceStatus::default(),
        blocks: vec![],
        interceptors: None,
        created_at: Utc::now(),
    };
    storage.create_sequence(&seq).await.unwrap();
    let trigger = TriggerDef {
        slug: "cross-trig".into(),
        sequence_name: "child-seq".into(),
        version: None,
        tenant_id: TenantId::unchecked("T2"),
        namespace: "default".into(),
        enabled: true,
        secret: None,
        trigger_type: TriggerType::Event,
        config: serde_json::Value::Null,
        created_at: Utc::now(),
        updated_at: Utc::now(),
    };
    storage.create_trigger(&trigger).await.unwrap();
    let ctx = mk_step_ctx(&caller, storage_dyn, json!({"trigger_slug": "cross-trig"}));
    let err = invoke_handler(&registry, "emit_event", ctx)
        .await
        .unwrap_err();
    assert!(matches!(err, StepError::Permanent { .. }));
}

#[tokio::test]
async fn handler_95_emit_event_disabled_trigger() {
    use orch8_types::trigger::{TriggerDef, TriggerType};
    let registry = make_registry();
    let storage = Arc::new(SqliteStorage::in_memory().await.unwrap());
    let storage_dyn: Arc<dyn StorageBackend> = storage.clone();
    let caller = mk_handler_instance("T1", InstanceState::Running);
    storage.create_instance(&caller).await.unwrap();
    let seq = SequenceDefinition {
        id: SequenceId::new(),
        tenant_id: TenantId::unchecked("T1"),
        namespace: Namespace::new("default"),
        name: "ch".into(),
        version: 1,
        deprecated: false,
        status: SequenceStatus::default(),
        blocks: vec![],
        interceptors: None,
        created_at: Utc::now(),
    };
    storage.create_sequence(&seq).await.unwrap();
    let trigger = TriggerDef {
        slug: "dis-trig".into(),
        sequence_name: "ch".into(),
        version: None,
        tenant_id: TenantId::unchecked("T1"),
        namespace: "default".into(),
        enabled: false,
        secret: None,
        trigger_type: TriggerType::Event,
        config: serde_json::Value::Null,
        created_at: Utc::now(),
        updated_at: Utc::now(),
    };
    storage.create_trigger(&trigger).await.unwrap();
    let ctx = mk_step_ctx(&caller, storage_dyn, json!({"trigger_slug": "dis-trig"}));
    let err = invoke_handler(&registry, "emit_event", ctx)
        .await
        .unwrap_err();
    assert!(matches!(err, StepError::Permanent { .. }));
}

#[tokio::test]
async fn handler_96_query_instance_same_tenant() {
    let registry = make_registry();
    let storage = Arc::new(SqliteStorage::in_memory().await.unwrap());
    let storage_dyn: Arc<dyn StorageBackend> = storage.clone();
    let caller = mk_handler_instance("T1", InstanceState::Running);
    let target = mk_handler_instance("T1", InstanceState::Scheduled);
    storage.create_instance(&caller).await.unwrap();
    storage.create_instance(&target).await.unwrap();
    let ctx = mk_step_ctx(
        &caller,
        storage_dyn,
        json!({"instance_id": target.id.to_string()}),
    );
    let result = invoke_handler(&registry, "query_instance", ctx)
        .await
        .unwrap();
    assert_eq!(result["found"], true);
    assert_eq!(result["state"], "scheduled");
}

#[tokio::test]
async fn handler_97_query_instance_missing() {
    let registry = make_registry();
    let storage = Arc::new(SqliteStorage::in_memory().await.unwrap());
    let storage_dyn: Arc<dyn StorageBackend> = storage.clone();
    let caller = mk_handler_instance("T1", InstanceState::Running);
    storage.create_instance(&caller).await.unwrap();
    let ctx = mk_step_ctx(
        &caller,
        storage_dyn,
        json!({"instance_id": InstanceId::new().to_string()}),
    );
    let result = invoke_handler(&registry, "query_instance", ctx)
        .await
        .unwrap();
    assert_eq!(result["found"], false);
}

#[tokio::test]
async fn handler_98_query_instance_cross_tenant() {
    let registry = make_registry();
    let storage = Arc::new(SqliteStorage::in_memory().await.unwrap());
    let storage_dyn: Arc<dyn StorageBackend> = storage.clone();
    let caller = mk_handler_instance("T1", InstanceState::Running);
    let target = mk_handler_instance("T2", InstanceState::Running);
    storage.create_instance(&caller).await.unwrap();
    storage.create_instance(&target).await.unwrap();
    let ctx = mk_step_ctx(
        &caller,
        storage_dyn,
        json!({"instance_id": target.id.to_string()}),
    );
    let result = invoke_handler(&registry, "query_instance", ctx)
        .await
        .unwrap();
    assert_eq!(result, json!({"found": false}));
}

#[tokio::test]
async fn handler_99_self_modify_valid() {
    let registry = make_registry();
    let storage: Arc<dyn StorageBackend> = Arc::new(SqliteStorage::in_memory().await.unwrap());
    let block_json = serde_json::to_value(BlockDefinition::Step(Box::new(StepDef {
        id: BlockId::new("injected"),
        handler: "noop".into(),
        params: json!({}),
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
    })))
    .unwrap();
    let ctx = StepContext {
        instance_id: InstanceId::new(),
        tenant_id: TenantId::unchecked("t"),
        block_id: BlockId::new("mod"),
        params: json!({"blocks": [block_json], "position": 2}),
        context: ExecutionContext::default(),
        attempt: 1,
        storage,
        wait_for_input: None,
    };
    let result = invoke_handler(&registry, "self_modify", ctx).await.unwrap();
    assert_eq!(result["_self_modify"], true);
    assert_eq!(result["injected_count"], 1);
    assert_eq!(result["position"], 2);
}

#[tokio::test]
async fn handler_100_self_modify_missing_blocks() {
    let registry = make_registry();
    let storage: Arc<dyn StorageBackend> = Arc::new(SqliteStorage::in_memory().await.unwrap());
    let ctx = StepContext {
        instance_id: InstanceId::new(),
        tenant_id: TenantId::unchecked("t"),
        block_id: BlockId::new("mod"),
        params: json!({}),
        context: ExecutionContext::default(),
        attempt: 1,
        storage,
        wait_for_input: None,
    };
    let err = invoke_handler(&registry, "self_modify", ctx)
        .await
        .unwrap_err();
    assert!(matches!(err, StepError::Permanent { .. }));
}
