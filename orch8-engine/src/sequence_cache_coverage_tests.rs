//! Coverage tests for the compiled-plan sidecar caching in [`SequenceCache`].
//!
//! Pins the cache-hit identity, invalidation, and tamper-evident recompilation
//! semantics of [`SequenceCache::optimization_for`].
//!
//! Count contract: 9 independently named unit tests.

use chrono::Utc;
use orch8_types::ids::{BlockId, Namespace, SequenceId, TenantId};
use orch8_types::sequence::{BlockDefinition, SequenceDefinition, SequenceStatus, StepDef};
use serde_json::json;

use super::*;

fn mk_seq(name: &str) -> SequenceDefinition {
    SequenceDefinition {
        schema: None,
        schema_version: orch8_types::sequence::SEQUENCE_SCHEMA_VERSION,
        id: SequenceId::new(),
        tenant_id: TenantId::unchecked("t"),
        namespace: Namespace::new("ns"),
        name: name.into(),
        version: 1,
        deprecated: false,
        status: SequenceStatus::Production,
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
            output_schema: None,
            when: None,
            compensation: None,
        }))],
        interceptors: None,
        input_schema: None,
        sla: None,
        on_failure: None,
        on_cancel: None,
        created_at: Utc::now(),
    }
}

fn cache() -> SequenceCache {
    SequenceCache::new(100, Duration::from_secs(60))
}

#[tokio::test]
async fn coverage_plan_cache_001_compiles_plan_for_valid_sequence() {
    let seq = mk_seq("flow");
    let ir = cache().optimization_for(&seq).await.unwrap();
    ir.verify_equivalent(&seq).unwrap();
    assert_eq!(ir.nodes.len(), 1);
}

#[tokio::test]
async fn coverage_plan_cache_002_second_lookup_returns_cached_arc() {
    let cache = cache();
    let seq = mk_seq("flow");
    let first = cache.optimization_for(&seq).await.unwrap();
    let second = cache.optimization_for(&seq).await.unwrap();
    assert!(Arc::ptr_eq(&first, &second));
}

#[tokio::test]
async fn coverage_plan_cache_003_distinct_sequences_get_distinct_plans() {
    let cache = cache();
    let one = cache.optimization_for(&mk_seq("one")).await.unwrap();
    let two = cache.optimization_for(&mk_seq("two")).await.unwrap();
    assert!(!Arc::ptr_eq(&one, &two));
    assert_ne!(one.source_sha256, two.source_sha256);
}

#[tokio::test]
async fn coverage_plan_cache_004_invalidate_by_id_forces_recompile() {
    let cache = cache();
    let seq = mk_seq("flow");
    let first = cache.optimization_for(&seq).await.unwrap();
    cache.invalidate_by_id(seq.id).await;
    let second = cache.optimization_for(&seq).await.unwrap();
    assert!(!Arc::ptr_eq(&first, &second));
    assert_eq!(first.source_sha256, second.source_sha256);
}

#[tokio::test]
async fn coverage_plan_cache_005_invalidate_all_forces_recompile() {
    let cache = cache();
    let seq = mk_seq("flow");
    let first = cache.optimization_for(&seq).await.unwrap();
    cache.invalidate_all();
    let second = cache.optimization_for(&seq).await.unwrap();
    assert!(!Arc::ptr_eq(&first, &second));
}

#[tokio::test]
async fn coverage_plan_cache_006_changed_definition_same_id_is_recompiled() {
    let cache = cache();
    let seq = mk_seq("flow");
    let stale = cache.optimization_for(&seq).await.unwrap();

    // Same id, changed content: the cached plan's equivalence proof must
    // fail closed and trigger a recompile instead of serving the stale IR.
    let mut changed = seq.clone();
    changed.name = "flow-renamed".into();
    let fresh = cache.optimization_for(&changed).await.unwrap();

    assert!(!Arc::ptr_eq(&stale, &fresh));
    assert_ne!(stale.source_sha256, fresh.source_sha256);
    fresh.verify_equivalent(&changed).unwrap();
    assert!(stale.verify_equivalent(&changed).is_err());
}

#[tokio::test]
async fn coverage_plan_cache_007_recompiled_plan_is_cached_again() {
    let cache = cache();
    let mut seq = mk_seq("flow");
    cache.optimization_for(&seq).await.unwrap();
    seq.name = "flow-v2".into();
    let first = cache.optimization_for(&seq).await.unwrap();
    let second = cache.optimization_for(&seq).await.unwrap();
    assert!(Arc::ptr_eq(&first, &second));
}

#[tokio::test]
async fn coverage_plan_cache_008_plan_flags_match_sequence_shape() {
    let cache = cache();
    let ir = cache.optimization_for(&mk_seq("flat")).await.unwrap();
    assert!(!ir.top_level_has_composite);
    assert!(!ir.top_level_has_plugin_handler);
}

#[tokio::test]
async fn coverage_plan_cache_009_invalid_sequence_is_a_safe_miss() {
    // A workflow the compiler rejects (empty blocks) must surface as `None`
    // rather than an error, and must not poison the cache for later lookups.
    let cache = cache();
    let mut invalid = mk_seq("invalid");
    invalid.blocks.clear();
    assert!(cache.optimization_for(&invalid).await.is_none());

    let valid = mk_seq("valid");
    let ir = cache.optimization_for(&valid).await.unwrap();
    assert_eq!(ir.nodes.len(), 1);
}
