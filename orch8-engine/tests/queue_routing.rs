//! Tests for dynamic queue routing resolution at enqueue.

use std::sync::Arc;

use orch8_engine::queue_routing::resolve_queue;
use orch8_storage::{StorageBackend, sqlite::SqliteStorage};
use orch8_types::ids::TenantId;
use orch8_types::queue_routing::QueueRoutingRule;

async fn storage() -> Arc<dyn StorageBackend> {
    Arc::new(SqliteStorage::in_memory().await.unwrap())
}

fn mk_rule(
    tenant: &str,
    handler: &str,
    match_queue: Option<&str>,
    override_to: &str,
    priority: i32,
) -> QueueRoutingRule {
    QueueRoutingRule {
        id: uuid::Uuid::now_v7(),
        tenant_id: tenant.into(),
        handler_name: handler.into(),
        match_queue: match_queue.map(String::from),
        queue_override: override_to.into(),
        priority,
        enabled: true,
        created_at: chrono::Utc::now(),
        updated_at: chrono::Utc::now(),
    }
}

#[tokio::test]
async fn no_rules_returns_declared_queue() {
    let s = storage().await;
    let t = TenantId::unchecked("t1");
    let got = resolve_queue(s.as_ref(), &t, "h", Some("default".into())).await;
    assert_eq!(got.as_deref(), Some("default"));
    // No declared queue, no rules -> None.
    assert_eq!(resolve_queue(s.as_ref(), &t, "h", None).await, None);
}

#[tokio::test]
async fn unconditional_rule_overrides_queue() {
    let s = storage().await;
    s.create_queue_routing_rule(&mk_rule("t1", "h", None, "vip", 0))
        .await
        .unwrap();
    let t = TenantId::unchecked("t1");
    let got = resolve_queue(s.as_ref(), &t, "h", Some("default".into())).await;
    assert_eq!(got.as_deref(), Some("vip"));
    // A different handler is unaffected.
    assert_eq!(
        resolve_queue(s.as_ref(), &t, "other", Some("default".into()))
            .await
            .as_deref(),
        Some("default")
    );
}

#[tokio::test]
async fn highest_priority_match_wins() {
    let s = storage().await;
    s.create_queue_routing_rule(&mk_rule("t1", "h", None, "low", 1))
        .await
        .unwrap();
    s.create_queue_routing_rule(&mk_rule("t1", "h", None, "high", 100))
        .await
        .unwrap();
    let t = TenantId::unchecked("t1");
    assert_eq!(
        resolve_queue(s.as_ref(), &t, "h", None).await.as_deref(),
        Some("high")
    );
}

#[tokio::test]
async fn match_queue_only_applies_on_match() {
    let s = storage().await;
    // Remap only tasks currently declared for queue "slow".
    s.create_queue_routing_rule(&mk_rule("t1", "h", Some("slow"), "fast", 0))
        .await
        .unwrap();
    let t = TenantId::unchecked("t1");
    // Matching current queue -> remapped.
    assert_eq!(
        resolve_queue(s.as_ref(), &t, "h", Some("slow".into()))
            .await
            .as_deref(),
        Some("fast")
    );
    // Non-matching current queue -> untouched.
    assert_eq!(
        resolve_queue(s.as_ref(), &t, "h", Some("other".into()))
            .await
            .as_deref(),
        Some("other")
    );
}

#[tokio::test]
async fn disabled_rule_is_ignored() {
    let s = storage().await;
    let mut rule = mk_rule("t1", "h", None, "vip", 0);
    rule.enabled = false;
    s.create_queue_routing_rule(&rule).await.unwrap();
    let t = TenantId::unchecked("t1");
    assert_eq!(
        resolve_queue(s.as_ref(), &t, "h", Some("default".into()))
            .await
            .as_deref(),
        Some("default")
    );
}
