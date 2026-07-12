//! Built-in `wait_for_event` handler — declarative durable event
//! correlation.
//!
//! Pair with `wait_for_input` on the step so the engine parks the
//! instance; the wait itself is registered just before parking (the
//! gate runs before the handler). Event ingestion resumes the instance,
//! after which this handler returns the matched payloads as the block's
//! output: `{{ outputs.<block_id>.events.<name>.payload }}`. Do not set
//! custom `wait_for_input.choices` (linted) — the resume signal carries
//! the default choice.
//!
//! ```json
//! {
//!   "id": "await_order_facts",
//!   "handler": "wait_for_event",
//!   "params": {
//!     "events": ["payment_received", "inventory_reserved"],
//!     "correlation_key": "{{ context.data.order_id }}",
//!     "join": "all"
//!   },
//!   "wait_for_input": { "prompt": "waiting for order events", "timeout": 86400000 }
//! }
//! ```
//!
//! ## Params
//!
//! | Field | Type | Default | Description |
//! |-------|------|---------|-------------|
//! | `events` | string or [string] | — | Event name(s) to wait for (required) |
//! | `correlation_key` | string | — | Tenant-scoped correlation key (required) |
//! | `join` | `any` \| `all` \| `count` | `all` | Join policy |
//! | `count` | integer | — | Required with `join: "count"` |
//!
//! Timeout behavior comes from `wait_for_input.timeout` (+ optional
//! `escalation_handler`) — the same machinery as human approvals.

use serde_json::{Value, json};
use tracing::debug;

use orch8_types::error::StepError;
use orch8_types::event_correlation::WaitStatus;

use super::StepContext;
use crate::event_correlation::register_wait;

pub async fn handle_wait_for_event(ctx: StepContext) -> Result<Value, StepError> {
    let wait = crate::event_correlation::parse_wait_params(
        &ctx.params,
        ctx.tenant_id.as_str(),
        ctx.instance_id.into_uuid(),
        ctx.block_id.as_str(),
    )
    .map_err(|e| StepError::Permanent {
        message: format!("wait_for_event: {e}"),
        details: None,
    })?;
    let event_names = wait.event_names.clone();
    let correlation_key = wait.correlation_key.clone();

    let registered = register_wait(&ctx.storage, wait)
        .await
        .map_err(|e| StepError::Retryable {
            message: format!("wait_for_event: registration failed: {e}"),
            details: None,
        })?;

    debug!(
        instance_id = %ctx.instance_id,
        block_id = %ctx.block_id,
        satisfied = registered.status == WaitStatus::Satisfied,
        "event wait registered"
    );

    // When the join is satisfied (either immediately, or via the gate
    // that just opened), return the matched payloads as the block's
    // output so downstream blocks read them via
    // `{{ outputs.<block>.events.<name>.payload... }}`.
    let mut by_name = serde_json::Map::new();
    let mut all = Vec::new();
    if registered.status == WaitStatus::Satisfied {
        for event_id in &registered.matched_event_ids {
            if let Ok(Some(event)) = ctx.storage.get_event(*event_id).await {
                let entry = json!({
                    "event_name": event.event_name,
                    "producer_event_id": event.producer_event_id,
                    "payload": event.payload,
                });
                by_name.insert(event.event_name.clone(), entry.clone());
                all.push(entry);
            }
        }
    }

    Ok(json!({
        "wait_id": registered.id,
        "events": by_name,
        "all": all,
        "waiting_for": event_names,
        "correlation_key": correlation_key,
        "satisfied": registered.status == WaitStatus::Satisfied,
        "matched": registered.matched_names,
    }))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    use orch8_storage::sqlite::SqliteStorage;
    use orch8_types::context::ExecutionContext;
    use orch8_types::event_correlation::JoinMode;
    use orch8_types::ids::{BlockId, InstanceId, TenantId};

    async fn ctx(params: Value) -> StepContext {
        let storage = Arc::new(SqliteStorage::in_memory().await.unwrap());
        StepContext {
            instance_id: InstanceId::new(),
            tenant_id: TenantId::unchecked("t1"),
            block_id: BlockId::new("await_events"),
            params,
            context: Arc::new(ExecutionContext::default()),
            attempt: 1,
            storage,
            wait_for_input: None,
        }
    }

    #[tokio::test]
    async fn registers_wait_with_all_join_default() {
        let c = ctx(json!({
            "events": ["payment_received", "inventory_reserved"],
            "correlation_key": "order-7",
        }))
        .await;
        let storage = Arc::clone(&c.storage);
        let instance_id = c.instance_id;
        let out = handle_wait_for_event(c).await.unwrap();
        assert_eq!(out["satisfied"], false);
        let wait = storage
            .get_event_wait(instance_id, "await_events")
            .await
            .unwrap()
            .unwrap();
        assert_eq!(wait.join_mode, JoinMode::All);
        assert_eq!(wait.correlation_key, "order-7");
    }

    #[tokio::test]
    async fn single_event_string_is_accepted() {
        let c = ctx(json!({"events": "paid", "correlation_key": "k"})).await;
        let out = handle_wait_for_event(c).await.unwrap();
        assert_eq!(out["waiting_for"], json!(["paid"]));
    }

    #[tokio::test]
    async fn pre_arrived_event_satisfies_immediately() {
        let c = ctx(json!({"events": "paid", "correlation_key": "order-1", "join": "any"})).await;
        let storage = Arc::clone(&c.storage);
        crate::event_correlation::ingest(
            &storage,
            crate::event_correlation::envelope("t1", "paid", "p-1", "order-1", json!({"amt": 9})),
        )
        .await
        .unwrap();
        let out = handle_wait_for_event(c).await.unwrap();
        assert_eq!(out["satisfied"], true);
        assert_eq!(out["matched"], json!(["paid"]));
        assert_eq!(out["events"]["paid"]["payload"]["amt"], 9);
    }

    #[tokio::test]
    async fn missing_params_fail_permanently() {
        for params in [
            json!({}),
            json!({"events": [], "correlation_key": "k"}),
            json!({"events": [1, 2], "correlation_key": "k"}),
            json!({"events": "x", "correlation_key": "  "}),
            json!({"events": "x", "correlation_key": "k", "join": "sometimes"}),
            json!({"events": "x", "correlation_key": "k", "join": "count"}),
            json!({"events": "x", "correlation_key": "k", "join": "count", "count": 0}),
        ] {
            let c = ctx(params.clone()).await;
            let err = handle_wait_for_event(c).await.unwrap_err();
            assert!(
                matches!(err, StepError::Permanent { .. }),
                "{params} should be permanent"
            );
        }
    }

    #[tokio::test]
    async fn count_join_parses() {
        let c = ctx(json!({
            "events": "reading", "correlation_key": "sensor-1",
            "join": "count", "count": 3
        }))
        .await;
        let storage = Arc::clone(&c.storage);
        let instance_id = c.instance_id;
        handle_wait_for_event(c).await.unwrap();
        let wait = storage
            .get_event_wait(instance_id, "await_events")
            .await
            .unwrap()
            .unwrap();
        assert_eq!(wait.join_mode, JoinMode::Count { count: 3 });
    }
}
