use std::sync::Arc;

use orch8_storage::sqlite::SqliteStorage;
use orch8_types::ids::TenantId;
use orch8_types::instance::{InstanceState, TaskInstance};
use orch8_types::sequence::SequenceDefinition;
use tonic::Code;
use uuid::Uuid;

use super::Orch8GrpcService;

fn sequence(tenant: &str) -> SequenceDefinition {
    serde_json::from_value(serde_json::json!({
        "id": Uuid::now_v7(),
        "tenant_id": tenant,
        "namespace": "default",
        "name": "sanitize-sequence",
        "version": 1,
        "deprecated": false,
        "blocks": [{"type": "step", "id": "step-1", "handler": "noop", "params": {}}],
        "created_at": "2026-01-01T00:00:00Z"
    }))
    .unwrap()
}

fn instance(sequence: &SequenceDefinition, tenant: &str) -> TaskInstance {
    serde_json::from_value(serde_json::json!({
        "id": Uuid::now_v7(),
        "sequence_id": sequence.id,
        "tenant_id": tenant,
        "namespace": "default",
        "state": "completed",
        "priority": "Normal",
        "timezone": "UTC",
        "metadata": {},
        "context": {
            "data": {"value": 7},
            "config": {},
            "audit": [],
            "runtime": {"dry_run": true, "dry_run_auto_approve": true}
        },
        "next_fire_at": "2030-01-01T00:00:00Z",
        "created_at": "2026-01-01T00:00:00Z",
        "updated_at": "2026-01-01T00:00:00Z"
    }))
    .unwrap()
}

async fn service(max_context_bytes: u32) -> Orch8GrpcService {
    Orch8GrpcService::with_max_context_bytes(
        Arc::new(SqliteStorage::in_memory().await.unwrap()),
        max_context_bytes,
    )
}

#[tokio::test]
async fn sanitization_resets_every_client_controlled_lifecycle_field() {
    let sequence = sequence("tenant-a");
    let mut instance = instance(&sequence, "tenant-a");
    let before = chrono::Utc::now();

    service(1024 * 1024)
        .await
        .sanitize_new_instance_with_sequence(None, &mut instance, &sequence)
        .unwrap();

    assert_eq!(instance.state, InstanceState::Scheduled);
    assert!(!instance.context.runtime.dry_run);
    assert!(!instance.context.runtime.dry_run_auto_approve);
    assert!(instance.next_fire_at.unwrap() >= before);
}

#[tokio::test]
async fn authenticated_tenant_fills_an_empty_body_tenant() {
    let sequence = sequence("tenant-a");
    let mut instance = instance(&sequence, "tenant-a");
    instance.tenant_id = TenantId::unchecked("");
    let caller = TenantId::unchecked("tenant-a");

    service(1024 * 1024)
        .await
        .sanitize_new_instance_with_sequence(Some(&caller), &mut instance, &sequence)
        .unwrap();

    assert_eq!(instance.tenant_id, caller);
}

#[tokio::test]
async fn authenticated_tenant_cannot_submit_another_tenant_in_the_body() {
    let sequence = sequence("tenant-a");
    let mut instance = instance(&sequence, "tenant-b");
    let caller = TenantId::unchecked("tenant-a");

    let error = service(1024 * 1024)
        .await
        .sanitize_new_instance_with_sequence(Some(&caller), &mut instance, &sequence)
        .unwrap_err();

    assert_eq!(error.code(), Code::PermissionDenied);
}

#[tokio::test]
async fn sequence_from_another_tenant_is_hidden() {
    let sequence = sequence("tenant-b");
    let mut instance = instance(&sequence, "tenant-a");

    let error = service(1024 * 1024)
        .await
        .sanitize_new_instance_with_sequence(None, &mut instance, &sequence)
        .unwrap_err();

    assert_eq!(error.code(), Code::NotFound);
}

#[tokio::test]
async fn configured_context_limit_is_enforced_before_creation() {
    let sequence = sequence("tenant-a");
    let mut instance = instance(&sequence, "tenant-a");
    instance.context.data = serde_json::json!({"large": "x".repeat(256)});

    let error = service(32)
        .await
        .sanitize_new_instance_with_sequence(None, &mut instance, &sequence)
        .unwrap_err();

    assert_eq!(error.code(), Code::InvalidArgument);
}

#[tokio::test]
async fn resource_warmup_dates_reject_invalid_values_without_mutation() {
    use crate::Orch8Service;
    use crate::proto::{AddResourceRequest, CreatePoolRequest, UpdateResourceRequest};
    use orch8_types::pool::{PoolResource, ResourcePool};
    use tonic::Request;

    let service = service(1024 * 1024).await;
    let response = service
        .create_pool(Request::new(CreatePoolRequest {
            tenant_id: "test".into(),
            name: "warmup".into(),
            strategy: "round_robin".into(),
        }))
        .await
        .unwrap()
        .into_inner();
    let pool: ResourcePool = serde_json::from_str(&response.pool_json).unwrap();
    let resource_json = |date: &str| {
        serde_json::json!({
            "resource_key": "sender", "name": "sender", "warmup_start": date,
            "daily_cap": 100, "warmup_days": 10, "warmup_start_cap": 1
        })
        .to_string()
    };
    let err = service
        .add_resource(Request::new(AddResourceRequest {
            pool_id: pool.id.to_string(),
            resource_json: resource_json("2026-02-30"),
        }))
        .await
        .unwrap_err();
    assert_eq!(err.code(), Code::InvalidArgument);
    assert!(
        service
            .storage
            .list_pool_resources(pool.id)
            .await
            .unwrap()
            .is_empty()
    );
    let response = service
        .add_resource(Request::new(AddResourceRequest {
            pool_id: pool.id.to_string(),
            resource_json: resource_json("2026-02-28"),
        }))
        .await
        .unwrap()
        .into_inner();
    let resource: PoolResource = serde_json::from_str(&response.resource_json).unwrap();
    let err = service
        .update_resource(Request::new(UpdateResourceRequest {
            pool_id: pool.id.to_string(),
            resource_id: resource.id.to_string(),
            update_json: serde_json::json!({"name": "changed", "warmup_start": "invalid"})
                .to_string(),
        }))
        .await
        .unwrap_err();
    assert_eq!(err.code(), Code::InvalidArgument);
    let persisted = service
        .storage
        .list_pool_resources(pool.id)
        .await
        .unwrap()
        .remove(0);
    assert_eq!(persisted.warmup_start, resource.warmup_start);
    assert_eq!(persisted.name, resource.name);
}
