//! Tenant storage placement fencing, identical for `SQLite` and Postgres.
//!
//! A placement only moves forward: a write must carry a strictly greater
//! epoch than the stored one, a losing writer leaves the tenant where it was,
//! and invalid placements (non-positive epoch, bad backend id) never reach
//! the database. Epochs fence each tenant independently.
//!
//! ```text
//! DATABASE_URL=postgres://… cargo test -p orch8-storage --test tenant_placement_contract
//! ```

#[macro_use]
mod support;

use chrono::{SubsecRound, Utc};
use orch8_storage::tenant_partition::{TenantPlacementStore, TenantStoragePlacement};
use orch8_types::error::StorageError;
use orch8_types::ids::TenantId;

fn tenant(id: &str) -> TenantId {
    TenantId::new(id).unwrap()
}

fn placement(tenant_id: &str, backend: &str, epoch: i64) -> TenantStoragePlacement {
    TenantStoragePlacement {
        tenant_id: tenant(tenant_id),
        backend_id: backend.into(),
        epoch,
        // Postgres keeps microseconds; truncate so the round trip is exact.
        updated_at: Utc::now().trunc_subsecs(6),
    }
}

async fn current(s: &dyn TenantPlacementStore, tenant_id: &str) -> Option<TenantStoragePlacement> {
    s.get_tenant_placement(&tenant(tenant_id)).await.unwrap()
}

async fn placement_round_trips_every_field(s: &dyn TenantPlacementStore) {
    let written = placement("tenant-a", "shard-a", 7);
    s.advance_tenant_placement(&written).await.unwrap();
    assert_eq!(current(s, "tenant-a").await, Some(written));
    assert_eq!(current(s, "never-placed").await, None);
}

async fn newer_epoch_moves_the_tenant(s: &dyn TenantPlacementStore) {
    s.advance_tenant_placement(&placement("tenant-a", "shard-a", 1))
        .await
        .unwrap();
    s.advance_tenant_placement(&placement("tenant-a", "shard-b", 2))
        .await
        .unwrap();
    let moved = current(s, "tenant-a").await.unwrap();
    assert_eq!((moved.backend_id.as_str(), moved.epoch), ("shard-b", 2));
}

async fn equal_or_older_epoch_is_fenced_and_leaves_the_tenant_in_place(
    s: &dyn TenantPlacementStore,
) {
    s.advance_tenant_placement(&placement("tenant-a", "shard-a", 5))
        .await
        .unwrap();
    for stale_epoch in [5, 3] {
        let result = s
            .advance_tenant_placement(&placement("tenant-a", "shard-b", stale_epoch))
            .await;
        assert!(
            matches!(result, Err(StorageError::Conflict(_))),
            "epoch {stale_epoch}: got {result:?}"
        );
    }
    let kept = current(s, "tenant-a").await.unwrap();
    assert_eq!((kept.backend_id.as_str(), kept.epoch), ("shard-a", 5));
}

async fn invalid_placements_never_reach_the_database(s: &dyn TenantPlacementStore) {
    let zero_epoch = s
        .advance_tenant_placement(&placement("tenant-a", "shard-a", 0))
        .await;
    let bad_backend = s
        .advance_tenant_placement(&placement("tenant-b", "bad id", 1))
        .await;
    assert!(
        matches!(zero_epoch, Err(StorageError::Conflict(_))),
        "got {zero_epoch:?}"
    );
    assert!(
        matches!(bad_backend, Err(StorageError::Unsupported(_))),
        "got {bad_backend:?}"
    );
    assert_eq!(current(s, "tenant-a").await, None);
    assert_eq!(current(s, "tenant-b").await, None);
}

async fn epochs_fence_each_tenant_independently(s: &dyn TenantPlacementStore) {
    s.advance_tenant_placement(&placement("tenant-a", "shard-a", 10))
        .await
        .unwrap();
    s.advance_tenant_placement(&placement("tenant-b", "shard-a", 1))
        .await
        .unwrap();
    assert_eq!(current(s, "tenant-b").await.unwrap().epoch, 1);
}

backend_contract!(
    placement_round_trips_every_field,
    newer_epoch_moves_the_tenant,
    equal_or_older_epoch_is_fenced_and_leaves_the_tenant_in_place,
    invalid_placements_never_reach_the_database,
    epochs_fence_each_tenant_independently,
);
