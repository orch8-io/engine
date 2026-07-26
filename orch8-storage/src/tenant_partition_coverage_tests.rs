//! Coverage tests for authoritative tenant-to-storage placement routing.
//!
//! Count contract: 56 independently named unit tests.

use super::*;
use std::sync::Mutex;

// ---------------------------------------------------------------------------
// In-memory placement store: pure stand-in for the SQL control plane, so the
// router can be exercised without a database. Optional error injection pins
// the fail-closed contract (store errors propagate, never fall back).
// ---------------------------------------------------------------------------

#[derive(Default)]
struct FakePlacementStore {
    placements: Mutex<HashMap<String, TenantStoragePlacement>>,
    fail_get: bool,
}

impl FakePlacementStore {
    fn seed(&self, placement: TenantStoragePlacement) {
        self.placements
            .lock()
            .unwrap()
            .insert(placement.tenant_id.to_string(), placement);
    }
}

#[async_trait]
impl TenantPlacementStore for FakePlacementStore {
    async fn get_tenant_placement(
        &self,
        tenant_id: &TenantId,
    ) -> Result<Option<TenantStoragePlacement>, StorageError> {
        if self.fail_get {
            return Err(StorageError::Connection("control plane unreachable".into()));
        }
        Ok(self
            .placements
            .lock()
            .unwrap()
            .get(tenant_id.as_str())
            .cloned())
    }

    async fn advance_tenant_placement(
        &self,
        placement: &TenantStoragePlacement,
    ) -> Result<(), StorageError> {
        validate_placement(placement)?;
        let mut guard = self.placements.lock().unwrap();
        match guard.entry(placement.tenant_id.to_string()) {
            Entry::Vacant(entry) => {
                entry.insert(placement.clone());
                Ok(())
            }
            Entry::Occupied(mut entry) => {
                if placement.epoch > entry.get().epoch {
                    entry.insert(placement.clone());
                    Ok(())
                } else {
                    Err(StorageError::Conflict("stale epoch".into()))
                }
            }
        }
    }
}

fn placement(tenant: &str, backend: &str, epoch: i64) -> TenantStoragePlacement {
    TenantStoragePlacement {
        tenant_id: TenantId::new(tenant).unwrap(),
        backend_id: backend.into(),
        epoch,
        updated_at: Utc::now(),
    }
}

async fn backend() -> Arc<dyn StorageBackend> {
    Arc::new(SqliteStorage::in_memory().await.unwrap())
}

// ---------------------------------------------------------------------------
// validate_backend_id boundary matrix
// ---------------------------------------------------------------------------

macro_rules! backend_id_case {
    ($name:ident, $value:expr, $valid:expr) => {
        #[test]
        fn $name() {
            assert_eq!(validate_backend_id(&$value).is_ok(), $valid);
        }
    };
}

backend_id_case!(coverage_partition_001_empty_backend_id_rejected, "", false);
backend_id_case!(coverage_partition_002_single_letter_allowed, "a", true);
backend_id_case!(coverage_partition_003_single_digit_allowed, "9", true);
backend_id_case!(coverage_partition_004_uppercase_allowed, "SHARD", true);
backend_id_case!(coverage_partition_005_hyphen_allowed, "shard-eu-1", true);
backend_id_case!(
    coverage_partition_006_underscore_allowed,
    "shard_eu_1",
    true
);
backend_id_case!(coverage_partition_007_dot_allowed, "shard.eu.1", true);
backend_id_case!(
    coverage_partition_008_mixed_alphabet_allowed,
    "A9-b_c.d",
    true
);
backend_id_case!(
    coverage_partition_009_128_bytes_allowed,
    "a".repeat(128),
    true
);
backend_id_case!(
    coverage_partition_010_129_bytes_rejected,
    "a".repeat(129),
    false
);
backend_id_case!(coverage_partition_011_space_rejected, "shard a", false);
backend_id_case!(coverage_partition_012_slash_rejected, "shard/a", false);
backend_id_case!(coverage_partition_013_colon_rejected, "shard:a", false);
backend_id_case!(
    coverage_partition_014_unicode_letter_rejected,
    "shard-é",
    false
);
backend_id_case!(coverage_partition_015_emoji_rejected, "shard-🚀", false);
backend_id_case!(coverage_partition_016_newline_rejected, "shard\na", false);
backend_id_case!(coverage_partition_017_tab_rejected, "shard\ta", false);
backend_id_case!(coverage_partition_018_plus_rejected, "shard+a", false);
backend_id_case!(coverage_partition_019_at_sign_rejected, "shard@a", false);
backend_id_case!(coverage_partition_020_leading_dot_allowed, ".hidden", true);
backend_id_case!(coverage_partition_021_only_dots_allowed, "...", true);
backend_id_case!(
    coverage_partition_022_trailing_whitespace_rejected,
    "shard ",
    false
);

#[test]
fn coverage_partition_023_invalid_backend_id_error_is_unsupported() {
    assert!(matches!(
        validate_backend_id("bad id"),
        Err(StorageError::Unsupported(_))
    ));
}

#[test]
fn coverage_partition_024_invalid_backend_id_error_documents_alphabet() {
    let Err(StorageError::Unsupported(message)) = validate_backend_id("") else {
        panic!("empty backend id must be rejected");
    };
    assert!(message.contains("1-128"));
}

// ---------------------------------------------------------------------------
// validate_placement fencing preconditions
// ---------------------------------------------------------------------------

#[test]
fn coverage_partition_025_zero_epoch_rejected_as_conflict() {
    assert!(matches!(
        validate_placement(&placement("tenant-a", "shard-a", 0)),
        Err(StorageError::Conflict(_))
    ));
}

#[test]
fn coverage_partition_026_negative_epoch_rejected_as_conflict() {
    assert!(matches!(
        validate_placement(&placement("tenant-a", "shard-a", -7)),
        Err(StorageError::Conflict(_))
    ));
}

#[test]
fn coverage_partition_027_epoch_one_allowed() {
    assert!(validate_placement(&placement("tenant-a", "shard-a", 1)).is_ok());
}

#[test]
fn coverage_partition_028_max_epoch_allowed() {
    assert!(validate_placement(&placement("tenant-a", "shard-a", i64::MAX)).is_ok());
}

#[test]
fn coverage_partition_029_invalid_backend_id_in_placement_rejected() {
    assert!(matches!(
        validate_placement(&placement("tenant-a", "bad backend", 1)),
        Err(StorageError::Unsupported(_))
    ));
}

#[test]
fn coverage_partition_030_valid_placement_accepted() {
    assert!(validate_placement(&placement("tenant-a", "shard-a", 3)).is_ok());
}

// ---------------------------------------------------------------------------
// Router registration and fail-closed routing (pure, fake control plane)
// ---------------------------------------------------------------------------

#[tokio::test]
async fn coverage_partition_031_register_valid_backend_succeeds() {
    let store: Arc<dyn TenantPlacementStore> = Arc::new(FakePlacementStore::default());
    let mut router = TenantPartitionRouter::new(store);
    assert!(router.register_backend("shard-a", backend().await).is_ok());
}

#[tokio::test]
async fn coverage_partition_032_duplicate_registration_conflicts() {
    let store: Arc<dyn TenantPlacementStore> = Arc::new(FakePlacementStore::default());
    let mut router = TenantPartitionRouter::new(store);
    router.register_backend("shard-a", backend().await).unwrap();
    assert!(matches!(
        router.register_backend("shard-a", backend().await),
        Err(StorageError::Conflict(_))
    ));
}

#[tokio::test]
async fn coverage_partition_033_duplicate_registration_error_names_backend() {
    let store: Arc<dyn TenantPlacementStore> = Arc::new(FakePlacementStore::default());
    let mut router = TenantPartitionRouter::new(store);
    router.register_backend("shard-a", backend().await).unwrap();
    let Err(StorageError::Conflict(message)) = router.register_backend("shard-a", backend().await)
    else {
        panic!("duplicate registration must conflict");
    };
    assert!(message.contains("shard-a"));
}

#[tokio::test]
async fn coverage_partition_034_invalid_backend_id_not_registered() {
    let store = Arc::new(FakePlacementStore::default());
    store.seed(placement("tenant-a", "bad id", 1));
    let placement_store: Arc<dyn TenantPlacementStore> = store;
    let mut router = TenantPartitionRouter::new(placement_store);
    assert!(matches!(
        router.register_backend("bad id", backend().await),
        Err(StorageError::Unsupported(_))
    ));
    // The failed registration must not leave a routable entry behind.
    assert!(matches!(
        router.route(&TenantId::new("tenant-a").unwrap()).await,
        Err(StorageError::Unsupported(_))
    ));
}

#[tokio::test]
async fn coverage_partition_035_unplaced_tenant_fails_closed_not_found() {
    let store: Arc<dyn TenantPlacementStore> = Arc::new(FakePlacementStore::default());
    let router = TenantPartitionRouter::new(store);
    assert!(matches!(
        router.route(&TenantId::new("ghost").unwrap()).await,
        Err(StorageError::NotFound { .. })
    ));
}

#[tokio::test]
async fn coverage_partition_036_not_found_error_carries_tenant_id() {
    let store: Arc<dyn TenantPlacementStore> = Arc::new(FakePlacementStore::default());
    let router = TenantPartitionRouter::new(store);
    let Err(StorageError::NotFound { entity, id }) =
        router.route(&TenantId::new("tenant-xyz").unwrap()).await
    else {
        panic!("unplaced tenant must be NotFound");
    };
    assert_eq!(entity, "tenant storage placement");
    assert_eq!(id, "tenant-xyz");
}

#[tokio::test]
async fn coverage_partition_037_placement_pointing_at_unregistered_backend_fails_closed() {
    let store = Arc::new(FakePlacementStore::default());
    store.seed(placement("tenant-a", "missing-shard", 1));
    let placement_store: Arc<dyn TenantPlacementStore> = store;
    let mut router = TenantPartitionRouter::new(placement_store);
    router
        .register_backend("other-shard", backend().await)
        .unwrap();
    assert!(matches!(
        router.route(&TenantId::new("tenant-a").unwrap()).await,
        Err(StorageError::Unsupported(_))
    ));
}

#[tokio::test]
async fn coverage_partition_038_route_returns_placement_verbatim() {
    let store = Arc::new(FakePlacementStore::default());
    let seeded = placement("tenant-a", "shard-a", 41);
    store.seed(seeded.clone());
    let placement_store: Arc<dyn TenantPlacementStore> = store;
    let mut router = TenantPartitionRouter::new(placement_store);
    router.register_backend("shard-a", backend().await).unwrap();
    let routed = router
        .route(&TenantId::new("tenant-a").unwrap())
        .await
        .unwrap();
    assert_eq!(routed.placement, seeded);
}

#[tokio::test]
async fn coverage_partition_039_route_returns_registered_backend_arc() {
    let store = Arc::new(FakePlacementStore::default());
    store.seed(placement("tenant-a", "shard-a", 1));
    let placement_store: Arc<dyn TenantPlacementStore> = store;
    let mut router = TenantPartitionRouter::new(placement_store);
    let shard = backend().await;
    router
        .register_backend("shard-a", Arc::clone(&shard))
        .unwrap();
    let routed = router
        .route(&TenantId::new("tenant-a").unwrap())
        .await
        .unwrap();
    assert!(Arc::ptr_eq(&routed.backend, &shard));
}

#[tokio::test]
async fn coverage_partition_040_tenants_route_to_distinct_backends() {
    let store = Arc::new(FakePlacementStore::default());
    store.seed(placement("tenant-a", "shard-a", 1));
    store.seed(placement("tenant-b", "shard-b", 1));
    let placement_store: Arc<dyn TenantPlacementStore> = store;
    let mut router = TenantPartitionRouter::new(placement_store);
    let shard_a = backend().await;
    let shard_b = backend().await;
    router
        .register_backend("shard-a", Arc::clone(&shard_a))
        .unwrap();
    router
        .register_backend("shard-b", Arc::clone(&shard_b))
        .unwrap();
    let routed_a = router
        .route(&TenantId::new("tenant-a").unwrap())
        .await
        .unwrap();
    let routed_b = router
        .route(&TenantId::new("tenant-b").unwrap())
        .await
        .unwrap();
    assert!(Arc::ptr_eq(&routed_a.backend, &shard_a));
    assert!(Arc::ptr_eq(&routed_b.backend, &shard_b));
}

#[tokio::test]
async fn coverage_partition_041_reroute_follows_newer_placement() {
    let store = Arc::new(FakePlacementStore::default());
    store.seed(placement("tenant-a", "shard-a", 1));
    let placement_store: Arc<dyn TenantPlacementStore> = store.clone();
    let mut router = TenantPartitionRouter::new(placement_store);
    let shard_a = backend().await;
    let shard_b = backend().await;
    router
        .register_backend("shard-a", Arc::clone(&shard_a))
        .unwrap();
    router
        .register_backend("shard-b", Arc::clone(&shard_b))
        .unwrap();
    let tenant = TenantId::new("tenant-a").unwrap();
    assert!(Arc::ptr_eq(
        &router.route(&tenant).await.unwrap().backend,
        &shard_a
    ));
    store
        .advance_tenant_placement(&placement("tenant-a", "shard-b", 2))
        .await
        .unwrap();
    let rerouted = router.route(&tenant).await.unwrap();
    assert!(Arc::ptr_eq(&rerouted.backend, &shard_b));
    assert_eq!(rerouted.placement.epoch, 2);
}

#[tokio::test]
async fn coverage_partition_042_control_plane_error_propagates() {
    let store = Arc::new(FakePlacementStore {
        fail_get: true,
        ..FakePlacementStore::default()
    });
    let placement_store: Arc<dyn TenantPlacementStore> = store;
    let router = TenantPartitionRouter::new(placement_store);
    assert!(matches!(
        router.route(&TenantId::new("tenant-a").unwrap()).await,
        Err(StorageError::Connection(_))
    ));
}

#[tokio::test]
async fn coverage_partition_043_router_without_backends_never_routes() {
    let store = Arc::new(FakePlacementStore::default());
    store.seed(placement("tenant-a", "shard-a", 1));
    let placement_store: Arc<dyn TenantPlacementStore> = store;
    let router = TenantPartitionRouter::new(placement_store);
    assert!(matches!(
        router.route(&TenantId::new("tenant-a").unwrap()).await,
        Err(StorageError::Unsupported(_))
    ));
}

#[tokio::test]
async fn coverage_partition_044_many_backends_registered_independently() {
    let store = Arc::new(FakePlacementStore::default());
    for index in 0..5 {
        store.seed(placement(
            &format!("tenant-{index}"),
            &format!("shard-{index}"),
            1,
        ));
    }
    let placement_store: Arc<dyn TenantPlacementStore> = store;
    let mut router = TenantPartitionRouter::new(placement_store);
    for index in 0..5 {
        router
            .register_backend(format!("shard-{index}"), backend().await)
            .unwrap();
    }
    for index in 0..5 {
        let routed = router
            .route(&TenantId::new(format!("tenant-{index}")).unwrap())
            .await
            .unwrap();
        assert_eq!(routed.placement.backend_id, format!("shard-{index}"));
    }
}

// ---------------------------------------------------------------------------
// Sqlite-backed placement store: epoch fencing and row round-trips against a
// real (in-memory) database — no Postgres required.
// ---------------------------------------------------------------------------

async fn control() -> Arc<SqliteStorage> {
    Arc::new(SqliteStorage::in_memory().await.unwrap())
}

#[tokio::test]
async fn coverage_partition_045_advance_then_get_round_trips_all_fields() {
    let store = control().await;
    let written = placement("tenant-a", "shard-a", 7);
    store.advance_tenant_placement(&written).await.unwrap();
    let read = store
        .get_tenant_placement(&TenantId::new("tenant-a").unwrap())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(read.tenant_id, written.tenant_id);
    assert_eq!(read.backend_id, "shard-a");
    assert_eq!(read.epoch, 7);
    // SQLite stores RFC 3339 text; the round trip must stay within a second.
    assert!(
        (read.updated_at - written.updated_at).num_seconds().abs() <= 1,
        "updated_at drifted: {} vs {}",
        read.updated_at,
        written.updated_at
    );
}

#[tokio::test]
async fn coverage_partition_046_get_unplaced_tenant_returns_none() {
    let store = control().await;
    assert!(
        store
            .get_tenant_placement(&TenantId::new("never-placed").unwrap())
            .await
            .unwrap()
            .is_none()
    );
}

#[tokio::test]
async fn coverage_partition_047_equal_epoch_advance_conflicts() {
    let store = control().await;
    store
        .advance_tenant_placement(&placement("tenant-a", "shard-a", 5))
        .await
        .unwrap();
    assert!(matches!(
        store
            .advance_tenant_placement(&placement("tenant-a", "shard-b", 5))
            .await,
        Err(StorageError::Conflict(_))
    ));
    // The losing writer must not have moved the tenant.
    let read = store
        .get_tenant_placement(&TenantId::new("tenant-a").unwrap())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(read.backend_id, "shard-a");
}

#[tokio::test]
async fn coverage_partition_048_older_epoch_advance_conflicts() {
    let store = control().await;
    store
        .advance_tenant_placement(&placement("tenant-a", "shard-a", 10))
        .await
        .unwrap();
    assert!(matches!(
        store
            .advance_tenant_placement(&placement("tenant-a", "shard-b", 3))
            .await,
        Err(StorageError::Conflict(_))
    ));
}

#[tokio::test]
async fn coverage_partition_049_newer_epoch_advance_moves_tenant() {
    let store = control().await;
    store
        .advance_tenant_placement(&placement("tenant-a", "shard-a", 1))
        .await
        .unwrap();
    store
        .advance_tenant_placement(&placement("tenant-a", "shard-b", 2))
        .await
        .unwrap();
    let read = store
        .get_tenant_placement(&TenantId::new("tenant-a").unwrap())
        .await
        .unwrap()
        .unwrap();
    assert_eq!((read.backend_id.as_str(), read.epoch), ("shard-b", 2));
}

#[tokio::test]
async fn coverage_partition_050_zero_epoch_never_reaches_database() {
    let store = control().await;
    assert!(matches!(
        store
            .advance_tenant_placement(&placement("tenant-a", "shard-a", 0))
            .await,
        Err(StorageError::Conflict(_))
    ));
    assert!(
        store
            .get_tenant_placement(&TenantId::new("tenant-a").unwrap())
            .await
            .unwrap()
            .is_none()
    );
}

#[tokio::test]
async fn coverage_partition_051_invalid_backend_id_never_reaches_database() {
    let store = control().await;
    assert!(matches!(
        store
            .advance_tenant_placement(&placement("tenant-a", "bad id", 1))
            .await,
        Err(StorageError::Unsupported(_))
    ));
    assert!(
        store
            .get_tenant_placement(&TenantId::new("tenant-a").unwrap())
            .await
            .unwrap()
            .is_none()
    );
}

#[tokio::test]
async fn coverage_partition_052_tenant_epochs_fence_independently() {
    let store = control().await;
    store
        .advance_tenant_placement(&placement("tenant-a", "shard-a", 10))
        .await
        .unwrap();
    // tenant-b starting at epoch 1 is not fenced by tenant-a's higher epoch.
    store
        .advance_tenant_placement(&placement("tenant-b", "shard-a", 1))
        .await
        .unwrap();
    let read = store
        .get_tenant_placement(&TenantId::new("tenant-b").unwrap())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(read.epoch, 1);
}

#[tokio::test]
async fn coverage_partition_053_unicode_tenant_id_round_trips() {
    let store = control().await;
    store
        .advance_tenant_placement(&placement("テナント-α", "shard-a", 1))
        .await
        .unwrap();
    let read = store
        .get_tenant_placement(&TenantId::new("テナント-α").unwrap())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(read.tenant_id.as_str(), "テナント-α");
}

#[tokio::test]
async fn coverage_partition_054_long_tenant_id_round_trips() {
    let store = control().await;
    let tenant = "t".repeat(512);
    store
        .advance_tenant_placement(&placement(&tenant, "shard-a", 1))
        .await
        .unwrap();
    let read = store
        .get_tenant_placement(&TenantId::new(&tenant).unwrap())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(read.tenant_id.as_str(), tenant);
}

#[tokio::test]
async fn coverage_partition_055_sqlite_store_routes_end_to_end() {
    let store = control().await;
    store
        .advance_tenant_placement(&placement("tenant-a", "shard-a", 1))
        .await
        .unwrap();
    let placement_store: Arc<dyn TenantPlacementStore> = store;
    let mut router = TenantPartitionRouter::new(placement_store);
    let shard = backend().await;
    router
        .register_backend("shard-a", Arc::clone(&shard))
        .unwrap();
    let routed = router
        .route(&TenantId::new("tenant-a").unwrap())
        .await
        .unwrap();
    assert!(Arc::ptr_eq(&routed.backend, &shard));
    assert_eq!(routed.placement.epoch, 1);
}

#[tokio::test]
async fn coverage_partition_056_unregistered_backend_error_names_tenant_and_backend() {
    let store = Arc::new(FakePlacementStore::default());
    store.seed(placement("tenant-a", "missing-shard", 1));
    let placement_store: Arc<dyn TenantPlacementStore> = store;
    let mut router = TenantPartitionRouter::new(placement_store);
    router
        .register_backend("other-shard", backend().await)
        .unwrap();
    let Err(StorageError::Unsupported(message)) =
        router.route(&TenantId::new("tenant-a").unwrap()).await
    else {
        panic!("unregistered backend must fail closed");
    };
    assert!(
        message.contains("tenant-a"),
        "error must name the tenant: {message}"
    );
    assert!(
        message.contains("missing-shard"),
        "error must name the unregistered backend: {message}"
    );
}
