//! MS-02: placement must see cloud/desktop runtimes even in a tenant with
//! thousands of live phones, and the GC must prune dead registrations.

use chrono::{Duration, Utc};

use orch8_engine::gc::{GcOptions, RUNTIME_CAPABILITY_RETENTION, gc_tick};
use orch8_engine::placement::candidate_runtimes;
use orch8_types::continuity::{
    CapsuleRequirements, RuntimeCapabilities, RuntimeId, RuntimeKind, RuntimeTrustLevel,
};
use orch8_types::ids::TenantId;

mod common;

fn runtime(kind: RuntimeKind, observed_ago_secs: i64, ttl_secs: i64) -> RuntimeCapabilities {
    let observed_at = Utc::now() - Duration::seconds(observed_ago_secs);
    RuntimeCapabilities {
        runtime_id: RuntimeId::new(),
        kind,
        trust: RuntimeTrustLevel::Registered,
        handlers: Vec::new(),
        plugins: Vec::new(),
        credentials: Vec::new(),
        regions: Vec::new(),
        hardware: Vec::new(),
        offline_capable: false,
        connectivity: None,
        battery_percent: None,
        estimated_cost_microunits: None,
        estimated_latency_ms: None,
        draining: false,
        capsule_signing_public_key: None,
        labels: std::collections::BTreeMap::new(),
        observed_at,
        expires_at: observed_at + Duration::seconds(ttl_secs),
    }
}

#[tokio::test]
async fn placement_candidates_include_cloud_and_desktop_among_thousands_of_newer_phones() {
    let storage = common::storage().await;
    let tenant = TenantId::unchecked("fleet");
    let cloud = runtime(RuntimeKind::Server, 900, 3600);
    let desktop = runtime(RuntimeKind::Desktop, 900, 3600);
    storage.upsert_runtime_capabilities(&tenant, &cloud).await.unwrap();
    storage.upsert_runtime_capabilities(&tenant, &desktop).await.unwrap();
    for _ in 0..1_100 {
        storage
            .upsert_runtime_capabilities(&tenant, &runtime(RuntimeKind::Mobile, 1, 3600))
            .await
            .unwrap();
    }
    let now = Utc::now();

    // The legacy listing is capped at the 1000 most recently observed: it
    // loses both servers (this is the bug).
    let listed = storage
        .list_runtime_capabilities(&tenant, now, 10_000)
        .await
        .unwrap();
    assert!(listed.iter().all(|r| r.kind == RuntimeKind::Mobile));

    // Placement with no kind constraint still sees them, ahead of the phones
    // (so the MAX_CANDIDATES bound inside choose_runtime drops phones first).
    let requirements = CapsuleRequirements::default();
    let candidates = candidate_runtimes(storage.as_ref(), &tenant, &requirements, &[], now)
        .await
        .unwrap();
    let ids: Vec<_> = candidates.iter().map(|r| r.runtime_id).collect();
    assert!(ids.contains(&cloud.runtime_id) && ids.contains(&desktop.runtime_id));
    assert_ne!(candidates[0].kind, RuntimeKind::Mobile, "phones sort last");
    assert_ne!(candidates[1].kind, RuntimeKind::Mobile);

    // A kind constraint is applied in storage.
    let only_desktop = CapsuleRequirements {
        runtime_kinds: vec![RuntimeKind::Desktop],
        ..CapsuleRequirements::default()
    };
    let candidates = candidate_runtimes(storage.as_ref(), &tenant, &only_desktop, &[], now)
        .await
        .unwrap();
    assert_eq!(candidates.len(), 1);
    assert_eq!(candidates[0].runtime_id, desktop.runtime_id);

    // A targeted runtime is a primary-key lookup, whatever the kind filter.
    let targeted = CapsuleRequirements {
        runtime_id: Some(cloud.runtime_id),
        ..CapsuleRequirements::default()
    };
    let candidates = candidate_runtimes(storage.as_ref(), &tenant, &targeted, &[], now)
        .await
        .unwrap();
    assert_eq!(candidates.len(), 1);
    assert_eq!(candidates[0].runtime_id, cloud.runtime_id);

    // Extra ids (e.g. the current owner) are always included, once.
    let with_owner = candidate_runtimes(
        storage.as_ref(),
        &tenant,
        &only_desktop,
        &[cloud.runtime_id, desktop.runtime_id],
        now,
    )
    .await
    .unwrap();
    assert_eq!(with_owner.len(), 2);
}

#[tokio::test]
async fn gc_prunes_registrations_expired_beyond_the_retention_but_keeps_recent_ones() {
    let storage = common::storage().await;
    let tenant = TenantId::unchecked("fleet");
    let long_dead = runtime(RuntimeKind::Mobile, 10 * 86_400, 60);
    let just_expired = runtime(RuntimeKind::Mobile, 600, 60);
    let live = runtime(RuntimeKind::Mobile, 1, 3600);
    for r in [&long_dead, &just_expired, &live] {
        storage.upsert_runtime_capabilities(&tenant, r).await.unwrap();
    }

    gc_tick(storage.as_ref(), &GcOptions::default(), Utc::now()).await;

    let far_past = Utc::now() - Duration::days(30);
    let remaining = |id: RuntimeId| {
        let storage = storage.clone();
        let tenant = tenant.clone();
        async move {
            // `observed_after` far in the past also returns expired rows
            // that are still stored (expires_at > far_past).
            storage
                .get_runtime_capability(&tenant, id, far_past)
                .await
                .unwrap()
                .is_some()
        }
    };
    assert!(!remaining(long_dead.runtime_id).await, "pruned");
    assert!(remaining(just_expired.runtime_id).await, "inside retention");
    assert!(remaining(live.runtime_id).await);
    assert_eq!(RUNTIME_CAPABILITY_RETENTION.as_secs(), 86_400);
}
