//! Regression tests for the mobile-server audit fixes (MS-01 .. MS-14) that
//! live in the storage layer, run on every backend via `backend_test!`.

use chrono::{DateTime, Duration, Utc};
use orch8_storage::backend_test;
use orch8_storage::testing::backends::BackendKind;
use orch8_storage::{
    MobileApprovalRequest, MobileCommand, MobileDevice, MobileInstanceStatus, TelemetryEvent,
};
use orch8_types::continuity::{
    RuntimeCapabilities, RuntimeId, RuntimeKind, RuntimeTrustLevel,
};
use orch8_types::error::StorageError;
use orch8_types::ids::TenantId;

fn caps(kind: RuntimeKind, observed_ago_secs: i64, ttl_secs: i64) -> RuntimeCapabilities {
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

// ---------------------------------------------------------------------------
// MS-01: rollback sample size, server receive time
// ---------------------------------------------------------------------------

fn event(seq: &str, ty: &str, client_time: DateTime<Utc>) -> TelemetryEvent {
    TelemetryEvent {
        event_type: ty.into(),
        payload: serde_json::json!({ "sequence_name": seq }).to_string(),
        device_id: "d1".into(),
        os_name: "iOS".into(),
        os_version: "18".into(),
        app_version: "1".into(),
        sdk_version: "1".into(),
        tenant_id: "t1".into(),
        created_at: client_time,
    }
}

backend_test!(
    async fn rollback_policy_has_default_min_samples_and_can_set_it(b) {
        b.storage
            .create_rollback_policy("t1", "seq", 0.5, 300, None, None, None)
            .await
            .unwrap();
        let p = b.storage.get_rollback_policy("t1", "seq").await.unwrap().unwrap();
        assert_eq!(p.min_samples, 20, "conservative default");

        b.storage.set_rollback_min_samples("t1", "seq", 3).await.unwrap();
        let p = b.storage.get_rollback_policy("t1", "seq").await.unwrap().unwrap();
        assert_eq!(p.min_samples, 3);
        let listed = b.storage.list_rollback_policies(Some("t1"), 10).await.unwrap();
        assert_eq!(listed[0].min_samples, 3);
    }
);

backend_test!(
    async fn error_stats_use_server_receive_time_not_client_created_at(b) {
        let now = Utc::now();
        // Client claims these happened days ago / far in the future; they were
        // all RECEIVED just now so they all belong to the trailing window.
        b.storage
            .ingest_telemetry_events_batch(&[
                event("seq", "InstanceCompleted", now - Duration::days(30)),
                event("seq", "InstanceFailed", now + Duration::days(30)),
                event("other", "InstanceFailed", now),
            ])
            .await
            .unwrap();
        let (failed, total) = b.storage.query_error_stats("t1", "seq", 300).await.unwrap();
        assert_eq!((failed, total), (1, 2));
        // Rate is failed / total, never errors / errors.
        let rate = b.storage.query_error_rate("t1", "seq", 300).await.unwrap();
        assert_eq!(rate, Some(0.5));
    }
);

backend_test!(
    async fn one_error_report_alone_has_total_of_one_sample(b) {
        b.storage
            .ingest_telemetry_error(
                "E", "m", None, "d1", "iOS", "18", "1", "1", "t1", None, Some("seq"),
            )
            .await
            .unwrap();
        let (failed, total) = b.storage.query_error_stats("t1", "seq", 300).await.unwrap();
        // The API gates on `total >= min_samples` (default 20), so a lone
        // forged report cannot reach the threshold decision.
        assert_eq!((failed, total), (1, 1));
        assert!(total < 20);
    }
);

// ---------------------------------------------------------------------------
// MS-02 / MS-11: runtime capability lookups and pruning
// ---------------------------------------------------------------------------

backend_test!(
    async fn runtime_capability_is_found_by_primary_key_beyond_any_list_window(b) {
        let tenant = TenantId::unchecked("t1");
        // 1,200 phones observed more recently than the cloud runtime.
        let server = caps(RuntimeKind::Server, 600, 3600);
        b.storage.upsert_runtime_capabilities(&tenant, &server).await.unwrap();
        for _ in 0..1_200 {
            let phone = caps(RuntimeKind::Mobile, 1, 3600);
            b.storage.upsert_runtime_capabilities(&tenant, &phone).await.unwrap();
        }
        let now = Utc::now();
        let window = b.storage.list_runtime_capabilities(&tenant, now, 10_000).await.unwrap();
        assert!(
            window.iter().all(|r| r.runtime_id != server.runtime_id),
            "the legacy list is capped to the 1000 most recently observed"
        );
        let found = b
            .storage
            .get_runtime_capability(&tenant, server.runtime_id, now)
            .await
            .unwrap();
        assert_eq!(found.map(|r| r.runtime_id), Some(server.runtime_id));
    }
);

backend_test!(
    async fn runtime_capability_lookup_is_live_and_tenant_scoped(b) {
        let tenant = TenantId::unchecked("t1");
        let live = caps(RuntimeKind::Edge, 1, 3600);
        let expired = caps(RuntimeKind::Edge, 7200, 60);
        b.storage.upsert_runtime_capabilities(&tenant, &live).await.unwrap();
        b.storage.upsert_runtime_capabilities(&tenant, &expired).await.unwrap();
        let now = Utc::now();
        assert!(b.storage.get_runtime_capability(&tenant, live.runtime_id, now).await.unwrap().is_some());
        assert!(
            b.storage.get_runtime_capability(&tenant, expired.runtime_id, now).await.unwrap().is_none(),
            "expired registration is not live"
        );
        let other = TenantId::unchecked("t2");
        assert!(b.storage.get_runtime_capability(&other, live.runtime_id, now).await.unwrap().is_none());
    }
);

backend_test!(
    async fn kind_filtered_list_caps_each_kind_separately(b) {
        let tenant = TenantId::unchecked("t1");
        let server = caps(RuntimeKind::Server, 900, 3600);
        b.storage.upsert_runtime_capabilities(&tenant, &server).await.unwrap();
        for i in 0..5 {
            b.storage
                .upsert_runtime_capabilities(&tenant, &caps(RuntimeKind::Mobile, i, 3600))
                .await
                .unwrap();
        }
        let now = Utc::now();
        // No kind filter, 2 per kind: the older server is still present.
        let all = b
            .storage
            .list_runtime_capabilities_by_kind(&tenant, now, &[], 2)
            .await
            .unwrap();
        assert_eq!(all.iter().filter(|r| r.kind == RuntimeKind::Mobile).count(), 2);
        assert!(all.iter().any(|r| r.runtime_id == server.runtime_id));
        // Restricted to servers only.
        let servers = b
            .storage
            .list_runtime_capabilities_by_kind(&tenant, now, &[RuntimeKind::Server], 10)
            .await
            .unwrap();
        assert_eq!(servers.len(), 1);
        assert_eq!(servers[0].kind, RuntimeKind::Server);
    }
);

backend_test!(
    async fn expired_runtime_capabilities_are_pruned(b) {
        let tenant = TenantId::unchecked("t1");
        let old = caps(RuntimeKind::Mobile, 3 * 86_400, 60);
        let live = caps(RuntimeKind::Mobile, 1, 3600);
        b.storage.upsert_runtime_capabilities(&tenant, &old).await.unwrap();
        b.storage.upsert_runtime_capabilities(&tenant, &live).await.unwrap();
        let cutoff = Utc::now() - Duration::days(1);
        assert_eq!(b.storage.prune_expired_runtime_capabilities(cutoff, 100).await.unwrap(), 1);
        assert_eq!(b.storage.prune_expired_runtime_capabilities(cutoff, 100).await.unwrap(), 0);
        assert!(
            b.storage
                .get_runtime_capability(&tenant, live.runtime_id, Utc::now())
                .await
                .unwrap()
                .is_some()
        );
    }
);

// ---------------------------------------------------------------------------
// MS-03 / MS-08: wake claim shape, per-device collapse
// ---------------------------------------------------------------------------

async fn register(b: &orch8_storage::testing::backends::TestBackend, device: &str, tenant: &str) {
    b.storage
        .register_mobile_device(&MobileDevice {
            device_id: device.into(),
            tenant_id: tenant.into(),
            push_token: Some(format!("tok-{device}")),
            platform: "ios".into(),
            app_version: None,
            active: true,
            last_sync_at: None,
            registered_at: String::new(),
        })
        .await
        .unwrap();
}

fn command(id: &str, device: &str) -> MobileCommand {
    MobileCommand {
        id: id.into(),
        device_id: device.into(),
        command_type: "sync".into(),
        payload: "{}".into(),
        created_at: String::new(),
        acked_at: None,
    }
}

backend_test!(
    async fn expired_in_flight_leases_and_pending_wakes_are_both_claimed(b) {
        register(&b, "dev", "t1").await;
        let now = Utc::now();
        let a = b.storage.enqueue_wake("t1", "dev", "c-pending", now - Duration::seconds(5)).await.unwrap();
        let leased = b.storage.enqueue_wake("t1", "dev", "c-leased", now - Duration::seconds(10)).await.unwrap();
        // Lease `c-leased` for 1 s, then let it expire.
        let first = b
            .storage
            .claim_due_wakes(now - Duration::seconds(5), now - Duration::seconds(4), 1)
            .await
            .unwrap();
        assert_eq!(first.len(), 1);
        assert_eq!(first[0].id, leased);
        // Later: the expired lease (in_flight branch) and the still-pending
        // row (pending branch) are both due.
        let later = b.storage.claim_due_wakes(now, now + Duration::seconds(30), 10).await.unwrap();
        let ids: Vec<_> = later.iter().map(|w| w.id).collect();
        assert!(ids.contains(&leased) && ids.contains(&a), "{ids:?}");
        assert_eq!(later.len(), 2);
    }
);

backend_test!(
    async fn claim_is_batch_capable_and_never_returns_a_row_twice(b) {
        register(&b, "dev", "t1").await;
        let now = Utc::now();
        for i in 0..7 {
            b.storage
                .enqueue_wake("t1", "dev", &format!("c{i}"), now - Duration::seconds(100 - i))
                .await
                .unwrap();
        }
        let first = b.storage.claim_due_wakes(now, now + Duration::seconds(30), 4).await.unwrap();
        let second = b.storage.claim_due_wakes(now, now + Duration::seconds(30), 4).await.unwrap();
        assert_eq!(first.len(), 4);
        assert_eq!(second.len(), 3);
        let mut ids: Vec<_> = first.iter().chain(second.iter()).map(|w| w.id).collect();
        ids.sort();
        ids.dedup();
        assert_eq!(ids.len(), 7);
    }
);

backend_test!(
    kinds = [postgres, encrypted_postgres],
    async fn claim_interleaves_tenants_so_a_deep_backlog_cannot_starve_others(b) {
        register(&b, "dev-a", "ta").await;
        register(&b, "dev-b", "tb").await;
        let now = Utc::now();
        for i in 0..10 {
            b.storage
                .enqueue_wake("ta", "dev-a", &format!("a{i}"), now - Duration::seconds(1_000 - i))
                .await
                .unwrap();
        }
        // The only tb wake is newer than every ta wake.
        let tb = b.storage.enqueue_wake("tb", "dev-b", "b0", now - Duration::seconds(1)).await.unwrap();
        let claimed = b.storage.claim_due_wakes(now, now + Duration::seconds(30), 2).await.unwrap();
        assert_eq!(claimed.len(), 2);
        assert!(
            claimed.iter().any(|w| w.id == tb),
            "tenant tb must get a slot in the first batch despite ta's older backlog"
        );
    }
);

backend_test!(
    async fn commands_for_one_device_collapse_to_one_pending_wake(b) {
        register(&b, "dev", "t1").await;
        let now = Utc::now();
        let mut last = uuid::Uuid::nil();
        for i in 0..5 {
            last = b
                .storage
                .create_mobile_command_with_wake(&command(&format!("c{i}"), "dev"), "t1", now + Duration::milliseconds(i))
                .await
                .unwrap();
        }
        // All five commands remain deliverable...
        assert_eq!(b.storage.fetch_pending_commands("dev", 50).await.unwrap().len(), 5);
        // ...but only the newest wake is pending.
        let wakes = b
            .storage
            .claim_due_wakes(now + Duration::seconds(1), now + Duration::seconds(30), 50)
            .await
            .unwrap();
        assert_eq!(wakes.len(), 1, "one wake per device, not one per command");
        assert_eq!(wakes[0].id, last);

        // A different device is unaffected.
        register(&b, "dev2", "t1").await;
        b.storage
            .create_mobile_command_with_wake(&command("x", "dev2"), "t1", now)
            .await
            .unwrap();
        let wakes = b
            .storage
            .claim_due_wakes(now + Duration::seconds(1), now + Duration::seconds(30), 50)
            .await
            .unwrap();
        assert_eq!(wakes.len(), 1);
        assert_eq!(wakes[0].device_id, "dev2");
    }
);

// ---------------------------------------------------------------------------
// MS-04 / MS-10: status batch (typed errors, monotonic, clamp, one statement)
// ---------------------------------------------------------------------------

fn status(device: &str, instance: &str, state: &str, at: &str) -> MobileInstanceStatus {
    MobileInstanceStatus {
        device_id: device.into(),
        instance_id: instance.into(),
        sequence_name: Some("seq".into()),
        state: state.into(),
        current_step: None,
        handler: None,
        context_summary: None,
        steps: None,
        updated_at: at.into(),
    }
}

backend_test!(
    async fn malformed_status_timestamp_is_a_typed_constraint_error_and_writes_nothing(b) {
        let good = status("d", "i-good", "running", "2026-01-01T00:00:00Z");
        let bad = status("d", "i-bad", "running", "not-a-timestamp");
        let err = b
            .storage
            .upsert_mobile_instance_status_batch(&[good, bad.clone()])
            .await
            .unwrap_err();
        assert!(matches!(err, StorageError::Constraint(_)), "{err:?}");
        assert!(b.storage.list_mobile_instance_status(None, Some("d"), 10).await.unwrap().is_empty());
        let err = b.storage.upsert_mobile_instance_status(&bad).await.unwrap_err();
        assert!(matches!(err, StorageError::Constraint(_)), "{err:?}");
    }
);

backend_test!(
    async fn delayed_status_retry_cannot_overwrite_newer_state_or_revive_terminal(b) {
        let newer = status("d", "i", "completed", "2026-03-01T10:00:00Z");
        b.storage.upsert_mobile_instance_status_batch(&[newer]).await.unwrap();
        // Delayed retry of an older "running" update arrives afterwards.
        let stale = status("d", "i", "running", "2026-03-01T09:00:00Z");
        b.storage.upsert_mobile_instance_status_batch(std::slice::from_ref(&stale)).await.unwrap();
        b.storage.upsert_mobile_instance_status(&stale).await.unwrap();
        let rows = b.storage.list_mobile_instance_status(None, Some("d"), 10).await.unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].state, "completed", "terminal state must not be revived");
        // An equal-or-newer update still applies.
        let fresh = status("d", "i", "completed", "2026-03-01T11:00:00Z");
        b.storage.upsert_mobile_instance_status_batch(&[fresh]).await.unwrap();
    }
);

backend_test!(
    async fn duplicate_keys_in_one_batch_collapse_to_the_newest(b) {
        b.storage
            .upsert_mobile_instance_status_batch(&[
                status("d", "i", "running", "2026-03-01T10:00:00Z"),
                status("d", "i", "completed", "2026-03-01T10:00:05Z"),
                status("d", "i", "paused", "2026-03-01T10:00:02Z"),
                status("d", "j", "running", "2026-03-01T10:00:00Z"),
            ])
            .await
            .unwrap();
        let rows = b.storage.list_mobile_instance_status(None, Some("d"), 10).await.unwrap();
        assert_eq!(rows.len(), 2);
        assert_eq!(rows.iter().find(|r| r.instance_id == "i").unwrap().state, "completed");
    }
);

backend_test!(
    async fn future_status_timestamps_are_clamped_to_the_server_clock(b) {
        let future = (Utc::now() + Duration::days(365)).to_rfc3339();
        b.storage
            .upsert_mobile_instance_status_batch(&[status("d", "i", "running", &future)])
            .await
            .unwrap();
        let rows = b.storage.list_mobile_instance_status(None, Some("d"), 10).await.unwrap();
        let stored = chrono::DateTime::parse_from_rfc3339(&rows[0].updated_at).unwrap();
        assert!(
            stored.with_timezone(&Utc) <= Utc::now() + Duration::seconds(5),
            "skewed clock must not sort first forever: {}",
            rows[0].updated_at
        );
        // A real update now (older than the skewed claim) is not blocked.
        let real = status("d", "i", "completed", &Utc::now().to_rfc3339());
        b.storage.upsert_mobile_instance_status_batch(&[real]).await.unwrap();
        let rows = b.storage.list_mobile_instance_status(None, Some("d"), 10).await.unwrap();
        assert_eq!(rows[0].state, "completed");
    }
);

backend_test!(
    async fn five_hundred_status_rows_and_approvals_batch_in_one_call(b) {
        let statuses: Vec<_> = (0..500)
            .map(|i| status("d", &format!("i{i}"), "running", "2026-03-01T10:00:00Z"))
            .collect();
        b.storage.upsert_mobile_instance_status_batch(&statuses).await.unwrap();
        assert_eq!(b.storage.list_mobile_instance_status(None, Some("d"), 1000).await.unwrap().len(), 500);

        let approvals: Vec<_> = (0..500)
            .map(|i| MobileApprovalRequest {
                id: uuid::Uuid::new_v4().to_string(),
                device_id: "d".into(),
                tenant_id: "t1".into(),
                instance_id: format!("i{}", i % 250),
                block_id: format!("b{}", i / 250),
                sequence_name: None,
                prompt: Some("ok?".into()),
                choices: None,
                store_as: None,
                timeout_secs: Some(60),
                metadata: None,
                state: "pending".into(),
                resolution: None,
                created_at: String::new(),
                resolved_at: None,
            })
            .collect();
        // 500 distinct (instance, block) pairs inserted, replay inserts none.
        assert_eq!(b.storage.insert_mobile_approvals_batch(&approvals).await.unwrap(), 500);
        assert_eq!(b.storage.insert_mobile_approvals_batch(&approvals).await.unwrap(), 0);
    }
);

// ---------------------------------------------------------------------------
// MS-14: idempotent deterministic command insert
// ---------------------------------------------------------------------------

backend_test!(
    async fn create_command_if_absent_is_idempotent(b) {
        let cmd = command("fixed-id", "dev");
        assert!(b.storage.create_mobile_command_if_absent(&cmd).await.unwrap());
        assert!(!b.storage.create_mobile_command_if_absent(&cmd).await.unwrap());
        assert_eq!(b.storage.fetch_pending_commands("dev", 10).await.unwrap().len(), 1);
    }
);

// ---------------------------------------------------------------------------
// MS-04: one undecryptable command must not poison the listing
// ---------------------------------------------------------------------------

backend_test!(
    kinds = [encrypted_sqlite, encrypted_postgres],
    async fn undecryptable_command_payload_is_marked_and_does_not_fail_the_listing(b) {
        let mut good = command("good", "dev");
        good.payload = r#"{"ok":true}"#.into();
        let mut bad = command("bad", "dev");
        bad.payload = r#"{"ok":false}"#.into();
        b.storage.create_mobile_command(&good).await.unwrap();
        b.storage.create_mobile_command(&bad).await.unwrap();
        // Corrupt the stored ciphertext of one row behind the wrapper's back.
        let corrupt = "UPDATE mobile_commands SET payload = 'enc:v1:garbage' WHERE id = 'bad'";
        match b.kind {
            BackendKind::EncryptedSqlite => {
                sqlx::query(corrupt).execute(b.sqlite_pool()).await.unwrap();
            }
            _ => {
                sqlx::query(corrupt).execute(b.pg_pool()).await.unwrap();
            }
        }
        let listed = b.storage.fetch_pending_commands("dev", 10).await.unwrap();
        assert_eq!(listed.len(), 2, "the corrupt row must not fail the whole listing");
        let by_id = |id: &str| listed.iter().find(|c| c.id == id).unwrap();
        assert_eq!(by_id("good").payload, r#"{"ok":true}"#);
        assert_eq!(by_id("bad").payload, orch8_storage::UNREADABLE_COMMAND_PAYLOAD);
    }
);

// ---------------------------------------------------------------------------
// MS-06: ineligible rows must not hide eligible ones in the claim scan window
// ---------------------------------------------------------------------------

fn worker_task(
    instance: orch8_types::ids::InstanceId,
    block: &str,
    created_at: DateTime<Utc>,
    requirements: orch8_types::continuity::CapsuleRequirements,
) -> orch8_types::worker::WorkerTask {
    orch8_types::worker::WorkerTask {
        id: uuid::Uuid::now_v7(),
        instance_id: instance,
        block_id: orch8_types::ids::BlockId::new(block),
        handler_name: "h".into(),
        queue_name: None,
        requirements,
        params: serde_json::json!({}),
        context: serde_json::json!({}),
        attempt: 0,
        timeout_ms: None,
        state: orch8_types::worker::WorkerTaskState::Pending,
        worker_id: None,
        claimed_at: None,
        heartbeat_at: None,
        claim_epoch: 0,
        resume_checkpoint: None,
        checkpoint_seq: 0,
        completed_at: None,
        output: None,
        error_message: None,
        error_retryable: None,
        created_at,
        effect_id: None,
        continuity_epoch: None,
        lease_secs: None,
        carries_credentials: false,
        claimed_runtime_kind: None,
    }
}

backend_test!(
    kinds = [sqlite, postgres],
    async fn phone_claim_finds_eligible_work_behind_a_wall_of_unclaimable_older_tasks(b) {
        use orch8_storage::testing::fixtures::{InstanceBuilder, SequenceBuilder};
        let seq = SequenceBuilder::new("t1").step("s", "h").build();
        b.storage.create_sequence(&seq).await.unwrap();
        let instance = InstanceBuilder::for_sequence(&seq).due_now().build();
        b.storage.create_instance(&instance).await.unwrap();

        let base = Utc::now() - Duration::hours(2);
        // More older tasks than the claim scan window (4096) that a phone can
        // never take: they need a label phones do not advertise.
        let mut labelled = orch8_types::continuity::CapsuleRequirements::default();
        labelled.labels.insert("gpu".into(), "a100".into());
        for i in 0..4_200 {
            let task = worker_task(
                instance.id,
                &format!("blocked-{i}"),
                base + Duration::milliseconds(i),
                labelled.clone(),
            );
            b.storage.create_worker_task(&task).await.unwrap();
        }
        // One newer task with no requirements -- the only one a phone can claim.
        let eligible = worker_task(
            instance.id,
            "eligible",
            Utc::now(),
            orch8_types::continuity::CapsuleRequirements::default(),
        );
        b.storage.create_worker_task(&eligible).await.unwrap();

        let mut phone = caps(RuntimeKind::Mobile, 0, 3600);
        phone.handlers = vec!["h".into()];
        let tenant = TenantId::unchecked("t1");
        let claimed = b
            .storage
            .claim_worker_tasks_matching("h", "phone-1", Some(&tenant), None, &phone, 5)
            .await
            .unwrap();
        assert_eq!(
            claimed.iter().map(|t| t.id).collect::<Vec<_>>(),
            vec![eligible.id],
            "{}: the eligible task must not be hidden by 4,200 older ineligible rows",
            b.name()
        );
    }
);

// kept to document that the postgres-only interleave test is gated on kind
#[test]
fn backend_kind_names_are_stable() {
    assert_eq!(BackendKind::Postgres.name(), "postgres");
}
