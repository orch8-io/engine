//! L2-09: lease decisions use the database clock, not the caller's.
//!
//! Each check plays two nodes with different wall clocks against one
//! Postgres. The node whose clock runs ahead must not be able to steal a
//! lease that is still live on the database clock. (`SQLite` is a
//! single-process store with one clock, so only the Postgres legs apply.)

use chrono::{Duration, Utc};
use serde_json::json;
use uuid::Uuid;

use orch8_storage::backend_test;
use orch8_storage::testing::fixtures::SequenceBuilder;
use orch8_types::cron::CronSchedule;
use orch8_types::ids::{Namespace, TenantId};
use orch8_types::trigger::{TriggerDef, TriggerType};

async fn trigger(b: &orch8_storage::testing::backends::TestBackend) -> String {
    let slug = format!("skew-{}", Uuid::new_v4());
    let now = Utc::now();
    b.storage
        .create_trigger(&TriggerDef {
            slug: slug.clone(),
            sequence_name: "seq".into(),
            version: None,
            tenant_id: TenantId::unchecked("t_skew"),
            namespace: "default".into(),
            enabled: true,
            secret: None,
            trigger_type: TriggerType::ActivepiecesPoll,
            config: json!({"piece": "stripe", "trigger": "t"}),
            created_at: now,
            updated_at: now,
        })
        .await
        .unwrap();
    slug
}

backend_test!(
    kinds = [postgres, encrypted_postgres],
    async fn fast_clock_node_cannot_steal_trigger_poll_lease(b) {
        let slug = trigger(&b).await;
        let now = Utc::now();
        assert!(
            b.storage
                .try_acquire_trigger_poll_lease(&slug, "node-a", now, now + Duration::seconds(30))
                .await
                .unwrap()
        );
        // Node B's clock is 10 minutes ahead: by *its* clock A's lease has
        // long expired. On the database clock it has not.
        let skewed = now + Duration::minutes(10);
        assert!(
            !b.storage
                .try_acquire_trigger_poll_lease(
                    &slug,
                    "node-b",
                    skewed,
                    skewed + Duration::seconds(30)
                )
                .await
                .unwrap(),
            "a fast-clock node must not steal a live lease"
        );
        // The owner can renew.
        assert!(
            b.storage
                .try_acquire_trigger_poll_lease(&slug, "node-a", now, now + Duration::seconds(30))
                .await
                .unwrap()
        );
    }
);

backend_test!(
    kinds = [postgres, encrypted_postgres],
    async fn slow_clock_node_cannot_hold_a_lease_past_real_expiry(b) {
        let slug = trigger(&b).await;
        // Node A's clock is an hour behind but it asks for a 1s lease.
        let slow = Utc::now() - Duration::hours(1);
        assert!(
            b.storage
                .try_acquire_trigger_poll_lease(&slug, "node-a", slow, slow + Duration::seconds(1))
                .await
                .unwrap()
        );
        tokio::time::sleep(std::time::Duration::from_millis(1_500)).await;
        // Correct-clock node B finds the lease expired on the database clock
        // (previously it looked an hour in the future and B was locked out).
        let now = Utc::now();
        assert!(
            b.storage
                .try_acquire_trigger_poll_lease(&slug, "node-b", now, now + Duration::seconds(30))
                .await
                .unwrap()
        );
    }
);

backend_test!(
    kinds = [postgres, encrypted_postgres],
    async fn fast_clock_node_cannot_reclaim_a_live_cron_claim(b) {
        let seq = SequenceBuilder::new("t_cron").step("s", "noop").build();
        b.storage.create_sequence(&seq).await.unwrap();
        let now = Utc::now();
        let schedule = CronSchedule {
            id: Uuid::now_v7(),
            tenant_id: TenantId::unchecked("t_cron"),
            namespace: Namespace::new("default"),
            sequence_id: seq.id,
            cron_expr: "0 * * * * * *".into(),
            timezone: "UTC".into(),
            enabled: true,
            metadata: json!({}),
            overlap_policy: orch8_types::cron::OverlapPolicy::default(),
            skipped_fires: 0,
            last_skipped_at: None,
            last_triggered_at: None,
            next_fire_at: Some(now - Duration::seconds(5)),
            created_at: now,
            updated_at: now,
        };
        b.storage.create_cron_schedule(&schedule).await.unwrap();
        assert_eq!(b.storage.claim_due_cron_schedules(now).await.unwrap().len(), 1);
        // 10 minutes ahead is past the 5 minute claim lease by the caller's
        // clock, but the database clock says the claim is live.
        let skewed = now + Duration::minutes(10);
        assert!(
            b.storage
                .claim_due_cron_schedules(skewed)
                .await
                .unwrap()
                .is_empty(),
            "a fast-clock node must not double-claim a schedule"
        );
    }
);

backend_test!(
    async fn retired_cron_claim_cannot_change_an_edited_schedule(b) {
        let seq = SequenceBuilder::new("t_cron").step("s", "noop").build();
        b.storage.create_sequence(&seq).await.unwrap();
        let now = Utc::now();
        let schedule = CronSchedule {
            id: Uuid::now_v7(),
            tenant_id: TenantId::unchecked("t_cron"),
            namespace: Namespace::new("default"),
            sequence_id: seq.id,
            cron_expr: "0 * * * * * *".into(),
            timezone: "UTC".into(),
            enabled: true,
            metadata: json!({}),
            overlap_policy: orch8_types::cron::OverlapPolicy::default(),
            skipped_fires: 0,
            last_skipped_at: None,
            last_triggered_at: None,
            next_fire_at: Some(now - Duration::seconds(5)),
            created_at: now,
            updated_at: now,
        };
        b.storage.create_cron_schedule(&schedule).await.unwrap();
        let first = b.storage.claim_due_cron_schedules(now).await.unwrap().pop().unwrap();
        let mut changed = first.clone();
        changed.cron_expr = "30 * * * * * *".into();
        let revised_time = now - Duration::seconds(1);
        changed.next_fire_at = Some(revised_time);
        b.storage.update_cron_schedule(&changed).await.unwrap();

        let current = b.storage.claim_due_cron_schedules(now).await.unwrap().pop().unwrap();
        assert!(!b.storage.advance_claimed_cron_schedule(&first, now, now + Duration::hours(1), true).await.unwrap());
        let stored = b.storage.get_cron_schedule(schedule.id).await.unwrap().unwrap();
        // PostgreSQL truncates timestamps to microseconds. Use the observed
        // occurrence as the comparison baseline across the backend matrix.
        assert_eq!(stored.next_fire_at, current.next_fire_at);
        assert_eq!(stored.cron_expr, changed.cron_expr);
        assert_eq!(stored.skipped_fires, 0);
        assert!(b.storage.advance_claimed_cron_schedule(&current, now, now + Duration::minutes(1), true).await.unwrap());
        assert!(!b.storage.advance_claimed_cron_schedule(&current, now, now + Duration::hours(1), true).await.unwrap());
        let stored = b.storage.get_cron_schedule(schedule.id).await.unwrap().unwrap();
        assert_eq!(stored.skipped_fires, 1);
        assert!(stored.next_fire_at.unwrap() > now);
    }
);
