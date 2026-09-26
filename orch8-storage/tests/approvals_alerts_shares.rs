//! Storage contract for approval action tokens, progress shares, and alert
//! rules. Runs against `SQLite` always and Postgres when `DATABASE_URL` is set.

use chrono::{Duration, Utc};
use uuid::Uuid;

use orch8_storage::StorageBackend;
use orch8_storage::postgres::PostgresStorage;
use orch8_storage::sqlite::SqliteStorage;
use orch8_types::alert::{AlertCondition, AlertDestination, AlertRule, AlertRuleState};
use orch8_types::approval_link::{ApprovalActionToken, ApprovalChannel, hash_token};
use orch8_types::ids::{BlockId, InstanceId, TenantId};
use orch8_types::progress_share::ProgressShare;

async fn backends() -> Vec<(&'static str, Box<dyn StorageBackend>)> {
    let mut out: Vec<(&'static str, Box<dyn StorageBackend>)> = vec![(
        "sqlite",
        Box::new(SqliteStorage::in_memory().await.unwrap()),
    )];
    if let Ok(url) = std::env::var("DATABASE_URL") {
        let pg = PostgresStorage::new(&url, 5, None).await.expect("connect");
        pg.run_migrations().await.expect("migrate");
        out.push(("postgres", Box::new(pg)));
    } else {
        eprintln!("postgres leg skipped: DATABASE_URL not set");
    }
    out
}

fn token(
    tenant: &TenantId,
    instance: InstanceId,
    block: &str,
    choice: &str,
    channel: ApprovalChannel,
    ttl: Duration,
) -> (String, ApprovalActionToken) {
    let raw = format!("{}{}", Uuid::new_v4().simple(), Uuid::new_v4().simple());
    let now = Utc::now();
    (
        raw.clone(),
        ApprovalActionToken {
            token_hash: hash_token(&raw),
            tenant_id: tenant.clone(),
            instance_id: instance,
            block_id: BlockId::new(block),
            choice: choice.into(),
            channel,
            recipient: Some("ops@example.com".into()),
            verify_secret_ref: None,
            created_at: now,
            expires_at: now + ttl,
            used_at: None,
        },
    )
}

#[tokio::test]
async fn approval_tokens_are_single_use_per_gate_and_expire() {
    for (name, s) in backends().await {
        let tenant = TenantId::unchecked(format!("appr-{}", Uuid::new_v4()));
        let instance = InstanceId::new();
        let (raw_yes, yes) = token(
            &tenant,
            instance,
            "gate",
            "approve",
            ApprovalChannel::Email,
            Duration::hours(1),
        );
        let (raw_no, no) = token(
            &tenant,
            instance,
            "gate",
            "reject",
            ApprovalChannel::Slack,
            Duration::hours(1),
        );
        let (raw_old, old) = token(
            &tenant,
            instance,
            "other",
            "approve",
            ApprovalChannel::Email,
            Duration::seconds(-5),
        );
        s.create_approval_tokens(&[yes.clone(), no, old])
            .await
            .unwrap();

        let now = Utc::now();
        assert!(
            s.has_live_approval_tokens(
                instance,
                &BlockId::new("gate"),
                ApprovalChannel::Email,
                now
            )
            .await
            .unwrap(),
            "{name}"
        );
        assert!(
            !s.has_live_approval_tokens(
                instance,
                &BlockId::new("gate"),
                ApprovalChannel::Teams,
                now
            )
            .await
            .unwrap(),
            "{name}"
        );
        assert_eq!(
            s.get_approval_token(&hash_token(&raw_yes))
                .await
                .unwrap()
                .unwrap()
                .choice,
            "approve"
        );

        let won = s
            .consume_approval_token(&hash_token(&raw_yes), now)
            .await
            .unwrap();
        assert_eq!(won.map(|t| t.choice), Some("approve".into()), "{name}");
        // Same token again, and the sibling on another channel, are burned.
        assert!(
            s.consume_approval_token(&hash_token(&raw_yes), now)
                .await
                .unwrap()
                .is_none(),
            "{name}"
        );
        assert!(
            s.consume_approval_token(&hash_token(&raw_no), now)
                .await
                .unwrap()
                .is_none(),
            "{name}"
        );
        assert!(
            !s.has_live_approval_tokens(
                instance,
                &BlockId::new("gate"),
                ApprovalChannel::Slack,
                now
            )
            .await
            .unwrap(),
            "{name}"
        );
        // Expired tokens never consume.
        assert!(
            s.consume_approval_token(&hash_token(&raw_old), now)
                .await
                .unwrap()
                .is_none(),
            "{name}"
        );
        assert!(
            s.consume_approval_token(&hash_token("unknown"), now)
                .await
                .unwrap()
                .is_none(),
            "{name}"
        );
    }
}

#[tokio::test]
async fn progress_shares_are_tenant_scoped_and_revocable() {
    for (name, s) in backends().await {
        let tenant = TenantId::unchecked(format!("share-{}", Uuid::new_v4()));
        let other = TenantId::unchecked(format!("other-{}", Uuid::new_v4()));
        let instance = InstanceId::new();
        let now = Utc::now();
        let share = ProgressShare {
            id: Uuid::now_v7(),
            token_hash: hash_token(&format!("tok-{}", Uuid::new_v4())),
            tenant_id: tenant.clone(),
            instance_id: instance,
            allowed_fields: vec!["order_id".into()],
            created_at: now,
            expires_at: now + Duration::days(1),
            revoked_at: None,
        };
        s.create_progress_share(&share).await.unwrap();
        let got = s
            .get_progress_share_by_hash(&share.token_hash)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(got.allowed_fields, vec!["order_id".to_string()], "{name}");
        assert_eq!(got.instance_id, instance);
        assert_eq!(
            s.list_progress_shares(&tenant, instance)
                .await
                .unwrap()
                .len(),
            1,
            "{name}"
        );
        assert!(
            s.list_progress_shares(&other, instance)
                .await
                .unwrap()
                .is_empty(),
            "{name}"
        );
        // Wrong tenant cannot revoke.
        assert!(
            !s.revoke_progress_share(&other, instance, share.id, now)
                .await
                .unwrap(),
            "{name}"
        );
        assert!(
            s.revoke_progress_share(&tenant, instance, share.id, now)
                .await
                .unwrap(),
            "{name}"
        );
        assert!(
            !s.revoke_progress_share(&tenant, instance, share.id, now)
                .await
                .unwrap(),
            "{name}"
        );
        let revoked = s
            .get_progress_share_by_hash(&share.token_hash)
            .await
            .unwrap()
            .unwrap();
        assert!(!revoked.is_live(Utc::now()), "{name}");
    }
}

#[tokio::test]
async fn alert_rules_crud_and_state_cas() {
    for (name, s) in backends().await {
        let tenant = TenantId::unchecked(format!("alert-{}", Uuid::new_v4()));
        let other = TenantId::unchecked(format!("other-{}", Uuid::new_v4()));
        let now = Utc::now();
        let mut rule = AlertRule {
            id: Uuid::now_v7(),
            tenant_id: tenant.clone(),
            name: "dlq".into(),
            enabled: true,
            condition: AlertCondition::DlqGrowth {
                threshold: Some(10),
                growth: None,
                window_secs: 300,
            },
            destination: AlertDestination::Pagerduty {
                routing_key_ref: "credentials://pd".into(),
                severity: "critical".into(),
            },
            cooldown_secs: 600,
            created_at: now,
            updated_at: now,
        };
        s.create_alert_rule(&rule).await.unwrap();
        assert_eq!(
            s.get_alert_rule(Some(&tenant), rule.id)
                .await
                .unwrap()
                .unwrap(),
            rule,
            "{name}"
        );
        assert!(
            s.get_alert_rule(Some(&other), rule.id)
                .await
                .unwrap()
                .is_none(),
            "{name}"
        );
        assert_eq!(
            s.list_alert_rules(Some(&tenant), 10).await.unwrap().len(),
            1,
            "{name}"
        );

        rule.enabled = false;
        rule.name = "renamed".into();
        assert!(s.update_alert_rule(&rule).await.unwrap(), "{name}");
        let mut foreign = rule.clone();
        foreign.tenant_id = other.clone();
        assert!(!s.update_alert_rule(&foreign).await.unwrap(), "{name}");
        assert_eq!(
            s.get_alert_rule(None, rule.id).await.unwrap().unwrap().name,
            "renamed"
        );

        // State CAS: insert once, then version-guarded updates.
        assert!(s.get_alert_rule_state(rule.id).await.unwrap().is_none());
        let mut st = AlertRuleState::new(rule.id);
        st.version = 1;
        st.firing = true;
        assert!(s.cas_alert_rule_state(&st, 0).await.unwrap(), "{name}");
        assert!(
            !s.cas_alert_rule_state(&st, 0).await.unwrap(),
            "{name} duplicate insert"
        );
        let mut next = s.get_alert_rule_state(rule.id).await.unwrap().unwrap();
        assert_eq!(next.version, 1);
        assert!(next.firing);
        next.version = 2;
        next.firing = false;
        assert!(s.cas_alert_rule_state(&next, 1).await.unwrap(), "{name}");
        assert!(
            !s.cas_alert_rule_state(&next, 1).await.unwrap(),
            "{name} stale version"
        );

        assert!(
            !s.delete_alert_rule(&other, rule.id).await.unwrap(),
            "{name}"
        );
        assert!(
            s.delete_alert_rule(&tenant, rule.id).await.unwrap(),
            "{name}"
        );
        assert!(
            s.get_alert_rule_state(rule.id).await.unwrap().is_none(),
            "{name}"
        );
    }
}
