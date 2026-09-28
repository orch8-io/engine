//! `TenancyStore` + sub-tenant columns (migration 097) on `SQLite` and, when
//! `DATABASE_URL` is set, on Postgres. Every test uses a unique tenant so
//! runs can share one database.

use std::sync::Arc;

use chrono::{Duration, Utc};
use uuid::Uuid;

use orch8_storage::StorageBackend;
use orch8_storage::postgres::PostgresStorage;
use orch8_storage::sqlite::SqliteStorage;
use orch8_types::context::ExecutionContext;
use orch8_types::error::StorageError;
use orch8_types::filter::{InstanceFilter, Pagination};
use orch8_types::ids::{InstanceId, Namespace, SequenceId, TenantId};
use orch8_types::instance::{InstanceState, Priority, TaskInstance};
use orch8_types::release::{ReleaseState, ReleaseTarget, WorkflowRelease};
use orch8_types::sequence::{SequenceDefinition, SequenceStatus};
use orch8_types::sub_tenant::{
    EmbedTheme, SUB_TENANT_QUOTA_PREFIX, SequenceEmbed, SubTenantLimits,
};

async fn backends() -> Vec<(&'static str, Arc<dyn StorageBackend>)> {
    let mut out: Vec<(&'static str, Arc<dyn StorageBackend>)> = vec![(
        "sqlite",
        Arc::new(SqliteStorage::in_memory().await.unwrap()),
    )];
    if let Ok(url) = std::env::var("DATABASE_URL") {
        let pg = PostgresStorage::new(&url, 5, None).await.unwrap();
        pg.run_migrations().await.unwrap();
        out.push(("postgres", Arc::new(pg)));
    }
    out
}

fn tenant() -> TenantId {
    TenantId::unchecked(format!("st-{}", Uuid::now_v7().simple()))
}

async fn sequence(
    storage: &dyn StorageBackend,
    tenant: &TenantId,
    sub: Option<&str>,
) -> SequenceDefinition {
    let seq = SequenceDefinition {
        placement: None,
        sub_tenant: sub.map(ToString::to_string),
        embed: Some(SequenceEmbed {
            visible_outputs: vec!["summary".into()],
            ..SequenceEmbed::default()
        }),
        schema: None,
        schema_version: orch8_types::sequence::SEQUENCE_SCHEMA_VERSION,
        id: SequenceId::new(),
        tenant_id: tenant.clone(),
        namespace: Namespace::new("default"),
        name: format!("seq-{}", Uuid::now_v7().simple()),
        version: 1,
        deprecated: false,
        status: SequenceStatus::default(),
        blocks: Vec::new(),
        interceptors: None,
        input_schema: None,
        sla: None,
        on_failure: None,
        on_cancel: None,
        created_at: Utc::now(),
    };
    storage.create_sequence(&seq).await.unwrap();
    seq
}

fn instance(seq: &SequenceDefinition, sub: Option<&str>) -> TaskInstance {
    let now = Utc::now();
    TaskInstance {
        id: InstanceId::new(),
        sub_tenant: sub.map(ToString::to_string),
        sequence_id: seq.id,
        tenant_id: seq.tenant_id.clone(),
        namespace: seq.namespace.clone(),
        state: InstanceState::Scheduled,
        next_fire_at: None,
        priority: Priority::Normal,
        timezone: "UTC".into(),
        metadata: serde_json::json!({}),
        context: ExecutionContext::default(),
        concurrency_key: None,
        max_concurrency: None,
        idempotency_key: None,
        session_id: None,
        parent_instance_id: None,
        budget: None,
        created_at: now,
        updated_at: now,
    }
}

fn is_sub_quota(err: &StorageError) -> bool {
    matches!(err, StorageError::QuotaExceeded(m) if m.starts_with(SUB_TENANT_QUOTA_PREFIX))
}

#[tokio::test]
async fn sub_tenant_round_trips_on_instances_and_sequences_and_filters() {
    for (name, storage) in backends().await {
        let t = tenant();
        let seq = sequence(storage.as_ref(), &t, Some("acme")).await;
        let fetched = storage.get_sequence(seq.id).await.unwrap().unwrap();
        assert_eq!(fetched.sub_tenant.as_deref(), Some("acme"), "{name}");
        assert_eq!(
            fetched.embed.unwrap().visible_outputs,
            vec!["summary".to_string()],
            "{name}"
        );

        let scoped = instance(&seq, Some("acme"));
        let other = instance(&seq, Some("globex"));
        let plain = instance(&seq, None);
        storage
            .create_sub_tenant_instances_admitted(std::slice::from_ref(&scoped), 100, Utc::now())
            .await
            .unwrap();
        storage
            .create_sub_tenant_instances_admitted(std::slice::from_ref(&other), 100, Utc::now())
            .await
            .unwrap();
        storage.create_instance(&plain).await.unwrap();

        let got = storage.get_instance(scoped.id).await.unwrap().unwrap();
        assert_eq!(got.sub_tenant.as_deref(), Some("acme"), "{name}");
        let got = storage.get_instance(plain.id).await.unwrap().unwrap();
        assert_eq!(got.sub_tenant, None, "{name}");

        let filter = InstanceFilter {
            tenant_id: Some(t.clone()),
            sub_tenant: Some("acme".into()),
            ..InstanceFilter::default()
        };
        let listed = storage
            .list_instances(&filter, &Pagination::default())
            .await
            .unwrap();
        assert_eq!(listed.len(), 1, "{name}");
        assert_eq!(listed[0].id, scoped.id, "{name}");
        assert_eq!(storage.count_instances(&filter).await.unwrap(), 1, "{name}");
        let all = InstanceFilter {
            tenant_id: Some(t.clone()),
            ..InstanceFilter::default()
        };
        assert_eq!(storage.count_instances(&all).await.unwrap(), 3, "{name}");
    }
}

#[tokio::test]
async fn caps_and_tenant_pool_are_enforced_atomically() {
    for (name, storage) in backends().await {
        let t = tenant();
        let seq = sequence(storage.as_ref(), &t, None).await;
        let now = Utc::now();

        // Concurrent cap.
        storage
            .put_sub_tenant_limits(
                &t,
                "acme",
                &SubTenantLimits {
                    max_executions_per_month: None,
                    max_concurrent: Some(1),
                },
            )
            .await
            .unwrap();
        storage
            .create_sub_tenant_instances_admitted(&[instance(&seq, Some("acme"))], 100, now)
            .await
            .unwrap();
        let err = storage
            .create_sub_tenant_instances_admitted(&[instance(&seq, Some("acme"))], 100, now)
            .await
            .unwrap_err();
        assert!(is_sub_quota(&err), "{name}: {err:?}");

        // Monthly cap counts the ledger, not live instances.
        storage
            .put_sub_tenant_limits(
                &t,
                "globex",
                &SubTenantLimits {
                    max_executions_per_month: Some(2),
                    max_concurrent: None,
                },
            )
            .await
            .unwrap();
        let batch = [
            instance(&seq, Some("globex")),
            instance(&seq, Some("globex")),
        ];
        assert_eq!(
            storage
                .create_sub_tenant_instances_admitted(&batch, 100, now)
                .await
                .unwrap(),
            2,
            "{name}"
        );
        let err = storage
            .create_sub_tenant_instances_admitted(&[instance(&seq, Some("globex"))], 100, now)
            .await
            .unwrap_err();
        assert!(is_sub_quota(&err), "{name}: {err:?}");
        // Next month the monthly budget is fresh again.
        storage
            .create_sub_tenant_instances_admitted(
                &[instance(&seq, Some("globex"))],
                100,
                now + Duration::days(40),
            )
            .await
            .unwrap();

        // Tenant pool: 4 active instances exist; a pool of 4 rejects more
        // with the plan error (not the sub-tenant code).
        let err = storage
            .create_sub_tenant_instances_admitted(&[instance(&seq, Some("initech"))], 4, now)
            .await
            .unwrap_err();
        assert!(
            matches!(&err, StorageError::QuotaExceeded(m) if !m.starts_with(SUB_TENANT_QUOTA_PREFIX)),
            "{name}: {err:?}"
        );

        // A rejected admission writes nothing.
        let initech = InstanceFilter {
            tenant_id: Some(t.clone()),
            sub_tenant: Some("initech".into()),
            ..InstanceFilter::default()
        };
        assert_eq!(
            storage.count_instances(&initech).await.unwrap(),
            0,
            "{name}"
        );

        // Mixed scopes are refused outright.
        let mixed = [instance(&seq, Some("a")), instance(&seq, Some("b"))];
        assert!(
            storage
                .create_sub_tenant_instances_admitted(&mixed, 100, now)
                .await
                .is_err(),
            "{name}"
        );

        let limits = storage.get_sub_tenant_limits(&t, "acme").await.unwrap();
        assert_eq!(limits.unwrap().max_concurrent, Some(1), "{name}");
        assert_eq!(
            storage.get_sub_tenant_limits(&t, "nobody").await.unwrap(),
            None,
            "{name}"
        );
    }
}

#[tokio::test]
async fn usage_comes_from_the_ledger_and_live_rows() {
    for (name, storage) in backends().await {
        let t = tenant();
        let seq = sequence(storage.as_ref(), &t, None).await;
        let now = Utc::now();
        let a1 = instance(&seq, Some("acme"));
        let a2 = instance(&seq, Some("acme"));
        storage
            .create_sub_tenant_instances_admitted(&[a1.clone(), a2.clone()], 100, now)
            .await
            .unwrap();
        storage
            .create_sub_tenant_instances_admitted(&[instance(&seq, Some("globex"))], 100, now)
            .await
            .unwrap();
        storage
            .update_instance_state(a1.id, InstanceState::Completed, None)
            .await
            .unwrap();
        storage
            .save_block_output(&orch8_types::output::BlockOutput {
                id: Uuid::now_v7(),
                instance_id: a1.id,
                block_id: orch8_types::ids::BlockId::new("summary".to_string()),
                output: serde_json::json!({"ok": true}),
                output_ref: None,
                output_size: 11,
                attempt: 0,
                created_at: Utc::now(),
            })
            .await
            .unwrap();

        let from = now - Duration::minutes(5);
        let to = Utc::now() + Duration::minutes(5);
        let usage = storage.sub_tenant_usage(&t, from, to).await.unwrap();
        assert_eq!(usage.len(), 2, "{name}: {usage:?}");
        let acme = usage.iter().find(|u| u.sub_tenant == "acme").unwrap();
        assert_eq!(acme.executions_started, 2, "{name}");
        assert_eq!(acme.executions_completed, 1, "{name}");
        assert_eq!(acme.steps_executed, 1, "{name}");
        assert!(acme.last_active_at.is_some(), "{name}");

        // Pruning the instance rows does not change started executions.
        let later = storage
            .sub_tenant_usage(&t, now + Duration::hours(1), now + Duration::hours(2))
            .await
            .unwrap();
        assert!(later.is_empty(), "{name}");

        assert!(
            storage.count_active_sub_tenants(from).await.unwrap() >= 2,
            "{name}"
        );
    }
}

#[tokio::test]
async fn embed_theme_and_release_target_persist() {
    for (name, storage) in backends().await {
        let t = tenant();
        assert_eq!(storage.get_embed_theme(&t).await.unwrap(), None, "{name}");
        let mut theme = EmbedTheme {
            logo_url: Some("https://cdn.example.com/l.svg".into()),
            hide_badge: true,
            ..EmbedTheme::default()
        };
        theme.css_vars.insert("accent".into(), "#0af".into());
        storage.put_embed_theme(&t, &theme).await.unwrap();
        theme.css_vars.insert("radius".into(), "4px".into());
        storage.put_embed_theme(&t, &theme).await.unwrap();
        assert_eq!(
            storage.get_embed_theme(&t).await.unwrap(),
            Some(theme.clone()),
            "{name}"
        );

        let baseline = sequence(storage.as_ref(), &t, None).await;
        let candidate = sequence(storage.as_ref(), &t, None).await;
        let now = Utc::now();
        let release = WorkflowRelease {
            id: Uuid::new_v4(),
            tenant_id: t.clone(),
            namespace: baseline.namespace.clone(),
            sequence_name: baseline.name.clone(),
            baseline_sequence_id: baseline.id,
            baseline_version: 1,
            candidate_sequence_id: candidate.id,
            candidate_version: 2,
            state: ReleaseState::Draft,
            canary_percent: 0,
            gates: Vec::new(),
            in_flight_policy: orch8_types::release::InFlightPolicy::default(),
            validation_summary: None,
            canary_started_at: None,
            target: Some(ReleaseTarget {
                sub_tenants: Some(vec!["acme".into()]),
                percentage: None,
            }),
            created_at: now,
            updated_at: now,
        };
        storage.create_release(&release).await.unwrap();
        let got = storage.get_release(release.id).await.unwrap().unwrap();
        assert_eq!(got.target, release.target, "{name}");

        let retarget = ReleaseTarget {
            sub_tenants: None,
            percentage: Some(30),
        };
        assert!(
            storage
                .set_release_target(release.id, Some(&retarget))
                .await
                .unwrap(),
            "{name}"
        );
        let got = storage.get_release(release.id).await.unwrap().unwrap();
        assert_eq!(got.target, Some(retarget), "{name}");
        assert!(storage.set_release_target(release.id, None).await.unwrap());
        assert_eq!(
            storage
                .get_release(release.id)
                .await
                .unwrap()
                .unwrap()
                .target,
            None,
            "{name}"
        );
        assert!(
            !storage
                .set_release_target(Uuid::new_v4(), None)
                .await
                .unwrap(),
            "{name}"
        );
    }
}
