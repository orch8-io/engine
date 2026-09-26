//! `AiStore` on Postgres (migrations 095–097): prompt registry, LLM response
//! cache, tenant budgets. Gated on `DATABASE_URL`; skipped when absent.
//! Every test uses a unique tenant so runs can share one database.

use chrono::{Duration, Utc};
use serde_json::json;
use uuid::Uuid;

use orch8_storage::postgres::PostgresStorage;
use orch8_storage::{AiStore, TelemetryStore, UsageEvent};
use orch8_types::ai::{
    BUDGET_THRESHOLD_EVENT, BudgetAlert, BudgetPeriod, LlmCacheEntry, PromptCanary, PromptLabel,
    PromptMessage, PromptTemplate, TenantBudget,
};
use orch8_types::error::StorageError;
use orch8_types::ids::InstanceId;

async fn store() -> Option<PostgresStorage> {
    let url = std::env::var("DATABASE_URL").ok()?;
    let storage = PostgresStorage::new(&url, 5, None)
        .await
        .expect("connect to DATABASE_URL");
    storage.run_migrations().await.expect("run migrations");
    Some(storage)
}

macro_rules! require_postgres {
    () => {
        match store().await {
            Some(s) => s,
            None => {
                eprintln!("skipping: DATABASE_URL not set");
                return;
            }
        }
    };
}

fn tenant() -> String {
    format!("ai-{}", Uuid::now_v7())
}

fn prompt(tenant: &str, name: &str, version: i32) -> PromptTemplate {
    let mut p = PromptTemplate {
        tenant_id: tenant.into(),
        name: name.into(),
        version,
        system: Some("You triage tickets.".into()),
        messages: vec![PromptMessage {
            role: "user".into(),
            content: format!("v{version}: {{{{ ticket }}}}"),
        }],
        variables: vec!["ticket".into()],
        model_params: json!({"model": "gpt-4o", "temperature": 0}),
        response_schema: Some(json!({"type": "object"})),
        description: None,
        content_hash: String::new(),
        created_at: Utc::now(),
    };
    p.content_hash = p.compute_content_hash();
    p
}

#[tokio::test]
async fn pg_prompt_versions_labels_and_isolation() {
    let s = require_postgres!();
    let (t1, t2) = (tenant(), tenant());
    s.insert_prompt_version(&prompt(&t1, "triage", 1))
        .await
        .unwrap();
    s.insert_prompt_version(&prompt(&t1, "triage", 2))
        .await
        .unwrap();
    s.insert_prompt_version(&prompt(&t2, "triage", 1))
        .await
        .unwrap();
    assert!(matches!(
        s.insert_prompt_version(&prompt(&t1, "triage", 2)).await,
        Err(StorageError::Conflict(_))
    ));
    let latest = s
        .get_latest_prompt_version(&t1, "triage")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(latest.version, 2);
    assert_eq!(latest.response_schema, Some(json!({"type": "object"})));
    assert_eq!(
        s.list_prompt_versions(&t1, None, 10).await.unwrap().len(),
        2
    );
    assert_eq!(
        s.list_prompt_versions(&t2, Some("triage"), 10)
            .await
            .unwrap()
            .len(),
        1
    );

    let mut label = PromptLabel {
        tenant_id: t1.clone(),
        name: "triage".into(),
        label: "production".into(),
        version: 1,
        canary: Some(PromptCanary {
            version: 2,
            percent: 25,
        }),
        updated_at: Utc::now(),
    };
    s.upsert_prompt_label(&label).await.unwrap();
    let got = s
        .get_prompt_label(&t1, "triage", "production")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(got.canary, label.canary);
    label.version = 2;
    label.canary = None;
    s.upsert_prompt_label(&label).await.unwrap();
    let got = s
        .get_prompt_label(&t1, "triage", "production")
        .await
        .unwrap()
        .unwrap();
    assert_eq!((got.version, got.canary), (2, None));
    assert!(
        s.get_prompt_label(&t2, "triage", "production")
            .await
            .unwrap()
            .is_none()
    );
    label.version = 42;
    assert!(
        s.upsert_prompt_label(&label).await.is_err(),
        "FK to versions"
    );
    assert_eq!(s.list_prompt_labels(&t1, None).await.unwrap().len(), 1);
    assert!(
        s.delete_prompt_label(&t1, "triage", "production")
            .await
            .unwrap()
    );
}

#[tokio::test]
async fn pg_llm_cache_tenant_expiry_and_partition() {
    let s = require_postgres!();
    let (t1, t2) = (tenant(), tenant());
    let now = Utc::now();
    let mk = |t: &str, key: &str, ttl: i64| LlmCacheEntry {
        tenant_id: t.into(),
        cache_key: key.into(),
        partition_key: "part".into(),
        provider: "openai".into(),
        model: "gpt-4o".into(),
        response: json!({"message": {"content": key}}),
        embedding: Some(json!([1.0, 0.0])),
        input_tokens: 7,
        output_tokens: 3,
        size_bytes: 64,
        created_at: now,
        expires_at: now + Duration::seconds(ttl),
    };
    s.put_llm_cache_entry(&mk(&t1, "k1", 60)).await.unwrap();
    s.put_llm_cache_entry(&mk(&t1, "k1", 120)).await.unwrap(); // upsert
    s.put_llm_cache_entry(&mk(&t1, "stale", -5)).await.unwrap();
    let hit = s
        .get_llm_cache_entry(&t1, "k1", now)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(hit.input_tokens, 7);
    assert!(
        s.get_llm_cache_entry(&t2, "k1", now)
            .await
            .unwrap()
            .is_none()
    );
    assert!(
        s.get_llm_cache_entry(&t1, "stale", now)
            .await
            .unwrap()
            .is_none()
    );
    assert_eq!(
        s.list_llm_cache_partition(&t1, "part", now, 10)
            .await
            .unwrap()
            .len(),
        1
    );
    assert!(s.delete_expired_llm_cache(now, 1_000).await.unwrap() >= 1);
    assert_eq!(s.purge_llm_cache(&t1).await.unwrap(), 1);
}

#[tokio::test]
async fn pg_budgets_alerts_and_billable_usage() {
    let s = require_postgres!();
    let t1 = tenant();
    let now = Utc::now();
    let budget = TenantBudget {
        id: Uuid::now_v7(),
        tenant_id: t1.clone(),
        model: Some("gpt-4o".into()),
        period: BudgetPeriod::Daily,
        limit_usd: 1.5,
        thresholds: vec![50, 80, 100],
        hard_cap: true,
        created_at: now,
        updated_at: now,
    };
    s.upsert_tenant_budget(&budget).await.unwrap();
    let mut hijack = budget.clone();
    hijack.tenant_id = tenant();
    assert!(s.upsert_tenant_budget(&hijack).await.is_err());
    assert_eq!(
        s.list_tenant_budgets(&t1).await.unwrap(),
        vec![budget.clone()]
    );

    let alert = BudgetAlert {
        id: Uuid::now_v7(),
        event: BUDGET_THRESHOLD_EVENT.into(),
        tenant_id: t1.clone(),
        budget_id: budget.id,
        model: budget.model.clone(),
        period: budget.period,
        period_start: budget.period.window(now).0,
        threshold_percent: 80,
        spend_usd: 1.3,
        limit_usd: 1.5,
        blocking: false,
        created_at: now,
    };
    assert!(s.record_budget_alert(&alert).await.unwrap());
    let mut dup = alert.clone();
    dup.id = Uuid::now_v7();
    assert!(!s.record_budget_alert(&dup).await.unwrap());
    assert_eq!(s.list_budget_alerts(&t1, 5).await.unwrap(), vec![alert]);
    assert!(s.delete_tenant_budget(&t1, budget.id).await.unwrap());

    // Instance token totals count billable usage only, not cache-hit savings.
    let instance = InstanceId::new();
    for kind in ["llm_tokens", "llm_cache_hit"] {
        s.record_usage_event(&UsageEvent {
            tenant_id: t1.clone(),
            instance_id: Some(instance),
            block_id: Some("llm".into()),
            kind: kind.into(),
            model: "gpt-4o".into(),
            input_tokens: 10,
            output_tokens: 5,
            created_at: now,
        })
        .await
        .unwrap();
    }
    assert_eq!(
        s.query_instance_usage_totals(instance).await.unwrap(),
        (10, 5)
    );
}
