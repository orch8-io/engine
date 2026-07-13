use chrono::{Duration, Utc};
use orch8_storage::{AdminStore, sqlite::SqliteStorage};
use orch8_types::ids::TenantId;
use orch8_types::trigger::{TriggerDef, TriggerType};

#[tokio::test]
async fn webhook_nonce_claim_is_atomic_and_expires() {
    let storage = SqliteStorage::in_memory().await.unwrap();
    let now = Utc::now();
    storage
        .create_trigger(&TriggerDef {
            slug: "signed-hook".into(),
            sequence_name: "sequence".into(),
            version: None,
            tenant_id: TenantId::unchecked("tenant"),
            namespace: "default".into(),
            enabled: true,
            secret: None,
            trigger_type: TriggerType::Webhook,
            config: serde_json::Value::Null,
            created_at: now,
            updated_at: now,
        })
        .await
        .unwrap();

    assert!(
        storage
            .claim_webhook_nonce("signed-hook", "nonce", now + Duration::minutes(5))
            .await
            .unwrap()
    );
    assert!(
        !storage
            .claim_webhook_nonce("signed-hook", "nonce", now + Duration::minutes(5))
            .await
            .unwrap()
    );

    // An already-expired claim is removed before the insert attempt.
    assert!(
        storage
            .claim_webhook_nonce("signed-hook", "expired", now - Duration::seconds(1))
            .await
            .unwrap()
    );
    assert!(
        storage
            .claim_webhook_nonce("signed-hook", "expired", now + Duration::minutes(5))
            .await
            .unwrap()
    );
}
