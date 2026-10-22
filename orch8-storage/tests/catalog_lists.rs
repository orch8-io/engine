//! Operator catalogs must enumerate independently of public API page caps.
use chrono::Utc;
use orch8_storage::backend_test;
use orch8_storage::testing::fixtures::SequenceBuilder;
use orch8_types::config::SecretString;
use orch8_types::credential::{CredentialDef, CredentialKind};
use orch8_types::cron::{CronSchedule, OverlapPolicy};
use orch8_types::ids::{Namespace, TenantId};

backend_test!(async fn complete_catalogs_keep_every_row_and_preserve_tenant_visibility(b) {
    let a = SequenceBuilder::new("tenant-a").step("s", "noop").build();
    let c = SequenceBuilder::new("tenant-b").step("s", "noop").build();
    b.storage.create_sequence(&a).await.unwrap();
    b.storage.create_sequence(&c).await.unwrap();
    let now = Utc::now();
    for index in 0..1001 {
        let seq = if index % 2 == 0 { &a } else { &c };
        b.storage.create_credential(&CredentialDef {
            id: format!("credential-{index:04}"),
            tenant_id: if index == 0 { String::new() } else { seq.tenant_id.to_string() },
            name: format!("Credential {index}"), kind: CredentialKind::default(),
            value: SecretString::from(format!("secret-{index}")),
            expires_at: None, refresh_url: None, refresh_token: Some(SecretString::from(format!("refresh-{index}"))),
            enabled: index % 3 != 0, description: None, created_at: now, updated_at: now,
        }).await.unwrap();
        b.storage.create_cron_schedule(&CronSchedule {
            id: uuid::Uuid::now_v7(), tenant_id: seq.tenant_id.clone(), namespace: Namespace::new("default"),
            sequence_id: seq.id, cron_expr: "0 9 * * *".into(), timezone: "UTC".into(),
            enabled: index % 3 != 0, metadata: serde_json::json!({"index": index}),
            overlap_policy: OverlapPolicy::default(), skipped_fires: 0, last_skipped_at: None,
            last_triggered_at: None, next_fire_at: None, created_at: now, updated_at: now,
        }).await.unwrap();
    }
    // Public list limits remain bounded.
    assert_eq!(b.storage.list_credentials(None, u32::MAX).await.unwrap().len(), 1000);
    assert_eq!(b.storage.list_cron_schedules(None, u32::MAX).await.unwrap().len(), 1000);
    let credentials = b.storage.list_all_credentials(None).await.unwrap();
    assert_eq!(credentials.len(), 1001);
    assert_eq!(credentials.iter().find(|v| v.id == "credential-1000").unwrap().value.expose(), "secret-1000");
    assert_eq!(credentials.iter().find(|v| v.id == "credential-1000").unwrap().refresh_token.as_ref().unwrap().expose(), "refresh-1000");
    assert_eq!(b.storage.list_all_cron_schedules(None).await.unwrap().len(), 1001);
    for (tenant, credential_count, cron_count) in [("tenant-a", 501, 501), ("tenant-b", 501, 500), ("missing", 1, 0)] {
        let tenant = TenantId::unchecked(tenant);
        let credentials = b.storage.list_all_credentials(Some(&tenant)).await.unwrap();
        assert_eq!(credentials.len(), credential_count);
        assert!(credentials.iter().all(|v| v.tenant_id.is_empty() || v.tenant_id == tenant.as_str()));
        let cron = b.storage.list_all_cron_schedules(Some(&tenant)).await.unwrap();
        assert_eq!(cron.len(), cron_count);
        assert!(cron.iter().all(|v| v.tenant_id == tenant));
    }
});
