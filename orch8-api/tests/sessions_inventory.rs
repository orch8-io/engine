use chrono::Utc;
use orch8_api::test_harness::spawn_test_server;
use orch8_storage::{
    AdminStore, InstanceStore, SequenceStore,
    testing::fixtures::{InstanceBuilder, SequenceBuilder},
};
use orch8_types::{
    ids::{InstanceId, Namespace, TenantId},
    instance::InstanceState,
    session::{Session, SessionState},
};
use reqwest::StatusCode;
use serde_json::{Value, json};
use uuid::Uuid;

#[tokio::test]
async fn pages_associated_execution_metadata_before_fetching_context() {
    let server = spawn_test_server().await;
    let tenant = TenantId::unchecked("customer");
    let sequence = SequenceBuilder::new("customer").step("one", "noop").build();
    server.storage.create_sequence(&sequence).await.unwrap();
    let session_id = Uuid::now_v7();
    let different_session = Uuid::now_v7();
    for (id, key) in [(session_id, "selected"), (different_session, "other")] {
        server
            .storage
            .create_session(&Session {
                id,
                tenant_id: tenant.clone(),
                session_key: key.into(),
                data: Value::Null,
                state: SessionState::Active,
                created_at: Utc::now(),
                updated_at: Utc::now(),
                expires_at: None,
            })
            .await
            .unwrap();
    }
    for index in 1..=55u128 {
        let mut instance = InstanceBuilder::for_sequence(&sequence)
            .id(InstanceId::from_uuid(Uuid::from_u128(index)))
            .state(if index == 53 {
                InstanceState::Paused
            } else {
                InstanceState::Completed
            })
            .build();
        instance.tenant_id = tenant.clone();
        instance.namespace = Namespace::new(if index == 54 {
            "Ops alternative"
        } else {
            "Ops_% North"
        });
        instance.session_id = Some(if index == 55 {
            different_session
        } else {
            session_id
        });
        instance.metadata = json!({"private":"FULL_CONTEXT_IS_SEPARATE"});
        server.storage.create_instance(&instance).await.unwrap();
    }
    let client = reqwest::Client::new();
    let url = format!("{}/sessions/{session_id}/instances", server.v1_url());
    let list = || client.get(&url).header("X-Tenant-Id", "customer");
    let filters = [
        ("view", "page"),
        ("search", "OPS_% north"),
        ("state", "completed"),
    ];
    let first: Value = list()
        .query(&filters)
        .send()
        .await
        .unwrap()
        .error_for_status()
        .unwrap()
        .json()
        .await
        .unwrap();
    let rows = first["items"].as_array().unwrap();
    assert_eq!(rows.len(), 50);
    assert_eq!(rows[0]["id"], Uuid::from_u128(52).to_string());
    assert!(rows.iter().all(|row| row.get("context").is_none()
        && row.get("metadata").is_none()
        && row["session_id"] == session_id.to_string()));
    let next = first["next_cursor"].as_str().unwrap();
    let second: Value = list()
        .query(&filters)
        .query(&[("cursor", next)])
        .send()
        .await
        .unwrap()
        .error_for_status()
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(second["items"].as_array().unwrap().len(), 2);
    assert_eq!(second["next_cursor"], Value::Null);
    let legacy: Value = list()
        .send()
        .await
        .unwrap()
        .error_for_status()
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(legacy.as_array().unwrap().len(), 54);
    assert_eq!(legacy[0]["metadata"]["private"], "FULL_CONTEXT_IS_SEPARATE");
    assert_eq!(
        client
            .get(url)
            .header("X-Tenant-Id", "other")
            .query(&filters)
            .send()
            .await
            .unwrap()
            .status(),
        StatusCode::NOT_FOUND
    );
}

#[tokio::test]
async fn lists_only_scoped_metadata_with_literal_filters_and_keyset_pages() {
    let server = spawn_test_server().await;
    let client = reqwest::Client::new();
    let base = server.v1_url();
    let now = Utc::now();
    for index in 1..=46u128 {
        let (tenant, key, state) = match index {
            43 => ("tenant", "onboarding-43".to_owned(), SessionState::Active),
            44 => ("tenant", "onboarding_%44".to_owned(), SessionState::Paused),
            45 => ("other", "onboarding_%45".to_owned(), SessionState::Active),
            46 => ("tenant", "Onboarding_%46".to_owned(), SessionState::Active),
            _ => (
                "tenant",
                format!("onboarding_%{index}"),
                SessionState::Active,
            ),
        };
        server
            .storage
            .create_session(&Session {
                id: Uuid::from_u128(index),
                tenant_id: TenantId::unchecked(tenant),
                session_key: key,
                data: json!({"private":"SESSION_PAYLOAD"}),
                state,
                created_at: now,
                updated_at: now,
                expires_at: None,
            })
            .await
            .unwrap();
    }
    let list = || client.get(format!("{base}/sessions"));
    assert_eq!(list().send().await.unwrap().status(), StatusCode::FORBIDDEN);
    let scoped = || list().header("X-Tenant-Id", "tenant");
    let filters = [("search", "onboarding_%"), ("state", "active")];
    let first: Value = scoped()
        .query(&filters)
        .send()
        .await
        .unwrap()
        .error_for_status()
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(first["tenant_id"], "tenant");
    assert_eq!(first["search"], "onboarding_%");
    assert_eq!(first["state"], "active");
    assert_eq!(first["cursor"], Value::Null);
    let items = first["items"].as_array().unwrap();
    assert_eq!(items.len(), 40);
    assert!(items.iter().all(|item| item["tenant_id"] == "tenant"
        && item["state"] == "active"
        && item.get("data").is_none()));
    assert_eq!(items[0]["id"], Uuid::from_u128(42).to_string());
    let cursor = first["next_cursor"].as_str().unwrap();
    assert_eq!(cursor, Uuid::from_u128(3).to_string());
    let second: Value = scoped()
        .query(&filters)
        .query(&[("cursor", cursor)])
        .send()
        .await
        .unwrap()
        .error_for_status()
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(second["cursor"], cursor);
    assert_eq!(second["next_cursor"], Value::Null);
    assert_eq!(second["items"].as_array().unwrap().len(), 2);
    assert_eq!(second["items"][0]["id"], Uuid::from_u128(2).to_string());
    assert_eq!(
        scoped()
            .query(&[("search", "\0")])
            .send()
            .await
            .unwrap()
            .status(),
        StatusCode::BAD_REQUEST
    );
}
