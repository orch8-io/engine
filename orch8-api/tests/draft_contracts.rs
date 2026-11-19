use orch8_storage::SequenceStore;
use serde_json::{Value, json};
use uuid::Uuid;

#[tokio::test]
async fn draft_contract_executes_only_mocks_and_leaves_production_storage_untouched() {
    let server = orch8_api::test_harness::spawn_test_server().await;
    let client = reqwest::Client::new();
    let id = Uuid::now_v7();
    let body = json!({
        "sequence": {
            "id":id, "name":"draft-check", "version":1, "tenant_id":"tenant-a",
            "namespace":"default", "created_at":"2026-01-01T00:00:00Z",
            "blocks":[{"type":"step","handler":"http_request","id":"send",
                       "params":{"url":"https://unreachable.invalid","method":"POST"}}]
        },
        "contract": {
            "cases":[{
                "name":"the mocked request completes", "input":{"order":"sample"},
                "mocks":[{"block":"send","type":"success","output":{"status":204}}],
                "expect":{"terminal_state":"completed","assertions":[
                    {"output":{"block":"send","path":"status"},"op":"equals","value":204}
                ]}
            }]
        }
    });
    let url = format!("{}/sequences/test-draft", server.v1_url());
    let response = client
        .post(&url)
        .header("x-tenant-id", "tenant-a")
        .json(&body)
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), reqwest::StatusCode::OK);
    let report: Value = response.json().await.unwrap();
    assert_eq!(report["passed"], true);
    assert_eq!(report["cases"][0]["executed_blocks"], json!(["send"]));
    assert!(
        server
            .storage
            .get_sequence(orch8_types::ids::SequenceId::from_uuid(id))
            .await
            .unwrap()
            .is_none()
    );

    let response = client
        .post(&url)
        .header("x-tenant-id", "tenant-b")
        .json(&body)
        .send()
        .await
        .unwrap();
    assert!(!response.status().is_success());
}
