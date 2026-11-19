use chrono::Utc;
use orch8_storage::{InstanceStore, OutputStore};
use orch8_types::contract::ContractSuite;
use orch8_types::ids::{BlockId, InstanceId};
use orch8_types::instance::InstanceState;
use orch8_types::output::BlockOutput;
use orch8_types::sequence::SequenceDefinition;
use reqwest::StatusCode;
use serde_json::{Value, json};
use uuid::Uuid;

#[tokio::test]
#[allow(clippy::too_many_lines)] // Exercise the complete release and export lifecycle.
async fn operator_can_export_a_runnable_redacted_regression_for_a_saved_divergence() {
    let server = orch8_api::test_harness::spawn_test_server().await;
    let client = reqwest::Client::new();
    let base = server.v1_url();
    let tenant = "tenant-a";
    let baseline = Uuid::now_v7();
    let candidate = Uuid::now_v7();
    let step = json!({"type":"step","id":"check.result","handler":"noop","params":{}});
    let extra = json!({
        "type":"step","id":"extra","handler":"noop","when":"include_extra == true",
        "params":{"password":"raw-definition-password","example":{"id":"payload-only-id","handler":"literal"}}
    });
    for (id, version, blocks) in [
        (baseline, 1, json!([step])),
        (candidate, 2, json!([step, extra])),
    ] {
        let response = client
            .post(format!("{base}/sequences"))
            .header("X-Tenant-Id", tenant)
            .header("X-Orch8-Sub-Tenant", "site-a")
            .json(
                &json!({"id":id,"version":version,"name":"inspection","tenant_id":tenant,
                          "namespace":"default","blocks":blocks,"created_at":Utc::now()}),
            )
            .send()
            .await
            .unwrap();
        assert_eq!(
            response.status(),
            StatusCode::CREATED,
            "{}",
            response.text().await.unwrap()
        );
    }
    let response = client
        .post(format!("{base}/instances"))
        .header("X-Tenant-Id", tenant)
        .header("X-Orch8-Sub-Tenant", "site-a")
        .json(&json!({
            "sequence_id":baseline,"tenant_id":tenant,"namespace":"default",
            "context":{"data":{"region,name":"test","include_extra":true,"password":"raw-input-password","customer":"private-customer"}}
        }))
        .send().await.unwrap();
    assert_eq!(response.status(), StatusCode::CREATED);
    let instance: InstanceId =
        serde_json::from_value(response.json::<Value>().await.unwrap()["id"].clone()).unwrap();
    // The latest successful attempt is the only one included in the fixture.
    let superseded = "old".repeat(180_000);
    for (block, output, reference) in [
        ("check.result", json!({"reply": superseded}), None),
        (
            "parked",
            json!({"reply": "pending"}),
            Some("__in_progress__"),
        ),
    ] {
        server
            .storage
            .save_block_output(&BlockOutput {
                id: Uuid::now_v7(),
                instance_id: instance,
                block_id: BlockId::new(block),
                output_size: u32::try_from(output.to_string().len()).unwrap(),
                output,
                output_ref: reference.map(str::to_owned),
                attempt: 0,
                created_at: Utc::now() - chrono::Duration::seconds(1),
            })
            .await
            .unwrap();
    }
    server.storage.save_block_output(&BlockOutput {
        id: Uuid::now_v7(),
        instance_id: instance,
        block_id: BlockId::new("check.result"),
        output: json!({"http.status":202,"reply":"Bearer raw-output-secret","customer":"private-output"}),
        output_ref: None,
        output_size: 70,
        attempt: 0,
        created_at: Utc::now(),
    }).await.unwrap();
    server
        .storage
        .update_instance_state(instance, InstanceState::Completed, None)
        .await
        .unwrap();
    let response = client
        .post(format!("{base}/releases"))
        .header("X-Tenant-Id", tenant)
        .json(&json!({"tenant_id":tenant,"baseline_sequence_id":baseline,"candidate_sequence_id":candidate}))
        .send().await.unwrap();
    assert_eq!(
        response.status(),
        StatusCode::CREATED,
        "{}",
        response.text().await.unwrap()
    );
    let release: Uuid =
        serde_json::from_value(response.json::<Value>().await.unwrap()["id"].clone()).unwrap();
    let response = client
        .post(format!("{base}/releases/{release}/validate"))
        .header("X-Tenant-Id", tenant)
        .json(&json!({}))
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let sample = response.json::<Value>().await.unwrap();
    assert_eq!(sample["divergences"][0]["blocks_added"], json!(["extra"]));
    assert_eq!(sample["divergences"][0]["blocks_removed"], json!([]));

    let fixture_url = format!("{base}/releases/{release}/divergences/{instance}/fixture");
    let default_fixture: Value = client
        .post(&fixture_url)
        .header("X-Tenant-Id", tenant)
        .json(&json!({}))
        .send()
        .await
        .unwrap()
        .error_for_status()
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(default_fixture["reproduces_divergence"], false);
    assert_eq!(default_fixture["contract"]["cases"][0]["input"], json!({}));
    assert_eq!(
        default_fixture["contract"]["cases"][0]["mocks"][0]["output"],
        json!({})
    );

    let response = client
        .post(&fixture_url)
        .header("X-Tenant-Id", tenant)
        .header("X-Orch8-Sub-Tenant", "site-a")
        .json(
            &json!({"input_keys":["region,name","include_extra","password"],"output_keys":{"check.result":["http.status","reply"]}}),
        )
        .send()
        .await
        .unwrap()
        .error_for_status()
        .unwrap();
    assert_eq!(response.headers()["cache-control"], "no-store");
    let fixture: Value = response.json().await.unwrap();
    assert_eq!(fixture["reproduces_divergence"], true);
    let mocks = fixture["contract"]["cases"][0]["mocks"].as_array().unwrap();
    let targets = mocks
        .iter()
        .map(|mock| mock["block"].as_str().unwrap())
        .collect::<Vec<_>>();
    assert_eq!(targets, ["check.result", "extra"]);
    assert_eq!(
        fixture["contract"]["cases"][0]["input"],
        json!({"region,name":"test","include_extra":true})
    );
    assert_eq!(
        fixture["contract"]["cases"][0]["mocks"][0]["output"],
        json!({"http.status":202,"reply":"[REDACTED]"})
    );
    for redacted in [
        "raw-definition-password",
        "raw-input-password",
        "raw-output-secret",
        "private-customer",
        "private-output",
    ] {
        assert!(!fixture.to_string().contains(redacted));
    }
    let mut definition: SequenceDefinition =
        serde_json::from_value(fixture["sequence"].clone()).unwrap();
    let contract: ContractSuite = serde_json::from_value(fixture["contract"].clone()).unwrap();
    let report = orch8::contract::run_suite(
        &definition,
        &contract,
        &orch8::contract::RunOptions::default(),
    )
    .await
    .unwrap();
    assert!(!report.passed);
    definition.blocks.pop();
    let fixed = orch8::contract::run_suite(
        &definition,
        &contract,
        &orch8::contract::RunOptions::default(),
    )
    .await
    .unwrap();
    assert!(fixed.passed);

    for (tenant, site) in [("tenant-b", "site-a"), (tenant, "site-b")] {
        let denied = client
            .post(&fixture_url)
            .header("X-Tenant-Id", tenant)
            .header("X-Orch8-Sub-Tenant", site)
            .json(&json!({}))
            .send()
            .await
            .unwrap();
        assert_eq!(denied.status(), StatusCode::NOT_FOUND);
    }
    for block in ["check.result", "extra"] {
        server
            .storage
            .save_block_output(&BlockOutput {
                id: Uuid::now_v7(),
                instance_id: instance,
                block_id: BlockId::new(block),
                output: json!({"reply": "x".repeat(300 * 1024)}),
                output_ref: None,
                output_size: 300 * 1024,
                attempt: 0,
                created_at: Utc::now(),
            })
            .await
            .unwrap();
    }
    let oversized = client
        .post(&fixture_url)
        .header("X-Tenant-Id", tenant)
        .header("X-Orch8-Sub-Tenant", "site-a")
        .json(&json!({"output_keys":{"check.result":["reply"],"extra":["reply"]}}))
        .send()
        .await
        .unwrap();
    assert_eq!(oversized.status(), StatusCode::BAD_REQUEST);
    assert!(
        oversized
            .text()
            .await
            .unwrap()
            .contains("Selected outputs exceed")
    );
    let absent = client
        .post(format!(
            "{base}/releases/{release}/divergences/{}/fixture",
            Uuid::now_v7()
        ))
        .header("X-Tenant-Id", tenant)
        .json(&json!({}))
        .send()
        .await
        .unwrap();
    assert_eq!(absent.status(), StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn fixture_export_requires_operator_and_bounds_field_selection() {
    use orch8_api::test_harness::{TestServerOptions, spawn_test_server_with};
    use orch8_storage::sqlite::SqliteStorage;
    use std::sync::Arc;

    let root = "replay-fixture-test-root";
    let storage = Arc::new(SqliteStorage::in_memory().await.unwrap());
    let server = spawn_test_server_with(
        storage,
        TestServerOptions {
            root_api_key: Some(root.into()),
            ..Default::default()
        },
    )
    .await;
    let client = reqwest::Client::new();
    let base = server.v1_url();
    let fixture = format!(
        "{base}/releases/{}/divergences/{}/fixture",
        Uuid::now_v7(),
        Uuid::now_v7()
    );
    for (capability, expected) in [
        ("publisher", StatusCode::FORBIDDEN),
        ("operator", StatusCode::NOT_FOUND),
    ] {
        let key: Value = client
            .post(format!("{base}/api-keys"))
            .header("X-API-Key", root)
            .json(&json!({"tenant_id":"tenant-a","name":capability,"capabilities":[capability]}))
            .send()
            .await
            .unwrap()
            .error_for_status()
            .unwrap()
            .json()
            .await
            .unwrap();
        let response = client
            .post(&fixture)
            .header("X-API-Key", key["secret"].as_str().unwrap())
            .json(&json!({}))
            .send()
            .await
            .unwrap();
        assert_eq!(response.status(), expected);
        if capability == "publisher" {
            assert!(
                response
                    .text()
                    .await
                    .unwrap()
                    .contains("requires Operator capability")
            );
        }
    }

    let keys = (0..128).map(|i| format!("field-{i}")).collect::<Vec<_>>();
    for (output_keys, expected) in [
        (json!({"step":["field"]}), StatusCode::BAD_REQUEST),
        (json!({}), StatusCode::NOT_FOUND),
    ] {
        let response = client
            .post(&fixture)
            .header("X-API-Key", root)
            .json(&json!({"input_keys":keys,"output_keys":output_keys}))
            .send()
            .await
            .unwrap();
        assert_eq!(response.status(), expected);
    }
}
