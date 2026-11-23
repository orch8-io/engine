use orch8_api::test_harness::spawn_test_server;
use reqwest::StatusCode;
use serde_json::{Value, json};

async fn create(client: &reqwest::Client, base: &str, data: Value) -> String {
    create_with_schema(client, base, data, Value::Null).await
}

async fn create_with_schema(
    client: &reqwest::Client,
    base: &str,
    data: Value,
    schema: Value,
) -> String {
    let sequence: Value = client.post(format!("{base}/sequences"))
        .header("X-Tenant-Id", "t1")
        .json(&json!({"id":uuid::Uuid::now_v7(),"created_at":chrono::Utc::now().to_rfc3339(),"deprecated":false,"tenant_id":"t1","namespace":"ns1","name":"context-patch-test","version":1,"input_schema":schema,"blocks":[{"type":"step","id":"s1","handler":"noop","params":{}}]}))
        .send().await.unwrap().error_for_status().unwrap().json().await.unwrap();
    let instance: Value = client.post(format!("{base}/instances"))
        .header("X-Tenant-Id", "t1")
        .json(&json!({"sequence_id":sequence["id"],"tenant_id":"t1","namespace":"ns1","context":{"data":data,"config":{"retained":"config"},"runtime":{"total_steps_executed":7}}}))
        .send().await.unwrap().error_for_status().unwrap().json().await.unwrap();
    instance["id"].as_str().unwrap().to_owned()
}

#[tokio::test]
async fn marker_shaped_json_round_trips_beside_a_real_legacy_reference() {
    use orch8_storage::{InstanceStore, ResourceStore, externalizing::literal_payload};
    use orch8_types::ids::InstanceId;

    let server = spawn_test_server().await;
    let client = reqwest::Client::new();
    let literal = json!({"_externalized":true,"_ref":"same-instance"});
    let id = create(
        &client,
        &server.base_url,
        json!({"literal":literal,"nested":[literal]}),
    )
    .await;
    let owner = InstanceId::from_uuid(id.parse().unwrap());
    let mut stored = server
        .storage
        .get_instance(owner)
        .await
        .unwrap()
        .unwrap()
        .context;
    assert_eq!(literal_payload(&stored.data["literal"]), Some(&literal));
    assert_eq!(literal_payload(&stored.data["nested"][0]), Some(&literal));
    stored.data["legacy"] = literal.clone();
    let payload = json!({"resolved":true});
    server
        .storage
        .save_externalized_state(owner, "same-instance", &payload)
        .await
        .unwrap();
    server
        .storage
        .update_instance_context(owner, &stored)
        .await
        .unwrap();

    let data = &read(&client, &server.base_url, &id).await["context"]["data"];
    assert_eq!(data["literal"], literal);
    assert_eq!(data["nested"][0], literal);
    assert_eq!(data["legacy"], payload);
}

#[tokio::test]
async fn sequence_externalizes_nested_value_and_pointer_patch_preserves_its_ref() {
    use orch8_engine::{
        handlers::HandlerRegistry, scheduler::tick_once, sequence_cache::SequenceCache,
    };
    use orch8_storage::{
        InstanceStore, OutputStore, ResourceStore, StorageBackend, externalizing::ref_key,
    };
    use orch8_types::{
        config::SchedulerConfig,
        ids::{BlockId, InstanceId},
        instance::InstanceState,
    };
    use std::sync::Arc;
    use tokio::sync::Semaphore;
    use tokio_util::sync::CancellationToken;

    let server = spawn_test_server().await;
    let client = reqwest::Client::new();
    let body = "report".repeat(16_384);
    let id = create_with_schema(
        &client,
        &server.base_url,
        json!({"report":{"body":body,"title":"draft"}}),
        json!({"type":"object","x-orch8-externalize":["/report/body"]}),
    )
    .await;
    let owner = InstanceId::from_uuid(id.parse().unwrap());
    let mut handlers = HandlerRegistry::new();
    handlers.register("noop", |ctx| {
        Box::pin(async move {
            Ok(json!({"body_length": ctx.context.data["report"]["body"].as_str().map(str::len)}))
        })
    });
    let storage: Arc<dyn StorageBackend> = server.storage.clone();
    tick_once(
        &storage,
        &Arc::new(handlers),
        &Arc::new(Semaphore::new(1)),
        &SchedulerConfig::default(),
        &Arc::new(SequenceCache::new(100, std::time::Duration::from_secs(60))),
        &CancellationToken::new(),
    )
    .await
    .unwrap();
    let before = server.storage.get_instance(owner).await.unwrap().unwrap();
    assert_eq!(before.state, InstanceState::Completed);
    let marker = &before.context.data["report"]["body"];
    let reference = ref_key(marker, false).unwrap();
    assert_eq!(
        server
            .storage
            .get_externalized_state(owner, reference)
            .await
            .unwrap(),
        Some(json!(body))
    );
    assert_eq!(
        server
            .storage
            .get_block_output(owner, &BlockId::new("s1"))
            .await
            .unwrap()
            .unwrap()
            .output["body_length"],
        json!(body.len())
    );

    let response = client
        .patch(format!("{}/instances/{id}/context/data", server.base_url))
        .header("X-Tenant-Id", "t1")
        .json(&json!({"pointer_patch":{"/report/title":"published"}}))
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let after = server.storage.get_instance(owner).await.unwrap().unwrap();
    assert_eq!(after.context.data["report"]["body"], *marker);
    assert_eq!(after.context.data["report"]["title"], "published");
    assert_eq!(
        server
            .storage
            .list_externalized_entries(owner, None, 10)
            .await
            .unwrap()
            .len(),
        1
    );
    assert_eq!(
        read(&client, &server.base_url, &id).await["context"]["data"],
        json!({"report":{"body":body,"title":"published"}}),
    );
}
async fn read(client: &reqwest::Client, base: &str, id: &str) -> Value {
    client
        .get(format!("{base}/instances/{id}"))
        .header("X-Tenant-Id", "t1")
        .send()
        .await
        .unwrap()
        .error_for_status()
        .unwrap()
        .json()
        .await
        .unwrap()
}
#[tokio::test]
async fn context_replacement_preserves_the_engine_run_and_rejects_runtime_forgery() {
    use orch8_storage::InstanceStore;
    let server = spawn_test_server().await;
    let client = reqwest::Client::new();
    let id = create(&client, &server.base_url, json!({"old":"data"})).await;
    let instance_id = orch8_types::ids::InstanceId::from_uuid(uuid::Uuid::parse_str(&id).unwrap());
    let mut instance = server
        .storage
        .get_instance(instance_id)
        .await
        .unwrap()
        .unwrap();
    instance.context.runtime.run_id = Some("original-run".into());
    instance.context.runtime.current_step = Some(orch8_types::ids::BlockId::new("s1"));
    instance.context.runtime.current_step_started_at = Some(chrono::Utc::now());
    instance.context.runtime.dry_run = true;
    server
        .storage
        .update_instance_context(instance_id, &instance.context)
        .await
        .unwrap();
    let original = read(&client, &server.base_url, &id).await["context"]["runtime"].clone();
    for runtime in [
        None,
        Some(json!({"run_id":"forged-run","dry_run":false,"total_steps_executed":0})),
    ] {
        let mut context = json!({"data":{"replacement":true}});
        if let Some(runtime) = runtime {
            context["runtime"] = runtime;
        }
        let response = client
            .patch(format!("{}/instances/{id}/context", server.base_url))
            .header("X-Tenant-Id", "t1")
            .json(&json!({"context":context}))
            .send()
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let after = read(&client, &server.base_url, &id).await;
        assert_eq!(after["context"]["runtime"], original);
        assert_eq!(after["context"]["data"], json!({"replacement":true}));
    }
}
#[tokio::test]
async fn data_patch_preserves_other_fields_and_context_sections() {
    let server = spawn_test_server().await;
    let client = reqwest::Client::new();
    let id = create(
        &client,
        &server.base_url,
        json!({"input":"keep","setting":"old"}),
    )
    .await;
    let before = read(&client, &server.base_url, &id).await;
    let response = client
        .patch(format!("{}/instances/{id}/context/data", server.base_url))
        .header("X-Tenant-Id", "t1")
        .json(&json!({"patch":{"setting":"new","other":42}}))
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let after = read(&client, &server.base_url, &id).await;
    assert_eq!(
        after["context"]["data"],
        json!({"input":"keep","setting":"new","other":42})
    );
    for section in ["config", "runtime", "audit"] {
        assert_eq!(after["context"][section], before["context"][section]);
    }
}
#[tokio::test]
async fn data_patch_rejects_invalid_shape_and_tenant_without_changes() {
    let server = spawn_test_server().await;
    let client = reqwest::Client::new();
    let id = create(&client, &server.base_url, json!({"keep":true})).await;
    for (tenant, body) in [
        ("t2", json!({"patch":{"keep":false}})),
        ("t1", json!({"patch":[],"runtime":{}})),
        ("t1", json!({"patch":{},"context":{}})),
    ] {
        let response = client
            .patch(format!("{}/instances/{id}/context/data", server.base_url))
            .header("X-Tenant-Id", tenant)
            .json(&body)
            .send()
            .await
            .unwrap();
        assert!(!response.status().is_success());
    }
    assert_eq!(
        read(&client, &server.base_url, &id).await["context"]["data"],
        json!({"keep":true})
    );
}
#[tokio::test]
async fn data_patch_does_not_replace_non_object_data() {
    let server = spawn_test_server().await;
    let client = reqwest::Client::new();
    let id = create(&client, &server.base_url, json!([1, 2])).await;
    let response = client
        .patch(format!("{}/instances/{id}/context/data", server.base_url))
        .header("X-Tenant-Id", "t1")
        .json(&json!({"patch":{"key":42}}))
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    assert_eq!(
        read(&client, &server.base_url, &id).await["context"]["data"],
        json!([1, 2])
    );
}
#[tokio::test]
async fn concurrent_accepted_patches_are_not_lost() {
    let server = spawn_test_server().await;
    let client = reqwest::Client::new();
    let id = create(&client, &server.base_url, json!({"keep":true})).await;
    let mut tasks = tokio::task::JoinSet::new();
    for index in 0..12 {
        let client = client.clone();
        let url = format!("{}/instances/{id}/context/data", server.base_url);
        tasks.spawn(async move {
            let key = format!("key{index}");
            let response = client
                .patch(url)
                .header("X-Tenant-Id", "t1")
                .json(&json!({"patch":{key.clone():index}}))
                .send()
                .await
                .unwrap();
            (key, index, response.status())
        });
    }
    let mut accepted = Vec::new();
    while let Some(result) = tasks.join_next().await {
        let (key, index, status) = result.unwrap();
        assert!(status == StatusCode::OK || status == StatusCode::CONFLICT);
        if status == StatusCode::OK {
            accepted.push((key, index));
        }
    }
    assert!(!accepted.is_empty());
    let after = read(&client, &server.base_url, &id).await;
    for (key, index) in accepted {
        assert_eq!(after["context"]["data"][key], index);
    }
    assert_eq!(after["context"]["data"]["keep"], true);
}

#[tokio::test]
async fn oversized_merge_leaves_context_unchanged() {
    let server = orch8_api::test_harness::spawn_test_server_with_context_limit(4096).await;
    let client = reqwest::Client::new();
    let id = create(&client, &server.base_url, json!({"keep":true})).await;
    let response = client
        .patch(format!("{}/instances/{id}/context/data", server.base_url))
        .header("X-Tenant-Id", "t1")
        .json(&json!({"patch":{"large":"x".repeat(5000)}}))
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::PAYLOAD_TOO_LARGE);
    assert_eq!(
        read(&client, &server.base_url, &id).await["context"]["data"],
        json!({"keep":true})
    );
}

#[tokio::test]
async fn data_patch_removes_only_requested_keys_and_patch_values_win() {
    let server = spawn_test_server().await;
    let client = reqwest::Client::new();
    let id = create(
        &client,
        &server.base_url,
        json!({"removed":1,"keep":2,"both":3}),
    )
    .await;
    let before = read(&client, &server.base_url, &id).await;
    let response = client
        .patch(format!("{}/instances/{id}/context/data", server.base_url))
        .header("X-Tenant-Id", "t1")
        .json(&json!({"patch":{"both":4},"remove_keys":["removed","both","absent"]}))
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let after = read(&client, &server.base_url, &id).await;
    assert_eq!(after["context"]["data"], json!({"keep":2,"both":4}));
    for section in ["config", "runtime", "audit"] {
        assert_eq!(after["context"][section], before["context"][section]);
    }
}
