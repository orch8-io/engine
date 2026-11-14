//! Immutable publication resources, rather than claimant assertions, govern leasing.
use super::*;
use orch8_types::delegation_definition::{
    DefinitionGraphLimits, protocol::DelegationProtocolCapabilities,
};
pub(super) fn ready_protocol() -> DelegationProtocolCapabilities {
    DelegationProtocolCapabilities::v1(
        DefinitionGraphLimits::default(),
        4 * 1024 * 1024,
        16 * 1024 * 1024,
    )
    .unwrap()
}
pub(super) async fn poll(
    server: &BackendTestServer,
    f: &Fixture,
    capabilities: &RuntimeCapabilities,
) -> Value {
    let response=reqwest::Client::new().post(format!("{}/workers/tasks/poll",server.v1_url())).header("x-api-key",ROOT)
        .json(&json!({"tenant_id":f.owner.tenant_id,"handler_name":"orch8.delegation","worker_id":capabilities.runtime_id,"capabilities":capabilities,"limit":1})).send().await.unwrap();
    assert!(
        response.status().is_success(),
        "{}",
        response.text().await.unwrap()
    );
    response.json().await.unwrap()
}
async fn incompatible(b: TestBackend) {
    let server = spawn_test_server_on(b.storage.clone(), Some(ROOT)).await;
    for case in 0..7 {
        let f = fixture(b.storage.as_ref(), &server, Mode::Remote).await;
        if case >= 5 {
            add_dependency(&b, &f).await;
        }
        let published = accepted(&server, &f.request).await;
        let task: Uuid = serde_json::from_value(published["mailbox_task_id"].clone()).unwrap();
        let runtime: RuntimeId =
            serde_json::from_value(f.request["delegation"]["destination_runtime_id"].clone())
                .unwrap();
        let mut caps = b
            .storage
            .get_runtime_capability(&f.owner.tenant_id, runtime, Utc::now())
            .await
            .unwrap()
            .unwrap();
        let mut protocol = ready_protocol();
        match case {
            0 => {}
            1 => protocol.max_definition_payload_bytes = 1,
            2 => protocol.max_definition_expanded_bytes = 1,
            3 => protocol.max_input_payload_bytes = 1,
            4 => protocol.max_input_expanded_bytes = 1,
            5 => protocol.max_definitions = 1,
            6 => protocol.max_selectors = 0,
            _ => unreachable!(),
        }
        caps.delegation_protocol = if case == 0 { None } else { Some(protocol) };
        let response = poll(&server, &f, &caps).await;
        assert!(
            response["tasks"].as_array().unwrap().is_empty(),
            "case {case}: incompatible worker acquired delegated work"
        );
        let pending = b.storage.get_worker_task(task).await.unwrap().unwrap();
        assert_eq!(pending.state, orch8_types::worker::WorkerTaskState::Pending);
        assert_eq!(pending.claim_epoch, 0);
        assert!(
            b.storage
                .get_worker_issued_claim(
                    Some(&f.owner.tenant_id),
                    task,
                    &orch8_types::worker::WorkerClaim::new(runtime.to_string(), 1)
                )
                .await
                .unwrap()
                .is_none()
        );
        caps.delegation_protocol = Some(exact_protocol(&b, &f, task).await);
        assert_eq!(
            poll(&server, &f, &caps).await["tasks"][0]["id"],
            published["mailbox_task_id"]
        );
    }
}
orch8_storage::backend_test!(async fn unsupported_or_undersized_delegation_protocol_never_issues_a_lease(b){incompatible(b).await;});

async fn legacy_paths(b: TestBackend) {
    let server = spawn_test_server_on(b.storage.clone(), Some(ROOT)).await;
    let f = fixture(b.storage.as_ref(), &server, Mode::Remote).await;
    let published = accepted(&server, &f.request).await;
    let task: Uuid = serde_json::from_value(published["mailbox_task_id"].clone()).unwrap();
    // Model mutable legacy requirements; publication identity must still govern.
    b.raw
        .set_json(
            "worker_tasks",
            "requirements",
            "id",
            &task.to_string(),
            &json!({}),
        )
        .await;
    match &b.raw {
        RawPool::Sqlite(pool) => {
            sqlx::query("UPDATE worker_tasks SET queue_name='legacy-protocol' WHERE id=?1")
                .bind(task.to_string())
                .execute(pool)
                .await
                .unwrap();
        }
        RawPool::Postgres(pool) => {
            sqlx::query("UPDATE worker_tasks SET queue_name='legacy-protocol' WHERE id=$1")
                .bind(task)
                .execute(pool)
                .await
                .unwrap();
        }
    }
    let s = b.storage.as_ref();
    for mode in 0..4 {
        let claims = match mode {
            0 => s.claim_worker_tasks("orch8.delegation", "legacy", 1).await,
            1 => {
                s.claim_worker_tasks_for_tenant("orch8.delegation", "legacy", &f.owner.tenant_id, 1)
                    .await
            }
            2 => {
                s.claim_worker_tasks_from_queue("legacy-protocol", "orch8.delegation", "legacy", 1)
                    .await
            }
            _ => {
                s.claim_worker_tasks_from_queue_for_tenant(
                    "legacy-protocol",
                    "orch8.delegation",
                    "legacy",
                    &f.owner.tenant_id,
                    1,
                )
                .await
            }
        }
        .unwrap();
        assert!(
            claims.is_empty(),
            "legacy path {mode} leased publication work"
        );
    }
    assert_eq!(
        b.storage
            .get_worker_task(task)
            .await
            .unwrap()
            .unwrap()
            .claim_epoch,
        0
    );
}
orch8_storage::backend_test!(async fn legacy_claim_selectors_cannot_bypass_the_publication_protocol(b){legacy_paths(b).await;});

async fn missing_history_backlog(b: TestBackend) {
    let server = spawn_test_server_on(b.storage.clone(), Some(ROOT)).await;
    let f = fixture(b.storage.as_ref(), &server, Mode::Remote).await;
    let sequence = SequenceBuilder::new(f.owner.tenant_id.as_str())
        .step("work", "orch8.delegation")
        .build();
    b.storage.create_sequence(&sequence).await.unwrap();
    let parent = InstanceBuilder::for_sequence(&sequence).due_now().build();
    b.storage.create_instance(&parent).await.unwrap();
    for index in 0..4097 {
        let task:orch8_types::worker::WorkerTask=serde_json::from_value(json!({"id":Uuid::now_v7(),"instance_id":parent.id,"block_id":format!("historical-{index}"),"handler_name":"orch8.delegation","params":{},"context":{},"state":"pending","attempt":0,"created_at":Utc::now()-chrono::Duration::hours(1)})).unwrap();
        b.storage.create_worker_task(&task).await.unwrap();
    }
    let published = accepted(&server, &f.request).await;
    let runtime: RuntimeId =
        serde_json::from_value(f.request["delegation"]["destination_runtime_id"].clone()).unwrap();
    let caps = b
        .storage
        .get_runtime_capability(&f.owner.tenant_id, runtime, Utc::now())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        poll(&server, &f, &caps).await["tasks"][0]["id"],
        published["mailbox_task_id"],
        "uncertified history consumed the bounded scan"
    );
    assert_eq!(
        b.raw
            .count(
                "worker_issued_claims",
                "instance_id",
                &parent.id.to_string()
            )
            .await,
        0
    );
}
orch8_storage::backend_test!(async fn uncertified_delegation_backlog_cannot_starve_a_supported_publication(b){missing_history_backlog(b).await;});

async fn add_dependency(b: &TestBackend, f: &Fixture) {
    use orch8_types::sequence::{BlockDefinition, SubSequenceDef};
    let child = SequenceBuilder::new(f.owner.tenant_id.as_str())
        .step("nested", "noop")
        .build();
    b.storage.create_sequence(&child).await.unwrap();
    let mut root = b
        .storage
        .get_sequence(f.sequence_id())
        .await
        .unwrap()
        .unwrap();
    root.blocks
        .push(BlockDefinition::SubSequence(Box::new(SubSequenceDef {
            id: orch8_types::ids::BlockId::new("child"),
            sequence_name: child.name,
            version: None,
            input: json!({}),
        })));
    b.storage.replace_sequence(root.id, &root).await.unwrap();
}
async fn exact_protocol(
    b: &TestBackend,
    f: &Fixture,
    task: Uuid,
) -> DelegationProtocolCapabilities {
    let graph = b
        .storage
        .get_published_delegation_definition(&f.owner.tenant_id, task)
        .await
        .unwrap()
        .unwrap();
    let input = b
        .storage
        .get_published_delegation_input(&f.owner.tenant_id, task)
        .await
        .unwrap()
        .unwrap();
    let wire = graph.wire().reference();
    let mut protocol = ready_protocol();
    protocol.max_definition_payload_bytes = wire.payload_bytes;
    protocol.max_definition_expanded_bytes = graph.wire().expanded_bytes();
    protocol.max_definitions = wire.definition_count;
    protocol.max_selectors = wire.selector_count;
    protocol.max_input_payload_bytes = input.reference().payload_bytes;
    protocol.max_input_expanded_bytes = input.reference().expanded_bytes;
    protocol
}

async fn destination_preflight(b: TestBackend) {
    let server = spawn_test_server_on(b.storage.clone(), Some(ROOT)).await;
    for missing in [false, true] {
        let f = fixture(b.storage.as_ref(), &server, Mode::Remote).await;
        let runtime: RuntimeId =
            serde_json::from_value(f.request["delegation"]["destination_runtime_id"].clone())
                .unwrap();
        let mut caps = b
            .storage
            .get_runtime_capability(&f.owner.tenant_id, runtime, Utc::now())
            .await
            .unwrap()
            .unwrap();
        let mut protocol = ready_protocol();
        protocol.max_input_payload_bytes = 1;
        caps.delegation_protocol = if missing { None } else { Some(protocol) };
        b.storage
            .upsert_runtime_capabilities(&f.owner.tenant_id, &caps)
            .await
            .unwrap();
        assert_eq!(
            claim(&server, &f.request).await.status(),
            reqwest::StatusCode::CONFLICT
        );
        assert_empty(b.storage.as_ref(), &f).await;
        for table in [
            "delegation_definitions",
            "delegation_definition_manifests",
            "delegation_publication_inputs",
        ] {
            assert_eq!(
                b.raw
                    .count(table, "tenant_id", f.owner.tenant_id.as_str())
                    .await,
                0
            );
        }
        caps.delegation_protocol = Some(ready_protocol());
        b.storage
            .upsert_runtime_capabilities(&f.owner.tenant_id, &caps)
            .await
            .unwrap();
        accepted(&server, &f.request).await;
    }
}
orch8_storage::backend_test!(async fn unsupported_destinations_do_not_spend_grants_or_retain_partial_publications(b){destination_preflight(b).await;});

async fn no_reconstruction(b: TestBackend) {
    let server = spawn_test_server_on(b.storage.clone(), Some(ROOT)).await;
    let f = fixture(b.storage.as_ref(), &server, Mode::Remote).await;
    let published = accepted(&server, &f.request).await;
    sql(&b,match &b.raw {
        RawPool::Sqlite(_)=>"DROP TRIGGER delegation_publication_input_delete; DELETE FROM delegation_publication_inputs;",
        RawPool::Postgres(_)=>"DROP TRIGGER delegation_publication_input_immutable ON delegation_publication_inputs; DELETE FROM delegation_publication_inputs;",
    }).await;
    let runtime: RuntimeId =
        serde_json::from_value(f.request["delegation"]["destination_runtime_id"].clone()).unwrap();
    let caps = b
        .storage
        .get_runtime_capability(&f.owner.tenant_id, runtime, Utc::now())
        .await
        .unwrap()
        .unwrap();
    assert!(
        poll(&server, &f, &caps).await["tasks"]
            .as_array()
            .unwrap()
            .is_empty()
    );
    assert_eq!(accepted(&server, &f.request).await, published);
    assert_eq!(
        b.raw
            .count(
                "delegation_publication_inputs",
                "tenant_id",
                f.owner.tenant_id.as_str()
            )
            .await,
        0
    );
    assert!(
        poll(&server, &f, &caps).await["tasks"]
            .as_array()
            .unwrap()
            .is_empty()
    );
}
orch8_storage::backend_test!(async fn old_publications_cannot_reconstruct_missing_input_to_gain_a_lease(b){no_reconstruction(b).await;});

async fn expired_publication(b: TestBackend) {
    let server = spawn_test_server_on(b.storage.clone(), Some(ROOT)).await;
    let mut f = fixture(b.storage.as_ref(), &server, Mode::Remote).await;
    f.request["delegation"]["expires_at"] = json!(Utc::now() + chrono::Duration::seconds(2));
    let published = accepted(&server, &f.request).await;
    tokio::time::sleep(std::time::Duration::from_secs(3)).await;
    let runtime: RuntimeId =
        serde_json::from_value(f.request["delegation"]["destination_runtime_id"].clone()).unwrap();
    let caps = b
        .storage
        .get_runtime_capability(&f.owner.tenant_id, runtime, Utc::now())
        .await
        .unwrap()
        .unwrap();
    assert!(
        poll(&server, &f, &caps).await["tasks"]
            .as_array()
            .unwrap()
            .is_empty()
    );
    let task: Uuid = serde_json::from_value(published["mailbox_task_id"].clone()).unwrap();
    assert_eq!(
        b.storage
            .get_worker_task(task)
            .await
            .unwrap()
            .unwrap()
            .claim_epoch,
        0
    );
}
orch8_storage::backend_test!(async fn expired_publications_are_filtered_before_worker_selection(b){expired_publication(b).await;});
