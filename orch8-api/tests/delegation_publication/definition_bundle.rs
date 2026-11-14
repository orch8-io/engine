//! Transactional dependency closure and catalog snapshot regressions.
use super::*;

async fn wait_for_backend_lock(pool: &sqlx::PgPool, backend_pid: i32, seconds: u64) {
    tokio::time::timeout(std::time::Duration::from_secs(seconds), async {
        loop {
            let waiting: bool = sqlx::query_scalar(
                "SELECT EXISTS(SELECT 1 FROM pg_stat_activity WHERE $1=ANY(pg_blocking_pids(pid)))",
            )
            .bind(backend_pid)
            .fetch_one(pool)
            .await
            .unwrap();
            if waiting {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("publisher did not reach its held database lock");
}

#[tokio::test]
async fn postgres_publication_cut_observes_retirement_committed_during_namespace_wait() {
    use orch8_storage::testing::backends::BackendKind;
    for kind in [BackendKind::Postgres, BackendKind::EncryptedPostgres] {
        let Some(b) = TestBackend::open(kind).await else {
            continue;
        };
        let RawPool::Postgres(pool) = &b.raw else {
            unreachable!()
        };
        let server = spawn_test_server_on(b.storage.clone(), Some(ROOT)).await;
        let f = fixture(b.storage.as_ref(), &server, Mode::Remote).await;
        let physical = InstanceId::from_uuid(f.delegation_id());
        let key = serde_json::to_string(&(
            "orch8-continuity-instance",
            f.owner.tenant_id.as_str(),
            physical,
        ))
        .unwrap();
        let mut retirement = pool.begin().await.unwrap();
        sqlx::query("SELECT pg_advisory_xact_lock(hashtextextended($1,0))")
            .bind(&key)
            .execute(&mut *retirement)
            .await
            .unwrap();
        // Model the permanent record after acknowledged native/owner history GC,
        // committing under the same namespace mutex as publication and cleanup.
        sqlx::query("INSERT INTO physical_instance_retirements(instance_id,tenant_id,continuity_id,capsule_id,retired_at) VALUES($1,$2,$3,$4,$5)")
            .bind(physical.into_uuid()).bind(f.owner.tenant_id.as_str()).bind(Uuid::now_v7()).bind(Uuid::now_v7()).bind(Utc::now())
            .execute(&mut *retirement).await.unwrap();
        let blocker: i32 = sqlx::query_scalar("SELECT pg_backend_pid()")
            .fetch_one(&mut *retirement)
            .await
            .unwrap();
        let url = format!("{}/continuity/delegations/claim", server.v1_url());
        let body = f.request.clone();
        let waiting = tokio::spawn(async move {
            reqwest::Client::new()
                .post(url)
                .header("x-api-key", ROOT)
                .json(&body)
                .send()
                .await
                .unwrap()
        });
        wait_for_backend_lock(pool, blocker, 3).await;
        retirement.commit().await.unwrap();
        let response = waiting.await.unwrap();
        assert_eq!(
            response.status(),
            reqwest::StatusCode::CONFLICT,
            "a stale catalog snapshot hid permanent retirement: {}",
            response.text().await.unwrap()
        );
        assert_empty(b.storage.as_ref(), &f).await;
    }
}

async fn unavailable_nested_handler_keeps_grant(b: TestBackend) {
    use orch8_types::sequence::{BlockDefinition, SubSequenceDef};
    let server = spawn_test_server_on(b.storage.clone(), Some(ROOT)).await;
    let f = fixture(b.storage.as_ref(), &server, Mode::Remote).await;
    let mut root = b
        .storage
        .get_sequence(f.sequence_id())
        .await
        .unwrap()
        .unwrap();
    let child = SequenceBuilder::new(f.owner.tenant_id.as_str())
        .step("work", "not-installed-on-destination")
        .build();
    root.blocks
        .push(BlockDefinition::SubSequence(Box::new(SubSequenceDef {
            id: orch8_types::ids::BlockId::new("child"),
            sequence_name: child.name.clone(),
            version: None,
            input: json!({}),
        })));
    b.storage.replace_sequence(root.id, &root).await.unwrap();
    b.storage.create_sequence(&child).await.unwrap();
    assert_eq!(
        claim(&server, &f.request).await.status(),
        reqwest::StatusCode::CONFLICT,
        "the root's handlers hid an unavailable nested handler"
    );
    assert_empty(b.storage.as_ref(), &f).await;
    for table in ["delegation_definitions", "delegation_definition_manifests"] {
        assert_eq!(
            b.raw
                .count(table, "tenant_id", f.owner.tenant_id.as_str())
                .await,
            0
        );
    }
}
orch8_storage::backend_test!(async fn unavailable_nested_handlers_do_not_spend_the_delegation_grant(b){unavailable_nested_handler_keeps_grant(b).await;});

#[tokio::test]
async fn postgres_capture_write_waits_recheck_deadlines_and_rollback_every_body() {
    use orch8_storage::testing::backends::BackendKind;
    for kind in [BackendKind::Postgres, BackendKind::EncryptedPostgres] {
        let Some(b) = TestBackend::open(kind).await else {
            continue;
        };
        let RawPool::Postgres(pool) = &b.raw else {
            unreachable!()
        };
        let server = spawn_test_server_on(b.storage.clone(), Some(ROOT)).await;
        for manifest in [false, true] {
            let mut f = fixture(b.storage.as_ref(), &server, Mode::Remote).await;
            let deadline = Utc::now() + chrono::Duration::seconds(3);
            f.request["delegation"]["expires_at"] = json!(deadline);
            let mut lock = pool.begin().await.unwrap();
            let statement = if manifest {
                "LOCK TABLE delegation_definition_manifests IN ACCESS EXCLUSIVE MODE"
            } else {
                "LOCK TABLE delegation_definitions IN ACCESS EXCLUSIVE MODE"
            };
            sqlx::query(statement).execute(&mut *lock).await.unwrap();
            let blocker: i32 = sqlx::query_scalar("SELECT pg_backend_pid()")
                .fetch_one(&mut *lock)
                .await
                .unwrap();
            let url = format!("{}/continuity/delegations/claim", server.v1_url());
            let body = f.request.clone();
            let waiting = tokio::spawn(async move {
                reqwest::Client::new()
                    .post(url)
                    .header("x-api-key", ROOT)
                    .json(&body)
                    .send()
                    .await
                    .unwrap()
            });
            wait_for_backend_lock(pool, blocker, 2).await;
            tokio::time::sleep(
                (deadline - Utc::now()).to_std().unwrap_or_default()
                    + std::time::Duration::from_millis(40),
            )
            .await;
            lock.commit().await.unwrap();
            assert_eq!(
                waiting.await.unwrap().status(),
                reqwest::StatusCode::CONFLICT
            );
            assert_empty(b.storage.as_ref(), &f).await;
            for table in ["delegation_definitions", "delegation_definition_manifests"] {
                assert_eq!(
                    b.raw
                        .count(table, "tenant_id", f.owner.tenant_id.as_str())
                        .await,
                    0
                );
            }
        }
    }
}

async fn dependency_closure_survives_catalog_changes(b: TestBackend) {
    use orch8_types::sequence::{BlockDefinition, SubSequenceDef};
    let server = spawn_test_server_on(b.storage.clone(), Some(ROOT)).await;
    let f = fixture(b.storage.as_ref(), &server, Mode::Remote).await;
    let mut root = b
        .storage
        .get_sequence(f.sequence_id())
        .await
        .unwrap()
        .unwrap();
    let mut child = SequenceBuilder::new(f.owner.tenant_id.as_str())
        .step("child-work", "noop")
        .build();
    let mut grandchild = SequenceBuilder::new(f.owner.tenant_id.as_str())
        .step("grandchild-work", "noop")
        .build();
    let reference = |id: &str, name: &str, version| {
        BlockDefinition::SubSequence(Box::new(SubSequenceDef {
            id: orch8_types::ids::BlockId::new(id),
            sequence_name: name.into(),
            version,
            input: json!({}),
        }))
    };
    root.blocks.push(reference("child", &child.name, None));
    root.on_failure = Some(vec![reference(
        "cleanup-grandchild",
        &grandchild.name,
        Some(1),
    )]);
    child
        .blocks
        .push(reference("nested", &grandchild.name, Some(1)));
    grandchild
        .blocks
        .push(reference("cycle", &root.name, Some(root.version)));
    b.storage.replace_sequence(root.id, &root).await.unwrap();
    b.storage.create_sequence(&child).await.unwrap();
    b.storage.create_sequence(&grandchild).await.unwrap();
    let publication = accepted(&server, &f.request).await;
    let task: Uuid = serde_json::from_value(publication["mailbox_task_id"].clone()).unwrap();
    let original_child = serde_json::to_value(&child).unwrap();
    let original_grandchild = serde_json::to_value(&grandchild).unwrap();
    child.blocks = vec![orch8_storage::testing::fixtures::step_block(
        "changed", "noop",
    )];
    b.storage.replace_sequence(child.id, &child).await.unwrap();
    b.storage.delete_sequence(grandchild.id).await.unwrap();
    let frozen = b
        .storage
        .get_published_delegation_definition(&f.owner.tenant_id, task)
        .await
        .unwrap()
        .unwrap();
    let body = serde_json::to_value(frozen).unwrap();
    let dependencies = body["dependencies"]
        .as_array()
        .expect("publication must retain its typed dependency closure");
    assert_eq!(dependencies.len(), 2);
    for expected in [original_child, original_grandchild] {
        assert!(dependencies.iter().any(|d| d["definition"] == expected));
    }
    assert_http_bundle_read(
        &b,
        &server,
        &f,
        task,
        body["bundle_sha256"].as_str().unwrap(),
    )
    .await;
}
orch8_storage::backend_test!(async fn published_dependency_closure_survives_catalog_mutation_and_deletion(b){dependency_closure_survives_catalog_changes(b).await;});

async fn assert_http_bundle_read(
    b: &TestBackend,
    server: &BackendTestServer,
    f: &Fixture,
    task: Uuid,
    expected_sha256: &str,
) {
    let runtime: RuntimeId =
        serde_json::from_value(f.request["delegation"]["destination_runtime_id"].clone()).unwrap();
    let capabilities = b
        .storage
        .get_runtime_capability(&f.owner.tenant_id, runtime, Utc::now())
        .await
        .unwrap()
        .unwrap();
    let client = reqwest::Client::new();
    let response = client.post(format!("{}/workers/tasks/poll", server.v1_url())).header("x-api-key",ROOT)
        .json(&json!({"handler_name":"orch8.delegation","worker_id":runtime,"capabilities":capabilities}))
        .send().await.unwrap();
    assert!(response.status().is_success());
    let claimed: Value = response.json().await.unwrap();
    assert_eq!(claimed["tasks"][0]["id"], task.to_string());
    let response = client
        .get(format!(
            "{}/workers/tasks/{task}/definition",
            server.v1_url()
        ))
        .header("x-api-key", ROOT)
        .query(&[
            ("worker_id", runtime.to_string()),
            (
                "claim_epoch",
                claimed["tasks"][0]["claim_epoch"].to_string(),
            ),
        ])
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), reqwest::StatusCode::OK);
    let claim = orch8_types::worker::WorkerClaim::new(
        runtime.to_string(),
        claimed["tasks"][0]["claim_epoch"].as_u64().unwrap(),
    );
    let verified = super::definition::verify_response(b, f, task, &claim, response).await;
    assert_eq!(verified.graph().sha256().unwrap(), expected_sha256);
}

async fn next_request(server: &BackendTestServer, f: &Fixture) -> Value {
    let grant = reqwest::Client::new().post(format!("{}/continuity/grants", server.v1_url()))
        .header("x-api-key", ROOT)
        .json(&json!({"tenant_id":f.owner.tenant_id,"continuity_id":f.owner.continuity_id,
            "destination_runtime_id":f.request["delegation"]["destination_runtime_id"],"allowed_actions":["accept"],"ttl_seconds":300}))
        .send().await.unwrap();
    assert!(grant.status().is_success());
    let grant: Value = grant.json().await.unwrap();
    let mut request = f.request.clone();
    request["delegation"]["id"] = json!(Uuid::now_v7());
    request["delegation"]["grant_id"] = grant["signed_grant"]["grant"]["id"].clone();
    request["signed_grant"] = grant["signed_grant"].clone();
    request["token"] = grant["token"].clone();
    request
}
async fn unpinned_child_identity(b: TestBackend) {
    use orch8_types::sequence::{BlockDefinition, SubSequenceDef};
    let server = spawn_test_server_on(b.storage.clone(), Some(ROOT)).await;
    let f = fixture(b.storage.as_ref(), &server, Mode::Remote).await;
    let mut root = b
        .storage
        .get_sequence(f.sequence_id())
        .await
        .unwrap()
        .unwrap();
    let child = SequenceBuilder::new(f.owner.tenant_id.as_str())
        .step("work", "noop")
        .build();
    root.blocks
        .push(BlockDefinition::SubSequence(Box::new(SubSequenceDef {
            id: orch8_types::ids::BlockId::new("child"),
            sequence_name: child.name.clone(),
            version: None,
            input: json!({}),
        })));
    b.storage.replace_sequence(root.id, &root).await.unwrap();
    b.storage.create_sequence(&child).await.unwrap();
    let first = accepted(&server, &f.request).await;
    let mut newer = child.clone();
    newer.id = SequenceId::new();
    newer.version = 2;
    b.storage.create_sequence(&newer).await.unwrap();
    let second = accepted(&server, &next_request(&server, &f).await).await;
    let mut bundles = Vec::new();
    for published in [first, second] {
        bundles.push(
            b.storage
                .get_published_delegation_definition(
                    &f.owner.tenant_id,
                    serde_json::from_value(published["mailbox_task_id"].clone()).unwrap(),
                )
                .await
                .unwrap()
                .unwrap(),
        );
    }
    assert_eq!(bundles[0].sequence_sha256, bundles[1].sequence_sha256);
    assert_ne!(bundles[0].bundle_sha256, bundles[1].bundle_sha256);
    assert_eq!(bundles[0].dependencies[0].definition.version, 1);
    assert_eq!(bundles[1].dependencies[0].definition.version, 2);
    assert_eq!(
        b.raw
            .count(
                "delegation_definition_manifests",
                "tenant_id",
                f.owner.tenant_id.as_str()
            )
            .await,
        2
    );
    assert_eq!(
        b.raw
            .count(
                "delegation_definitions",
                "tenant_id",
                f.owner.tenant_id.as_str()
            )
            .await,
        3
    );
}
orch8_storage::backend_test!(async fn identical_root_publications_pin_distinct_latest_children(b){unpinned_child_identity(b).await;});

async fn missing_dependency_keeps_grant(b: TestBackend) {
    use orch8_types::sequence::{BlockDefinition, SubSequenceDef};
    let server = spawn_test_server_on(b.storage.clone(), Some(ROOT)).await;
    for foreign_tenant in [true, false] {
        let f = fixture(b.storage.as_ref(), &server, Mode::Remote).await;
        let mut root = b
            .storage
            .get_sequence(f.sequence_id())
            .await
            .unwrap()
            .unwrap();
        let mut child = SequenceBuilder::new(f.owner.tenant_id.as_str())
            .step("work", "noop")
            .build();
        if foreign_tenant {
            child.tenant_id = orch8_types::ids::TenantId::unchecked("another-account");
        } else {
            child.namespace = orch8_types::ids::Namespace::new("another-catalog");
        }
        root.blocks
            .push(BlockDefinition::SubSequence(Box::new(SubSequenceDef {
                id: orch8_types::ids::BlockId::new("child"),
                sequence_name: child.name.clone(),
                version: None,
                input: json!({}),
            })));
        b.storage.replace_sequence(root.id, &root).await.unwrap();
        b.storage.create_sequence(&child).await.unwrap();
        assert!(!claim(&server, &f.request).await.status().is_success());
        assert_empty(b.storage.as_ref(), &f).await;
        for table in ["delegation_definitions", "delegation_definition_manifests"] {
            assert_eq!(
                b.raw
                    .count(table, "tenant_id", f.owner.tenant_id.as_str())
                    .await,
                0
            );
        }
    }
}
orch8_storage::backend_test!(async fn missing_or_foreign_dependencies_leave_the_grant_and_no_bundle(b){missing_dependency_keeps_grant(b).await;});

async fn mutate_catalog(
    b: &TestBackend,
    pool: &sqlx::PgPool,
    child: &orch8_types::sequence::SequenceDefinition,
    change: &str,
) {
    match change {
        "insert" => {
            let mut newer = child.clone();
            newer.id = SequenceId::new();
            newer.version = 2;
            b.storage.create_sequence(&newer).await.unwrap();
        }
        "deprecate" => {
            sqlx::query("UPDATE sequences SET deprecated=true WHERE id=$1")
                .bind(child.id.into_uuid())
                .execute(pool)
                .await
                .unwrap();
        }
        "replace" => {
            let mut replaced = child.clone();
            replaced.blocks = vec![orch8_storage::testing::fixtures::step_block(
                "changed", "noop",
            )];
            b.storage
                .replace_sequence(child.id, &replaced)
                .await
                .unwrap();
        }
        "delete" => {
            b.storage.delete_sequence(child.id).await.unwrap();
        }
        _ => unreachable!("unknown catalog fixture change"),
    }
}

#[tokio::test]
async fn postgres_dependency_selection_uses_one_cut_across_concurrent_catalog_changes() {
    use orch8_storage::testing::backends::BackendKind;
    use orch8_types::sequence::{BlockDefinition, SubSequenceDef};
    for kind in [BackendKind::Postgres, BackendKind::EncryptedPostgres] {
        let Some(b) = TestBackend::open(kind).await else {
            continue;
        };
        let RawPool::Postgres(pool) = &b.raw else {
            unreachable!()
        };
        let server = spawn_test_server_on(b.storage.clone(), Some(ROOT)).await;
        for change in ["insert", "deprecate", "replace", "delete"] {
            let f = fixture(b.storage.as_ref(), &server, Mode::Remote).await;
            let mut root = b
                .storage
                .get_sequence(f.sequence_id())
                .await
                .unwrap()
                .unwrap();
            let child = SequenceBuilder::new(f.owner.tenant_id.as_str())
                .step("work", "noop")
                .build();
            root.blocks
                .push(BlockDefinition::SubSequence(Box::new(SubSequenceDef {
                    id: orch8_types::ids::BlockId::new("child"),
                    sequence_name: child.name.clone(),
                    version: None,
                    input: json!({}),
                })));
            b.storage.replace_sequence(root.id, &root).await.unwrap();
            b.storage.create_sequence(&child).await.unwrap();
            let mut lock = pool.begin().await.unwrap();
            sqlx::query("SELECT id FROM continuation_grants WHERE id=$1 FOR UPDATE")
                .bind(f.grant_id.into_uuid())
                .execute(&mut *lock)
                .await
                .unwrap();
            let blocker: i32 = sqlx::query_scalar("SELECT pg_backend_pid()")
                .fetch_one(&mut *lock)
                .await
                .unwrap();
            let url = format!("{}/continuity/delegations/claim", server.v1_url());
            let body = f.request.clone();
            let waiting = tokio::spawn(async move {
                reqwest::Client::new()
                    .post(url)
                    .header("x-api-key", ROOT)
                    .json(&body)
                    .send()
                    .await
                    .unwrap()
            });
            wait_for_backend_lock(pool, blocker, 3).await;
            mutate_catalog(&b, pool, &child, change).await;
            lock.commit().await.unwrap();
            let response = waiting.await.unwrap();
            assert!(
                response.status().is_success(),
                "{change}: {}",
                response.text().await.unwrap()
            );
            let published: Value = response.json().await.unwrap();
            let frozen = b
                .storage
                .get_published_delegation_definition(
                    &f.owner.tenant_id,
                    serde_json::from_value(published["mailbox_task_id"].clone()).unwrap(),
                )
                .await
                .unwrap()
                .unwrap();
            assert_eq!(
                serde_json::to_value(&frozen.dependencies[0].definition).unwrap(),
                serde_json::to_value(&child).unwrap(),
                "{change}: mixed catalog cuts"
            );
        }
    }
}

async fn decoded_definition_limit_keeps_publication_retryable(b: TestBackend) {
    use orch8_types::sequence::BlockDefinition;
    let server = spawn_test_server_on(b.storage.clone(), Some(ROOT)).await;
    for mode in [Mode::Hosted, Mode::Remote] {
        let f = fixture(b.storage.as_ref(), &server, mode).await;
        let mut definition = b
            .storage
            .get_sequence(f.sequence_id())
            .await
            .unwrap()
            .unwrap();
        let BlockDefinition::Step(step) = &mut definition.blocks[0] else {
            panic!("fixture step")
        };
        step.params = json!({"dense": vec![0; 300_000]});
        assert!(
            serde_json::to_vec(&definition).unwrap().len()
                < orch8_types::delegation_definition::MAX_DEFINITION_BYTES
        );
        b.storage
            .replace_sequence(definition.id, &definition)
            .await
            .unwrap();
        let response = claim(&server, &f.request).await;
        assert_eq!(
            response.status(),
            reqwest::StatusCode::CONFLICT,
            "{}",
            response.text().await.unwrap()
        );
        assert_empty(b.storage.as_ref(), &f).await;
        for table in ["delegation_definitions", "delegation_definition_manifests"] {
            assert_eq!(
                b.raw
                    .count(table, "tenant_id", f.owner.tenant_id.as_str())
                    .await,
                0
            );
        }
        let BlockDefinition::Step(step) = &mut definition.blocks[0] else {
            unreachable!()
        };
        step.params = json!({});
        b.storage
            .replace_sequence(definition.id, &definition)
            .await
            .unwrap();
        let published = accepted(&server, &f.request).await;
        assert_eq!(
            accepted(&server, &f.request).await,
            published,
            "the original unspent grant must remain retryable"
        );
    }
}
orch8_storage::backend_test!(async fn decoded_definition_limits_rollback_publication_and_keep_the_grant_retryable(b){decoded_definition_limit_keeps_publication_retryable(b).await;});

async fn request_input_is_not_a_frozen_definition(b: TestBackend) {
    let server = spawn_test_server_on(b.storage.clone(), Some(ROOT)).await;
    let mut f = fixture(b.storage.as_ref(), &server, Mode::Inline).await;
    f.request["input"]["dense"] = json!(vec![0; 200_000]);
    let published = accepted(&server, &f.request).await;
    let task: Uuid = serde_json::from_value(published["mailbox_task_id"].clone()).unwrap();
    let bundle = b
        .storage
        .get_published_delegation_definition(&f.owner.tenant_id, task)
        .await
        .unwrap()
        .unwrap();
    let payload = std::str::from_utf8(bundle.wire().payload()).unwrap();
    assert!(!payload.contains("DELEGATION_INPUT_CANARY"));
    assert!(!payload.contains("dense"));
    assert_eq!(accepted(&server, &f.request).await, published);
}
orch8_storage::backend_test!(async fn dynamic_request_input_does_not_consume_the_definition_decoded_budget(b){request_input_is_not_a_frozen_definition(b).await;});
