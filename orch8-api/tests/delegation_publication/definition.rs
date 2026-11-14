use super::*;

async fn definition_is_frozen(b: TestBackend) {
    let server = spawn_test_server_on(b.storage.clone(), Some(ROOT)).await;
    for mode in [Mode::Remote, Mode::Hosted, Mode::Inline] {
        assert_frozen(&b, &server, mode).await;
    }
}
async fn assert_frozen(b: &TestBackend, server: &BackendTestServer, mode: Mode) {
    let (f, task, claim) = super::outcome::held_mailbox(b, server, mode).await;
    let original = b
        .storage
        .get_sequence(f.sequence_id())
        .await
        .unwrap()
        .unwrap();
    let mut changed = serde_json::to_value(&original).unwrap();
    changed["blocks"][0]["params"] = json!({"changed_after_publication":true});
    match &b.raw {
        RawPool::Sqlite(_) => {
            b.raw
                .set_json(
                    "sequences",
                    "blocks",
                    "id",
                    &f.sequence_id().to_string(),
                    &changed["blocks"],
                )
                .await;
        }
        RawPool::Postgres(_) => {
            b.raw
                .set_json(
                    "sequences",
                    "definition",
                    "id",
                    &f.sequence_id().to_string(),
                    &changed,
                )
                .await;
        }
    }
    assert_ne!(
        serde_json::to_value(
            b.storage
                .get_sequence(f.sequence_id())
                .await
                .unwrap()
                .unwrap()
        )
        .unwrap(),
        serde_json::to_value(&original).unwrap()
    );
    let response = reqwest::Client::new()
        .get(format!(
            "{}/workers/tasks/{task}/definition",
            server.v1_url()
        ))
        .header("x-api-key", ROOT)
        .query(&[
            ("worker_id", claim.worker_id.clone()),
            ("claim_epoch", claim.claim_epoch.to_string()),
        ])
        .send()
        .await
        .unwrap();
    assert_eq!(
        response.status(),
        reqwest::StatusCode::OK,
        "{}",
        response.text().await.unwrap()
    );
    let verified = verify_response(b, &f, task, &claim, response).await;
    assert_eq!(
        serde_json::to_value(&verified.root().definition).unwrap(),
        serde_json::to_value(original).unwrap(),
        "a claimed delegation used mutable catalog content"
    );
}
orch8_storage::backend_test!(async fn claimed_delegation_reads_its_original_frozen_definition(b){definition_is_frozen(b).await;});

async fn read(
    server: &BackendTestServer,
    id: Uuid,
    claim: &orch8_types::worker::WorkerClaim,
    key: &str,
) -> reqwest::Response {
    reqwest::Client::new()
        .get(format!("{}/workers/tasks/{id}/definition", server.v1_url()))
        .header("x-api-key", key)
        .query(&[
            ("worker_id", claim.worker_id.clone()),
            ("claim_epoch", claim.claim_epoch.to_string()),
        ])
        .send()
        .await
        .unwrap()
}
pub(super) async fn verify_response(
    b: &TestBackend,
    f: &Fixture,
    task: Uuid,
    claim: &orch8_types::worker::WorkerClaim,
    response: reqwest::Response,
) -> orch8_types::delegation_definition::wire::VerifiedDefinitionGraph {
    use orch8_types::delegation_definition::{
        DefinitionGraphLimits,
        wire::{DefinitionReadScope, DefinitionWireEnvelope},
    };
    let expected = b
        .storage
        .get_published_delegation_definition(&f.owner.tenant_id, task)
        .await
        .unwrap()
        .unwrap();
    let scope = DefinitionReadScope {
        tenant_id: f.owner.tenant_id.clone(),
        task_id: task,
        worker_id: claim.worker_id.clone(),
        claim_epoch: claim.claim_epoch,
    };
    let envelope: DefinitionWireEnvelope = response.json().await.unwrap();
    envelope
        .verify(
            &scope,
            expected.wire().reference(),
            DefinitionGraphLimits::default(),
        )
        .unwrap()
}

async fn reader_fences(b: TestBackend) {
    use orch8_types::{
        api_key::{ApiCapability, mint_scoped},
        worker::WorkerClaim,
    };
    let server = spawn_test_server_on(b.storage.clone(), Some(ROOT)).await;
    let (f, task, claim) = super::outcome::held_mailbox(&b, &server, Mode::Remote).await;
    let good = read(&server, task, &claim, ROOT).await;
    assert_eq!(good.status(), reqwest::StatusCode::OK);
    assert_eq!(good.headers()["cache-control"], "no-store");
    for (tenant, status) in [
        (f.owner.tenant_id.as_str(), reqwest::StatusCode::FORBIDDEN),
        ("other-tenant", reqwest::StatusCode::NOT_FOUND),
    ] {
        let key = mint_scoped(tenant, "definition peer", None, vec![ApiCapability::Worker]);
        b.storage.create_api_key(&key.record).await.unwrap();
        assert_eq!(
            read(&server, task, &claim, &key.secret).await.status(),
            status
        );
    }
    for wrong in [
        WorkerClaim::new("another-worker", claim.claim_epoch),
        WorkerClaim::new(&claim.worker_id, claim.claim_epoch + 1),
    ] {
        assert_eq!(
            read(&server, task, &wrong, ROOT).await.status(),
            reqwest::StatusCode::CONFLICT
        );
    }
    let current = b
        .storage
        .get_continuity_execution_by_instance(&f.owner.tenant_id, f.anchor())
        .await
        .unwrap()
        .unwrap();
    let moved = ContinuityExecution {
        epoch: current.epoch.checked_next().unwrap(),
        owner_runtime_id: RuntimeId::new(),
        updated_at: Utc::now(),
        ..current.clone()
    };
    assert!(
        b.storage
            .cas_continuity_owner(
                &current.tenant_id,
                current.continuity_id,
                current.epoch,
                current.owner_runtime_id,
                &moved
            )
            .await
            .unwrap()
    );
    assert_eq!(
        read(&server, task, &claim, ROOT).await.status(),
        reqwest::StatusCode::CONFLICT
    );
}
orch8_storage::backend_test!(async fn frozen_definition_reads_require_the_original_claim_and_current_owner(b){reader_fences(b).await;});

async fn definition_survives_catalog_gc(b: TestBackend) {
    let server = spawn_test_server_on(b.storage.clone(), Some(ROOT)).await;
    // Hosted parent uses a separate sequence, so the delegated catalog row can be collected.
    let (f, task, claim) = super::outcome::held_mailbox(&b, &server, Mode::Hosted).await;
    let original = b
        .storage
        .get_sequence(f.sequence_id())
        .await
        .unwrap()
        .unwrap();
    sql(
        &b,
        &format!("DELETE FROM sequences WHERE id='{}'", f.sequence_id()),
    )
    .await;
    assert!(
        b.storage
            .get_sequence(f.sequence_id())
            .await
            .unwrap()
            .is_none()
    );
    let response = read(&server, task, &claim, ROOT).await;
    assert_eq!(
        response.status(),
        reqwest::StatusCode::OK,
        "{}",
        response.text().await.unwrap()
    );
    let verified = verify_response(&b, &f, task, &claim, response).await;
    assert_eq!(
        serde_json::to_value(&verified.root().definition).unwrap(),
        serde_json::to_value(original).unwrap()
    );
    b.storage
        .update_instance_state(f.anchor(), InstanceState::Cancelled, None)
        .await
        .unwrap();
    assert_eq!(
        read(&server, task, &claim, ROOT).await.status(),
        reqwest::StatusCode::CONFLICT
    );
}
orch8_storage::backend_test!(async fn frozen_definition_survives_catalog_gc_but_terminal_parents_cannot_read(b){definition_survives_catalog_gc(b).await;});

async fn snapshot_is_immutable(b: TestBackend) {
    let server = spawn_test_server_on(b.storage.clone(), Some(ROOT)).await;
    let (f, task, _) = super::outcome::held_mailbox(&b, &server, Mode::Remote).await;
    let frozen = b
        .storage
        .get_published_delegation_definition(&f.owner.tenant_id, task)
        .await
        .unwrap()
        .unwrap();
    match &b.raw {
        RawPool::Postgres(pool) => {
            assert!(sqlx::query("UPDATE delegation_definition_manifests SET manifest=manifest WHERE tenant_id=$1").bind(f.owner.tenant_id.as_str()).execute(pool).await.is_err());
            assert!(
                sqlx::query("DELETE FROM delegation_definition_manifests WHERE tenant_id=$1")
                    .bind(f.owner.tenant_id.as_str())
                    .execute(pool)
                    .await
                    .is_err()
            );
            assert!(
                sqlx::query(
                    "UPDATE delegation_definitions SET definition=definition WHERE tenant_id=$1"
                )
                .bind(f.owner.tenant_id.as_str())
                .execute(pool)
                .await
                .is_err()
            );
            assert!(
                sqlx::query("DELETE FROM delegation_definitions WHERE tenant_id=$1")
                    .bind(f.owner.tenant_id.as_str())
                    .execute(pool)
                    .await
                    .is_err()
            );
            let mut tx = pool.begin().await.unwrap();
            assert!(
                sqlx::raw_sql(include_str!(
                    "../../../migrations/down/144_delegation_definitions.sql"
                ))
                .execute(&mut *tx)
                .await
                .is_err()
            );
            tx.rollback().await.unwrap();
        }
        RawPool::Sqlite(pool) => {
            assert!(sqlx::query("UPDATE delegation_definition_manifests SET manifest=manifest WHERE tenant_id=?1").bind(f.owner.tenant_id.as_str()).execute(pool).await.is_err());
            assert!(
                sqlx::query("DELETE FROM delegation_definition_manifests WHERE tenant_id=?1")
                    .bind(f.owner.tenant_id.as_str())
                    .execute(pool)
                    .await
                    .is_err()
            );
            assert!(
                sqlx::query(
                    "UPDATE delegation_definitions SET definition=definition WHERE tenant_id=?1"
                )
                .bind(f.owner.tenant_id.as_str())
                .execute(pool)
                .await
                .is_err()
            );
            assert!(
                sqlx::query("DELETE FROM delegation_definitions WHERE tenant_id=?1")
                    .bind(f.owner.tenant_id.as_str())
                    .execute(pool)
                    .await
                    .is_err()
            );
        }
    }
    assert_eq!(
        b.storage
            .get_published_delegation_definition(&f.owner.tenant_id, task)
            .await
            .unwrap()
            .unwrap()
            .sequence_sha256,
        frozen.sequence_sha256
    );
    assert!(
        b.storage
            .get_published_delegation_definition(
                &orch8_types::ids::TenantId::new("foreign").unwrap(),
                task
            )
            .await
            .unwrap()
            .is_none()
    );
}
orch8_storage::backend_test!(async fn frozen_definitions_are_scope_bound_immutable_and_downgrade_protected(b){snapshot_is_immutable(b).await;});

async fn key_lease(
    b: &TestBackend,
    server: &BackendTestServer,
) -> (
    Fixture,
    Uuid,
    orch8_types::worker::WorkerClaim,
    orch8_types::api_key::NewApiKey,
) {
    use orch8_types::api_key::{ApiCapability, mint_scoped};
    let f = fixture(b.storage.as_ref(), server, Mode::Remote).await;
    let reply = accepted(server, &f.request).await;
    let task: Uuid = serde_json::from_value(reply["mailbox_task_id"].clone()).unwrap();
    let key = mint_scoped(
        f.owner.tenant_id.as_str(),
        "definition owner",
        None,
        vec![ApiCapability::Worker],
    );
    b.storage.create_api_key(&key.record).await.unwrap();
    let runtime: RuntimeId =
        serde_json::from_value(f.request["delegation"]["destination_runtime_id"].clone()).unwrap();
    let caps = b
        .storage
        .get_runtime_capability(&f.owner.tenant_id, runtime, Utc::now())
        .await
        .unwrap()
        .unwrap();
    let response = reqwest::Client::new()
        .post(format!("{}/workers/tasks/poll", server.v1_url()))
        .header("x-api-key", &key.secret)
        .json(&json!({"handler_name":"orch8.delegation","worker_id":runtime,"capabilities":caps}))
        .send()
        .await
        .unwrap();
    assert!(
        response.status().is_success(),
        "{}",
        response.text().await.unwrap()
    );
    let body: Value = response.json().await.unwrap();
    let claim = orch8_types::worker::WorkerClaim::new(
        runtime.to_string(),
        body["tasks"][0]["claim_epoch"].as_u64().unwrap(),
    );
    (f, task, claim, key)
}

async fn key_owned_read(b: TestBackend) {
    let server = spawn_test_server_on(b.storage.clone(), Some(ROOT)).await;
    let (_, task, claim, key) = key_lease(&b, &server).await;
    assert_eq!(
        read(&server, task, &claim, &key.secret).await.status(),
        reqwest::StatusCode::OK
    );
    assert_eq!(
        read(&server, task, &claim, ROOT).await.status(),
        reqwest::StatusCode::FORBIDDEN,
        "root must not impersonate another recorded reporter"
    );
    assert!(b.storage.revoke_api_key(&key.record.id).await.unwrap());
    assert_eq!(
        read(&server, task, &claim, &key.secret).await.status(),
        reqwest::StatusCode::UNAUTHORIZED
    );
}
orch8_storage::backend_test!(async fn only_the_original_valid_worker_key_can_read_its_frozen_definition(b){key_owned_read(b).await;});

async fn missing_snapshot_has_no_live_fallback(b: TestBackend) {
    let server = spawn_test_server_on(b.storage.clone(), Some(ROOT)).await;
    let (f, task, claim) = super::outcome::held_mailbox(&b, &server, Mode::Remote).await;
    // Model an older publication: the live catalog still matches its retained hash,
    // but no historical frozen body was certified by that publication transaction.
    match &b.raw {
        RawPool::Postgres(pool) => {
            sqlx::raw_sql(
                "DROP TRIGGER delegation_publication_input_immutable ON delegation_publication_inputs; DELETE FROM delegation_publication_inputs; DROP TRIGGER delegation_definition_manifest_immutable ON delegation_definition_manifests; DELETE FROM delegation_definition_manifests; DROP TRIGGER delegation_definition_immutable ON delegation_definitions;",
            )
            .execute(pool)
            .await
            .unwrap();
            sqlx::query("DELETE FROM delegation_definitions WHERE tenant_id=$1")
                .bind(f.owner.tenant_id.as_str())
                .execute(pool)
                .await
                .unwrap();
        }
        RawPool::Sqlite(pool) => {
            sqlx::raw_sql("DROP TRIGGER delegation_publication_input_delete; DELETE FROM delegation_publication_inputs; DROP TRIGGER delegation_definition_manifest_delete; DELETE FROM delegation_definition_manifests; DROP TRIGGER delegation_definition_delete;")
                .execute(pool)
                .await
                .unwrap();
            sqlx::query("DELETE FROM delegation_definitions WHERE tenant_id=?1")
                .bind(f.owner.tenant_id.as_str())
                .execute(pool)
                .await
                .unwrap();
        }
    }
    for _ in 0..2 {
        assert_eq!(
            read(&server, task, &claim, ROOT).await.status(),
            reqwest::StatusCode::CONFLICT
        );
        assert_eq!(
            b.raw
                .count(
                    "delegation_definitions",
                    "tenant_id",
                    f.owner.tenant_id.as_str()
                )
                .await,
            0
        );
    }
}
orch8_storage::backend_test!(async fn older_publications_never_certify_a_replacement_live_definition(b){missing_snapshot_has_no_live_fallback(b).await;});

async fn partial_bundle_has_no_fallback(b: TestBackend) {
    let server = spawn_test_server_on(b.storage.clone(), Some(ROOT)).await;
    for missing_body in [false, true] {
        let (f, task, claim) = super::outcome::held_mailbox(&b, &server, Mode::Remote).await;
        let remove = match (&b.raw, missing_body) {
            (RawPool::Postgres(_), false) => {
                "DROP TRIGGER delegation_publication_input_immutable ON delegation_publication_inputs; DELETE FROM delegation_publication_inputs; DROP TRIGGER delegation_definition_manifest_immutable ON delegation_definition_manifests; DELETE FROM delegation_definition_manifests;"
            }
            (RawPool::Postgres(_), true) => {
                "DROP TRIGGER delegation_definition_immutable ON delegation_definitions; DELETE FROM delegation_definitions;"
            }
            (RawPool::Sqlite(_), false) => {
                "DROP TRIGGER delegation_publication_input_delete; DELETE FROM delegation_publication_inputs; DROP TRIGGER delegation_definition_manifest_delete; DELETE FROM delegation_definition_manifests;"
            }
            (RawPool::Sqlite(_), true) => {
                "DROP TRIGGER delegation_definition_delete; DELETE FROM delegation_definitions;"
            }
        };
        sql(&b, remove).await;
        assert_eq!(
            read(&server, task, &claim, ROOT).await.status(),
            if missing_body {
                reqwest::StatusCode::INTERNAL_SERVER_ERROR
            } else {
                reqwest::StatusCode::CONFLICT
            }
        );
        assert_eq!(
            b.raw
                .count(
                    "delegation_definitions",
                    "tenant_id",
                    f.owner.tenant_id.as_str()
                )
                .await,
            i64::from(!missing_body)
        );
        assert_eq!(
            b.raw
                .count(
                    "delegation_definition_manifests",
                    "tenant_id",
                    f.owner.tenant_id.as_str()
                )
                .await,
            i64::from(missing_body)
        );
    }
}
orch8_storage::backend_test!(async fn partial_definition_bundles_cannot_reconstruct_missing_evidence_from_catalog(b){partial_bundle_has_no_fallback(b).await;});

pub(super) async fn authority_change_during_read(b: &TestBackend, kind: &str, resource: &str) {
    let (storage, faults) = orch8_storage::testing::faulty::FaultStorage::wrap(b.storage.clone());
    let server = spawn_test_server_on(storage, Some(ROOT)).await;
    let (_, task, claim, key) = key_lease(b, &server).await;
    let guard = faults
        .on(if resource == "input" {
            "get_published_delegation_input"
        } else {
            "get_published_delegation_definition"
        })
        .times(1)
        .pause();
    let url = format!("{}/workers/tasks/{task}/{resource}", server.v1_url());
    let secret = key.secret.clone();
    let request = tokio::spawn(async move {
        reqwest::Client::new()
            .get(url)
            .header("x-api-key", secret)
            .query(&[
                ("worker_id", claim.worker_id),
                ("claim_epoch", claim.claim_epoch.to_string()),
            ])
            .send()
            .await
            .unwrap()
    });
    tokio::time::timeout(std::time::Duration::from_secs(5), guard.entered())
        .await
        .unwrap();
    match kind {
        "revoke" => assert!(b.storage.revoke_api_key(&key.record.id).await.unwrap()),
        "expire" => {
            b.raw
                .backdate(
                    "api_keys",
                    "expires_at",
                    "id",
                    &key.record.id,
                    Utc::now() - chrono::Duration::seconds(1),
                )
                .await;
        }
        "downscope" => {
            b.raw
                .set_json(
                    "api_keys",
                    "capabilities_json",
                    "id",
                    &key.record.id,
                    &json!(["publisher"]),
                )
                .await;
        }
        _ => unreachable!(),
    }
    guard.release();
    let response = tokio::time::timeout(std::time::Duration::from_secs(5), request)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        response.status(),
        if kind == "downscope" {
            reqwest::StatusCode::FORBIDDEN
        } else {
            reqwest::StatusCode::UNAUTHORIZED
        },
        "revoked authority disclosed a frozen workflow after storage wait"
    );
}

async fn revoked_during_read(b: TestBackend) {
    for kind in ["revoke", "expire", "downscope"] {
        authority_change_during_read(&b, kind, "definition").await;
    }
}
orch8_storage::backend_test!(async fn definition_read_rechecks_revocation_after_waiting_for_storage(b){revoked_during_read(b).await;});

async fn shared_definition_ignores_task_inputs(b: TestBackend) {
    let server = spawn_test_server_on(b.storage.clone(), Some(ROOT)).await;
    let f = fixture(b.storage.as_ref(), &server, Mode::Remote).await;
    let first = accepted(&server, &f.request).await;
    let catalog = b
        .storage
        .get_sequence(f.sequence_id())
        .await
        .unwrap()
        .unwrap();
    // Creation time is excluded from the semantic fingerprint, but remains in
    // the retained document bytes. A later publication must pin the shared row.
    b.raw
        .backdate(
            "sequences",
            "created_at",
            "id",
            &f.sequence_id().to_string(),
            catalog.created_at - chrono::Duration::days(1),
        )
        .await;
    let mut second = f.request.clone();
    let grant=reqwest::Client::new().post(format!("{}/continuity/grants",server.v1_url())).header("x-api-key",ROOT)
        .json(&json!({"tenant_id":f.owner.tenant_id,"continuity_id":f.owner.continuity_id,"destination_runtime_id":f.request["delegation"]["destination_runtime_id"],"allowed_actions":["accept"],"ttl_seconds":300})).send().await.unwrap();
    assert!(grant.status().is_success());
    let grant: Value = grant.json().await.unwrap();
    second["delegation"]["id"] = json!(Uuid::now_v7());
    second["delegation"]["grant_id"] = grant["signed_grant"]["grant"]["id"].clone();
    second["signed_grant"] = grant["signed_grant"].clone();
    second["token"] = grant["token"].clone();
    second["input"] = json!({"private":"SECOND_TASK_INPUT_CANARY"});
    let second = accepted(&server, &second).await;
    assert_ne!(first["mailbox_task_id"], second["mailbox_task_id"]);
    assert_eq!(
        b.raw
            .count(
                "delegation_definitions",
                "tenant_id",
                f.owner.tenant_id.as_str()
            )
            .await,
        1
    );
    let mut hashes = Vec::new();
    let mut wire_references = Vec::new();
    for task in [
        first["mailbox_task_id"].clone(),
        second["mailbox_task_id"].clone(),
    ] {
        let frozen = b
            .storage
            .get_published_delegation_definition(
                &f.owner.tenant_id,
                serde_json::from_value(task).unwrap(),
            )
            .await
            .unwrap()
            .unwrap();
        let text = serde_json::to_string(&frozen).unwrap();
        assert!(!text.contains("TASK_INPUT_CANARY") && !text.contains("DELEGATION_INPUT_CANARY"));
        let payload = std::str::from_utf8(frozen.wire().payload()).unwrap();
        assert!(
            !payload.contains("TASK_INPUT_CANARY") && !payload.contains("DELEGATION_INPUT_CANARY")
        );
        wire_references.push(frozen.wire().reference().clone());
        hashes.push(frozen.sequence_sha256);
    }
    assert_eq!(hashes[0], hashes[1]);
    assert_eq!(
        wire_references[0], wire_references[1],
        "catalog metadata changes must not reserialize the already retained shared body"
    );
}
orch8_storage::backend_test!(async fn repeated_publications_share_definition_bytes_without_persisting_task_inputs(b){shared_definition_ignores_task_inputs(b).await;});

async fn oversized_definition_keeps_grant(b: TestBackend) {
    let server = spawn_test_server_on(b.storage.clone(), Some(ROOT)).await;
    let f = fixture(b.storage.as_ref(), &server, Mode::Remote).await;
    let mut body = serde_json::to_value(
        b.storage
            .get_sequence(f.sequence_id())
            .await
            .unwrap()
            .unwrap(),
    )
    .unwrap();
    body["blocks"][0]["params"] =
        json!({"padding":"x".repeat(orch8_storage::delegation_definition::MAX_DEFINITION_BYTES)});
    match &b.raw {
        RawPool::Sqlite(_) => {
            b.raw
                .set_json(
                    "sequences",
                    "blocks",
                    "id",
                    &f.sequence_id().to_string(),
                    &body["blocks"],
                )
                .await;
        }
        RawPool::Postgres(_) => {
            b.raw
                .set_json(
                    "sequences",
                    "definition",
                    "id",
                    &f.sequence_id().to_string(),
                    &body,
                )
                .await;
        }
    }
    assert!(!claim(&server, &f.request).await.status().is_success());
    assert_empty(b.storage.as_ref(), &f).await;
    assert_eq!(
        b.raw
            .count(
                "delegation_definitions",
                "tenant_id",
                f.owner.tenant_id.as_str()
            )
            .await,
        0
    );
}
orch8_storage::backend_test!(async fn oversized_frozen_definitions_do_not_spend_grants_or_publish_partial_work(b){oversized_definition_keeps_grant(b).await;});

async fn published_debug_omits_static_private_values(b: TestBackend) {
    const SECRET: &str = "static-definition-private-value";
    let server = spawn_test_server_on(b.storage.clone(), Some(ROOT)).await;
    let f = fixture(b.storage.as_ref(), &server, Mode::Remote).await;
    let mut definition = b
        .storage
        .get_sequence(f.sequence_id())
        .await
        .unwrap()
        .unwrap();
    let orch8_types::sequence::BlockDefinition::Step(step) = &mut definition.blocks[0] else {
        panic!("fixture step")
    };
    step.params = json!({"private": SECRET});
    b.storage
        .replace_sequence(definition.id, &definition)
        .await
        .unwrap();
    let published = accepted(&server, &f.request).await;
    let task: Uuid = serde_json::from_value(published["mailbox_task_id"].clone()).unwrap();
    let retained = b
        .storage
        .get_published_delegation_definition(&f.owner.tenant_id, task)
        .await
        .unwrap()
        .unwrap();
    assert!(
        std::str::from_utf8(retained.wire().payload())
            .unwrap()
            .contains(SECRET),
        "the transport must retain the actual definition"
    );
    assert!(!format!("{retained:?}").contains(SECRET));
}
orch8_storage::backend_test!(async fn published_definition_debug_redacts_static_private_parameters(b){published_debug_omits_static_private_values(b).await;});
