//! Exact-byte delivery and authority tests; input is independent of live mailbox JSON.
use super::*;
use orch8_types::delegation_definition::{
    input::DelegationInputEnvelope, input::InputLimits, wire::DefinitionReadScope,
};
use orch8_types::worker::WorkerClaim;

async fn read(
    server: &BackendTestServer,
    task: Uuid,
    claim: &WorkerClaim,
    key: &str,
) -> reqwest::Response {
    reqwest::Client::new()
        .get(format!("{}/workers/tasks/{task}/input", server.v1_url()))
        .header("x-api-key", key)
        .query(&[
            ("worker_id", claim.worker_id.clone()),
            ("claim_epoch", claim.claim_epoch.to_string()),
        ])
        .send()
        .await
        .unwrap()
}
async fn exact_input(b: TestBackend) {
    let server = spawn_test_server_on(b.storage.clone(), Some(ROOT)).await;
    for mode in [Mode::Hosted, Mode::Remote, Mode::Inline] {
        let (f, task, claim) = super::outcome::held_mailbox(&b, &server, mode).await;
        let retained = b
            .storage
            .get_published_delegation_input(&f.owner.tenant_id, task)
            .await
            .unwrap()
            .unwrap();
        assert!(!format!("{retained:?}").contains("DELEGATION_INPUT_CANARY"));
        let scope = DefinitionReadScope {
            tenant_id: f.owner.tenant_id.clone(),
            task_id: task,
            worker_id: claim.worker_id.clone(),
            claim_epoch: claim.claim_epoch,
        };
        let response = read(&server, task, &claim, ROOT).await;
        assert_eq!(
            response.status(),
            reqwest::StatusCode::OK,
            "{}",
            response.text().await.unwrap()
        );
        assert_eq!(response.headers()["cache-control"], "no-store");
        let envelope: DelegationInputEnvelope = response.json().await.unwrap();
        let verified = envelope
            .verify(
                &scope,
                retained.request_sha256(),
                retained.reference(),
                InputLimits::default(),
            )
            .unwrap();
        assert_eq!(verified.value(), &f.request["input"]);
        let frozen = b
            .storage
            .get_published_delegation_definition(&f.owner.tenant_id, task)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            retained.usage().definition_expanded_bytes,
            frozen.wire().expanded_bytes()
        );
        for wrong in [
            WorkerClaim::new("other-worker", claim.claim_epoch),
            WorkerClaim::new(&claim.worker_id, claim.claim_epoch + 1),
        ] {
            assert_eq!(
                read(&server, task, &wrong, ROOT).await.status(),
                reqwest::StatusCode::CONFLICT
            );
        }
        let foreign = orch8_types::api_key::mint_scoped(
            "foreign-input-tenant",
            "reader",
            None,
            vec![orch8_types::api_key::ApiCapability::Worker],
        );
        b.storage.create_api_key(&foreign.record).await.unwrap();
        assert_eq!(
            read(&server, task, &claim, &foreign.secret).await.status(),
            reqwest::StatusCode::NOT_FOUND
        );
        let peer = orch8_types::api_key::mint_scoped(
            f.owner.tenant_id.as_str(),
            "peer",
            None,
            vec![orch8_types::api_key::ApiCapability::Worker],
        );
        b.storage.create_api_key(&peer.record).await.unwrap();
        assert_eq!(
            read(&server, task, &claim, &peer.secret).await.status(),
            reqwest::StatusCode::FORBIDDEN
        );
        move_owner(&b, &f).await;
        assert_eq!(
            read(&server, task, &claim, ROOT).await.status(),
            reqwest::StatusCode::CONFLICT
        );
    }
}
orch8_storage::backend_test!(async fn published_input_requires_the_original_claim_reporter_and_current_owner(b){exact_input(b).await;});

async fn revoke_wait(b: TestBackend) {
    for kind in ["revoke", "expire", "downscope"] {
        super::definition::authority_change_during_read(&b, kind, "input").await;
    }
}
orch8_storage::backend_test!(async fn input_read_rechecks_authority_after_storage_wait(b){revoke_wait(b).await;});

async fn corrupt_retained(b: TestBackend) {
    let server = spawn_test_server_on(b.storage.clone(), Some(ROOT)).await;
    let (f, task, claim) = super::outcome::held_mailbox(&b, &server, Mode::Remote).await;
    let remove = match &b.raw {
        RawPool::Sqlite(_) => "DROP TRIGGER delegation_publication_input_update;",
        RawPool::Postgres(_) => {
            "DROP TRIGGER delegation_publication_input_immutable ON delegation_publication_inputs;"
        }
    };
    sql(&b, remove).await;
    for damaged in [
        json!("CORRUPT_PRIVATE_INPUT"),
        json!([0, 0, 0]),
        json!({"private":"CORRUPT_PRIVATE_INPUT"}),
    ] {
        b.raw
            .set_json(
                "delegation_publication_inputs",
                "stored_input",
                "task_id",
                &task.to_string(),
                &damaged,
            )
            .await;
        let response = read(&server, task, &claim, ROOT).await;
        assert_eq!(
            response.status(),
            reqwest::StatusCode::INTERNAL_SERVER_ERROR
        );
        let error = response.text().await.unwrap();
        assert!(!error.contains("CORRUPT_PRIVATE_INPUT"));
        assert!(!error.contains("DELEGATION_INPUT_CANARY"));
    }
    match &b.raw {
        RawPool::Sqlite(pool) => {
            sqlx::raw_sql("DROP TRIGGER delegation_publication_input_delete; DELETE FROM delegation_publication_inputs;").execute(pool).await.unwrap();
        }
        RawPool::Postgres(pool) => {
            sqlx::query("DELETE FROM delegation_publication_inputs WHERE task_id=$1")
                .bind(task)
                .execute(pool)
                .await
                .unwrap();
        }
    }
    assert_eq!(
        read(&server, task, &claim, ROOT).await.status(),
        reqwest::StatusCode::CONFLICT
    );
    let publication = b
        .storage
        .get_delegation_publication(&f.owner.tenant_id, f.delegation_id())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(publication.task_id, task);
}
orch8_storage::backend_test!(async fn corrupted_retained_input_fails_without_disclosure_or_live_fallback(b){corrupt_retained(b).await;});

async fn move_owner(b: &TestBackend, f: &Fixture) {
    let owner = b
        .storage
        .get_continuity_execution_by_instance(&f.owner.tenant_id, f.anchor())
        .await
        .unwrap()
        .unwrap();
    let moved = ContinuityExecution {
        epoch: owner.epoch.checked_next().unwrap(),
        owner_runtime_id: RuntimeId::new(),
        updated_at: Utc::now(),
        ..owner.clone()
    };
    assert!(
        b.storage
            .cas_continuity_owner(
                &owner.tenant_id,
                owner.continuity_id,
                owner.epoch,
                owner.owner_runtime_id,
                &moved
            )
            .await
            .unwrap()
    );
}
