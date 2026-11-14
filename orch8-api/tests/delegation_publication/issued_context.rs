//! Poll carries independent exact-byte references captured with the lease.
use super::*;
use orch8_types::delegation_definition::{
    DefinitionGraphLimits,
    claim::DelegationClaimContext,
    input::{DelegationInputEnvelope, InputLimits},
    wire::{DefinitionReadScope, DefinitionWireEnvelope},
};

pub(super) async fn capabilities(b: &TestBackend, f: &Fixture) -> RuntimeCapabilities {
    let runtime: RuntimeId =
        serde_json::from_value(f.request["delegation"]["destination_runtime_id"].clone()).unwrap();
    b.storage
        .get_runtime_capability(&f.owner.tenant_id, runtime, Utc::now())
        .await
        .unwrap()
        .unwrap()
}
pub(super) async fn poll_raw(
    server: &BackendTestServer,
    f: &Fixture,
    caps: &RuntimeCapabilities,
) -> reqwest::Response {
    reqwest::Client::new().post(format!("{}/workers/tasks/poll",server.v1_url())).header("x-api-key",ROOT)
        .json(&json!({"tenant_id":f.owner.tenant_id,"handler_name":"orch8.delegation","worker_id":caps.runtime_id,"capabilities":caps,"limit":1})).send().await.unwrap()
}
async fn read(
    server: &BackendTestServer,
    context: &DelegationClaimContext,
    resource: &str,
) -> reqwest::Response {
    reqwest::Client::new()
        .get(format!(
            "{}/workers/tasks/{}/{resource}",
            server.v1_url(),
            context.scope.task_id
        ))
        .header("x-api-key", ROOT)
        .query(&[
            ("worker_id", context.scope.worker_id.clone()),
            ("claim_epoch", context.scope.claim_epoch.to_string()),
        ])
        .send()
        .await
        .unwrap()
}
async fn exact_delivery(b: TestBackend) {
    let server = spawn_test_server_on(b.storage.clone(), Some(ROOT)).await;
    for mode in [Mode::Remote, Mode::Hosted, Mode::Inline] {
        let mut f = fixture(b.storage.as_ref(), &server, mode).await;
        f.request["input"] = json!({"private":"ISSUED_INPUT_CANARY\0😺","wide":u64::MAX,"body":"x".repeat(512*1024)});
        let published = accepted(&server, &f.request).await;
        let caps = capabilities(&b, &f).await;
        let response = poll_raw(&server, &f, &caps).await;
        assert_eq!(
            response.status(),
            reqwest::StatusCode::OK,
            "{}",
            response.text().await.unwrap()
        );
        let wire = response.text().await.unwrap();
        assert!(wire.len() < 16_384);
        assert!(!wire.contains("ISSUED_INPUT_CANARY"));
        let wire: Value = serde_json::from_str(&wire).unwrap();
        let task = &wire["tasks"][0];
        let context: DelegationClaimContext =
            serde_json::from_value(task["delegation"].clone()).unwrap();
        context
            .validate_scope(&DefinitionReadScope {
                tenant_id: f.owner.tenant_id.clone(),
                task_id: serde_json::from_value(published["mailbox_task_id"].clone()).unwrap(),
                worker_id: caps.runtime_id.to_string(),
                claim_epoch: task["claim_epoch"].as_u64().unwrap(),
            })
            .unwrap();
        assert_eq!(task["params"], json!({}));
        assert_eq!(task["context"], json!({}));
        let claim = orch8_types::worker::WorkerClaim::new(
            &context.scope.worker_id,
            context.scope.claim_epoch,
        );
        assert_eq!(
            b.storage
                .get_worker_delegation_context(&f.owner.tenant_id, context.scope.task_id, &claim)
                .await
                .unwrap()
                .as_ref(),
            Some(&context)
        );
        verify_payloads(&server, &context, &f.request["input"]).await;
        let issued = b
            .storage
            .get_worker_issued_claim(Some(&f.owner.tenant_id), context.scope.task_id, &claim)
            .await
            .unwrap()
            .unwrap();
        let receipt = serde_json::to_value(issued).unwrap();
        assert_eq!(receipt["version"], 1);
        assert!(receipt.get("delegation").is_none());
        b.storage
            .delete_worker_task(context.scope.task_id)
            .await
            .unwrap();
        assert_eq!(
            b.storage
                .get_worker_delegation_context(&f.owner.tenant_id, context.scope.task_id, &claim)
                .await
                .unwrap()
                .as_ref(),
            Some(&context)
        );
        assert!(
            b.storage
                .get_worker_delegation_context(
                    &orch8_types::ids::TenantId::unchecked("other"),
                    context.scope.task_id,
                    &claim
                )
                .await
                .unwrap()
                .is_none()
        );
    }
}
orch8_storage::backend_test!(async fn issued_context_binds_both_payloads_and_survives_hot_task_cleanup(b){exact_delivery(b).await;});

async fn atomic_capture(b: TestBackend) {
    let server = spawn_test_server_on(b.storage.clone(), Some(ROOT)).await;
    for suppress in [false, true] {
        let f = fixture(b.storage.as_ref(), &server, Mode::Remote).await;
        let published = accepted(&server, &f.request).await;
        let task: Uuid = serde_json::from_value(published["mailbox_task_id"].clone()).unwrap();
        let caps = capabilities(&b, &f).await;
        fault(&b, "worker_issued_delegation_contexts", suppress, true).await;
        assert!(
            poll_raw(&server, &f, &caps)
                .await
                .status()
                .is_server_error()
        );
        fault(&b, "worker_issued_delegation_contexts", false, false).await;
        let stored = b.storage.get_worker_task(task).await.unwrap().unwrap();
        assert_eq!(stored.state, orch8_types::worker::WorkerTaskState::Pending);
        assert_eq!(stored.claim_epoch, 0);
        assert_eq!(
            b.raw
                .count("worker_issued_claims", "task_id", &task.to_string())
                .await,
            0
        );
        assert_eq!(
            b.raw
                .count(
                    "worker_issued_delegation_contexts",
                    "task_id",
                    &task.to_string()
                )
                .await,
            0
        );
        let response = poll_raw(&server, &f, &caps).await;
        assert_eq!(response.status(), reqwest::StatusCode::OK);
        let wire: Value = response.json().await.unwrap();
        assert_eq!(wire["tasks"][0]["claim_epoch"], 1);
        for sql in [
            "UPDATE worker_issued_delegation_contexts SET record=record",
            "DELETE FROM worker_issued_delegation_contexts",
        ] {
            let rejected = match &b.raw {
                RawPool::Sqlite(pool) => sqlx::query(sql).execute(pool).await.is_err(),
                RawPool::Postgres(pool) => sqlx::query(sql).execute(pool).await.is_err(),
            };
            assert!(rejected);
        }
    }
}
orch8_storage::backend_test!(async fn issued_context_failure_rolls_back_the_entire_lease(b){atomic_capture(b).await;});

pub(super) async fn verify_payloads(
    server: &BackendTestServer,
    context: &DelegationClaimContext,
    expected: &Value,
) {
    let input: DelegationInputEnvelope = read(server, context, "input").await.json().await.unwrap();
    assert_eq!(
        input
            .verify(
                &context.scope,
                &context.request_sha256,
                &context.input,
                InputLimits::default()
            )
            .unwrap()
            .value(),
        expected
    );
    let definition: DefinitionWireEnvelope = read(server, context, "definition")
        .await
        .json()
        .await
        .unwrap();
    definition
        .verify(
            &context.scope,
            &context.definition,
            DefinitionGraphLimits::default(),
        )
        .unwrap();
}
