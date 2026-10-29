//! Context admission measures the same wrapped result as durable application.
use super::*;
use orch8_types::{context::ExecutionContext, worker::WorkerClaim};

#[derive(Debug, PartialEq, Eq)]
enum Report {
    Accepted,
    TooLarge,
    Unexpected(String),
}
async fn report(
    http: &BackendTestServer,
    grpc: Option<&mut GrpcWorker>,
    tenant: &str,
    task: Uuid,
    claim: &WorkerClaim,
    output: &Value,
) -> Report {
    if let Some(grpc) = grpc {
        let mut request = tonic::Request::new(orch8_grpc::proto::CompleteTaskRequest {
            task_id: task.to_string(),
            worker_id: claim.worker_id.clone(),
            claim_epoch: claim.claim_epoch,
            output_json: output.to_string(),
        });
        request
            .metadata_mut()
            .insert("x-api-key", ROOT.parse().unwrap());
        request
            .metadata_mut()
            .insert("x-tenant-id", tenant.parse().unwrap());
        return match grpc.client.complete_task(request).await {
            Ok(_) => Report::Accepted,
            Err(error) if error.code() == tonic::Code::InvalidArgument => Report::TooLarge,
            Err(error) => Report::Unexpected(error.to_string()),
        };
    }
    let response = reqwest::Client::new()
        .post(format!("{}/workers/tasks/{task}/complete", http.v1_url()))
        .header("x-api-key", ROOT)
        .json(&json!({"worker_id":claim.worker_id,"claim_epoch":claim.claim_epoch,"output":output}))
        .send()
        .await
        .unwrap();
    match response.status() {
        reqwest::StatusCode::OK => Report::Accepted,
        reqwest::StatusCode::PAYLOAD_TOO_LARGE => Report::TooLarge,
        _ => Report::Unexpected(response.text().await.unwrap()),
    }
}
fn projected(
    context: &ExecutionContext,
    f: &Fixture,
    claim: &WorkerClaim,
    output: &Value,
) -> ExecutionContext {
    let mut expected = context.clone();
    if !expected.data.is_object() {
        expected.data = json!({});
    }
    let data = expected.data.as_object_mut().unwrap();
    let delegations = data.entry("delegations").or_insert_with(|| json!({}));
    if !delegations.is_object() {
        *delegations = json!({});
    }
    delegations.as_object_mut().unwrap().insert(f.delegation_id().to_string(),
        json!({"status":"completed","delegation_id":f.delegation_id(),"runtime_id":claim.worker_id,"output":output}));
    expected
}
async fn bounded_server(b: &TestBackend, max_context_bytes: u32) -> BackendTestServer {
    orch8_api::test_harness::spawn_test_server_with(
        b.storage.clone(),
        orch8_api::test_harness::TestServerOptions {
            root_api_key: Some(ROOT.into()),
            max_context_bytes,
            ..Default::default()
        },
    )
    .await
}
async fn assert_native_unchanged(
    b: &TestBackend,
    f: &Fixture,
    initial: &orch8_types::instance::TaskInstance,
) {
    let after = b.storage.get_instance(f.anchor()).await.unwrap().unwrap();
    assert_eq!(
        serde_json::to_value(after).unwrap(),
        serde_json::to_value(initial).unwrap()
    );
    assert!(
        b.storage
            .get_block_output(
                f.anchor(),
                &orch8_types::ids::BlockId::new(format!("delegation-{}", f.delegation_id()))
            )
            .await
            .unwrap()
            .is_none()
    );
}

async fn one_limit_case(
    b: &TestBackend,
    base: &BackendTestServer,
    mode: Mode,
    use_grpc: bool,
    output: Value,
    reject: bool,
    replace: bool,
) {
    let (f, task, claim) = held_mailbox(b, base, mode).await;
    if replace {
        b.storage.merge_context_data(f.anchor(), "delegations", &json!({f.delegation_id().to_string():{"old":"x".repeat(2048)},"sibling":{"keep":true}})).await.unwrap();
    }
    let initial = b.storage.get_instance(f.anchor()).await.unwrap().unwrap();
    let expected = projected(&initial.context, &f, &claim, &output);
    let limit = u32::try_from(expected.serialized_size()).unwrap() - u32::from(reject);
    let http = bounded_server(b, limit).await;
    let mut grpc = if use_grpc {
        Some(grpc_worker_with_limit(b, limit).await)
    } else {
        None
    };
    let first = report(
        &http,
        grpc.as_mut(),
        f.owner.tenant_id.as_str(),
        task,
        &claim,
        &output,
    )
    .await;
    let final_output = if reject {
        assert_eq!(
            first,
            Report::TooLarge,
            "accepted a wrapped result one byte over its context ceiling"
        );
        assert!(
            b.storage
                .get_worker_accepted_outcome(Some(&f.owner.tenant_id), task, &claim)
                .await
                .unwrap()
                .is_none()
        );
        let held = b.storage.get_worker_task(task).await.unwrap().unwrap();
        assert_eq!(held.state, orch8_types::worker::WorkerTaskState::Claimed);
        assert_eq!(
            b.storage
                .get_effect_receipt(&f.owner.tenant_id, held.effect_id.unwrap())
                .await
                .unwrap()
                .unwrap()
                .state,
            orch8_types::continuity::EffectState::Dispatched
        );
        assert_native_unchanged(b, &f, &initial).await;
        let corrected = json!("");
        assert_eq!(
            report(
                &http,
                grpc.as_mut(),
                f.owner.tenant_id.as_str(),
                task,
                &claim,
                &corrected
            )
            .await,
            Report::Accepted
        );
        corrected
    } else {
        assert_eq!(
            first,
            Report::Accepted,
            "rejected an exact-ceiling result or measured a replaced value twice"
        );
        output
    };
    let final_parent = b.storage.get_instance(f.anchor()).await.unwrap().unwrap();
    assert_eq!(
        serde_json::to_value(&final_parent.context).unwrap(),
        serde_json::to_value(projected(&initial.context, &f, &claim, &final_output)).unwrap()
    );
    assert_eq!(
        final_parent.state,
        if mode == Mode::Hosted {
            InstanceState::Scheduled
        } else {
            InstanceState::Completed
        }
    );
    if let Some(grpc) = grpc {
        drop(grpc.client);
        grpc.stop.cancel();
        grpc.server.await.unwrap();
    }
}
async fn exact_context_limits(b: TestBackend, use_grpc: bool) {
    let base = spawn_test_server_on(b.storage.clone(), Some(ROOT)).await;
    for mode in [Mode::Hosted, Mode::Remote] {
        for output in [
            json!(null),
            json!("λ\0".repeat(128)),
            json!(["x".repeat(256)]),
            json!({"blob":"x".repeat(256),"tiny":f64::from_bits(1),"precise":0.100_000_000_000_000_02}),
        ] {
            for reject in [false, true] {
                one_limit_case(&b, &base, mode, use_grpc, output.clone(), reject, false).await;
            }
        }
        one_limit_case(
            &b,
            &base,
            mode,
            use_grpc,
            json!({"replacement":true}),
            false,
            true,
        )
        .await;
    }
}
orch8_storage::backend_test!(async fn http_context_limits_measure_the_exact_published_mailbox_result(b){exact_context_limits(b, false).await;});
orch8_storage::backend_test!(async fn grpc_context_limits_measure_the_exact_published_mailbox_result(b){exact_context_limits(b, true).await;});
