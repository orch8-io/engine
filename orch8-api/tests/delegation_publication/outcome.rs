//! Durable mailbox results across storage backends and worker transports.
use super::*;

async fn held_mailbox(
    b: &TestBackend,
    server: &BackendTestServer,
    mode: Mode,
) -> (Fixture, Uuid, orch8_types::worker::WorkerClaim) {
    let f = fixture(b.storage.as_ref(), server, mode).await;
    let published = accepted(server, &f.request).await;
    let runtime: RuntimeId =
        serde_json::from_value(f.request["delegation"]["destination_runtime_id"].clone()).unwrap();
    let capabilities = b
        .storage
        .get_runtime_capability(&f.owner.tenant_id, runtime, Utc::now())
        .await
        .unwrap()
        .unwrap();
    let response = reqwest::Client::new().post(format!("{}/workers/tasks/poll", server.v1_url()))
        .header("x-api-key", ROOT).json(&json!({"tenant_id":f.owner.tenant_id,
        "handler_name":"orch8.delegation","worker_id":runtime,"capabilities":capabilities,"limit":100}))
        .send().await.unwrap();
    assert!(response.status().is_success());
    let response: Value = response.json().await.unwrap();
    let task = &response["tasks"][0];
    assert_eq!(task["id"], published["mailbox_task_id"]);
    let task_id: Uuid = serde_json::from_value(task["id"].clone()).unwrap();
    let claim = orch8_types::worker::WorkerClaim::new(
        runtime.to_string(),
        task["claim_epoch"].as_u64().unwrap(),
    );
    (f, task_id, claim)
}

async fn result_fault(b: &TestBackend, table: &str, suppress: bool, install: bool) {
    assert!(
        [
            "block_outputs",
            "task_instances",
            "worker_outcome_integrations"
        ]
        .contains(&table)
    );
    if !install {
        let remove = match &b.raw {
            RawPool::Sqlite(_) => "DROP TRIGGER reject_delegation_result;".to_owned(),
            RawPool::Postgres(_) => format!(
                "DROP TRIGGER reject_delegation_result ON {table}; DROP FUNCTION reject_delegation_result();"
            ),
        };
        sql(b, &remove).await;
        return;
    }
    let event = if table == "task_instances" {
        "UPDATE"
    } else {
        "INSERT"
    };
    let install = match &b.raw {
        RawPool::Sqlite(_) => format!(
            "CREATE TRIGGER reject_delegation_result BEFORE {event} ON {table} BEGIN SELECT RAISE({}); END;",
            if suppress {
                "IGNORE"
            } else {
                "ABORT,'injected parent failure'"
            }
        ),
        RawPool::Postgres(_) => format!(
            "CREATE FUNCTION reject_delegation_result() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN {} END $$; CREATE TRIGGER reject_delegation_result BEFORE {event} ON {table} FOR EACH ROW EXECUTE FUNCTION reject_delegation_result();",
            if suppress {
                "RETURN NULL;"
            } else {
                "RAISE EXCEPTION 'injected parent failure';"
            }
        ),
    };
    sql(b, &install).await;
}

async fn assert_parent_result(b: &TestBackend, f: &Fixture, output: &Value) {
    let parent = b.storage.get_instance(f.anchor()).await.unwrap().unwrap();
    assert_eq!(
        parent.context.data["delegations"][f.delegation_id().to_string()]["output"],
        *output
    );
    assert_eq!(
        parent.state,
        if f.mode == Mode::Hosted {
            InstanceState::Scheduled
        } else {
            InstanceState::Completed
        }
    );
}

async fn completion_recovers_parent_write_failure(b: TestBackend) {
    let server = spawn_test_server_on(b.storage.clone(), Some(ROOT)).await;
    for (mode, table, suppress) in [Mode::Hosted, Mode::Remote].into_iter().flat_map(|mode| {
        [
            "block_outputs",
            "task_instances",
            "worker_outcome_integrations",
        ]
        .into_iter()
        .flat_map(move |table| {
            [false, true]
                .into_iter()
                .map(move |suppress| (mode, table, suppress))
        })
    }) {
        let (f, task_id, claim) = held_mailbox(&b, &server, mode).await;
        let output =
            json!({"nul":"result\0λ","tiny":f64::from_bits(1),"precise":0.100_000_000_000_000_02});
        let body =
            json!({"worker_id":claim.worker_id,"claim_epoch":claim.claim_epoch,"output":output});
        let url = format!("{}/workers/tasks/{task_id}/complete", server.v1_url());
        result_fault(&b, table, suppress, true).await;
        let failed = reqwest::Client::new()
            .post(&url)
            .header("x-api-key", ROOT)
            .json(&body)
            .send()
            .await
            .unwrap();
        assert!(
            failed.status().is_server_error()
                || (suppress && failed.status() == reqwest::StatusCode::CONFLICT),
            "{}",
            failed.text().await.unwrap()
        );
        let retained = b
            .storage
            .get_worker_accepted_outcome(Some(&f.owner.tenant_id), task_id, &claim)
            .await
            .unwrap()
            .expect("completed provider fact was lost");
        assert_eq!(retained.output, output);
        let partial_output = b
            .storage
            .get_block_output(
                f.anchor(),
                &orch8_types::ids::BlockId::new(format!("delegation-{}", f.delegation_id())),
            )
            .await
            .unwrap();
        result_fault(&b, table, suppress, false).await;
        // A committed first block output is not proof of context/wake application.
        assert!(
            partial_output.is_none(),
            "parent output escaped the failed integration transaction"
        );
        sql(
            &b,
            &format!("DELETE FROM worker_tasks WHERE id='{task_id}'"),
        )
        .await;
        let recovered = orch8_storage::worker_outcome::integrate_worker_outcome(
            b.storage.as_ref(),
            &f.owner.tenant_id,
            task_id,
            &claim,
            16 * 1024 * 1024,
        )
        .await
        .unwrap();
        assert_eq!(
            recovered,
            orch8_storage::worker_outcome::WorkerOutcomeApplication::Applied
        );
        let mut changed = body;
        changed["output"] = json!({"different":"must never replace the accepted result"});
        let retried = reqwest::Client::new()
            .post(&url)
            .header("x-api-key", ROOT)
            .json(&changed)
            .send()
            .await
            .unwrap();
        assert!(
            retried.status().is_success(),
            "{}",
            retried.text().await.unwrap()
        );
        assert_parent_result(&b, &f, &output).await;
    }
}
orch8_storage::backend_test!(async fn completed_mailbox_recovers_atomically_without_the_hot_task(b){completion_recovers_parent_write_failure(b).await;});

async fn move_mailbox_owner(b: &TestBackend, f: &Fixture) {
    let current = b
        .storage
        .get_continuity_execution_by_instance(&f.owner.tenant_id, f.anchor())
        .await
        .unwrap()
        .unwrap();
    let next = ContinuityExecution {
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
                &next
            )
            .await
            .unwrap()
    );
}

async fn paused_or_moved_parent(b: TestBackend) {
    use orch8_storage::worker_outcome::{
        WorkerOutcomeApplication as Application, integrate_worker_outcome,
    };
    let server = spawn_test_server_on(b.storage.clone(), Some(ROOT)).await;
    for (mode, moved) in [Mode::Hosted, Mode::Remote]
        .into_iter()
        .flat_map(|mode| [false, true].into_iter().map(move |moved| (mode, moved)))
    {
        let (f, task, claim) = held_mailbox(&b, &server, mode).await;
        let output = json!({"received":"once"});
        assert!(
            b.storage
                .complete_worker_task(task, &claim, &output)
                .await
                .unwrap()
        );
        if moved {
            move_mailbox_owner(&b, &f).await;
        } else {
            sql(
                &b,
                &format!(
                    "UPDATE task_instances SET state='paused' WHERE id='{}'",
                    f.anchor()
                ),
            )
            .await;
        }
        sql(&b, &format!("DELETE FROM worker_tasks WHERE id='{task}'")).await;
        let result = integrate_worker_outcome(
            b.storage.as_ref(),
            &f.owner.tenant_id,
            task,
            &claim,
            16 * 1024 * 1024,
        )
        .await
        .unwrap();
        let parent = b.storage.get_instance(f.anchor()).await.unwrap().unwrap();
        if moved {
            assert_eq!(result, Application::ReconciliationRequired);
            assert!(parent.context.data.get("delegations").is_none());
        } else if mode == Mode::Remote {
            assert_eq!(result, Application::Deferred);
            assert_eq!(parent.state, InstanceState::Paused);
            assert!(parent.context.data.get("delegations").is_none());
            sql(
                &b,
                &format!(
                    "UPDATE task_instances SET state='waiting' WHERE id='{}'",
                    f.anchor()
                ),
            )
            .await;
            assert_eq!(
                integrate_worker_outcome(
                    b.storage.as_ref(),
                    &f.owner.tenant_id,
                    task,
                    &claim,
                    16 * 1024 * 1024
                )
                .await
                .unwrap(),
                Application::Applied
            );
            assert_eq!(
                b.storage
                    .get_instance(f.anchor())
                    .await
                    .unwrap()
                    .unwrap()
                    .state,
                InstanceState::Completed
            );
        } else {
            assert_eq!(result, Application::Applied);
            assert_eq!(parent.state, InstanceState::Paused);
            assert_eq!(
                parent.context.data["delegations"][f.delegation_id().to_string()]["output"],
                output
            );
        }
    }
}
orch8_storage::backend_test!(async fn mailbox_recovery_preserves_pause_and_rejects_a_new_owner(b){paused_or_moved_parent(b).await;});

async fn concurrent_result_recovery(b: TestBackend) {
    use orch8_storage::worker_outcome::{
        WorkerOutcomeApplication as Application, integrate_worker_outcome,
    };
    let server = spawn_test_server_on(b.storage.clone(), Some(ROOT)).await;
    for mode in [Mode::Hosted, Mode::Remote] {
        let (f, task, claim) = held_mailbox(&b, &server, mode).await;
        assert!(
            b.storage
                .complete_worker_task(task, &claim, &json!({"value":7}))
                .await
                .unwrap()
        );
        sql(&b, &format!("DELETE FROM worker_tasks WHERE id='{task}'")).await;
        assert_eq!(
            integrate_worker_outcome(b.storage.as_ref(), &f.owner.tenant_id, task, &claim, 1)
                .await
                .unwrap(),
            Application::Deferred
        );
        let mut workers = tokio::task::JoinSet::new();
        for _ in 0..12 {
            let (storage, tenant, claim) =
                (b.storage.clone(), f.owner.tenant_id.clone(), claim.clone());
            workers.spawn(async move {
                integrate_worker_outcome(storage.as_ref(), &tenant, task, &claim, 16 * 1024 * 1024)
                    .await
                    .unwrap()
            });
        }
        let mut applied = 0;
        while let Some(result) = workers.join_next().await {
            match result.unwrap() {
                Application::Applied => applied += 1,
                Application::AlreadyApplied | Application::Deferred => {}
                other => panic!("unexpected concurrent application: {other:?}"),
            }
        }
        assert_eq!(applied, 1);
        assert_eq!(
            integrate_worker_outcome(
                b.storage.as_ref(),
                &f.owner.tenant_id,
                task,
                &claim,
                16 * 1024 * 1024
            )
            .await
            .unwrap(),
            Application::AlreadyApplied
        );
        let count: i64 = match &b.raw {
            RawPool::Sqlite(pool) => {
                sqlx::query_scalar("SELECT COUNT(*) FROM block_outputs WHERE instance_id=?1")
                    .bind(f.anchor().to_string())
                    .fetch_one(pool)
                    .await
                    .unwrap()
            }
            RawPool::Postgres(pool) => {
                sqlx::query_scalar("SELECT COUNT(*) FROM block_outputs WHERE instance_id=$1")
                    .bind(f.anchor().into_uuid())
                    .fetch_one(pool)
                    .await
                    .unwrap()
            }
        };
        assert_eq!(count, 1, "competing recovery appended duplicate outcomes");
    }
}
orch8_storage::backend_test!(async fn concurrent_cold_mailbox_recovery_applies_once(b){concurrent_result_recovery(b).await;});

async fn proxy_metadata_cannot_turn_mailbox_into_native_work(b: TestBackend) {
    let server = spawn_test_server_on(b.storage.clone(), Some(ROOT)).await;
    let (f, task, claim) = held_mailbox(&b, &server, Mode::Remote).await;
    assert!(
        b.storage
            .complete_worker_task(task, &claim, &json!({"done":true}))
            .await
            .unwrap()
    );
    sql(
        &b,
        &format!(
            "UPDATE task_instances SET metadata='{{}}' WHERE id='{}'",
            f.anchor()
        ),
    )
    .await;
    let result = orch8_storage::worker_outcome::integrate_worker_outcome(
        b.storage.as_ref(),
        &f.owner.tenant_id,
        task,
        &claim,
        16 * 1024 * 1024,
    )
    .await;
    assert!(
        result.is_err(),
        "editable metadata reclassified the published proxy"
    );
    let parent = b.storage.get_instance(f.anchor()).await.unwrap().unwrap();
    assert_eq!(parent.state, InstanceState::Waiting);
    assert!(parent.context.data.get("delegations").is_none());
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
orch8_storage::backend_test!(async fn proxy_classification_requires_immutable_publication_evidence(b){proxy_metadata_cannot_turn_mailbox_into_native_work(b).await;});

struct GrpcWorker {
    client: orch8_grpc::proto::orch8_service_client::Orch8ServiceClient<tonic::transport::Channel>,
    stop: tokio_util::sync::CancellationToken,
    server: tokio::task::JoinHandle<()>,
}
async fn grpc_worker(b: &TestBackend) -> GrpcWorker {
    grpc_worker_with_limit(b, orch8_types::context::DEFAULT_MAX_CONTEXT_BYTES).await
}
async fn grpc_worker_with_limit(b: &TestBackend, max_context_bytes: u32) -> GrpcWorker {
    use orch8_grpc::{
        Orch8ServiceServer, proto::orch8_service_client::Orch8ServiceClient,
        service::Orch8GrpcService,
    };
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let storage = b.storage.clone();
    let stop = tokio_util::sync::CancellationToken::new();
    let stopping = stop.clone();
    let grpc = tokio::spawn(async move {
        let auth = orch8_grpc::auth::GrpcAuthLayer::new(
            storage.clone(),
            Some(orch8_types::auth::precompute_secret_digest(ROOT)),
            true,
        );
        tonic::transport::Server::builder()
            .layer(auth)
            .add_service(Orch8ServiceServer::new(
                Orch8GrpcService::with_max_context_bytes(storage, max_context_bytes),
            ))
            .serve_with_incoming_shutdown(
                tokio_stream::wrappers::TcpListenerStream::new(listener),
                stopping.cancelled_owned(),
            )
            .await
            .unwrap();
    });
    let client = Orch8ServiceClient::connect(format!("http://{addr}"))
        .await
        .unwrap();
    GrpcWorker {
        client,
        stop,
        server: grpc,
    }
}

async fn cross_transport_result_recovery(b: TestBackend) {
    use orch8_grpc::proto::CompleteTaskRequest;
    let server = spawn_test_server_on(b.storage.clone(), Some(ROOT)).await;
    let mut grpc = grpc_worker(&b).await;
    for (mode, grpc_first) in [Mode::Hosted, Mode::Remote]
        .into_iter()
        .flat_map(|mode| [false, true].into_iter().map(move |first| (mode, first)))
    {
        let (f, task, claim) = held_mailbox(&b, &server, mode).await;
        let output = json!({"nul":"cross\0λ","tiny":f64::from_bits(1)});
        let body =
            json!({"worker_id":claim.worker_id,"claim_epoch":claim.claim_epoch,"output":output});
        let url = format!("{}/workers/tasks/{task}/complete", server.v1_url());
        let request = |output: &Value| {
            let mut req = tonic::Request::new(CompleteTaskRequest {
                task_id: task.to_string(),
                worker_id: claim.worker_id.clone(),
                claim_epoch: claim.claim_epoch,
                output_json: output.to_string(),
            });
            req.metadata_mut()
                .insert("x-api-key", ROOT.parse().unwrap());
            req.metadata_mut()
                .insert("x-tenant-id", f.owner.tenant_id.as_str().parse().unwrap());
            req
        };
        result_fault(&b, "task_instances", false, true).await;
        if grpc_first {
            let error = grpc
                .client
                .complete_task(request(&output))
                .await
                .unwrap_err();
            assert_eq!(error.code(), tonic::Code::Internal);
        } else {
            let response = reqwest::Client::new()
                .post(&url)
                .header("x-api-key", ROOT)
                .json(&body)
                .send()
                .await
                .unwrap();
            assert!(response.status().is_server_error());
        }
        result_fault(&b, "task_instances", false, false).await;
        sql(&b, &format!("DELETE FROM worker_tasks WHERE id='{task}'")).await;
        let changed = json!({"changed":"ignored"});
        if grpc_first {
            let mut body = body;
            body["output"] = changed;
            let response = reqwest::Client::new()
                .post(&url)
                .header("x-api-key", ROOT)
                .json(&body)
                .send()
                .await
                .unwrap();
            assert!(
                response.status().is_success(),
                "{}",
                response.text().await.unwrap()
            );
        } else {
            grpc.client.complete_task(request(&changed)).await.unwrap();
        }
        assert_parent_result(&b, &f, &output).await;
    }
    drop(grpc.client);
    grpc.stop.cancel();
    grpc.server.await.unwrap();
}
orch8_storage::backend_test!(async fn accepted_mailbox_results_recover_across_http_and_grpc(b){cross_transport_result_recovery(b).await;});

async fn historical_hot_completion_without_acceptance(b: TestBackend) {
    use orch8_types::{ids::BlockId, output::BlockOutput, worker::WorkerTaskState};
    let server = spawn_test_server_on(b.storage.clone(), Some(ROOT)).await;
    let (f, original, claim) = held_mailbox(&b, &server, Mode::Hosted).await;
    let id = Uuid::now_v7();
    let mut historical = b.storage.get_worker_task(original).await.unwrap().unwrap();
    historical.id = Uuid::now_v7();
    historical.block_id = BlockId::new(format!("delegation-{id}"));
    historical.effect_id = None;
    historical.continuity_epoch = None;
    historical.params = json!({"delegation_id":id});
    historical.state = WorkerTaskState::Completed;
    historical.output = Some(json!({"original":"historical provider output"}));
    historical.completed_at = Some(Utc::now());
    b.storage.create_worker_task(&historical).await.unwrap();
    // PostgreSQL's dispatch insert deliberately omits completion columns;
    // seed an actual pre-journal row, not a fresh dispatch with missing worker.
    if let RawPool::Postgres(pool) = &b.raw {
        sqlx::query("UPDATE worker_tasks SET worker_id=$1,output=$2,output_encoding=NULL,completed_at=$3 WHERE id=$4")
            .bind(&claim.worker_id).bind(historical.output.as_ref().unwrap())
            .bind(Utc::now()).bind(historical.id).execute(pool).await.unwrap();
    }
    assert!(
        b.storage
            .get_worker_accepted_outcome(Some(&f.owner.tenant_id), historical.id, &claim)
            .await
            .unwrap()
            .is_none()
    );
    // Model the pre-journal server dying after its first native write.
    b.storage
        .save_block_output(&BlockOutput {
            id: Uuid::now_v7(),
            instance_id: f.anchor(),
            block_id: historical.block_id.clone(),
            output: json!({"status":"completed"}),
            output_ref: None,
            output_size: 22,
            attempt: 0,
            created_at: Utc::now(),
        })
        .await
        .unwrap();
    let response = reqwest::Client::new().post(format!("{}/workers/tasks/{}/complete", server.v1_url(), historical.id))
        .header("x-api-key", ROOT).json(&json!({"worker_id":claim.worker_id,"claim_epoch":claim.claim_epoch,"output":{"changed":"ignored"}}))
        .send().await.unwrap();
    assert!(
        response.status().is_success(),
        "{}",
        response.text().await.unwrap()
    );
    let parent = b.storage.get_instance(f.anchor()).await.unwrap().unwrap();
    assert_eq!(parent.state, InstanceState::Scheduled);
    assert_eq!(
        parent.context.data["delegations"][id.to_string()]["output"],
        historical.output.unwrap()
    );
}
orch8_storage::backend_test!(async fn historical_completed_mailboxes_retry_without_inventing_acceptance(b){historical_hot_completion_without_acceptance(b).await;});

#[path = "limits.rs"]
mod limits;
