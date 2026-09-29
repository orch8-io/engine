//! Worker-protocol guarantees through the real transports (HTTP router with
//! API-key auth, gRPC) and the real scheduler, on `SQLite` and Postgres:
//!
//! * HTTP `fail` is one fenced resolution (effect `unknown`, retry policy,
//!   idempotent re-report, 404 once superseded by a retry);
//! * gRPC complete / fail / release settle effects like HTTP;
//! * a placement rejection on the flat path is observable (output + audit);
//! * a browser session never receives a credential-bearing task, and the
//!   context it does receive is redacted.
#![allow(clippy::too_many_lines)]

use std::time::Duration;

use orch8_e2e::{Backend, Cloud, backends, wait_for};
use orch8_grpc::proto;
use orch8_grpc::proto::orch8_service_client::Orch8ServiceClient;
use orch8_types::continuity::EffectState;
use orch8_types::worker::{WorkerTask, WorkerTaskState};
use serde_json::{Value, json};
use uuid::Uuid;

const LONG: Duration = Duration::from_secs(30);

fn step(id: &str, handler: &str, params: &Value, attempts: Option<u32>) -> Value {
    let mut step = json!({"type": "step", "id": id, "handler": handler, "params": params,
                          "cancellable": true});
    if let Some(max_attempts) = attempts {
        step["retry"] =
            json!({"max_attempts": max_attempts, "initial_backoff": 10, "max_backoff": 10});
    }
    step
}

/// Start `[step]` and wait until its worker task is dispatched (bound to an
/// effect id) and pending.
fn dispatched(cloud: &Cloud, handler: &str, attempts: Option<u32>) -> (Uuid, WorkerTask) {
    let sequence = cloud.create_sequence(
        handler,
        &json!([step("work", handler, &json!({"n": 1}), attempts)]),
    );
    let instance = cloud.create_instance(sequence, &json!({}));
    let task = wait_for("dispatch", LONG, || {
        cloud
            .tasks(instance)
            .into_iter()
            .find(|task| task.state == WorkerTaskState::Pending && task.effect_id.is_some())
    });
    (instance, task)
}

fn receipt_states(cloud: &Cloud, instance: Uuid) -> Vec<EffectState> {
    cloud
        .block_receipts(instance, "work")
        .iter()
        .map(|receipt| receipt.state)
        .collect()
}

fn poll(cloud: &Cloud, key: &str, handler: &str, worker: &str) -> Vec<Value> {
    let (status, body) = cloud.call(
        key,
        "POST",
        "/workers/tasks/poll",
        Some(&json!({"handler_name": handler, "worker_id": worker, "limit": 10})),
    );
    assert_eq!(status, 200, "poll: {body}");
    body["tasks"].as_array().cloned().unwrap_or_default()
}

fn fail(cloud: &Cloud, key: &str, task: &Value, worker: &str, retryable: bool) -> u16 {
    cloud
        .call(
            key,
            "POST",
            &format!("/workers/tasks/{}/fail", task["id"].as_str().unwrap()),
            Some(
                &json!({"worker_id": worker, "claim_epoch": task["claim_epoch"],
                         "message": "boom", "retryable": retryable}),
            ),
        )
        .0
}

#[test]
fn http_fail_is_one_fenced_resolution() {
    for backend in backends() {
        let cloud = Cloud::start(&backend, |_| {});
        let backend = cloud.backend;
        let key = cloud.mint_key(&["worker"]);

        // Retry policy: 200, receipt unknown, next attempt with a fresh
        // effect id; the superseded task answers 404.
        let handler = format!("ext.retry.{}", Uuid::now_v7().simple());
        let (instance, first) = dispatched(&cloud, &handler, Some(2));
        let claimed = poll(&cloud, &key, &handler, "w1").remove(0);
        assert_eq!(claimed["effect_id"], first.effect_id.unwrap().to_string());
        let other = json!({"id": claimed["id"], "claim_epoch": claimed["claim_epoch"]});
        assert_eq!(
            fail(&cloud, &key, &other, "intruder", true),
            409,
            "{backend}"
        );
        assert_eq!(fail(&cloud, &key, &claimed, "w1", true), 200, "{backend}");
        assert_eq!(
            receipt_states(&cloud, instance)[0],
            EffectState::Unknown,
            "{backend}"
        );
        assert!(cloud.task(first.id).is_none(), "{backend}");
        assert_eq!(
            fail(&cloud, &key, &claimed, "w1", true),
            404,
            "{backend}: superseded by its retry"
        );
        let retry = wait_for("re-dispatch", LONG, || {
            cloud
                .tasks(instance)
                .into_iter()
                .find(|task| task.attempt == first.attempt + 1 && task.effect_id.is_some())
        });
        assert_ne!(retry.effect_id, first.effect_id, "{backend}");
        let reclaimed = poll(&cloud, &key, &handler, "w1").remove(0);
        assert_eq!(reclaimed["id"], retry.id.to_string(), "{backend}");
        assert_eq!(
            reclaimed["effect_id"],
            retry.effect_id.unwrap().to_string(),
            "{backend}: claimed with the effect id bound at re-dispatch"
        );
        // Retries exhausted: the task is failed (kept), the flat instance
        // fails, and the same lease re-reporting gets 200.
        assert_eq!(fail(&cloud, &key, &reclaimed, "w1", true), 200, "{backend}");
        assert_eq!(fail(&cloud, &key, &reclaimed, "w1", true), 200, "{backend}");
        assert_eq!(
            cloud.task(retry.id).unwrap().state,
            WorkerTaskState::Failed,
            "{backend}"
        );
        cloud.wait_state(instance, "failed", LONG);
        assert_eq!(
            receipt_states(&cloud, instance),
            [EffectState::Unknown, EffectState::Unknown],
            "{backend}"
        );

        // No retry policy + non-retryable: fails straight away.
        let handler = format!("ext.once.{}", Uuid::now_v7().simple());
        let (instance, task) = dispatched(&cloud, &handler, None);
        let claimed = poll(&cloud, &key, &handler, "w2").remove(0);
        assert_eq!(fail(&cloud, &key, &claimed, "w2", false), 200, "{backend}");
        let failed = cloud.task(task.id).unwrap();
        assert_eq!(failed.state, WorkerTaskState::Failed, "{backend}");
        assert_eq!(failed.error_retryable, Some(false), "{backend}");
        cloud.wait_state(instance, "failed", LONG);
        assert_eq!(fail(&cloud, &key, &claimed, "w2", false), 200, "{backend}");
    }
}

/// Rolling upgrade: an older node re-dispatches a retry attempt with
/// `ON CONFLICT DO NOTHING`, so the pre-inserted retry row keeps
/// `awaiting_dispatch` and no effect id — unclaimable for current pollers.
/// The real reaper finalizes it once it is older than the grace period:
/// bound to the receipt the older dispatch created, claimable, and settled
/// normally. Finalization is idempotent and leaves fresh rows alone.
#[test]
fn rolling_upgrade_stranded_retry_row_self_heals() {
    for backend in backends() {
        let cloud = Cloud::start(&backend, |_| {});
        let backend = cloud.backend;
        let key = cloud.mint_key(&["worker"]);
        let handler = format!("ext.upgrade.{}", Uuid::now_v7().simple());
        let (instance, first) = dispatched(&cloud, &handler, Some(3));
        let claimed = poll(&cloud, &key, &handler, "w1").remove(0);
        assert_eq!(fail(&cloud, &key, &claimed, "w1", true), 200, "{backend}");
        let retry = wait_for("re-dispatch", LONG, || {
            cloud
                .tasks(instance)
                .into_iter()
                .find(|task| task.attempt == first.attempt + 1 && task.effect_id.is_some())
        });
        cloud.wait_state(instance, "waiting", LONG);
        let bound_effect = retry.effect_id.unwrap();

        // The older node's re-dispatch: receipt created, row never bound.
        cloud.strand_dispatch(retry.id, chrono::Duration::zero());
        let stranded = cloud.task(retry.id).unwrap();
        assert_eq!(stranded.effect_id, None, "{backend}");
        assert!(cloud.awaiting_dispatch(retry.id), "{backend}");
        assert!(
            poll(&cloud, &key, &handler, "w2").is_empty(),
            "{backend}: stranded rows are unclaimable"
        );
        // Younger than the grace period: the reaper leaves it alone.
        assert_eq!(
            cloud
                .block_on(orch8_engine::worker_lease::finalize_stranded_dispatches(
                    cloud.storage.as_ref(),
                    orch8_engine::worker_lease::STRANDED_DISPATCH_GRACE,
                ))
                .unwrap(),
            0,
            "{backend}"
        );
        assert!(cloud.awaiting_dispatch(retry.id), "{backend}");

        // Past the grace period, the running reaper heals it.
        cloud.strand_dispatch(retry.id, chrono::Duration::hours(1));
        let healed = wait_for("the reaper's finalization", LONG, || {
            cloud
                .task(retry.id)
                .filter(|task| task.effect_id.is_some() && !cloud.awaiting_dispatch(task.id))
        });
        assert_eq!(
            healed.effect_id,
            Some(bound_effect),
            "{backend}: bound to the receipt the older dispatch created"
        );
        assert_eq!(healed.continuity_epoch, retry.continuity_epoch, "{backend}");
        assert_eq!(healed.state, WorkerTaskState::Pending, "{backend}");
        assert_eq!(
            cloud
                .block_on(orch8_engine::worker_lease::finalize_stranded_dispatches(
                    cloud.storage.as_ref(),
                    std::time::Duration::ZERO,
                ))
                .unwrap(),
            0,
            "{backend}: idempotent"
        );

        // Claimable with that effect id; completion commits it.
        let reclaimed = poll(&cloud, &key, &handler, "w2").remove(0);
        assert_eq!(reclaimed["id"], retry.id.to_string(), "{backend}");
        assert_eq!(
            reclaimed["effect_id"],
            bound_effect.to_string(),
            "{backend}"
        );
        let (status, body) = cloud.call(
            &key,
            "POST",
            &format!("/workers/tasks/{}/complete", retry.id),
            Some(
                &json!({"worker_id": "w2", "claim_epoch": reclaimed["claim_epoch"],
                         "output": {"ok": true}}),
            ),
        );
        assert_eq!(status, 200, "{backend}: {body}");
        cloud.wait_state(instance, "completed", LONG);
        assert_eq!(
            receipt_states(&cloud, instance),
            [EffectState::Unknown, EffectState::Committed],
            "{backend}"
        );
    }
}

async fn grpc(cloud: &Cloud) -> Orch8ServiceClient<tonic::transport::Channel> {
    Orch8ServiceClient::connect(format!("http://{}", cloud.grpc_addr))
        .await
        .expect("grpc connect")
}

async fn grpc_poll(
    client: &mut Orch8ServiceClient<tonic::transport::Channel>,
    handler: &str,
) -> Vec<WorkerTask> {
    client
        .poll_tasks(proto::PollTasksRequest {
            handler_name: handler.into(),
            worker_id: "grpc-worker".into(),
            limit: 10,
        })
        .await
        .expect("poll")
        .into_inner()
        .tasks_json
        .iter()
        .map(|task| serde_json::from_str(task).expect("task json"))
        .collect()
}

#[test]
fn grpc_complete_fail_release_settle_effects_like_http() {
    for backend in backends() {
        let cloud = Cloud::start(&backend, |_| {});
        let backend = cloud.backend;

        // Complete → receipt committed by its stored id; instance completes.
        let handler = format!("grpc.ok.{}", Uuid::now_v7().simple());
        let (instance, _) = dispatched(&cloud, &handler, None);
        let task = cloud
            .block_on(async {
                let mut client = grpc(&cloud).await;
                grpc_poll(&mut client, &handler).await
            })
            .remove(0);
        cloud
            .block_on(async {
                grpc(&cloud)
                    .await
                    .complete_task(proto::CompleteTaskRequest {
                        task_id: task.id.to_string(),
                        worker_id: "grpc-worker".into(),
                        output_json: json!({"ok": true}).to_string(),
                        claim_epoch: task.claim_epoch,
                    })
                    .await
            })
            .expect("complete");
        assert_eq!(
            receipt_states(&cloud, instance),
            [EffectState::Committed],
            "{backend}"
        );
        cloud.wait_state(instance, "completed", LONG);
        let late = cloud.block_on(async {
            grpc(&cloud)
                .await
                .complete_task(proto::CompleteTaskRequest {
                    task_id: task.id.to_string(),
                    worker_id: "grpc-worker".into(),
                    output_json: "{}".into(),
                    claim_epoch: task.claim_epoch,
                })
                .await
        });
        assert_eq!(
            late.unwrap_err().code(),
            tonic::Code::FailedPrecondition,
            "{backend}: fenced"
        );

        // Fail (retryable, retry policy) → receipt unknown, next attempt;
        // re-reporting against the superseded task is NotFound.
        let handler = format!("grpc.fail.{}", Uuid::now_v7().simple());
        let (instance, first) = dispatched(&cloud, &handler, Some(2));
        let task = cloud
            .block_on(async { grpc_poll(&mut grpc(&cloud).await, &handler).await })
            .remove(0);
        let fail = |task: &WorkerTask| proto::FailTaskRequest {
            task_id: task.id.to_string(),
            worker_id: "grpc-worker".into(),
            message: "device error".into(),
            retryable: true,
            claim_epoch: task.claim_epoch,
        };
        cloud
            .block_on(async { grpc(&cloud).await.fail_task(fail(&task)).await })
            .expect("fail");
        assert_eq!(
            receipt_states(&cloud, instance)[0],
            EffectState::Unknown,
            "{backend}"
        );
        assert!(cloud.task(first.id).is_none(), "{backend}");
        let again = cloud.block_on(async { grpc(&cloud).await.fail_task(fail(&task)).await });
        assert_eq!(
            again.unwrap_err().code(),
            tonic::Code::NotFound,
            "{backend}"
        );
        let retry = wait_for("grpc retry", LONG, || {
            cloud
                .tasks(instance)
                .into_iter()
                .find(|task| task.attempt == first.attempt + 1 && task.effect_id.is_some())
        });
        assert_ne!(retry.effect_id, first.effect_id, "{backend}");

        // Release before start → pending, receipt untouched; release after
        // start → receipt unknown and the retry policy.
        let handler = format!("grpc.release.{}", Uuid::now_v7().simple());
        let (instance, first) = dispatched(&cloud, &handler, Some(2));
        let release = |task: &WorkerTask, started: bool| proto::ReleaseTaskRequest {
            task_id: task.id.to_string(),
            worker_id: "grpc-worker".into(),
            claim_epoch: task.claim_epoch,
            started,
        };
        let task = cloud
            .block_on(async { grpc_poll(&mut grpc(&cloud).await, &handler).await })
            .remove(0);
        cloud
            .block_on(async { grpc(&cloud).await.release_task(release(&task, false)).await })
            .expect("release before start");
        assert_eq!(
            cloud.task(first.id).unwrap().state,
            WorkerTaskState::Pending,
            "{backend}"
        );
        assert_eq!(
            receipt_states(&cloud, instance),
            [EffectState::Dispatched],
            "{backend}"
        );
        let stale =
            cloud.block_on(async { grpc(&cloud).await.release_task(release(&task, true)).await });
        assert_eq!(
            stale.unwrap_err().code(),
            tonic::Code::FailedPrecondition,
            "{backend}: the old claim is fenced"
        );
        let task = cloud
            .block_on(async { grpc_poll(&mut grpc(&cloud).await, &handler).await })
            .remove(0);
        assert!(task.claim_epoch > 1, "{backend}");
        cloud
            .block_on(async { grpc(&cloud).await.release_task(release(&task, true)).await })
            .expect("release after start");
        assert_eq!(
            receipt_states(&cloud, instance)[0],
            EffectState::Unknown,
            "{backend}"
        );
        wait_for("release retry", LONG, || {
            cloud
                .tasks(instance)
                .into_iter()
                .find(|task| task.attempt == first.attempt + 1 && task.effect_id.is_some())
        });

        // Output provenance: a desktop node claims over HTTP (so the claim
        // carries its runtime kind) and one task completes over HTTP, the
        // other over gRPC. Both record the same evidence: the audit event
        // with runtime kind + id and output digest, and a provenance entry.
        let key = cloud.mint_key(&["worker"]);
        let runtime = Uuid::now_v7();
        for transport in ["http", "grpc"] {
            let handler = format!("prov.{transport}.{}", Uuid::now_v7().simple());
            let (instance, _) = dispatched(&cloud, &handler, None);
            let now = chrono::Utc::now();
            let (status, body) = cloud.call(
                &key,
                "POST",
                "/workers/tasks/poll",
                Some(&json!({
                    "handler_name": handler, "worker_id": runtime.to_string(), "limit": 1,
                    "capabilities": {
                        "runtime_id": runtime, "kind": "desktop", "trust": "registered",
                        "handlers": [handler], "offline_capable": true,
                        "observed_at": now.to_rfc3339(),
                        "expires_at": (now + chrono::Duration::seconds(240)).to_rfc3339(),
                    },
                })),
            );
            assert_eq!(status, 200, "{backend}: {body}");
            let task = body["tasks"][0].clone();
            let output = json!({"rendered": transport});
            if transport == "http" {
                let (status, body) = cloud.call(
                    &key,
                    "POST",
                    &format!("/workers/tasks/{}/complete", task["id"].as_str().unwrap()),
                    Some(&json!({"worker_id": runtime.to_string(),
                                 "claim_epoch": task["claim_epoch"], "output": output})),
                );
                assert_eq!(status, 200, "{backend}: {body}");
            } else {
                cloud
                    .block_on(async {
                        grpc(&cloud)
                            .await
                            .complete_task(proto::CompleteTaskRequest {
                                task_id: task["id"].as_str().unwrap().to_owned(),
                                worker_id: runtime.to_string(),
                                output_json: output.to_string(),
                                claim_epoch: task["claim_epoch"].as_u64().unwrap(),
                            })
                            .await
                    })
                    .expect("grpc complete");
            }
            cloud.wait_state(instance, "completed", LONG);
            let audit = cloud
                .block_on(
                    cloud
                        .storage
                        .list_audit_log(orch8_types::ids::InstanceId::from_uuid(instance), 100),
                )
                .unwrap();
            let evidence = audit
                .iter()
                .find(|entry| entry.event_type == "worker_output_provenance")
                .unwrap_or_else(|| panic!("{backend}/{transport}: no provenance: {audit:?}"));
            assert_eq!(
                evidence.details["runtime_kind"], "desktop",
                "{backend}/{transport}"
            );
            assert_eq!(
                evidence.details["runtime_id"],
                runtime.to_string(),
                "{backend}/{transport}"
            );
            assert_eq!(
                evidence.details["task_id"], task["id"],
                "{backend}/{transport}"
            );
            assert_eq!(
                evidence.details["output_sha256"].as_str().map(str::len),
                Some(64),
                "{backend}/{transport}"
            );
            let tenant = orch8_types::ids::TenantId::unchecked(&cloud.tenant);
            let chain = cloud.block_on(async {
                let execution = cloud
                    .storage
                    .get_continuity_execution_by_instance(
                        &tenant,
                        orch8_types::ids::InstanceId::from_uuid(instance),
                    )
                    .await
                    .unwrap()
                    .expect("side-effecting steps enroll the instance");
                cloud
                    .storage
                    .list_provenance(&tenant, execution.continuity_id, 100)
                    .await
                    .unwrap()
            });
            assert!(
                chain.iter().any(|entry| entry.kind == "remote_step_output"
                    && entry
                        .redacted_summary
                        .as_deref()
                        .is_some_and(|summary| summary.contains("desktop runtime"))),
                "{backend}/{transport}: {chain:?}"
            );
        }
    }
}

fn create_credential(cloud: &Cloud, value: &str) -> String {
    let id = format!("cred-{}", Uuid::now_v7().simple());
    let (status, body) = cloud.root(
        "POST",
        "/credentials",
        Some(
            &json!({"id": id, "name": "api key", "kind": "api_key", "value": value,
                     "tenant_id": cloud.tenant}),
        ),
    );
    assert!(status == 200 || status == 201, "credential: {body}");
    id
}

/// A placement rejection on the flat (step-only) path used to fail the
/// instance with the reason only in a webhook payload. It is now in the
/// step's `__error__` output and a `remote_dispatch_rejected` audit event.
#[test]
fn flat_path_placement_rejection_is_observable() {
    for backend in backends() {
        let cloud = Cloud::start(&backend, |_| {});
        let backend = cloud.backend;
        let credential = create_credential(&cloud, "sk_live_x");
        let sequence = cloud.create_sequence(
            "form",
            &json!([step(
                "form",
                "form_x",
                &json!({"api_key": format!("credentials://{credential}"),
                        "$runtime": {"runtime_kinds": ["browser"]}}),
                None
            )]),
        );
        let instance = cloud.create_instance(sequence, &json!({}));
        cloud.wait_state(instance, "failed", LONG);

        let outputs = cloud.block_outputs(instance, "form");
        assert_eq!(outputs.len(), 1, "{backend}: {outputs:?}");
        assert_eq!(outputs[0]["output"]["__error__"], true, "{backend}");
        assert_eq!(
            outputs[0]["output"]["message"],
            "steps placed on browser runtimes cannot receive credentials",
            "{backend}"
        );
        let (status, audit) = cloud.root("GET", &format!("/instances/{instance}/audit"), None);
        assert_eq!(status, 200, "{backend}: {audit}");
        let entries = audit
            .as_array()
            .cloned()
            .or_else(|| audit["items"].as_array().cloned())
            .unwrap_or_default();
        let rejection = entries
            .iter()
            .find(|entry| entry["event_type"] == "remote_dispatch_rejected")
            .unwrap_or_else(|| panic!("{backend}: no rejection audit: {audit}"));
        assert_eq!(rejection["block_id"], "form", "{backend}");
        assert_eq!(
            rejection["details"]["error"],
            "steps placed on browser runtimes cannot receive credentials",
            "{backend}"
        );
        assert!(
            cloud.tasks(instance).is_empty(),
            "{backend}: nothing was enqueued"
        );
    }
}

/// (g) Through the real router: a browser session polling a handler never
/// receives the task whose params used `credentials://`, while a regular
/// worker can; the context a browser does receive is redacted.
#[test]
fn g_browser_session_never_receives_secrets() {
    for backend in backends() {
        let cloud = Cloud::start(&backend, |_| {});
        run_browser_secrets(&cloud, &backend);
    }
}

fn run_browser_secrets(cloud: &Cloud, backend: &Backend) {
    let backend = backend.name();
    let credential = create_credential(cloud, "sk_live_cred_value_123");
    let handler = format!("page_read_{}", Uuid::now_v7().simple());

    // X: params reference a credential (not placed on browsers, so it is
    // dispatched — but it must never reach a browser).
    let secret_sequence = cloud.create_sequence(
        "secret",
        &json!([step(
            "read",
            &handler,
            &json!({"api_key": format!("credentials://{credential}"), "selector": "#total"}),
            None
        )]),
    );
    let secret_instance = cloud.create_instance(secret_sequence, &json!({}));
    // Y: no credentials, but secret-shaped data and config in its context.
    let plain_sequence = cloud.create_sequence(
        "plain",
        &json!([step("read", &handler, &json!({"selector": "#x"}), None)]),
    );
    let (status, created) = cloud.root(
        "POST",
        "/instances",
        Some(&json!({
            "sequence_id": plain_sequence, "tenant_id": cloud.tenant, "namespace": "e2e",
            "context": {
                "data": {"page": "checkout", "stripe_secret": "sk_live_data_secret_456",
                         "saved_ref": format!("credentials://{credential}")},
                "config": {"stripe": "sk_live_config_789"}, "audit": [],
            },
        })),
    );
    assert_eq!(status, 201, "{backend}: {created}");
    let plain_instance: Uuid = created["id"].as_str().unwrap().parse().unwrap();

    let secret_task = wait_for("secret dispatch", LONG, || {
        cloud.tasks(secret_instance).into_iter().next()
    });
    let plain_task = wait_for("plain dispatch", LONG, || {
        cloud.tasks(plain_instance).into_iter().next()
    });
    assert!(secret_task.carries_credentials, "{backend}");
    assert!(!plain_task.carries_credentials, "{backend}");
    assert!(
        !secret_task.params.to_string().contains("credentials://"),
        "{backend}: resolved server-side"
    );

    let (status, session) = cloud.root(
        "POST",
        "/runtimes/browser-sessions",
        Some(&json!({"handlers": [handler], "ttl_secs": 300})),
    );
    assert_eq!(status, 201, "{backend}: {session}");
    let token = session["token"].as_str().unwrap();
    let runtime = session["runtime_id"].as_str().unwrap();
    let delivered = poll(cloud, token, &handler, runtime);
    assert!(
        delivered
            .iter()
            .all(|task| task["id"] != secret_task.id.to_string()),
        "{backend}: a credential-bearing task reached a browser"
    );
    let task = delivered
        .iter()
        .find(|task| task["id"] == plain_task.id.to_string())
        .unwrap_or_else(|| panic!("{backend}: plain task not delivered: {delivered:?}"));
    let wire = task.to_string();
    for secret in [
        "sk_live_cred_value_123",
        "sk_live_data_secret_456",
        "sk_live_config_789",
        "credentials://",
    ] {
        assert!(
            !wire.contains(secret),
            "{backend}: {secret} leaked to a browser: {wire}"
        );
    }
    let context = &task["context"];
    assert!(
        context["config"].is_null()
            || context["config"]
                .as_object()
                .is_some_and(serde_json::Map::is_empty),
        "{backend}: config dropped: {context}"
    );
    assert_eq!(
        context["data"]["page"], "checkout",
        "{backend}: page data kept"
    );
    assert!(
        context["data"].get("saved_ref").is_none(),
        "{backend}: credential-reference entries removed: {context}"
    );

    // Still pending for everyone else: a regular worker gets it.
    assert_eq!(
        cloud.task(secret_task.id).unwrap().state,
        WorkerTaskState::Pending,
        "{backend}"
    );
    let worker = cloud.mint_key(&["worker"]);
    let claimed = poll(cloud, &worker, &handler, "server-worker");
    assert!(
        claimed
            .iter()
            .any(|task| task["id"] == secret_task.id.to_string()),
        "{backend}: {claimed:?}"
    );
}
