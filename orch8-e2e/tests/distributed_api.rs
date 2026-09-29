//! Worker-protocol guarantees through the real transports (HTTP router with
//! API-key auth, gRPC) and the real scheduler, on `SQLite` and Postgres:
//!
//! * HTTP `fail` is one fenced resolution (effect `unknown`, retry policy,
//!   idempotent re-report, 404 once superseded by a retry);
//! * gRPC complete / fail / release settle effects like HTTP;
//! * a placement rejection on the flat path is observable (output + audit);
//! * a browser session never receives a credential-bearing task, and the
//!   context it does receive is redacted;
//! * a device session (a phone's scoped credential) reaches only its own
//!   device's mobile routes, the lease protocol as its own runtime, and the
//!   delegation calls for executions its runtime owns — everything else is
//!   refused.
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

// ---------------------------------------------------------------------------
// Device sessions: the phone's scoped credential
// ---------------------------------------------------------------------------

/// A live capability registration for `runtime` of `kind`.
fn runtime_caps(runtime: &str, kind: &str, handlers: &[&str]) -> Value {
    let now = chrono::Utc::now();
    json!({
        "runtime_id": runtime, "kind": kind, "trust": "registered",
        "handlers": handlers, "offline_capable": true,
        "observed_at": now.to_rfc3339(),
        "expires_at": (now + chrono::Duration::seconds(240)).to_rfc3339(),
    })
}

fn mint_device_session(cloud: &Cloud, device: &str, runtime: &str, ttl_secs: u32) -> String {
    let (status, session) = cloud.root(
        "POST",
        "/runtimes/device-sessions",
        Some(&json!({"device_id": device, "runtime_id": runtime,
                     "handlers": ["phone_scan"], "ttl_secs": ttl_secs})),
    );
    assert_eq!(status, 201, "{session}");
    assert_eq!(session["runtime_id"], runtime);
    assert_eq!(session["device_id"], device);
    session["token"].as_str().unwrap().to_owned()
}

/// (h) Every action outside a device session's scope is refused through the
/// real router, on both backends — including object-level checks on the
/// routes it may call (other devices, other runtimes, executions and
/// delegations its runtime neither owns nor serves).
#[test]
fn h_device_session_is_scoped_to_its_own_device_and_runtime() {
    for backend in backends() {
        let cloud = Cloud::start(&backend, |_| {});
        run_device_session_scope(&cloud, &backend);
    }
}

fn run_device_session_scope(cloud: &Cloud, backend: &Backend) {
    let backend = backend.name();
    let tenant = cloud.tenant.clone();
    let (device_a, runtime_a) = ("phone-a", Uuid::now_v7().to_string());
    let (device_b, runtime_b) = ("phone-b", Uuid::now_v7().to_string());
    let desktop = Uuid::now_v7().to_string();
    let token_a = mint_device_session(cloud, device_a, &runtime_a, 600);
    let token_b = mint_device_session(cloud, device_b, &runtime_b, 600);
    let expect = |token: &str, method: &str, path: &str, body: Option<Value>, want: &[u16]| {
        let (status, answer) = cloud.call(token, method, path, body.as_ref());
        assert!(
            want.contains(&status),
            "{backend}: {method} {path} -> {status} (want {want:?}): {answer}"
        );
        answer
    };

    // Minting is operator-only and the reserved capability is not storable.
    expect(
        &token_a,
        "POST",
        "/runtimes/device-sessions",
        Some(json!({"device_id": device_a, "runtime_id": runtime_a})),
        &[403],
    );
    let worker = cloud.mint_key(&["worker", "device"]);
    expect(
        &worker,
        "POST",
        "/runtimes/device-sessions",
        Some(json!({"device_id": device_a, "runtime_id": runtime_a})),
        &[403],
    );
    let (status, body) = cloud.root(
        "POST",
        "/api-keys",
        Some(&json!({
        "tenant_id": tenant, "name": "x", "capabilities": ["device_node"]})),
    );
    assert_eq!(status, 400, "{backend}: {body}");

    // Own device and runtime: allowed.
    expect(
        &token_a,
        "POST",
        "/mobile/devices/register",
        Some(json!({"device_id": device_a, "platform": "ios"})),
        &[201],
    );
    let advertised = expect(
        &token_a,
        "POST",
        &format!("/mobile/devices/{device_a}/runtime"),
        Some(
            json!({"capabilities": runtime_caps(&runtime_a, "mobile", &["phone_scan", "charge_card"])}),
        ),
        &[201],
    );
    assert_eq!(
        advertised["handlers"],
        json!(["phone_scan"]),
        "{backend}: advertised handlers are clamped to the grant"
    );
    expect(
        &token_a,
        "POST",
        "/mobile/sync",
        Some(json!({"device_id": device_a})),
        &[200],
    );
    expect(
        &token_a,
        "POST",
        "/workers/tasks/poll",
        Some(json!({"handler_name": "phone_scan", "worker_id": runtime_a})),
        &[200],
    );
    expect(
        &token_a,
        "GET",
        &format!("/runtimes?tenant_id={tenant}"),
        None,
        &[200],
    );
    let own = expect(
        &token_a,
        "POST",
        "/continuity/executions",
        Some(json!({"tenant_id": tenant, "instance_id": Uuid::now_v7(),
                     "runtime_id": runtime_a, "hosted_by_runtime": true})),
        &[201],
    );

    // Route families outside the scope: refused before routing.
    let id = Uuid::now_v7();
    for (method, path, body) in [
        ("GET", "/workers/tasks".to_owned(), None),
        ("GET", "/workers/tasks/stats".to_owned(), None),
        ("GET", format!("/workers/tasks/{id}/attempts"), None),
        (
            "POST",
            "/workers/tasks/poll/queue".to_owned(),
            Some(json!({"queue_name": "q", "handler_name": "phone_scan", "worker_id": runtime_a})),
        ),
        (
            "POST",
            "/workers/commands".to_owned(),
            Some(json!({"worker_id": runtime_a})),
        ),
        ("GET", format!("/workers/{runtime_a}/commands"), None),
        ("GET", "/workers".to_owned(), None),
        (
            "POST",
            "/runtimes/browser-sessions".to_owned(),
            Some(json!({"handlers": ["x"]})),
        ),
        (
            "POST",
            "/runtimes/register".to_owned(),
            Some(
                json!({"tenant_id": tenant, "capabilities": runtime_caps(&runtime_a, "desktop", &[])}),
            ),
        ),
        ("POST", "/sequences".to_owned(), Some(json!({}))),
        ("GET", format!("/sequences/{id}"), None),
        (
            "POST",
            "/credentials".to_owned(),
            Some(json!({"id": "c", "value": "v"})),
        ),
        ("GET", "/credentials".to_owned(), None),
        (
            "POST",
            "/api-keys".to_owned(),
            Some(json!({"tenant_id": tenant, "name": "x"})),
        ),
        ("POST", "/instances".to_owned(), Some(json!({}))),
        ("GET", "/instances".to_owned(), None),
        ("GET", "/mobile/devices".to_owned(), None),
        ("GET", "/mobile/approvals".to_owned(), None),
        ("GET", "/mobile/status".to_owned(), None),
        (
            "POST",
            "/mobile/commands".to_owned(),
            Some(json!({"device_id": device_b})),
        ),
        (
            "POST",
            format!("/mobile/approvals/{id}/resolve"),
            Some(json!({})),
        ),
        (
            "GET",
            format!(
                "/continuity/executions/{}?tenant_id={tenant}",
                own["continuity_id"].as_str().unwrap()
            ),
            None,
        ),
        (
            "POST",
            "/continuity/grants/consume".to_owned(),
            Some(json!({})),
        ),
        ("POST", "/continuity/handoffs".to_owned(), Some(json!({}))),
        (
            "POST",
            "/continuity/capsules/import".to_owned(),
            Some(json!({})),
        ),
    ] {
        expect(&token_a, method, &path, body, &[403]);
    }

    // Object-level: other devices and runtimes.
    expect(
        &token_a,
        "POST",
        "/mobile/devices/register",
        Some(json!({"device_id": device_b, "platform": "ios"})),
        &[403],
    );
    expect(
        &token_a,
        "POST",
        "/mobile/sync",
        Some(json!({"device_id": device_b})),
        &[403],
    );
    expect(
        &token_a,
        "POST",
        &format!("/mobile/devices/{device_b}/runtime"),
        Some(json!({"capabilities": runtime_caps(&runtime_a, "mobile", &[])})),
        &[403],
    );
    expect(
        &token_a,
        "POST",
        &format!("/mobile/devices/{device_a}/runtime"),
        Some(json!({"capabilities": runtime_caps(&runtime_b, "mobile", &[])})),
        &[403],
    );
    expect(
        &token_a,
        "POST",
        "/mobile/sync",
        Some(json!({"device_id": device_a,
        "step_delegations": [{"request_id": "r", "instance_id": "i", "block_id": "b",
                              "handler": "h", "params": {"k": "credentials://stripe"}}]})),
        &[403],
    );
    expect(
        &token_a,
        "POST",
        "/workers/tasks/poll",
        Some(json!({"handler_name": "phone_scan", "worker_id": runtime_b})),
        &[403],
    );
    expect(
        &token_a,
        "POST",
        "/workers/tasks/poll",
        Some(json!({"handler_name": "charge_card", "worker_id": runtime_a})),
        &[403],
    );
    expect(
        &token_a,
        "POST",
        &format!("/workers/tasks/{id}/complete"),
        Some(json!({"worker_id": runtime_b, "claim_epoch": 1, "output": {}})),
        &[403],
    );
    expect(
        &token_a,
        "POST",
        "/continuity/executions",
        Some(json!({"tenant_id": tenant, "instance_id": Uuid::now_v7(),
                     "runtime_id": runtime_b, "hosted_by_runtime": true})),
        &[403],
    );
    expect(
        &token_a,
        "POST",
        "/continuity/executions",
        Some(json!({"tenant_id": tenant, "instance_id": Uuid::now_v7(),
                     "runtime_id": runtime_a})),
        &[403],
    );

    // A delegation between two other runtimes (phone B → desktop), set up by
    // the operator: phone A can neither grant, claim, nor read it.
    for (runtime, kind, handlers) in [
        (&runtime_b, "mobile", vec![]),
        (
            &desktop,
            "desktop",
            vec![
                orch8_engine::delegation::DELEGATION_HANDLER,
                "desktop_classify",
            ],
        ),
    ] {
        let (status, body) = cloud.root("POST", "/runtimes/register",
            Some(&json!({"tenant_id": tenant, "capabilities": runtime_caps(runtime, kind, &handlers)})));
        assert_eq!(status, 201, "{backend}: {body}");
    }
    let sub_sequence = cloud.create_sequence(
        "classify",
        &json!([step("classify", "desktop_classify", &json!({}), None)]),
    );
    let (status, execution_b) = cloud.root(
        "POST",
        "/continuity/executions",
        Some(&json!({"tenant_id": tenant, "instance_id": Uuid::now_v7(),
                      "runtime_id": runtime_b, "hosted_by_runtime": true})),
    );
    assert_eq!(status, 201, "{backend}: {execution_b}");
    let grant_body = json!({"tenant_id": tenant, "continuity_id": execution_b["continuity_id"],
        "destination_runtime_id": desktop, "allowed_actions": ["accept"], "ttl_seconds": 300});
    expect(
        &token_a,
        "POST",
        "/continuity/grants",
        Some(grant_body.clone()),
        &[404],
    );
    let mut resume = grant_body.clone();
    resume["allowed_actions"] = json!(["resume"]);
    expect(&token_b, "POST", "/continuity/grants", Some(resume), &[403]);
    let grant = expect(
        &token_b,
        "POST",
        "/continuity/grants",
        Some(grant_body),
        &[201],
    );
    let delegation_id = Uuid::now_v7();
    let claim = json!({
        "tenant_id": tenant,
        "delegation": {
            "id": delegation_id, "tenant_id": tenant,
            "parent_continuity_id": execution_b["continuity_id"],
            "parent_epoch": execution_b["epoch"],
            "source_runtime_id": runtime_b, "destination_runtime_id": desktop,
            "sub_sequence_id": sub_sequence,
            "grant_id": grant["signed_grant"]["grant"]["id"],
            "expires_at": (chrono::Utc::now() + chrono::Duration::minutes(4)).to_rfc3339(),
        },
        "signed_grant": grant["signed_grant"], "token": grant["token"], "input": {},
    });
    expect(
        &token_a,
        "POST",
        "/continuity/delegations/claim",
        Some(claim.clone()),
        &[403],
    );
    expect(
        &token_b,
        "POST",
        "/continuity/delegations/claim",
        Some(claim),
        &[200],
    );
    let read = format!("/continuity/delegations/{delegation_id}?tenant_id={tenant}");
    expect(&token_a, "GET", &read, None, &[404]);
    let status = expect(&token_b, "GET", &read, None, &[200]);
    assert_eq!(
        status["delegation"]["source_runtime_id"],
        runtime_b.as_str(),
        "{backend}"
    );

    // Tenant, forgery, and expiry.
    let status = cloud.block_on(async {
        let response = reqwest::Client::new()
            .post(format!("{}/mobile/sync", cloud.v1()))
            .header("x-api-key", &token_a)
            .header("X-Tenant-Id", "someone-else")
            .json(&json!({"device_id": device_a}))
            .send()
            .await
            .unwrap();
        response.status().as_u16()
    });
    assert_eq!(status, 403, "{backend}: foreign X-Tenant-Id");
    let (payload, _) = token_a["dst_".len()..].split_once('.').unwrap();
    let forged = format!("dst_{payload}.{}", "A".repeat(43));
    expect(
        &forged,
        "POST",
        "/mobile/sync",
        Some(json!({"device_id": device_a})),
        &[401],
    );
    let short = mint_device_session(cloud, device_a, &runtime_a, 1);
    std::thread::sleep(Duration::from_millis(2_100));
    expect(
        &short,
        "POST",
        "/mobile/sync",
        Some(json!({"device_id": device_a})),
        &[401],
    );
}
