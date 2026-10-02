//! Hybrid executors behind a **shared, header-routed** load balancer — the
//! topology of Orch8 Cloud's managed engines, which all share one Fly app
//! and are reached only with a `fly-force-instance-id` header.
//!
//! The front ([`orch8_e2e::RoutedFront`]) forwards a request to this engine
//! only when it carries the routing header with this engine's id, and serves
//! gRPC (h2) and REST (HTTP/1.1) on different ports. Proven:
//!
//! 1. a join token with `endpoint` (gRPC port), `api_url` (REST port) and
//!    `headers` joins an executor over the gRPC worker stream **and** the
//!    managed-control session, and a placed step runs on it;
//! 2. an executor on the HTTP polling transport uses `api_url` with the same
//!    headers and runs a placed step;
//! 3. every request the front saw from those executors carried the header,
//!    on both listeners;
//! 4. the same token without `headers` never reaches the engine (the front
//!    refuses every request and no runtime registers).

use std::collections::BTreeMap;
use std::time::{Duration, Instant};

use orch8_e2e::{Backend, Cloud, CloudOptions, ExecutorProcess, RoutedFront, backends};
use orch8_types::join_token::JoinToken;
use serde_json::json;
use uuid::Uuid;

const ROUTE_HEADER: &str = "fly-force-instance-id";
const MACHINE_ID: &str = "148e21ea7d9389";

fn wait_until<T>(what: &str, limit: Duration, mut probe: impl FnMut() -> Option<T>) -> T {
    let deadline = Instant::now() + limit;
    loop {
        if let Some(value) = probe() {
            return value;
        }
        assert!(
            Instant::now() < deadline,
            "timed out after {limit:?} waiting for {what}"
        );
        std::thread::sleep(Duration::from_millis(50));
    }
}

fn replica(token: &JoinToken, host: &str) -> String {
    orch8_engine::remote_executor::replica_runtime_id(token.runtime_id, &token.worker_id(host))
        .to_string()
}

fn registered(cloud: &Cloud, runtime_id: &str) -> bool {
    cloud
        .runtimes()
        .into_iter()
        .any(|r| r.runtime_id.to_string() == runtime_id && !r.draining)
}

#[test]
fn hybrid_executors_through_a_header_routed_front() {
    for backend in backends() {
        run(&backend);
    }
}

#[allow(clippy::too_many_lines)]
fn run(backend: &Backend) {
    let cloud = Cloud::start_with(
        backend,
        |_| {},
        CloudOptions {
            scheduler: orch8_types::config::SchedulerConfig {
                tick_interval_ms: 50,
                ..orch8_types::config::SchedulerConfig::default()
            },
            grpc_auth: true,
        },
    );
    let front = RoutedFront::start(&cloud, ROUTE_HEADER, MACHINE_ID);
    let dir = tempfile::tempdir().expect("tempdir");

    let token = JoinToken {
        v: 1,
        endpoint: front.grpc_endpoint.clone(),
        api_key: cloud.mint_key(&["worker"]),
        tenant_id: cloud.tenant.clone(),
        runtime_id: Uuid::now_v7(),
        worker_id_prefix: "acme-routed".into(),
        labels: BTreeMap::from([("site".into(), "vpc".into())]),
        region: None,
        api_url: Some(front.api_url.clone()),
        headers: BTreeMap::from([(ROUTE_HEADER.into(), MACHINE_ID.into())]),
    };
    let env = |transport: &str| -> Vec<(&'static str, String)> {
        vec![
            (
                "ORCH8_EXECUTOR_CA_CERT",
                orch8_e2e::tls_fixture("ca.pem").display().to_string(),
            ),
            ("ORCH8_EXECUTOR_POLL_INTERVAL_MS", "100".into()),
            ("ORCH8_EXECUTOR_HEARTBEAT_SECS", "1".into()),
            ("ORCH8_EXECUTOR_DRAIN_TIMEOUT_SECS", "1".into()),
            ("ORCH8_EXECUTOR_TRANSPORT", transport.to_owned()),
        ]
    };
    let placed = cloud.create_sequence(
        "routed-placed",
        &json!([
            {"type": "step", "id": "vpc_step", "handler": "transform",
             "placement": {"labels": {"site": "vpc"}},
             "params": {"order": "{{context.data.order}}"}}
        ]),
    );
    let ran_on = |instance: Uuid| -> String {
        let tasks = cloud.tasks(instance);
        assert_eq!(tasks.len(), 1, "{tasks:?}");
        tasks[0].worker_id.clone().expect("claimed by a worker")
    };

    // ---- 1: gRPC worker stream + managed control through the front ----
    let grpc_exec = ExecutorProcess::spawn("g1", &token.encode(), &env("grpc"), dir.path());
    let g1 = replica(&token, "g1");
    wait_until(
        "the gRPC executor to register",
        Duration::from_secs(30),
        || registered(&cloud, &g1).then_some(()),
    );
    // The token's runtime id is the managed-control session.
    let control = token.runtime_id.to_string();
    wait_until(
        "the managed-control session to register",
        Duration::from_secs(30),
        || registered(&cloud, &control).then_some(()),
    );
    let first = cloud.create_instance(placed, &json!({"order": "R-1"}));
    cloud.wait_state(first, "completed", Duration::from_secs(60));
    assert_eq!(ran_on(first), g1);
    drop(grpc_exec);

    // ---- 2: HTTP polling through the REST port (api_url) ----
    let http_exec = ExecutorProcess::spawn("h1", &token.encode(), &env("http"), dir.path());
    let h1 = replica(&token, "h1");
    wait_until(
        "the HTTP executor to register",
        Duration::from_secs(30),
        || registered(&cloud, &h1).then_some(()),
    );
    let second = cloud.create_instance(placed, &json!({"order": "R-2"}));
    cloud.wait_state(second, "completed", Duration::from_secs(60));
    assert_eq!(ran_on(second), h1);
    drop(http_exec);

    // ---- 3: every request carried the routing header, on both ports ----
    let hits = front.hits();
    assert!(
        hits.iter()
            .any(|h| h.listener == "grpc" && h.path.ends_with("/WorkerStream")),
        "{hits:?}"
    );
    assert!(
        hits.iter()
            .any(|h| h.listener == "rest" && h.path.starts_with("/api/v1/workers/")),
        "{hits:?}"
    );
    let refused: Vec<_> = hits.iter().filter(|h| !h.routed).collect();
    assert!(
        refused.is_empty(),
        "requests without the routing header: {refused:?}"
    );

    // ---- 4: without the headers the shared front reaches no engine ----
    let unrouted = JoinToken {
        runtime_id: Uuid::now_v7(),
        worker_id_prefix: "acme-unrouted".into(),
        headers: BTreeMap::new(),
        ..token.clone()
    };
    let lost = ExecutorProcess::spawn("u1", &unrouted.encode(), &env("auto"), dir.path());
    wait_until(
        "the front to refuse the unrouted executor on both ports",
        Duration::from_secs(30),
        || {
            let refused = front
                .hits()
                .into_iter()
                .filter(|h| !h.routed)
                .collect::<Vec<_>>();
            (refused.iter().any(|h| h.listener == "grpc")
                && refused.iter().any(|h| h.listener == "rest"))
            .then_some(())
        },
    );
    std::thread::sleep(Duration::from_secs(1));
    assert!(!registered(&cloud, &replica(&unrouted, "u1")));
    assert!(!registered(&cloud, &unrouted.runtime_id.to_string()));
    drop(lost);
}
