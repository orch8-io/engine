//! Crash recovery of the real `orch8-server` process (T3-GAP-01, T3-GAP-05
//! shape): `SIGKILL` mid-step, restart on the same database, and two
//! schedulers sharing one database. Side effects are counted outside the
//! server (the test's own worker, or a loopback receiver), so "exactly
//! once" is observed, not inferred from the server's own bookkeeping.
//!
//! Pattern to copy: `ControlPlane::new(&backend)`, `plane.spawn(..)`,
//! drive it over HTTP, `node.kill()` + `node.restart()`, assert on what the
//! outside world saw plus the instance's outputs.

use std::time::Duration;

use serde_json::{Value, json};
use uuid::Uuid;

use orch8_e2e::control_plane::ControlPlane;
use orch8_e2e::{Backend, backends, wait_for};
use orch8_engine::testing::http::MockHttp;

const LONG: Duration = Duration::from_secs(45);

fn step(id: &str, handler: &str, params: &Value) -> Value {
    json!({"type": "step", "id": id, "handler": handler, "params": params, "cancellable": true})
}

/// Short crash-recovery window; worker claims are not reaped during the
/// test (the worker is alive, only the server crashes).
fn crash_env() -> Vec<(&'static str, String)> {
    vec![
        ("ORCH8_STALE_INSTANCE_THRESHOLD_SECS", "3".into()),
        ("ORCH8_WORKER_REAPER_STALE_SECS", "300".into()),
    ]
}

/// T3-GAP-01 (worker variant): the server is killed (`SIGKILL`) while an external
/// worker holds step `a`. After a restart on the same database the worker's
/// completion (old task id and claim epoch) is accepted, the instance
/// completes, and `a` ran exactly once: one task was ever handed out and
/// one output was recorded.
#[test]
fn sigkill_while_a_worker_holds_the_step_completes_it_exactly_once() {
    for backend in backends() {
        let plane = ControlPlane::new(&backend);
        let mut node = plane.spawn("node-a", &crash_env());
        let handler = format!("fx.{}", Uuid::now_v7().simple());
        let sequence = plane.create_sequence(
            &node,
            "crash",
            &json!([
                step("a", &handler, &json!({})),
                step("b", "noop", &json!({}))
            ]),
        );
        let id = plane.create_instance(&node, sequence, &json!({}));
        let task = wait_for("task for `a` dispatched", LONG, || {
            plane.poll_tasks(&node, &handler, "w1").into_iter().next()
        });

        node.kill();
        node.restart();

        let status = plane.complete_task(&node, &task, "w1", &json!({"fx": "done"}));
        assert_eq!(status, 200, "{}: completion after restart", plane.backend);
        plane.wait_state(&node, id, "completed", LONG);
        assert!(
            plane.poll_tasks(&node, &handler, "w2").is_empty(),
            "{}: `a` was dispatched again after the crash",
            plane.backend
        );
        assert_eq!(
            plane.block_outputs(&node, id, "a").len(),
            1,
            "{}",
            plane.backend
        );
    }
}

/// T3-GAP-01 (in-process step variant, needs
/// `ORCH8_STALE_INSTANCE_THRESHOLD_SECS`): `a` POSTs to a receiver, then
/// `b` sleeps; the server is killed (`SIGKILL`) during `b`, leaving the instance
/// `running` with a dead lease. After a restart the stale-instance reaper
/// reclaims it, `b` re-runs and the instance completes, while `a`'s side
/// effect (already committed) is not repeated.
#[test]
fn sigkill_mid_step_recovers_the_instance_without_repeating_committed_effects() {
    for backend in backends() {
        let plane = ControlPlane::new(&backend);
        let receiver = plane.block_on(MockHttp::start(Vec::new()));
        let mut env = crash_env();
        // The receiver is on loopback.
        env.push(("ORCH8_ALLOW_INTERNAL_URLS", "1".into()));
        let mut node = plane.spawn("node-a", &env);
        let sequence = plane.create_sequence(
            &node,
            "crash-sleep",
            &json!([
                step(
                    "a",
                    "http_request",
                    &json!({"url": receiver.url("/fx"), "method": "POST", "body": "{}"})
                ),
                step("b", "sleep", &json!({"duration_ms": 5000})),
                step("c", "noop", &json!({})),
            ]),
        );
        let id = plane.create_instance(&node, sequence, &json!({}));
        wait_for("`a` committed and `b` in flight", LONG, || {
            let running = plane.instance(&node, id)["state"] == "running";
            (running && plane.block_outputs(&node, id, "a").len() == 1).then_some(())
        });

        node.kill();
        node.restart();

        plane.wait_state(&node, id, "completed", LONG);
        assert_eq!(receiver.request_count(), 1, "{}: `a` re-ran", plane.backend);
        assert_eq!(
            plane.block_outputs(&node, id, "a").len(),
            1,
            "{}",
            plane.backend
        );
        assert_eq!(
            plane.block_outputs(&node, id, "c").len(),
            1,
            "{}",
            plane.backend
        );
    }
}

/// T3-GAP-05 shape (Postgres only): two schedulers on one database run a
/// batch of instances; every instance completes and every side effect is
/// delivered exactly once (no instance is executed by both nodes).
#[test]
fn two_schedulers_on_one_database_run_each_instance_exactly_once() {
    for backend in backends() {
        if matches!(backend, Backend::Sqlite) {
            continue;
        }
        let plane = ControlPlane::new(&backend);
        let receiver = plane.block_on(MockHttp::start(Vec::new()));
        let env = vec![("ORCH8_ALLOW_INTERNAL_URLS", "1".to_owned())];
        let a = plane.spawn("node-a", &env);
        let b = plane.spawn("node-b", &env);
        let sequence = plane.create_sequence(
            &a,
            "two-nodes",
            &json!([step(
                "fx",
                "http_request",
                &json!({"url": receiver.url("/fx"), "method": "POST",
                        "body": "n={{context.data.n}}"})
            )]),
        );
        let ids: Vec<Uuid> = (0..20)
            .map(|n| {
                let via = if n % 2 == 0 { &a } else { &b };
                plane.create_instance(via, sequence, &json!({"n": n}))
            })
            .collect();

        for id in &ids {
            plane.wait_state(&a, *id, "completed", LONG);
        }
        let mut bodies: Vec<String> = receiver
            .requests()
            .iter()
            .map(orch8_engine::testing::http::Recorded::body_text)
            .collect();
        bodies.sort();
        let mut expected: Vec<String> = (0..20).map(|n| format!("n={n}")).collect();
        expected.sort();
        assert_eq!(bodies, expected, "each instance's effect exactly once");
    }
}
