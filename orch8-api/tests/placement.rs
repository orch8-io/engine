//! E2E tests for placement policies, rate budgets, create-time placement
//! validation, and priority lanes (docs/PLACEMENT.md).

use orch8_api::test_harness::spawn_test_server;
use reqwest::StatusCode;
use serde_json::{Value, json};

fn sequence(tenant: &str, name: &str, blocks: &Value, placement: Option<&Value>) -> Value {
    let mut body = json!({
        "id": uuid::Uuid::now_v7(),
        "tenant_id": tenant,
        "namespace": "default",
        "name": name,
        "version": 1,
        "blocks": blocks,
        "created_at": "2026-01-01T00:00:00Z"
    });
    if let Some(placement) = placement {
        body["placement"] = placement.clone();
    }
    body
}

#[tokio::test]
async fn placement_policies_put_get_round_trip_and_validation() {
    let srv = spawn_test_server().await;
    let client = reqwest::Client::new();
    let url = format!("{}/placement/policies", srv.v1_url());

    let empty: Value = client
        .get(&url)
        .header("X-Tenant-Id", "acme")
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(empty, json!({"items": []}));

    let policies = json!({"items": [{
        "name": "eu-billing",
        "match": {"sequence": "billing"},
        "require": {"residency": "eu", "labels": {"pci": "true"}},
        "prefer": {"labels": {"tier": "fast"}}
    }]});
    let resp = client
        .put(&url)
        .header("X-Tenant-Id", "acme")
        .json(&policies)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let stored: Value = client
        .get(&url)
        .header("X-Tenant-Id", "acme")
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(stored, policies);

    // Tenant isolation: another tenant sees nothing.
    let other: Value = client
        .get(&url)
        .header("X-Tenant-Id", "globex")
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(other, json!({"items": []}));

    // Duplicate names and empty facts are rejected.
    for bad in [
        json!({"items": [{"name": "a", "match": {}}, {"name": "a", "match": {}}]}),
        json!({"items": [{"name": "a", "match": {}, "require": {"residency": " "}}]}),
        json!({"items": [{"name": "", "match": {}}]}),
    ] {
        let resp = client
            .put(&url)
            .header("X-Tenant-Id", "acme")
            .json(&bad)
            .send()
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::BAD_REQUEST, "{bad}");
    }
}

#[tokio::test]
async fn rate_budget_crud() {
    let srv = spawn_test_server().await;
    let client = reqwest::Client::new();
    let base = srv.v1_url();

    let resp = client
        .put(format!("{base}/rate-budgets/stripe-api"))
        .header("X-Tenant-Id", "acme")
        .json(&json!({"capacity": 10, "refill_per_sec": 2.5}))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let budget: Value = resp.json().await.unwrap();
    assert_eq!(budget["key"], "stripe-api");
    assert_eq!(budget["tenant_id"], "acme");
    assert_eq!(budget["tokens"], 10.0);

    let list: Value = client
        .get(format!("{base}/rate-budgets"))
        .header("X-Tenant-Id", "acme")
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(list["items"].as_array().unwrap().len(), 1);

    for (key, body) in [
        ("bad key", json!({"capacity": 1, "refill_per_sec": 1.0})),
        ("ok", json!({"capacity": 0, "refill_per_sec": 1.0})),
        ("ok", json!({"capacity": 1, "refill_per_sec": 0.0})),
    ] {
        let resp = client
            .put(format!("{base}/rate-budgets/{key}"))
            .header("X-Tenant-Id", "acme")
            .json(&body)
            .send()
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::BAD_REQUEST, "{key} {body}");
    }

    let resp = client
        .delete(format!("{base}/rate-budgets/stripe-api"))
        .header("X-Tenant-Id", "acme")
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::NO_CONTENT);
    let resp = client
        .delete(format!("{base}/rate-budgets/stripe-api"))
        .header("X-Tenant-Id", "acme")
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn sequence_create_validates_placement() {
    let srv = spawn_test_server().await;
    let client = reqwest::Client::new();
    let url = format!("{}/sequences", srv.v1_url());

    let ok = sequence(
        "acme",
        "placed-ok",
        &json!([{"type": "step", "id": "charge", "handler": "stripe_charge",
                 "rate_budget": "stripe-api",
                 "placement": {"region": "eu-west-1", "labels": {"pci": "true"},
                               "affinity": "instance"}}]),
        Some(&json!({"residency": "eu", "priority_lane": "premium"})),
    );
    let resp = client
        .post(&url)
        .header("X-Tenant-Id", "acme")
        .json(&ok)
        .send()
        .await
        .unwrap();
    assert_eq!(
        resp.status(),
        StatusCode::CREATED,
        "{:?}",
        resp.text().await
    );

    // Hybrid: a remote-executable built-in may be placed on executors.
    let remote_builtin = sequence(
        "acme",
        "remote-builtin",
        &json!([{"type": "step", "id": "s", "handler": "http_request",
                 "placement": {"residency": "eu"}}]),
        None,
    );
    let resp = client
        .post(&url)
        .header("X-Tenant-Id", "acme")
        .json(&remote_builtin)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::CREATED);

    let rejected = [
        // A step cannot escape the sequence's residency.
        sequence(
            "acme",
            "escape",
            &json!([{"type": "step", "id": "s", "handler": "ext",
                     "placement": {"residency": "us"}}]),
            Some(&json!({"residency": "eu"})),
        ),
        // priority_lane is sequence-level.
        sequence(
            "acme",
            "step-lane",
            &json!([{"type": "step", "id": "s", "handler": "ext",
                     "placement": {"priority_lane": "premium"}}]),
            None,
        ),
        // Built-ins that manipulate engine state run on the engine node,
        // never on a placed worker (remote-executable ones such as
        // `http_request` may be placed; see below).
        sequence(
            "acme",
            "builtin",
            &json!([{"type": "step", "id": "s", "handler": "set_state",
                     "placement": {"residency": "eu"}}]),
            None,
        ),
        // Invalid rate budget key.
        sequence(
            "acme",
            "budget-key",
            &json!([{"type": "step", "id": "s", "handler": "ext",
                     "rate_budget": "has space"}]),
            None,
        ),
    ];
    for body in rejected {
        let resp = client
            .post(&url)
            .header("X-Tenant-Id", "acme")
            .json(&body)
            .send()
            .await
            .unwrap();
        assert!(
            resp.status().is_client_error(),
            "{}: {}",
            body["name"],
            resp.status()
        );
    }
}

#[tokio::test]
async fn priority_lane_sets_instance_priority() {
    let srv = spawn_test_server().await;
    let client = reqwest::Client::new();
    let base = srv.v1_url();

    let seq = sequence(
        "acme",
        "lane-seq",
        &json!([{"type": "step", "id": "s", "handler": "noop"}]),
        Some(&json!({"priority_lane": "batch"})),
    );
    let seq_id = seq["id"].clone();
    let resp = client
        .post(format!("{base}/sequences"))
        .header("X-Tenant-Id", "acme")
        .json(&seq)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::CREATED);

    let create = |extra: Value| {
        let mut body = json!({
            "sequence_id": seq_id,
            "tenant_id": "acme",
            "namespace": "default",
            "next_fire_at": "2099-01-01T00:00:00Z"
        });
        for (key, value) in extra.as_object().unwrap() {
            body[key] = value.clone();
        }
        body
    };
    for (extra, expected) in [
        (json!({}), "Low"),
        (json!({"priority_lane": "premium"}), "High"),
        (
            json!({"priority": "Critical", "priority_lane": "batch"}),
            "Critical",
        ),
    ] {
        let resp = client
            .post(format!("{base}/instances"))
            .header("X-Tenant-Id", "acme")
            .json(&create(extra.clone()))
            .send()
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::CREATED, "{extra}");
        let id = resp.json::<Value>().await.unwrap()["id"].clone();
        let inst: Value = client
            .get(format!("{base}/instances/{}", id.as_str().unwrap()))
            .header("X-Tenant-Id", "acme")
            .send()
            .await
            .unwrap()
            .json()
            .await
            .unwrap();
        assert_eq!(inst["priority"], expected, "{extra}: {inst}");
    }
}
