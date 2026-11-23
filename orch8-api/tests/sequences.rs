//! E2E tests for the Sequences API.

use orch8_api::test_harness::spawn_test_server;
use reqwest::StatusCode;
use serde_json::json;
use uuid::Uuid;

#[tokio::test]
async fn create_sequence_and_get_by_id_round_trip() {
    let srv = spawn_test_server().await;
    let client = reqwest::Client::new();

    let seq_id = Uuid::now_v7();
    let body = json!({
        "id": seq_id,
        "tenant_id": "t1",
        "namespace": "ns1",
        "name": "test-seq",
        "version": 1,
        "deprecated": false,
        "blocks": [
            {
                "type": "step",
                "id": "s1",
                "handler": "noop",
                "params": {},
                "cancellable": true
            }
        ],
        "interceptors": null,
        "created_at": chrono::Utc::now().to_rfc3339()
    });

    let resp = client
        .post(format!("{}/sequences", srv.base_url))
        .header("X-Tenant-Id", "t1")
        .json(&body)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::CREATED);

    let resp = client
        .get(format!("{}/sequences/{seq_id}", srv.base_url))
        .header("X-Tenant-Id", "t1")
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let fetched: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(fetched["name"], "test-seq");
    assert_eq!(fetched["version"], 1);
}

#[tokio::test]
async fn create_sequence_accepts_yaml_content_type() {
    let srv = spawn_test_server().await;
    let client = reqwest::Client::new();
    let seq_id = Uuid::now_v7();
    let yaml = format!(
        "id: {seq_id}\ntenant_id: t1\nnamespace: ns1\nname: yaml-seq\nversion: 1\n\
         created_at: '{}'\nblocks:\n  - type: step\n    id: s1\n    handler: noop\n    \
         params:\n      message: 'hello: yaml'\n",
        chrono::Utc::now().to_rfc3339()
    );
    let resp = client
        .post(format!("{}/sequences", srv.base_url))
        .header("X-Tenant-Id", "t1")
        .header("Content-Type", "application/yaml")
        .body(yaml)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::CREATED);

    let fetched: serde_json::Value = client
        .get(format!("{}/sequences/{seq_id}", srv.base_url))
        .header("X-Tenant-Id", "t1")
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(fetched["name"], "yaml-seq");
    assert_eq!(fetched["blocks"][0]["params"]["message"], "hello: yaml");

    // A YAML syntax error is a 400 that points at the line and column.
    let resp = client
        .post(format!("{}/sequences", srv.base_url))
        .header("X-Tenant-Id", "t1")
        .header("Content-Type", "text/yaml; charset=utf-8")
        .body("name: x\nblocks:\n  - id: [broken\n")
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
    let body: serde_json::Value = resp.json().await.unwrap();
    let message = body["error"]["message"].as_str().unwrap();
    assert!(message.contains("invalid YAML at line"), "{message}");
    assert!(message.contains("column"), "{message}");

    // The same YAML is accepted by the draft preflight endpoint.
    let resp = client
        .post(format!("{}/sequences/preflight", srv.base_url))
        .header("X-Tenant-Id", "t1")
        .header("Content-Type", "application/x-yaml")
        .body(format!(
            "id: {}\ntenant_id: t1\nnamespace: ns1\nname: yaml-pre\nversion: 1\n\
             created_at: '2026-01-01T00:00:00Z'\nblocks:\n  - {{type: step, id: a, handler: noop}}\n",
            Uuid::now_v7()
        ))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let report: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(report["sequence_name"], "yaml-pre");
}

#[tokio::test]
async fn validation_errors_carry_stable_error_code_and_docs_url() {
    let srv = spawn_test_server().await;
    let client = reqwest::Client::new();
    let body = json!({
        "id": Uuid::now_v7(),
        "tenant_id": "t1",
        "namespace": "ns1",
        "name": "dup",
        "version": 1,
        "blocks": [
            {"type": "step", "id": "a", "handler": "noop"},
            {"type": "step", "id": "a", "handler": "noop"}
        ],
        "created_at": chrono::Utc::now().to_rfc3339()
    });
    let resp = client
        .post(format!("{}/sequences", srv.base_url))
        .header("X-Tenant-Id", "t1")
        .json(&body)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
    let body: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(body["error"]["code"], "invalid_argument");
    assert_eq!(body["error"]["error_code"], "ORCH8-V005");
    assert_eq!(
        body["error"]["docs_url"],
        "https://orch8.io/docs/errors#ORCH8-V005"
    );

    // JSON syntax errors now come back in the same envelope, with location.
    let resp = client
        .post(format!("{}/sequences", srv.base_url))
        .header("X-Tenant-Id", "t1")
        .header("Content-Type", "application/json")
        .body("{\n  \"name\": ,\n}")
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
    let body: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(body["error"]["error_code"], "ORCH8-V001");
    assert!(
        body["error"]["message"]
            .as_str()
            .unwrap()
            .contains("invalid JSON at line 2"),
        "{body}"
    );
}

#[tokio::test]
async fn sequence_decode_is_lenient_by_default_and_strict_on_request() {
    let srv = spawn_test_server().await;
    let client = reqwest::Client::new();
    let make_body = |id: Uuid, name: &str| {
        json!({
            "id": id,
            "tenant_id": "t1",
            "namespace": "ns1",
            "name": name,
            "version": 1,
            "blocks": [{
                "type": "router",
                "id": "route",
                "routes": [{
                    "condition": "true",
                    "blocks": [{
                        "type": "step",
                        "id": "work",
                        "handler": "noop",
                        "wehn": "true"
                    }]
                }]
            }],
            "created_at": chrono::Utc::now().to_rfc3339()
        })
    };

    let response = client
        .post(format!("{}/sequences", srv.base_url))
        .header("X-Tenant-Id", "t1")
        .json(&make_body(Uuid::now_v7(), "lenient-sequence"))
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::CREATED);
    let body: serde_json::Value = response.json().await.unwrap();
    assert!(body["warnings"].as_array().is_some_and(|warnings| {
        warnings.iter().any(|warning| {
            warning
                .as_str()
                .is_some_and(|warning| warning.contains("blocks[0].routes[0].blocks[0].wehn"))
        })
    }));

    let response = client
        .post(format!("{}/sequences?strict=true", srv.base_url))
        .header("X-Tenant-Id", "t1")
        .json(&make_body(Uuid::now_v7(), "strict-sequence"))
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::BAD_REQUEST);

    for endpoint in ["preflight", "dataflow"] {
        let response = client
            .post(format!("{}/sequences/{endpoint}", srv.base_url))
            .header("X-Tenant-Id", "t1")
            .json(&make_body(Uuid::now_v7(), &format!("lenient-{endpoint}")))
            .send()
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK, "{endpoint}");
        let body: serde_json::Value = response.json().await.unwrap();
        assert!(
            body.to_string().contains("UNKNOWN_SEQUENCE_FIELD"),
            "{endpoint} omitted compatibility warnings: {body}"
        );

        let response = client
            .post(format!("{}/sequences/{endpoint}?strict=true", srv.base_url))
            .header("X-Tenant-Id", "t1")
            .json(&make_body(Uuid::now_v7(), &format!("strict-{endpoint}")))
            .send()
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::BAD_REQUEST, "{endpoint}");
    }
}

#[tokio::test]
async fn create_sequence_with_duplicate_block_ids_returns_400() {
    let srv = spawn_test_server().await;
    let client = reqwest::Client::new();

    let body = json!({
        "id": Uuid::now_v7(),
        "tenant_id": "t1",
        "namespace": "ns1",
        "name": "dup-seq",
        "version": 1,
        "deprecated": false,
        "blocks": [
            {
                "type": "step",
                "id": "s1",
                "handler": "noop",
                "params": {},
                "cancellable": true
            },
            {
                "type": "step",
                "id": "s1",
                "handler": "log",
                "params": {},
                "cancellable": true
            }
        ],
        "interceptors": null,
        "created_at": chrono::Utc::now().to_rfc3339()
    });

    let resp = client
        .post(format!("{}/sequences", srv.base_url))
        .header("X-Tenant-Id", "t1")
        .json(&body)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
}

#[tokio::test]
async fn get_sequence_by_name_returns_latest_version() {
    let srv = spawn_test_server().await;
    let client = reqwest::Client::new();

    let seq_id_v1 = Uuid::now_v7();
    let seq_id_v2 = Uuid::now_v7();

    for (id, version) in [(seq_id_v1, 1), (seq_id_v2, 2)] {
        let body = json!({
            "id": id,
            "tenant_id": "t1",
            "namespace": "ns1",
            "name": "versioned-seq",
            "version": version,
            "deprecated": false,
            "blocks": [
                {
                    "type": "step",
                    "id": "s1",
                    "handler": "noop",
                    "params": {},
                    "cancellable": true
                }
            ],
            "interceptors": null,
            "created_at": chrono::Utc::now().to_rfc3339()
        });
        let resp = client
            .post(format!("{}/sequences", srv.base_url))
            .header("X-Tenant-Id", "t1")
            .json(&body)
            .send()
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::CREATED);
    }

    let resp = client
        .get(format!(
            "{}/sequences/by-name?tenant_id=t1&namespace=ns1&name=versioned-seq",
            srv.base_url
        ))
        .header("X-Tenant-Id", "t1")
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let fetched: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(fetched["version"], 2);
}

#[tokio::test]
async fn deprecate_sequence_hides_from_by_name_lookup() {
    let srv = spawn_test_server().await;
    let client = reqwest::Client::new();

    let seq_id = Uuid::now_v7();
    let body = json!({
        "id": seq_id,
        "tenant_id": "t1",
        "namespace": "ns1",
        "name": "dep-seq",
        "version": 1,
        "deprecated": false,
        "blocks": [
            {
                "type": "step",
                "id": "s1",
                "handler": "noop",
                "params": {},
                "cancellable": true
            }
        ],
        "interceptors": null,
        "created_at": chrono::Utc::now().to_rfc3339()
    });

    let resp = client
        .post(format!("{}/sequences", srv.base_url))
        .header("X-Tenant-Id", "t1")
        .json(&body)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::CREATED);

    let resp = client
        .post(format!("{}/sequences/{seq_id}/deprecate", srv.base_url))
        .header("X-Tenant-Id", "t1")
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::NO_CONTENT);

    let resp = client
        .get(format!(
            "{}/sequences/by-name?tenant_id=t1&namespace=ns1&name=dep-seq",
            srv.base_url
        ))
        .header("X-Tenant-Id", "t1")
        .send()
        .await
        .unwrap();
    // No non-deprecated version exists → 404.
    assert_eq!(resp.status(), StatusCode::NOT_FOUND);
}

/// C1-1 / T2-GAP-API-06: a ~1 KB billion-laughs YAML body is refused with
/// a 400 instead of expanding into millions of values.
#[tokio::test]
async fn yaml_alias_bomb_is_rejected_quickly_with_bounded_memory() {
    let srv = spawn_test_server().await;
    let client = reqwest::Client::new();
    let leaf = vec!["x"; 200].join(", ");
    let nine = |name: &str| vec![format!("*{name}"); 9].join(", ");
    let bomb = format!(
        "tenant_id: t1\nnamespace: ns1\nname: bomb\na: &a [{leaf}]\nb: &b [{}]\nc: &c [{}]\nd: &d [{}]\ne: [{}]\n",
        nine("a"),
        nine("b"),
        nine("c"),
        nine("d")
    );
    let started = std::time::Instant::now();

    let resp = client
        .post(format!("{}/sequences", srv.base_url))
        .header("X-Tenant-Id", "t1")
        .header("Content-Type", "application/yaml")
        .body(bomb)
        .send()
        .await
        .unwrap();

    assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
    assert!(
        started.elapsed() < std::time::Duration::from_secs(1),
        "took {:?}",
        started.elapsed()
    );
}

/// API-07: `limit=0` used to return `items: []` with `has_more: true` (an
/// infinite client paging loop). The limit is clamped to at least one row,
/// matching the `clamp(1, ..)` convention of the other list endpoints.
#[tokio::test]
async fn list_sequences_with_limit_zero_returns_a_non_empty_page() {
    let srv = spawn_test_server().await;
    let client = reqwest::Client::new();
    for name in ["limit-a", "limit-b"] {
        let resp = client
            .post(format!("{}/sequences", srv.base_url))
            .header("X-Tenant-Id", "t1")
            .json(&json!({
                "id": Uuid::now_v7(),
                "tenant_id": "t1",
                "namespace": "ns1",
                "name": name,
                "version": 1,
                "blocks": [{ "type": "step", "id": "s1", "handler": "noop", "params": {} }],
                "created_at": chrono::Utc::now().to_rfc3339()
            }))
            .send()
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::CREATED);
    }

    let body: serde_json::Value = client
        .get(format!("{}/sequences?tenant_id=t1&limit=0", srv.base_url))
        .header("X-Tenant-Id", "t1")
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();

    assert_eq!(body["items"].as_array().unwrap().len(), 1);
    assert_eq!(body["has_more"], true);
}

#[tokio::test]
async fn list_sequence_versions_returns_all_versions() {
    let srv = spawn_test_server().await;
    let client = reqwest::Client::new();

    let seq_id_v1 = Uuid::now_v7();
    let seq_id_v2 = Uuid::now_v7();

    for (id, version) in [(seq_id_v1, 1), (seq_id_v2, 2)] {
        let body = json!({
            "id": id,
            "tenant_id": "t1",
            "namespace": "ns1",
            "name": "list-seq",
            "version": version,
            "deprecated": false,
            "blocks": [
                {
                    "type": "step",
                    "id": "s1",
                    "handler": "noop",
                    "params": {},
                    "cancellable": true
                }
            ],
            "interceptors": null,
            "created_at": chrono::Utc::now().to_rfc3339()
        });
        let resp = client
            .post(format!("{}/sequences", srv.base_url))
            .header("X-Tenant-Id", "t1")
            .json(&body)
            .send()
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::CREATED);
    }

    let resp = client
        .get(format!(
            "{}/sequences/versions?tenant_id=t1&namespace=ns1&name=list-seq",
            srv.base_url
        ))
        .header("X-Tenant-Id", "t1")
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let versions: Vec<serde_json::Value> = resp.json().await.unwrap();
    assert_eq!(versions.len(), 2);
}

#[tokio::test]
async fn delete_sequence_returns_404_for_unknown_id() {
    let srv = spawn_test_server().await;
    let client = reqwest::Client::new();

    let resp = client
        .delete(format!("{}/sequences/{}", srv.base_url, Uuid::now_v7()))
        .header("X-Tenant-Id", "t1")
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn get_sequence_returns_404_for_unknown_id() {
    let srv = spawn_test_server().await;
    let client = reqwest::Client::new();

    let resp = client
        .get(format!("{}/sequences/{}", srv.base_url, Uuid::now_v7()))
        .header("X-Tenant-Id", "t1")
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::NOT_FOUND);
}

/// `Stab#14`: `migrate_instance` used to fetch the target sequence and
/// immediately discard it — never comparing tenants. That let tenant A
/// rebind their instance onto tenant B's sequence, so the scheduler would
/// execute A's instance against B's blocks (crossing the isolation
/// boundary). The fix rejects cross-tenant target sequences with 403.
#[tokio::test]
async fn migrate_instance_rejects_cross_tenant_target_sequence() {
    let srv = spawn_test_server().await;
    let client = reqwest::Client::new();

    // Sequence owned by tenant "t1".
    let seq_t1 = Uuid::now_v7();
    let body_t1 = json!({
        "id": seq_t1,
        "tenant_id": "t1",
        "namespace": "ns1",
        "name": "seq-t1",
        "version": 1,
        "deprecated": false,
        "blocks": [
            { "type": "step", "id": "s1", "handler": "noop", "params": {}, "cancellable": true }
        ],
        "interceptors": null,
        "created_at": chrono::Utc::now().to_rfc3339()
    });
    let resp = client
        .post(format!("{}/sequences", srv.base_url))
        .header("X-Tenant-Id", "t1")
        .json(&body_t1)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::CREATED);

    // Sequence owned by tenant "t2" — the cross-tenant migration target.
    let seq_t2 = Uuid::now_v7();
    let body_t2 = json!({
        "id": seq_t2,
        "tenant_id": "t2",
        "namespace": "ns1",
        "name": "seq-t2",
        "version": 1,
        "deprecated": false,
        "blocks": [
            { "type": "step", "id": "s1", "handler": "noop", "params": {}, "cancellable": true }
        ],
        "interceptors": null,
        "created_at": chrono::Utc::now().to_rfc3339()
    });
    let resp = client
        .post(format!("{}/sequences", srv.base_url))
        .header("X-Tenant-Id", "t2")
        .json(&body_t2)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::CREATED);

    // Create a t1 instance.
    let inst_body = json!({
        "sequence_id": seq_t1,
        "tenant_id": "t1",
        "namespace": "ns1",
        "context": { "data": {}, "config": {}, "audit": [] }
    });
    let resp = client
        .post(format!("{}/instances", srv.base_url))
        .header("X-Tenant-Id", "t1")
        .json(&inst_body)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::CREATED);
    let created: serde_json::Value = resp.json().await.unwrap();
    let inst_id = created["id"].as_str().unwrap().to_string();

    // Attempt to migrate t1's instance onto t2's sequence — must be
    // rejected with 403 (isolation breach), not silently accepted.
    let migrate_body = json!({
        "instance_id": inst_id,
        "target_sequence_id": seq_t2,
    });
    let resp = client
        .post(format!("{}/sequences/migrate-instance", srv.base_url))
        .header("X-Tenant-Id", "t1")
        .json(&migrate_body)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::FORBIDDEN);
}

#[tokio::test]
async fn unpublish_sequence_works() {
    let srv = spawn_test_server().await;
    let client = reqwest::Client::new();

    let body = json!({
        "id": Uuid::now_v7(),
        "tenant_id": "t1",
        "namespace": "default",
        "name": "seq-to-unpublish",
        "version": 1,
        "deprecated": false,
        "blocks": [{ "type": "step", "id": "s1", "handler": "noop", "params": {}, "cancellable": true }],
        "created_at": chrono::Utc::now().to_rfc3339()
    });

    let resp = client
        .post(format!("{}/sequences", srv.base_url))
        .header("X-Tenant-Id", "t1")
        .json(&body)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::CREATED);

    let resp = client
        .post(format!(
            "{}/sequences/seq-to-unpublish/unpublish",
            srv.base_url
        ))
        .header("X-Tenant-Id", "t1")
        .json(&json!({ "delete": false }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::NO_CONTENT);
}

/// L3-03: `unpublish {delete:true}` must not cascade-delete live instances:
/// 409 with the active count, nothing deprecated or deleted.
#[tokio::test]
async fn unpublish_delete_rejects_active_instances() {
    let srv = spawn_test_server().await;
    let client = reqwest::Client::new();
    let seq_id = Uuid::now_v7();
    let body = json!({
        "id": seq_id,
        "tenant_id": "t1",
        "namespace": "default",
        "name": "seq-unpublish-live",
        "version": 1,
        "deprecated": false,
        "blocks": [{ "type": "step", "id": "s1", "handler": "noop", "params": {}, "cancellable": true }],
        "created_at": chrono::Utc::now().to_rfc3339()
    });
    let resp = client
        .post(format!("{}/sequences", srv.base_url))
        .header("X-Tenant-Id", "t1")
        .json(&body)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::CREATED);

    let resp = client
        .post(format!("{}/instances", srv.base_url))
        .header("X-Tenant-Id", "t1")
        .json(&json!({
            "sequence_id": seq_id,
            "tenant_id": "t1",
            "namespace": "default",
            "context": { "data": {}, "config": {}, "audit": [] }
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::CREATED);
    let created: serde_json::Value = resp.json().await.unwrap();
    let inst_id = created["id"].as_str().unwrap().to_string();

    let resp = client
        .post(format!(
            "{}/sequences/seq-unpublish-live/unpublish",
            srv.base_url
        ))
        .header("X-Tenant-Id", "t1")
        .json(&json!({ "delete": true }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::CONFLICT);
    let text = resp.text().await.unwrap();
    assert!(text.contains("1 active instance"), "got {text}");

    let resp = client
        .get(format!("{}/instances/{inst_id}", srv.base_url))
        .header("X-Tenant-Id", "t1")
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK, "the instance must survive");
    let resp = client
        .get(format!("{}/sequences/{seq_id}", srv.base_url))
        .header("X-Tenant-Id", "t1")
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let seq: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(
        seq["deprecated"],
        json!(false),
        "a rejected unpublish changes nothing"
    );
}

#[tokio::test]
async fn promote_sequence_works() {
    let srv = spawn_test_server().await;
    let client = reqwest::Client::new();

    let body = json!({
        "id": Uuid::now_v7(),
        "tenant_id": "t1",
        "namespace": "default",
        "name": "seq-to-promote",
        "version": 1,
        "deprecated": false,
        "status": "staging",
        "blocks": [{ "type": "step", "id": "s1", "handler": "noop", "params": {}, "cancellable": true }],
        "created_at": chrono::Utc::now().to_rfc3339()
    });

    let resp = client
        .post(format!("{}/sequences", srv.base_url))
        .header("X-Tenant-Id", "t1")
        .json(&body)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::CREATED);

    let resp = client
        .post(format!("{}/sequences/seq-to-promote/promote", srv.base_url))
        .header("X-Tenant-Id", "t1")
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::CREATED);
    let created: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(created["version"], 2);
}

#[tokio::test]
async fn input_schema_rejects_bad_instance_data_with_422() {
    let srv = spawn_test_server().await;
    let client = reqwest::Client::new();

    let seq_id = Uuid::now_v7();
    let body = json!({
        "id": seq_id,
        "tenant_id": "t1",
        "namespace": "ns1",
        "name": "schema-seq",
        "version": 1,
        "deprecated": false,
        "blocks": [{ "type": "step", "id": "s1", "handler": "noop", "params": {}, "cancellable": true }],
        "input_schema": {
            "type": "object",
            "properties": {
                "email": { "type": "string" },
                "age": { "type": "integer", "minimum": 0 }
            },
            "required": ["email"]
        },
        "created_at": chrono::Utc::now().to_rfc3339()
    });
    let resp = client
        .post(format!("{}/sequences", srv.base_url))
        .header("X-Tenant-Id", "t1")
        .json(&body)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::CREATED);

    // Missing required `email` -> 422.
    let bad = json!({
        "sequence_id": seq_id,
        "tenant_id": "t1",
        "namespace": "ns1",
        "context": { "data": { "age": 30 }, "config": {}, "audit": [] }
    });
    let resp = client
        .post(format!("{}/instances", srv.base_url))
        .header("X-Tenant-Id", "t1")
        .json(&bad)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::UNPROCESSABLE_ENTITY);

    // A nested value violation reports the JSON pointer to the rejected field.
    let invalid_age = json!({
        "sequence_id": seq_id,
        "tenant_id": "t1",
        "namespace": "ns1",
        "context": { "data": { "email": "a@b.com", "age": -1 }, "config": {}, "audit": [] }
    });
    let resp = client
        .post(format!("{}/instances", srv.base_url))
        .header("X-Tenant-Id", "t1")
        .json(&invalid_age)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::UNPROCESSABLE_ENTITY);
    assert!(resp.text().await.unwrap().contains("/age"));

    // Valid payload -> 201.
    let good = json!({
        "sequence_id": seq_id,
        "tenant_id": "t1",
        "namespace": "ns1",
        "context": { "data": { "email": "a@b.com", "age": 30 }, "config": {}, "audit": [] }
    });
    let resp = client
        .post(format!("{}/instances", srv.base_url))
        .header("X-Tenant-Id", "t1")
        .json(&good)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::CREATED);
}

#[tokio::test]
async fn create_sequence_with_malformed_input_schema_returns_400() {
    let srv = spawn_test_server().await;
    let client = reqwest::Client::new();

    let body = json!({
        "id": Uuid::now_v7(),
        "tenant_id": "t1",
        "namespace": "ns1",
        "name": "bad-schema-seq",
        "version": 1,
        "deprecated": false,
        "blocks": [{ "type": "step", "id": "s1", "handler": "noop", "params": {}, "cancellable": true }],
        "input_schema": { "type": "not-a-real-type" },
        "created_at": chrono::Utc::now().to_rfc3339()
    });
    let resp = client
        .post(format!("{}/sequences", srv.base_url))
        .header("X-Tenant-Id", "t1")
        .json(&body)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
}

/// L3-09: `migrate-instance` used to rebind blindly. An instance mid-way
/// through a composite owns an execution tree; a target version that drops,
/// retypes or re-parents a live block must be rejected with 409, a
/// structurally identical (or purely additive) one must still be accepted.
#[tokio::test]
#[allow(clippy::too_many_lines)]
async fn migrate_instance_rejects_structurally_incompatible_target() {
    use orch8_storage::{ExecutionTreeStore, InstanceStore};
    use orch8_types::execution::{BlockType, ExecutionNode, NodeState};
    use orch8_types::ids::{BlockId, ExecutionNodeId, InstanceId};

    let srv = spawn_test_server().await;
    let client = reqwest::Client::new();

    let step = |id: &str| json!({ "type": "step", "id": id, "handler": "noop", "params": {}, "cancellable": true });
    let publish = |name: &str, version: i32, blocks: serde_json::Value| {
        let id = Uuid::now_v7();
        let body = json!({
            "id": id, "tenant_id": "t1", "namespace": "ns1", "name": name,
            "version": version, "deprecated": false, "blocks": blocks,
            "created_at": chrono::Utc::now().to_rfc3339()
        });
        (id, body)
    };
    let par = |branches: serde_json::Value| json!({ "type": "parallel", "id": "par", "branches": branches });
    let versions = [
        // v1: the version the instance runs.
        publish("mig", 1, json!([par(json!([[step("a")], [step("b")]]))])),
        // additive: a new trailing block.
        publish(
            "mig",
            2,
            json!([par(json!([[step("a")], [step("b")]])), step("tail")]),
        ),
        // `b` removed.
        publish("mig", 3, json!([par(json!([[step("a")]]))])),
        // `par` retyped to a race.
        publish(
            "mig",
            4,
            json!([{ "type": "race", "id": "par", "branches": [[step("a")], [step("b")]] }]),
        ),
        // `b` moved out from under `par`.
        publish("mig", 5, json!([par(json!([[step("a")]])), step("b")])),
    ];
    for (_, body) in &versions {
        let resp = client
            .post(format!("{}/sequences", srv.base_url))
            .header("X-Tenant-Id", "t1")
            .json(body)
            .send()
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::CREATED, "{body}");
    }

    let resp = client
        .post(format!("{}/instances", srv.base_url))
        .header("X-Tenant-Id", "t1")
        .json(&json!({
            "sequence_id": versions[0].0, "tenant_id": "t1", "namespace": "ns1",
            "context": { "data": {}, "config": {}, "audit": [] }
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::CREATED);
    let created: serde_json::Value = resp.json().await.unwrap();
    let inst_id = created["id"].as_str().unwrap().to_string();
    let instance_id = InstanceId::from_uuid(inst_id.parse().unwrap());

    // The tree the scheduler would have built: `par` running, `a` done,
    // `b` still pending.
    let node =
        |block: &str, block_type, parent: Option<ExecutionNodeId>, branch, state| ExecutionNode {
            id: ExecutionNodeId::new(),
            instance_id,
            block_id: BlockId::new(block),
            parent_id: parent,
            block_type,
            branch_index: branch,
            state,
            started_at: None,
            completed_at: None,
        };
    let par_node = node("par", BlockType::Parallel, None, None, NodeState::Running);
    let nodes = vec![
        node(
            "a",
            BlockType::Step,
            Some(par_node.id),
            Some(0),
            NodeState::Completed,
        ),
        node(
            "b",
            BlockType::Step,
            Some(par_node.id),
            Some(1),
            NodeState::Pending,
        ),
        par_node,
    ];
    srv.storage
        .create_execution_nodes_batch(&nodes)
        .await
        .unwrap();

    let migrate = |target: Uuid| {
        client
            .post(format!("{}/sequences/migrate-instance", srv.base_url))
            .header("X-Tenant-Id", "t1")
            .json(&json!({ "instance_id": inst_id, "target_sequence_id": target }))
            .send()
    };
    for (idx, needle) in [(2, "'b'"), (3, "'par'"), (4, "'b'")] {
        let resp = migrate(versions[idx].0).await.unwrap();
        assert_eq!(resp.status(), StatusCode::CONFLICT, "version {}", idx + 1);
        let body: serde_json::Value = resp.json().await.unwrap();
        let message = body["error"]["message"].as_str().unwrap();
        assert!(
            message.contains("structurally incompatible") && message.contains(needle),
            "{message}"
        );
        // Rejected migrations leave the binding untouched.
        let stored = srv
            .storage
            .get_instance(instance_id)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(stored.sequence_id.into_uuid(), versions[0].0);
    }

    let resp = migrate(versions[1].0).await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let stored = srv
        .storage
        .get_instance(instance_id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(stored.sequence_id.into_uuid(), versions[1].0);
}

#[tokio::test]
async fn dataflow_api_rejects_unguarded_nullable_parent() {
    let srv = spawn_test_server().await;
    let response = reqwest::Client::new()
        .post(format!("{}/sequences/dataflow", srv.base_url))
        .header("X-Tenant-Id", "t1")
        .json(&json!({
            "name":"nullable-parent","tenant_id":"t1","namespace":"default",
            "id": Uuid::now_v7(), "version": 1, "created_at": chrono::Utc::now(),
            "input_schema":{
                "type":"object","required":["address"],
                "properties":{"address":{
                    "type":["null","object"],"required":["city"],
                    "properties":{"city":{"type":"string"}}
                }}
            },
            "blocks":[{"type":"step","id":"use","handler":"noop","params":{
                "city":"{{ data.address.city }}"
            }}]
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let body: serde_json::Value = response.json().await.unwrap();
    assert_eq!(body["report"]["findings"][0]["code"], "VALUE_MAY_BE_NULL");
    assert_eq!(
        body["report"]["findings"][0]["reference"],
        "data.address.city"
    );
}

#[tokio::test]
async fn dataflow_api_reports_producer_order_and_branch_availability() {
    let srv = spawn_test_server().await;
    let sequence: serde_json::Value = serde_json::from_str(include_str!(
        "../../orch8-engine/tests/fixtures/typed_dataflow_order.json"
    ))
    .unwrap();
    let response = reqwest::Client::new()
        .post(format!("{}/sequences/dataflow", srv.base_url))
        .header("X-Tenant-Id", "t1")
        .json(&sequence)
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let body: serde_json::Value = response.json().await.unwrap();
    let findings = body["report"]["findings"].as_array().unwrap();
    assert_eq!(findings.len(), 2, "{body}");
    assert_eq!(findings[0]["consumer"], "post_branch_reader");
    assert_eq!(findings[0]["code"], "PRODUCER_MAY_NOT_RUN");
    assert_eq!(findings[0]["severity"], "warning");
    assert_eq!(findings[1]["consumer"], "premature_reader");
    assert_eq!(findings[1]["code"], "PRODUCER_RUNS_LATER");
    assert_eq!(findings[1]["severity"], "error");
}

#[tokio::test]
async fn dataflow_api_returns_resolvable_source_locations() {
    let srv = spawn_test_server().await;
    let sequence: serde_json::Value = serde_json::from_str(include_str!(
        "../../orch8-engine/tests/fixtures/typed_dataflow_source.json"
    ))
    .unwrap();
    let response = reqwest::Client::new()
        .post(format!("{}/sequences/dataflow", srv.base_url))
        .header("X-Tenant-Id", "t1")
        .json(&sequence)
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let body: serde_json::Value = response.json().await.unwrap();
    let finding = &body["report"]["findings"][0];
    assert_eq!(finding["code"], "SCHEMA_PATH_MISSING");
    let source = &finding["source"];
    let text = sequence
        .pointer(source["sequence_pointer"].as_str().unwrap())
        .unwrap()
        .as_str()
        .unwrap();
    let start = usize::try_from(source["start_byte"].as_u64().unwrap()).unwrap();
    let end = usize::try_from(source["end_byte"].as_u64().unwrap()).unwrap();
    assert_eq!(&text[start..end], "context.outputs.create.missing");
    assert_eq!(
        sequence
            .pointer(source["producer_schema_pointer"].as_str().unwrap())
            .unwrap()["$ref"],
        "#/$defs/record"
    );
}

#[tokio::test]
async fn dataflow_api_reads_the_item_schema_in_foreach_bodies() {
    let srv = spawn_test_server().await;
    let response = reqwest::Client::new()
        .post(format!("{}/sequences/dataflow", srv.base_url))
        .header("X-Tenant-Id", "t1")
        .json(&json!({
            "name":"typed-items","tenant_id":"t1","namespace":"default",
            "id":Uuid::now_v7(),"version":1,"created_at":chrono::Utc::now(),
            "input_schema":{
                "type":"object","required":["rows"],"properties":{"rows":{
                    "type":"array","items":{
                        "type":"object","required":["number"],"additionalProperties":false,
                        "properties":{"number":{"type":"integer"}}
                    }
                }}
            },
            "blocks":[{"type":"for_each","id":"items","collection":"rows","item_var":"row","body":[
                {"type":"step","id":"read","handler":"transform","params":{
                    "valid":"{{ data.row.number }}", "invalid":"{{ data.row.rows }}"
                }}
            ]}]
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let body: serde_json::Value = response.json().await.unwrap();
    let findings = body["report"]["findings"].as_array().unwrap();
    assert_eq!(findings.len(), 1, "{body}");
    assert_eq!(findings[0]["code"], "SCHEMA_PATH_MISSING");
    assert_eq!(findings[0]["reference"], "data.row.rows");
    assert_eq!(findings[0]["consumer"], "read");
}

#[tokio::test]
async fn dataflow_api_distinguishes_tuple_elements() {
    let srv = spawn_test_server().await;
    let response = reqwest::Client::new()
        .post(format!("{}/sequences/dataflow", srv.base_url))
        .header("X-Tenant-Id", "t1")
        .json(&json!({
            "name":"tuple-input","tenant_id":"t1","namespace":"default",
            "id": Uuid::now_v7(), "version": 1, "created_at": chrono::Utc::now(),
            "input_schema":{
                "type":"object","required":["tuple"],
                "properties":{"tuple":{
                    "type":"array","minItems":1,"items":false,
                    "prefixItems":[{"type":"string"},{"type":"number"}]
                }}
            },
            "blocks":[{"type":"step","id":"use","handler":"noop","params":{
                "required":"{{ data.tuple.0 }}",
                "optional":"{{ data.tuple.1 }}",
                "prohibited":"{{ data.tuple.2 }}"
            }}]
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let body: serde_json::Value = response.json().await.unwrap();
    let findings = body["report"]["findings"].as_array().unwrap();
    assert_eq!(findings.len(), 2, "{findings:?}");
    assert!(
        findings
            .iter()
            .any(|f| { f["reference"] == "data.tuple.1" && f["code"] == "VALUE_MAY_BE_ABSENT" })
    );
    assert!(
        findings
            .iter()
            .any(|f| { f["reference"] == "data.tuple.2" && f["code"] == "SCHEMA_PATH_MISSING" })
    );
}

#[tokio::test]
async fn dataflow_api_shares_local_schema_refs_between_preflight_and_generated_types() {
    let srv = spawn_test_server().await;
    let response = reqwest::Client::new()
        .post(format!("{}/sequences/dataflow", srv.base_url))
        .header("X-Tenant-Id", "t1")
        .json(&json!({
            "name":"definition-references","tenant_id":"t1","namespace":"default",
            "id": Uuid::now_v7(), "version": 1, "created_at": chrono::Utc::now(),
            "input_schema":{
                "type":"object","required":["billing","shipping"],
                "properties":{
                    "billing":{"$ref":"#/$defs/address"},
                    "shipping":{"$ref":"#/$defs/address"}
                },
                "$defs":{"address":{
                    "type":"object","required":["city"],"additionalProperties":false,
                    "properties":{"city":{"type":"string"}}
                }}
            },
            "blocks":[{"type":"step","id":"use","handler":"noop","params":{
                "billing":"{{ data.billing.city }}",
                "shipping":"{{ data.shipping.city }}",
                "invalid":"{{ data.billing.cty }}"
            }}]
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let body: serde_json::Value = response.json().await.unwrap();
    let findings = body["report"]["findings"].as_array().unwrap();
    assert_eq!(findings.len(), 1, "{findings:?}");
    assert_eq!(findings[0]["code"], "SCHEMA_PATH_MISSING");
    assert_eq!(findings[0]["reference"], "data.billing.cty");
    assert_eq!(
        body["generated"]["typescript"]
            .as_str()
            .unwrap()
            .matches(r#""city": string"#)
            .count(),
        2
    );
}

#[tokio::test]
async fn dataflow_api_rejects_numeric_use_of_a_string_map_value() {
    let srv = spawn_test_server().await;
    let response = reqwest::Client::new()
        .post(format!("{}/sequences/dataflow", srv.base_url))
        .header("X-Tenant-Id", "t1")
        .json(&json!({
            "name":"typed-map","tenant_id":"t1","namespace":"default",
            "id": Uuid::now_v7(), "version":1, "created_at":chrono::Utc::now(),
            "input_schema":{
                "type":"object","required":["labels"],
                "properties":{"labels":{"type":"object","additionalProperties":{"type":"string"}}}
            },
            "blocks":[{"type":"step","id":"use","handler":"noop",
                "when":"data.labels.invoice / 2 > 7"
            }]
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let body: serde_json::Value = response.json().await.unwrap();
    let findings = body["report"]["findings"].as_array().unwrap();
    assert_eq!(findings.len(), 2, "{findings:?}");
    assert_eq!(findings[0]["code"], "NUMERIC_OPERAND_TYPE");
    assert_eq!(findings[1]["code"], "VALUE_MAY_BE_ABSENT");
    assert_eq!(findings[0]["reference"], "data.labels.invoice");
}

#[tokio::test]
async fn server_preflight_reports_the_same_schema_path_code_as_the_playground() {
    let srv = spawn_test_server().await;
    let sequence: serde_json::Value = serde_json::from_str(include_str!(
        "../../orch8-engine/tests/fixtures/typed_dataflow_preview.json"
    ))
    .unwrap();
    let response = reqwest::Client::new()
        .post(format!("{}/sequences/preflight", srv.base_url))
        .header("X-Tenant-Id", "t1")
        .json(&sequence)
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let report: serde_json::Value = response.json().await.unwrap();
    assert_eq!(report["overall"], "fail", "{report}");
    let check = report["checks"]
        .as_array()
        .unwrap()
        .iter()
        .find(|check| check["id"] == "typed_dataflow_compatible")
        .unwrap();
    assert_eq!(check["findings"][0]["code"], "SCHEMA_PATH_MISSING");
    assert!(
        check["findings"][0]["summary"]
            .as_str()
            .unwrap()
            .contains("data.regon")
    );
}

#[tokio::test]
async fn configuration_schema_rejects_misspelled_references_and_missing_activation_values() {
    let server = spawn_test_server().await;
    let client = reqwest::Client::new();
    let id = Uuid::now_v7();
    let mut sequence = json!({
        "id":id,"name":"configuration-schema","tenant_id":"t1","namespace":"default",
        "version":1,"created_at":chrono::Utc::now(),
        "input_schema":{"x-orch8-config-schema":{
            "type":"object","required":["region"],"additionalProperties":false,
            "properties":{"region":{"type":"string"}}
        }},
        "blocks":[{"type":"step","id":"check","handler":"noop","params":{"region":"{{ config.regoin }}"}}]
    });
    let response = client
        .post(format!("{}/sequences/dataflow", server.base_url))
        .header("X-Tenant-Id", "t1")
        .json(&sequence)
        .send()
        .await
        .unwrap();
    let body: serde_json::Value = response.json().await.unwrap();
    let findings = body["report"]["findings"].as_array().unwrap();
    assert!(
        findings
            .iter()
            .any(|item| item["reference"] == "config.regoin"
                && item["severity"] == "error"
                && item["source"]["producer_schema_pointer"]
                    == "/input_schema/x-orch8-config-schema"),
        "{body}"
    );

    sequence["blocks"][0]["params"]["region"] = json!("{{ config.region }}");
    let base = server.v1_url();
    assert_eq!(
        client
            .post(format!("{base}/sequences"))
            .header("X-Tenant-Id", "t1")
            .json(&sequence)
            .send()
            .await
            .unwrap()
            .status(),
        StatusCode::CREATED
    );
    for (config, expected) in [
        (json!({}), StatusCode::UNPROCESSABLE_ENTITY),
        (json!({"region":"us-east-1"}), StatusCode::CREATED),
    ] {
        let response = client
            .post(format!("{base}/instances"))
            .header("X-Tenant-Id", "t1")
            .json(
                &json!({"sequence_id":id, "tenant_id":"t1", "namespace":"default",
                "context":{"data":{},"config":config}}),
            )
            .send()
            .await
            .unwrap();
        assert_eq!(response.status(), expected);
    }
}

#[tokio::test]
async fn pinned_child_boundaries_preflight_and_publish_use_the_requested_revision() {
    let server = spawn_test_server().await;
    let base = server.v1_url();
    let client = reqwest::Client::new();
    let send = |path: &str, document: &serde_json::Value| {
        client
            .post(format!("{base}/sequences{path}"))
            .header("X-Tenant-Id", "t1")
            .json(document)
            .send()
    };
    let child_id = Uuid::now_v7();
    let mut child = json!({
        "id":child_id,"name":"child","tenant_id":"t1","namespace":"default","version":1,
        "created_at":chrono::Utc::now(),
        "input_schema":{
            "type":"object","required":["amount"],"properties":{"amount":{"type":"integer"}},
            "x-orch8-child-result-schema":{
                "type":"array","minItems":1,
                "items":{"type":"object","required":["output"],"properties":{
                    "output":{"type":"object","additionalProperties":false,
                              "required":["total"],"properties":{"total":{"type":"number"}}}
                }}
            }
        },
        "blocks":[{"type":"step","id":"child_step","handler":"noop"}]
    });
    assert_eq!(
        send("", &child).await.unwrap().status(),
        StatusCode::CREATED
    );
    child["id"] = json!(Uuid::now_v7());
    child["version"] = json!(2);
    child["input_schema"]["properties"]["amount"]["type"] = json!("string");
    assert_eq!(
        send("", &child).await.unwrap().status(),
        StatusCode::CREATED
    );
    let mut parent = json!({
        "id":Uuid::now_v7(),"name":"parent","tenant_id":"t1","namespace":"default","version":1,
        "created_at":chrono::Utc::now(),
        "blocks":[
            {"type":"sub_sequence","id":"call","sequence_name":"child","version":1,
             "input":{"amount":"12"}},
            {"type":"step","id":"read","handler":"noop",
             "params":{"result":"{{ outputs.call[0].output.unknown }}"}}
        ]
    });
    let report: serde_json::Value = send("/preflight", &parent)
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    let boundaries = report["checks"]
        .as_array()
        .unwrap()
        .iter()
        .find(|check| check["id"] == "pinned_child_contracts")
        .unwrap();
    assert_eq!(boundaries["status"], "fail", "{report}");
    let findings = boundaries["findings"].as_array().unwrap();
    assert_eq!(findings.len(), 2, "{report}");
    for finding in findings {
        assert_eq!(
            finding["evidence"][0]["data"]["child_sequence_id"],
            child_id.to_string()
        );
    }
    assert_eq!(
        findings[0]["evidence"][0]["data"]["parent_pointer"],
        "/blocks/0/input/amount"
    );
    assert_eq!(
        findings[1]["evidence"][0]["data"]["parent_pointer"],
        "/blocks/1/params/result"
    );
    assert_eq!(
        send("", &parent).await.unwrap().status(),
        StatusCode::UNPROCESSABLE_ENTITY
    );
    parent["blocks"][0]["input"]["amount"] = json!(12);
    parent["blocks"][1]["params"]["result"] = json!("{{ outputs.call[0].output.total }}");
    assert_eq!(
        send("", &parent).await.unwrap().status(),
        StatusCode::CREATED
    );
}
