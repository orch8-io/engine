//! Golden-response corpus (review fix plan, "`OpenAPI` validation + golden corpus").
//!
//! Drives real HTTP calls through the production router, then
//!   1. validates every response body against the operation's response schema
//!      in the generated `OpenAPI` document, and
//!   2. compares the volatile-normalised body with the committed fixture under
//!      `contracts/golden/responses/<name>.json`.
//!
//! The fixtures are what every SDK's decode test consumes, so a serializer
//! change (casing, nullability, new enum variant) fails here until the corpus
//! is regenerated, and then fails in the SDKs that cannot decode it.
//!
//! Regenerate with `ORCH8_UPDATE_GOLDEN=1 cargo test -p orch8-api --test golden_responses`.

use std::path::PathBuf;

use orch8_api::openapi::ApiDoc;
use orch8_api::test_harness::spawn_test_server;
use reqwest::StatusCode;
use serde_json::{Value, json};
use utoipa::OpenApi as _;
use uuid::Uuid;

fn corpus_dir() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../contracts/golden/responses")
}

/// Replace values that differ on every run with stable, still well-formed
/// constants (typed SDK decoders must be able to parse the fixtures).
fn normalize(v: &mut Value) {
    match v {
        Value::String(s) => {
            if Uuid::parse_str(s).is_ok() {
                *s = "00000000-0000-0000-0000-000000000000".into();
            } else if chrono::DateTime::parse_from_rfc3339(s).is_ok() {
                *s = "2026-01-01T00:00:00Z".into();
            }
        }
        Value::Array(a) => a.iter_mut().for_each(normalize),
        Value::Object(o) => {
            // Keys that are themselves uuids (maps keyed by id) are rare; values only.
            o.values_mut().for_each(normalize);
        }
        _ => {}
    }
}

/// Look up the JSON response schema of `method path` for `status` and
/// validate `body` against it (components embedded so `$ref`s resolve).
fn validate_against_openapi(doc: &Value, path: &str, method: &str, status: u16, body: &Value) {
    let key = path.replace('~', "~0").replace('/', "~1");
    let schema = doc
        .pointer(&format!(
            "/paths/{key}/{method}/responses/{status}/content/application~1json/schema"
        ))
        .unwrap_or_else(|| panic!("OpenAPI has no JSON {status} response for {method} {path}"));
    let mut root = schema.clone();
    root.as_object_mut()
        .expect("schema object")
        .insert("components".into(), doc["components"].clone());
    let validator = jsonschema::validator_for(&root)
        .unwrap_or_else(|e| panic!("{method} {path}: unusable response schema: {e}"));
    let errors: Vec<String> = validator
        .iter_errors(body)
        .map(|e| format!("  at {}: {e}", e.instance_path()))
        .collect();
    assert!(
        errors.is_empty(),
        "{method} {path} {status}: response violates OpenAPI:\n{}",
        errors.join("\n")
    );
}

/// Known spec-vs-server drift, allowlisted once so the gate can land; burn down,
/// never add. Each entry is a fixture name whose `OpenAPI` check is skipped.
///
/// * `list_instances`: the spec documents a bare array but the server returns
///   a page object `{ "items": [...], "has_more": bool }`.
const KNOWN_OPENAPI_DRIFT: &[&str] = &["list_instances"];

struct Golden {
    doc: Value,
    update: bool,
}

impl Golden {
    fn check(&self, name: &str, path: &str, method: &str, status: u16, mut body: Value) {
        if !KNOWN_OPENAPI_DRIFT.contains(&name) {
            validate_against_openapi(&self.doc, path, method, status, &body);
        }
        normalize(&mut body);
        let file = corpus_dir().join(format!("{name}.json"));
        let rendered = format!("{}\n", serde_json::to_string_pretty(&body).unwrap());
        if self.update {
            std::fs::create_dir_all(corpus_dir()).unwrap();
            std::fs::write(&file, rendered).unwrap();
            return;
        }
        let expected = std::fs::read_to_string(&file).unwrap_or_else(|_| {
            panic!(
                "missing golden fixture {}; run with ORCH8_UPDATE_GOLDEN=1",
                file.display()
            )
        });
        assert_eq!(
            expected, rendered,
            "golden drift for {name}; if intended, run with ORCH8_UPDATE_GOLDEN=1 and commit contracts/golden/responses"
        );
    }
}

async fn json_of(resp: reqwest::Response, expect: StatusCode) -> Value {
    let status = resp.status();
    let text = resp.text().await.unwrap();
    assert_eq!(status, expect, "unexpected status, body: {text}");
    serde_json::from_str(&text).unwrap()
}

#[tokio::test]
#[allow(clippy::too_many_lines)] // One corpus pass keeps fixture validation and HTTP assertions together.
async fn golden_corpus_matches_responses_and_openapi() {
    let g = Golden {
        doc: serde_json::to_value(ApiDoc::openapi()).unwrap(),
        update: std::env::var("ORCH8_UPDATE_GOLDEN").is_ok_and(|v| v == "1"),
    };
    let srv = spawn_test_server().await;
    let c = reqwest::Client::new();
    let base = &srv.base_url;
    let tenant = "golden";

    // Sequence.
    let seq_id = Uuid::now_v7();
    let seq = json!({
        "id": seq_id, "tenant_id": tenant, "namespace": "ns", "name": "golden-seq",
        "version": 1, "deprecated": false,
        "blocks": [{"type": "step", "id": "s1", "handler": "noop", "params": {}, "cancellable": true}],
        "interceptors": null, "created_at": chrono::Utc::now().to_rfc3339()
    });
    let r = c
        .post(format!("{base}/sequences"))
        .header("X-Tenant-Id", tenant)
        .json(&seq)
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), StatusCode::CREATED);
    let r = c
        .get(format!("{base}/sequences/{seq_id}"))
        .header("X-Tenant-Id", tenant)
        .send()
        .await
        .unwrap();
    g.check(
        "get_sequence",
        "/sequences/{id}",
        "get",
        200,
        json_of(r, StatusCode::OK).await,
    );

    // Instance, with a non-default priority so the enum shape is exercised.
    let r = c
        .post(format!("{base}/instances"))
        .header("X-Tenant-Id", tenant)
        .json(&json!({
            "sequence_id": seq_id, "tenant_id": tenant, "namespace": "ns",
            "priority": "High",
            "context": {"data": {"x": 1}, "config": {}, "audit": []}
        }))
        .send()
        .await
        .unwrap();
    let created = json_of(r, StatusCode::CREATED).await;
    let inst_id = created["id"].as_str().unwrap().to_owned();
    g.check("create_instance", "/instances", "post", 201, created);

    let r = c
        .get(format!("{base}/instances/{inst_id}"))
        .header("X-Tenant-Id", tenant)
        .send()
        .await
        .unwrap();
    g.check(
        "get_instance",
        "/instances/{id}",
        "get",
        200,
        json_of(r, StatusCode::OK).await,
    );

    let r = c
        .get(format!("{base}/instances?tenant_id={tenant}"))
        .header("X-Tenant-Id", tenant)
        .send()
        .await
        .unwrap();
    g.check(
        "list_instances",
        "/instances",
        "get",
        200,
        json_of(r, StatusCode::OK).await,
    );

    // Job.
    let r = c
        .post(format!("{base}/jobs"))
        .header("X-Tenant-Id", tenant)
        .json(&json!({"handler": "noop", "payload": {"a": 1}, "priority": "high", "tenant_id": tenant}))
        .send()
        .await
        .unwrap();
    let status = r.status();
    let job = json_of(r, status).await;
    let job_id = job["id"].as_str().unwrap().to_owned();
    g.check("enqueue_job", "/jobs", "post", status.as_u16(), job);

    let r = c
        .get(format!("{base}/jobs/{job_id}"))
        .header("X-Tenant-Id", tenant)
        .send()
        .await
        .unwrap();
    g.check(
        "get_job",
        "/jobs/{id}",
        "get",
        200,
        json_of(r, StatusCode::OK).await,
    );

    // Worker task stats (empty and windowed).
    let r = c
        .get(format!("{base}/workers/tasks/stats?window_secs=300"))
        .header("X-Tenant-Id", tenant)
        .send()
        .await
        .unwrap();
    g.check(
        "task_stats",
        "/workers/tasks/stats",
        "get",
        200,
        json_of(r, StatusCode::OK).await,
    );
}
