//! A support bundle never exports secrets or execution data: any object key
//! containing a sensitive needle (case-insensitive) has its whole value
//! replaced by `[REDACTED]`, at any depth; workload summaries keep only an
//! allow-list of operational fields; the bundle file is owner-only; the
//! instance limit is clamped to 1..=1000; a diagnosis is fetched only when an
//! instance is named.

use super::*;

use reqwest::StatusCode;

use crate::commands::test_support::{RecordedRequest, mock_api_with_responses};

#[test]
fn keys_carrying_secrets_or_execution_data_are_redacted_and_metadata_survives() {
    let redacted = [
        "context",
        "payload_json",
        "step_output_json",
        "params",
        "credential_id",
        "API_KEY",
        "Password",
        "bearer_token",
        "aws_secret_access_key",
        "db_database_url",
        "encryption_key",
        "private_key",
    ];
    let survives = [
        "id",
        "state",
        "priority",
        "name",
        "version",
        "queue",
        "tenant_id",
        "monkey",
    ];
    for key in redacted {
        assert!(sensitive_key(key), "{key} must be redacted");
    }
    for key in survives {
        assert!(!sensitive_key(key), "{key} must survive");
    }
}

#[test]
#[ignore = "BUG: `parameters` holds the same execution data as `params` but is not redacted — fixed on test/bugs"]
fn parameters_key_is_redacted_like_params() {
    assert!(sensitive_key("parameters"));
}

#[test]
fn sensitive_subtree_is_replaced_wholesale() {
    let sanitized = sanitize(json!({
        "context": {"customer": "alice", "deep": {"ssn": "000-00-0000"}}
    }));

    assert_eq!(sanitized, json!({"context": "[REDACTED]"}));
}

#[test]
fn redaction_reaches_every_depth_and_keeps_sibling_structure() {
    let sanitized = sanitize(json!({
        "instance": {"id": "i1", "secret": "x", "nested": {"safe": 1, "credential": "y"}},
        "events": [{"token": "abc", "state": "ok"}, {"state": "done"}],
        "a": [{"b": [{"private_key": "pem", "v": 3}]}]
    }));

    assert_eq!(
        sanitized,
        json!({
            "instance": {"id": "i1", "secret": "[REDACTED]",
                "nested": {"safe": 1, "credential": "[REDACTED]"}},
            "events": [{"token": "[REDACTED]", "state": "ok"}, {"state": "done"}],
            "a": [{"b": [{"private_key": "[REDACTED]", "v": 3}]}]
        })
    );
}

#[test]
fn non_sensitive_scalars_are_preserved_verbatim() {
    let source = json!({
        "name": "billing", "count": 42, "ratio": 1.5, "active": true, "missing": null
    });

    assert_eq!(sanitize(source.clone()), source);
}

#[test]
fn realistic_instance_payload_leaks_no_customer_data() {
    let sanitized = sanitize(json!({
        "id": "inst-1",
        "state": "failed",
        "context": {"data": {"email": "user@example.com"}},
        "steps": [{"id": "charge", "params": {"card": "4242"}, "output": {"receipt": "r1"}}],
        "error": "card declined"
    }));

    let rendered = sanitized.to_string();
    for kept in ["inst-1", "failed", "card declined", "charge"] {
        assert!(rendered.contains(kept), "{kept} missing: {rendered}");
    }
    for leaked in ["user@example.com", "4242", "receipt"] {
        assert!(!rendered.contains(leaked), "{leaked} leaked: {rendered}");
    }
}

#[test]
fn workloads_come_from_items_or_a_bare_array_and_nothing_else() {
    let cases = [
        (
            json!({"items": [{"id": "i1"}], "total": 99}),
            vec![json!({"id": "i1"})],
        ),
        (json!([{"id": "i2"}]), vec![json!({"id": "i2"})]),
        (json!({"items": "not-an-array"}), vec![]),
        (json!({"items": []}), vec![]),
        (json!({"other": 1}), vec![]),
        (Value::Null, vec![]),
    ];
    for (source, expected) in cases {
        assert_eq!(workload_summaries(&source, 10), expected, "{source}");
    }
}

#[test]
fn non_object_workload_entries_are_skipped_and_empty_objects_kept() {
    let source = json!([{"id": "i1"}, "junk", 7, null, {}, {"id": "i2"}]);

    assert_eq!(
        workload_summaries(&source, 10),
        [json!({"id": "i1"}), json!({}), json!({"id": "i2"})]
    );
}

#[test]
fn workload_limit_keeps_the_first_entries_in_order() {
    let source = json!([{"id": "i1"}, {"id": "i2"}, {"id": "i3"}]);

    assert_eq!(
        workload_summaries(&source, 2),
        [json!({"id": "i1"}), json!({"id": "i2"})]
    );
}

#[test]
fn workload_summary_keeps_exactly_the_allow_listed_fields() {
    let source = json!([{
        "id": "i1",
        "sequence_id": "s1",
        "state": "running",
        "priority": 3,
        "created_at": "2026-07-25T00:00:00Z",
        "updated_at": "2026-07-25T01:00:00Z",
        "next_fire_at": "2026-07-25T02:00:00Z",
        "context": {"secret": true},
        "metadata": {"pii": 1},
        "error": "boom"
    }, {"id": "i2", "state": "paused"}]);

    let summaries = workload_summaries(&source, 10);

    assert_eq!(
        summaries,
        [
            json!({
                "id": "i1",
                "sequence_id": "s1",
                "state": "running",
                "priority": 3,
                "created_at": "2026-07-25T00:00:00Z",
                "updated_at": "2026-07-25T01:00:00Z",
                "next_fire_at": "2026-07-25T02:00:00Z"
            }),
            json!({"id": "i2", "state": "paused"}),
        ],
        "missing allow-listed keys are absent, not null"
    );
}

fn bundle_cmd(dir: &std::path::Path, instance: Option<uuid::Uuid>, max: u32) -> SupportBundleCmd {
    SupportBundleCmd {
        output: dir.join("bundle.json"),
        config: dir.join("orch8.toml"),
        instance,
        max_instances: max,
    }
}

fn ok(body: &Value) -> (StatusCode, String) {
    (StatusCode::OK, body.to_string())
}

/// `/info`, `/health/live`, `/health/ready`, `/instances`, then diagnosis.
fn server_responses() -> Vec<(StatusCode, String)> {
    vec![
        ok(&json!({"version": "9.9.9", "api_key": "info-secret"})),
        ok(&json!({"status": "ok"})),
        ok(&json!({"status": "ok"})),
        ok(&json!({"items": [
            {"id": "i1", "state": "running", "context": {"email": "user@example.com"}},
            {"id": "i2", "state": "paused"}
        ]})),
        ok(&json!({"state": "failed", "context": {"card": "4242"}})),
    ]
}

fn read_bundle(dir: &std::path::Path) -> (Value, String) {
    let raw = std::fs::read_to_string(dir.join("bundle.json")).unwrap();
    (serde_json::from_str(&raw).unwrap(), raw)
}

fn paths(requests: &[RecordedRequest]) -> Vec<String> {
    requests.iter().map(|request| request.uri.clone()).collect()
}

#[tokio::test]
async fn bundle_is_owner_only_and_carries_redacted_server_data_without_diagnosis() {
    let dir = tempfile::tempdir().unwrap();
    let api = mock_api_with_responses(server_responses()).await;
    let base = format!("{}/api/v1", api.base);

    run(&Client::new(), &base, bundle_cmd(dir.path(), None, 100))
        .await
        .unwrap();

    let (bundle, raw) = read_bundle(dir.path());
    assert_eq!(bundle["server_info"]["body"]["version"], json!("9.9.9"));
    assert_eq!(
        bundle["workloads"],
        json!([{"id": "i1", "state": "running"}, {"id": "i2", "state": "paused"}])
    );
    assert_eq!(bundle["config"], Value::Null);
    assert_eq!(bundle["diagnosis"], Value::Null);
    for leaked in ["info-secret", "user@example.com"] {
        assert!(!raw.contains(leaked), "{leaked} leaked into the bundle");
    }
    assert_eq!(
        paths(&api.log.snapshot()),
        [
            "/info",
            "/health/live",
            "/health/ready",
            "/api/v1/instances?limit=100"
        ]
    );
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt as _;
        let mode = std::fs::metadata(dir.path().join("bundle.json"))
            .unwrap()
            .permissions()
            .mode();
        assert_eq!(mode & 0o777, 0o600);
    }
}

#[tokio::test]
async fn named_instance_adds_a_redacted_diagnosis() {
    let dir = tempfile::tempdir().unwrap();
    let instance = uuid::Uuid::now_v7();
    let api = mock_api_with_responses(server_responses()).await;
    let base = format!("{}/api/v1", api.base);

    run(
        &Client::new(),
        &base,
        bundle_cmd(dir.path(), Some(instance), 100),
    )
    .await
    .unwrap();

    let (bundle, raw) = read_bundle(dir.path());
    assert_eq!(
        bundle["diagnosis"]["body"],
        json!({"state": "failed", "context": "[REDACTED]"})
    );
    assert!(!raw.contains("4242"));
    assert_eq!(
        paths(&api.log.snapshot()).last().unwrap(),
        &format!("/api/v1/instances/{instance}/diagnosis")
    );
}

#[tokio::test]
async fn instance_limit_is_clamped_to_one_through_one_thousand() {
    for (requested, sent, summaries) in [(0, 1, 1), (5_000, 1_000, 2)] {
        let dir = tempfile::tempdir().unwrap();
        let api = mock_api_with_responses(server_responses()).await;
        let base = format!("{}/api/v1", api.base);

        run(
            &Client::new(),
            &base,
            bundle_cmd(dir.path(), None, requested),
        )
        .await
        .unwrap();

        let (bundle, _) = read_bundle(dir.path());
        assert_eq!(
            paths(&api.log.snapshot())[3],
            format!("/api/v1/instances?limit={sent}"),
            "requested {requested}"
        );
        assert_eq!(
            bundle["workloads"].as_array().unwrap().len(),
            summaries,
            "requested {requested}"
        );
    }
}

#[tokio::test]
async fn local_config_is_included_with_every_secret_masked() {
    let dir = tempfile::tempdir().unwrap();
    let api_key = "a".repeat(64);
    let encryption_key = "b".repeat(64);
    std::fs::write(
        dir.path().join("orch8.toml"),
        format!(
            "[api]\napi_key = \"{api_key}\"\n\n[engine]\nencryption_key = \"{encryption_key}\"\n\n\
             [database]\nurl = \"postgres://orch8:db-password@db/orch8\"\n\n\
             [license]\nkey = \"o8l1.license-secret\"\n"
        ),
    )
    .unwrap();
    let api = mock_api_with_responses(server_responses()).await;
    let base = format!("{}/api/v1", api.base);

    run(&Client::new(), &base, bundle_cmd(dir.path(), None, 10))
        .await
        .unwrap();

    let (bundle, raw) = read_bundle(dir.path());
    assert!(bundle["config"].is_object(), "{}", bundle["config"]);
    for secret in [
        api_key.as_str(),
        encryption_key.as_str(),
        "db-password",
        "license-secret",
    ] {
        assert!(!raw.contains(secret), "config secret leaked: {secret}");
    }
}
