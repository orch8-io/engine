//! End-to-end: interactive approval actions (magic links, Slack), public
//! progress links, and alert-rule CRUD.
#![allow(clippy::too_many_lines)]

use chrono::Utc;
use hmac::{KeyInit, Mac};
use orch8_api::test_harness::{TestServer, spawn_test_server};
use orch8_storage::{AdminStore, InstanceStore, SignalStore};
use orch8_types::approval_link::{ApprovalActionToken, ApprovalChannel, hash_token};
use orch8_types::context::ExecutionContext;
use orch8_types::ids::{BlockId, InstanceId, Namespace, SequenceId, TenantId};
use orch8_types::instance::{InstanceState, Priority, TaskInstance};
use reqwest::StatusCode;
use serde_json::{Value, json};

const TENANT: &str = "acme";

async fn setup() -> (TestServer, reqwest::Client, InstanceId) {
    let srv = spawn_test_server().await;
    let client = reqwest::Client::new();
    let sequence_id = SequenceId::new();
    let now = Utc::now();
    let resp = client
        .post(format!("{}/sequences", srv.v1_url()))
        .header("X-Tenant-Id", TENANT)
        .json(&json!({"id": sequence_id, "tenant_id": TENANT, "namespace": "default", "name": "refund",
            "version": 1, "created_at": now, "blocks": [
                {"type": "step", "id": "ask_manager", "handler": "noop", "params": {}},
                {"type": "step", "id": "manager_decision", "handler": "noop", "params": {},
                 "wait_for_input": {"prompt": "Refund <b>$120</b>?", "choices": [
                    {"label": "Approve", "value": "approve"}, {"label": "Reject", "value": "reject"}]}},
                {"type": "step", "id": "pay_out", "handler": "noop", "params": {}}
            ]}))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::CREATED);
    let instance = TaskInstance {
        id: InstanceId::new(),
        sequence_id,
        tenant_id: TenantId::unchecked(TENANT),
        namespace: Namespace::new("default"),
        state: InstanceState::Waiting,
        next_fire_at: None,
        priority: Priority::Normal,
        timezone: "UTC".into(),
        metadata: json!({}),
        context: ExecutionContext {
            data: json!({"order_id": "ord_42", "email": "secret@customer.example", "amount": 120}),
            ..ExecutionContext::default()
        },
        concurrency_key: None,
        max_concurrency: None,
        idempotency_key: None,
        session_id: None,
        parent_instance_id: None,
        budget: None,
        created_at: now,
        updated_at: now,
    };
    srv.storage.create_instance(&instance).await.unwrap();
    (srv, client, instance.id)
}

async fn issue(
    srv: &TestServer,
    instance: InstanceId,
    choice: &str,
    channel: ApprovalChannel,
    ttl_secs: i64,
    verify_ref: Option<&str>,
) -> String {
    let raw = format!(
        "{}{}",
        uuid::Uuid::new_v4().simple(),
        uuid::Uuid::new_v4().simple()
    );
    let now = Utc::now();
    srv.storage
        .create_approval_tokens(&[ApprovalActionToken {
            token_hash: hash_token(&raw),
            tenant_id: TenantId::unchecked(TENANT),
            instance_id: instance,
            block_id: BlockId::new("manager_decision"),
            choice: choice.into(),
            channel,
            recipient: Some("boss@acme.example".into()),
            verify_secret_ref: verify_ref.map(str::to_string),
            created_at: now,
            expires_at: now + chrono::Duration::seconds(ttl_secs),
            used_at: None,
        }])
        .await
        .unwrap();
    raw
}

async fn human_signals(srv: &TestServer, instance: InstanceId) -> Vec<Value> {
    srv.storage
        .get_pending_signals(instance)
        .await
        .unwrap()
        .into_iter()
        .filter(|s| matches!(&s.signal_type, orch8_types::signal::SignalType::Custom(n) if n == "human_input:manager_decision"))
        .map(|s| s.payload)
        .collect()
}

#[tokio::test]
async fn magic_link_get_is_safe_and_post_records_once() {
    let (srv, client, instance) = setup().await;
    let raw = issue(
        &srv,
        instance,
        "approve",
        ApprovalChannel::Email,
        3600,
        None,
    )
    .await;
    let sibling = issue(&srv, instance, "reject", ApprovalChannel::Email, 3600, None).await;
    let url = format!("{}/approvals/act/{raw}", srv.base_url);

    // GET (prefetcher / scanner) never changes state.
    for _ in 0..2 {
        let resp = client.get(&url).send().await.unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
        let csp = resp.headers()["content-security-policy"]
            .to_str()
            .unwrap()
            .to_string();
        assert!(csp.contains("default-src 'none'") && !csp.contains("unsafe-inline"));
        assert_eq!(resp.headers()["referrer-policy"], "no-referrer");
        let html = resp.text().await.unwrap();
        assert!(html.contains("Confirm: Approve"));
        assert!(
            html.contains("Refund &lt;b&gt;$120&lt;/b&gt;?"),
            "prompt must be escaped"
        );
    }
    assert!(human_signals(&srv, instance).await.is_empty());

    let resp = client
        .post(&url)
        .form(&[("comment", "looks fine")])
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let signals = human_signals(&srv, instance).await;
    assert_eq!(signals.len(), 1);
    assert_eq!(signals[0]["value"], "approve");
    assert_eq!(signals[0]["comment"], "looks fine");
    assert_eq!(signals[0]["decided_by"]["channel"], "email");

    let audit = srv.storage.list_audit_log(instance, 50).await.unwrap();
    let entry = audit
        .iter()
        .find(|a| a.event_type == "approval_decision")
        .expect("audit entry");
    assert_eq!(entry.block_id.as_deref(), Some("manager_decision"));
    assert_eq!(entry.details["choice"], "approve");

    // Single use, and the sibling choice is burned too.
    assert_eq!(
        client.post(&url).send().await.unwrap().status(),
        StatusCode::GONE
    );
    let sib = client
        .post(format!("{}/approvals/act/{sibling}", srv.base_url))
        .header("accept", "application/json")
        .send()
        .await
        .unwrap();
    assert_eq!(sib.status(), StatusCode::GONE);
    assert_eq!(human_signals(&srv, instance).await.len(), 1);
}

#[tokio::test]
async fn expired_unknown_and_slack_tokens_are_refused_on_magic_link_route() {
    let (srv, client, instance) = setup().await;
    let expired = issue(&srv, instance, "approve", ApprovalChannel::Teams, -10, None).await;
    let slack = issue(
        &srv,
        instance,
        "approve",
        ApprovalChannel::Slack,
        3600,
        Some("slack-app/signing_secret"),
    )
    .await;
    for raw in [expired.as_str(), slack.as_str(), "short", &"x".repeat(43)] {
        let url = format!("{}/approvals/act/{raw}", srv.base_url);
        assert_eq!(
            client.get(&url).send().await.unwrap().status(),
            StatusCode::GONE,
            "{raw}"
        );
        assert_eq!(
            client.post(&url).send().await.unwrap().status(),
            StatusCode::GONE,
            "{raw}"
        );
    }
    assert!(human_signals(&srv, instance).await.is_empty());
    // JSON (Teams Action.Http style) callers get JSON.
    let teams = issue(&srv, instance, "reject", ApprovalChannel::Teams, 3600, None).await;
    let resp = client
        .post(format!("{}/approvals/act/{teams}", srv.base_url))
        .json(&json!({"comment": "no budget"}))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let body: Value = resp.json().await.unwrap();
    assert_eq!(body["status"], "recorded");
    assert_eq!(human_signals(&srv, instance).await[0]["value"], "reject");
}

fn slack_sign(secret: &[u8], ts: &str, body: &str) -> String {
    let mut mac = hmac::Hmac::<sha2::Sha256>::new_from_slice(secret).unwrap();
    mac.update(format!("v0:{ts}:{body}").as_bytes());
    mac.finalize()
        .into_bytes()
        .iter()
        .fold(String::from("v0="), |mut s, b| {
            use std::fmt::Write as _;
            let _ = write!(s, "{b:02x}");
            s
        })
}

#[tokio::test]
async fn slack_interaction_requires_valid_signature_and_is_single_use() {
    let (srv, client, instance) = setup().await;
    client
        .post(format!("{}/credentials", srv.base_url))
        .header("X-Tenant-Id", TENANT)
        .json(
            &json!({"id": "slack-app", "name": "slack", "kind": "api_key", "tenant_id": TENANT,
            "value": "{\"signing_secret\":\"slack-signing-secret\"}"}),
        )
        .send()
        .await
        .unwrap()
        .error_for_status()
        .unwrap();
    let raw = issue(
        &srv,
        instance,
        "approve",
        ApprovalChannel::Slack,
        3600,
        Some("slack-app/signing_secret"),
    )
    .await;
    let payload = json!({"type": "block_actions", "user": {"id": "U1", "username": "boss"},
        "team": {"id": "T1"}, "actions": [{"action_id": "orch8_approval_0", "value": raw}]});
    let body = format!(
        "payload={}",
        url::form_urlencoded::byte_serialize(payload.to_string().as_bytes()).collect::<String>()
    );
    let url = format!("{}/approvals/slack/interactions", srv.base_url);
    let send = |ts: String, sig: String| {
        client
            .post(&url)
            .header("content-type", "application/x-www-form-urlencoded")
            .header("x-slack-request-timestamp", ts)
            .header("x-slack-signature", sig)
            .body(body.clone())
            .send()
    };
    let now = Utc::now().timestamp().to_string();
    // Wrong secret / stale timestamp are rejected before the token is used.
    assert_eq!(
        send(now.clone(), slack_sign(b"wrong", &now, &body))
            .await
            .unwrap()
            .status(),
        StatusCode::UNAUTHORIZED
    );
    let old = (Utc::now().timestamp() - 600).to_string();
    assert_eq!(
        send(
            old.clone(),
            slack_sign(b"slack-signing-secret", &old, &body)
        )
        .await
        .unwrap()
        .status(),
        StatusCode::UNAUTHORIZED
    );
    assert!(human_signals(&srv, instance).await.is_empty());

    let ok = send(
        now.clone(),
        slack_sign(b"slack-signing-secret", &now, &body),
    )
    .await
    .unwrap();
    assert_eq!(ok.status(), StatusCode::OK);
    let signals = human_signals(&srv, instance).await;
    assert_eq!(signals.len(), 1);
    assert_eq!(signals[0]["decided_by"]["user_id"], "U1");
    // A second click is acknowledged but records nothing.
    let again = send(
        now.clone(),
        slack_sign(b"slack-signing-secret", &now, &body),
    )
    .await
    .unwrap();
    assert_eq!(again.status(), StatusCode::OK);
    assert!(again.text().await.unwrap().contains("already recorded"));
    assert_eq!(human_signals(&srv, instance).await.len(), 1);
}

#[tokio::test]
async fn public_progress_is_redacted_revocable_and_tenant_scoped() {
    let (srv, client, instance) = setup().await;
    let share_url = format!("{}/instances/{instance}/share", srv.v1_url());
    // Another tenant cannot mint a share.
    let foreign = client
        .post(&share_url)
        .header("X-Tenant-Id", "evil")
        .json(&json!({}))
        .send()
        .await
        .unwrap();
    assert_eq!(foreign.status(), StatusCode::NOT_FOUND);
    // Bad allowlist.
    let bad = client
        .post(&share_url)
        .header("X-Tenant-Id", TENANT)
        .json(&json!({"allowed_fields": ["a.b"]}))
        .send()
        .await
        .unwrap();
    assert_eq!(bad.status(), StatusCode::BAD_REQUEST);

    let created: Value = client
        .post(&share_url)
        .header("X-Tenant-Id", TENANT)
        .json(&json!({"expires_in_secs": 3600}))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    let token = created["token"].as_str().unwrap().to_string();
    assert!(
        created["embed_snippet"]
            .as_str()
            .unwrap()
            .contains("embed.js")
    );

    let resp = client
        .get(format!("{}/public/progress/{token}", srv.base_url))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    assert_eq!(resp.headers()["cache-control"], "no-store");
    let view: Value = resp.json().await.unwrap();
    assert_eq!(view["total"], 3);
    assert_eq!(view["steps"][1]["label"], "Manager decision");
    assert!(
        view.get("data").is_none(),
        "no context data without an allowlist"
    );
    let text = view.to_string();
    assert!(!text.contains("secret@customer.example") && !text.contains("ord_42"));

    // Allowlisted field only.
    let with_fields: Value = client
        .post(&share_url)
        .header("X-Tenant-Id", TENANT)
        .json(&json!({"allowed_fields": ["order_id"]}))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    let t2 = with_fields["token"].as_str().unwrap();
    let v2: Value = client
        .get(format!("{}/public/progress/{t2}", srv.base_url))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(v2["data"], json!({"order_id": "ord_42"}));

    // Embed page + loader.
    let embed = client
        .get(format!("{}/public/progress/{token}/embed", srv.base_url))
        .send()
        .await
        .unwrap();
    assert_eq!(embed.status(), StatusCode::OK);
    let csp = embed.headers()["content-security-policy"]
        .to_str()
        .unwrap()
        .to_string();
    assert!(
        csp.contains("script-src 'sha256-")
            && csp.contains("connect-src 'self'")
            && !csp.contains("unsafe")
    );
    let loader = client
        .get(format!("{}/public/progress/embed.js", srv.base_url))
        .send()
        .await
        .unwrap();
    assert_eq!(loader.status(), StatusCode::OK);
    assert!(
        loader.headers()["content-type"]
            .to_str()
            .unwrap()
            .contains("javascript")
    );

    // Listing never returns tokens; revoke closes the link (fail-closed 404).
    let shares: Value = client
        .get(format!("{}/instances/{instance}/shares", srv.v1_url()))
        .header("X-Tenant-Id", TENANT)
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(shares.as_array().unwrap().len(), 2);
    assert!(!shares.to_string().contains(&token));
    let id = created["id"].as_str().unwrap();
    let del = client
        .delete(format!("{}/instances/{instance}/share/{id}", srv.v1_url()))
        .header("X-Tenant-Id", TENANT)
        .send()
        .await
        .unwrap();
    assert_eq!(del.status(), StatusCode::NO_CONTENT);
    for path in [
        format!("/public/progress/{token}"),
        format!("/public/progress/{token}/embed"),
    ] {
        assert_eq!(
            client
                .get(format!("{}{path}", srv.base_url))
                .send()
                .await
                .unwrap()
                .status(),
            StatusCode::NOT_FOUND
        );
    }
    assert_eq!(
        client
            .get(format!(
                "{}/public/progress/{}",
                srv.base_url,
                "z".repeat(43)
            ))
            .send()
            .await
            .unwrap()
            .status(),
        StatusCode::NOT_FOUND
    );
}

#[tokio::test]
async fn alert_rule_crud_is_tenant_scoped_and_rejects_literal_secrets() {
    let srv = spawn_test_server().await;
    let client = reqwest::Client::new();
    let url = format!("{}/alerts/rules", srv.v1_url());
    let literal = client
        .post(&url)
        .header("X-Tenant-Id", TENANT)
        .json(
            &json!({"name": "dlq", "condition": {"kind": "dlq_growth", "threshold": 10},
            "destination": {"type": "slack", "url_ref": "https://hooks.slack.com/services/x"}}),
        )
        .send()
        .await
        .unwrap();
    assert_eq!(literal.status(), StatusCode::BAD_REQUEST);

    let created: Value = client
        .post(&url)
        .header("X-Tenant-Id", TENANT)
        .json(&json!({"name": "dlq", "condition": {"kind": "dlq_growth", "threshold": 10, "growth": 5},
            "destination": {"type": "pagerduty", "routing_key_ref": "credentials://pd/routing_key"},
            "cooldown_secs": 600}))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    let id = created["id"].as_str().unwrap().to_string();
    assert_eq!(created["tenant_id"], TENANT);

    let list: Value = client
        .get(&url)
        .header("X-Tenant-Id", TENANT)
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(list.as_array().unwrap().len(), 1);
    let other: Value = client
        .get(&url)
        .header("X-Tenant-Id", "other")
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert!(other.as_array().unwrap().is_empty());
    assert_eq!(
        client
            .get(format!("{url}/{id}"))
            .header("X-Tenant-Id", "other")
            .send()
            .await
            .unwrap()
            .status(),
        StatusCode::NOT_FOUND
    );

    let updated = client
        .put(format!("{url}/{id}"))
        .header("X-Tenant-Id", TENANT)
        .json(&json!({"name": "dlq-renamed", "enabled": false,
            "condition": {"kind": "worker_pool_empty", "handler": "charge"},
            "destination": {"type": "webhook", "url": "https://ops.example.com/hook", "secret_ref": "credentials://hook"}}))
        .send()
        .await
        .unwrap();
    assert_eq!(updated.status(), StatusCode::OK);
    let got: Value = client
        .get(format!("{url}/{id}"))
        .header("X-Tenant-Id", TENANT)
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(got["name"], "dlq-renamed");
    assert_eq!(got["condition"]["kind"], "worker_pool_empty");

    assert_eq!(
        client
            .delete(format!("{url}/{id}"))
            .header("X-Tenant-Id", "other")
            .send()
            .await
            .unwrap()
            .status(),
        StatusCode::NOT_FOUND
    );
    assert_eq!(
        client
            .delete(format!("{url}/{id}"))
            .header("X-Tenant-Id", TENANT)
            .send()
            .await
            .unwrap()
            .status(),
        StatusCode::NO_CONTENT
    );
    assert!(
        srv.storage
            .get_alert_rule(None, uuid::Uuid::parse_str(&id).unwrap())
            .await
            .unwrap()
            .is_none()
    );
}
