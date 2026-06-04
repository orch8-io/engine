//! Integration tests for per-tenant API key authentication.
//!
//! Verifies that a per-tenant key binds tenant identity to the key record
//! (so the unauthenticated `X-Tenant-Id` header can't cross tenants), that the
//! root key remains an unscoped admin, and that revoked/expired/unknown keys
//! are rejected.

use std::sync::Arc;

use axum::routing::get;
use axum::Router;
use orch8_storage::sqlite::SqliteStorage;
use orch8_storage::StorageBackend;
use orch8_types::api_key;

const ROOT_KEY: &str = "root-admin-key-supersecret";

/// Probe handler: report the resolved tenant (or "admin"/"anon") so tests can
/// assert which identity the auth layer established.
async fn whoami(
    tenant: orch8_api::auth::OptionalTenant,
    admin: orch8_api::auth::OptionalAdmin,
) -> String {
    if let Some(axum::Extension(ctx)) = tenant {
        format!("tenant:{}", ctx.tenant_id.as_str())
    } else if admin.is_some() {
        "admin".to_string()
    } else {
        "anon".to_string()
    }
}

struct Srv {
    base: String,
    _shutdown: tokio_util::sync::CancellationToken,
}

async fn spawn(storage: Arc<dyn StorageBackend>, require_tenant: bool) -> Srv {
    let shutdown = tokio_util::sync::CancellationToken::new();
    let auth_storage = storage.clone();
    let root_digest = orch8_types::auth::precompute_secret_digest(ROOT_KEY);
    let app: Router = Router::new()
        .route("/whoami", get(whoami))
        .layer(axum::middleware::from_fn(move |req, next| async move {
            orch8_api::auth::tenant_middleware(require_tenant, req, next).await
        }))
        .layer(axum::middleware::from_fn(move |req, next| {
            let s = auth_storage.clone();
            async move {
                orch8_api::auth::api_key_middleware(s, Some(root_digest), req, next).await
            }
        }));

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let cancel = shutdown.clone();
    let (ready_tx, ready_rx) = tokio::sync::oneshot::channel::<()>();
    tokio::spawn(async move {
        let _ = ready_tx.send(());
        let _ = axum::serve(listener, app)
            .with_graceful_shutdown(async move { cancel.cancelled().await })
            .await;
    });
    ready_rx.await.unwrap();
    Srv {
        base: format!("http://{addr}"),
        _shutdown: shutdown,
    }
}

async fn storage() -> Arc<dyn StorageBackend> {
    Arc::new(SqliteStorage::in_memory().await.unwrap())
}

#[tokio::test]
async fn root_key_is_admin_when_tenant_optional() {
    // With tenant enforcement off, the root key authenticates as the unscoped
    // admin and needs no tenant header.
    let srv = spawn(storage().await, false).await;
    let body = reqwest::Client::new()
        .get(format!("{}/whoami", srv.base))
        .header("x-api-key", ROOT_KEY)
        .send()
        .await
        .unwrap()
        .text()
        .await
        .unwrap();
    assert_eq!(body, "admin");
}

#[tokio::test]
async fn root_key_without_tenant_header_rejected_when_required() {
    // `require_tenant` applies uniformly: the root key is NOT exempt — it must
    // present an X-Tenant-Id. (Only a per-tenant key, which binds its own
    // tenant, is exempt.)
    let srv = spawn(storage().await, true).await;
    let status = reqwest::Client::new()
        .get(format!("{}/whoami", srv.base))
        .header("x-api-key", ROOT_KEY)
        .send()
        .await
        .unwrap()
        .status();
    assert_eq!(status, reqwest::StatusCode::BAD_REQUEST);

    // Supplying a header lets the admin through, scoped to that tenant.
    let body = reqwest::Client::new()
        .get(format!("{}/whoami", srv.base))
        .header("x-api-key", ROOT_KEY)
        .header("x-tenant-id", "acme")
        .send()
        .await
        .unwrap()
        .text()
        .await
        .unwrap();
    assert_eq!(body, "tenant:acme");
}

#[tokio::test]
async fn missing_key_is_unauthorized() {
    let srv = spawn(storage().await, true).await;
    let status = reqwest::Client::new()
        .get(format!("{}/whoami", srv.base))
        .send()
        .await
        .unwrap()
        .status();
    assert_eq!(status, reqwest::StatusCode::UNAUTHORIZED);
}

#[tokio::test]
async fn per_tenant_key_binds_tenant_from_record() {
    let store = storage().await;
    let minted = api_key::mint("acme", "ci", None);
    store.create_api_key(&minted.record).await.unwrap();
    let srv = spawn(store, true).await;

    // No header at all → tenant comes from the key, not a 400.
    let body = reqwest::Client::new()
        .get(format!("{}/whoami", srv.base))
        .header("x-api-key", &minted.secret)
        .send()
        .await
        .unwrap()
        .text()
        .await
        .unwrap();
    assert_eq!(body, "tenant:acme");
}

#[tokio::test]
async fn per_tenant_key_cannot_spoof_other_tenant_via_header() {
    let store = storage().await;
    let minted = api_key::mint("acme", "ci", None);
    store.create_api_key(&minted.record).await.unwrap();
    let srv = spawn(store, true).await;

    // A mismatching X-Tenant-Id must be rejected — the key can't act as "evil".
    let status = reqwest::Client::new()
        .get(format!("{}/whoami", srv.base))
        .header("x-api-key", &minted.secret)
        .header("x-tenant-id", "evil")
        .send()
        .await
        .unwrap()
        .status();
    assert_eq!(status, reqwest::StatusCode::FORBIDDEN);

    // A matching header is fine.
    let body = reqwest::Client::new()
        .get(format!("{}/whoami", srv.base))
        .header("x-api-key", &minted.secret)
        .header("x-tenant-id", "acme")
        .send()
        .await
        .unwrap()
        .text()
        .await
        .unwrap();
    assert_eq!(body, "tenant:acme");
}

#[tokio::test]
async fn revoked_key_is_rejected() {
    let store = storage().await;
    let minted = api_key::mint("acme", "ci", None);
    store.create_api_key(&minted.record).await.unwrap();
    store.revoke_api_key(&minted.record.id).await.unwrap();
    let srv = spawn(store, true).await;

    let status = reqwest::Client::new()
        .get(format!("{}/whoami", srv.base))
        .header("x-api-key", &minted.secret)
        .send()
        .await
        .unwrap()
        .status();
    assert_eq!(status, reqwest::StatusCode::UNAUTHORIZED);
}

#[tokio::test]
async fn unknown_key_is_rejected() {
    let srv = spawn(storage().await, true).await;
    let status = reqwest::Client::new()
        .get(format!("{}/whoami", srv.base))
        .header("x-api-key", "sk_not_a_real_key")
        .send()
        .await
        .unwrap()
        .status();
    assert_eq!(status, reqwest::StatusCode::UNAUTHORIZED);
}
