//! Runtime-node identity and control-plane client for a mobile device.
//!
//! A phone joins the distributed execution mesh as a runtime node of kind
//! `mobile`. This module owns:
//!
//! - the node's **persistent identity** (`runtime_id`, a UUID stored in the
//!   local `SQLite` database so it survives app restarts and OS kills);
//! - the **capability advertisement** sent to the control plane
//!   (`POST /mobile/devices/register`, `POST /mobile/devices/{id}/runtime`,
//!   and the `capabilities` block of every worker poll);
//! - the **lease protocol HTTP client** (`/workers/tasks/poll`, `heartbeat`,
//!   `complete`, `fail`, `release`) used by [`crate::worker`].
//!
//! Every capability advertisement lives for at most five minutes on the
//! server, so [`NodeClient::capabilities`] always stamps a fresh
//! `observed_at`/`expires_at` pair and the engine re-advertises well before
//! the TTL lapses.

use std::sync::{Arc, Mutex as StdMutex};
use std::time::Duration;

use serde::{Deserialize, Serialize};
use sqlx::SqlitePool;
use tracing::debug;

use orch8_types::continuity::RuntimeId;
use orch8_types::continuity::{
    RuntimeCapabilities, RuntimeConnectivity, RuntimeKind, RuntimeTrustLevel,
};

use crate::credential::Credential;
use crate::error::MobileError;

/// Lifetime of one capability advertisement. The server caps it at five
/// minutes; stay a little under so clock skew never makes it invalid.
pub(crate) const CAPABILITY_TTL: Duration = Duration::from_secs(290);
/// Re-advertise this long after the previous advertisement, leaving a
/// minute of slack for a slow network or a briefly suspended app.
pub(crate) const READVERTISE_INTERVAL: Duration = Duration::from_secs(230);
/// Cap on any control-plane response body we buffer.
const MAX_RESPONSE_BYTES: usize = 8 * 1024 * 1024;

/// Current network path, reported with the node's capabilities.
#[derive(Debug, Clone, Copy, PartialEq, Eq, uniffi::Enum)]
pub enum NodeConnectivity {
    Offline,
    Metered,
    Wifi,
    Ethernet,
}

impl From<NodeConnectivity> for RuntimeConnectivity {
    fn from(value: NodeConnectivity) -> Self {
        match value {
            NodeConnectivity::Offline => Self::Offline,
            NodeConnectivity::Metered => Self::Metered,
            NodeConnectivity::Wifi => Self::Wifi,
            NodeConnectivity::Ethernet => Self::Ethernet,
        }
    }
}

/// What this device advertises to the control plane when it joins the
/// runtime mesh. Every field has a default, so hosts only set what they know.
#[derive(Debug, Clone, uniffi::Record)]
pub struct NodeCapabilities {
    /// Handler names this node serves. Empty = every app-native handler
    /// registered with `register_handler`. Built-in handlers are only served
    /// remotely when listed here explicitly.
    #[uniffi(default)]
    pub handlers: Vec<String>,
    #[uniffi(default)]
    pub regions: Vec<String>,
    /// Free-form hardware facts (`camera`, `nfc`, `secure-enclave`, …).
    /// `device:<device_id>` is always added.
    #[uniffi(default)]
    pub hardware: Vec<String>,
    #[uniffi(default)]
    pub plugins: Vec<String>,
    /// Credential binding *names* available on the device (never secrets).
    #[uniffi(default)]
    pub credentials: Vec<String>,
    #[uniffi(default = true)]
    pub offline_capable: bool,
    #[uniffi(default = None)]
    pub connectivity: Option<NodeConnectivity>,
    #[uniffi(default = None)]
    pub battery_percent: Option<u8>,
    /// `ios` / `android`; inferred from the build target when absent.
    #[uniffi(default = None)]
    pub platform: Option<String>,
    /// APNs/FCM token used for id-only wake-up hints.
    #[uniffi(default = None)]
    pub push_token: Option<String>,
    #[uniffi(default = None)]
    pub app_version: Option<String>,
    /// Control-plane API base (e.g. `https://api.orch8.io/api/v1`). When
    /// absent it is derived from `sync_url` by stripping `/mobile/sync`.
    #[uniffi(default = None)]
    pub api_base_url: Option<String>,
    /// Base64 Ed25519 key that signs capsules exported by this device.
    #[uniffi(default = None)]
    pub capsule_signing_public_key: Option<String>,
}

impl Default for NodeCapabilities {
    fn default() -> Self {
        Self {
            handlers: Vec::new(),
            regions: Vec::new(),
            hardware: Vec::new(),
            plugins: Vec::new(),
            credentials: Vec::new(),
            offline_capable: true,
            connectivity: None,
            battery_percent: None,
            platform: None,
            push_token: None,
            app_version: None,
            api_base_url: None,
            capsule_signing_public_key: None,
        }
    }
}

/// Result of `register_node`.
#[derive(Debug, Clone, uniffi::Record)]
pub struct NodeRegistration {
    /// Stable runtime UUID (also the `worker_id` used for task leases).
    pub runtime_id: String,
    pub device_id: String,
    /// Handlers advertised to the control plane.
    pub handlers: Vec<String>,
    /// RFC 3339 expiry of the advertisement just sent; the engine refreshes
    /// it automatically before then.
    pub expires_at: String,
}

pub(crate) fn default_platform() -> &'static str {
    if cfg!(target_os = "ios") {
        "ios"
    } else if cfg!(target_os = "android") {
        "android"
    } else {
        std::env::consts::OS
    }
}

/// Derive the API base from a sync URL such as
/// `https://api.orch8.io/api/v1/mobile/sync` → `https://api.orch8.io/api/v1`.
pub(crate) fn derive_api_base(sync_url: &str) -> Option<String> {
    let trimmed = sync_url.trim_end_matches('/');
    trimmed
        .strip_suffix("/mobile/sync")
        .filter(|base| !base.is_empty())
        .map(str::to_string)
}

// ---------------------------------------------------------------------------
// Persistent identity
// ---------------------------------------------------------------------------

pub(crate) async fn init_tables(pool: &SqlitePool) -> Result<(), sqlx::Error> {
    sqlx::query(
        "CREATE TABLE IF NOT EXISTS mobile_node_identity (
            key   TEXT PRIMARY KEY,
            value TEXT NOT NULL
        )",
    )
    .execute(pool)
    .await?;
    Ok(())
}

/// Load the node's runtime id, creating and persisting one on first use.
/// `INSERT OR IGNORE` + re-read makes concurrent first calls converge on a
/// single id.
pub(crate) async fn load_or_create_runtime_id(pool: &SqlitePool) -> Result<RuntimeId, MobileError> {
    let fresh = uuid::Uuid::new_v4().to_string();
    sqlx::query("INSERT OR IGNORE INTO mobile_node_identity (key, value) VALUES ('runtime_id', ?)")
        .bind(&fresh)
        .execute(pool)
        .await
        .map_err(storage_err)?;
    let stored: String =
        sqlx::query_scalar("SELECT value FROM mobile_node_identity WHERE key = 'runtime_id'")
            .fetch_one(pool)
            .await
            .map_err(storage_err)?;
    let uuid = uuid::Uuid::parse_str(&stored).map_err(|e| MobileError::Engine {
        message: format!("corrupt stored runtime id: {e}"),
    })?;
    Ok(RuntimeId::from_uuid(uuid))
}

pub(crate) fn storage_err(e: sqlx::Error) -> MobileError {
    MobileError::Storage {
        message: e.to_string(),
    }
}

// ---------------------------------------------------------------------------
// HTTP client
// ---------------------------------------------------------------------------

/// Outcome class of a lease mutation (heartbeat/complete/fail/release).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum LeaseResponse {
    /// 2xx — accepted.
    Accepted,
    /// 404 / 409 / 410 — the task is gone or no longer ours (stale epoch,
    /// reclaimed, reaped). Stop acting on it and forget the claim.
    LostOwnership,
    /// 404/405 on an endpoint an older server may not have (release).
    Unsupported,
    /// Network error, 408/429/5xx — try again later.
    Retry,
    /// Any other 4xx — the request itself is wrong; retrying cannot help.
    Rejected,
}

#[derive(Debug, Deserialize)]
pub(crate) struct PollResponse {
    #[serde(default)]
    pub tasks: Vec<RemoteTask>,
    #[serde(default)]
    pub lease_secs: Option<u64>,
    #[serde(default)]
    pub heartbeat_interval_secs: Option<u64>,
    #[serde(default)]
    pub poll_after_ms: Option<u64>,
}

/// A claimed worker task, parsed leniently: only the fields the device needs
/// are required, and every distributed-execution field is optional so the
/// SDK works against servers with and without the v1 contract additions.
#[derive(Debug, Clone, Deserialize)]
pub(crate) struct RemoteTask {
    pub id: uuid::Uuid,
    pub instance_id: uuid::Uuid,
    pub block_id: String,
    pub handler_name: String,
    #[serde(default)]
    pub params: serde_json::Value,
    #[serde(default)]
    pub context: serde_json::Value,
    #[serde(default)]
    pub attempt: u32,
    #[serde(default)]
    pub timeout_ms: Option<i64>,
    #[serde(default)]
    pub claim_epoch: u64,
    #[serde(default)]
    pub effect_id: Option<String>,
    #[serde(default)]
    pub continuity_epoch: Option<u64>,
    #[serde(default)]
    pub lease_secs: Option<u32>,
    #[serde(default)]
    pub resume_checkpoint: Option<serde_json::Value>,
}

#[derive(Serialize)]
struct PollBody<'a> {
    handler_name: &'a str,
    worker_id: &'a str,
    limit: u32,
    #[serde(skip_serializing_if = "Option::is_none")]
    version: Option<&'a str>,
    capabilities: &'a RuntimeCapabilities,
}

/// Mutable advertisement facts (battery, connectivity, draining) that the
/// host can update between registrations.
#[derive(Debug, Clone)]
pub(crate) struct Advertisement {
    pub handlers: Vec<String>,
    pub caps: NodeCapabilities,
    pub draining: bool,
}

/// Control-plane client bound to one node identity and credential.
pub(crate) struct NodeClient {
    http: reqwest::Client,
    api_base: String,
    credential: Arc<Credential>,
    device_id: String,
    runtime_id: RuntimeId,
    advertisement: StdMutex<Advertisement>,
}

impl NodeClient {
    /// Build a client for a validated public HTTPS API base.
    pub fn new(
        api_base: String,
        credential: Arc<Credential>,
        device_id: String,
        runtime_id: RuntimeId,
        advertisement: Advertisement,
    ) -> Result<Arc<Self>, MobileError> {
        validate_api_base(&api_base)?;
        Ok(Self::new_unchecked(
            api_base,
            credential,
            device_id,
            runtime_id,
            advertisement,
        ))
    }

    /// Skip URL validation — tests point this at a loopback mock server.
    pub(crate) fn new_unchecked(
        api_base: String,
        credential: Arc<Credential>,
        device_id: String,
        runtime_id: RuntimeId,
        advertisement: Advertisement,
    ) -> Arc<Self> {
        Arc::new(Self {
            http: crate::build_mobile_http_client(Duration::from_secs(30)),
            api_base: api_base.trim_end_matches('/').to_string(),
            credential,
            device_id,
            runtime_id,
            advertisement: StdMutex::new(advertisement),
        })
    }

    pub fn runtime_id(&self) -> RuntimeId {
        self.runtime_id
    }

    pub fn worker_id(&self) -> String {
        self.runtime_id.to_string()
    }

    pub fn api_base(&self) -> &str {
        &self.api_base
    }

    /// Whether calls carry a scoped device session (which cannot publish
    /// sequences: the control plane publishes delegated steps itself).
    pub fn is_device_session(&self) -> bool {
        self.credential.is_device_session()
    }

    pub fn handlers(&self) -> Vec<String> {
        self.advertisement().handlers
    }

    fn advertisement(&self) -> Advertisement {
        self.advertisement
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone()
    }

    pub fn update_advertisement(&self, f: impl FnOnce(&mut Advertisement)) {
        f(&mut self
            .advertisement
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner));
    }

    /// A freshly stamped capability advertisement.
    pub fn capabilities(&self) -> RuntimeCapabilities {
        let ad = self.advertisement();
        let now = chrono::Utc::now();
        let ttl = chrono::Duration::from_std(CAPABILITY_TTL).unwrap_or(chrono::Duration::MAX);
        let mut hardware = ad.caps.hardware.clone();
        let device_fact = format!("device:{}", self.device_id);
        if !self.device_id.is_empty() && !hardware.contains(&device_fact) {
            hardware.push(device_fact);
        }
        RuntimeCapabilities {
            runtime_id: self.runtime_id,
            kind: RuntimeKind::Mobile,
            trust: RuntimeTrustLevel::Registered,
            handlers: ad.handlers,
            plugins: ad.caps.plugins,
            credentials: ad.caps.credentials,
            regions: ad.caps.regions,
            hardware,
            offline_capable: ad.caps.offline_capable,
            connectivity: ad.caps.connectivity.map(Into::into),
            battery_percent: ad.caps.battery_percent.map(|p| p.min(100)),
            estimated_cost_microunits: None,
            estimated_latency_ms: None,
            draining: ad.draining,
            capsule_signing_public_key: ad.caps.capsule_signing_public_key,
            observed_at: now,
            expires_at: now + ttl,
        }
    }

    fn url(&self, path: &str) -> String {
        format!("{}/{}", self.api_base, path.trim_start_matches('/'))
    }

    /// `POST` `body` to `path` with the node credential, refreshing it and
    /// retrying once on `401`.
    async fn post<B: Serialize + ?Sized>(
        &self,
        path: &str,
        body: &B,
    ) -> reqwest::Result<reqwest::Response> {
        let url = self.url(path);
        self.credential
            .send(|token| {
                self.http
                    .post(&url)
                    .header("x-api-key", token)
                    .header("x-device-id", &self.device_id)
                    .json(body)
            })
            .await
    }

    /// `POST /mobile/devices/register` then `POST /mobile/devices/{id}/runtime`.
    /// Returns the advertisement's expiry.
    pub async fn register(&self) -> Result<chrono::DateTime<chrono::Utc>, MobileError> {
        let ad = self.advertisement();
        let platform = ad
            .caps
            .platform
            .clone()
            .unwrap_or_else(|| default_platform().to_string());
        let device_body = serde_json::json!({
            "device_id": self.device_id,
            "push_token": ad.caps.push_token,
            "platform": platform,
            "app_version": ad.caps.app_version,
        });
        let resp = self
            .post("mobile/devices/register", &device_body)
            .await
            .map_err(|e| network_err("register device", &e))?;
        expect_success("register device", resp).await?;
        self.advertise().await
    }

    /// Refresh the runtime advertisement (liveness + current facts).
    pub async fn advertise(&self) -> Result<chrono::DateTime<chrono::Utc>, MobileError> {
        let capabilities = self.capabilities();
        let expires_at = capabilities.expires_at;
        let path = format!(
            "mobile/devices/{}/runtime",
            urlencode_path_segment(&self.device_id)
        );
        let resp = self
            .post(&path, &serde_json::json!({ "capabilities": capabilities }))
            .await
            .map_err(|e| network_err("advertise runtime", &e))?;
        expect_success("advertise runtime", resp).await?;
        debug!(runtime_id = %self.runtime_id, "mobile runtime capabilities advertised");
        Ok(expires_at)
    }

    pub async fn poll(
        &self,
        handler: &str,
        limit: u32,
        version: Option<&str>,
    ) -> Result<PollResponse, MobileError> {
        let capabilities = self.capabilities();
        let worker_id = self.worker_id();
        let body = PollBody {
            handler_name: handler,
            worker_id: &worker_id,
            limit: limit.max(1),
            version,
            capabilities: &capabilities,
        };
        let resp = self
            .post("workers/tasks/poll", &body)
            .await
            .map_err(|e| network_err("poll tasks", &e))?;
        let resp = expect_success("poll tasks", resp).await?;
        let bytes = read_capped(resp).await?;
        serde_json::from_slice(&bytes).map_err(|e| MobileError::Engine {
            message: format!("poll tasks: invalid response: {e}"),
        })
    }

    pub async fn heartbeat(&self, task_id: uuid::Uuid, claim_epoch: u64) -> LeaseResponse {
        self.lease_call(
            &format!("workers/tasks/{task_id}/heartbeat"),
            &serde_json::json!({ "worker_id": self.worker_id(), "claim_epoch": claim_epoch }),
            false,
        )
        .await
    }

    pub async fn complete(
        &self,
        task_id: uuid::Uuid,
        claim_epoch: u64,
        output: &serde_json::Value,
    ) -> LeaseResponse {
        self.lease_call(
            &format!("workers/tasks/{task_id}/complete"),
            &serde_json::json!({
                "worker_id": self.worker_id(),
                "claim_epoch": claim_epoch,
                "output": output,
            }),
            false,
        )
        .await
    }

    pub async fn fail(
        &self,
        task_id: uuid::Uuid,
        claim_epoch: u64,
        message: &str,
        retryable: bool,
    ) -> LeaseResponse {
        self.lease_call(
            &format!("workers/tasks/{task_id}/fail"),
            &serde_json::json!({
                "worker_id": self.worker_id(),
                "claim_epoch": claim_epoch,
                "message": message,
                "retryable": retryable,
            }),
            false,
        )
        .await
    }

    /// `POST /workers/tasks/{id}/release`. Returns [`LeaseResponse::Unsupported`]
    /// on 404/405 so the caller can fall back to a retryable `fail` against a
    /// server that predates the release endpoint.
    pub async fn release(
        &self,
        task_id: uuid::Uuid,
        claim_epoch: u64,
        started: bool,
    ) -> LeaseResponse {
        self.lease_call(
            &format!("workers/tasks/{task_id}/release"),
            &serde_json::json!({
                "worker_id": self.worker_id(),
                "claim_epoch": claim_epoch,
                "started": started,
            }),
            true,
        )
        .await
    }

    /// `POST` a JSON body to a control-plane path with the node credential.
    /// `Ok((status, body))` for any HTTP answer (the body is `Null` when it
    /// is not JSON); `Err` only when the control plane is unreachable.
    pub(crate) async fn post_json(
        &self,
        path: &str,
        body: &serde_json::Value,
    ) -> Result<(u16, serde_json::Value), MobileError> {
        let resp = self
            .post(path, body)
            .await
            .map_err(|e| network_err(path, &e))?;
        json_answer(resp).await
    }

    /// `GET` a control-plane path with the node credential (see
    /// [`Self::post_json`]).
    pub(crate) async fn get_json(
        &self,
        path: &str,
    ) -> Result<(u16, serde_json::Value), MobileError> {
        let url = self.url(path);
        let resp = self
            .credential
            .send(|token| {
                self.http
                    .get(&url)
                    .header("x-api-key", token)
                    .header("x-device-id", &self.device_id)
            })
            .await
            .map_err(|e| network_err(path, &e))?;
        json_answer(resp).await
    }

    async fn lease_call(
        &self,
        path: &str,
        body: &serde_json::Value,
        missing_route_is_unsupported: bool,
    ) -> LeaseResponse {
        match self.post(path, body).await {
            Ok(resp) => classify_lease_status(resp.status().as_u16(), missing_route_is_unsupported),
            Err(e) => {
                debug!(
                    path,
                    error = %orch8_engine::outbound::redact_error(&e),
                    "lease call failed"
                );
                LeaseResponse::Retry
            }
        }
    }
}

/// The control-plane base must be a public HTTPS URL. Test builds that
/// enable the `loopback-control-plane` feature (end-to-end suites running a
/// real server on `127.0.0.1`) may also use plain `http://` on a loopback
/// host; release builds never enable it.
fn validate_api_base(api_base: &str) -> Result<(), MobileError> {
    #[cfg(feature = "loopback-control-plane")]
    if reqwest::Url::parse(api_base).is_ok_and(|url| {
        url.scheme() == "http"
            && matches!(url.host_str(), Some("127.0.0.1" | "localhost" | "[::1]"))
    }) {
        return Ok(());
    }
    crate::validate_https_url(api_base)
}

pub(crate) fn classify_lease_status(
    status: u16,
    missing_route_is_unsupported: bool,
) -> LeaseResponse {
    match status {
        200..=299 => LeaseResponse::Accepted,
        404 | 405 if missing_route_is_unsupported => LeaseResponse::Unsupported,
        404 | 409 | 410 => LeaseResponse::LostOwnership,
        408 | 425 | 429 | 500..=599 => LeaseResponse::Retry,
        _ => LeaseResponse::Rejected,
    }
}

fn urlencode_path_segment(segment: &str) -> String {
    use std::fmt::Write as _;
    let mut out = String::with_capacity(segment.len());
    for byte in segment.bytes() {
        if byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.' | b'~') {
            out.push(char::from(byte));
        } else {
            let _ = write!(out, "%{byte:02X}");
        }
    }
    out
}

fn network_err(what: &str, e: &reqwest::Error) -> MobileError {
    MobileError::Engine {
        message: format!("{what}: {}", orch8_engine::outbound::redact_error(e)),
    }
}

async fn expect_success(
    what: &str,
    resp: reqwest::Response,
) -> Result<reqwest::Response, MobileError> {
    let status = resp.status();
    if status.is_success() {
        return Ok(resp);
    }
    let body = read_capped(resp).await.unwrap_or_default();
    let snippet = String::from_utf8_lossy(&body[..body.len().min(512)]).into_owned();
    Err(MobileError::Engine {
        message: format!("{what}: HTTP {status}: {snippet}"),
    })
}

async fn json_answer(resp: reqwest::Response) -> Result<(u16, serde_json::Value), MobileError> {
    let status = resp.status().as_u16();
    let body = read_capped(resp).await?;
    Ok((
        status,
        serde_json::from_slice(&body).unwrap_or(serde_json::Value::Null),
    ))
}

async fn read_capped(resp: reqwest::Response) -> Result<Vec<u8>, MobileError> {
    orch8_engine::handlers::builtin::read_body_capped(resp, MAX_RESPONSE_BYTES)
        .await
        .map_err(|e| match e {
            orch8_engine::handlers::builtin::BodyReadError::TooLarge(_) => MobileError::Engine {
                message: format!("response exceeds {MAX_RESPONSE_BYTES} bytes"),
            },
            orch8_engine::handlers::builtin::BodyReadError::Io(message) => {
                MobileError::Engine { message }
            }
        })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn client() -> Arc<NodeClient> {
        NodeClient::new_unchecked(
            "http://127.0.0.1:1/api/v1/".into(),
            Credential::new("key".into()),
            "dev-1".into(),
            RuntimeId::new(),
            Advertisement {
                handlers: vec!["scan".into()],
                caps: NodeCapabilities::default(),
                draining: false,
            },
        )
    }

    #[test]
    fn api_base_is_derived_from_sync_url() {
        assert_eq!(
            derive_api_base("https://api.orch8.io/api/v1/mobile/sync").as_deref(),
            Some("https://api.orch8.io/api/v1")
        );
        assert_eq!(
            derive_api_base("https://api.orch8.io/api/v1/mobile/sync/").as_deref(),
            Some("https://api.orch8.io/api/v1")
        );
        assert_eq!(derive_api_base("https://api.orch8.io/other"), None);
    }

    #[test]
    fn capabilities_are_mobile_fresh_and_within_server_ttl() {
        let c = client();
        let caps = c.capabilities();
        assert_eq!(caps.kind, RuntimeKind::Mobile);
        assert_eq!(caps.trust, RuntimeTrustLevel::Registered);
        assert_eq!(caps.handlers, vec!["scan".to_string()]);
        assert!(caps.hardware.contains(&"device:dev-1".to_string()));
        let ttl = caps.expires_at - caps.observed_at;
        assert!(ttl > chrono::Duration::zero());
        assert!(ttl <= chrono::Duration::minutes(5));
        assert!(READVERTISE_INTERVAL < CAPABILITY_TTL);
        // Serialized shape matches the server's RuntimeCapabilities.
        let json = serde_json::to_value(&caps).unwrap();
        assert_eq!(json["kind"], "mobile");
        assert_eq!(json["runtime_id"], c.worker_id());
    }

    #[test]
    fn lease_status_classification() {
        assert_eq!(classify_lease_status(200, false), LeaseResponse::Accepted);
        assert_eq!(
            classify_lease_status(409, false),
            LeaseResponse::LostOwnership
        );
        assert_eq!(
            classify_lease_status(404, false),
            LeaseResponse::LostOwnership
        );
        assert_eq!(classify_lease_status(404, true), LeaseResponse::Unsupported);
        assert_eq!(classify_lease_status(405, true), LeaseResponse::Unsupported);
        assert_eq!(classify_lease_status(503, false), LeaseResponse::Retry);
        assert_eq!(classify_lease_status(429, false), LeaseResponse::Retry);
        assert_eq!(classify_lease_status(400, false), LeaseResponse::Rejected);
    }

    #[test]
    fn remote_task_parses_with_and_without_contract_fields() {
        let old: RemoteTask = serde_json::from_value(serde_json::json!({
            "id": uuid::Uuid::new_v4(), "instance_id": uuid::Uuid::new_v4(),
            "block_id": "b", "handler_name": "scan", "params": {}, "context": {},
            "attempt": 1, "claim_epoch": 3, "state": "claimed",
            "created_at": "2026-01-01T00:00:00Z"
        }))
        .unwrap();
        assert!(old.effect_id.is_none() && old.lease_secs.is_none());
        let new: RemoteTask = serde_json::from_value(serde_json::json!({
            "id": uuid::Uuid::new_v4(), "instance_id": uuid::Uuid::new_v4(),
            "block_id": "b", "handler_name": "scan", "claim_epoch": 1,
            "effect_id": "eff-1", "continuity_epoch": 4, "lease_secs": 120,
            "target_runtime_id": "x", "runtime_kinds": ["mobile"]
        }))
        .unwrap();
        assert_eq!(new.effect_id.as_deref(), Some("eff-1"));
        assert_eq!(new.lease_secs, Some(120));
        assert_eq!(new.continuity_epoch, Some(4));
    }

    #[test]
    fn device_id_is_path_encoded() {
        assert_eq!(urlencode_path_segment("a b/c"), "a%20b%2Fc");
        assert_eq!(urlencode_path_segment("dev-1_x.y~z"), "dev-1_x.y~z");
    }

    #[tokio::test]
    async fn runtime_id_persists_across_calls() {
        let pool = SqlitePool::connect("sqlite::memory:").await.unwrap();
        init_tables(&pool).await.unwrap();
        let a = load_or_create_runtime_id(&pool).await.unwrap();
        let b = load_or_create_runtime_id(&pool).await.unwrap();
        assert_eq!(a, b);
    }
}
