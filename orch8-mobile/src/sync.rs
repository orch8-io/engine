//! Sync infrastructure: manifest fetch, signature verification, sequence download,
//! and reconciliation.

use std::collections::{HashMap, HashSet};
use std::sync::{Arc, OnceLock};
use std::time::Duration;

use base64::{Engine as _, engine::general_purpose::STANDARD as BASE64};
use chrono::{DateTime, Utc};
use ed25519_dalek::{Signature, Verifier, VerifyingKey};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use tokio_util::sync::CancellationToken;
use tracing::{info, warn};

#[path = "catalog_sync.rs"]
#[allow(dead_code)]
pub(crate) mod catalog;
#[path = "sync_run.rs"]
mod run;

use orch8_storage::StorageBackend;
use orch8_types::ids::Namespace;
use orch8_types::sequence::SequenceDefinition;

/// Strip credential-bearing URL components from sync retry diagnostics.
fn redacted_url(url: &str) -> String {
    orch8_engine::outbound::redact_url(url)
}

/// Lowercase hex-encode bytes without pulling in the `hex` crate.
fn to_hex(bytes: &[u8]) -> String {
    use std::fmt::Write as _;
    let mut s = String::with_capacity(bytes.len() * 2);
    for b in bytes {
        let _ = write!(s, "{b:02x}");
    }
    s
}

/// Parse a `Retry-After` header in its seconds form (the HTTP-date form is
/// not supported -- servers rate-limiting an API almost universally send
/// seconds, and a missing/unparseable header just falls back to our own
/// computed backoff).
fn retry_after(resp: &reqwest::Response) -> Option<Duration> {
    resp.headers()
        .get(reqwest::header::RETRY_AFTER)
        .and_then(|v| v.to_str().ok())
        .and_then(|s| s.trim().parse::<u64>().ok())
        .map(Duration::from_secs)
}

/// Grow a backoff delay by a genuinely random factor in `[1.5, 2.5)` (M-23).
/// The previous formula (`1.5 + attempt * 0.5`) was fully determined by the
/// attempt number alone, so every client that failed at the same moment
/// (e.g. a shared server outage) would retry in lockstep and re-hammer the
/// server the instant it recovers -- real jitter decorrelates them.
fn jittered_backoff(current: Duration) -> Duration {
    use rand::RngExt;
    let factor = rand::rng().random_range(1.5..2.5);
    current.mul_f64(factor)
}

use crate::error::{MobileError, SyncError};
use crate::storage::MobileStorage;

/// Maximum response body size for a manifest or a single sequence download.
/// Bounds memory so a malicious/misbehaving server can't OOM the device with an
/// unbounded body. Mirrors `MAX_SEQUENCES_RESPONSE_BYTES` in `lib.rs`.
const MAX_SYNC_RESPONSE_BYTES: u64 = 5 * 1024 * 1024;

/// Read a response body, rejecting it if it exceeds [`MAX_SYNC_RESPONSE_BYTES`].
/// Checks the advertised `Content-Length` up front (cheap rejection of honest
/// large bodies), then streams the body chunk-by-chunk and aborts as soon as
/// the running total exceeds the cap — buffering the full body first (e.g.
/// `Response::bytes()`) would let a server that omits or lies about
/// `Content-Length` defeat the cap and OOM the device. `what` names the
/// resource for error messages.
async fn read_body_capped(resp: reqwest::Response, what: &str) -> Result<Vec<u8>, MobileError> {
    if let Some(len) = resp.content_length()
        && len > MAX_SYNC_RESPONSE_BYTES
    {
        return Err(SyncError::Network {
            message: format!("{what} too large: {len} bytes exceeds limit"),
        }
        .into());
    }
    orch8_engine::handlers::builtin::read_body_capped(
        resp,
        usize::try_from(MAX_SYNC_RESPONSE_BYTES).unwrap_or(usize::MAX),
    )
    .await
    .map_err(|error| match error {
        orch8_engine::handlers::builtin::BodyReadError::TooLarge(_) => SyncError::Network {
            message: format!("{what} too large: exceeds {MAX_SYNC_RESPONSE_BYTES} byte limit"),
        }
        .into(),
        orch8_engine::handlers::builtin::BodyReadError::Io(message) => SyncError::Network {
            message: format!("failed to read {what} body: {message}"),
        }
        .into(),
    })
}

/// Root of trust: embedded Ed25519 public key for verifying manifest signatures.
#[derive(Clone)]
pub struct RootKey {
    pubkey: VerifyingKey,
}

impl RootKey {
    /// Load from a base64-encoded 32-byte Ed25519 public key.
    pub fn from_base64(b64: &str) -> Result<Self, SyncError> {
        let bytes = BASE64
            .decode(b64)
            .map_err(|e| SyncError::SignatureInvalid {
                message: format!("invalid base64 root key: {e}"),
            })?;
        let pubkey = VerifyingKey::from_bytes(&bytes.try_into().map_err(|_| {
            SyncError::SignatureInvalid {
                message: "invalid Ed25519 public key length".to_string(),
            }
        })?)
        .map_err(|e| SyncError::SignatureInvalid {
            message: format!("invalid Ed25519 public key: {e}"),
        })?;
        Ok(Self { pubkey })
    }

    pub fn verify(&self, message: &[u8], signature_bytes: &[u8]) -> Result<(), SyncError> {
        let sig_bytes: &[u8; 64] =
            signature_bytes
                .try_into()
                .map_err(|_| SyncError::SignatureInvalid {
                    message: "invalid signature length (expected 64 bytes)".to_string(),
                })?;
        let sig = Signature::from_bytes(sig_bytes);
        self.pubkey
            .verify(message, &sig)
            .map_err(|e| SyncError::SignatureInvalid {
                message: format!("signature verification failed: {e}"),
            })
    }
}

/// A signing key entry in the manifest.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SigningKeyEntry {
    pub key_id: String,
    pub public_key: String, // base64-encoded Ed25519 public key
}

/// A sequence entry in the manifest.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ManifestSequenceEntry {
    pub name: String,
    pub version: i32,
    pub url: String,
    pub signing_key_id: String,
    pub sha256: String,
    pub required_handlers: Vec<String>,
    pub min_sdk_version: String,
}

/// A removed sequence entry with TTL.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RemovedEntry {
    pub name: String,
    pub removed_at: DateTime<Utc>,
}

/// The signed manifest fetched from the CDN.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Manifest {
    pub signing_keys: Vec<SigningKeyEntry>,
    pub sequences: Vec<ManifestSequenceEntry>,
    pub removed: Vec<RemovedEntry>,
    #[allow(clippy::struct_field_names)]
    pub manifest_version: i64,
    pub generated_at: DateTime<Utc>,
}

/// Result of a sync operation.
#[derive(Debug, Clone, Default, uniffi::Record)]
pub struct SyncResult {
    pub added: u32,
    pub updated: u32,
    pub removed: u32,
    pub skipped: u32,
    pub signature_failures: u32,
}

/// Detached-signature URL for a sequence URL: swap a trailing `.json` path
/// extension for `.sig` (or append `.sig`). Only the path changes, so a
/// hostname or query string containing `.json` is left alone.
fn signature_url(sequence_url: &reqwest::Url) -> reqwest::Url {
    let mut sig = sequence_url.clone();
    let path = sequence_url.path();
    let sig_path = path
        .strip_suffix(".json")
        .map_or_else(|| format!("{path}.sig"), |stem| format!("{stem}.sig"));
    sig.set_path(&sig_path);
    sig
}

/// Decode a base64 Ed25519 detached signature.
fn decode_signature(sig_b64: &str) -> Result<Signature, String> {
    let bytes = BASE64
        .decode(sig_b64.trim())
        .map_err(|e| format!("signature decode failed: {e}"))?;
    let bytes: [u8; 64] = bytes
        .try_into()
        .map_err(|_| "signature wrong length".to_string())?;
    Ok(Signature::from_bytes(&bytes))
}

/// Decode a base64 Ed25519 verifying key.
fn decode_verifying_key(key_b64: &str) -> Result<VerifyingKey, String> {
    let bytes = BASE64
        .decode(key_b64)
        .map_err(|e| format!("key decode failed: {e}"))?;
    let bytes: [u8; 32] = bytes
        .try_into()
        .map_err(|_| "key invalid length".to_string())?;
    VerifyingKey::from_bytes(&bytes).map_err(|e| format!("key invalid: {e}"))
}

/// Auth configuration for sync requests.
#[cfg_attr(test, derive(Clone))]
pub enum SyncAuth {
    /// Token provided via callback interface.
    Bearer(String),
    /// Token embedded directly in the manifest URL (signed URL mode).
    UrlToken,
}

/// Bearer authority is bound to the manifest origin. Entry downloads keep
/// their own transport origin even when their content signature is trusted.
enum ScopedSyncAuth<'a> {
    Bearer {
        token: &'a str,
        manifest_url: reqwest::Url,
    },
    UrlToken,
}

impl SyncAuth {
    fn for_manifest(&self, manifest_url: &str) -> Result<ScopedSyncAuth<'_>, MobileError> {
        match self {
            Self::Bearer(token) => {
                if token.trim().is_empty() {
                    return Err(MobileError::InvalidInput {
                        detail: "manifest token provider returned an empty token".into(),
                    });
                }
                let manifest_url =
                    reqwest::Url::parse(manifest_url).map_err(|_| MobileError::InvalidInput {
                        detail: "invalid manifest URL".into(),
                    })?;
                Ok(ScopedSyncAuth::Bearer {
                    token,
                    manifest_url,
                })
            }
            Self::UrlToken => Ok(ScopedSyncAuth::UrlToken),
        }
    }
}

impl ScopedSyncAuth<'_> {
    fn bearer_for(&self, target: &reqwest::Url) -> Option<&str> {
        match self {
            Self::Bearer {
                token,
                manifest_url,
            } if manifest_url.origin() == target.origin() => Some(token),
            _ => None,
        }
    }
}

/// Metadata key holding the SDK-version + handler-set fingerprint the cached
/// manifest cursor was computed under (MC-01).
const ENV_FINGERPRINT_KEY: &str = "sync_env_fingerprint";

/// Orchestrates manifest fetch, verification, diff, and sequence download.
#[derive(Clone)]
pub struct SyncOrchestrator {
    mobile_storage: Arc<MobileStorage>,
    backend: Arc<dyn StorageBackend>,
    root_key: RootKey,
    http: Arc<OnceLock<reqwest::Client>>,
    admission: Arc<tokio::sync::Semaphore>,
    // Set only on a per-operation snapshot; shared state and admission remain
    // shared across snapshots, while cancellation belongs to one run.
    run_cancelled: Option<CancellationToken>,
    run_deadline: Option<tokio::time::Instant>,
    sdk_version: String,
    max_stored_sequences: u32,
    /// Test-only escape hatch: loopback `http://` mock servers. Production
    /// always requires public `https://` sequence/signature URLs.
    allow_insecure_urls: bool,
}

impl SyncOrchestrator {
    pub fn new(
        mobile_storage: Arc<MobileStorage>,
        backend: Arc<dyn StorageBackend>,
        root_key: RootKey,
        sdk_version: String,
        max_stored_sequences: u32,
    ) -> Self {
        Self {
            mobile_storage,
            backend,
            root_key,
            http: Arc::new(OnceLock::new()),
            admission: Arc::new(tokio::sync::Semaphore::new(1)),
            run_cancelled: None,
            run_deadline: None,
            sdk_version,
            max_stored_sequences,
            allow_insecure_urls: false,
        }
    }

    /// Allow `http://` loopback sequence URLs (unit tests with mock servers).
    #[cfg(test)]
    pub(crate) fn with_insecure_urls_for_tests(mut self) -> Self {
        self.allow_insecure_urls = true;
        self
    }

    /// Resolve a manifest entry URL against the manifest URL. The publisher
    /// emits relative URLs (`sequences/foo/1.json`); absolute URLs pass
    /// through `Url::join` unchanged. The result must be a public HTTPS URL.
    fn resolve_entry_url(
        &self,
        manifest_url: &str,
        entry_url: &str,
    ) -> Result<reqwest::Url, MobileError> {
        let base = reqwest::Url::parse(manifest_url).map_err(|e| MobileError::InvalidInput {
            detail: format!("invalid manifest URL: {e}"),
        })?;
        let url = base
            .join(entry_url)
            .map_err(|e| MobileError::InvalidInput {
                detail: format!("invalid sequence URL {entry_url:?}: {e}"),
            })?;
        if !self.allow_insecure_urls {
            crate::validate_https_url(url.as_str())?;
        }
        Ok(url)
    }

    #[cfg(test)]
    pub(crate) fn set_http_client_for_test(&self, client: reqwest::Client) {
        assert!(
            self.http.set(client).is_ok(),
            "install the test client before the first sync"
        );
    }

    fn http_client(&self) -> &reqwest::Client {
        self.http
            .get_or_init(|| crate::build_mobile_http_client(Duration::from_secs(30)))
    }

    /// HTTP GET with exponential backoff and jitter for retryable errors
    /// (server errors, rate limiting, and timeouts). Honors a `Retry-After`
    /// response header (seconds form) when the server sends one, rather than
    /// always falling back to our own guessed backoff (M-23).
    async fn http_get_with_retry(
        &self,
        url: &str,
        auth: &ScopedSyncAuth<'_>,
        if_none_match: Option<&str>,
    ) -> Result<reqwest::Response, MobileError> {
        let target = reqwest::Url::parse(url).map_err(|_| MobileError::InvalidInput {
            detail: "invalid sync request URL".into(),
        })?;
        let mut delay = Duration::from_millis(200);
        let max_retries = 3u32;
        let mut last_err = None;

        for attempt in 0..=max_retries {
            let mut req = self.http_client().get(target.clone());
            if let Some(token) = auth.bearer_for(&target) {
                req = req.header("authorization", format!("Bearer {token}"));
            }
            if let Some(etag) = if_none_match {
                req = req.header(reqwest::header::IF_NONE_MATCH, etag);
            }

            match self.io(req.send()).await? {
                Ok(resp)
                    if (resp.status().is_server_error()
                        || resp.status() == reqwest::StatusCode::TOO_MANY_REQUESTS)
                        && attempt < max_retries =>
                {
                    let status = resp.status();
                    let display_url = redacted_url(url);
                    let wait = retry_after(&resp).unwrap_or(delay);
                    warn!(attempt, %status, %display_url, wait_ms = wait.as_millis(), "retryable HTTP error");
                    self.io(tokio::time::sleep(wait)).await?;
                    // Real random jitter (M-23): the previous "1.5 +
                    // attempt*0.5" factor was fully determined by the
                    // attempt number, so every client that failed at the
                    // same moment (e.g. a server outage) would retry in
                    // perfect lockstep and re-hammer the server the instant
                    // it recovers.
                    delay = jittered_backoff(delay);
                    last_err = Some(format!("HTTP {status}"));
                }
                Ok(resp) => return Ok(resp),
                Err(e) if attempt < max_retries && e.is_timeout() => {
                    let display_url = redacted_url(url);
                    warn!(attempt, %display_url, "request timeout, retrying");
                    self.io(tokio::time::sleep(delay)).await?;
                    delay = jittered_backoff(delay);
                    // `without_url`: reqwest errors embed the full request URL
                    // (including the query string), which carries the auth
                    // token in `SyncAuth::UrlToken` signed-URL mode — never
                    // propagate that into errors returned to the host app.
                    last_err = Some(e.without_url().to_string());
                }
                Err(e) => {
                    return Err(SyncError::Network {
                        message: format!("request failed: {}", e.without_url()),
                    }
                    .into());
                }
            }
        }

        Err(SyncError::Network {
            message: format!(
                "request failed after {max_retries} retries: {}",
                last_err.unwrap_or_default()
            ),
        }
        .into())
    }

    /// Fingerprint of everything that decides whether a manifest entry is
    /// installable on this device: SDK version + registered handler names.
    fn env_fingerprint(&self, registered_handlers: &HashSet<String>) -> String {
        let mut names: Vec<&str> = registered_handlers.iter().map(String::as_str).collect();
        names.sort_unstable();
        let mut hasher = Sha256::new();
        hasher.update(self.sdk_version.as_bytes());
        for n in names {
            hasher.update([0u8]);
            hasher.update(n.as_bytes());
        }
        to_hex(&hasher.finalize())
    }

    /// Perform a full sync against the given manifest URL.
    #[cfg(test)]
    pub async fn sync(
        &self,
        manifest_url: &str,
        auth: &SyncAuth,
        registered_handlers: &HashSet<String>,
    ) -> Result<SyncResult, MobileError> {
        let auth = auth.clone();
        self.sync_with_auth(
            manifest_url,
            registered_handlers,
            CancellationToken::new(),
            move |_| std::future::ready(Ok(auth)),
        )
        .await
    }

    async fn sync_inner(
        &self,
        manifest_url: &str,
        auth: &SyncAuth,
        registered_handlers: &HashSet<String>,
    ) -> Result<SyncResult, MobileError> {
        self.check_active()?;
        let mut result = SyncResult::default();
        let scoped_auth = auth.for_manifest(manifest_url)?;

        // MC-01: the cached ETag / last_manifest_version are only valid for the
        // environment (SDK version + registered handler set) that evaluated
        // the manifest. After an app upgrade the same manifest can install
        // sequences that were previously skipped, so a changed fingerprint
        // bypasses both the 304 shortcut and the replay guard's equality case.
        let fingerprint = self.env_fingerprint(registered_handlers);
        let stored_fingerprint = self
            .mobile_storage
            .get_sync_metadata(ENV_FINGERPRINT_KEY)
            .await
            .map_err(|e| MobileError::Storage {
                detail: e.to_string(),
            })?;
        let env_changed = stored_fingerprint.as_deref() != Some(fingerprint.as_str());

        let trust = self.mobile_storage.manifest_trust().await?;

        // A previously evaluated ETag cannot stand in for an accepted document
        // whose effects have not finished. Fetch the complete document on retry.
        // 1. Fetch manifest with ETag support.
        let (manifest_bytes, maybe_etag) = self
            .fetch_manifest(
                manifest_url,
                &scoped_auth,
                !env_changed && !trust.pending() && trust.applied_version.is_some(),
            )
            .await?;

        if manifest_bytes.is_empty() {
            return Ok(result);
        }

        // 2. Verify manifest signature.
        let manifest: Manifest = self.verify_and_parse_manifest(&manifest_bytes)?;

        let body = manifest_bytes
            .splitn(2, |byte| *byte == b'\n')
            .nth(1)
            .ok_or_else(|| SyncError::InvalidManifest {
                message: "manifest missing body".into(),
            })?;
        let digest = to_hex(&Sha256::digest(body));

        // Acceptance precedes workflow effects, independently of whether every
        // download succeeds. An older signed document cannot undo a removal or
        // key rotation just because the applied/evaluation cursor trails it.
        if trust
            .floor()
            .is_some_and(|floor| manifest.manifest_version < floor)
        {
            warn!(
                version = manifest.manifest_version,
                "rejecting manifest below durable acceptance fence"
            );
            return Ok(result);
        }
        trust.check_candidate(manifest.manifest_version, &digest)?;
        if trust.applied_version == Some(manifest.manifest_version) && !env_changed {
            return Ok(result);
        }

        // 3. Atomically accept the verified document and its validated key set.
        let trusted_keys = self.cache_signing_keys(&manifest, &digest).await?;

        // 4. Reconcile removed sequences.
        let local_sequences = self.list_local_sequences().await?;
        result.removed = self.reconcile_removed(&manifest, &local_sequences).await?;

        // 5. Download and verify each sequence.
        let (added, updated, skipped, sig_failures, retryable_skips) = self
            .download_and_verify_sequences(
                manifest_url,
                &manifest,
                &scoped_auth,
                registered_handlers,
                &local_sequences,
                &trusted_keys,
            )
            .await?;
        result.added = added;
        result.updated = updated;
        result.skipped = skipped;
        result.signature_failures = sig_failures;

        // 6. Evict oldest sequences if over limit.
        self.evict_excess_sequences().await?;

        // 7. Persist sync metadata (including the applied manifest version so a
        // later replay of an older manifest is rejected in step 2a). MC-01:
        // when any entry was skipped for a transient reason (download
        // failure) the cursor is NOT advanced, so the next sync re-fetches the
        // manifest instead of getting a 304 and never installing the entry.
        self.check_active()?;
        if retryable_skips == 0 {
            self.persist_accepted_sync_metadata(
                maybe_etag,
                manifest.manifest_version,
                &fingerprint,
                &digest,
            )
            .await?;
        } else {
            warn!(
                retryable_skips,
                "sync had transient skips; manifest cursor not advanced"
            );
            self.mobile_storage
                .commit_manifest_evaluation(
                    &[("last_sync_ts", Some(&Utc::now().to_rfc3339()))],
                    manifest.manifest_version,
                    &digest,
                )
                .await?;
        }

        self.check_active()?;
        info!(
            added = result.added,
            updated = result.updated,
            removed = result.removed,
            skipped = result.skipped,
            "sync completed"
        );
        Ok(result)
    }

    /// Validate signing keys from the manifest against the root key and cache them
    /// in local storage.
    async fn cache_signing_keys(
        &self,
        manifest: &Manifest,
        digest: &str,
    ) -> Result<HashMap<String, VerifyingKey>, MobileError> {
        self.check_active()?;
        let mut trusted_keys = HashMap::new();
        for entry in &manifest.signing_keys {
            self.check_active()?;
            if entry.key_id.trim().is_empty() || trusted_keys.contains_key(&entry.key_id) {
                return Err(SyncError::InvalidManifest {
                    message: "manifest signing key IDs must be nonempty and unique".into(),
                }
                .into());
            }
            let key = decode_verifying_key(&entry.public_key)
                .map_err(|message| SyncError::SignatureInvalid { message })?;
            trusted_keys.insert(entry.key_id.clone(), key);
        }
        // Parse the entire root-authorized set before changing trust. Revocation
        // is mandatory: on any storage failure, keep the previous complete set
        // and abort before reconciling/installing workflows or advancing cursors.
        let keys: Vec<_> = manifest
            .signing_keys
            .iter()
            .map(|entry| (entry.key_id.as_str(), entry.public_key.as_str()))
            .collect();
        self.check_active()?;
        self.mobile_storage
            .accept_manifest_trust(&keys, manifest.manifest_version, digest)
            .await
            .map_err(|error| MobileError::Storage {
                detail: error.to_string(),
            })?;

        self.check_active()?;
        Ok(trusted_keys)
    }

    #[cfg(test)]
    async fn cache_test_signing_keys(
        &self,
        manifest: &Manifest,
    ) -> Result<HashMap<String, VerifyingKey>, MobileError> {
        let digest = to_hex(&Sha256::digest(serde_json::to_vec(manifest).unwrap()));
        self.cache_signing_keys(manifest, &digest).await
    }

    /// Reconcile removed sequences: process explicit removals from the manifest
    /// and perform full reconciliation if the last sync was more than 30 days ago.
    async fn reconcile_removed(
        &self,
        manifest: &Manifest,
        local_sequences: &HashMap<String, i32>,
    ) -> Result<u32, MobileError> {
        self.check_active()?;
        let mut removed_count = 0u32;

        // Reconcile removed entries.
        let cutoff = Utc::now() - chrono::Duration::days(30);
        for removed in &manifest.removed {
            self.check_active()?;
            if removed.removed_at < cutoff {
                continue; // ignore old removals
            }
            if local_sequences.contains_key(&removed.name) {
                if let Err(e) = self.remove_local_sequence(&removed.name).await {
                    warn!(name = %removed.name, error = %e, "failed to remove local sequence");
                } else {
                    removed_count += 1;
                }
            }
        }

        // Full reconciliation: if last sync > 30 days ago, treat any local sequence
        // not in manifest as removed.
        self.check_active()?;
        let last_sync = self.get_last_sync_ts().await?;
        let thirty_days_ago = Utc::now() - chrono::Duration::days(30);
        if last_sync.is_none_or(|t| t < thirty_days_ago) {
            let manifest_names: HashSet<&str> = manifest
                .sequences
                .iter()
                .map(|sequence| sequence.name.as_str())
                .collect();
            for name in local_sequences.keys() {
                self.check_active()?;
                if !manifest_names.contains(name.as_str()) {
                    if let Err(e) = self.remove_local_sequence(name).await {
                        warn!(name = %name, error = %e, "failed to remove stale local sequence");
                    } else {
                        removed_count += 1;
                    }
                }
            }
        }

        Ok(removed_count)
    }

    /// Download, hash-verify, and signature-verify each sequence in the manifest,
    /// then upsert new or updated sequences into local storage.
    /// Returns `(added, updated, skipped, signature_failures, retryable_skips)`
    /// where `retryable_skips` counts entries skipped for a transient reason
    /// (download failure) that must not advance the manifest cursor.
    #[allow(clippy::too_many_lines)]
    async fn download_and_verify_sequences(
        &self,
        manifest_url: &str,
        manifest: &Manifest,
        auth: &ScopedSyncAuth<'_>,
        registered_handlers: &HashSet<String>,
        local_sequences: &HashMap<String, i32>,
        trusted_keys: &HashMap<String, VerifyingKey>,
    ) -> Result<(u32, u32, u32, u32, u32), MobileError> {
        self.check_active()?;
        let mut retryable = 0u32;
        let mut added = 0u32;
        let mut updated = 0u32;
        let mut skipped = 0u32;
        let mut sig_failures = 0u32;

        for entry in &manifest.sequences {
            self.check_active()?;
            if !self.version_meets_min(&self.sdk_version, &entry.min_sdk_version) {
                warn!(
                    name = %entry.name,
                    min_sdk = %entry.min_sdk_version,
                    sdk = %self.sdk_version,
                    "sequence requires newer SDK — skipping"
                );
                skipped += 1;
                continue;
            }

            let missing_handlers: Vec<&String> = entry
                .required_handlers
                .iter()
                .filter(|h| !registered_handlers.contains(h.as_str()))
                .collect();
            if !missing_handlers.is_empty() {
                warn!(
                    name = %entry.name,
                    missing = ?missing_handlers,
                    "sequence requires unregistered handlers — skipping"
                );
                skipped += 1;
                continue;
            }

            // H-15: skip before downloading anything when the local copy is
            // already current. The manifest already tells us the version, so
            // downloading the sequence body + detached signature only to
            // discover we're up to date wastes a device's bandwidth/battery
            // on every sync for every sequence that hasn't changed — which,
            // in steady state, is nearly all of them.
            let existing_version = local_sequences.get(&entry.name);
            if let Some(existing_version) = existing_version
                && *existing_version >= entry.version
            {
                skipped += 1;
                continue;
            }

            // Admit the entry before making any body/signature request.
            // Only the current root-signed manifest authorizes signing keys.
            // A historical cache entry must never resurrect a rotated-out key.
            let Some(signing_pk) = trusted_keys.get(&entry.signing_key_id) else {
                warn!(name = %entry.name, key_id = %entry.signing_key_id, "no trusted signing key — skipping");
                sig_failures += 1;
                continue;
            };

            let seq_url = match self.resolve_entry_url(manifest_url, &entry.url) {
                Ok(url) => url,
                Err(e) => {
                    warn!(name = %entry.name, error = %e, "unusable sequence URL — skipping");
                    skipped += 1;
                    continue;
                }
            };
            let seq_json = match self.download_sequence(seq_url.as_str(), auth).await {
                Ok(json) => json,
                Err(e) => {
                    warn!(name = %entry.name, error = %e, "failed to download sequence");
                    skipped += 1;
                    retryable += 1;
                    continue;
                }
            };

            self.check_active()?;
            let computed_hash = to_hex(&Sha256::digest(&seq_json));
            if computed_hash != entry.sha256 {
                warn!(
                    name = %entry.name,
                    expected = %entry.sha256,
                    got = %computed_hash,
                    "sequence hash mismatch — skipping"
                );
                sig_failures += 1;
                continue;
            }

            // MOB-N3: every per-entry signature failure (download, decode,
            // length, verify) counts and skips just this entry — a single
            // malformed `.sig` must not abort the sync for every sequence.
            let sig_url = signature_url(&seq_url);
            let verified = match self.download_sequence(sig_url.as_str(), auth).await {
                Ok(sig_b64) => decode_signature(&sig_b64).and_then(|sig| {
                    signing_pk
                        .verify(seq_json.as_bytes(), &sig)
                        .map_err(|e| format!("verification failed: {e}"))
                }),
                Err(e) => Err(format!("download failed: {e}")),
            };
            if let Err(message) = verified {
                warn!(name = %entry.name, %message, "sequence signature invalid — skipping");
                if message.starts_with("download failed") {
                    retryable += 1;
                }
                sig_failures += 1;
                continue;
            }

            // H-14: a single poison manifest entry (passes hash + signature
            // but fails to deserialize) must not abort the whole sync — skip
            // just this entry so every other sequence in the manifest still
            // syncs, and so the caller's ETag/version persistence (which only
            // runs on a successful return) isn't blocked forever by one bad
            // entry the server will keep serving on every future sync.
            let seq: SequenceDefinition = match serde_json::from_str(&seq_json) {
                Ok(seq) => seq,
                Err(e) => {
                    warn!(name = %entry.name, error = %e, "sequence JSON invalid — skipping");
                    sig_failures += 1;
                    continue;
                }
            };

            // Persistence serializes the owned definition into SQLite fields.
            // Release the capped raw response first so its allocation (up to
            // 5 MiB) does not overlap those temporary serialization buffers.
            drop(seq_json);
            self.upsert_sequence(&seq).await?;
            if existing_version.is_some() {
                updated += 1;
            } else {
                added += 1;
            }
        }

        Ok((added, updated, skipped, sig_failures, retryable))
    }

    async fn persist_accepted_sync_metadata(
        &self,
        etag: Option<String>,
        version: i64,
        fingerprint: &str,
        digest: &str,
    ) -> Result<(), MobileError> {
        self.persist_sync_metadata_inner(
            etag,
            Some(version),
            Some(fingerprint),
            Some((version, digest)),
        )
        .await
    }

    /// Prepare legacy metadata snapshots for transaction/upgrade tests.
    #[cfg(test)]
    async fn persist_sync_metadata(
        &self,
        etag: Option<String>,
        manifest_version: Option<i64>,
        fingerprint: Option<&str>,
    ) -> Result<(), MobileError> {
        self.persist_sync_metadata_inner(etag, manifest_version, fingerprint, None)
            .await
    }

    /// Persist sync timestamp, optional `ETag`, and optional applied manifest
    /// version after a successful sync.
    async fn persist_sync_metadata_inner(
        &self,
        etag: Option<String>,
        manifest_version: Option<i64>,
        fingerprint: Option<&str>,
        acceptance: Option<(i64, &str)>,
    ) -> Result<(), MobileError> {
        self.check_active()?;
        let timestamp = Utc::now().to_rfc3339();
        let version = manifest_version.map(|value| value.to_string());
        let mut changes = vec![
            ("last_sync_ts", Some(timestamp.as_str())),
            ("manifest_etag", etag.as_deref()),
        ];
        if let Some(version) = version.as_deref() {
            changes.push(("last_manifest_version", Some(version)));
        }
        if let Some(fingerprint) = fingerprint {
            changes.push((ENV_FINGERPRINT_KEY, Some(fingerprint)));
        }
        match acceptance {
            Some((version, digest)) => {
                self.mobile_storage
                    .commit_manifest_evaluation(&changes, version, digest)
                    .await
            }
            None => {
                self.mobile_storage
                    .update_sync_metadata_batch(&changes)
                    .await
            }
        }
        .map_err(MobileError::from)
    }

    /// Last successfully-applied manifest version, used to reject replays of
    /// older signed manifests. Returns `None` before the first sync.
    #[cfg(test)]
    async fn get_last_manifest_version(&self) -> Result<Option<i64>, MobileError> {
        let raw = self
            .mobile_storage
            .get_sync_metadata("last_manifest_version")
            .await
            .map_err(|e| MobileError::Storage {
                detail: e.to_string(),
            })?;
        match raw {
            Some(s) => s
                .parse::<i64>()
                .map(Some)
                .map_err(|e| MobileError::Storage {
                    detail: format!("bad last_manifest_version: {e}"),
                }),
            None => Ok(None),
        }
    }

    async fn fetch_manifest(
        &self,
        url: &str,
        auth: &ScopedSyncAuth<'_>,
        use_cached_etag: bool,
    ) -> Result<(Vec<u8>, Option<String>), MobileError> {
        let cached_etag = if use_cached_etag {
            self.mobile_storage
                .get_sync_metadata("manifest_etag")
                .await
                .map_err(|e| MobileError::Storage {
                    detail: e.to_string(),
                })?
        } else {
            None
        };

        let response = self
            .http_get_with_retry(url, auth, cached_etag.as_deref())
            .await?;

        if response.status() == reqwest::StatusCode::NOT_MODIFIED {
            if cached_etag.is_none() {
                return Err(SyncError::Network {
                    message: "manifest returned 304 without an eligible validator".into(),
                }
                .into());
            }
            return Ok((Vec::new(), None));
        }

        if !response.status().is_success() {
            return Err(SyncError::Network {
                message: format!("manifest fetch returned {}", response.status()),
            }
            .into());
        }

        let etag = response
            .headers()
            .get("etag")
            .and_then(|v| v.to_str().ok().map(String::from));
        let bytes = self.io(read_body_capped(response, "manifest")).await??;
        Ok((bytes, etag))
    }

    fn verify_and_parse_manifest(&self, bytes: &[u8]) -> Result<Manifest, MobileError> {
        let divider = bytes.iter().position(|&b| b == b'\n');
        let (body, sig_b64) = match divider {
            Some(idx) => {
                let sig_part =
                    std::str::from_utf8(&bytes[..idx]).map_err(|_| SyncError::InvalidManifest {
                        message: "manifest signature is not valid utf-8".to_string(),
                    })?;
                (&bytes[idx + 1..], sig_part)
            }
            None => {
                return Err(SyncError::InvalidManifest {
                    message: "manifest missing signature divider".to_string(),
                }
                .into());
            }
        };

        let sig_bytes = BASE64
            .decode(sig_b64.trim())
            .map_err(|e| SyncError::SignatureInvalid {
                message: format!("manifest signature base64 decode failed: {e}"),
            })?;

        self.root_key.verify(body, &sig_bytes)?;

        let manifest: Manifest =
            serde_json::from_slice(body).map_err(|e| SyncError::InvalidManifest {
                message: format!("manifest JSON parse failed: {e}"),
            })?;

        Ok(manifest)
    }

    async fn download_sequence(
        &self,
        url: &str,
        auth: &ScopedSyncAuth<'_>,
    ) -> Result<String, MobileError> {
        let response = self.http_get_with_retry(url, auth, None).await?;

        if !response.status().is_success() {
            return Err(SyncError::Network {
                message: format!("sequence download returned {}", response.status()),
            }
            .into());
        }

        let bytes = self.io(read_body_capped(response, "sequence")).await??;
        let text = String::from_utf8(bytes).map_err(|e| SyncError::Network {
            message: format!("sequence body is not valid utf-8: {e}"),
        })?;
        Ok(text)
    }

    async fn list_local_sequences(&self) -> Result<HashMap<String, i32>, MobileError> {
        let tenant = crate::mobile_tenant_id();
        let ns = Namespace::new("default");
        self.mobile_storage
            .list_local_sequence_versions(&tenant, &ns, 1000)
            .await
            .map_err(|e| MobileError::Storage {
                detail: e.to_string(),
            })
            .map(|versions| versions.into_iter().collect())
    }

    async fn remove_local_sequence(&self, name: &str) -> Result<(), MobileError> {
        self.check_active()?;
        let tenant = crate::mobile_tenant_id();
        let ns = Namespace::new("default");
        let seq = self
            .backend
            .get_sequence_by_name(&tenant, &ns, name, None)
            .await
            .map_err(|e| MobileError::Storage {
                detail: e.to_string(),
            })?;
        if seq.is_none() {
            return Ok(());
        }
        self.check_active()?;
        // Remove every stored version. A version that still has in-flight
        // instances is kept (storage refuses the delete, L3-04) but
        // deprecated so no new run starts on it; a later sync retries.
        let versions = self
            .backend
            .list_sequence_versions(&tenant, &ns, name)
            .await
            .map_err(|e| MobileError::Storage {
                detail: e.to_string(),
            })?;
        for version in versions {
            self.check_active()?;
            match self.backend.delete_sequence(version.id).await {
                Ok(()) => {}
                Err(orch8_types::error::StorageError::Conflict(message)) => {
                    warn!(%name, version = version.version, %message, "keeping removed sequence version with in-flight instances");
                    self.check_active()?;
                    self.backend
                        .deprecate_sequence(version.id)
                        .await
                        .map_err(|e| MobileError::Storage {
                            detail: e.to_string(),
                        })?;
                }
                Err(e) => {
                    return Err(MobileError::Storage {
                        detail: e.to_string(),
                    });
                }
            }
        }
        Ok(())
    }

    /// True when any non-terminal instance still runs sequence `id`.
    async fn has_live_instances(
        &self,
        id: orch8_types::ids::SequenceId,
    ) -> Result<bool, MobileError> {
        use orch8_types::instance::InstanceState;
        let filter = orch8_types::filter::InstanceFilter {
            sequence_id: Some(id),
            states: Some(vec![
                InstanceState::Scheduled,
                InstanceState::Running,
                InstanceState::Paused,
                InstanceState::Waiting,
            ]),
            ..Default::default()
        };
        self.backend
            .count_instances(&filter)
            .await
            .map(|n| n > 0)
            .map_err(|e| MobileError::Storage {
                detail: e.to_string(),
            })
    }

    /// Delete superseded versions of `name` (all but `keep`) once their
    /// in-flight instances have finished. Versions still in use are skipped
    /// (storage refuses the delete) and retried on the next upgrade.
    async fn prune_superseded_versions(&self, name: &str, keep: orch8_types::ids::SequenceId) {
        if self.check_active().is_err() {
            return;
        }
        let tenant = crate::mobile_tenant_id();
        let ns = Namespace::new("default");
        let versions = match self
            .backend
            .list_sequence_versions(&tenant, &ns, name)
            .await
        {
            Ok(versions) => versions,
            Err(e) => {
                warn!(%name, error = %e, "failed to list sequence versions for pruning");
                return;
            }
        };
        for version in versions.into_iter().filter(|v| v.id != keep) {
            if self.check_active().is_err() {
                return;
            }
            match self.backend.delete_sequence(version.id).await {
                Ok(()) => {
                    info!(%name, version = version.version, "pruned superseded sequence version");
                }
                Err(orch8_types::error::StorageError::Conflict(_)) => {}
                Err(e) => {
                    warn!(%name, version = version.version, error = %e, "failed to prune superseded sequence version");
                }
            }
        }
    }

    async fn upsert_sequence(&self, seq: &SequenceDefinition) -> Result<(), MobileError> {
        self.check_active()?;
        let tenant = crate::mobile_tenant_id();
        let ns = Namespace::new("default");
        match self
            .backend
            .get_sequence_by_name(&tenant, &ns, &seq.name, None)
            .await
        {
            Ok(Some(existing)) => {
                self.check_active()?;
                // L3-04: never replace a version that in-flight instances
                // still run — that cascade-deleted them. Install the new
                // version alongside (new instances resolve the latest) and
                // retire the old one once its instances finish.
                let live = self.has_live_instances(existing.id).await?;
                self.check_active()?;
                let keep = if live {
                    let mut new = seq.clone();
                    if new.id == existing.id {
                        new.id = orch8_types::ids::SequenceId::new();
                    }
                    self.backend
                        .create_sequence(&new)
                        .await
                        .map_err(|e| MobileError::Storage {
                            detail: format!("failed to add sequence '{}': {e}", seq.name),
                        })?;
                    new.id
                } else {
                    // M-18: delete-then-create as two independent calls could
                    // leave the old row deleted with the new one never created
                    // if create_sequence failed in between. `replace_sequence`
                    // runs both in a single transaction (all-or-nothing).
                    self.backend
                        .replace_sequence(existing.id, seq)
                        .await
                        .map_err(|e| MobileError::Storage {
                            detail: format!("failed to replace sequence '{}': {e}", seq.name),
                        })?;
                    seq.id
                };
                self.prune_superseded_versions(&seq.name, keep).await;
            }
            Ok(None) => {
                self.check_active()?;
                self.backend
                    .create_sequence(seq)
                    .await
                    .map_err(|e| MobileError::Storage {
                        detail: e.to_string(),
                    })?;
            }
            Err(e) => {
                return Err(MobileError::Storage {
                    detail: e.to_string(),
                });
            }
        }
        Ok(())
    }

    async fn evict_excess_sequences(&self) -> Result<(), MobileError> {
        self.check_active()?;
        if self.max_stored_sequences == 0 {
            return Ok(());
        }
        let tenant = crate::mobile_tenant_id();
        let ns = Namespace::new("default");
        let candidates = self
            .mobile_storage
            .list_excess_oldest_local_sequences(&tenant, &ns, self.max_stored_sequences)
            .await
            .map_err(|e| MobileError::Storage {
                detail: e.to_string(),
            })?;
        for (sequence_id, name) in candidates {
            self.check_active()?;
            if let Err(e) = self.backend.delete_sequence(sequence_id).await {
                warn!(%name, error = %e, "failed to evict excess sequence");
            } else {
                info!(%name, "evicted oldest sequence (over limit)");
            }
        }
        Ok(())
    }

    async fn get_last_sync_ts(&self) -> Result<Option<DateTime<Utc>>, MobileError> {
        let raw = self
            .mobile_storage
            .get_sync_metadata("last_sync_ts")
            .await
            .map_err(|e| MobileError::Storage {
                detail: e.to_string(),
            })?;
        match raw {
            Some(s) => s.parse().map(Some).map_err(|e| MobileError::InvalidInput {
                detail: format!("bad last_sync_ts: {e}"),
            }),
            None => Ok(None),
        }
    }

    #[allow(clippy::unused_self)]
    fn version_meets_min(&self, sdk: &str, min: &str) -> bool {
        sdk_version_meets_min(sdk, min)
    }
}

/// Numeric dotted-version comparison: missing components count as zero and
/// any unparseable component fails closed.
pub(crate) fn sdk_version_meets_min(sdk: &str, min: &str) -> bool {
    // Fail closed: a version string with an unparseable component must not
    // silently compare as if the component were absent. Keep consuming
    // both iterators after the first difference so a late invalid component
    // still rejects the entire version without allocating component vectors.
    let mut sdk_parts = sdk.split('.');
    let mut min_parts = min.split('.');
    let mut ordering = std::cmp::Ordering::Equal;
    loop {
        let (sdk_part, min_part) = (sdk_parts.next(), min_parts.next());
        if sdk_part.is_none() && min_part.is_none() {
            break;
        }
        let Some(sdk_part) = sdk_part.map_or(Some(0), |part| part.parse::<u32>().ok()) else {
            return false;
        };
        let Some(min_part) = min_part.map_or(Some(0), |part| part.parse::<u32>().ok()) else {
            return false;
        };
        if ordering == std::cmp::Ordering::Equal {
            ordering = sdk_part.cmp(&min_part);
        }
    }
    ordering != std::cmp::Ordering::Less
}

#[cfg(test)]
mod tests {
    use super::*;
    use orch8_storage::sqlite::SqliteStorage;
    use orch8_types::sequence::SequenceStatus;

    /// M-23: the previous "jitter" was `1.5 + attempt * 0.5` — fully
    /// determined by the attempt number, so every call with the same
    /// `current` delay produced the exact same result. Real jitter must
    /// vary across calls.
    #[test]
    fn jittered_backoff_is_actually_random() {
        let base = Duration::from_millis(200);
        let results: std::collections::HashSet<_> = (0..20)
            .map(|_| jittered_backoff(base).as_micros())
            .collect();
        assert!(
            results.len() > 1,
            "20 calls with the same input produced the same output — not random"
        );
    }

    #[test]
    fn jittered_backoff_stays_within_documented_range() {
        let base = Duration::from_millis(200);
        for _ in 0..100 {
            let result = jittered_backoff(base);
            assert!(
                result >= base.mul_f64(1.5) && result < base.mul_f64(2.5),
                "backoff {result:?} outside the documented [1.5x, 2.5x) range"
            );
        }
    }

    #[tokio::test]
    async fn constructor_does_not_initialize_http_client() {
        let sqlite = Arc::new(SqliteStorage::in_memory().await.unwrap());
        let mobile_storage = Arc::new(MobileStorage::new(sqlite.clone()));
        let orchestrator = SyncOrchestrator::new(
            mobile_storage,
            sqlite,
            RootKey {
                pubkey: VerifyingKey::from_bytes(&[0u8; 32]).unwrap(),
            },
            "0.4.0".to_string(),
            50,
        );

        assert!(orchestrator.http.get().is_none());
    }

    #[tokio::test]
    async fn version_meets_min_works() {
        let sqlite = Arc::new(SqliteStorage::in_memory().await.unwrap());
        let mobile_storage = Arc::new(MobileStorage::new(sqlite.clone()));
        let root =
            RootKey::from_base64("MCowBQYDK2VwAyEAJr8nz5g7XPz8r2M6p/7u0k4v1i8v3y5x9B0jK7mNqRs=");
        // The above base64 is 44 chars which is wrong for 32-byte key; test the logic path.
        let orch = SyncOrchestrator::new(
            mobile_storage,
            sqlite,
            root.unwrap_or_else(|_| RootKey {
                pubkey: VerifyingKey::from_bytes(&[0u8; 32]).unwrap(),
            }),
            "0.4.0".to_string(),
            50,
        );

        assert!(orch.version_meets_min("0.4.0", "0.3.0"));
        assert!(orch.version_meets_min("0.4.0", "0.4.0"));
        assert!(!orch.version_meets_min("0.3.0", "0.4.0"));
        assert!(orch.version_meets_min("1.0.0", "0.9.9"));
        assert!(!orch.version_meets_min("0.4.0", "0.4.1"));
    }

    /// Unparseable version components must fail closed: `filter_map`-style
    /// parsing would silently treat "0.4.x" as [0, 4], i.e. as if the garbage
    /// component were absent.
    #[tokio::test]
    async fn version_meets_min_fails_closed_on_garbage() {
        let sqlite = Arc::new(SqliteStorage::in_memory().await.unwrap());
        let mobile_storage = Arc::new(MobileStorage::new(sqlite.clone()));
        let orch = SyncOrchestrator::new(
            mobile_storage,
            sqlite,
            RootKey {
                pubkey: VerifyingKey::from_bytes(&[0u8; 32]).unwrap(),
            },
            "0.4.0".to_string(),
            50,
        );

        assert!(!orch.version_meets_min("0.4.x", "0.3.0"));
        assert!(!orch.version_meets_min("0.4.0", "0.4.x"));
        assert!(!orch.version_meets_min("1.x", "0.9"));
        assert!(!orch.version_meets_min("0.9", "1.x"));
        assert!(!orch.version_meets_min("", "0.1.0"));
    }

    #[test]
    fn root_key_from_base64_rejects_bad_input() {
        let result = RootKey::from_base64("not-valid-base64!!!");
        assert!(result.is_err());
    }

    #[test]
    fn root_key_from_base64_rejects_wrong_length() {
        let result = RootKey::from_base64(&BASE64.encode([0u8; 16]));
        assert!(result.is_err());
    }

    #[test]
    fn root_key_verify_rejects_bad_signature() {
        let key = RootKey {
            pubkey: VerifyingKey::from_bytes(&[0u8; 32]).unwrap(),
        };
        let result = key.verify(b"hello", &[0u8; 64]);
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn cache_signing_keys_stores_in_mobile_storage() {
        let sqlite = Arc::new(SqliteStorage::in_memory().await.unwrap());
        let mobile_storage = Arc::new(MobileStorage::new(sqlite.clone()));
        let signing_key = ed25519_dalek::SigningKey::from_bytes(&[1u8; 32]);
        let pk_b64 = BASE64.encode(signing_key.verifying_key().to_bytes());

        let orch = SyncOrchestrator::new(
            mobile_storage.clone(),
            sqlite,
            RootKey {
                pubkey: VerifyingKey::from_bytes(&[0u8; 32]).unwrap(),
            },
            "0.4.0".to_string(),
            50,
        );

        let manifest = Manifest {
            signing_keys: vec![SigningKeyEntry {
                key_id: "test-key".to_string(),
                public_key: pk_b64,
            }],
            sequences: vec![],
            removed: vec![],
            manifest_version: 1,
            generated_at: Utc::now(),
        };

        let keys = orch.cache_test_signing_keys(&manifest).await.unwrap();
        assert_eq!(keys.len(), 1);
        assert!(keys.contains_key("test-key"));

        let persisted = mobile_storage.get_trusted_key("test-key").await.unwrap();
        assert!(persisted.is_some());
    }

    #[tokio::test]
    async fn persist_sync_metadata_writes_timestamp() {
        let sqlite = Arc::new(SqliteStorage::in_memory().await.unwrap());
        let mobile_storage = Arc::new(MobileStorage::new(sqlite.clone()));
        let orch = SyncOrchestrator::new(
            mobile_storage.clone(),
            sqlite,
            RootKey {
                pubkey: VerifyingKey::from_bytes(&[0u8; 32]).unwrap(),
            },
            "0.4.0".to_string(),
            50,
        );

        orch.persist_sync_metadata(Some("etag-123".to_string()), None, None)
            .await
            .unwrap();

        let ts = mobile_storage
            .get_sync_metadata("last_sync_ts")
            .await
            .unwrap();
        assert!(ts.is_some());
        let etag = mobile_storage
            .get_sync_metadata("manifest_etag")
            .await
            .unwrap();
        assert_eq!(etag.as_deref(), Some("etag-123"));
    }

    #[tokio::test]
    async fn persist_sync_metadata_without_etag() {
        let sqlite = Arc::new(SqliteStorage::in_memory().await.unwrap());
        let mobile_storage = Arc::new(MobileStorage::new(sqlite.clone()));
        let orch = SyncOrchestrator::new(
            mobile_storage.clone(),
            sqlite,
            RootKey {
                pubkey: VerifyingKey::from_bytes(&[0u8; 32]).unwrap(),
            },
            "0.4.0".to_string(),
            50,
        );

        orch.persist_sync_metadata(None, None, None).await.unwrap();

        let ts = mobile_storage
            .get_sync_metadata("last_sync_ts")
            .await
            .unwrap();
        assert!(ts.is_some());
        let etag = mobile_storage
            .get_sync_metadata("manifest_etag")
            .await
            .unwrap();
        assert!(etag.is_none());
    }

    #[test]
    fn verify_and_parse_manifest_rejects_no_divider() {
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        rt.block_on(async {
            let sqlite = Arc::new(SqliteStorage::in_memory().await.unwrap());
            let mobile_storage = Arc::new(MobileStorage::new(sqlite.clone()));
            let orch = SyncOrchestrator::new(
                mobile_storage,
                sqlite,
                RootKey {
                    pubkey: VerifyingKey::from_bytes(&[0u8; 32]).unwrap(),
                },
                "0.4.0".to_string(),
                50,
            );

            let result = orch.verify_and_parse_manifest(b"no-newline-here");
            assert!(result.is_err());
        });
    }

    #[test]
    fn version_meets_min_edge_cases() {
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        rt.block_on(async {
            let sqlite = Arc::new(SqliteStorage::in_memory().await.unwrap());
            let mobile_storage = Arc::new(MobileStorage::new(sqlite.clone()));
            let orch = SyncOrchestrator::new(
                mobile_storage,
                sqlite,
                RootKey {
                    pubkey: VerifyingKey::from_bytes(&[0u8; 32]).unwrap(),
                },
                "0.4.0".to_string(),
                50,
            );

            assert!(orch.version_meets_min("0.4", "0.4.0"));
            assert!(orch.version_meets_min("0.4.0", "0.4"));
            assert!(orch.version_meets_min("1", "0.9.9"));
            assert!(!orch.version_meets_min("0", "0.0.1"));
        });
    }

    pub(super) fn make_test_sequence(name: &str, created_at: DateTime<Utc>) -> SequenceDefinition {
        use orch8_types::ids::{Namespace, SequenceId, TenantId};
        SequenceDefinition {
            embed: None,
            sub_tenant: None,
            schema: None,
            schema_version: orch8_types::sequence::SEQUENCE_SCHEMA_VERSION,
            id: SequenceId::new(),
            tenant_id: TenantId::new("mobile").unwrap(),
            namespace: Namespace::new("default"),
            name: name.to_string(),
            version: 1,
            deprecated: false,
            status: SequenceStatus::default(),
            blocks: vec![],
            interceptors: None,
            input_schema: None,
            sla: None,
            on_failure: None,
            on_cancel: None,
            placement: None,
            created_at,
        }
    }

    #[tokio::test]
    async fn local_sequence_projection_keeps_latest_version() {
        let sqlite = Arc::new(SqliteStorage::in_memory().await.unwrap());
        let mobile_storage = Arc::new(MobileStorage::new(sqlite.clone()));
        let backend: Arc<dyn StorageBackend> = sqlite.clone();
        let orch = SyncOrchestrator::new(
            mobile_storage,
            backend.clone(),
            RootKey {
                pubkey: VerifyingKey::from_bytes(&[0u8; 32]).unwrap(),
            },
            "0.4.0".to_string(),
            10,
        );

        let mut version_one = make_test_sequence("shared", Utc::now());
        version_one.version = 1;
        backend.create_sequence(&version_one).await.unwrap();
        let mut version_three = make_test_sequence("shared", Utc::now());
        version_three.version = 3;
        backend.create_sequence(&version_three).await.unwrap();

        let versions = orch.list_local_sequences().await.unwrap();
        assert_eq!(versions, HashMap::from([("shared".to_string(), 3)]));
    }

    #[tokio::test]
    async fn evict_excess_sequences_removes_oldest() {
        let sqlite = Arc::new(SqliteStorage::in_memory().await.unwrap());
        let mobile_storage = Arc::new(MobileStorage::new(sqlite.clone()));
        let backend: Arc<dyn StorageBackend> = sqlite.clone();

        let orch = SyncOrchestrator::new(
            mobile_storage,
            backend.clone(),
            RootKey {
                pubkey: VerifyingKey::from_bytes(&[0u8; 32]).unwrap(),
            },
            "0.4.0".to_string(),
            3,
        );

        let base = Utc::now() - chrono::Duration::hours(10);
        for i in 0..5 {
            let seq = make_test_sequence(&format!("seq-{i}"), base + chrono::Duration::hours(i));
            backend.create_sequence(&seq).await.unwrap();
        }

        let tenant = orch8_types::ids::TenantId::new("mobile").unwrap();
        let ns = orch8_types::ids::Namespace::new("default");
        let before = backend
            .list_sequences(Some(&tenant), Some(&ns), 100, 0)
            .await
            .unwrap();
        assert_eq!(before.len(), 5);

        orch.evict_excess_sequences().await.unwrap();

        let after = backend
            .list_sequences(Some(&tenant), Some(&ns), 100, 0)
            .await
            .unwrap();
        assert_eq!(after.len(), 3);

        let remaining_names: Vec<&str> = after.iter().map(|s| s.name.as_str()).collect();
        assert!(!remaining_names.contains(&"seq-0"));
        assert!(!remaining_names.contains(&"seq-1"));
        assert!(remaining_names.contains(&"seq-2"));
        assert!(remaining_names.contains(&"seq-3"));
        assert!(remaining_names.contains(&"seq-4"));
    }

    #[tokio::test]
    async fn evict_excess_sequences_noop_when_under_limit() {
        let sqlite = Arc::new(SqliteStorage::in_memory().await.unwrap());
        let mobile_storage = Arc::new(MobileStorage::new(sqlite.clone()));
        let backend: Arc<dyn StorageBackend> = sqlite.clone();

        let orch = SyncOrchestrator::new(
            mobile_storage,
            backend.clone(),
            RootKey {
                pubkey: VerifyingKey::from_bytes(&[0u8; 32]).unwrap(),
            },
            "0.4.0".to_string(),
            10,
        );

        for i in 0..3 {
            let seq = make_test_sequence(&format!("seq-{i}"), Utc::now());
            backend.create_sequence(&seq).await.unwrap();
        }

        orch.evict_excess_sequences().await.unwrap();

        let tenant = orch8_types::ids::TenantId::new("mobile").unwrap();
        let ns = orch8_types::ids::Namespace::new("default");
        let after = backend
            .list_sequences(Some(&tenant), Some(&ns), 100, 0)
            .await
            .unwrap();
        assert_eq!(after.len(), 3);
    }

    #[tokio::test]
    async fn evict_excess_sequences_zero_limit_is_noop() {
        let sqlite = Arc::new(SqliteStorage::in_memory().await.unwrap());
        let mobile_storage = Arc::new(MobileStorage::new(sqlite.clone()));
        let backend: Arc<dyn StorageBackend> = sqlite.clone();

        let orch = SyncOrchestrator::new(
            mobile_storage,
            backend.clone(),
            RootKey {
                pubkey: VerifyingKey::from_bytes(&[0u8; 32]).unwrap(),
            },
            "0.4.0".to_string(),
            0,
        );

        for i in 0..5 {
            let seq = make_test_sequence(&format!("seq-{i}"), Utc::now());
            backend.create_sequence(&seq).await.unwrap();
        }

        orch.evict_excess_sequences().await.unwrap();

        let tenant = orch8_types::ids::TenantId::new("mobile").unwrap();
        let ns = orch8_types::ids::Namespace::new("default");
        let after = backend
            .list_sequences(Some(&tenant), Some(&ns), 100, 0)
            .await
            .unwrap();
        assert_eq!(after.len(), 5);
    }

    #[tokio::test]
    async fn reconcile_removed_skips_old_removals() {
        let sqlite = Arc::new(SqliteStorage::in_memory().await.unwrap());
        let mobile_storage = Arc::new(MobileStorage::new(sqlite.clone()));
        let backend: Arc<dyn StorageBackend> = sqlite.clone();

        let orch = SyncOrchestrator::new(
            mobile_storage.clone(),
            backend.clone(),
            RootKey {
                pubkey: VerifyingKey::from_bytes(&[0u8; 32]).unwrap(),
            },
            "0.4.0".to_string(),
            50,
        );

        // Set a recent last_sync so full reconciliation is skipped.
        mobile_storage
            .set_sync_metadata("last_sync_ts", &Utc::now().to_rfc3339())
            .await
            .unwrap();

        let base = Utc::now() - chrono::Duration::days(40);
        let old_seq = make_test_sequence("old-seq", base);
        backend.create_sequence(&old_seq).await.unwrap();

        let local = orch.list_local_sequences().await.unwrap();
        assert!(local.contains_key("old-seq"));

        let manifest = Manifest {
            signing_keys: vec![],
            sequences: vec![],
            removed: vec![RemovedEntry {
                name: "old-seq".to_string(),
                removed_at: base + chrono::Duration::days(1),
            }],
            manifest_version: 1,
            generated_at: Utc::now(),
        };

        let removed = orch.reconcile_removed(&manifest, &local).await.unwrap();
        assert_eq!(removed, 0, "removals older than 30 days should be skipped");

        // The sequence should still exist.
        let tenant = orch8_types::ids::TenantId::new("mobile").unwrap();
        let ns = orch8_types::ids::Namespace::new("default");
        let after = backend
            .list_sequences(Some(&tenant), Some(&ns), 100, 0)
            .await
            .unwrap();
        assert_eq!(after.len(), 1);
    }

    #[tokio::test]
    async fn reconcile_removed_deletes_recent_removals() {
        let sqlite = Arc::new(SqliteStorage::in_memory().await.unwrap());
        let mobile_storage = Arc::new(MobileStorage::new(sqlite.clone()));
        let backend: Arc<dyn StorageBackend> = sqlite.clone();

        let orch = SyncOrchestrator::new(
            mobile_storage.clone(),
            backend.clone(),
            RootKey {
                pubkey: VerifyingKey::from_bytes(&[0u8; 32]).unwrap(),
            },
            "0.4.0".to_string(),
            50,
        );

        // Set a recent last_sync so full reconciliation is skipped.
        mobile_storage
            .set_sync_metadata("last_sync_ts", &Utc::now().to_rfc3339())
            .await
            .unwrap();

        let recent_seq = make_test_sequence("recent-seq", Utc::now());
        backend.create_sequence(&recent_seq).await.unwrap();

        let local = orch.list_local_sequences().await.unwrap();

        let manifest = Manifest {
            signing_keys: vec![],
            sequences: vec![],
            removed: vec![RemovedEntry {
                name: "recent-seq".to_string(),
                removed_at: Utc::now(),
            }],
            manifest_version: 1,
            generated_at: Utc::now(),
        };

        let removed = orch.reconcile_removed(&manifest, &local).await.unwrap();
        assert_eq!(removed, 1, "recent removals should be processed");

        let tenant = orch8_types::ids::TenantId::new("mobile").unwrap();
        let ns = orch8_types::ids::Namespace::new("default");
        let after = backend
            .list_sequences(Some(&tenant), Some(&ns), 100, 0)
            .await
            .unwrap();
        assert_eq!(after.len(), 0);
    }

    #[tokio::test]
    async fn sync_returns_empty_when_not_modified() {
        let sqlite = Arc::new(SqliteStorage::in_memory().await.unwrap());
        let mobile_storage = Arc::new(MobileStorage::new(sqlite.clone()));
        let backend: Arc<dyn StorageBackend> = sqlite.clone();

        // Pre-seed an ETag so the sync sends If-None-Match.
        mobile_storage
            .set_sync_metadata("manifest_etag", "etag-abc")
            .await
            .unwrap();

        let orch = SyncOrchestrator::new(
            mobile_storage,
            backend,
            RootKey {
                pubkey: VerifyingKey::from_bytes(&[0u8; 32]).unwrap(),
            },
            "0.4.0".to_string(),
            50,
        );

        orch.mobile_storage
            .set_sync_metadata(ENV_FINGERPRINT_KEY, &orch.env_fingerprint(&HashSet::new()))
            .await
            .unwrap();
        orch.mobile_storage
            .set_sync_metadata("last_manifest_version", "1")
            .await
            .unwrap();

        // Spin up a tiny HTTP server that returns 304.
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        let server = tokio::spawn(async move {
            use tokio::io::AsyncReadExt;
            use tokio::io::AsyncWriteExt;
            let (mut socket, _) = listener.accept().await.unwrap();
            let mut buf = vec![0u8; 4096];
            let _n = socket.read(&mut buf).await.unwrap();
            let response = "HTTP/1.1 304 Not Modified\r\nContent-Length: 0\r\n\r\n";
            socket.write_all(response.as_bytes()).await.unwrap();
        });

        let url = format!("http://127.0.0.1:{port}/manifest");
        let result = orch
            .sync(&url, &SyncAuth::UrlToken, &HashSet::new())
            .await
            .unwrap();
        assert!(orch.http.get().is_some());

        server.await.unwrap();

        assert_eq!(result.added, 0);
        assert_eq!(result.updated, 0);
        assert_eq!(result.removed, 0);
        assert_eq!(result.skipped, 0);
        assert_eq!(result.signature_failures, 0);
    }

    /// H-15: a sequence whose local copy is already at (or ahead of) the
    /// manifest's version must be skipped *before* any network request — not
    /// downloaded (and its detached signature downloaded) only to be
    /// discarded afterward once the version check finally runs.
    #[tokio::test]
    async fn download_and_verify_sequences_skips_before_download_when_up_to_date() {
        let sqlite = Arc::new(SqliteStorage::in_memory().await.unwrap());
        let mobile_storage = Arc::new(MobileStorage::new(sqlite.clone()));
        let backend: Arc<dyn StorageBackend> = sqlite.clone();

        let orch = SyncOrchestrator::new(
            mobile_storage,
            backend.clone(),
            RootKey {
                pubkey: VerifyingKey::from_bytes(&[0u8; 32]).unwrap(),
            },
            "0.4.0".to_string(),
            50,
        )
        .with_insecure_urls_for_tests();

        // Local copy already at version 2.
        let mut existing = make_test_sequence("seq-a", Utc::now());
        existing.version = 2;
        backend.create_sequence(&existing).await.unwrap();
        let local_sequences = orch.list_local_sequences().await.unwrap();

        // A listener that flags whether anything ever connects to it. A
        // bounded wait lets the test observe "no connection" without hanging
        // forever if the fix regresses and a download is attempted.
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        let connected = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let connected2 = Arc::clone(&connected);
        let server = tokio::spawn(async move {
            if let Ok(Ok(_)) =
                tokio::time::timeout(Duration::from_millis(300), listener.accept()).await
            {
                connected2.store(true, std::sync::atomic::Ordering::SeqCst);
            }
        });

        let manifest = Manifest {
            signing_keys: vec![],
            sequences: vec![ManifestSequenceEntry {
                name: "seq-a".to_string(),
                version: 2, // same as local -> must skip without downloading
                url: format!("http://127.0.0.1:{port}/seq-a.json"),
                signing_key_id: "k1".to_string(),
                sha256: "deadbeef".to_string(),
                required_handlers: vec![],
                min_sdk_version: "0.0.0".to_string(),
            }],
            removed: vec![],
            manifest_version: 1,
            generated_at: Utc::now(),
        };

        let trusted_keys = HashMap::new();
        let (added, updated, skipped, sig_failures, _retryable) = orch
            .download_and_verify_sequences(
                "http://127.0.0.1/manifest",
                &manifest,
                &ScopedSyncAuth::UrlToken,
                &HashSet::new(),
                &local_sequences,
                &trusted_keys,
            )
            .await
            .unwrap();

        server.await.unwrap();

        assert_eq!(added, 0);
        assert_eq!(updated, 0);
        assert_eq!(skipped, 1);
        assert_eq!(sig_failures, 0);
        assert!(
            !connected.load(std::sync::atomic::Ordering::SeqCst),
            "an up-to-date sequence must not be downloaded"
        );
    }

    /// The version check must still allow a genuinely newer manifest entry
    /// through to download — the skip-before-download fix must not turn into
    /// a skip-always.
    #[tokio::test]
    async fn download_and_verify_sequences_downloads_when_newer_version_available() {
        let sqlite = Arc::new(SqliteStorage::in_memory().await.unwrap());
        let mobile_storage = Arc::new(MobileStorage::new(sqlite.clone()));
        let backend: Arc<dyn StorageBackend> = sqlite.clone();

        let orch = SyncOrchestrator::new(
            mobile_storage,
            backend.clone(),
            RootKey {
                pubkey: VerifyingKey::from_bytes(&[0u8; 32]).unwrap(),
            },
            "0.4.0".to_string(),
            50,
        )
        .with_insecure_urls_for_tests();

        // Local copy is behind the manifest's version.
        let mut existing = make_test_sequence("seq-a", Utc::now());
        existing.version = 1;
        backend.create_sequence(&existing).await.unwrap();
        let local_sequences = orch.list_local_sequences().await.unwrap();

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        let connected = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let connected2 = Arc::clone(&connected);
        let server = tokio::spawn(async move {
            use tokio::io::AsyncWriteExt;
            // Only one request is expected: the (attempted) sequence
            // download. Respond with a 404 — this test only cares that the
            // connection was attempted, not that the full flow succeeds.
            if let Ok(Ok((mut socket, _))) =
                tokio::time::timeout(Duration::from_secs(5), listener.accept()).await
            {
                connected2.store(true, std::sync::atomic::Ordering::SeqCst);
                let _ = socket
                    .write_all(b"HTTP/1.1 404 Not Found\r\nContent-Length: 0\r\n\r\n")
                    .await;
            }
        });

        let manifest = Manifest {
            signing_keys: vec![],
            sequences: vec![ManifestSequenceEntry {
                name: "seq-a".to_string(),
                version: 2, // newer than local -> must attempt download
                url: format!("http://127.0.0.1:{port}/seq-a.json"),
                signing_key_id: "k1".to_string(),
                sha256: "deadbeef".to_string(),
                required_handlers: vec![],
                min_sdk_version: "0.0.0".to_string(),
            }],
            removed: vec![],
            manifest_version: 1,
            generated_at: Utc::now(),
        };

        let trusted_keys = HashMap::from([(
            "k1".to_string(),
            ed25519_dalek::SigningKey::from_bytes(&[7; 32]).verifying_key(),
        )]);
        let (_added, _updated, skipped, _sig_failures, _retryable) = orch
            .download_and_verify_sequences(
                "http://127.0.0.1/manifest",
                &manifest,
                &ScopedSyncAuth::UrlToken,
                &HashSet::new(),
                &local_sequences,
                &trusted_keys,
            )
            .await
            .unwrap();

        server.await.unwrap();

        assert!(
            connected.load(std::sync::atomic::Ordering::SeqCst),
            "a genuinely newer sequence must still be downloaded"
        );
        // The download itself fails (mock server returns 404), which is
        // counted as `skipped` too — but that's the pre-existing
        // download-failure path, not the skip-before-download check this
        // test targets. What matters here is that `connected` is true.
        assert_eq!(skipped, 1);
    }

    /// H-14: a poison manifest entry — one that passes hash AND signature
    /// verification but fails to deserialize as a `SequenceDefinition` —
    /// must not abort the whole sync. Every other entry in the same manifest
    /// must still be processed (previously a bare `?` on the JSON parse
    /// propagated an error out of the whole loop).
    #[test]
    fn signature_url_only_rewrites_path_extension() {
        let url = reqwest::Url::parse("https://cdn.json.example/a.json/seq.json?x=.json").unwrap();
        assert_eq!(
            signature_url(&url).as_str(),
            "https://cdn.json.example/a.json/seq.sig?x=.json"
        );
        let url = reqwest::Url::parse("https://cdn.example/seq").unwrap();
        assert_eq!(signature_url(&url).as_str(), "https://cdn.example/seq.sig");
    }

    #[tokio::test]
    async fn resolve_entry_url_joins_relative_and_enforces_https() {
        let sqlite = Arc::new(SqliteStorage::in_memory().await.unwrap());
        let orch = SyncOrchestrator::new(
            Arc::new(MobileStorage::new(sqlite.clone())),
            sqlite,
            RootKey {
                pubkey: VerifyingKey::from_bytes(&[0u8; 32]).unwrap(),
            },
            "0.4.0".to_string(),
            50,
        );
        let base = "https://cdn.example/t/manifest.json";
        assert_eq!(
            orch.resolve_entry_url(base, "sequences/a/1.json")
                .unwrap()
                .as_str(),
            "https://cdn.example/t/sequences/a/1.json"
        );
        assert!(
            orch.resolve_entry_url(base, "http://cdn.example/a.json")
                .is_err()
        );
        assert!(
            orch.resolve_entry_url(base, "https://127.0.0.1/a.json")
                .is_err()
        );
    }

    /// MOB-N3 + MOB-N4: a malformed `.sig` counts as one signature failure
    /// (the rest of the manifest still syncs), and relative entry URLs are
    /// resolved against the manifest URL.
    #[tokio::test]
    async fn malformed_signature_skips_entry_and_relative_urls_resolve() {
        use ed25519_dalek::Signer;
        use tokio::io::{AsyncReadExt, AsyncWriteExt};

        let sqlite = Arc::new(SqliteStorage::in_memory().await.unwrap());
        let orch = SyncOrchestrator::new(
            Arc::new(MobileStorage::new(sqlite.clone())),
            sqlite,
            RootKey {
                pubkey: VerifyingKey::from_bytes(&[0u8; 32]).unwrap(),
            },
            "0.4.0".to_string(),
            50,
        )
        .with_insecure_urls_for_tests();
        let signing_key = ed25519_dalek::SigningKey::from_bytes(&[7u8; 32]);
        let bad_seq = make_test_sequence("seq-bad", Utc::now());
        let bad_json = serde_json::to_vec(&bad_seq).unwrap();
        let good_seq = make_test_sequence("seq-good", Utc::now());
        let good_json = serde_json::to_vec(&good_seq).unwrap();
        let good_sig = signing_key.sign(&good_json);
        let (bad_body, good_body) = (bad_json.clone(), good_json.clone());

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        let paths = Arc::new(std::sync::Mutex::new(Vec::<String>::new()));
        let seen = Arc::clone(&paths);
        let server = tokio::spawn(async move {
            for _ in 0..4 {
                let Ok((mut socket, _)) = listener.accept().await else {
                    break;
                };
                let mut buf = vec![0u8; 4096];
                let n = socket.read(&mut buf).await.unwrap_or(0);
                let req = String::from_utf8_lossy(&buf[..n]);
                let path = req
                    .lines()
                    .next()
                    .unwrap_or("")
                    .split_whitespace()
                    .nth(1)
                    .unwrap_or("")
                    .to_string();
                seen.lock().unwrap().push(path.clone());
                let body: Vec<u8> = match path.as_str() {
                    "/m/bad.json" => bad_body.clone(),
                    "/m/bad.sig" => b"%%% not base64 %%%".to_vec(),
                    "/m/good.json" => good_body.clone(),
                    _ => BASE64.encode(good_sig.to_bytes()).into_bytes(),
                };
                let head = format!(
                    "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                    body.len()
                );
                let _ = socket.write_all(head.as_bytes()).await;
                let _ = socket.write_all(&body).await;
            }
        });

        let entry = |name: &str, url: &str, json: &[u8]| ManifestSequenceEntry {
            name: name.to_string(),
            version: 1,
            url: url.to_string(),
            signing_key_id: "k1".to_string(),
            sha256: to_hex(&Sha256::digest(json)),
            required_handlers: vec![],
            min_sdk_version: "0.0.0".to_string(),
        };
        let manifest = Manifest {
            signing_keys: vec![],
            sequences: vec![
                entry("seq-bad", "bad.json", &bad_json),
                entry("seq-good", "good.json", &good_json),
            ],
            removed: vec![],
            manifest_version: 1,
            generated_at: Utc::now(),
        };
        let mut trusted_keys = HashMap::new();
        trusted_keys.insert("k1".to_string(), signing_key.verifying_key());

        let (added, _, skipped, sig_failures, _retryable) = orch
            .download_and_verify_sequences(
                &format!("http://127.0.0.1:{port}/m/manifest.json"),
                &manifest,
                &ScopedSyncAuth::UrlToken,
                &HashSet::new(),
                &HashMap::new(),
                &trusted_keys,
            )
            .await
            .expect("a malformed .sig must not abort the sync");
        server.await.unwrap();
        assert_eq!(sig_failures, 1);
        assert_eq!(added, 1);
        assert_eq!(skipped, 0);
        assert_eq!(
            *paths.lock().unwrap(),
            vec!["/m/bad.json", "/m/bad.sig", "/m/good.json", "/m/good.sig"]
        );
    }

    #[tokio::test]
    #[allow(
        clippy::too_many_lines,
        clippy::case_sensitive_file_extension_comparisons
    )]
    async fn download_and_verify_sequences_skips_poison_entry_but_processes_the_rest_h14() {
        use ed25519_dalek::Signer;
        use tokio::io::{AsyncReadExt, AsyncWriteExt};

        let sqlite = Arc::new(SqliteStorage::in_memory().await.unwrap());
        let mobile_storage = Arc::new(MobileStorage::new(sqlite.clone()));
        let backend: Arc<dyn StorageBackend> = sqlite.clone();

        let orch = SyncOrchestrator::new(
            mobile_storage,
            backend,
            RootKey {
                pubkey: VerifyingKey::from_bytes(&[0u8; 32]).unwrap(),
            },
            "0.4.0".to_string(),
            50,
        )
        .with_insecure_urls_for_tests();

        let signing_key = ed25519_dalek::SigningKey::from_bytes(&[7u8; 32]);
        let verifying_key = signing_key.verifying_key();

        // Valid signature + hash over bytes that are NOT a valid SequenceDefinition.
        let poison_json = br#"{"not": "a sequence"}"#.to_vec();
        let poison_hash = to_hex(&Sha256::digest(&poison_json));
        let poison_sig = signing_key.sign(&poison_json);

        let good_seq = make_test_sequence("seq-good", Utc::now());
        let good_json = serde_json::to_vec(&good_seq).unwrap();
        let good_hash = to_hex(&Sha256::digest(&good_json));
        let good_sig = signing_key.sign(&good_json);

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        let server = tokio::spawn(async move {
            // 4 requests total: poison.json, poison.sig, good.json, good.sig.
            for _ in 0..4 {
                let Ok((mut socket, _)) = listener.accept().await else {
                    break;
                };
                let mut buf = vec![0u8; 4096];
                let n = socket.read(&mut buf).await.unwrap_or(0);
                let req = String::from_utf8_lossy(&buf[..n]);
                let path = req
                    .lines()
                    .next()
                    .unwrap_or("")
                    .split_whitespace()
                    .nth(1)
                    .unwrap_or("")
                    .to_string();
                let body: Vec<u8> = if path.contains("poison") {
                    if path.ends_with(".sig") {
                        BASE64.encode(poison_sig.to_bytes()).into_bytes()
                    } else {
                        poison_json.clone()
                    }
                } else if path.ends_with(".sig") {
                    BASE64.encode(good_sig.to_bytes()).into_bytes()
                } else {
                    good_json.clone()
                };
                let response = format!(
                    "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                    body.len()
                );
                let _ = socket.write_all(response.as_bytes()).await;
                let _ = socket.write_all(&body).await;
            }
        });

        let manifest = Manifest {
            signing_keys: vec![],
            sequences: vec![
                ManifestSequenceEntry {
                    name: "seq-poison".to_string(),
                    version: 1,
                    url: format!("http://127.0.0.1:{port}/poison.json"),
                    signing_key_id: "k1".to_string(),
                    sha256: poison_hash,
                    required_handlers: vec![],
                    min_sdk_version: "0.0.0".to_string(),
                },
                ManifestSequenceEntry {
                    name: "seq-good".to_string(),
                    version: 1,
                    url: format!("http://127.0.0.1:{port}/good.json"),
                    signing_key_id: "k1".to_string(),
                    sha256: good_hash,
                    required_handlers: vec![],
                    min_sdk_version: "0.0.0".to_string(),
                },
            ],
            removed: vec![],
            manifest_version: 1,
            generated_at: Utc::now(),
        };

        let mut trusted_keys = HashMap::new();
        trusted_keys.insert("k1".to_string(), verifying_key);
        let local_sequences = HashMap::new();

        let (added, updated, skipped, sig_failures, _retryable) = orch
            .download_and_verify_sequences(
                "http://127.0.0.1/manifest",
                &manifest,
                &ScopedSyncAuth::UrlToken,
                &HashSet::new(),
                &local_sequences,
                &trusted_keys,
            )
            .await
            .expect("H-14: a poison entry must not abort the whole sync");

        server.await.unwrap();

        assert_eq!(
            sig_failures, 1,
            "the poison entry must be counted as a failure, not abort the sync"
        );
        assert_eq!(
            added, 1,
            "the well-formed sibling entry must still be processed"
        );
        assert_eq!(updated, 0);
        assert_eq!(skipped, 0);
    }

    /// M-18: `upsert_sequence` used to delete the old row then create the new
    /// one as two independent calls — if the create step failed, the old row
    /// was gone forever with the new one never persisted. `replace_sequence`
    /// now runs both in one transaction, so a failed create must roll back to
    /// the pre-upsert state instead of orphaning the sequence.
    #[tokio::test]
    async fn upsert_sequence_rolls_back_atomically_when_create_step_fails_m18() {
        let sqlite = Arc::new(SqliteStorage::in_memory().await.unwrap());
        let mobile_storage = Arc::new(MobileStorage::new(sqlite.clone()));
        let backend: Arc<dyn StorageBackend> = sqlite.clone();

        let orch = SyncOrchestrator::new(
            mobile_storage,
            backend.clone(),
            RootKey {
                pubkey: VerifyingKey::from_bytes(&[0u8; 32]).unwrap(),
            },
            "0.4.0".to_string(),
            50,
        );

        // Existing sequence that the upsert will attempt to replace.
        let existing = make_test_sequence("seq-a", Utc::now());
        let existing_id = existing.id;
        backend.create_sequence(&existing).await.unwrap();

        // A second, unrelated sequence whose id we'll deliberately collide
        // with, to force the create step of the replace to fail.
        let other = make_test_sequence("seq-other", Utc::now());
        backend.create_sequence(&other).await.unwrap();

        // "New" definition for seq-a that reuses `other`'s id — the INSERT
        // will hit a PRIMARY KEY conflict.
        let mut conflicting_new = make_test_sequence("seq-a", Utc::now());
        conflicting_new.id = other.id;

        let result = orch.upsert_sequence(&conflicting_new).await;
        assert!(result.is_err(), "the conflicting create must fail");

        let still_there = backend.get_sequence(existing_id).await.unwrap();
        assert!(
            still_there.is_some(),
            "M-18: the old sequence must survive a failed replace, not be left deleted"
        );
        let other_untouched = backend.get_sequence(other.id).await.unwrap();
        assert!(
            other_untouched.is_some(),
            "the unrelated sequence must be untouched"
        );
    }

    /// L3-04: a manifest version bump must not cascade-delete instances that
    /// are still running the old version. The new version is installed
    /// alongside; the old one is pruned once its instances are terminal.
    #[tokio::test]
    async fn upsert_sequence_keeps_old_version_for_in_flight_instances() {
        use orch8_types::instance::{InstanceState, Priority, TaskInstance};
        let sqlite = Arc::new(SqliteStorage::in_memory().await.unwrap());
        let mobile_storage = Arc::new(MobileStorage::new(sqlite.clone()));
        let backend: Arc<dyn StorageBackend> = sqlite.clone();
        let orch = SyncOrchestrator::new(
            mobile_storage,
            backend.clone(),
            RootKey {
                pubkey: VerifyingKey::from_bytes(&[0u8; 32]).unwrap(),
            },
            "0.4.0".to_string(),
            50,
        );

        let v1 = make_test_sequence("habit", Utc::now());
        backend.create_sequence(&v1).await.unwrap();
        let now = Utc::now();
        let instance = TaskInstance {
            claimed_at: None,
            id: orch8_types::ids::InstanceId::new(),
            sub_tenant: None,
            sequence_id: v1.id,
            tenant_id: v1.tenant_id.clone(),
            namespace: v1.namespace.clone(),
            state: InstanceState::Waiting,
            next_fire_at: None,
            priority: Priority::Normal,
            timezone: "UTC".into(),
            metadata: serde_json::json!({}),
            context: orch8_types::context::ExecutionContext::default(),
            concurrency_key: None,
            max_concurrency: None,
            idempotency_key: None,
            session_id: None,
            parent_instance_id: None,
            budget: None,
            created_at: now,
            updated_at: now,
        };
        backend.create_instance(&instance).await.unwrap();

        let mut v2 = make_test_sequence("habit", Utc::now());
        v2.version = 2;
        orch.upsert_sequence(&v2).await.unwrap();

        let kept = backend.get_instance(instance.id).await.unwrap();
        assert!(
            kept.is_some(),
            "the in-flight instance must survive the upgrade"
        );
        assert_eq!(kept.unwrap().sequence_id, v1.id, "it keeps running v1");
        assert!(backend.get_sequence(v1.id).await.unwrap().is_some());
        let latest = backend
            .get_sequence_by_name(&v1.tenant_id, &v1.namespace, "habit", None)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(latest.version, 2, "new runs resolve the new version");

        // Once the instance finishes, the next upgrade retires v1.
        backend
            .update_instance_state(instance.id, InstanceState::Completed, None)
            .await
            .unwrap();
        let mut v3 = make_test_sequence("habit", Utc::now());
        v3.version = 3;
        orch.upsert_sequence(&v3).await.unwrap();
        let versions = backend
            .list_sequence_versions(&v1.tenant_id, &v1.namespace, "habit")
            .await
            .unwrap();
        assert_eq!(
            versions.iter().map(|v| v.version).collect::<Vec<_>>(),
            vec![3],
            "superseded versions without live instances are pruned"
        );
    }

    /// M-23: previously a 429 was not retried at all (only 5xx and
    /// timeouts were), and even where retries did happen the wait time was
    /// always our own guessed backoff, ignoring any `Retry-After` the server
    /// sent. This test's mock server returns 429 with `Retry-After: 0` once,
    /// then 200 — proving both that a 429 now gets retried, and that a
    /// present `Retry-After` header is honored rather than skipped.
    #[tokio::test]
    async fn http_get_with_retry_retries_429_and_honors_retry_after() {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};

        let sqlite = Arc::new(SqliteStorage::in_memory().await.unwrap());
        let mobile_storage = Arc::new(MobileStorage::new(sqlite.clone()));
        let orch = SyncOrchestrator::new(
            mobile_storage,
            sqlite,
            RootKey {
                pubkey: VerifyingKey::from_bytes(&[0u8; 32]).unwrap(),
            },
            "0.4.0".to_string(),
            50,
        );

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        let server = tokio::spawn(async move {
            for i in 0..2 {
                let Ok((mut socket, _)) = listener.accept().await else {
                    break;
                };
                let mut buf = vec![0u8; 4096];
                let _ = socket.read(&mut buf).await;
                let response = if i == 0 {
                    "HTTP/1.1 429 Too Many Requests\r\nRetry-After: 0\r\nContent-Length: 0\r\nConnection: close\r\n\r\n"
                } else {
                    "HTTP/1.1 200 OK\r\nContent-Length: 2\r\nConnection: close\r\n\r\nok"
                };
                let _ = socket.write_all(response.as_bytes()).await;
                let _ = socket.shutdown().await;
            }
        });

        let url = format!("http://127.0.0.1:{port}/manifest");
        let start = std::time::Instant::now();
        let resp = orch
            .http_get_with_retry(&url, &ScopedSyncAuth::UrlToken, None)
            .await
            .unwrap();
        let elapsed = start.elapsed();
        assert_eq!(resp.status(), reqwest::StatusCode::OK);
        // The default computed backoff starts at 200ms; if `Retry-After: 0`
        // were being ignored, this retry would take at least that long.
        // Honoring it means the retry fires essentially immediately.
        assert!(
            elapsed < Duration::from_millis(150),
            "retry took {elapsed:?} — Retry-After: 0 was not honored (fell back to computed backoff)"
        );

        server.await.unwrap();
    }

    #[test]
    fn redacted_url_drops_everything_from_the_first_question_mark() {
        let cases = [
            (
                "https://cdn.example.com/manifest?token=secret",
                "https://cdn.example.com/manifest",
            ),
            (
                "https://cdn.example.com/manifest",
                "https://cdn.example.com/manifest",
            ),
            (
                "https://cdn.example.com/m?a=1?b=2",
                "https://cdn.example.com/m",
            ),
        ];
        for (url, expected) in cases {
            assert_eq!(redacted_url(url), expected, "case: {url}");
        }
    }

    #[test]
    fn to_hex_emits_lowercase_byte_pairs() {
        assert_eq!(to_hex(&[]), "");
        assert_eq!(to_hex(&[0xde, 0xad, 0x00, 0xff]), "dead00ff");
    }

    #[test]
    fn sdk_version_comparison_is_numeric_and_fails_closed() {
        let cases = [
            ("0.4.10", "0.4.9", true, "numeric, not lexicographic"),
            ("1.0.0.0", "1.0", true, "missing components are zero"),
            ("1..0", "0.1", false, "empty component fails closed"),
            (
                "4294967296.0",
                "0.1",
                false,
                "u32-overflowing component fails closed",
            ),
            (
                "0.1",
                "4294967296.0",
                false,
                "overflow in the minimum fails closed",
            ),
        ];
        for (sdk, min, expected, case) in cases {
            assert_eq!(
                sdk_version_meets_min(sdk, min),
                expected,
                "{case}: {sdk} >= {min}"
            );
        }
    }

    pub(super) fn signed_manifest_bytes(
        signing_key: &ed25519_dalek::SigningKey,
        body: &[u8],
    ) -> Vec<u8> {
        use ed25519_dalek::Signer;
        let signature = signing_key.sign(body);
        let mut bytes = BASE64.encode(signature.to_bytes()).into_bytes();
        bytes.push(b'\n');
        bytes.extend_from_slice(body);
        bytes
    }

    fn minimal_manifest_json() -> Vec<u8> {
        serde_json::to_vec(&serde_json::json!({
            "signing_keys": [],
            "sequences": [],
            "removed": [],
            "manifest_version": 7,
            "generated_at": Utc::now(),
        }))
        .unwrap()
    }

    async fn orchestrator_trusting(pubkey: VerifyingKey) -> SyncOrchestrator {
        let sqlite = Arc::new(SqliteStorage::in_memory().await.unwrap());
        let mobile_storage = Arc::new(MobileStorage::new(sqlite.clone()));
        SyncOrchestrator::new(
            mobile_storage,
            sqlite,
            RootKey { pubkey },
            "0.4.0".to_string(),
            50,
        )
    }

    #[tokio::test]
    async fn manifest_signed_by_the_root_key_parses() {
        let signing_key = ed25519_dalek::SigningKey::from_bytes(&[9u8; 32]);
        let orch = orchestrator_trusting(signing_key.verifying_key()).await;
        let bytes = signed_manifest_bytes(&signing_key, &minimal_manifest_json());

        let manifest = orch.verify_and_parse_manifest(&bytes).unwrap();

        assert_eq!(manifest.manifest_version, 7);
        assert!(manifest.sequences.is_empty());
    }

    #[tokio::test]
    async fn manifest_signed_by_another_key_is_rejected_as_a_signature_failure() {
        let signing_key = ed25519_dalek::SigningKey::from_bytes(&[9u8; 32]);
        let other = ed25519_dalek::SigningKey::from_bytes(&[3u8; 32]);
        let orch = orchestrator_trusting(other.verifying_key()).await;
        let bytes = signed_manifest_bytes(&signing_key, &minimal_manifest_json());

        let error = orch
            .verify_and_parse_manifest(&bytes)
            .unwrap_err()
            .to_string();

        assert!(error.contains("signature verification failed"), "{error}");
        assert!(
            !error.contains("base64"),
            "a well-formed signature from the wrong key fails at verification, not decoding: {error}"
        );
    }

    #[tokio::test]
    async fn manifest_with_a_non_base64_signature_is_rejected_before_verification() {
        let orch = orchestrator_trusting(VerifyingKey::from_bytes(&[0u8; 32]).unwrap()).await;

        let error = orch
            .verify_and_parse_manifest(b"!!!not-base64!!!\n{}")
            .unwrap_err()
            .to_string();

        assert!(error.contains("base64 decode failed"), "{error}");
    }

    // ---- MC-01: manifest cursor must not advance past skipped entries ----

    type Routes = Arc<std::sync::Mutex<HashMap<String, Vec<u8>>>>;

    /// Tiny multi-request HTTP mock: serves `routes` (404 otherwise).
    async fn spawn_route_server(routes: Routes) -> u16 {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        tokio::spawn(async move {
            loop {
                let Ok((mut socket, _)) = listener.accept().await else {
                    break;
                };
                let routes = Arc::clone(&routes);
                tokio::spawn(async move {
                    let mut buf = vec![0u8; 8192];
                    let n = socket.read(&mut buf).await.unwrap_or(0);
                    let req = String::from_utf8_lossy(&buf[..n]).to_string();
                    let path = req
                        .lines()
                        .next()
                        .unwrap_or("")
                        .split_whitespace()
                        .nth(1)
                        .unwrap_or("")
                        .to_string();
                    let body = routes.lock().unwrap().get(&path).cloned();
                    let (status, body) =
                        body.map_or(("404 Not Found", Vec::new()), |b| ("200 OK", b));
                    let head = format!(
                        "HTTP/1.1 {status}\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                        body.len()
                    );
                    let _ = socket.write_all(head.as_bytes()).await;
                    let _ = socket.write_all(&body).await;
                });
            }
        });
        port
    }

    struct SyncFixture {
        orch: SyncOrchestrator,
        mobile_storage: Arc<MobileStorage>,
        routes: Routes,
        manifest_url: String,
        seq_json: Vec<u8>,
        seq_sig: Vec<u8>,
    }

    async fn sync_fixture(required_handlers: Vec<String>) -> SyncFixture {
        use ed25519_dalek::Signer;
        let sqlite = Arc::new(SqliteStorage::in_memory().await.unwrap());
        let mobile_storage = Arc::new(MobileStorage::new(sqlite.clone()));
        let key = ed25519_dalek::SigningKey::from_bytes(&[9u8; 32]);
        let orch = SyncOrchestrator::new(
            Arc::clone(&mobile_storage),
            sqlite,
            RootKey {
                pubkey: key.verifying_key(),
            },
            "0.4.0".to_string(),
            50,
        )
        .with_insecure_urls_for_tests();

        let seq = make_test_sequence("seq-mc01", Utc::now());
        let seq_json = serde_json::to_vec(&seq).unwrap();
        let seq_sig = BASE64.encode(key.sign(&seq_json).to_bytes()).into_bytes();
        let manifest = serde_json::to_vec(&serde_json::json!({
            "signing_keys": [{
                "key_id": "k1",
                "public_key": BASE64.encode(key.verifying_key().to_bytes()),
            }],
            "sequences": [{
                "name": "seq-mc01",
                "version": 1,
                "url": "seq.json",
                "signing_key_id": "k1",
                "sha256": to_hex(&Sha256::digest(&seq_json)),
                "required_handlers": required_handlers,
                "min_sdk_version": "0.0.0",
            }],
            "removed": [],
            "manifest_version": 5,
            "generated_at": Utc::now(),
        }))
        .unwrap();
        let routes: Routes = Arc::new(std::sync::Mutex::new(HashMap::new()));
        routes.lock().unwrap().insert(
            "/manifest".to_string(),
            signed_manifest_bytes(&key, &manifest),
        );
        let port = spawn_route_server(Arc::clone(&routes)).await;
        SyncFixture {
            orch,
            mobile_storage,
            routes,
            manifest_url: format!("http://127.0.0.1:{port}/manifest"),
            seq_json,
            seq_sig,
        }
    }

    #[tokio::test]
    async fn mc01_download_failure_does_not_advance_manifest_cursor() {
        let f = sync_fixture(vec![]).await;
        let handlers = HashSet::new();

        // Sequence + signature are not served yet: transient download failure.
        let r = f
            .orch
            .sync(&f.manifest_url, &SyncAuth::UrlToken, &handlers)
            .await
            .unwrap();
        assert_eq!((r.added, r.skipped), (0, 1));
        assert_eq!(
            f.mobile_storage
                .get_sync_metadata("last_manifest_version")
                .await
                .unwrap(),
            None,
            "a transiently-skipped entry must leave the cursor unadvanced"
        );

        // Network recovers: the very same manifest version now installs.
        {
            let mut routes = f.routes.lock().unwrap();
            routes.insert("/seq.json".to_string(), f.seq_json.clone());
            routes.insert("/seq.sig".to_string(), f.seq_sig.clone());
        }
        let r = f
            .orch
            .sync(&f.manifest_url, &SyncAuth::UrlToken, &handlers)
            .await
            .unwrap();
        assert_eq!(r.added, 1, "retry must install the previously failed entry");
        assert_eq!(
            f.mobile_storage
                .get_sync_metadata("last_manifest_version")
                .await
                .unwrap()
                .as_deref(),
            Some("5")
        );
    }

    #[tokio::test]
    async fn mc01_registering_a_missing_handler_reopens_the_same_manifest_version() {
        let f = sync_fixture(vec!["h1".to_string()]).await;
        {
            let mut routes = f.routes.lock().unwrap();
            routes.insert("/seq.json".to_string(), f.seq_json.clone());
            routes.insert("/seq.sig".to_string(), f.seq_sig.clone());
        }

        // Old app build: handler missing -> skipped, cursor persisted.
        let r = f
            .orch
            .sync(&f.manifest_url, &SyncAuth::UrlToken, &HashSet::new())
            .await
            .unwrap();
        assert_eq!((r.added, r.skipped), (0, 1));

        // Unchanged environment: replay guard still applies.
        let r = f
            .orch
            .sync(&f.manifest_url, &SyncAuth::UrlToken, &HashSet::new())
            .await
            .unwrap();
        assert_eq!(
            (r.added, r.skipped),
            (0, 0),
            "same env + same version is a no-op"
        );

        // App upgrade registers the handler: same manifest now installs.
        let handlers: HashSet<String> = ["h1".to_string()].into_iter().collect();
        let r = f
            .orch
            .sync(&f.manifest_url, &SyncAuth::UrlToken, &handlers)
            .await
            .unwrap();
        assert_eq!(r.added, 1);
    }
}

#[cfg(test)]
#[path = "sync_auth_tests.rs"]
mod auth_tests;
