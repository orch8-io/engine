//! Sync infrastructure: manifest fetch, signature verification, sequence download,
//! and reconciliation.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::Duration;

use base64::{Engine as _, engine::general_purpose::STANDARD as BASE64};
use chrono::{DateTime, Utc};
use ed25519_dalek::{Signature, Verifier, VerifyingKey};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use tracing::{info, warn};

use orch8_storage::StorageBackend;
use orch8_types::ids::Namespace;
use orch8_types::sequence::SequenceDefinition;

/// Redact query parameters from a URL so sync logs never leak auth tokens
/// when `SyncAuth::UrlToken` is in use.
fn redacted_url(url: &str) -> String {
    url.split_once('?')
        .map_or(url, |(base, _)| base)
        .to_string()
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
    use rand::Rng;
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
/// large bodies) and re-checks the buffered length (catches a lying/absent
/// header). `what` names the resource for error messages.
async fn read_body_capped(resp: reqwest::Response, what: &str) -> Result<Vec<u8>, MobileError> {
    if let Some(len) = resp.content_length()
        && len > MAX_SYNC_RESPONSE_BYTES
    {
        return Err(SyncError::Network {
            message: format!("{what} too large: {len} bytes exceeds limit"),
        }
        .into());
    }
    let bytes = resp.bytes().await.map_err(|e| SyncError::Network {
        message: format!("failed to read {what} body: {e}"),
    })?;
    if bytes.len() as u64 > MAX_SYNC_RESPONSE_BYTES {
        return Err(SyncError::Network {
            message: format!("{what} too large: {} bytes exceeds limit", bytes.len()),
        }
        .into());
    }
    Ok(bytes.to_vec())
}

/// Root of trust: embedded Ed25519 public key for verifying manifest signatures.
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

/// Auth configuration for sync requests.
pub enum SyncAuth {
    /// Token provided via callback interface.
    Bearer(String),
    /// Token embedded directly in the manifest URL (signed URL mode).
    UrlToken,
}

/// Orchestrates manifest fetch, verification, diff, and sequence download.
pub struct SyncOrchestrator {
    mobile_storage: Arc<MobileStorage>,
    backend: Arc<dyn StorageBackend>,
    root_key: RootKey,
    http: reqwest::Client,
    sdk_version: String,
    max_stored_sequences: u32,
}

impl SyncOrchestrator {
    pub fn new(
        mobile_storage: Arc<MobileStorage>,
        backend: Arc<dyn StorageBackend>,
        root_key: RootKey,
        sdk_version: String,
        max_stored_sequences: u32,
    ) -> Self {
        // The builder only uses constants, so failure is a programming error.
        #[allow(clippy::expect_used)]
        let http = reqwest::Client::builder()
            .timeout(Duration::from_secs(30))
            .redirect(reqwest::redirect::Policy::none())
            .dns_resolver(Arc::new(orch8_engine::handlers::builtin::SsrfGuardResolver))
            .build()
            .expect("reqwest client builds");
        Self {
            mobile_storage,
            backend,
            root_key,
            http,
            sdk_version,
            max_stored_sequences,
        }
    }

    /// HTTP GET with exponential backoff and jitter for retryable errors
    /// (server errors, rate limiting, and timeouts). Honors a `Retry-After`
    /// response header (seconds form) when the server sends one, rather than
    /// always falling back to our own guessed backoff (M-23).
    async fn http_get_with_retry(
        &self,
        url: &str,
        auth: &SyncAuth,
        extra_headers: Option<(&str, &str)>,
    ) -> Result<reqwest::Response, MobileError> {
        let mut delay = Duration::from_millis(200);
        let max_retries = 3u32;
        let mut last_err = None;

        for attempt in 0..=max_retries {
            let mut req = self.http.get(url);
            match auth {
                SyncAuth::Bearer(token) => {
                    req = req.header("authorization", format!("Bearer {token}"));
                }
                SyncAuth::UrlToken => {}
            }
            if let Some((k, v)) = extra_headers {
                req = req.header(k, v);
            }

            match req.send().await {
                Ok(resp)
                    if (resp.status().is_server_error()
                        || resp.status() == reqwest::StatusCode::TOO_MANY_REQUESTS)
                        && attempt < max_retries =>
                {
                    let status = resp.status();
                    let display_url = redacted_url(url);
                    let wait = retry_after(&resp).unwrap_or(delay);
                    warn!(attempt, %status, %display_url, wait_ms = wait.as_millis(), "retryable HTTP error");
                    tokio::time::sleep(wait).await;
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
                    tokio::time::sleep(delay).await;
                    delay = jittered_backoff(delay);
                    last_err = Some(e.to_string());
                }
                Err(e) => {
                    return Err(SyncError::Network {
                        message: format!("request failed: {e}"),
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

    /// Perform a full sync against the given manifest URL.
    pub async fn sync(
        &self,
        manifest_url: &str,
        auth: &SyncAuth,
        registered_handlers: &HashSet<String>,
    ) -> Result<SyncResult, MobileError> {
        let mut result = SyncResult::default();

        // 1. Fetch manifest with ETag support.
        let (manifest_bytes, maybe_etag) = self.fetch_manifest(manifest_url, auth).await?;

        if manifest_bytes.is_empty() {
            return Ok(result);
        }

        // 2. Verify manifest signature.
        let manifest: Manifest = self.verify_and_parse_manifest(&manifest_bytes)?;

        // 2a. Reject stale/replayed manifests. A signature only proves the
        // manifest was authentic at some point — it does not prove freshness.
        // Without this check an attacker who captured an older validly-signed
        // manifest could replay it to reinstall sequences that were since
        // removed (e.g. a withdrawn buggy/exploitable workflow). Enforce a
        // strictly-increasing manifest_version.
        if let Some(last_version) = self.get_last_manifest_version().await?
            && manifest.manifest_version <= last_version
        {
            warn!(
                seen = manifest.manifest_version,
                last = last_version,
                "rejecting stale/replayed manifest (version not newer than last applied)"
            );
            return Ok(result);
        }

        // 3. Validate signing keys against root key and cache them.
        let mut trusted_keys = self.cache_signing_keys(&manifest).await?;

        // 4. Reconcile removed sequences.
        let local_sequences = self.list_local_sequences().await?;
        result.removed = self.reconcile_removed(&manifest, &local_sequences).await?;

        // 5. Download and verify each sequence.
        let (added, updated, skipped, sig_failures) = self
            .download_and_verify_sequences(
                &manifest,
                auth,
                registered_handlers,
                &local_sequences,
                &mut trusted_keys,
            )
            .await?;
        result.added = added;
        result.updated = updated;
        result.skipped = skipped;
        result.signature_failures = sig_failures;

        // 6. Evict oldest sequences if over limit.
        self.evict_excess_sequences().await?;

        // 7. Persist sync metadata (including the applied manifest version so a
        // later replay of an older manifest is rejected in step 2a).
        self.persist_sync_metadata(maybe_etag, Some(manifest.manifest_version))
            .await?;

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
    ) -> Result<HashMap<String, VerifyingKey>, MobileError> {
        let mut trusted_keys: HashMap<String, VerifyingKey> = HashMap::new();
        for entry in &manifest.signing_keys {
            let pk_bytes =
                BASE64
                    .decode(&entry.public_key)
                    .map_err(|e| SyncError::SignatureInvalid {
                        message: format!("bad base64 for signing key {}: {e}", entry.key_id),
                    })?;
            let pk = VerifyingKey::from_bytes(&pk_bytes.try_into().map_err(|_| {
                SyncError::SignatureInvalid {
                    message: format!("bad public key length for {}", entry.key_id),
                }
            })?)
            .map_err(|e| SyncError::SignatureInvalid {
                message: format!("bad public key for {}: {e}", entry.key_id),
            })?;
            trusted_keys.insert(entry.key_id.clone(), pk);
            self.mobile_storage
                .upsert_trusted_key(&entry.key_id, &entry.public_key)
                .await
                .map_err(|e| MobileError::Storage {
                    message: e.to_string(),
                })?;
        }

        // Revoke rotated-out keys: the manifest's signing_keys is the current
        // authoritative set, so any locally-cached key whose key_id is no longer
        // present has been rotated out and must stop validating sequences.
        // Without this, a key compromised after rotation would keep verifying
        // forever. (The manifest itself is signed by the root key, so its
        // key-set cannot be forged.)
        let current: HashSet<&str> = manifest
            .signing_keys
            .iter()
            .map(|k| k.key_id.as_str())
            .collect();
        let stored =
            self.mobile_storage
                .list_trusted_keys()
                .await
                .map_err(|e| MobileError::Storage {
                    message: e.to_string(),
                })?;
        for (key_id, _) in stored {
            if !current.contains(key_id.as_str()) {
                if let Err(e) = self.mobile_storage.delete_trusted_key(&key_id).await {
                    warn!(key_id = %key_id, error = %e, "failed to revoke rotated-out signing key");
                } else {
                    info!(key_id = %key_id, "revoked rotated-out signing key");
                }
            }
        }

        Ok(trusted_keys)
    }

    /// Reconcile removed sequences: process explicit removals from the manifest
    /// and perform full reconciliation if the last sync was more than 30 days ago.
    async fn reconcile_removed(
        &self,
        manifest: &Manifest,
        local_sequences: &HashMap<String, SequenceDefinition>,
    ) -> Result<u32, MobileError> {
        let mut removed_count = 0u32;
        let manifest_names: HashSet<String> =
            manifest.sequences.iter().map(|s| s.name.clone()).collect();

        // Reconcile removed entries.
        let cutoff = Utc::now() - chrono::Duration::days(30);
        for removed in &manifest.removed {
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
        let last_sync = self.get_last_sync_ts().await?;
        let thirty_days_ago = Utc::now() - chrono::Duration::days(30);
        if last_sync.is_none_or(|t| t < thirty_days_ago) {
            for name in local_sequences.keys() {
                if !manifest_names.contains(name) {
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
    /// Returns `(added, updated, skipped, signature_failures)`.
    #[allow(clippy::too_many_lines)]
    async fn download_and_verify_sequences(
        &self,
        manifest: &Manifest,
        auth: &SyncAuth,
        registered_handlers: &HashSet<String>,
        local_sequences: &HashMap<String, SequenceDefinition>,
        trusted_keys: &mut HashMap<String, VerifyingKey>,
    ) -> Result<(u32, u32, u32, u32), MobileError> {
        let mut added = 0u32;
        let mut updated = 0u32;
        let mut skipped = 0u32;
        let mut sig_failures = 0u32;

        for entry in &manifest.sequences {
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
            let existing = local_sequences.get(&entry.name);
            if let Some(existing_seq) = existing
                && existing_seq.version >= entry.version
            {
                skipped += 1;
                continue;
            }

            let seq_json = match self.download_sequence(&entry.url, auth).await {
                Ok(json) => json,
                Err(e) => {
                    warn!(name = %entry.name, error = %e, "failed to download sequence");
                    skipped += 1;
                    continue;
                }
            };

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

            // Verify sequence signature using trusted key.
            if !trusted_keys.contains_key(&entry.signing_key_id) {
                let cached = self
                    .mobile_storage
                    .get_trusted_key(&entry.signing_key_id)
                    .await
                    .map_err(|e| MobileError::Storage {
                        message: e.to_string(),
                    })?;
                if let Some(cached_b64) = cached {
                    let bytes =
                        BASE64
                            .decode(&cached_b64)
                            .map_err(|e| SyncError::SignatureInvalid {
                                message: format!("cached key decode failed: {e}"),
                            })?;
                    let pk = VerifyingKey::from_bytes(&bytes.try_into().map_err(|_| {
                        SyncError::SignatureInvalid {
                            message: "cached key invalid length".to_string(),
                        }
                    })?)
                    .map_err(|e| SyncError::SignatureInvalid {
                        message: format!("cached key invalid: {e}"),
                    })?;
                    trusted_keys.insert(entry.signing_key_id.clone(), pk);
                }
            }

            let Some(signing_pk) = trusted_keys.get(&entry.signing_key_id) else {
                warn!(name = %entry.name, key_id = %entry.signing_key_id, "no trusted signing key — skipping");
                sig_failures += 1;
                continue;
            };

            let sig_url = entry.url.replace(".json", ".sig");
            match self.download_sequence(&sig_url, auth).await {
                Ok(sig_b64) => {
                    let sig_bytes =
                        BASE64
                            .decode(sig_b64.trim())
                            .map_err(|e| SyncError::SignatureInvalid {
                                message: format!("sequence signature decode failed: {e}"),
                            })?;
                    let sig: Signature =
                        Signature::from_bytes(&sig_bytes.try_into().map_err(|_| {
                            SyncError::SignatureInvalid {
                                message: "sequence signature wrong length".to_string(),
                            }
                        })?);
                    if let Err(e) = signing_pk.verify(seq_json.as_bytes(), &sig) {
                        warn!(name = %entry.name, error = %e, "sequence signature verification failed — skipping");
                        sig_failures += 1;
                        continue;
                    }
                }
                Err(e) => {
                    warn!(name = %entry.name, error = %e, "failed to download sequence signature — skipping");
                    sig_failures += 1;
                    continue;
                }
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

            self.upsert_sequence(&seq).await?;
            if existing.is_some() {
                updated += 1;
            } else {
                added += 1;
            }
        }

        Ok((added, updated, skipped, sig_failures))
    }

    /// Persist sync timestamp, optional `ETag`, and optional applied manifest
    /// version after a successful sync.
    async fn persist_sync_metadata(
        &self,
        etag: Option<String>,
        manifest_version: Option<i64>,
    ) -> Result<(), MobileError> {
        self.mobile_storage
            .set_sync_metadata("last_sync_ts", &Utc::now().to_rfc3339())
            .await
            .map_err(|e| MobileError::Storage {
                message: e.to_string(),
            })?;
        if let Some(etag) = etag {
            self.mobile_storage
                .set_sync_metadata("manifest_etag", &etag)
                .await
                .map_err(|e| MobileError::Storage {
                    message: e.to_string(),
                })?;
        }
        if let Some(version) = manifest_version {
            self.mobile_storage
                .set_sync_metadata("last_manifest_version", &version.to_string())
                .await
                .map_err(|e| MobileError::Storage {
                    message: e.to_string(),
                })?;
        }
        Ok(())
    }

    /// Last successfully-applied manifest version, used to reject replays of
    /// older signed manifests. Returns `None` before the first sync.
    async fn get_last_manifest_version(&self) -> Result<Option<i64>, MobileError> {
        let raw = self
            .mobile_storage
            .get_sync_metadata("last_manifest_version")
            .await
            .map_err(|e| MobileError::Storage {
                message: e.to_string(),
            })?;
        match raw {
            Some(s) => s
                .parse::<i64>()
                .map(Some)
                .map_err(|e| MobileError::Storage {
                    message: format!("bad last_manifest_version: {e}"),
                }),
            None => Ok(None),
        }
    }

    async fn fetch_manifest(
        &self,
        url: &str,
        auth: &SyncAuth,
    ) -> Result<(Vec<u8>, Option<String>), MobileError> {
        let cached_etag = self
            .mobile_storage
            .get_sync_metadata("manifest_etag")
            .await
            .map_err(|e| MobileError::Storage {
                message: e.to_string(),
            })?;

        let extra = cached_etag.as_deref().map(|etag| ("if-none-match", etag));
        let response = self.http_get_with_retry(url, auth, extra).await?;

        if response.status() == reqwest::StatusCode::NOT_MODIFIED {
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
        let bytes = read_body_capped(response, "manifest").await?;
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

    async fn download_sequence(&self, url: &str, auth: &SyncAuth) -> Result<String, MobileError> {
        let response = self.http_get_with_retry(url, auth, None).await?;

        if !response.status().is_success() {
            return Err(SyncError::Network {
                message: format!("sequence download returned {}", response.status()),
            }
            .into());
        }

        let bytes = read_body_capped(response, "sequence").await?;
        let text = String::from_utf8(bytes).map_err(|e| SyncError::Network {
            message: format!("sequence body is not valid utf-8: {e}"),
        })?;
        Ok(text)
    }

    async fn list_local_sequences(
        &self,
    ) -> Result<HashMap<String, SequenceDefinition>, MobileError> {
        let tenant = crate::mobile_tenant_id();
        let ns = Namespace::new("default");
        let seqs = self
            .backend
            .list_sequences(Some(&tenant), Some(&ns), 1000, 0)
            .await
            .map_err(|e| MobileError::Storage {
                message: e.to_string(),
            })?;
        let mut map = HashMap::new();
        for seq in seqs {
            map.insert(seq.name.clone(), seq);
        }
        Ok(map)
    }

    async fn remove_local_sequence(&self, name: &str) -> Result<(), MobileError> {
        let tenant = crate::mobile_tenant_id();
        let ns = Namespace::new("default");
        let seq = self
            .backend
            .get_sequence_by_name(&tenant, &ns, name, None)
            .await
            .map_err(|e| MobileError::Storage {
                message: e.to_string(),
            })?;
        if let Some(s) = seq {
            self.backend
                .delete_sequence(s.id)
                .await
                .map_err(|e| MobileError::Storage {
                    message: e.to_string(),
                })?;
        }
        Ok(())
    }

    async fn upsert_sequence(&self, seq: &SequenceDefinition) -> Result<(), MobileError> {
        let tenant = crate::mobile_tenant_id();
        let ns = Namespace::new("default");
        match self
            .backend
            .get_sequence_by_name(&tenant, &ns, &seq.name, None)
            .await
        {
            Ok(Some(existing)) => {
                // M-18: delete-then-create as two independent calls could
                // leave the old row deleted with the new one never created if
                // create_sequence failed in between. `replace_sequence` runs
                // both in a single transaction so the upsert is all-or-nothing.
                self.backend
                    .replace_sequence(existing.id, seq)
                    .await
                    .map_err(|e| MobileError::Storage {
                        message: format!("failed to replace sequence '{}': {e}", seq.name),
                    })?;
            }
            Ok(None) => {
                self.backend
                    .create_sequence(seq)
                    .await
                    .map_err(|e| MobileError::Storage {
                        message: e.to_string(),
                    })?;
            }
            Err(e) => {
                return Err(MobileError::Storage {
                    message: e.to_string(),
                });
            }
        }
        Ok(())
    }

    async fn evict_excess_sequences(&self) -> Result<(), MobileError> {
        if self.max_stored_sequences == 0 {
            return Ok(());
        }
        let tenant = crate::mobile_tenant_id();
        let ns = Namespace::new("default");
        let seqs = self
            .backend
            .list_sequences(Some(&tenant), Some(&ns), u32::MAX, 0)
            .await
            .map_err(|e| MobileError::Storage {
                message: e.to_string(),
            })?;
        #[allow(clippy::cast_possible_truncation)]
        let count = seqs.len() as u32;
        if count <= self.max_stored_sequences {
            return Ok(());
        }
        let mut sorted = seqs;
        sorted.sort_by_key(|s| s.created_at);
        let to_evict = (count - self.max_stored_sequences) as usize;
        for seq in sorted.iter().take(to_evict) {
            if let Err(e) = self.backend.delete_sequence(seq.id).await {
                warn!(name = %seq.name, error = %e, "failed to evict excess sequence");
            } else {
                info!(name = %seq.name, "evicted oldest sequence (over limit)");
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
                message: e.to_string(),
            })?;
        match raw {
            Some(s) => s.parse().map(Some).map_err(|e| MobileError::InvalidInput {
                message: format!("bad last_sync_ts: {e}"),
            }),
            None => Ok(None),
        }
    }

    #[allow(clippy::unused_self)]
    fn version_meets_min(&self, sdk: &str, min: &str) -> bool {
        let sdk_parts: Vec<u32> = sdk.split('.').filter_map(|s| s.parse().ok()).collect();
        let min_parts: Vec<u32> = min.split('.').filter_map(|s| s.parse().ok()).collect();
        for i in 0..min_parts.len().max(sdk_parts.len()) {
            let s = sdk_parts.get(i).copied().unwrap_or(0);
            let m = min_parts.get(i).copied().unwrap_or(0);
            if s > m {
                return true;
            }
            if s < m {
                return false;
            }
        }
        true
    }
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

        let keys = orch.cache_signing_keys(&manifest).await.unwrap();
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

        orch.persist_sync_metadata(Some("etag-123".to_string()), None)
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

        orch.persist_sync_metadata(None, None).await.unwrap();

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

    fn make_test_sequence(name: &str, created_at: DateTime<Utc>) -> SequenceDefinition {
        use orch8_types::ids::{Namespace, SequenceId, TenantId};
        SequenceDefinition {
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
            created_at,
        }
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
        );

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

        let mut trusted_keys = HashMap::new();
        let (added, updated, skipped, sig_failures) = orch
            .download_and_verify_sequences(
                &manifest,
                &SyncAuth::UrlToken,
                &HashSet::new(),
                &local_sequences,
                &mut trusted_keys,
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
        );

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
                tokio::time::timeout(Duration::from_millis(300), listener.accept()).await
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

        let mut trusted_keys = HashMap::new();
        let (_added, _updated, skipped, _sig_failures) = orch
            .download_and_verify_sequences(
                &manifest,
                &SyncAuth::UrlToken,
                &HashSet::new(),
                &local_sequences,
                &mut trusted_keys,
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
        );

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

        let (added, updated, skipped, sig_failures) = orch
            .download_and_verify_sequences(
                &manifest,
                &SyncAuth::UrlToken,
                &HashSet::new(),
                &local_sequences,
                &mut trusted_keys,
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
            .http_get_with_retry(&url, &SyncAuth::UrlToken, None)
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
}
