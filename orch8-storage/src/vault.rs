//! BYOK payload vault: externalized payloads in a customer-owned bucket,
//! envelope-encrypted under a customer-managed key.
//!
//! When a vault is attached to [`EncryptingStorage`](crate::encrypting::EncryptingStorage),
//! every externalized payload (large context fields, externalized block
//! outputs, `for_each` snapshots, federation results routed through
//! externalized state) is:
//!
//! 1. encrypted with AES-256-GCM under a data-encryption key (DEK), with the
//!    instance id, ref key, and object path bound as associated data;
//! 2. written as an object to the customer's S3-compatible bucket;
//! 3. replaced in the database by a small **reference**
//!    `{"_o8vault": {"v":1,"object":…,"kid":…,"dek":…}}` whose `dek` is the
//!    DEK *wrapped* by the customer's key provider (AWS KMS or a static key).
//!
//! A process without the bucket credentials and the key provider — for
//! example a managed control plane — only ever sees references. See
//! `docs/FEDERATION.md (BYOK section)` for the exact guarantee and its limits.

use std::sync::Arc;
use std::time::{Duration, Instant};

use aes_gcm::Aes256Gcm;
use aes_gcm::aead::{Aead, Generate, Key, KeyInit, Nonce, Payload};
use async_trait::async_trait;
use base64::Engine as _;
use base64::engine::general_purpose::STANDARD as BASE64;
use bytes::Bytes;
use object_store::{ObjectStore, ObjectStoreExt, PutPayload, path::Path};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use zeroize::Zeroizing;

use orch8_types::error::StorageError;
use orch8_types::ids::InstanceId;

use crate::encrypting::{ExternalPayloadVault, VAULT_REF_KEY};

/// A DEK is reused for at most this long / this many payloads, bounding both
/// key-provider calls and the blast radius of one DEK.
const DEK_MAX_AGE: Duration = Duration::from_secs(300);
const DEK_MAX_USES: u32 = 10_000;
/// Unwrapped DEKs kept in memory (keyed by their wrapped form).
const UNWRAP_CACHE_ENTRIES: u64 = 1_024;
const UNWRAP_CACHE_TTL: Duration = Duration::from_secs(600);
const KMS_CONTEXT: &str = "orch8-externalized-payload-v1";

fn hex(bytes: &[u8]) -> String {
    use std::fmt::Write as _;
    bytes
        .iter()
        .fold(String::with_capacity(bytes.len() * 2), |mut out, b| {
            let _ = write!(out, "{b:02x}");
            out
        })
}

fn vault_err(message: impl Into<String>) -> StorageError {
    StorageError::Encryption(format!("payload vault: {}", message.into()))
}

/// Customer-managed key that wraps/unwraps DEKs. The engine never sees the
/// key material of a KMS-backed provider.
#[async_trait]
pub trait KeyProvider: Send + Sync + 'static {
    /// Stable identifier recorded in every reference (e.g. the KMS key ARN).
    fn key_id(&self) -> &str;
    async fn wrap(&self, dek: &[u8; 32]) -> Result<Vec<u8>, StorageError>;
    async fn unwrap(&self, wrapped: &[u8]) -> Result<Zeroizing<[u8; 32]>, StorageError>;
}

/// Local AES-256-GCM key-wrapping provider. For tests, and for operators who
/// hold their key in their own secret manager and inject it only into their
/// executors.
pub struct StaticKeyProvider {
    key_id: String,
    cipher: Aes256Gcm,
}

impl StaticKeyProvider {
    /// # Errors
    /// Returns an error unless `hex_key` is 64 hex characters.
    pub fn from_hex(key_id: impl Into<String>, hex_key: &str) -> Result<Self, StorageError> {
        let bytes: Vec<u8> = (0..hex_key.len())
            .step_by(2)
            .map(|i| {
                hex_key
                    .get(i..i + 2)
                    .and_then(|b| u8::from_str_radix(b, 16).ok())
            })
            .collect::<Option<_>>()
            .filter(|b: &Vec<u8>| b.len() == 32 && hex_key.len() == 64)
            .ok_or_else(|| vault_err("static key must be 64 hex characters"))?;
        let bytes = Zeroizing::new(bytes);
        Ok(Self {
            key_id: key_id.into(),
            cipher: Aes256Gcm::new_from_slice(&bytes)
                .map_err(|_| vault_err("invalid static key"))?,
        })
    }
}

#[async_trait]
impl KeyProvider for StaticKeyProvider {
    fn key_id(&self) -> &str {
        &self.key_id
    }

    async fn wrap(&self, dek: &[u8; 32]) -> Result<Vec<u8>, StorageError> {
        let nonce = Nonce::<Aes256Gcm>::generate();
        let sealed = self
            .cipher
            .encrypt(
                &nonce,
                Payload {
                    msg: dek,
                    aad: KMS_CONTEXT.as_bytes(),
                },
            )
            .map_err(|_| vault_err("DEK wrap failed"))?;
        let mut out = nonce.to_vec();
        out.extend_from_slice(&sealed);
        Ok(out)
    }

    async fn unwrap(&self, wrapped: &[u8]) -> Result<Zeroizing<[u8; 32]>, StorageError> {
        if wrapped.len() < 12 {
            return Err(vault_err("wrapped DEK is truncated"));
        }
        let (nonce, sealed) = wrapped.split_at(12);
        let nonce = Nonce::<Aes256Gcm>::try_from(nonce).map_err(|_| vault_err("bad DEK nonce"))?;
        let plain = Zeroizing::new(
            self.cipher
                .decrypt(
                    &nonce,
                    Payload {
                        msg: sealed,
                        aad: KMS_CONTEXT.as_bytes(),
                    },
                )
                .map_err(|_| vault_err("DEK unwrap failed (wrong key?)"))?,
        );
        let mut dek = Zeroizing::new([0u8; 32]);
        if plain.len() != 32 {
            return Err(vault_err("unwrapped DEK has the wrong length"));
        }
        dek.copy_from_slice(&plain);
        Ok(dek)
    }
}

/// Static AWS credentials for [`AwsKmsKeyProvider`].
#[derive(Clone)]
pub struct AwsCredentials {
    pub access_key_id: String,
    pub secret_access_key: String,
    pub session_token: Option<String>,
}

impl std::fmt::Debug for AwsCredentials {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AwsCredentials")
            .field("access_key_id", &self.access_key_id)
            .finish_non_exhaustive()
    }
}

/// AWS KMS (`TrentService.Encrypt` / `Decrypt`) over HTTPS, signed with
/// `SigV4`. The customer's key never leaves KMS; only DEKs are wrapped.
pub struct AwsKmsKeyProvider {
    key_arn: String,
    region: String,
    endpoint: String,
    credentials: AwsCredentials,
    http: reqwest::Client,
}

impl AwsKmsKeyProvider {
    /// `endpoint` overrides `https://kms.<region>.amazonaws.com` (VPC
    /// endpoints, `LocalStack`).
    ///
    /// # Errors
    /// Returns an error when the ARN does not name a region.
    pub fn new(
        key_arn: impl Into<String>,
        region: Option<String>,
        endpoint: Option<String>,
        credentials: AwsCredentials,
    ) -> Result<Self, StorageError> {
        let key_arn = key_arn.into();
        let region = region
            .filter(|r| !r.is_empty())
            .or_else(|| key_arn.split(':').nth(3).map(ToOwned::to_owned))
            .filter(|r| !r.is_empty())
            .ok_or_else(|| {
                vault_err("KMS key ARN must include a region (arn:aws:kms:<region>:…)")
            })?;
        let endpoint = endpoint
            .filter(|e| !e.is_empty())
            .unwrap_or_else(|| format!("https://kms.{region}.amazonaws.com"));
        let http = reqwest::Client::builder()
            .timeout(Duration::from_secs(10))
            .redirect(reqwest::redirect::Policy::none())
            .build()
            .map_err(|e| vault_err(e.to_string()))?;
        Ok(Self {
            key_arn,
            region,
            endpoint: endpoint.trim_end_matches('/').to_owned(),
            credentials,
            http,
        })
    }

    async fn call(&self, target: &str, body: &Value) -> Result<Value, StorageError> {
        let payload = serde_json::to_vec(body).map_err(StorageError::Serialization)?;
        let host = self
            .endpoint
            .split("://")
            .nth(1)
            .unwrap_or(&self.endpoint)
            .split('/')
            .next()
            .unwrap_or_default()
            .to_owned();
        let now = chrono::Utc::now();
        let amz_date = now.format("%Y%m%dT%H%M%SZ").to_string();
        let mut headers = vec![
            (
                "content-type".to_owned(),
                "application/x-amz-json-1.1".to_owned(),
            ),
            ("host".to_owned(), host),
            ("x-amz-date".to_owned(), amz_date.clone()),
            ("x-amz-target".to_owned(), format!("TrentService.{target}")),
        ];
        if let Some(token) = &self.credentials.session_token {
            headers.push(("x-amz-security-token".to_owned(), token.clone()));
        }
        let authorization = sigv4_authorization(&SigV4Request {
            method: "POST",
            path: "/",
            query: "",
            headers: &headers,
            payload: &payload,
            amz_date: &amz_date,
            region: &self.region,
            service: "kms",
            access_key_id: &self.credentials.access_key_id,
            secret_access_key: &self.credentials.secret_access_key,
        });
        let mut request = self.http.post(format!("{}/", self.endpoint)).body(payload);
        for (name, value) in &headers {
            if name != "host" {
                request = request.header(name.as_str(), value.as_str());
            }
        }
        let response = request
            .header("authorization", authorization)
            .send()
            .await
            .map_err(|e| StorageError::Connection(format!("KMS: {}", e.without_url())))?;
        let status = response.status();
        let text = response
            .text()
            .await
            .map_err(|e| StorageError::Connection(format!("KMS: {e}")))?;
        if !status.is_success() {
            let kind = serde_json::from_str::<Value>(&text)
                .ok()
                .and_then(|v| {
                    v.get("__type")
                        .and_then(Value::as_str)
                        .map(ToOwned::to_owned)
                })
                .unwrap_or_default();
            return Err(if status.is_server_error() || status.as_u16() == 429 {
                StorageError::Connection(format!("KMS {target} failed: HTTP {status} {kind}"))
            } else {
                vault_err(format!("KMS {target} refused: HTTP {status} {kind}"))
            });
        }
        serde_json::from_str(&text).map_err(StorageError::Serialization)
    }
}

#[async_trait]
impl KeyProvider for AwsKmsKeyProvider {
    fn key_id(&self) -> &str {
        &self.key_arn
    }

    async fn wrap(&self, dek: &[u8; 32]) -> Result<Vec<u8>, StorageError> {
        let response = self
            .call(
                "Encrypt",
                &json!({
                    "KeyId": self.key_arn,
                    "Plaintext": BASE64.encode(dek),
                    "EncryptionContext": { "orch8": KMS_CONTEXT },
                }),
            )
            .await?;
        response
            .get("CiphertextBlob")
            .and_then(Value::as_str)
            .and_then(|b| BASE64.decode(b).ok())
            .ok_or_else(|| vault_err("KMS Encrypt returned no CiphertextBlob"))
    }

    async fn unwrap(&self, wrapped: &[u8]) -> Result<Zeroizing<[u8; 32]>, StorageError> {
        let response = self
            .call(
                "Decrypt",
                &json!({
                    "KeyId": self.key_arn,
                    "CiphertextBlob": BASE64.encode(wrapped),
                    "EncryptionContext": { "orch8": KMS_CONTEXT },
                }),
            )
            .await?;
        let plain = Zeroizing::new(
            response
                .get("Plaintext")
                .and_then(Value::as_str)
                .and_then(|b| BASE64.decode(b).ok())
                .ok_or_else(|| vault_err("KMS Decrypt returned no Plaintext"))?,
        );
        if plain.len() != 32 {
            return Err(vault_err("KMS returned a DEK of the wrong length"));
        }
        let mut dek = Zeroizing::new([0u8; 32]);
        dek.copy_from_slice(&plain);
        Ok(dek)
    }
}

/// Inputs to [`sigv4_authorization`]. `headers` must be lowercase names and
/// include `host` and `x-amz-date`.
pub struct SigV4Request<'a> {
    pub method: &'a str,
    pub path: &'a str,
    pub query: &'a str,
    pub headers: &'a [(String, String)],
    pub payload: &'a [u8],
    pub amz_date: &'a str,
    pub region: &'a str,
    pub service: &'a str,
    pub access_key_id: &'a str,
    pub secret_access_key: &'a str,
}

fn hmac_sha256(key: &[u8], data: &[u8]) -> Vec<u8> {
    use hmac::{Hmac, KeyInit as _, Mac as _};
    let mut mac = <Hmac<Sha256>>::new_from_slice(key).expect("HMAC accepts any key length");
    mac.update(data);
    mac.finalize().into_bytes().to_vec()
}

/// AWS Signature Version 4 `Authorization` header value.
#[must_use]
pub fn sigv4_authorization(req: &SigV4Request<'_>) -> String {
    let mut headers: Vec<(String, String)> = req
        .headers
        .iter()
        .map(|(n, v)| (n.to_ascii_lowercase(), v.trim().to_owned()))
        .collect();
    headers.sort();
    let canonical_headers = headers.iter().fold(String::new(), |mut out, (n, v)| {
        use std::fmt::Write as _;
        let _ = writeln!(out, "{n}:{v}");
        out
    });
    let signed_headers = headers
        .iter()
        .map(|(n, _)| n.as_str())
        .collect::<Vec<_>>()
        .join(";");
    let canonical_request = format!(
        "{}\n{}\n{}\n{}\n{}\n{}",
        req.method,
        req.path,
        req.query,
        canonical_headers,
        signed_headers,
        hex(&Sha256::digest(req.payload))
    );
    let date = &req.amz_date[..8.min(req.amz_date.len())];
    let scope = format!("{date}/{}/{}/aws4_request", req.region, req.service);
    let string_to_sign = format!(
        "AWS4-HMAC-SHA256\n{}\n{scope}\n{}",
        req.amz_date,
        hex(&Sha256::digest(canonical_request.as_bytes()))
    );
    let k_date = hmac_sha256(
        format!("AWS4{}", req.secret_access_key).as_bytes(),
        date.as_bytes(),
    );
    let k_region = hmac_sha256(&k_date, req.region.as_bytes());
    let k_service = hmac_sha256(&k_region, req.service.as_bytes());
    let k_signing = hmac_sha256(&k_service, b"aws4_request");
    let signature = hex(&hmac_sha256(&k_signing, string_to_sign.as_bytes()));
    format!(
        "AWS4-HMAC-SHA256 Credential={}/{scope}, SignedHeaders={signed_headers}, Signature={signature}",
        req.access_key_id
    )
}

struct ActiveDek {
    dek: Zeroizing<[u8; 32]>,
    wrapped: Vec<u8>,
    created: Instant,
    uses: u32,
}

/// Customer-bucket payload vault.
pub struct PayloadVault {
    store: Arc<dyn ObjectStore>,
    prefix: String,
    provider: Arc<dyn KeyProvider>,
    active: tokio::sync::Mutex<Option<ActiveDek>>,
    unwrapped: moka::sync::Cache<Vec<u8>, Arc<Zeroizing<[u8; 32]>>>,
}

impl std::fmt::Debug for PayloadVault {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PayloadVault")
            .field("prefix", &self.prefix)
            .field("key_id", &self.provider.key_id())
            .finish_non_exhaustive()
    }
}

impl PayloadVault {
    #[must_use]
    pub fn new(store: Arc<dyn ObjectStore>, prefix: &str, provider: Arc<dyn KeyProvider>) -> Self {
        Self {
            store,
            prefix: prefix.trim_matches('/').to_owned(),
            provider,
            active: tokio::sync::Mutex::new(None),
            unwrapped: moka::sync::Cache::builder()
                .max_capacity(UNWRAP_CACHE_ENTRIES)
                .time_to_live(UNWRAP_CACHE_TTL)
                .build(),
        }
    }

    /// Vault over an S3-compatible bucket (the artifact store's builder).
    ///
    /// # Errors
    /// Client construction errors.
    pub fn s3(
        cfg: &crate::artifacts::S3Config,
        prefix: &str,
        provider: Arc<dyn KeyProvider>,
    ) -> Result<Self, StorageError> {
        // Without explicit keys, fall back to the standard AWS environment
        // chain (env vars, web identity / IRSA, instance metadata).
        let mut builder = if cfg.access_key_id.is_empty() {
            object_store::aws::AmazonS3Builder::from_env()
        } else {
            object_store::aws::AmazonS3Builder::new()
                .with_access_key_id(&cfg.access_key_id)
                .with_secret_access_key(&cfg.secret_access_key)
        }
        .with_bucket_name(&cfg.bucket)
        .with_allow_http(cfg.allow_http);
        if !cfg.region.is_empty() {
            builder = builder.with_region(&cfg.region);
        }
        if !cfg.endpoint.is_empty() {
            builder = builder.with_endpoint(&cfg.endpoint);
        }
        let store = builder.build().map_err(|e| vault_err(e.to_string()))?;
        Ok(Self::new(Arc::new(store), prefix, provider))
    }

    /// Vault on a local directory. Development and tests only.
    ///
    /// # Errors
    /// Directory creation / open errors.
    pub fn local(
        path: &str,
        prefix: &str,
        provider: Arc<dyn KeyProvider>,
    ) -> Result<Self, StorageError> {
        std::fs::create_dir_all(path).map_err(|e| vault_err(format!("local path {path}: {e}")))?;
        let fs = object_store::local::LocalFileSystem::new_with_prefix(path)
            .map_err(|e| vault_err(e.to_string()))?;
        Ok(Self::new(Arc::new(fs), prefix, provider))
    }

    fn aad(instance_id: InstanceId, ref_key: &str, object: &str) -> Vec<u8> {
        format!("orch8-vault-v1\u{0}{instance_id}\u{0}{ref_key}\u{0}{object}").into_bytes()
    }

    async fn current_dek(&self) -> Result<(Zeroizing<[u8; 32]>, Vec<u8>), StorageError> {
        let mut guard = self.active.lock().await;
        let fresh = guard
            .as_ref()
            .is_some_and(|a| a.created.elapsed() < DEK_MAX_AGE && a.uses < DEK_MAX_USES);
        if !fresh {
            let key = Key::<Aes256Gcm>::generate();
            let mut dek = Zeroizing::new([0u8; 32]);
            dek.copy_from_slice(&key);
            let wrapped = self.provider.wrap(&dek).await?;
            *guard = Some(ActiveDek {
                dek,
                wrapped,
                created: Instant::now(),
                uses: 0,
            });
        }
        let active = guard.as_mut().expect("initialized above");
        active.uses += 1;
        Ok((active.dek.clone(), active.wrapped.clone()))
    }

    async fn unwrap_cached(
        &self,
        wrapped: &[u8],
    ) -> Result<Arc<Zeroizing<[u8; 32]>>, StorageError> {
        if let Some(dek) = self.unwrapped.get(wrapped) {
            return Ok(dek);
        }
        let dek = Arc::new(self.provider.unwrap(wrapped).await?);
        self.unwrapped.insert(wrapped.to_vec(), Arc::clone(&dek));
        Ok(dek)
    }
}

#[async_trait]
impl ExternalPayloadVault for PayloadVault {
    async fn seal(
        &self,
        instance_id: InstanceId,
        ref_key: &str,
        value: &Value,
    ) -> Result<Value, StorageError> {
        let plaintext =
            Zeroizing::new(serde_json::to_vec(value).map_err(StorageError::Serialization)?);
        let (dek, wrapped) = self.current_dek().await?;
        let object = format!(
            "{}/{instance_id}/{}-{}",
            self.prefix,
            &hex(&Sha256::digest(ref_key.as_bytes()))[..32],
            uuid::Uuid::now_v7()
        );
        let cipher = Aes256Gcm::new_from_slice(dek.as_slice()).map_err(|_| vault_err("bad DEK"))?;
        let nonce = Nonce::<Aes256Gcm>::generate();
        let aad = Self::aad(instance_id, ref_key, &object);
        let sealed = cipher
            .encrypt(
                &nonce,
                Payload {
                    msg: &plaintext,
                    aad: &aad,
                },
            )
            .map_err(|_| vault_err("payload encryption failed"))?;
        let mut body = nonce.to_vec();
        body.extend_from_slice(&sealed);
        self.store
            .put(
                &Path::from(object.clone()),
                PutPayload::from_bytes(Bytes::from(body)),
            )
            .await
            .map_err(|e| StorageError::Backend(format!("payload vault put: {e}")))?;
        Ok(json!({
            VAULT_REF_KEY: {
                "v": 1,
                "object": object,
                "kid": self.provider.key_id(),
                "dek": BASE64.encode(&wrapped),
                "alg": "A256GCM",
            }
        }))
    }

    async fn open(
        &self,
        instance_id: InstanceId,
        ref_key: &str,
        reference: &Value,
    ) -> Result<Value, StorageError> {
        let inner = reference
            .get(VAULT_REF_KEY)
            .ok_or_else(|| vault_err("not a vault reference"))?;
        let object = inner
            .get("object")
            .and_then(Value::as_str)
            .ok_or_else(|| vault_err("reference has no object"))?;
        let wrapped = inner
            .get("dek")
            .and_then(Value::as_str)
            .and_then(|d| BASE64.decode(d).ok())
            .ok_or_else(|| vault_err("reference has no wrapped DEK"))?;
        let dek = self.unwrap_cached(&wrapped).await?;
        let body = self
            .store
            .get(&Path::from(object.to_owned()))
            .await
            .map_err(|e| StorageError::Backend(format!("payload vault get: {e}")))?
            .bytes()
            .await
            .map_err(|e| StorageError::Backend(format!("payload vault read: {e}")))?;
        if body.len() < 12 {
            return Err(vault_err("object is truncated"));
        }
        let (nonce, sealed) = body.split_at(12);
        let nonce =
            Nonce::<Aes256Gcm>::try_from(nonce).map_err(|_| vault_err("bad object nonce"))?;
        let cipher = Aes256Gcm::new_from_slice(dek.as_slice()).map_err(|_| vault_err("bad DEK"))?;
        let aad = Self::aad(instance_id, ref_key, object);
        let plain = Zeroizing::new(
            cipher
                .decrypt(
                    &nonce,
                    Payload {
                        msg: sealed,
                        aad: &aad,
                    },
                )
                .map_err(|_| {
                    vault_err("payload authentication failed (tampered, moved, or wrong key)")
                })?,
        );
        serde_json::from_slice(&plain).map_err(StorageError::Serialization)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn vault() -> (PayloadVault, Arc<dyn ObjectStore>) {
        let store: Arc<dyn ObjectStore> = Arc::new(object_store::memory::InMemory::new());
        let provider = Arc::new(StaticKeyProvider::from_hex("test-key", &"ab".repeat(32)).unwrap());
        (
            PayloadVault::new(Arc::clone(&store), "tenant-bucket", provider),
            store,
        )
    }

    #[tokio::test]
    async fn round_trip_and_reference_carries_no_plaintext() {
        let (vault, _) = vault();
        let instance = InstanceId::new();
        let value = json!({"ssn": "123-45-6789", "notes": "x".repeat(100)});
        let reference = vault
            .seal(instance, "k:ctx:data:pii", &value)
            .await
            .unwrap();
        let text = reference.to_string();
        assert!(!text.contains("123-45-6789"));
        assert!(crate::encrypting::is_vault_reference(&reference));
        assert_eq!(
            vault
                .open(instance, "k:ctx:data:pii", &reference)
                .await
                .unwrap(),
            value
        );
    }

    #[tokio::test]
    async fn reference_is_bound_to_instance_and_ref_key() {
        let (vault, _) = vault();
        let instance = InstanceId::new();
        let reference = vault.seal(instance, "a", &json!(1)).await.unwrap();
        assert!(
            vault
                .open(InstanceId::new(), "a", &reference)
                .await
                .is_err()
        );
        assert!(vault.open(instance, "b", &reference).await.is_err());
    }

    #[tokio::test]
    async fn another_key_cannot_open() {
        let (vault, store) = vault();
        let instance = InstanceId::new();
        let reference = vault.seal(instance, "a", &json!({"x": 1})).await.unwrap();
        let other = PayloadVault::new(
            store,
            "tenant-bucket",
            Arc::new(StaticKeyProvider::from_hex("other", &"cd".repeat(32)).unwrap()),
        );
        assert!(other.open(instance, "a", &reference).await.is_err());
    }

    #[tokio::test]
    async fn dek_is_reused_within_its_window() {
        let (vault, _) = vault();
        let instance = InstanceId::new();
        let a = vault.seal(instance, "a", &json!(1)).await.unwrap();
        let b = vault.seal(instance, "b", &json!(2)).await.unwrap();
        assert_eq!(a[VAULT_REF_KEY]["dek"], b[VAULT_REF_KEY]["dek"]);
        assert_ne!(a[VAULT_REF_KEY]["object"], b[VAULT_REF_KEY]["object"]);
    }

    #[tokio::test]
    async fn encrypting_storage_keeps_only_references_in_the_database() {
        use crate::ResourceStore as _;
        let raw = Arc::new(crate::sqlite::SqliteStorage::in_memory().await.unwrap());
        let encryptor =
            orch8_types::encryption::FieldEncryptor::from_hex_key(&"11".repeat(32)).unwrap();
        let (vault, _) = vault();
        let with_vault = crate::encrypting::EncryptingStorage::new(raw.clone(), encryptor.clone())
            .with_vault(Arc::new(vault));
        // A control plane with the same database but no vault access.
        let control_plane = crate::encrypting::EncryptingStorage::new(raw.clone(), encryptor);

        let seq: orch8_types::sequence::SequenceDefinition = serde_json::from_value(json!({
            "id": uuid::Uuid::now_v7(), "tenant_id": "t", "namespace": "default",
            "name": "s", "version": 1, "blocks": [], "created_at": chrono::Utc::now()
        }))
        .unwrap();
        crate::SequenceStore::create_sequence(&*raw, &seq)
            .await
            .unwrap();
        let instance = InstanceId::new();
        let now = chrono::Utc::now();
        let task: orch8_types::instance::TaskInstance = serde_json::from_value(json!({
            "id": instance, "sequence_id": seq.id, "tenant_id": "t", "namespace": "default",
            "state": "scheduled", "next_fire_at": now, "priority": "Normal", "timezone": "UTC",
            "metadata": {}, "context": {}, "created_at": now, "updated_at": now
        }))
        .unwrap();
        crate::InstanceStore::create_instance(&*raw, &task)
            .await
            .unwrap();
        let secret = json!({"card": "4111-1111-1111-1111"});
        with_vault
            .save_externalized_state(instance, "i:ctx:data:card", &secret)
            .await
            .unwrap();

        let stored = raw
            .get_externalized_state(instance, "i:ctx:data:card")
            .await
            .unwrap()
            .unwrap();
        assert!(
            crate::encrypting::is_vault_reference(&stored),
            "db holds {stored}"
        );
        assert!(!stored.to_string().contains("4111"));
        assert_eq!(
            with_vault
                .get_externalized_state(instance, "i:ctx:data:card")
                .await
                .unwrap()
                .unwrap(),
            secret
        );
        let seen = control_plane
            .get_externalized_state(instance, "i:ctx:data:card")
            .await
            .unwrap()
            .unwrap();
        assert!(
            crate::encrypting::is_vault_reference(&seen),
            "reference-only view: {seen}"
        );
    }

    /// AWS `SigV4` reference vector (IAM `ListUsers`, AWS General Reference).
    #[test]
    fn sigv4_matches_the_aws_reference_vector() {
        let headers = vec![
            (
                "content-type".to_owned(),
                "application/x-www-form-urlencoded; charset=utf-8".to_owned(),
            ),
            ("host".to_owned(), "iam.amazonaws.com".to_owned()),
            ("x-amz-date".to_owned(), "20150830T123600Z".to_owned()),
        ];
        let auth = sigv4_authorization(&SigV4Request {
            method: "GET",
            path: "/",
            query: "Action=ListUsers&Version=2010-05-08",
            headers: &headers,
            payload: b"",
            amz_date: "20150830T123600Z",
            region: "us-east-1",
            service: "iam",
            access_key_id: "AKIDEXAMPLE",
            secret_access_key: "wJalrXUtnFEMI/K7MDENG+bPxRfiCYEXAMPLEKEY",
        });
        assert_eq!(
            auth,
            "AWS4-HMAC-SHA256 Credential=AKIDEXAMPLE/20150830/us-east-1/iam/aws4_request, \
             SignedHeaders=content-type;host;x-amz-date, \
             Signature=5d672d79c15b13162d9279b0855cfba6789a8edb4c82c400e06b5924a6f2b5d7"
        );
    }
}
