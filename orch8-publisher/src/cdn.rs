//! CDN abstraction for publishing sequences and manifests.

use std::time::{SystemTime, UNIX_EPOCH};

use hmac::{Hmac, KeyInit, Mac};
use reqwest::header::HeaderValue;
use sha2::{Digest, Sha256};
use tracing::debug;

type HmacSha256 = Hmac<Sha256>;

/// `SigV4` URI-encode a single path segment or query key/value: percent-encode
/// every byte except unreserved characters (`A-Z a-z 0-9 - . _ ~`), per the
/// AWS Signature Version 4 spec (`UriEncode`).
fn sigv4_uri_encode(s: &str) -> String {
    use std::fmt::Write;
    let mut out = String::with_capacity(s.len());
    for b in s.bytes() {
        let c = b as char;
        if c.is_ascii_alphanumeric() || matches!(c, '-' | '.' | '_' | '~') {
            out.push(c);
        } else {
            out.push('%');
            write!(out, "{b:02X}").unwrap();
        }
    }
    out
}

/// `SigV4` canonical URI: percent-encode each path segment individually,
/// leaving the `/` separators alone (M-22 fix — previously the raw,
/// unencoded `url.path()` was used, so any reserved character in the path
/// broke the signature's byte-for-byte match with what S3 recomputes).
fn canonical_uri(path: &str) -> String {
    path.split('/')
        .map(sigv4_uri_encode)
        .collect::<Vec<_>>()
        .join("/")
}

/// `SigV4` canonical query string: percent-encode each key/value, then sort
/// the pairs by (encoded key, encoded value) in byte order (M-22 fix —
/// previously the raw, unsorted `url.query()` was used, so a request with
/// out-of-order or unencoded query params produced a signature S3 would
/// reject).
fn canonical_query(query: Option<&str>) -> String {
    let Some(q) = query.filter(|q| !q.is_empty()) else {
        return String::new();
    };
    let mut pairs: Vec<(String, String)> = q
        .split('&')
        .map(|kv| {
            let mut it = kv.splitn(2, '=');
            let k = it.next().unwrap_or("");
            let v = it.next().unwrap_or("");
            (sigv4_uri_encode(k), sigv4_uri_encode(v))
        })
        .collect();
    pairs.sort();
    pairs
        .into_iter()
        .map(|(k, v)| format!("{k}={v}"))
        .collect::<Vec<_>>()
        .join("&")
}

/// Trait for CDN backends (S3, R2, GCS, etc.).
#[async_trait::async_trait]
pub trait CdnBackend: Send + Sync {
    /// Upload bytes to `path` with the given `content_type` and `cache_control`.
    async fn upload(
        &self,
        path: &str,
        bytes: Vec<u8>,
        content_type: Option<&str>,
        cache_control: Option<&str>,
    ) -> Result<(), CdnError>;

    /// Delete the object at `path`.
    async fn delete(&self, path: &str) -> Result<(), CdnError>;

    /// Get the current `ETag` for the object at `path`.
    async fn get_etag(&self, path: &str) -> Result<Option<String>, CdnError>;
}

#[derive(Debug, thiserror::Error)]
pub enum CdnError {
    #[error("upload failed: {0}")]
    Upload(String),
    #[error("delete failed: {0}")]
    Delete(String),
    #[error("etag fetch failed: {0}")]
    Etag(String),
    #[error("request signing failed: {0}")]
    Signing(String),
    #[error("optimistic concurrency conflict")]
    Conflict,
}

/// Total attempts for a single CDN operation (1 initial + 2 retries).
/// Upload/delete are idempotent (content-addressed PUT, DELETE, HEAD), so
/// retrying transient failures is safe.
const MAX_ATTEMPTS: u32 = 3;

/// Send a request, retrying transient failures (5xx responses, connect and
/// timeout errors) with a short linear backoff. Returns the final response —
/// including non-5xx error statuses, which the caller maps to an error — or
/// the last transport error.
async fn send_with_retry(
    request: reqwest::RequestBuilder,
) -> Result<reqwest::Response, reqwest::Error> {
    let mut attempt = 0u32;
    loop {
        attempt += 1;
        let Some(req) = request.try_clone() else {
            // Non-cloneable (streaming) body: single attempt, no retry.
            return request.send().await;
        };
        match req.send().await {
            Ok(resp) if resp.status().is_server_error() && attempt < MAX_ATTEMPTS => {
                debug!(status = %resp.status(), attempt, "transient 5xx from CDN, retrying");
            }
            Err(e) if (e.is_connect() || e.is_timeout()) && attempt < MAX_ATTEMPTS => {
                debug!(error = %e, attempt, "transient connection error, retrying");
            }
            other => return other,
        }
        tokio::time::sleep(std::time::Duration::from_millis(100 * u64::from(attempt))).await;
    }
}

/// S3-compatible CDN backend.
pub struct S3CdnBackend {
    endpoint: String,
    bucket: String,
    region: String,
    access_key: String,
    secret_key: String,
    http: reqwest::Client,
}

impl Drop for S3CdnBackend {
    fn drop(&mut self) {
        use zeroize::Zeroize;
        self.secret_key.zeroize();
    }
}

impl S3CdnBackend {
    pub fn new(
        endpoint: String,
        bucket: String,
        region: String,
        access_key: String,
        secret_key: String,
    ) -> Self {
        // The builder only uses constants, so failure is a programming error.
        #[allow(clippy::expect_used)]
        let http = reqwest::Client::builder()
            .timeout(std::time::Duration::from_secs(30))
            .build()
            .expect("reqwest client builds");
        Self {
            endpoint,
            bucket,
            region,
            access_key,
            secret_key,
            http,
        }
    }

    fn url(&self, path: &str) -> String {
        format!(
            "{}/{}/{}",
            self.endpoint.trim_end_matches('/'),
            self.bucket,
            path.trim_start_matches('/')
        )
    }

    fn sign_request(
        &self,
        method: &str,
        url: &reqwest::Url,
        raw_path: &str,
        headers: &mut reqwest::header::HeaderMap,
        payload_hash: &str,
    ) -> Result<(), CdnError> {
        let timestamp = signing_timestamp(SystemTime::now())?;
        let date_stamp = format_date(timestamp);
        let amz_date = format_amz_date(timestamp);

        headers.insert(
            "x-amz-date",
            HeaderValue::from_str(&amz_date).map_err(|e| CdnError::Signing(e.to_string()))?,
        );
        headers.insert(
            "x-amz-content-sha256",
            HeaderValue::from_str(payload_hash).map_err(|e| CdnError::Signing(e.to_string()))?,
        );

        let host = url.host_str().unwrap_or("");
        headers.insert(
            "host",
            HeaderValue::from_str(host).map_err(|e| CdnError::Signing(e.to_string()))?,
        );

        let content_type = headers
            .get("content-type")
            .and_then(|v| v.to_str().ok())
            .unwrap_or("")
            .to_string();
        let (canonical_headers, signed_headers) = if content_type.is_empty() {
            (
                format!(
                    "host:{host}\nx-amz-content-sha256:{payload_hash}\nx-amz-date:{amz_date}\n"
                ),
                "host;x-amz-content-sha256;x-amz-date".to_string(),
            )
        } else {
            (
                format!(
                    "content-type:{content_type}\nhost:{host}\nx-amz-content-sha256:{payload_hash}\nx-amz-date:{amz_date}\n"
                ),
                "content-type;host;x-amz-content-sha256;x-amz-date".to_string(),
            )
        };

        // SigV4's canonical URI must be the *single*-encoded form of the raw
        // path. `url.path()` is already percent-encoded by the `url` crate, so
        // feeding it through `canonical_uri` would double-encode (`%20` ->
        // `%2520`) while the request line keeps the single-encoded form and
        // S3 rejects the signature. Rebuild the canonical URI from the raw
        // object path instead.
        let canonical_request = format!(
            "{method}\n{uri}\n{query_string}\n{canonical_headers}\n{signed_headers}\n{payload_hash}",
            uri = canonical_uri(&format!(
                "/{}/{}",
                self.bucket,
                raw_path.trim_start_matches('/')
            )),
            query_string = canonical_query(url.query()),
        );

        let scope = format!("{date_stamp}/{}/s3/aws4_request", self.region);
        let string_to_sign = format!(
            "AWS4-HMAC-SHA256\n{amz_date}\n{scope}\n{hashed_canonical_request}",
            hashed_canonical_request = hex::encode(Sha256::digest(canonical_request.as_bytes())),
        );

        let signing_key = get_signing_key(&self.secret_key, &date_stamp, &self.region, "s3");
        let signature = hex::encode(hmac_sha256(&signing_key, string_to_sign.as_bytes()));

        let auth_header = format!(
            "AWS4-HMAC-SHA256 Credential={access_key}/{scope},SignedHeaders={signed_headers},Signature={signature}",
            access_key = self.access_key,
        );
        headers.insert(
            "authorization",
            HeaderValue::from_str(&auth_header).map_err(|e| CdnError::Signing(e.to_string()))?,
        );

        Ok(())
    }
}

#[async_trait::async_trait]
impl CdnBackend for S3CdnBackend {
    async fn upload(
        &self,
        path: &str,
        bytes: Vec<u8>,
        content_type: Option<&str>,
        cache_control: Option<&str>,
    ) -> Result<(), CdnError> {
        let url_str = self.url(path);
        let url = reqwest::Url::parse(&url_str).map_err(|e| CdnError::Upload(e.to_string()))?;
        let payload_hash = hex::encode(Sha256::digest(&bytes));

        let mut headers = reqwest::header::HeaderMap::new();
        if let Some(ct) = content_type {
            headers.insert(
                "content-type",
                HeaderValue::from_str(ct).map_err(|e| CdnError::Upload(e.to_string()))?,
            );
        }
        if let Some(cc) = cache_control {
            headers.insert(
                "cache-control",
                HeaderValue::from_str(cc).map_err(|e| CdnError::Upload(e.to_string()))?,
            );
        }
        self.sign_request("PUT", &url, path, &mut headers, &payload_hash)?;

        let response = send_with_retry(self.http.put(url).headers(headers).body(bytes))
            .await
            .map_err(|e| CdnError::Upload(e.to_string()))?;

        if !response.status().is_success() {
            let body = response.text().await.unwrap_or_default();
            return Err(CdnError::Upload(format!("S3 PUT failed: {body}")));
        }
        debug!(path = %path, "uploaded to S3");
        Ok(())
    }

    async fn delete(&self, path: &str) -> Result<(), CdnError> {
        let url_str = self.url(path);
        let url = reqwest::Url::parse(&url_str).map_err(|e| CdnError::Delete(e.to_string()))?;
        let empty_hash = hex::encode(Sha256::digest(b""));

        let mut headers = reqwest::header::HeaderMap::new();
        self.sign_request("DELETE", &url, path, &mut headers, &empty_hash)?;

        let response = send_with_retry(self.http.delete(url).headers(headers))
            .await
            .map_err(|e| CdnError::Delete(e.to_string()))?;

        if !response.status().is_success() {
            let body = response.text().await.unwrap_or_default();
            return Err(CdnError::Delete(format!("S3 DELETE failed: {body}")));
        }
        debug!(path = %path, "deleted from S3");
        Ok(())
    }

    async fn get_etag(&self, path: &str) -> Result<Option<String>, CdnError> {
        let url_str = self.url(path);
        let url = reqwest::Url::parse(&url_str).map_err(|e| CdnError::Etag(e.to_string()))?;
        let empty_hash = hex::encode(Sha256::digest(b""));

        let mut headers = reqwest::header::HeaderMap::new();
        self.sign_request("HEAD", &url, path, &mut headers, &empty_hash)?;

        let response = send_with_retry(self.http.head(url).headers(headers))
            .await
            .map_err(|e| CdnError::Etag(e.to_string()))?;

        if response.status().is_success() {
            Ok(response
                .headers()
                .get("etag")
                .and_then(|v| v.to_str().ok().map(String::from)))
        } else if response.status() == reqwest::StatusCode::NOT_FOUND {
            Ok(None)
        } else {
            Err(CdnError::Etag(format!(
                "S3 HEAD returned {}",
                response.status()
            )))
        }
    }
}

fn format_date(timestamp: u64) -> String {
    let secs = i64::try_from(timestamp).unwrap_or(i64::MAX);
    let dt = chrono::DateTime::from_timestamp(secs, 0).unwrap_or_else(chrono::Utc::now);
    dt.format("%Y%m%d").to_string()
}

fn signing_timestamp(now: SystemTime) -> Result<u64, CdnError> {
    now.duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_secs())
        .map_err(|_| CdnError::Signing("system clock is before UNIX epoch".to_string()))
}

fn format_amz_date(timestamp: u64) -> String {
    let secs = i64::try_from(timestamp).unwrap_or(i64::MAX);
    let dt = chrono::DateTime::from_timestamp(secs, 0).unwrap_or_else(chrono::Utc::now);
    dt.format("%Y%m%dT%H%M%SZ").to_string()
}

fn hmac_sha256(key: &[u8], data: &[u8]) -> Vec<u8> {
    let mut mac = HmacSha256::new_from_slice(key)
        .unwrap_or_else(|_| unreachable!("HmacSha256 accepts keys of any length"));
    mac.update(data);
    mac.finalize().into_bytes().to_vec()
}

fn get_signing_key(secret: &str, date_stamp: &str, region: &str, service: &str) -> Vec<u8> {
    let k_date = hmac_sha256(format!("AWS4{secret}").as_bytes(), date_stamp.as_bytes());
    let k_region = hmac_sha256(&k_date, region.as_bytes());
    let k_service = hmac_sha256(&k_region, service.as_bytes());
    hmac_sha256(&k_service, b"aws4_request")
}

type CdnStore = std::collections::HashMap<String, (Vec<u8>, Option<String>)>;

/// In-memory CDN backend for testing.
pub struct MemoryCdnBackend {
    pub store: tokio::sync::Mutex<CdnStore>,
}

impl Default for MemoryCdnBackend {
    fn default() -> Self {
        Self::new()
    }
}

impl MemoryCdnBackend {
    pub fn new() -> Self {
        Self {
            store: tokio::sync::Mutex::new(std::collections::HashMap::new()),
        }
    }
}

#[async_trait::async_trait]
impl CdnBackend for MemoryCdnBackend {
    async fn upload(
        &self,
        path: &str,
        bytes: Vec<u8>,
        _content_type: Option<&str>,
        cache_control: Option<&str>,
    ) -> Result<(), CdnError> {
        let mut store = self.store.lock().await;
        store.insert(path.to_string(), (bytes, cache_control.map(String::from)));
        Ok(())
    }

    async fn delete(&self, path: &str) -> Result<(), CdnError> {
        let mut store = self.store.lock().await;
        store.remove(path);
        Ok(())
    }

    async fn get_etag(&self, path: &str) -> Result<Option<String>, CdnError> {
        let store = self.store.lock().await;
        Ok(store
            .get(path)
            .map(|(bytes, _)| format!("\"{}\"", hex::encode(Sha256::digest(bytes)))))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn memory_backend_roundtrip() {
        let backend = MemoryCdnBackend::new();
        backend
            .upload(
                "test.txt",
                b"hello".to_vec(),
                Some("text/plain"),
                Some("max-age=3600"),
            )
            .await
            .unwrap();
        let etag = backend.get_etag("test.txt").await.unwrap();
        assert!(etag.is_some());
        backend.delete("test.txt").await.unwrap();
        let etag = backend.get_etag("test.txt").await.unwrap();
        assert!(etag.is_none());
    }

    // M-22: reserved characters in a path segment must be percent-encoded
    // per the SigV4 spec, not passed through raw.
    #[test]
    fn canonical_uri_percent_encodes_reserved_characters() {
        assert_eq!(
            canonical_uri("/manifests/v1 (draft).json"),
            "/manifests/v1%20%28draft%29.json"
        );
        assert_eq!(canonical_uri("/a/b/c"), "/a/b/c");
        assert_eq!(canonical_uri("/unreserved-._~ok"), "/unreserved-._~ok");
    }

    // M-22: query parameters must be sorted by key (then value) — an
    // out-of-order query string must canonicalize identically regardless of
    // the order the caller happened to build it in.
    #[test]
    fn canonical_query_sorts_out_of_order_params() {
        assert_eq!(
            canonical_query(Some("b=2&a=1")),
            canonical_query(Some("a=1&b=2")),
        );
        assert_eq!(canonical_query(Some("b=2&a=1")), "a=1&b=2");
    }

    // M-22: repeated keys with different values must be sorted by value too,
    // not left in encounter order.
    #[test]
    fn canonical_query_sorts_repeated_keys_by_value() {
        assert_eq!(canonical_query(Some("k=2&k=1")), "k=1&k=2");
    }

    #[test]
    fn canonical_query_percent_encodes_keys_and_values() {
        assert_eq!(canonical_query(Some("a b=c d")), "a%20b=c%20d");
    }

    #[test]
    fn canonical_query_empty_or_none_is_empty_string() {
        assert_eq!(canonical_query(None), "");
        assert_eq!(canonical_query(Some("")), "");
    }

    #[test]
    fn signing_key_deterministic() {
        let key1 = get_signing_key("secret", "20260101", "us-east-1", "s3");
        let key2 = get_signing_key("secret", "20260101", "us-east-1", "s3");
        assert_eq!(key1, key2);
    }

    #[test]
    fn signing_timestamp_rejects_pre_epoch_clock() {
        let before_epoch = UNIX_EPOCH - std::time::Duration::from_secs(1);
        let err = signing_timestamp(before_epoch).unwrap_err();
        assert!(matches!(err, CdnError::Signing(message) if message.contains("before UNIX epoch")));
    }

    #[test]
    fn signing_timestamp_accepts_epoch_and_later() {
        assert_eq!(signing_timestamp(UNIX_EPOCH).unwrap(), 0);
        assert_eq!(
            signing_timestamp(UNIX_EPOCH + std::time::Duration::from_secs(42)).unwrap(),
            42
        );
    }

    // Regression: the canonical URI must be single-encoded from the raw
    // object path. `url.path()` is already percent-encoded by the `url`
    // crate; re-encoding it turned `%20` into `%2520` while the request line
    // kept the single-encoded form, so S3 rejected the signature.
    #[test]
    fn sign_request_single_encodes_reserved_path_characters() {
        let backend = S3CdnBackend::new(
            "https://s3.example.com".to_string(),
            "bucket".to_string(),
            "us-east-1".to_string(),
            "AKID".to_string(),
            "secret".to_string(),
        );
        let raw_path = "tenant x/sequences/v1 (draft).json";
        let url = reqwest::Url::parse(&backend.url(raw_path)).unwrap();
        let payload_hash = hex::encode(Sha256::digest(b""));
        let mut headers = reqwest::header::HeaderMap::new();
        backend
            .sign_request("PUT", &url, raw_path, &mut headers, &payload_hash)
            .unwrap();

        // Recompute the expected signature independently, using the
        // single-encoded canonical URI.
        let amz_date = headers
            .get("x-amz-date")
            .unwrap()
            .to_str()
            .unwrap()
            .to_string();
        let date_stamp = &amz_date[..8];
        let canonical_headers = format!(
            "host:s3.example.com\nx-amz-content-sha256:{payload_hash}\nx-amz-date:{amz_date}\n"
        );
        let canonical_request = format!(
            "PUT\n/bucket/tenant%20x/sequences/v1%20%28draft%29.json\n\n{canonical_headers}\nhost;x-amz-content-sha256;x-amz-date\n{payload_hash}"
        );
        let scope = format!("{date_stamp}/us-east-1/s3/aws4_request");
        let string_to_sign = format!(
            "AWS4-HMAC-SHA256\n{amz_date}\n{scope}\n{}",
            hex::encode(Sha256::digest(canonical_request.as_bytes()))
        );
        let signing_key = get_signing_key("secret", date_stamp, "us-east-1", "s3");
        let expected_signature = hex::encode(hmac_sha256(&signing_key, string_to_sign.as_bytes()));

        let auth = headers.get("authorization").unwrap().to_str().unwrap();
        assert!(
            auth.contains(&format!("Signature={expected_signature}")),
            "authorization header used a different canonical request: {auth}"
        );
    }
}
