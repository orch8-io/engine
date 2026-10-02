//! Startup wiring for the opt-in federation transport, the BYOK payload
//! vault, and active-passive region fencing. Everything here is off unless
//! its environment variables are set.

use std::sync::Arc;
use std::time::Duration;

use anyhow::Context;

use orch8_engine::federation::FederationClient;
use orch8_storage::artifacts::S3Config;
use orch8_storage::encrypting::ExternalPayloadVault;
use orch8_storage::vault::{
    AwsCredentials, AwsKmsKeyProvider, KeyProvider, PayloadVault, StaticKeyProvider,
};

fn env(name: &str) -> Option<String> {
    std::env::var(name)
        .ok()
        .map(|v| v.trim().to_owned())
        .filter(|v| !v.is_empty())
}

fn env_flag(name: &str) -> bool {
    env(name).is_some_and(|v| matches!(v.as_str(), "1" | "true" | "yes"))
}

/// Build the BYOK vault from `ORCH8_BYOK_*`. `None` when not configured.
///
/// Bucket: `ORCH8_BYOK_BUCKET` (S3-compatible) or `ORCH8_BYOK_LOCAL_PATH`
/// (filesystem, development only), `ORCH8_BYOK_PREFIX` (default `orch8`),
/// `ORCH8_BYOK_REGION`, `ORCH8_BYOK_ENDPOINT`, `ORCH8_BYOK_ACCESS_KEY_ID` /
/// `ORCH8_BYOK_SECRET_ACCESS_KEY` (else the AWS default chain),
/// `ORCH8_BYOK_ALLOW_HTTP`.
///
/// Key: `ORCH8_BYOK_KMS_KEY_ARN` (+ `ORCH8_BYOK_KMS_ENDPOINT`, credentials
/// from `AWS_ACCESS_KEY_ID` / `AWS_SECRET_ACCESS_KEY` / `AWS_SESSION_TOKEN`)
/// or `ORCH8_BYOK_STATIC_KEY` (64 hex) + `ORCH8_BYOK_STATIC_KEY_ID`.
///
/// # Errors
/// A partially configured vault fails startup rather than silently storing
/// payloads in the engine database.
pub fn payload_vault_from_env() -> anyhow::Result<Option<Arc<dyn ExternalPayloadVault>>> {
    let bucket = env("ORCH8_BYOK_BUCKET");
    let local = env("ORCH8_BYOK_LOCAL_PATH");
    if bucket.is_none() && local.is_none() {
        if env("ORCH8_BYOK_KMS_KEY_ARN").is_some() || env("ORCH8_BYOK_STATIC_KEY").is_some() {
            anyhow::bail!(
                "a BYOK key is configured but no ORCH8_BYOK_BUCKET / ORCH8_BYOK_LOCAL_PATH"
            );
        }
        return Ok(None);
    }
    let provider: Arc<dyn KeyProvider> = if let Some(arn) = env("ORCH8_BYOK_KMS_KEY_ARN") {
        let credentials = AwsCredentials {
            access_key_id: env("AWS_ACCESS_KEY_ID")
                .context("ORCH8_BYOK_KMS_KEY_ARN requires AWS_ACCESS_KEY_ID")?,
            secret_access_key: env("AWS_SECRET_ACCESS_KEY")
                .context("ORCH8_BYOK_KMS_KEY_ARN requires AWS_SECRET_ACCESS_KEY")?,
            session_token: env("AWS_SESSION_TOKEN"),
        };
        Arc::new(AwsKmsKeyProvider::new(
            arn,
            env("ORCH8_BYOK_KMS_REGION"),
            env("ORCH8_BYOK_KMS_ENDPOINT"),
            credentials,
        )?)
    } else if let Some(key) = env("ORCH8_BYOK_STATIC_KEY") {
        Arc::new(StaticKeyProvider::from_hex(
            env("ORCH8_BYOK_STATIC_KEY_ID").unwrap_or_else(|| "static".into()),
            &key,
        )?)
    } else {
        anyhow::bail!(
            "BYOK bucket configured without ORCH8_BYOK_KMS_KEY_ARN or ORCH8_BYOK_STATIC_KEY"
        );
    };
    let prefix = env("ORCH8_BYOK_PREFIX").unwrap_or_else(|| "orch8".into());
    let vault = if let Some(bucket) = bucket {
        PayloadVault::s3(
            &S3Config {
                bucket,
                region: env("ORCH8_BYOK_REGION").unwrap_or_default(),
                endpoint: env("ORCH8_BYOK_ENDPOINT").unwrap_or_default(),
                access_key_id: env("ORCH8_BYOK_ACCESS_KEY_ID").unwrap_or_default(),
                secret_access_key: env("ORCH8_BYOK_SECRET_ACCESS_KEY").unwrap_or_default(),
                allow_http: env_flag("ORCH8_BYOK_ALLOW_HTTP"),
            },
            &prefix,
            provider,
        )?
    } else {
        let path = local.unwrap_or_default();
        tracing::warn!(%path, "BYOK vault on the local filesystem — development only");
        PayloadVault::local(&path, &prefix, provider)?
    };
    tracing::info!(
        ?vault,
        "BYOK payload vault enabled: externalized payloads leave the engine database"
    );
    Ok(Some(Arc::new(vault)))
}

/// Outbound federation client, available whenever the engine has a signing
/// key. The client only acts on peers an operator registered.
pub fn federation_client(
    crypto: Option<&orch8_api::ContinuityCrypto>,
) -> Option<Arc<FederationClient>> {
    let crypto = crypto?;
    match FederationClient::new(
        crypto.signing_key.clone(),
        orch8_api::federation::allow_http_peers(),
    ) {
        Ok(client) => Some(Arc::new(client)),
        Err(error) => {
            tracing::error!(%error, "federation client unavailable");
            None
        }
    }
}

/// Active-passive fence settings (`ORCH8_FAILOVER_REGION`,
/// `ORCH8_FAILOVER_POLL_SECS`, default 5).
#[derive(Debug, Clone)]
pub struct FailoverSettings {
    pub region: String,
    pub poll: Duration,
    /// Fail closed after this long without a successful fence read.
    pub blind_window: Duration,
}

/// # Errors
/// Invalid region name or poll interval.
pub fn failover_from_env() -> anyhow::Result<Option<FailoverSettings>> {
    let Some(region) = env("ORCH8_FAILOVER_REGION") else {
        return Ok(None);
    };
    if !orch8_types::federation::is_valid_region(&region) {
        anyhow::bail!("ORCH8_FAILOVER_REGION must be 1-64 of [a-z0-9-]");
    }
    let poll_secs = match env("ORCH8_FAILOVER_POLL_SECS") {
        None => 5,
        Some(v) => v
            .parse::<u64>()
            .ok()
            .filter(|v| (1..=60).contains(v))
            .context("ORCH8_FAILOVER_POLL_SECS must be 1-60")?,
    };
    Ok(Some(FailoverSettings {
        region,
        poll: Duration::from_secs(poll_secs),
        blind_window: Duration::from_secs(poll_secs * 3),
    }))
}
