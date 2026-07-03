//! Sequence publishing: serialize → hash → sign → upload to CDN.

use base64::{engine::general_purpose::STANDARD as BASE64, Engine as _};
use ed25519_dalek::Signer;
use sha2::{Digest, Sha256};
use tracing::info;

use orch8_types::sequence::SequenceDefinition;

use crate::cdn::{CdnBackend, CdnError};
use crate::manifest::{self, ManifestGenerator, ManifestSequence, ManifestSigningKey};

/// Publishes sequences to a CDN backend.
pub struct SequencePublisher {
    cdn: Box<dyn CdnBackend>,
    manifest_gen: ManifestGenerator,
    tenant_id: String,
    signing_key_id: String,
    min_sdk_version: String,
}

fn validate_tenant_id(tenant_id: &str) -> Result<(), PublishError> {
    if tenant_id.is_empty() {
        return Err(PublishError::Config("tenant_id cannot be empty".into()));
    }
    if tenant_id.contains('/') || tenant_id.contains('\\') || tenant_id.contains("..") {
        return Err(PublishError::Config(format!(
            "tenant_id contains path metacharacters: {tenant_id}"
        )));
    }
    Ok(())
}

impl SequencePublisher {
    pub fn new(
        cdn: Box<dyn CdnBackend>,
        manifest_gen: ManifestGenerator,
        tenant_id: String,
        signing_key_id: String,
    ) -> Result<Self, PublishError> {
        validate_tenant_id(&tenant_id)?;
        Ok(Self {
            cdn,
            manifest_gen,
            tenant_id,
            signing_key_id,
            min_sdk_version: env!("CARGO_PKG_VERSION").to_string(),
        })
    }

    #[must_use]
    pub fn with_min_sdk_version(mut self, version: String) -> Self {
        self.min_sdk_version = version;
        self
    }

    /// Publish a single sequence: upload JSON, return manifest entry.
    pub async fn publish_sequence(
        &self,
        seq: &SequenceDefinition,
        signing_key: &ed25519_dalek::SigningKey,
    ) -> Result<ManifestSequence, PublishError> {
        if seq.tenant_id.as_str() != self.tenant_id {
            return Err(PublishError::Config(format!(
                "sequence tenant {} does not match publisher tenant {}",
                seq.tenant_id.as_str(),
                self.tenant_id
            )));
        }

        let json = manifest::canonical_json(seq)
            .map_err(|e| PublishError::Serialization(e.to_string()))?;
        let hash = hex::encode(Sha256::digest(&json));
        let signature = signing_key.sign(json.as_bytes());
        let sig_b64 = BASE64.encode(signature.to_bytes());

        // Content-addressed path.
        let path = format!("{}/sequences/{}.json", self.tenant_id, hash);

        // Upload sequence JSON.
        self.cdn
            .upload(
                &path,
                json.into_bytes(),
                Some("application/json"),
                Some("immutable, max-age=31536000"),
            )
            .await
            .map_err(PublishError::Cdn)?;

        // Upload detached signature.
        let sig_path = format!("{}/sequences/{}.sig", self.tenant_id, hash);
        self.cdn
            .upload(
                &sig_path,
                sig_b64.into_bytes(),
                Some("application/octet-stream"),
                Some("immutable, max-age=31536000"),
            )
            .await
            .map_err(PublishError::Cdn)?;

        let mut required_handlers: Vec<String> = seq
            .blocks
            .iter()
            .filter_map(|b| {
                if let orch8_types::sequence::BlockDefinition::Step(s) = b {
                    Some(s.handler.clone())
                } else {
                    None
                }
            })
            .collect();
        required_handlers.sort_unstable();
        required_handlers.dedup();

        info!(name = %seq.name, version = seq.version, hash = %hash, "published sequence");

        Ok(ManifestSequence {
            name: seq.name.clone(),
            version: seq.version,
            url: format!("/{path}"),
            signing_key_id: self.signing_key_id.clone(),
            sha256: hash,
            required_handlers,
            min_sdk_version: self.min_sdk_version.clone(),
        })
    }

    /// Regenerate and upload the manifest.
    pub async fn publish_manifest(
        &self,
        sequences: Vec<ManifestSequence>,
        removed: Vec<crate::manifest::ManifestRemoved>,
        other_keys: Vec<ManifestSigningKey>,
    ) -> Result<(), PublishError> {
        // Trust-boundary checks: every sequence URL must live under this
        // publisher's tenant prefix, and no caller-supplied key may shadow the
        // generator's own key_id.
        let prefix = format!("/{}/sequences/", self.tenant_id);
        for seq in &sequences {
            if !seq.url.starts_with(&prefix) {
                return Err(PublishError::Config(format!(
                    "sequence url {} is outside tenant {}",
                    seq.url, self.tenant_id
                )));
            }
        }
        for key in &other_keys {
            if key.key_id == self.signing_key_id {
                return Err(PublishError::Config(format!(
                    "other_keys contains the generator's key_id {}",
                    self.signing_key_id
                )));
            }
        }

        let signed = self
            .manifest_gen
            .generate(sequences, removed, other_keys)
            .map_err(|e| PublishError::Serialization(e.to_string()))?;

        let manifest_bytes =
            format!("{}\n{}", signed.signature_b64, signed.canonical_json).into_bytes();

        let path = format!("{}/manifest.json", self.tenant_id);
        self.cdn
            .upload(
                &path,
                manifest_bytes,
                Some("application/json"),
                Some("max-age=60"),
            )
            .await
            .map_err(PublishError::Cdn)?;

        info!(tenant = %self.tenant_id, "published manifest");
        Ok(())
    }
}

#[derive(Debug, thiserror::Error)]
pub enum PublishError {
    #[error("serialization failed: {0}")]
    Serialization(String),
    #[error("CDN error: {0}")]
    Cdn(#[from] CdnError),
    #[error("configuration error: {0}")]
    Config(String),
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cdn::MemoryCdnBackend;
    use crate::manifest::ManifestGenerator;
    use ed25519_dalek::SigningKey;
    use orch8_types::sequence::SequenceStatus;
    use rand_core::OsRng;

    fn setup() -> (SequencePublisher, SigningKey) {
        let cdn = Box::new(MemoryCdnBackend::new());
        let signing_key = SigningKey::generate(&mut OsRng);
        let manifest_gen = ManifestGenerator::new(signing_key.clone(), "key1".to_string());
        let publisher =
            SequencePublisher::new(cdn, manifest_gen, "tenant1".to_string(), "key1".to_string())
                .expect("valid tenant_id");
        (publisher, signing_key)
    }

    #[tokio::test]
    async fn publish_sequence_creates_entry() {
        let (publisher, key) = setup();
        let seq = SequenceDefinition {
            id: orch8_types::ids::SequenceId::new(),
            tenant_id: orch8_types::ids::TenantId::new("tenant1").unwrap(),
            namespace: orch8_types::ids::Namespace::new("default"),
            name: "test_seq".to_string(),
            version: 1,
            deprecated: false,
            status: SequenceStatus::default(),
            blocks: vec![orch8_types::sequence::BlockDefinition::Step(Box::new(
                orch8_types::sequence::StepDef {
                    id: orch8_types::ids::BlockId::new("step1"),
                    handler: "echo".to_string(),
                    params: serde_json::json!({}),
                    delay: None,
                    retry: None,
                    timeout: None,
                    rate_limit_key: None,
                    send_window: None,
                    context_access: None,
                    cancellable: true,
                    wait_for_input: None,
                    queue_name: None,
                    deadline: None,
                    on_deadline_breach: None,
                    fallback_handler: None,
                    cache_key: None,
                },
            ))],
            interceptors: None,
            input_schema: None,
            sla: None,
            on_failure: None,
            on_cancel: None,
            created_at: chrono::Utc::now(),
        };

        let entry = publisher.publish_sequence(&seq, &key).await.unwrap();
        assert_eq!(entry.name, "test_seq");
        assert!(!entry.sha256.is_empty());
    }

    #[tokio::test]
    async fn publish_sequence_uploads_json_and_sig() {
        let cdn = Box::new(MemoryCdnBackend::new());
        let signing_key = SigningKey::generate(&mut OsRng);
        let manifest_gen = ManifestGenerator::new(signing_key.clone(), "key1".to_string());
        let publisher =
            SequencePublisher::new(cdn, manifest_gen, "t1".to_string(), "key1".to_string())
                .expect("valid tenant_id");

        let seq = SequenceDefinition {
            id: orch8_types::ids::SequenceId::new(),
            tenant_id: orch8_types::ids::TenantId::new("t1").unwrap(),
            namespace: orch8_types::ids::Namespace::new("default"),
            name: "my_seq".to_string(),
            version: 2,
            deprecated: false,
            status: SequenceStatus::default(),
            blocks: vec![],
            interceptors: None,
            input_schema: None,
            sla: None,
            on_failure: None,
            on_cancel: None,
            created_at: chrono::Utc::now(),
        };

        let entry = publisher
            .publish_sequence(&seq, &signing_key)
            .await
            .unwrap();
        assert_eq!(entry.name, "my_seq");
        assert_eq!(entry.version, 2);
        assert_eq!(entry.signing_key_id, "key1");
        assert!(entry.url.starts_with("/t1/sequences/"));
        assert!(std::path::Path::new(&entry.url)
            .extension()
            .is_some_and(|ext| ext.eq_ignore_ascii_case("json")));
    }

    #[tokio::test]
    async fn publish_manifest_creates_manifest_file() {
        let (publisher, _key) = setup();
        publisher
            .publish_manifest(vec![], vec![], vec![])
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn publish_sequence_deduplicates_required_handlers() {
        let (publisher, key) = setup();
        let seq = SequenceDefinition {
            id: orch8_types::ids::SequenceId::new(),
            tenant_id: orch8_types::ids::TenantId::new("tenant1").unwrap(),
            namespace: orch8_types::ids::Namespace::new("default"),
            name: "multi_handler".to_string(),
            version: 1,
            deprecated: false,
            status: SequenceStatus::default(),
            blocks: vec![
                orch8_types::sequence::BlockDefinition::Step(Box::new(
                    orch8_types::sequence::StepDef {
                        id: orch8_types::ids::BlockId::new("s1"),
                        handler: "echo".to_string(),
                        params: serde_json::json!({}),
                        delay: None,
                        retry: None,
                        timeout: None,
                        rate_limit_key: None,
                        send_window: None,
                        context_access: None,
                        cancellable: true,
                        wait_for_input: None,
                        queue_name: None,
                        deadline: None,
                        on_deadline_breach: None,
                        fallback_handler: None,
                        cache_key: None,
                    },
                )),
                orch8_types::sequence::BlockDefinition::Step(Box::new(
                    orch8_types::sequence::StepDef {
                        id: orch8_types::ids::BlockId::new("s2"),
                        handler: "echo".to_string(),
                        params: serde_json::json!({}),
                        delay: None,
                        retry: None,
                        timeout: None,
                        rate_limit_key: None,
                        send_window: None,
                        context_access: None,
                        cancellable: true,
                        wait_for_input: None,
                        queue_name: None,
                        deadline: None,
                        on_deadline_breach: None,
                        fallback_handler: None,
                        cache_key: None,
                    },
                )),
            ],
            interceptors: None,
            input_schema: None,
            sla: None,
            on_failure: None,
            on_cancel: None,
            created_at: chrono::Utc::now(),
        };

        let entry = publisher.publish_sequence(&seq, &key).await.unwrap();
        assert_eq!(entry.required_handlers, vec!["echo"]);
    }

    fn make_seq(name: &str, version: i32, tenant_id: &str) -> SequenceDefinition {
        SequenceDefinition {
            id: orch8_types::ids::SequenceId::new(),
            tenant_id: orch8_types::ids::TenantId::new(tenant_id).unwrap(),
            namespace: orch8_types::ids::Namespace::new("default"),
            name: name.to_string(),
            version,
            deprecated: false,
            status: SequenceStatus::default(),
            blocks: vec![orch8_types::sequence::BlockDefinition::Step(Box::new(
                orch8_types::sequence::StepDef {
                    id: orch8_types::ids::BlockId::new("s1"),
                    handler: "echo".to_string(),
                    params: serde_json::json!({"key": "value"}),
                    delay: None,
                    retry: None,
                    timeout: None,
                    rate_limit_key: None,
                    send_window: None,
                    context_access: None,
                    cancellable: true,
                    wait_for_input: None,
                    queue_name: None,
                    deadline: None,
                    on_deadline_breach: None,
                    fallback_handler: None,
                    cache_key: None,
                },
            ))],
            interceptors: None,
            input_schema: None,
            sla: None,
            on_failure: None,
            on_cancel: None,
            created_at: chrono::DateTime::parse_from_rfc3339("2025-01-01T00:00:00Z")
                .unwrap()
                .with_timezone(&chrono::Utc),
        }
    }

    #[tokio::test]
    async fn publish_sequence_deterministic_hash() {
        let key = SigningKey::generate(&mut OsRng);

        let seq = make_seq("deterministic", 1, "t");

        let cdn1 = Box::new(MemoryCdnBackend::new());
        let gen1 = ManifestGenerator::new(key.clone(), "k".to_string());
        let pub1 = SequencePublisher::new(cdn1, gen1, "t".to_string(), "k".to_string())
            .expect("valid tenant_id");

        let cdn2 = Box::new(MemoryCdnBackend::new());
        let gen2 = ManifestGenerator::new(key.clone(), "k".to_string());
        let pub2 = SequencePublisher::new(cdn2, gen2, "t".to_string(), "k".to_string())
            .expect("valid tenant_id");

        let e1 = pub1.publish_sequence(&seq, &key).await.unwrap();
        let e2 = pub2.publish_sequence(&seq, &key).await.unwrap();
        assert_eq!(e1.sha256, e2.sha256, "same sequence must produce same hash");
    }

    #[tokio::test]
    async fn publish_sequence_hash_changes_on_content_change() {
        let (publisher, key) = setup();

        let seq_v1 = make_seq("evolving", 1, "tenant1");
        let seq_v2 = make_seq("evolving", 2, "tenant1");

        let e1 = publisher.publish_sequence(&seq_v1, &key).await.unwrap();
        let e2 = publisher.publish_sequence(&seq_v2, &key).await.unwrap();
        assert_ne!(
            e1.sha256, e2.sha256,
            "different content must produce different hash"
        );
    }

    #[tokio::test]
    async fn min_sdk_version_default_from_cargo_pkg() {
        let (publisher, key) = setup();
        let seq = make_seq("sdk_ver", 1, "tenant1");

        let entry = publisher.publish_sequence(&seq, &key).await.unwrap();
        assert_eq!(entry.min_sdk_version, env!("CARGO_PKG_VERSION"));
    }

    #[tokio::test]
    async fn min_sdk_version_override_via_builder() {
        let cdn = Box::new(MemoryCdnBackend::new());
        let signing_key = SigningKey::generate(&mut OsRng);
        let manifest_gen = ManifestGenerator::new(signing_key.clone(), "key1".to_string());
        let publisher =
            SequencePublisher::new(cdn, manifest_gen, "tenant1".to_string(), "key1".to_string())
                .expect("valid tenant_id")
                .with_min_sdk_version("2.0.0".to_string());

        let seq = make_seq("sdk_override", 1, "tenant1");
        let entry = publisher
            .publish_sequence(&seq, &signing_key)
            .await
            .unwrap();
        assert_eq!(entry.min_sdk_version, "2.0.0");
    }

    #[test]
    fn new_rejects_empty_tenant_id() {
        let cdn = Box::new(MemoryCdnBackend::new());
        let key = SigningKey::generate(&mut OsRng);
        let gen = ManifestGenerator::new(key, "k".to_string());
        let result = SequencePublisher::new(cdn, gen, String::new(), "k".to_string());
        let Err(err) = result else {
            panic!("expected empty tenant_id to be rejected");
        };
        assert!(err.to_string().contains("tenant_id cannot be empty"));
    }

    #[test]
    fn new_rejects_path_metacharacters_in_tenant_id() {
        let key = SigningKey::generate(&mut OsRng);
        for bad in ["a/b", "a\\\\b", "../tenant", ".."] {
            let cdn = Box::new(MemoryCdnBackend::new());
            let gen = ManifestGenerator::new(key.clone(), "k".to_string());
            let result = SequencePublisher::new(cdn, gen, bad.to_string(), "k".to_string());
            let Err(err) = result else {
                panic!("expected rejection for tenant_id {bad}");
            };
            assert!(
                err.to_string().contains("path metacharacters"),
                "expected rejection for tenant_id {bad}"
            );
        }
    }

    #[tokio::test]
    async fn publish_sequence_rejects_mismatched_tenant() {
        let (publisher, key) = setup();
        let seq = make_seq("wrong_tenant", 1, "other_tenant");
        let result = publisher.publish_sequence(&seq, &key).await;
        let Err(err) = result else {
            panic!("expected tenant mismatch to be rejected");
        };
        assert!(err.to_string().contains("does not match publisher tenant"));
    }

    #[tokio::test]
    async fn publish_manifest_rejects_url_outside_tenant() {
        let (publisher, _key) = setup();
        let seq = ManifestSequence {
            name: "bad".to_string(),
            version: 1,
            url: "/other_tenant/sequences/bad_v1.json".to_string(),
            signing_key_id: "key1".to_string(),
            sha256: "abc".to_string(),
            required_handlers: vec![],
            min_sdk_version: "0.1.0".to_string(),
        };
        let result = publisher.publish_manifest(vec![seq], vec![], vec![]).await;
        let Err(err) = result else {
            panic!("expected outside-tenant URL to be rejected");
        };
        assert!(err.to_string().contains("outside tenant"));
    }

    #[tokio::test]
    async fn publish_manifest_rejects_other_key_shadowing_generator() {
        let (publisher, _key) = setup();
        let other = crate::manifest::ManifestSigningKey {
            key_id: "key1".to_string(),
            public_key: "abc".to_string(),
        };
        let result = publisher
            .publish_manifest(vec![], vec![], vec![other])
            .await;
        let Err(err) = result else {
            panic!("expected generator key shadow to be rejected");
        };
        assert!(err.to_string().contains("generator's key_id"));
    }
}
