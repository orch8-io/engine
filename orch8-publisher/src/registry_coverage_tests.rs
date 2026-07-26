//! Registry head-update serialization and versioning coverage.
//!
//! Count contract: 30 independently named unit tests.

use std::collections::BTreeMap;
use std::sync::Arc;

use chrono::TimeZone;
use rand_core::OsRng;

use super::*;
use crate::cdn::MemoryCdnBackend;
use crate::package::{PackageManifest, PackageRequirements, build_package};

fn package(key: &SigningKey, name: &str, version: &str) -> SignedPackage {
    build_package(
        PackageManifest {
            name: name.into(),
            version: version.into(),
            description: "coverage fixture".into(),
            publisher: "Acme".into(),
            requirements: PackageRequirements::default(),
            created_at: Utc.with_ymd_and_hms(2026, 7, 25, 0, 0, 0).unwrap(),
        },
        BTreeMap::from([("README.md".into(), "# fixture".into())]),
        key,
    )
    .unwrap()
}

fn fixture() -> (
    Arc<MemoryCdnBackend>,
    PackageRegistryPublisher,
    SigningKey,
    RegistryIndex,
    TransparencyLedger,
) {
    let cdn = Arc::new(MemoryCdnBackend::new());
    let publisher =
        PackageRegistryPublisher::new(Box::new(Arc::clone(&cdn)), "tenant-a", "acme").unwrap();
    let key = SigningKey::generate(&mut OsRng);
    let index = RegistryIndex::new("tenant-a", "acme");
    let ledger = TransparencyLedger::default();
    (cdn, publisher, key, index, ledger)
}

fn published_at(hour: u32) -> DateTime<Utc> {
    Utc.with_ymd_and_hms(2026, 7, 25, hour, 0, 0).unwrap()
}

async fn publish_one(
    publisher: &PackageRegistryPublisher,
    key: &SigningKey,
    index: &mut RegistryIndex,
    ledger: &mut TransparencyLedger,
    version: &str,
) -> RegistryVersion {
    publisher
        .publish(
            &package(key, "acme/checkout", version),
            index,
            ledger,
            key,
            published_at(1),
        )
        .await
        .unwrap()
}

/// Backend whose unconditional uploads fail, to exercise the pre-head-write
/// failure path.
struct FailUploadBackend;

#[async_trait::async_trait]
impl CdnBackend for FailUploadBackend {
    async fn upload(
        &self,
        _path: &str,
        _bytes: Vec<u8>,
        _content_type: Option<&str>,
        _cache_control: Option<&str>,
    ) -> Result<(), CdnError> {
        Err(CdnError::Upload("injected upload failure".into()))
    }

    async fn delete(&self, _path: &str) -> Result<(), CdnError> {
        Ok(())
    }

    async fn get_etag(&self, _path: &str) -> Result<Option<String>, CdnError> {
        Ok(None)
    }
}

/// Backend that stores immutable objects but rejects the index head write.
struct FailCasBackend {
    inner: MemoryCdnBackend,
}

#[async_trait::async_trait]
impl CdnBackend for FailCasBackend {
    async fn upload(
        &self,
        path: &str,
        bytes: Vec<u8>,
        content_type: Option<&str>,
        cache_control: Option<&str>,
    ) -> Result<(), CdnError> {
        self.inner
            .upload(path, bytes, content_type, cache_control)
            .await
    }

    async fn delete(&self, path: &str) -> Result<(), CdnError> {
        self.inner.delete(path).await
    }

    async fn get_etag(&self, path: &str) -> Result<Option<String>, CdnError> {
        self.inner.get_etag(path).await
    }

    async fn upload_if_match(
        &self,
        _path: &str,
        _bytes: Vec<u8>,
        _content_type: Option<&str>,
        _cache_control: Option<&str>,
        _expected_etag: Option<&str>,
    ) -> Result<String, CdnError> {
        Err(CdnError::Upload("injected head write failure".into()))
    }
}

/// Backend that records the order of writes so tests can pin the publication
/// sequence (immutable objects first, index head last).
struct RecordingBackend {
    inner: MemoryCdnBackend,
    writes: tokio::sync::Mutex<Vec<String>>,
}

impl RecordingBackend {
    fn new() -> Self {
        Self {
            inner: MemoryCdnBackend::new(),
            writes: tokio::sync::Mutex::new(Vec::new()),
        }
    }
}

#[async_trait::async_trait]
impl CdnBackend for RecordingBackend {
    async fn upload(
        &self,
        path: &str,
        bytes: Vec<u8>,
        content_type: Option<&str>,
        cache_control: Option<&str>,
    ) -> Result<(), CdnError> {
        self.writes.lock().await.push(format!("put:{path}"));
        self.inner
            .upload(path, bytes, content_type, cache_control)
            .await
    }

    async fn delete(&self, path: &str) -> Result<(), CdnError> {
        self.inner.delete(path).await
    }

    async fn get_etag(&self, path: &str) -> Result<Option<String>, CdnError> {
        self.inner.get_etag(path).await
    }

    async fn upload_if_match(
        &self,
        path: &str,
        bytes: Vec<u8>,
        content_type: Option<&str>,
        cache_control: Option<&str>,
        expected_etag: Option<&str>,
    ) -> Result<String, CdnError> {
        self.writes.lock().await.push(format!("cas:{path}"));
        self.inner
            .upload_if_match(path, bytes, content_type, cache_control, expected_etag)
            .await
    }
}

#[test]
fn coverage_registry_001_new_index_has_expected_defaults() {
    let index = RegistryIndex::new("tenant-a", "acme");
    assert_eq!(index.schema_version, 1);
    assert_eq!(index.tenant_id, "tenant-a");
    assert_eq!(index.namespace, "acme");
    assert!(index.packages.is_empty());
    assert_eq!(index.ledger_head, None);
    assert_eq!(index.source_etag, None);
}

#[test]
fn coverage_registry_002_with_source_etag_attaches_loaded_etag() {
    let index = RegistryIndex::new("tenant-a", "acme").with_source_etag("\"abc\"");
    assert_eq!(index.source_etag.as_deref(), Some("\"abc\""));
}

#[test]
fn coverage_registry_003_source_etag_is_never_serialized() {
    let index = RegistryIndex::new("tenant-a", "acme").with_source_etag("\"abc\"");
    let json = canonical_json(&index).unwrap();
    assert!(!json.contains("source_etag"));
    let decoded: RegistryIndex = serde_json::from_str(&json).unwrap();
    assert_eq!(decoded.source_etag, None);
    assert_eq!(decoded, RegistryIndex::new("tenant-a", "acme"));
}

#[tokio::test]
async fn coverage_registry_004_first_publish_creates_index_and_sets_etag() {
    let (cdn, publisher, key, mut index, mut ledger) = fixture();
    publish_one(&publisher, &key, &mut index, &mut ledger, "1.0.0").await;
    let etag = index.source_etag.clone().unwrap();
    let store = cdn.store.lock().await;
    assert!(store.contains_key("tenant-a/registry/acme/index.json"));
    drop(store);
    assert_eq!(
        cdn.get_etag("tenant-a/registry/acme/index.json")
            .await
            .unwrap()
            .as_deref(),
        Some(etag.as_str())
    );
}

#[tokio::test]
async fn coverage_registry_005_index_head_uses_sixty_second_cache_window() {
    let (cdn, publisher, key, mut index, mut ledger) = fixture();
    publish_one(&publisher, &key, &mut index, &mut ledger, "1.0.0").await;
    let store = cdn.store.lock().await;
    assert_eq!(
        store["tenant-a/registry/acme/index.json"].1.as_deref(),
        Some("max-age=60")
    );
}

#[tokio::test]
async fn coverage_registry_006_ledger_snapshot_is_hash_addressed_and_immutable() {
    let (cdn, publisher, key, mut index, mut ledger) = fixture();
    let version = publish_one(&publisher, &key, &mut index, &mut ledger, "1.0.0").await;
    let path = format!(
        "tenant-a/registry/acme/transparency/ledgers/{}.json",
        version.ledger_entry_hash
    );
    let store = cdn.store.lock().await;
    assert_eq!(
        store[&path].1.as_deref(),
        Some("immutable, max-age=31536000")
    );
}

#[tokio::test]
async fn coverage_registry_007_legacy_ledger_head_path_is_not_written() {
    let (cdn, publisher, key, mut index, mut ledger) = fixture();
    publish_one(&publisher, &key, &mut index, &mut ledger, "1.0.0").await;
    let store = cdn.store.lock().await;
    assert!(!store.contains_key("tenant-a/registry/acme/transparency/ledger.json"));
}

#[tokio::test]
async fn coverage_registry_008_package_and_entry_objects_are_immutable() {
    let (cdn, publisher, key, mut index, mut ledger) = fixture();
    publish_one(&publisher, &key, &mut index, &mut ledger, "1.0.0").await;
    let store = cdn.store.lock().await;
    let mut immutable_objects = 0;
    for (path, (_, cache_control)) in store.iter() {
        if path.contains("/packages/") || path.contains("/transparency/entries/") {
            immutable_objects += 1;
            assert_eq!(
                cache_control.as_deref(),
                Some("immutable, max-age=31536000"),
                "{path} must be immutable"
            );
        }
    }
    // One publish writes exactly one package object and one entry object; the
    // loop above must not have matched nothing.
    assert_eq!(immutable_objects, 2);
}

#[tokio::test]
async fn coverage_registry_009_entry_path_uses_zero_padded_sequence() {
    let (cdn, publisher, key, mut index, mut ledger) = fixture();
    let version = publish_one(&publisher, &key, &mut index, &mut ledger, "1.0.0").await;
    let expected = format!(
        "tenant-a/registry/acme/transparency/entries/00000000000000000000-{}.json",
        version.ledger_entry_hash
    );
    let store = cdn.store.lock().await;
    assert!(store.contains_key(&expected), "missing {expected}");
}

#[tokio::test]
async fn coverage_registry_010_immutable_objects_upload_before_index_head() {
    let cdn = Arc::new(RecordingBackend::new());
    let publisher =
        PackageRegistryPublisher::new(Box::new(Arc::clone(&cdn)), "tenant-a", "acme").unwrap();
    let key = SigningKey::generate(&mut OsRng);
    let mut index = RegistryIndex::new("tenant-a", "acme");
    let mut ledger = TransparencyLedger::default();
    publish_one(&publisher, &key, &mut index, &mut ledger, "1.0.0").await;
    let writes = cdn.writes.lock().await;
    assert_eq!(writes.len(), 4);
    assert!(writes[0].starts_with("put:") && writes[0].contains("/packages/"));
    assert!(writes[1].starts_with("put:") && writes[1].contains("/transparency/entries/"));
    assert!(writes[2].starts_with("put:") && writes[2].contains("/transparency/ledgers/"));
    assert_eq!(writes[3], "cas:tenant-a/registry/acme/index.json");
}

#[tokio::test]
async fn coverage_registry_011_stored_index_matches_local_state() {
    let (cdn, publisher, key, mut index, mut ledger) = fixture();
    publish_one(&publisher, &key, &mut index, &mut ledger, "1.0.0").await;
    publish_one(&publisher, &key, &mut index, &mut ledger, "1.1.0").await;
    let store = cdn.store.lock().await;
    let stored: RegistryIndex =
        serde_json::from_slice(&store["tenant-a/registry/acme/index.json"].0).unwrap();
    // The stored document never carries transport metadata.
    assert_eq!(stored.source_etag, None);
    let mut local = index.clone();
    local.source_etag = None;
    assert_eq!(stored, local);
}

#[tokio::test]
async fn coverage_registry_012_etag_advances_on_every_publish() {
    let (_cdn, publisher, key, mut index, mut ledger) = fixture();
    publish_one(&publisher, &key, &mut index, &mut ledger, "1.0.0").await;
    let first = index.source_etag.clone().unwrap();
    publish_one(&publisher, &key, &mut index, &mut ledger, "1.1.0").await;
    let second = index.source_etag.clone().unwrap();
    assert_ne!(first, second);
}

#[tokio::test]
async fn coverage_registry_013_external_overwrite_conflicts_next_publish() {
    let (cdn, publisher, key, mut index, mut ledger) = fixture();
    publish_one(&publisher, &key, &mut index, &mut ledger, "1.0.0").await;
    // An out-of-band writer moves the head without telling our index.
    cdn.upload(
        "tenant-a/registry/acme/index.json",
        b"{}".to_vec(),
        Some("application/json"),
        Some("max-age=60"),
    )
    .await
    .unwrap();
    let error = publisher
        .publish(
            &package(&key, "acme/checkout", "1.1.0"),
            &mut index,
            &mut ledger,
            &key,
            published_at(2),
        )
        .await
        .unwrap_err();
    assert!(matches!(error, RegistryError::Cdn(CdnError::Conflict)));
}

#[tokio::test]
async fn coverage_registry_014_conflict_preserves_winning_index_bytes() {
    let (cdn, publisher, key, mut index, mut ledger) = fixture();
    publish_one(&publisher, &key, &mut index, &mut ledger, "1.0.0").await;
    let winner_bytes = {
        let store = cdn.store.lock().await;
        store["tenant-a/registry/acme/index.json"].0.clone()
    };
    let mut stale_index = RegistryIndex::new("tenant-a", "acme");
    let mut stale_ledger = TransparencyLedger::default();
    publisher
        .publish(
            &package(&key, "acme/checkout", "1.1.0"),
            &mut stale_index,
            &mut stale_ledger,
            &key,
            published_at(2),
        )
        .await
        .unwrap_err();
    let store = cdn.store.lock().await;
    assert_eq!(store["tenant-a/registry/acme/index.json"].0, winner_bytes);
}

#[tokio::test]
async fn coverage_registry_015_conflict_leaves_caller_state_unchanged() {
    let (_cdn, publisher, key, mut index, mut ledger) = fixture();
    publish_one(&publisher, &key, &mut index, &mut ledger, "1.0.0").await;
    // Fork stale state from the initial publish, then advance the real head.
    let mut stale_index = index.clone();
    let mut stale_ledger = ledger.clone();
    publish_one(&publisher, &key, &mut index, &mut ledger, "1.1.0").await;
    let before_index = stale_index.clone();
    let before_ledger = stale_ledger.clone();
    publisher
        .publish(
            &package(&key, "acme/checkout", "1.2.0"),
            &mut stale_index,
            &mut stale_ledger,
            &key,
            published_at(3),
        )
        .await
        .unwrap_err();
    assert_eq!(stale_index, before_index);
    assert_eq!(stale_ledger, before_ledger);
}

#[tokio::test]
async fn coverage_registry_016_reload_and_retry_after_conflict_succeeds() {
    let (cdn, publisher, key, mut index, mut ledger) = fixture();
    publish_one(&publisher, &key, &mut index, &mut ledger, "1.0.0").await;
    let mut stale_index = RegistryIndex::new("tenant-a", "acme");
    let mut stale_ledger = TransparencyLedger::default();
    publisher
        .publish(
            &package(&key, "acme/checkout", "1.1.0"),
            &mut stale_index,
            &mut stale_ledger,
            &key,
            published_at(2),
        )
        .await
        .unwrap_err();
    // Reload the new head: index bytes plus their ETag.
    let index_bytes = {
        let store = cdn.store.lock().await;
        store["tenant-a/registry/acme/index.json"].0.clone()
    };
    let mut reloaded: RegistryIndex = serde_json::from_slice(&index_bytes).unwrap();
    reloaded.source_etag = cdn
        .get_etag("tenant-a/registry/acme/index.json")
        .await
        .unwrap();
    let mut reloaded_ledger = ledger.clone();
    publisher
        .publish(
            &package(&key, "acme/checkout", "1.1.0"),
            &mut reloaded,
            &mut reloaded_ledger,
            &key,
            published_at(2),
        )
        .await
        .unwrap();
    assert_eq!(reloaded.packages["acme/checkout"].len(), 2);
    assert_eq!(reloaded_ledger.entries.len(), 2);
}

#[tokio::test]
async fn coverage_registry_017_competing_writers_serialize_through_cas() {
    let (cdn, publisher, key, mut index_a, mut ledger_a) = fixture();
    publish_one(&publisher, &key, &mut index_a, &mut ledger_a, "1.0.0").await;
    // Writer B starts from an empty (stale) view and loses the first race.
    let mut index_b = RegistryIndex::new("tenant-a", "acme");
    let mut ledger_b = TransparencyLedger::default();
    let error = publisher
        .publish(
            &package(&key, "acme/checkout", "1.1.0"),
            &mut index_b,
            &mut ledger_b,
            &key,
            published_at(2),
        )
        .await
        .unwrap_err();
    assert!(matches!(error, RegistryError::Cdn(CdnError::Conflict)));
    // B reloads both heads from the CDN and retries.
    let index_bytes = {
        let store = cdn.store.lock().await;
        store["tenant-a/registry/acme/index.json"].0.clone()
    };
    let mut reloaded_index: RegistryIndex = serde_json::from_slice(&index_bytes).unwrap();
    reloaded_index.source_etag = cdn
        .get_etag("tenant-a/registry/acme/index.json")
        .await
        .unwrap();
    let ledger_path = format!(
        "tenant-a/registry/acme/transparency/ledgers/{}.json",
        reloaded_index.ledger_head.clone().unwrap()
    );
    let ledger_bytes = {
        let store = cdn.store.lock().await;
        store[&ledger_path].0.clone()
    };
    let mut reloaded_ledger: TransparencyLedger = serde_json::from_slice(&ledger_bytes).unwrap();
    publisher
        .publish(
            &package(&key, "acme/checkout", "1.1.0"),
            &mut reloaded_index,
            &mut reloaded_ledger,
            &key,
            published_at(2),
        )
        .await
        .unwrap();
    reloaded_ledger.verify().unwrap();
    reloaded_index.verify_against(&reloaded_ledger).unwrap();
    assert_eq!(reloaded_ledger.entries.len(), 2);
    assert_eq!(
        reloaded_ledger.entries[1].previous_hash.as_deref(),
        Some(reloaded_ledger.entries[0].entry_hash.as_str())
    );
    assert!(reloaded_ledger.contains_head(ledger_a.head().unwrap()));
}

#[tokio::test]
async fn coverage_registry_018_deleted_index_conflicts_when_etag_expected() {
    let (cdn, publisher, key, mut index, mut ledger) = fixture();
    publish_one(&publisher, &key, &mut index, &mut ledger, "1.0.0").await;
    cdn.delete("tenant-a/registry/acme/index.json")
        .await
        .unwrap();
    let error = publisher
        .publish(
            &package(&key, "acme/checkout", "1.1.0"),
            &mut index,
            &mut ledger,
            &key,
            published_at(2),
        )
        .await
        .unwrap_err();
    assert!(matches!(error, RegistryError::Cdn(CdnError::Conflict)));
}

#[tokio::test]
async fn coverage_registry_019_out_of_namespace_package_is_rejected() {
    let (_cdn, publisher, key, mut index, mut ledger) = fixture();
    let error = publisher
        .publish(
            &package(&key, "other/checkout", "1.0.0"),
            &mut index,
            &mut ledger,
            &key,
            published_at(1),
        )
        .await
        .unwrap_err();
    assert!(matches!(error, RegistryError::InvalidConfig(_)));
    assert!(index.packages.is_empty());
    assert!(ledger.entries.is_empty());
}

#[tokio::test]
async fn coverage_registry_020_mismatched_ledger_signing_key_is_rejected() {
    let (_cdn, publisher, key, mut index, mut ledger) = fixture();
    let other_key = SigningKey::generate(&mut OsRng);
    let error = publisher
        .publish(
            &package(&key, "acme/checkout", "1.0.0"),
            &mut index,
            &mut ledger,
            &other_key,
            published_at(1),
        )
        .await
        .unwrap_err();
    assert!(matches!(error, RegistryError::InvalidConfig(_)));
    assert!(index.packages.is_empty());
    assert!(ledger.entries.is_empty());
}

#[tokio::test]
async fn coverage_registry_021_duplicate_version_reports_package_and_version() {
    let (_cdn, publisher, key, mut index, mut ledger) = fixture();
    publish_one(&publisher, &key, &mut index, &mut ledger, "1.0.0").await;
    let error = publisher
        .publish(
            &package(&key, "acme/checkout", "1.0.0"),
            &mut index,
            &mut ledger,
            &key,
            published_at(2),
        )
        .await
        .unwrap_err();
    assert!(
        matches!(
            error,
            RegistryError::DuplicateVersion { ref package, ref version }
                if package == "acme/checkout" && version == "1.0.0"
        ),
        "unexpected error: {error}"
    );
}

#[tokio::test]
async fn coverage_registry_022_unexpected_schema_version_is_rejected() {
    let (_cdn, publisher, key, mut index, mut ledger) = fixture();
    index.schema_version = 2;
    let error = publisher
        .publish(
            &package(&key, "acme/checkout", "1.0.0"),
            &mut index,
            &mut ledger,
            &key,
            published_at(1),
        )
        .await
        .unwrap_err();
    assert!(matches!(error, RegistryError::InvalidConfig(_)));
}

#[tokio::test]
async fn coverage_registry_023_entry_sequence_increments_per_publish() {
    let (_cdn, publisher, key, mut index, mut ledger) = fixture();
    for (expected, version) in [(0, "1.0.0"), (1, "1.1.0"), (2, "1.2.0")] {
        publish_one(&publisher, &key, &mut index, &mut ledger, version).await;
        assert_eq!(ledger.entries.last().unwrap().sequence, expected);
    }
}

#[tokio::test]
async fn coverage_registry_024_previous_hash_links_form_a_chain() {
    let (_cdn, publisher, key, mut index, mut ledger) = fixture();
    publish_one(&publisher, &key, &mut index, &mut ledger, "1.0.0").await;
    publish_one(&publisher, &key, &mut index, &mut ledger, "1.1.0").await;
    assert_eq!(ledger.entries[0].previous_hash, None);
    assert_eq!(
        ledger.entries[1].previous_hash.as_deref(),
        Some(ledger.entries[0].entry_hash.as_str())
    );
    assert_eq!(ledger.head(), Some(ledger.entries[1].entry_hash.as_str()));
    assert_eq!(index.ledger_head.as_deref(), ledger.head());
}

#[tokio::test]
async fn coverage_registry_025_versions_are_ordered_oldest_to_newest() {
    let (_cdn, publisher, key, mut index, mut ledger) = fixture();
    for version in ["1.0.0", "1.1.0", "1.2.0"] {
        publish_one(&publisher, &key, &mut index, &mut ledger, version).await;
    }
    let versions: Vec<&str> = index.packages["acme/checkout"]
        .iter()
        .map(|version| version.version.as_str())
        .collect();
    assert_eq!(versions, ["1.0.0", "1.1.0", "1.2.0"]);
}

#[tokio::test]
async fn coverage_registry_026_package_url_layout_matches_registry_paths() {
    let (_cdn, publisher, key, mut index, mut ledger) = fixture();
    let version = publish_one(&publisher, &key, &mut index, &mut ledger, "1.0.0").await;
    assert_eq!(
        version.package_url,
        format!(
            "/tenant-a/registry/acme/packages/checkout/1.0.0/{}.orch8pkg",
            version.content_hash
        )
    );
}

#[tokio::test]
async fn coverage_registry_027_version_records_its_ledger_entry_hash() {
    let (_cdn, publisher, key, mut index, mut ledger) = fixture();
    let version = publish_one(&publisher, &key, &mut index, &mut ledger, "1.0.0").await;
    assert_eq!(version.ledger_entry_hash, ledger.entries[0].entry_hash);
    assert_eq!(
        index.ledger_head.as_deref(),
        Some(version.ledger_entry_hash.as_str())
    );
}

#[tokio::test]
async fn coverage_registry_028_failed_package_upload_leaves_state_unchanged() {
    let publisher =
        PackageRegistryPublisher::new(Box::new(FailUploadBackend), "tenant-a", "acme").unwrap();
    let key = SigningKey::generate(&mut OsRng);
    let mut index = RegistryIndex::new("tenant-a", "acme");
    let mut ledger = TransparencyLedger::default();
    let error = publisher
        .publish(
            &package(&key, "acme/checkout", "1.0.0"),
            &mut index,
            &mut ledger,
            &key,
            published_at(1),
        )
        .await
        .unwrap_err();
    assert!(matches!(error, RegistryError::Cdn(CdnError::Upload(_))));
    assert_eq!(index, RegistryIndex::new("tenant-a", "acme"));
    assert_eq!(ledger, TransparencyLedger::default());
}

#[tokio::test]
async fn coverage_registry_029_failed_head_write_is_safe_to_retry() {
    let cdn = Arc::new(FailCasBackend {
        inner: MemoryCdnBackend::new(),
    });
    let publisher =
        PackageRegistryPublisher::new(Box::new(Arc::clone(&cdn)), "tenant-a", "acme").unwrap();
    let key = SigningKey::generate(&mut OsRng);
    let mut index = RegistryIndex::new("tenant-a", "acme");
    let mut ledger = TransparencyLedger::default();
    let signed = package(&key, "acme/checkout", "1.0.0");
    let error = publisher
        .publish(&signed, &mut index, &mut ledger, &key, published_at(1))
        .await
        .unwrap_err();
    assert!(matches!(error, RegistryError::Cdn(CdnError::Upload(_))));
    assert_eq!(index, RegistryIndex::new("tenant-a", "acme"));
    assert_eq!(ledger, TransparencyLedger::default());
    // The immutable objects landed but the head did not, so the same attempt
    // against a healthy backend must succeed unchanged.
    let healthy = Arc::new(MemoryCdnBackend::new());
    {
        let failed_store = cdn.inner.store.lock().await;
        let mut healthy_store = healthy.store.lock().await;
        healthy_store.clone_from(&failed_store);
    }
    let publisher =
        PackageRegistryPublisher::new(Box::new(Arc::clone(&healthy)), "tenant-a", "acme").unwrap();
    publisher
        .publish(&signed, &mut index, &mut ledger, &key, published_at(1))
        .await
        .unwrap();
    assert_eq!(ledger.entries.len(), 1);
    index.verify_against(&ledger).unwrap();
}

#[tokio::test]
async fn coverage_registry_030_ledger_snapshot_matches_local_ledger() {
    let (cdn, publisher, key, mut index, mut ledger) = fixture();
    publish_one(&publisher, &key, &mut index, &mut ledger, "1.0.0").await;
    publish_one(&publisher, &key, &mut index, &mut ledger, "1.1.0").await;
    let path = format!(
        "tenant-a/registry/acme/transparency/ledgers/{}.json",
        ledger.head().unwrap()
    );
    let store = cdn.store.lock().await;
    let snapshot: TransparencyLedger = serde_json::from_slice(&store[&path].0).unwrap();
    assert_eq!(snapshot, ledger);
    snapshot.verify().unwrap();
}
