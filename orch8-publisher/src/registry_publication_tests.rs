//! Registry publication ordering and head compare-and-swap.
//!
//! Rule under test: a publish writes every immutable object (package, ledger
//! entry, ledger snapshot) before it swaps the mutable `index.json` head with a
//! conditional write against the `ETag` the caller loaded. A stale or missing
//! head is a `Conflict` that leaves the winner's bytes and the caller's local
//! state untouched, and a failed attempt is always safe to retry.

use std::collections::BTreeMap;
use std::sync::Arc;

use chrono::TimeZone;
use rand::rng;

use super::*;
use crate::cdn::MemoryCdnBackend;
use crate::package::{PackageManifest, PackageRequirements, build_package};

fn package(key: &SigningKey, name: &str, version: &str) -> SignedPackage {
    build_package(
        PackageManifest {
            name: name.into(),
            version: version.into(),
            description: "publication fixture".into(),
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
    let key = SigningKey::generate(&mut rng());
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
fn source_etag_is_never_serialized() {
    let index = RegistryIndex::new("tenant-a", "acme").with_source_etag("\"abc\"");
    let json = canonical_json(&index).unwrap();
    assert!(!json.contains("source_etag"));
    let decoded: RegistryIndex = serde_json::from_str(&json).unwrap();
    assert_eq!(decoded.source_etag, None);
    assert_eq!(decoded, RegistryIndex::new("tenant-a", "acme"));
}

#[tokio::test]
async fn first_publish_creates_index_and_sets_etag() {
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
async fn index_head_uses_sixty_second_cache_window() {
    let (cdn, publisher, key, mut index, mut ledger) = fixture();
    publish_one(&publisher, &key, &mut index, &mut ledger, "1.0.0").await;
    let store = cdn.store.lock().await;
    assert_eq!(
        store["tenant-a/registry/acme/index.json"].1.as_deref(),
        Some("max-age=60")
    );
}

#[tokio::test]
async fn ledger_snapshot_is_hash_addressed_and_immutable() {
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
async fn legacy_ledger_head_path_is_not_written() {
    let (cdn, publisher, key, mut index, mut ledger) = fixture();
    publish_one(&publisher, &key, &mut index, &mut ledger, "1.0.0").await;
    let store = cdn.store.lock().await;
    assert!(!store.contains_key("tenant-a/registry/acme/transparency/ledger.json"));
}

#[tokio::test]
async fn package_and_entry_objects_are_immutable() {
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
async fn immutable_objects_upload_before_index_head() {
    let cdn = Arc::new(RecordingBackend::new());
    let publisher =
        PackageRegistryPublisher::new(Box::new(Arc::clone(&cdn)), "tenant-a", "acme").unwrap();
    let key = SigningKey::generate(&mut rng());
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
async fn stored_index_matches_local_state() {
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
async fn external_overwrite_conflicts_next_publish() {
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
async fn conflict_preserves_winning_index_bytes() {
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
async fn competing_writers_serialize_through_cas() {
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
async fn deleted_index_conflicts_when_etag_expected() {
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
async fn out_of_namespace_package_is_rejected() {
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
async fn mismatched_ledger_signing_key_is_rejected() {
    let (_cdn, publisher, key, mut index, mut ledger) = fixture();
    let other_key = SigningKey::generate(&mut rng());
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
async fn duplicate_version_reports_package_and_version() {
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
async fn unexpected_schema_version_is_rejected() {
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
async fn versions_are_ordered_oldest_to_newest() {
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
async fn failed_package_upload_leaves_state_unchanged() {
    let publisher =
        PackageRegistryPublisher::new(Box::new(FailUploadBackend), "tenant-a", "acme").unwrap();
    let key = SigningKey::generate(&mut rng());
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
async fn failed_head_write_is_safe_to_retry() {
    let cdn = Arc::new(FailCasBackend {
        inner: MemoryCdnBackend::new(),
    });
    let publisher =
        PackageRegistryPublisher::new(Box::new(Arc::clone(&cdn)), "tenant-a", "acme").unwrap();
    let key = SigningKey::generate(&mut rng());
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
async fn ledger_snapshot_matches_local_ledger() {
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
