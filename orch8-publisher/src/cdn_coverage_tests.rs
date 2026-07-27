//! Conditional-write (compare-and-swap) coverage for the CDN backends.
//!
//! Count contract: 26 independently named unit tests.

use super::*;

fn quoted_sha256(bytes: &[u8]) -> String {
    format!("\"{}\"", hex::encode(Sha256::digest(bytes)))
}

#[tokio::test]
async fn coverage_cdn_001_cas_creates_object_when_absent_and_none_expected() {
    let backend = MemoryCdnBackend::new();
    let etag = backend
        .upload_if_match("index.json", b"v1".to_vec(), None, None, None)
        .await
        .unwrap();
    assert_eq!(etag, quoted_sha256(b"v1"));
    assert_eq!(
        backend.get_etag("index.json").await.unwrap().as_deref(),
        Some(etag.as_str())
    );
}

#[tokio::test]
async fn coverage_cdn_002_cas_conflict_when_none_expected_but_object_exists() {
    let backend = MemoryCdnBackend::new();
    backend
        .upload("index.json", b"v1".to_vec(), None, None)
        .await
        .unwrap();
    let error = backend
        .upload_if_match("index.json", b"v2".to_vec(), None, None, None)
        .await
        .unwrap_err();
    assert!(matches!(error, CdnError::Conflict));
}

#[tokio::test]
async fn coverage_cdn_003_cas_conflict_when_etag_expected_but_object_absent() {
    let backend = MemoryCdnBackend::new();
    let error = backend
        .upload_if_match(
            "index.json",
            b"v1".to_vec(),
            None,
            None,
            Some("\"anything\""),
        )
        .await
        .unwrap_err();
    assert!(matches!(error, CdnError::Conflict));
}

#[tokio::test]
async fn coverage_cdn_004_cas_conflict_on_stale_etag() {
    let backend = MemoryCdnBackend::new();
    backend
        .upload("index.json", b"v1".to_vec(), None, None)
        .await
        .unwrap();
    let error = backend
        .upload_if_match(
            "index.json",
            b"v2".to_vec(),
            None,
            None,
            Some(&quoted_sha256(b"stale")),
        )
        .await
        .unwrap_err();
    assert!(matches!(error, CdnError::Conflict));
}

#[tokio::test]
async fn coverage_cdn_005_cas_succeeds_with_matching_etag_and_swaps_bytes() {
    let backend = MemoryCdnBackend::new();
    backend
        .upload("index.json", b"v1".to_vec(), None, None)
        .await
        .unwrap();
    let old_etag = backend.get_etag("index.json").await.unwrap().unwrap();
    let new_etag = backend
        .upload_if_match("index.json", b"v2".to_vec(), None, None, Some(&old_etag))
        .await
        .unwrap();
    assert_eq!(new_etag, quoted_sha256(b"v2"));
    assert_ne!(new_etag, old_etag);
}

#[tokio::test]
async fn coverage_cdn_006_conflict_leaves_stored_bytes_untouched() {
    let backend = MemoryCdnBackend::new();
    backend
        .upload("index.json", b"v1".to_vec(), None, Some("max-age=60"))
        .await
        .unwrap();
    backend
        .upload_if_match(
            "index.json",
            b"v2".to_vec(),
            None,
            Some("no-store"),
            Some("\"stale\""),
        )
        .await
        .unwrap_err();
    let store = backend.store.lock().await;
    let (bytes, cache_control) = store.get("index.json").unwrap();
    assert_eq!(bytes, b"v1");
    assert_eq!(cache_control.as_deref(), Some("max-age=60"));
}

#[tokio::test]
async fn coverage_cdn_007_cas_stores_cache_control_on_success() {
    let backend = MemoryCdnBackend::new();
    backend
        .upload_if_match("index.json", b"v1".to_vec(), None, Some("max-age=60"), None)
        .await
        .unwrap();
    let store = backend.store.lock().await;
    assert_eq!(
        store.get("index.json").unwrap().1.as_deref(),
        Some("max-age=60")
    );
}

#[tokio::test]
async fn coverage_cdn_008_cas_updates_cache_control_on_overwrite() {
    let backend = MemoryCdnBackend::new();
    backend
        .upload("index.json", b"v1".to_vec(), None, Some("max-age=60"))
        .await
        .unwrap();
    let etag = backend.get_etag("index.json").await.unwrap().unwrap();
    backend
        .upload_if_match(
            "index.json",
            b"v2".to_vec(),
            None,
            Some("max-age=30"),
            Some(&etag),
        )
        .await
        .unwrap();
    let store = backend.store.lock().await;
    assert_eq!(
        store.get("index.json").unwrap().1.as_deref(),
        Some("max-age=30")
    );
}

#[tokio::test]
async fn coverage_cdn_009_cas_ignores_content_type_in_memory_backend() {
    let backend = MemoryCdnBackend::new();
    backend
        .upload_if_match(
            "index.json",
            b"v1".to_vec(),
            Some("application/json"),
            None,
            None,
        )
        .await
        .unwrap();
    assert_eq!(
        backend.get_etag("index.json").await.unwrap().as_deref(),
        Some(quoted_sha256(b"v1").as_str())
    );
}

#[tokio::test]
async fn coverage_cdn_010_cas_chain_advances_and_rejects_replay() {
    let backend = MemoryCdnBackend::new();
    let etag_a = backend
        .upload_if_match("index.json", b"a".to_vec(), None, None, None)
        .await
        .unwrap();
    let etag_b = backend
        .upload_if_match("index.json", b"b".to_vec(), None, None, Some(&etag_a))
        .await
        .unwrap();
    // Replaying the retired etag must fail even though it was valid once.
    let error = backend
        .upload_if_match("index.json", b"c".to_vec(), None, None, Some(&etag_a))
        .await
        .unwrap_err();
    assert!(matches!(error, CdnError::Conflict));
    let etag_c = backend
        .upload_if_match("index.json", b"c".to_vec(), None, None, Some(&etag_b))
        .await
        .unwrap();
    assert_eq!(etag_c, quoted_sha256(b"c"));
}

#[tokio::test]
async fn coverage_cdn_011_cas_preconditions_are_scoped_per_path() {
    let backend = MemoryCdnBackend::new();
    backend
        .upload("a/index.json", b"a".to_vec(), None, None)
        .await
        .unwrap();
    // `b/index.json` does not exist, so `If-None-Match: *` must succeed there
    // even though another path already holds an object.
    let etag = backend
        .upload_if_match("b/index.json", b"b".to_vec(), None, None, None)
        .await
        .unwrap();
    assert_eq!(etag, quoted_sha256(b"b"));
    // The other path must be untouched.
    assert_eq!(
        backend.get_etag("a/index.json").await.unwrap().as_deref(),
        Some(quoted_sha256(b"a").as_str())
    );
}

#[tokio::test]
async fn coverage_cdn_012_cas_supports_empty_payload() {
    let backend = MemoryCdnBackend::new();
    let etag = backend
        .upload_if_match("empty.bin", Vec::new(), None, None, None)
        .await
        .unwrap();
    assert_eq!(etag, quoted_sha256(b""));
}

#[tokio::test]
async fn coverage_cdn_013_unconditional_upload_invalidates_prior_etag() {
    let backend = MemoryCdnBackend::new();
    let etag = backend
        .upload_if_match("index.json", b"v1".to_vec(), None, None, None)
        .await
        .unwrap();
    // An unconditional writer (no precondition) silently advances the object.
    backend
        .upload("index.json", b"v2".to_vec(), None, None)
        .await
        .unwrap();
    let error = backend
        .upload_if_match("index.json", b"v3".to_vec(), None, None, Some(&etag))
        .await
        .unwrap_err();
    assert!(matches!(error, CdnError::Conflict));
}

/// Backend that relies on the default `upload_if_match` implementation.
struct LegacyBackend;

#[async_trait::async_trait]
impl CdnBackend for LegacyBackend {
    async fn upload(
        &self,
        _path: &str,
        _bytes: Vec<u8>,
        _content_type: Option<&str>,
        _cache_control: Option<&str>,
    ) -> Result<(), CdnError> {
        Ok(())
    }

    async fn delete(&self, _path: &str) -> Result<(), CdnError> {
        Ok(())
    }

    async fn get_etag(&self, _path: &str) -> Result<Option<String>, CdnError> {
        Ok(None)
    }
}

#[tokio::test]
async fn coverage_cdn_014_default_upload_if_match_reports_unsupported() {
    let error = LegacyBackend
        .upload_if_match("index.json", b"v1".to_vec(), None, None, None)
        .await
        .unwrap_err();
    assert!(matches!(error, CdnError::ConditionalWritesUnsupported));
}

#[test]
fn coverage_cdn_015_conditional_writes_unsupported_display() {
    assert_eq!(
        CdnError::ConditionalWritesUnsupported.to_string(),
        "CDN backend does not support atomic conditional writes"
    );
}

#[test]
fn coverage_cdn_016_conflict_display() {
    assert_eq!(
        CdnError::Conflict.to_string(),
        "optimistic concurrency conflict"
    );
}

#[tokio::test]
async fn coverage_cdn_017_arc_delegates_successful_cas() {
    let backend = std::sync::Arc::new(MemoryCdnBackend::new());
    let via_arc: std::sync::Arc<MemoryCdnBackend> = std::sync::Arc::clone(&backend);
    let etag =
        CdnBackend::upload_if_match(&via_arc, "index.json", b"v1".to_vec(), None, None, None)
            .await
            .unwrap();
    assert_eq!(etag, quoted_sha256(b"v1"));
}

#[tokio::test]
async fn coverage_cdn_018_arc_delegates_cas_conflict() {
    let backend = std::sync::Arc::new(MemoryCdnBackend::new());
    backend
        .upload("index.json", b"v1".to_vec(), None, None)
        .await
        .unwrap();
    let error =
        CdnBackend::upload_if_match(&backend, "index.json", b"v2".to_vec(), None, None, None)
            .await
            .unwrap_err();
    assert!(matches!(error, CdnError::Conflict));
}

#[tokio::test]
async fn coverage_cdn_019_arc_delegation_matches_inner_etag() {
    let backend = std::sync::Arc::new(MemoryCdnBackend::new());
    CdnBackend::upload_if_match(&backend, "index.json", b"v1".to_vec(), None, None, None)
        .await
        .unwrap();
    let via_arc = CdnBackend::get_etag(&backend, "index.json").await.unwrap();
    let direct = backend.get_etag("index.json").await.unwrap();
    assert_eq!(via_arc, direct);
    assert_eq!(via_arc.as_deref(), Some(quoted_sha256(b"v1").as_str()));
}

#[tokio::test]
async fn coverage_cdn_020_cas_after_delete_recreates_with_none_expected() {
    let backend = MemoryCdnBackend::new();
    backend
        .upload("index.json", b"v1".to_vec(), None, None)
        .await
        .unwrap();
    backend.delete("index.json").await.unwrap();
    let etag = backend
        .upload_if_match("index.json", b"v2".to_vec(), None, None, None)
        .await
        .unwrap();
    assert_eq!(etag, quoted_sha256(b"v2"));
}

#[tokio::test]
async fn coverage_cdn_021_delete_then_cas_with_old_etag_conflicts() {
    let backend = MemoryCdnBackend::new();
    let etag = backend
        .upload_if_match("index.json", b"v1".to_vec(), None, None, None)
        .await
        .unwrap();
    backend.delete("index.json").await.unwrap();
    let error = backend
        .upload_if_match("index.json", b"v2".to_vec(), None, None, Some(&etag))
        .await
        .unwrap_err();
    assert!(matches!(error, CdnError::Conflict));
}

#[tokio::test]
async fn coverage_cdn_022_etag_is_deterministic_for_identical_bytes() {
    let backend = MemoryCdnBackend::new();
    backend
        .upload("one.json", b"same".to_vec(), None, None)
        .await
        .unwrap();
    backend
        .upload("two.json", b"same".to_vec(), None, None)
        .await
        .unwrap();
    assert_eq!(
        backend.get_etag("one.json").await.unwrap(),
        backend.get_etag("two.json").await.unwrap()
    );
    assert_eq!(
        backend.get_etag("one.json").await.unwrap().as_deref(),
        Some(quoted_sha256(b"same").as_str())
    );
}

#[tokio::test]
async fn coverage_cdn_023_get_etag_returns_none_for_unknown_path() {
    let backend = MemoryCdnBackend::new();
    assert_eq!(backend.get_etag("missing.json").await.unwrap(), None);
}

#[tokio::test]
async fn coverage_cdn_024_concurrent_cas_admits_exactly_one_winner() {
    use std::sync::Arc;
    let backend = Arc::new(MemoryCdnBackend::new());
    let mut handles = Vec::new();
    for attempt in 0..8_u8 {
        let backend = Arc::clone(&backend);
        handles.push(tokio::spawn(async move {
            backend
                .upload_if_match("index.json", vec![attempt], None, None, None)
                .await
        }));
    }
    let mut wins = 0;
    let mut conflicts = 0;
    for handle in handles {
        match handle.await.unwrap() {
            Ok(_) => wins += 1,
            Err(CdnError::Conflict) => conflicts += 1,
            Err(other) => panic!("unexpected error: {other}"),
        }
    }
    assert_eq!(wins, 1);
    assert_eq!(conflicts, 7);
}

#[tokio::test]
async fn coverage_cdn_025_delete_of_missing_object_succeeds() {
    let backend = MemoryCdnBackend::new();
    backend.delete("missing.json").await.unwrap();
    assert_eq!(backend.get_etag("missing.json").await.unwrap(), None);
}

#[tokio::test]
async fn coverage_cdn_026_unconditional_upload_replaces_bytes_and_cache_control() {
    let backend = MemoryCdnBackend::new();
    backend
        .upload("index.json", b"v1".to_vec(), None, Some("max-age=60"))
        .await
        .unwrap();
    backend
        .upload("index.json", b"v2".to_vec(), None, None)
        .await
        .unwrap();
    let store = backend.store.lock().await;
    let (bytes, cache_control) = store.get("index.json").unwrap();
    assert_eq!(bytes, b"v2");
    assert_eq!(cache_control, &None);
}
