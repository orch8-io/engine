use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use tokio::sync::{Notify, Semaphore};

use super::*;
use crate::cdn::MemoryCdnBackend;

/// Pause one write so two public removals overlap at the CDN boundary.
struct PausedCdn {
    inner: MemoryCdnBackend,
    pause_next: AtomicBool,
    paused: Notify,
    resume: Semaphore,
}

#[async_trait::async_trait]
impl CdnBackend for PausedCdn {
    async fn upload(
        &self,
        path: &str,
        bytes: Vec<u8>,
        content_type: Option<&str>,
        cache_control: Option<&str>,
    ) -> Result<(), CdnError> {
        if self.pause_next.swap(false, Ordering::SeqCst) {
            self.paused.notify_one();
            self.resume.acquire().await.unwrap().forget();
        }
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
}

fn sequence(name: &str) -> SequenceDefinition {
    serde_json::from_value(serde_json::json!({
        "id": orch8_types::ids::SequenceId::new(),
        "tenant_id": "tenant1", "namespace": "default", "name": name,
        "version": 1, "created_at": chrono::Utc::now(),
        "blocks": [{"type": "step", "id": "step", "handler": "noop"}]
    }))
    .unwrap()
}

#[tokio::test]
async fn concurrent_removals_preserve_both_changes_in_the_published_manifest() {
    let cdn = Arc::new(PausedCdn {
        inner: MemoryCdnBackend::new(),
        pause_next: AtomicBool::new(false),
        paused: Notify::new(),
        resume: Semaphore::new(0),
    });
    let key = ed25519_dalek::SigningKey::generate(&mut rand::rng());
    let publisher = SequencePublisher::new(
        Box::new(Arc::clone(&cdn)),
        ManifestGenerator::new(key.clone(), "key1".into()),
        "tenant1".into(),
        "key1".into(),
    )
    .unwrap();
    let first = publisher
        .publish_sequence(&sequence("first"), &key)
        .await
        .unwrap();
    let second = publisher
        .publish_sequence(&sequence("second"), &key)
        .await
        .unwrap();
    publisher
        .publish_manifest(vec![first, second], vec![], vec![])
        .await
        .unwrap();
    cdn.pause_next.store(true, Ordering::SeqCst);
    let now = chrono::Utc::now();

    let remove_second = async {
        cdn.paused.notified().await;
        // The first write is waiting. Give it permission to finish, then
        // poll the second removal before the first future can resume.
        cdn.resume.add_permits(1);
        publisher.remove_from_manifest("second", now).await
    };
    tokio::time::timeout(std::time::Duration::from_secs(5), async {
        tokio::try_join!(publisher.remove_from_manifest("first", now), remove_second)
    })
    .await
    .unwrap()
    .unwrap();

    let store = cdn.inner.store.lock().await;
    let bytes = &store.get("tenant1/manifest.json").unwrap().0;
    let signed = std::str::from_utf8(bytes).unwrap();
    let (_, body) = signed.split_once('\n').unwrap();
    let body: serde_json::Value = serde_json::from_str(body).unwrap();
    assert!(body["sequences"].as_array().unwrap().is_empty());
    let mut removed: Vec<_> = body["removed"]
        .as_array()
        .unwrap()
        .iter()
        .map(|entry| entry["name"].as_str().unwrap())
        .collect();
    removed.sort_unstable();
    assert_eq!(removed, ["first", "second"]);
}
