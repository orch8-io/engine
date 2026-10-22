//! Complete logical payload catalogs retain metadata and instance ownership.
use chrono::{Duration, Timelike, Utc};
use orch8_storage::testing::fixtures::{InstanceBuilder, SequenceBuilder};
use orch8_storage::{backend_test, externalizing::ExternalizedEntry};
use serde_json::json;

fn payload(index: usize) -> serde_json::Value {
    if index == 0 {
        json!({"index": index, "blob": "private-payload".repeat(1000)})
    } else {
        json!({"index": index})
    }
}

backend_test!(async fn payload_catalog_is_complete_scoped_and_preserves_metadata(b) {
    let sequence = SequenceBuilder::new("catalog").step("work", "noop").build();
    b.storage.create_sequence(&sequence).await.unwrap();
    let instance = InstanceBuilder::for_sequence(&sequence).build();
    let other = InstanceBuilder::for_sequence(&sequence).build();
    b.storage.create_instance(&instance).await.unwrap();
    b.storage.create_instance(&other).await.unwrap();
    let created_at = (Utc::now() - Duration::days(3)).with_nanosecond(123_456_000).unwrap();
    for index in 0..1001 {
        b.storage.restore_externalized_entry(&ExternalizedEntry {
            instance_id: instance.id, ref_key: format!("payload{index:04}"), payload: payload(index),
            created_at, expires_at: if index % 2 == 0 { None } else { Some(created_at + Duration::days(30)) },
        }).await.unwrap();
    }
    assert!(b.storage.list_externalized_entries(other.id, None, 1000).await.unwrap().is_empty());
    assert!(b.storage.list_externalized_entries(instance.id, None, 0).await.unwrap().is_empty());
    assert_eq!(b.storage.list_externalized_entries(instance.id, None, u32::MAX).await.unwrap().len(), 1000);
    let mut all = Vec::new();
    let mut after = None;
    loop {
        let page = b.storage.list_externalized_entries(instance.id, after.as_deref(), 137).await.unwrap();
        if page.is_empty() { break; }
        after = page.last().map(|entry| entry.ref_key.clone());
        all.extend(page);
    }
    assert_eq!(all.len(), 1001);
    for (index, entry) in all.iter().enumerate() {
        assert_eq!(entry.instance_id, instance.id);
        assert_eq!(entry.ref_key, format!("payload{index:04}"));
        assert_eq!(entry.payload, payload(index));
        assert_eq!(entry.created_at, created_at);
        assert_eq!(entry.expires_at, if index % 2 == 0 { None } else { Some(created_at + Duration::days(30)) });
    }
    let mut collision = all[0].clone();
    collision.instance_id = other.id;
    collision.payload = json!("must not overwrite the owner");
    assert!(b.storage.restore_externalized_entry(&collision).await.is_err());
    assert_eq!(b.storage.get_externalized_state(instance.id, &all[0].ref_key).await.unwrap(), Some(payload(0)));
    assert!(b.storage.get_externalized_state(other.id, &all[0].ref_key).await.unwrap().is_none());
});
