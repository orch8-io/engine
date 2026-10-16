//! Persisted API-key grants never widen, identical for `SQLite` and Postgres.
//!
//! A key reads back with exactly the capabilities it was stored with. In
//! particular an empty grant must never come back as `ApiCapability::all()`:
//! the store either refuses to persist it or returns a key that grants
//! nothing.
//!
//! ```text
//! DATABASE_URL=postgres://… cargo test -p orch8-storage --test api_key_grant_contract
//! ```

#[macro_use]
mod support;

use orch8_storage::StorageBackend;
use orch8_types::api_key::{ApiCapability, mint_scoped};

async fn scoped_grant_reads_back_exactly(s: &dyn StorageBackend) {
    let key = mint_scoped(
        "tenant-a",
        "ci-worker",
        None,
        vec![ApiCapability::Worker, ApiCapability::Auditor],
    );
    s.create_api_key(&key.record).await.unwrap();

    let stored = s
        .lookup_api_key_by_hash(&key.record.key_hash)
        .await
        .unwrap()
        .expect("key persisted");

    assert_eq!(
        stored.capabilities,
        vec![ApiCapability::Worker, ApiCapability::Auditor]
    );
}

async fn empty_grant_never_reads_back_as_every_capability(s: &dyn StorageBackend) {
    let key = mint_scoped("tenant-a", "empty-grant", None, Vec::new());

    // Refusing to persist an empty grant is an acceptable fix; only a
    // persisted key can widen.
    if s.create_api_key(&key.record).await.is_ok() {
        let stored = s
            .lookup_api_key_by_hash(&key.record.key_hash)
            .await
            .unwrap()
            .expect("key persisted");
        assert!(
            stored.capabilities.is_empty(),
            "an empty grant widened to {:?}",
            stored.capabilities
        );
    }
}

backend_contract!(
    scoped_grant_reads_back_exactly,
    #[ignore = "BUG: an empty persisted capability list is read back as ApiCapability::all() (T4 #4) — fixed on test/bugs"]
    empty_grant_never_reads_back_as_every_capability,
);
