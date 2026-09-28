//! Artifact backend placeholder for builds without the `artifacts` feature
//! (the embedded mobile engine). It keeps the backends' `artifact_store`
//! plumbing type-checking while making an artifact store impossible to
//! construct, so every artifact call returns the same permanent
//! `Unsupported` error as an unconfigured server.

use std::convert::Infallible;
use std::sync::Arc;

use bytes::Bytes;
use uuid::Uuid;

use orch8_types::artifact::{ArtifactMeta, ArtifactRef};
use orch8_types::error::StorageError;

/// Uninhabited: no value of this type can exist without the `artifacts` feature.
pub struct ObjectArtifactStore {
    never: Infallible,
}

/// Mirrors `artifacts::require_store`; always fails because no store can exist.
pub fn require_store(
    store: Option<&Arc<ObjectArtifactStore>>,
) -> Result<&ObjectArtifactStore, StorageError> {
    store
        .map(Arc::as_ref)
        .ok_or_else(|| StorageError::Unsupported("artifact storage is not configured".into()))
}

#[allow(clippy::unused_async)]
impl ObjectArtifactStore {
    pub async fn put(
        &self,
        _instance_id: &str,
        _content_type: &str,
        _bytes: Bytes,
    ) -> Result<ArtifactRef, StorageError> {
        match self.never {}
    }

    pub async fn put_with_id(
        &self,
        _instance_id: &str,
        _artifact_id: Uuid,
        _content_type: &str,
        _bytes: Bytes,
    ) -> Result<ArtifactRef, StorageError> {
        match self.never {}
    }

    pub async fn get(&self, _key: &str) -> Result<Option<Vec<u8>>, StorageError> {
        match self.never {}
    }

    pub async fn delete(&self, _key: &str) -> Result<(), StorageError> {
        match self.never {}
    }

    pub async fn list(&self, _instance_id: &str) -> Result<Vec<ArtifactMeta>, StorageError> {
        match self.never {}
    }
}
