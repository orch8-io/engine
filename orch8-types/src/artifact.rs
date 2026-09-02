//! Artifact references — durable binary blobs (images, rendered pages, files)
//! produced and consumed by steps.
//!
//! The engine is JSON-only on the hot path: block outputs and instance context
//! are JSON, and the context is size-capped. Binary payloads therefore never
//! travel inline — a step stores the bytes in the configured artifact backend
//! (local filesystem, S3-compatible object storage, …) and passes this small
//! [`ArtifactRef`] downstream instead. The ref is plain JSON, so it survives
//! checkpointing, replay, and crash recovery like any other step output.

use serde::{Deserialize, Serialize};

/// A durable reference to stored bytes. Travels in step outputs / context as
/// `{ "artifact": { ... } }` — never the bytes themselves.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ArtifactRef {
    /// Opaque artifact id (unique within the instance).
    pub id: String,
    /// Owning instance — artifacts are instance-scoped (object keys are
    /// prefixed with it, so an instance's blobs can be enumerated via
    /// `list_artifacts` and removed via `delete_artifact`). Note: unlike DB
    /// rows, object-store blobs are not yet swept automatically on instance
    /// deletion — that cleanup is a follow-up.
    pub instance_id: String,
    /// Backend object key (e.g. `"<instance_id>/<id>"`).
    pub key: String,
    /// MIME type captured at store time (e.g. `image/png`, `text/html`).
    pub content_type: String,
    /// Size in bytes.
    pub size: u64,
    /// Logical URI: `artifact://<key>`.
    pub uri: String,
}

impl ArtifactRef {
    /// Wrap this ref as the canonical handler output shape: `{ "artifact": {…} }`.
    #[must_use]
    pub fn into_output(self) -> serde_json::Value {
        serde_json::json!({ "artifact": self })
    }
}

/// Bytes fetched from the artifact backend, with the recorded content type.
#[derive(Debug, Clone)]
pub struct ArtifactObject {
    pub content_type: String,
    pub bytes: Vec<u8>,
}

/// Metadata for one stored artifact, as returned by a listing. Named fields
/// instead of a bare `(String, u64)` tuple so call sites read clearly.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ArtifactMeta {
    /// Backend object key (`<instance_id>/<id>`).
    pub key: String,
    /// Size in bytes.
    pub size: u64,
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample() -> ArtifactRef {
        ArtifactRef {
            id: "a1".into(),
            instance_id: "i1".into(),
            key: "i1/a1".into(),
            content_type: "image/png".into(),
            size: 42,
            uri: "artifact://i1/a1".into(),
        }
    }

    #[test]
    fn round_trips_through_json() {
        let r = sample();
        let json = serde_json::to_value(&r).unwrap();
        assert_eq!(json["content_type"], "image/png");
        assert_eq!(json["uri"], "artifact://i1/a1");
        let back: ArtifactRef = serde_json::from_value(json).unwrap();
        assert_eq!(back, r);
    }

    #[test]
    fn into_output_wraps_under_artifact_key() {
        let out = sample().into_output();
        assert_eq!(out["artifact"]["id"], "a1");
        assert_eq!(out["artifact"]["size"], 42);
    }
}
