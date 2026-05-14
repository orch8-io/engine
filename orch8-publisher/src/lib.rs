//! `orch8-publisher` — Manifest generation, sequence publishing, and push fan-out.
//!
//! This crate is used server-side to publish sequences and manifests to a CDN,
//! and to notify mobile devices of updates.

pub mod cdn;
pub mod manifest;
pub mod publish;
pub mod push;

pub use cdn::{CdnBackend, CdnError, MemoryCdnBackend, S3CdnBackend};
pub use manifest::{
    ManifestBody, ManifestGenerator, ManifestRemoved, ManifestSequence, ManifestSigningKey,
    SignedManifest,
};
pub use publish::{PublishError, SequencePublisher};
pub use push::{PushError, PushNotifier};
