//! `orch8-publisher` — Manifest generation and sequence publishing.
//!
//! This crate is used server-side to publish sequences and manifests to a
//! CDN. Push fan-out to mobile devices lives in `orch8-push` (`FcmProvider`,
//! `ApnsProvider`) — this crate previously carried its own `PushNotifier`
//! (M-20), an unused, never-wired-up duplicate whose APNs path was a stub
//! that always returned an error.

pub mod capsule;
pub mod cdn;
pub mod grant;
pub mod manifest;
pub mod package;
pub mod publish;

pub use cdn::{CdnBackend, CdnError, MemoryCdnBackend, S3CdnBackend};
pub use manifest::{
    ManifestBody, ManifestGenerator, ManifestRemoved, ManifestSequence, ManifestSigningKey,
    SignedManifest,
};
pub use publish::{PublishError, SequencePublisher};
