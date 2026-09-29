//! Sub-tenants: end customers of a vendor, scoped *inside* an engine tenant.
//!
//! A vendor embedding Orch8 sends `X-Orch8-Sub-Tenant: <id>` from its
//! backend. Instances (and the sequences a sub-tenant owns) carry the id as a
//! nullable `sub_tenant`; absent means tenant-level (the pre-existing
//! behaviour). Per-sub-tenant caps are enforced inside the tenant's pooled
//! plan limits, and activity is metered per sub-tenant for Embedded billing.

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use utoipa::ToSchema;

/// Request header carrying the sub-tenant id.
pub const SUB_TENANT_HEADER: &str = "x-orch8-sub-tenant";
/// Maximum sub-tenant id length (bytes; ids are ASCII).
pub const MAX_SUB_TENANT_LEN: usize = 128;

/// Validate a sub-tenant id: 1–128 chars of `[A-Za-z0-9._:-]`.
pub fn validate_sub_tenant(id: &str) -> Result<(), String> {
    if id.is_empty() || id.len() > MAX_SUB_TENANT_LEN {
        return Err(format!(
            "sub-tenant id must be 1-{MAX_SUB_TENANT_LEN} characters"
        ));
    }
    if !id
        .bytes()
        .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'.' | b'_' | b':' | b'-'))
    {
        return Err("sub-tenant id may only contain [A-Za-z0-9._:-]".into());
    }
    Ok(())
}

/// Start of the UTC calendar month containing `now` (monthly caps window).
#[must_use]
pub fn month_start(now: DateTime<Utc>) -> DateTime<Utc> {
    use chrono::{Datelike, TimeZone};
    Utc.with_ymd_and_hms(now.year(), now.month(), 1, 0, 0, 0)
        .single()
        .unwrap_or(now)
}

/// Per-sub-tenant caps. `None` = uncapped (the tenant pool still applies).
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
#[serde(deny_unknown_fields)]
pub struct SubTenantLimits {
    /// Executions (top-level instances) the sub-tenant may start per
    /// calendar month (UTC).
    #[serde(default)]
    pub max_executions_per_month: Option<u64>,
    /// Non-terminal instances the sub-tenant may hold at once.
    #[serde(default)]
    pub max_concurrent: Option<u32>,
}

impl SubTenantLimits {
    #[must_use]
    pub const fn is_unlimited(&self) -> bool {
        self.max_executions_per_month.is_none() && self.max_concurrent.is_none()
    }
}

/// Prefix of the `StorageError::QuotaExceeded` message a sub-tenant cap
/// produces, so the API can map it to `sub_tenant_quota_exceeded`.
pub const SUB_TENANT_QUOTA_PREFIX: &str = "sub_tenant_quota_exceeded";

/// Metered activity of one sub-tenant over a window.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
pub struct SubTenantUsage {
    pub sub_tenant: String,
    /// Executions started in the window (durable counter; survives instance
    /// retention/pruning).
    pub executions_started: u64,
    /// Instances of the sub-tenant that completed in the window.
    pub executions_completed: u64,
    /// Steps (block outputs) recorded in the window for the sub-tenant.
    pub steps_executed: u64,
    pub last_active_at: Option<DateTime<Utc>>,
}

/// Namespace in which Orch8 Cloud publishes vendor gallery templates.
pub const GALLERY_NAMESPACE: &str = "embed-gallery";

/// Per-sequence embedding settings. Nothing from a run's context or outputs
/// is shown to embedded (end-customer) viewers unless the sequence lists the
/// step in `visible_outputs`.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize, ToSchema)]
pub struct SequenceEmbed {
    /// Step ids whose outputs embedded viewers may see.
    #[serde(default)]
    pub visible_outputs: Vec<String>,
    /// Gallery template (tenant-level, namespace `embed-gallery`): listed
    /// to every sub-tenant, read-only, copyable by the embedded builder.
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub gallery: bool,
    /// Gallery template descriptor (opaque to the engine).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub template: Option<serde_json::Value>,
    /// Display title.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub title: Option<String>,
    /// Display description.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
}

impl SequenceEmbed {
    /// Whether `seq` is a published gallery template: tenant-level, in
    /// [`GALLERY_NAMESPACE`], flagged `embed.gallery`.
    #[must_use]
    pub fn is_gallery_template(seq: &crate::sequence::SequenceDefinition) -> bool {
        seq.sub_tenant.is_none()
            && seq.namespace.as_str() == GALLERY_NAMESPACE
            && seq.embed.as_ref().is_some_and(|e| e.gallery)
    }
}

/// Tenant-wide embed theme served to embed-kit components.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
#[serde(deny_unknown_fields)]
pub struct EmbedTheme {
    /// CSS custom properties (`--orch8-*`), name → value.
    #[serde(default)]
    pub css_vars: std::collections::BTreeMap<String, String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub logo_url: Option<String>,
    /// Requested badge removal. Only honoured with a `white_label` license.
    #[serde(default)]
    pub hide_badge: bool,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sub_tenant_ids_are_bounded_and_charset_restricted() {
        assert!(validate_sub_tenant("acme").is_ok());
        assert!(validate_sub_tenant("org:acme.eu-1_x").is_ok());
        assert!(validate_sub_tenant(&"a".repeat(128)).is_ok());
        assert!(validate_sub_tenant("").is_err());
        assert!(validate_sub_tenant(&"a".repeat(129)).is_err());
        assert!(validate_sub_tenant("a b").is_err());
        assert!(validate_sub_tenant("a/b").is_err());
        assert!(validate_sub_tenant("é").is_err());
    }

    #[test]
    fn month_start_truncates_to_the_first_utc_midnight() {
        let now = DateTime::parse_from_rfc3339("2026-09-28T13:14:15Z")
            .unwrap()
            .with_timezone(&Utc);
        assert_eq!(month_start(now).to_rfc3339(), "2026-09-01T00:00:00+00:00");
    }

    #[test]
    fn limits_reject_unknown_fields() {
        assert!(serde_json::from_str::<SubTenantLimits>(r#"{"max_concurrent":1}"#).is_ok());
        assert!(serde_json::from_str::<SubTenantLimits>(r#"{"max":1}"#).is_err());
    }
}
