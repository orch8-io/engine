//! Shared bounds for mobile telemetry ingestion.
//!
//! Both transports (HTTP `/telemetry/mobile` and gRPC `IngestTelemetryBatch`)
//! enforce the same limits through [`validate_event_fields`], so neither can
//! be used to bypass the other's field/payload/event-type bounds.

/// Most events one batch may carry.
pub const MAX_TELEMETRY_EVENTS: usize = 1_000;
/// Total accepted payload bytes per batch.
pub const MAX_TELEMETRY_BATCH_PAYLOAD_BYTES: usize = 4 * 1024 * 1024;
/// Largest single event payload.
pub const MAX_TELEMETRY_EVENT_PAYLOAD_BYTES: usize = 256 * 1024;
/// Longest `message` accepted on an error report.
pub const MAX_ERROR_MESSAGE_BYTES: usize = 8 * 1024;
/// Longest `stack_trace` accepted on an error report.
pub const MAX_ERROR_STACK_BYTES: usize = 64 * 1024;

/// Why an event was refused (`code` is the stable machine-readable reason).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TelemetryRejection {
    pub code: &'static str,
    pub message: String,
}

fn reject(code: &'static str, message: impl Into<String>) -> TelemetryRejection {
    TelemetryRejection {
        code,
        message: message.into(),
    }
}

/// One device metadata field: `(name, value, max_bytes)`.
pub type DeviceField<'a> = (&'static str, &'a str, usize);

/// The five device metadata fields with their byte limits, in the shape
/// [`validate_event_fields`] takes.
#[must_use]
pub fn device_field_limits<'a>(
    device_id: &'a str,
    os_name: &'a str,
    os_version: &'a str,
    app_version: &'a str,
    sdk_version: &'a str,
) -> [DeviceField<'a>; 5] {
    [
        ("device_id", device_id, 256),
        ("os_name", os_name, 64),
        ("os_version", os_version, 64),
        ("app_version", app_version, 64),
        ("sdk_version", sdk_version, 64),
    ]
}

/// Validate one event's type, payload and device metadata. Returns the
/// trimmed event type on success.
///
/// `accepted_payload_bytes` is the payload total already accepted from earlier
/// events of the same batch.
///
/// # Errors
/// A [`TelemetryRejection`] naming the first violated bound.
pub fn validate_event_fields<'a>(
    event_type: &'a str,
    payload_json: &str,
    device: [DeviceField<'_>; 5],
    accepted_payload_bytes: usize,
) -> Result<&'a str, TelemetryRejection> {
    let event_type = event_type.trim();
    if event_type.is_empty() || event_type.len() > 128 {
        return Err(reject(
            "invalid_event_type",
            "event_type must contain 1..=128 bytes",
        ));
    }
    if payload_json.len() > MAX_TELEMETRY_EVENT_PAYLOAD_BYTES {
        return Err(reject("payload_too_large", "payload_json exceeds 256 KiB"));
    }
    if accepted_payload_bytes.saturating_add(payload_json.len()) > MAX_TELEMETRY_BATCH_PAYLOAD_BYTES
    {
        return Err(reject(
            "batch_payload_limit_exceeded",
            "accepted payloads would exceed the 4 MiB batch limit",
        ));
    }
    if serde_json::from_str::<serde_json::Value>(payload_json).is_err() {
        return Err(reject(
            "invalid_payload_json",
            "payload_json must be valid JSON",
        ));
    }
    for (name, value, max_bytes) in device {
        if value.len() > max_bytes {
            return Err(reject(
                "metadata_too_large",
                format!("{name} exceeds {max_bytes} bytes"),
            ));
        }
    }
    Ok(event_type)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn dev<'a>() -> [DeviceField<'a>; 5] {
        device_field_limits("d", "iOS", "17", "1", "1")
    }

    #[test]
    fn accepts_valid_event_and_trims_type() {
        assert_eq!(
            validate_event_fields("  Sync ", "{}", dev(), 0).unwrap(),
            "Sync"
        );
    }

    #[test]
    fn rejects_each_bound() {
        let long = "x".repeat(129);
        assert_eq!(
            validate_event_fields(&long, "{}", dev(), 0).unwrap_err().code,
            "invalid_event_type"
        );
        assert_eq!(
            validate_event_fields("a", "not json", dev(), 0).unwrap_err().code,
            "invalid_payload_json"
        );
        let big = format!("\"{}\"", "x".repeat(MAX_TELEMETRY_EVENT_PAYLOAD_BYTES));
        assert_eq!(
            validate_event_fields("a", &big, dev(), 0).unwrap_err().code,
            "payload_too_large"
        );
        assert_eq!(
            validate_event_fields("a", "{}", dev(), MAX_TELEMETRY_BATCH_PAYLOAD_BYTES)
                .unwrap_err()
                .code,
            "batch_payload_limit_exceeded"
        );
        let id = "d".repeat(257);
        assert_eq!(
            validate_event_fields("a", "{}", device_field_limits(&id, "", "", "", ""), 0)
                .unwrap_err()
                .code,
            "metadata_too_large"
        );
    }
}
