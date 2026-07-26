//! Coverage tests for the bounded telemetry ingress: per-field byte limits,
//! per-event and per-batch payload ceilings, JSON/timestamp validation, and
//! tenant stamping — including exactly-at-limit versus one-over boundaries.
//!
//! Count contract: 46 independently named unit tests.

use super::*;

const TENANT: &str = "acme";

fn tenant() -> TenantId {
    TenantId::unchecked(TENANT)
}

fn valid_input() -> proto::TelemetryEventInput {
    proto::TelemetryEventInput {
        event_type: "worker.started".into(),
        payload_json: r#"{"worker":"alpha"}"#.into(),
        device_id: "device-1".into(),
        os_name: "linux".into(),
        os_version: "6.12".into(),
        app_version: "1.0.0".into(),
        sdk_version: "1.0.0".into(),
        created_at: "2026-07-25T10:00:00Z".into(),
    }
}

fn validate(
    index: usize,
    input: &proto::TelemetryEventInput,
    accepted_payload_bytes: usize,
) -> Result<TelemetryEvent, proto::TelemetryEventRejection> {
    validate_telemetry_event(index, input, &tenant(), accepted_payload_bytes)
}

/// A syntactically valid JSON document occupying exactly `total_bytes`.
fn json_of_size(total_bytes: usize) -> String {
    let overhead = r#"{"d":""}"#.len();
    assert!(total_bytes >= overhead);
    format!(r#"{{"d":"{}"}}"#, "x".repeat(total_bytes - overhead))
}

macro_rules! rejection_case {
    ($name:ident, $input:expr, $code:expr) => {
        #[test]
        fn $name() {
            let rejection = validate(0, &$input, 0).unwrap_err();
            assert_eq!(rejection.code, $code);
        }
    };
}

#[test]
fn coverage_telemetry_001_valid_event_is_accepted_verbatim() {
    let event = validate(0, &valid_input(), 0).unwrap();
    assert_eq!(event.event_type, "worker.started");
    assert_eq!(event.payload, r#"{"worker":"alpha"}"#);
    assert_eq!(event.device_id, "device-1");
    assert_eq!(event.os_name, "linux");
    assert_eq!(event.os_version, "6.12");
    assert_eq!(event.app_version, "1.0.0");
    assert_eq!(event.sdk_version, "1.0.0");
}

rejection_case!(
    coverage_telemetry_002_empty_event_type_is_rejected,
    proto::TelemetryEventInput {
        event_type: String::new(),
        ..valid_input()
    },
    "invalid_event_type"
);

rejection_case!(
    coverage_telemetry_003_whitespace_only_event_type_is_rejected,
    proto::TelemetryEventInput {
        event_type: "   ".into(),
        ..valid_input()
    },
    "invalid_event_type"
);

#[test]
fn coverage_telemetry_004_event_type_is_trimmed_on_accept() {
    let input = proto::TelemetryEventInput {
        event_type: "  worker.ready  ".into(),
        ..valid_input()
    };
    let event = validate(0, &input, 0).unwrap();
    assert_eq!(event.event_type, "worker.ready");
}

#[test]
fn coverage_telemetry_005_event_type_at_128_bytes_is_accepted() {
    let input = proto::TelemetryEventInput {
        event_type: "e".repeat(128),
        ..valid_input()
    };
    assert!(validate(0, &input, 0).is_ok());
}

rejection_case!(
    coverage_telemetry_006_event_type_at_129_bytes_is_rejected,
    proto::TelemetryEventInput {
        event_type: "e".repeat(129),
        ..valid_input()
    },
    "invalid_event_type"
);

#[test]
fn coverage_telemetry_007_event_type_bound_counts_utf8_bytes() {
    // 64 × 'é' == 128 bytes — at the limit.
    let input = proto::TelemetryEventInput {
        event_type: "é".repeat(64),
        ..valid_input()
    };
    assert!(validate(0, &input, 0).is_ok());
}

rejection_case!(
    coverage_telemetry_008_multibyte_event_type_over_byte_limit_is_rejected,
    proto::TelemetryEventInput {
        // 65 × 'é' == 130 bytes.
        event_type: "é".repeat(65),
        ..valid_input()
    },
    "invalid_event_type"
);

#[test]
fn coverage_telemetry_009_payload_exactly_256_kib_is_accepted() {
    let input = proto::TelemetryEventInput {
        payload_json: json_of_size(MAX_TELEMETRY_EVENT_PAYLOAD_BYTES),
        ..valid_input()
    };
    assert!(validate(0, &input, 0).is_ok());
}

rejection_case!(
    coverage_telemetry_010_payload_one_byte_over_256_kib_is_rejected,
    proto::TelemetryEventInput {
        payload_json: json_of_size(MAX_TELEMETRY_EVENT_PAYLOAD_BYTES + 1),
        ..valid_input()
    },
    "payload_too_large"
);

#[test]
fn coverage_telemetry_011_batch_total_exactly_at_4_mib_is_accepted() {
    let input = valid_input();
    let headroom = MAX_TELEMETRY_BATCH_PAYLOAD_BYTES - input.payload_json.len();
    assert!(validate(0, &input, headroom).is_ok());
}

#[test]
fn coverage_telemetry_012_batch_total_one_byte_over_4_mib_is_rejected() {
    let input = valid_input();
    let headroom = MAX_TELEMETRY_BATCH_PAYLOAD_BYTES - input.payload_json.len() + 1;
    let rejection = validate(0, &input, headroom).unwrap_err();
    assert_eq!(rejection.code, "batch_payload_limit_exceeded");
}

#[test]
fn coverage_telemetry_013_batch_accounting_saturates_instead_of_wrapping() {
    let input = valid_input();
    let rejection = validate(0, &input, usize::MAX).unwrap_err();
    assert_eq!(rejection.code, "batch_payload_limit_exceeded");
}

rejection_case!(
    coverage_telemetry_014_non_json_payload_is_rejected,
    proto::TelemetryEventInput {
        payload_json: "not-json".into(),
        ..valid_input()
    },
    "invalid_payload_json"
);

rejection_case!(
    coverage_telemetry_015_empty_payload_is_rejected_as_invalid_json,
    proto::TelemetryEventInput {
        payload_json: String::new(),
        ..valid_input()
    },
    "invalid_payload_json"
);

#[test]
fn coverage_telemetry_016_json_null_payload_is_accepted() {
    let input = proto::TelemetryEventInput {
        payload_json: "null".into(),
        ..valid_input()
    };
    let event = validate(0, &input, 0).unwrap();
    assert_eq!(event.payload, "null");
}

rejection_case!(
    coverage_telemetry_017_json_with_trailing_garbage_is_rejected,
    proto::TelemetryEventInput {
        payload_json: "{} garbage".into(),
        ..valid_input()
    },
    "invalid_payload_json"
);

#[test]
fn coverage_telemetry_018_device_id_at_256_bytes_is_accepted() {
    let input = proto::TelemetryEventInput {
        device_id: "d".repeat(256),
        ..valid_input()
    };
    assert!(validate(0, &input, 0).is_ok());
}

rejection_case!(
    coverage_telemetry_019_device_id_at_257_bytes_is_rejected,
    proto::TelemetryEventInput {
        device_id: "d".repeat(257),
        ..valid_input()
    },
    "metadata_too_large"
);

#[test]
fn coverage_telemetry_020_os_name_at_64_bytes_is_accepted() {
    let input = proto::TelemetryEventInput {
        os_name: "o".repeat(64),
        ..valid_input()
    };
    assert!(validate(0, &input, 0).is_ok());
}

rejection_case!(
    coverage_telemetry_021_os_name_at_65_bytes_is_rejected,
    proto::TelemetryEventInput {
        os_name: "o".repeat(65),
        ..valid_input()
    },
    "metadata_too_large"
);

rejection_case!(
    coverage_telemetry_022_os_version_at_65_bytes_is_rejected,
    proto::TelemetryEventInput {
        os_version: "v".repeat(65),
        ..valid_input()
    },
    "metadata_too_large"
);

rejection_case!(
    coverage_telemetry_023_app_version_at_65_bytes_is_rejected,
    proto::TelemetryEventInput {
        app_version: "a".repeat(65),
        ..valid_input()
    },
    "metadata_too_large"
);

rejection_case!(
    coverage_telemetry_024_sdk_version_at_65_bytes_is_rejected,
    proto::TelemetryEventInput {
        sdk_version: "s".repeat(65),
        ..valid_input()
    },
    "metadata_too_large"
);

#[test]
fn coverage_telemetry_025_metadata_bound_names_the_offending_field() {
    let input = proto::TelemetryEventInput {
        device_id: "d".repeat(257),
        ..valid_input()
    };
    let rejection = validate(0, &input, 0).unwrap_err();
    assert!(rejection.message.contains("device_id"));
}

#[test]
fn coverage_telemetry_026_rfc3339_zulu_timestamp_is_accepted() {
    let event = validate(0, &valid_input(), 0).unwrap();
    assert_eq!(
        event.created_at,
        chrono::DateTime::parse_from_rfc3339("2026-07-25T10:00:00Z")
            .unwrap()
            .with_timezone(&chrono::Utc)
    );
}

#[test]
fn coverage_telemetry_027_offset_timestamp_is_normalized_to_utc() {
    let input = proto::TelemetryEventInput {
        created_at: "2026-07-25T12:30:00+02:30".into(),
        ..valid_input()
    };
    let event = validate(0, &input, 0).unwrap();
    assert_eq!(
        event.created_at,
        chrono::DateTime::parse_from_rfc3339("2026-07-25T10:00:00Z")
            .unwrap()
            .with_timezone(&chrono::Utc)
    );
}

rejection_case!(
    coverage_telemetry_028_garbage_timestamp_is_rejected,
    proto::TelemetryEventInput {
        created_at: "not-a-timestamp".into(),
        ..valid_input()
    },
    "invalid_created_at"
);

rejection_case!(
    coverage_telemetry_029_empty_timestamp_is_rejected,
    proto::TelemetryEventInput {
        created_at: String::new(),
        ..valid_input()
    },
    "invalid_created_at"
);

rejection_case!(
    coverage_telemetry_030_impossible_calendar_date_is_rejected,
    proto::TelemetryEventInput {
        created_at: "2026-13-45T99:99:99Z".into(),
        ..valid_input()
    },
    "invalid_created_at"
);

#[test]
fn coverage_telemetry_031_tenant_is_stamped_onto_accepted_event() {
    let event = validate(0, &valid_input(), 0).unwrap();
    assert_eq!(event.tenant_id, TENANT);
}

#[test]
fn coverage_telemetry_032_rejection_carries_the_event_index() {
    let input = proto::TelemetryEventInput {
        payload_json: "not-json".into(),
        ..valid_input()
    };
    let rejection = validate(7, &input, 0).unwrap_err();
    assert_eq!(rejection.index, 7);
}

#[test]
fn coverage_telemetry_033_bounded_field_allows_exact_limit() {
    assert!(bounded_telemetry_field("abcd", 4));
}

#[test]
fn coverage_telemetry_034_bounded_field_rejects_one_over_limit() {
    assert!(!bounded_telemetry_field("abcde", 4));
}

#[test]
fn coverage_telemetry_035_bounded_field_allows_empty_value() {
    assert!(bounded_telemetry_field("", 0));
}

#[test]
fn coverage_telemetry_036_rejection_builder_sets_all_fields() {
    let rejection = telemetry_rejection(3, "some_code", "some message");
    assert_eq!(rejection.index, 3);
    assert_eq!(rejection.code, "some_code");
    assert_eq!(rejection.message, "some message");
}

// --- the IngestTelemetryBatch RPC against in-memory storage ---

async fn service() -> Orch8GrpcService {
    let storage: Arc<dyn StorageBackend> = Arc::new(
        orch8_storage::sqlite::SqliteStorage::in_memory()
            .await
            .unwrap(),
    );
    Orch8GrpcService::new(storage)
}

fn batch_request(
    body_tenant: &str,
    events: Vec<proto::TelemetryEventInput>,
    caller: Option<&TenantId>,
) -> Request<proto::IngestTelemetryBatchRequest> {
    stream_request(
        proto::IngestTelemetryBatchRequest {
            tenant_id: body_tenant.into(),
            events,
        },
        caller,
    )
}

#[tokio::test]
async fn coverage_telemetry_037_batch_without_any_tenant_is_invalid_argument() {
    let service = service().await;
    let status = service
        .ingest_telemetry_batch(batch_request("", vec![valid_input()], None))
        .await
        .unwrap_err();
    assert_eq!(status.code(), tonic::Code::InvalidArgument);
}

#[tokio::test]
async fn coverage_telemetry_038_caller_tenant_fills_empty_body_tenant() {
    let service = service().await;
    let caller = TenantId::unchecked("caller-tenant");
    let response = service
        .ingest_telemetry_batch(batch_request("", vec![valid_input()], Some(&caller)))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(response.accepted, 1);
}

#[tokio::test]
async fn coverage_telemetry_039_conflicting_body_tenant_is_permission_denied() {
    let service = service().await;
    let caller = TenantId::unchecked("caller-tenant");
    let status = service
        .ingest_telemetry_batch(batch_request(
            "other-tenant",
            vec![valid_input()],
            Some(&caller),
        ))
        .await
        .unwrap_err();
    assert_eq!(status.code(), tonic::Code::PermissionDenied);
}

#[tokio::test]
async fn coverage_telemetry_040_valid_batch_reports_all_events_accepted() {
    let service = service().await;
    let response = service
        .ingest_telemetry_batch(batch_request(
            TENANT,
            vec![valid_input(), valid_input(), valid_input()],
            None,
        ))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(response.accepted, 3);
    assert!(response.rejected.is_empty());
}

#[tokio::test]
async fn coverage_telemetry_041_mixed_batch_reports_indexed_rejections() {
    let service = service().await;
    let mut invalid_type = valid_input();
    invalid_type.event_type = String::new();
    let mut invalid_json = valid_input();
    invalid_json.payload_json = "not-json".into();
    let response = service
        .ingest_telemetry_batch(batch_request(
            TENANT,
            vec![valid_input(), invalid_type, valid_input(), invalid_json],
            None,
        ))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(response.accepted, 2);
    assert_eq!(response.rejected.len(), 2);
    assert_eq!(response.rejected[0].index, 1);
    assert_eq!(response.rejected[0].code, "invalid_event_type");
    assert_eq!(response.rejected[1].index, 3);
    assert_eq!(response.rejected[1].code, "invalid_payload_json");
}

#[tokio::test]
async fn coverage_telemetry_042_event_1001_is_rejected_beyond_batch_limit() {
    let service = service().await;
    let events = (0..=MAX_TELEMETRY_EVENTS)
        .map(|_| valid_input())
        .collect::<Vec<_>>();
    let response = service
        .ingest_telemetry_batch(batch_request(TENANT, events, None))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(response.accepted, 1_000);
    assert_eq!(response.rejected.len(), 1);
    assert_eq!(response.rejected[0].index, 1_000);
    assert_eq!(response.rejected[0].code, "batch_event_limit_exceeded");
}

#[tokio::test]
async fn coverage_telemetry_043_empty_batch_is_accepted_as_zero() {
    let service = service().await;
    let response = service
        .ingest_telemetry_batch(batch_request(TENANT, Vec::new(), None))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(response.accepted, 0);
    assert!(response.rejected.is_empty());
}

#[tokio::test]
async fn coverage_telemetry_044_cumulative_payload_ceiling_rejects_the_overflowing_event() {
    let service = service().await;
    // Sixteen max-size events exactly fill the 4 MiB batch budget; the
    // seventeenth must be rejected while the rest are accepted.
    let full_payload = valid_input();
    let events = (0..17)
        .map(|_| proto::TelemetryEventInput {
            payload_json: json_of_size(MAX_TELEMETRY_EVENT_PAYLOAD_BYTES),
            ..full_payload.clone()
        })
        .collect::<Vec<_>>();
    let response = service
        .ingest_telemetry_batch(batch_request(TENANT, events, None))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(response.accepted, 16);
    assert_eq!(response.rejected.len(), 1);
    assert_eq!(response.rejected[0].index, 16);
    assert_eq!(response.rejected[0].code, "batch_payload_limit_exceeded");
}

#[tokio::test]
async fn coverage_telemetry_045_events_after_a_rejection_still_count_toward_budget() {
    let service = service().await;
    // Index 0 is malformed JSON and consumes no budget; the next sixteen
    // max-size events exactly fill the 4 MiB batch budget, so the trailing
    // valid event must be rejected for the budget — proving a rejection does
    // not free or skip budget accounting for the events that follow it.
    let full_budget = proto::TelemetryEventInput {
        payload_json: json_of_size(MAX_TELEMETRY_EVENT_PAYLOAD_BYTES),
        ..valid_input()
    };
    let mut malformed = valid_input();
    malformed.payload_json = "not-json".into();
    let mut events = vec![malformed];
    events.extend((0..16).map(|_| full_budget.clone()));
    events.push(valid_input());
    let response = service
        .ingest_telemetry_batch(batch_request(TENANT, events, None))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(response.accepted, 16);
    assert_eq!(response.rejected.len(), 2);
    assert_eq!(response.rejected[0].index, 0);
    assert_eq!(response.rejected[0].code, "invalid_payload_json");
    assert_eq!(response.rejected[1].index, 17);
    assert_eq!(response.rejected[1].code, "batch_payload_limit_exceeded");
}

#[test]
fn coverage_telemetry_046_empty_metadata_fields_are_accepted() {
    let input = proto::TelemetryEventInput {
        device_id: String::new(),
        os_name: String::new(),
        os_version: String::new(),
        app_version: String::new(),
        sdk_version: String::new(),
        ..valid_input()
    };
    let event = validate(0, &input, 0).unwrap();
    assert!(event.device_id.is_empty());
    assert!(event.os_name.is_empty());
    assert!(event.os_version.is_empty());
    assert!(event.app_version.is_empty());
    assert!(event.sdk_version.is_empty());
}
