//! Bounded telemetry ingress: every event field is limited in UTF-8 *bytes*
//! (not chars), a single payload is capped at 256 KiB and a batch at 4 MiB of
//! accepted payload (saturating, never wrapping), a batch holds at most 1,000
//! events, and invalid events are rejected individually by index while the
//! rest of the batch is still ingested under the caller's tenant.

use super::*;
use orch8_storage::telemetry_validation::{
    MAX_TELEMETRY_BATCH_PAYLOAD_BYTES, MAX_TELEMETRY_EVENT_PAYLOAD_BYTES,
};

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
        app_version: "3.4.5".into(),
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

fn utc(timestamp: &str) -> chrono::DateTime<chrono::Utc> {
    chrono::DateTime::parse_from_rfc3339(timestamp)
        .unwrap()
        .with_timezone(&chrono::Utc)
}

#[test]
fn accepted_event_keeps_each_field_and_is_stamped_with_the_tenant() {
    let event = validate(0, &valid_input(), 0).unwrap();
    assert_eq!(event.event_type, "worker.started");
    assert_eq!(event.payload, r#"{"worker":"alpha"}"#);
    assert_eq!(event.device_id, "device-1");
    assert_eq!(event.os_name, "linux");
    assert_eq!(event.os_version, "6.12");
    assert_eq!(event.app_version, "3.4.5");
    assert_eq!(event.sdk_version, "1.0.0");
    assert_eq!(event.created_at, utc("2026-07-25T10:00:00Z"));
    assert_eq!(event.tenant_id, TENANT);
}

#[test]
fn invalid_events_are_rejected_with_a_specific_code() {
    let cases = [
        (
            "empty event type",
            proto::TelemetryEventInput {
                event_type: String::new(),
                ..valid_input()
            },
            "invalid_event_type",
        ),
        (
            "whitespace event type",
            proto::TelemetryEventInput {
                event_type: "   ".into(),
                ..valid_input()
            },
            "invalid_event_type",
        ),
        (
            "129-byte event type",
            proto::TelemetryEventInput {
                event_type: "e".repeat(129),
                ..valid_input()
            },
            "invalid_event_type",
        ),
        (
            "65 x 'é' = 130-byte event type",
            proto::TelemetryEventInput {
                event_type: "é".repeat(65),
                ..valid_input()
            },
            "invalid_event_type",
        ),
        (
            "payload one byte over 256 KiB",
            proto::TelemetryEventInput {
                payload_json: json_of_size(MAX_TELEMETRY_EVENT_PAYLOAD_BYTES + 1),
                ..valid_input()
            },
            "payload_too_large",
        ),
        (
            "non-JSON payload",
            proto::TelemetryEventInput {
                payload_json: "not-json".into(),
                ..valid_input()
            },
            "invalid_payload_json",
        ),
        (
            "garbage timestamp",
            proto::TelemetryEventInput {
                created_at: "not-a-timestamp".into(),
                ..valid_input()
            },
            "invalid_created_at",
        ),
    ];
    for (case, input, code) in cases {
        let rejection = validate(0, &input, 0).expect_err(case);
        assert_eq!(rejection.code, code, "case: {case}");
    }
}

#[test]
fn metadata_fields_are_bounded_in_bytes_and_name_the_offending_field() {
    type Setter = fn(&mut proto::TelemetryEventInput, String);
    let fields: [(&str, usize, Setter); 5] = [
        ("device_id", 256, |input, value| input.device_id = value),
        ("os_name", 64, |input, value| input.os_name = value),
        ("os_version", 64, |input, value| input.os_version = value),
        ("app_version", 64, |input, value| input.app_version = value),
        ("sdk_version", 64, |input, value| input.sdk_version = value),
    ];
    for (field, limit, set) in fields {
        let mut empty = valid_input();
        set(&mut empty, String::new());
        assert!(validate(0, &empty, 0).is_ok(), "{field}: empty is allowed");

        let mut at_limit = valid_input();
        set(&mut at_limit, "m".repeat(limit));
        assert!(validate(0, &at_limit, 0).is_ok(), "{field}: {limit} bytes");

        let mut over = valid_input();
        set(&mut over, "m".repeat(limit + 1));
        let rejection = validate(0, &over, 0).expect_err(field);
        assert_eq!(rejection.code, "metadata_too_large", "{field}");
        assert!(
            rejection.message.contains(field),
            "{field}: {}",
            rejection.message
        );
    }
}

#[test]
fn event_type_is_trimmed_on_accept() {
    let input = proto::TelemetryEventInput {
        event_type: "  worker.ready  ".into(),
        ..valid_input()
    };
    let event = validate(0, &input, 0).unwrap();
    assert_eq!(event.event_type, "worker.ready");
}

#[test]
fn event_type_limit_is_128_utf8_bytes() {
    for (case, event_type) in [
        ("128 ascii bytes", "e".repeat(128)),
        ("64 x 'é' = 128 bytes", "é".repeat(64)),
    ] {
        let input = proto::TelemetryEventInput {
            event_type,
            ..valid_input()
        };
        assert!(validate(0, &input, 0).is_ok(), "case: {case}");
    }
}

#[test]
fn payload_of_exactly_256_kib_is_accepted() {
    let input = proto::TelemetryEventInput {
        payload_json: json_of_size(MAX_TELEMETRY_EVENT_PAYLOAD_BYTES),
        ..valid_input()
    };
    assert!(validate(0, &input, 0).is_ok());
}

#[test]
fn json_null_payload_is_accepted() {
    let input = proto::TelemetryEventInput {
        payload_json: "null".into(),
        ..valid_input()
    };
    let event = validate(0, &input, 0).unwrap();
    assert_eq!(event.payload, "null");
}

#[test]
fn batch_budget_admits_an_event_that_lands_exactly_on_4_mib() {
    let input = valid_input();
    let headroom = MAX_TELEMETRY_BATCH_PAYLOAD_BYTES - input.payload_json.len();
    assert!(validate(0, &input, headroom).is_ok());
}

#[test]
fn batch_budget_rejects_an_event_one_byte_past_4_mib() {
    let input = valid_input();
    let headroom = MAX_TELEMETRY_BATCH_PAYLOAD_BYTES - input.payload_json.len() + 1;
    let rejection = validate(0, &input, headroom).unwrap_err();
    assert_eq!(rejection.code, "batch_payload_limit_exceeded");
}

#[test]
fn batch_budget_accounting_saturates_instead_of_wrapping() {
    let rejection = validate(0, &valid_input(), usize::MAX).unwrap_err();
    assert_eq!(rejection.code, "batch_payload_limit_exceeded");
}

#[test]
fn offset_timestamp_is_normalized_to_utc() {
    let input = proto::TelemetryEventInput {
        created_at: "2026-07-25T12:30:00+02:30".into(),
        ..valid_input()
    };
    let event = validate(0, &input, 0).unwrap();
    assert_eq!(event.created_at, utc("2026-07-25T10:00:00Z"));
}

#[test]
fn rejection_carries_the_event_index() {
    let input = proto::TelemetryEventInput {
        payload_json: "not-json".into(),
        ..valid_input()
    };
    let rejection = validate(7, &input, 0).unwrap_err();
    assert_eq!(rejection.index, 7);
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
async fn batch_without_any_tenant_is_invalid_argument() {
    let service = service().await;
    let status = service
        .ingest_telemetry_batch(batch_request("", vec![valid_input()], None))
        .await
        .unwrap_err();
    assert_eq!(status.code(), tonic::Code::InvalidArgument);
}

#[tokio::test]
async fn caller_tenant_satisfies_an_empty_body_tenant() {
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
async fn body_tenant_conflicting_with_caller_is_permission_denied() {
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
async fn valid_batch_reports_every_event_accepted() {
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
async fn events_beyond_the_1000_event_cap_are_rejected_individually() {
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
async fn empty_batch_is_accepted_as_zero_events() {
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
async fn cumulative_payload_ceiling_rejects_the_overflowing_event() {
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
async fn rejected_events_consume_no_batch_budget() {
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
