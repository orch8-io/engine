//! Coverage tests for the resumable tenant change-feed cursor.
//!
//! Count contract: 12 independently named unit tests.

use super::*;

fn entry(id: Uuid, created_at: DateTime<Utc>) -> AuditLogEntry {
    AuditLogEntry {
        id,
        instance_id: InstanceId::new(),
        tenant_id: TenantId::unchecked("tenant_1"),
        event_type: "state_transition".into(),
        from_state: None,
        to_state: None,
        block_id: None,
        details: serde_json::json!({}),
        created_at,
    }
}

fn cursor() -> ChangeCursor {
    ChangeCursor {
        created_at: Utc::now(),
        id: Uuid::now_v7(),
    }
}

#[test]
fn coverage_audit_001_cursor_from_entry_copies_the_position() {
    let id = Uuid::now_v7();
    let created_at = Utc::now();
    let cursor = ChangeCursor::from(&entry(id, created_at));
    assert_eq!(cursor.id, id);
    assert_eq!(cursor.created_at, created_at);
}

#[test]
fn coverage_audit_002_cursor_equality_ignores_non_position_fields() {
    let id = Uuid::now_v7();
    let created_at = Utc::now();
    let mut other = entry(id, created_at);
    other.event_type = "signal_received".into();
    other.details = serde_json::json!({"payload": [1, 2, 3]});
    other.block_id = Some("step_9".into());
    assert_eq!(
        ChangeCursor::from(&entry(id, created_at)),
        ChangeCursor::from(&other)
    );
}

#[test]
fn coverage_audit_003_cursor_serde_round_trip() {
    let original = cursor();
    let json = serde_json::to_string(&original).unwrap();
    let back: ChangeCursor = serde_json::from_str(&json).unwrap();
    assert_eq!(back, original);
}

#[test]
fn coverage_audit_004_cursor_json_uses_created_at_and_id_keys() {
    let value = serde_json::to_value(cursor()).unwrap();
    assert!(value.get("created_at").is_some());
    assert!(value.get("id").is_some());
    assert_eq!(value.as_object().unwrap().len(), 2);
}

#[test]
fn coverage_audit_005_cursor_rejects_a_payload_missing_id() {
    let payload = serde_json::json!({"created_at": Utc::now()});
    assert!(serde_json::from_value::<ChangeCursor>(payload).is_err());
}

#[test]
fn coverage_audit_006_cursor_rejects_a_payload_missing_created_at() {
    let payload = serde_json::json!({"id": Uuid::now_v7()});
    assert!(serde_json::from_value::<ChangeCursor>(payload).is_err());
}

#[test]
fn coverage_audit_007_cursor_equality_requires_the_same_id() {
    let at = Utc::now();
    let a = ChangeCursor {
        created_at: at,
        id: Uuid::now_v7(),
    };
    let b = ChangeCursor {
        created_at: at,
        id: Uuid::now_v7(),
    };
    assert_ne!(a, b);
}

#[test]
fn coverage_audit_008_cursor_equality_requires_the_same_timestamp() {
    let id = Uuid::now_v7();
    let a = ChangeCursor {
        created_at: Utc::now(),
        id,
    };
    let b = ChangeCursor {
        created_at: a.created_at + chrono::Duration::milliseconds(1),
        id,
    };
    assert_ne!(a, b);
}

#[test]
fn coverage_audit_009_cursor_is_copy() {
    let a = cursor();
    let b = a;
    // Both bindings remain usable, which only compiles for `Copy`.
    assert_eq!(a, b);
    assert_eq!(a.id, b.id);
}

#[test]
fn coverage_audit_010_cursor_from_borrowed_entry_leaves_entry_intact() {
    let source = entry(Uuid::now_v7(), Utc::now());
    let cursor = ChangeCursor::from(&source);
    // The borrow ends with the conversion; the entry is still fully usable.
    assert_eq!(source.id, cursor.id);
    assert_eq!(source.event_type, "state_transition");
}

#[test]
fn coverage_audit_011_cursor_round_trip_preserves_subsecond_precision() {
    let created_at = DateTime::parse_from_rfc3339("2026-07-25T12:34:56.123456789Z")
        .unwrap()
        .with_timezone(&Utc);
    let original = ChangeCursor {
        created_at,
        id: Uuid::now_v7(),
    };
    let back: ChangeCursor =
        serde_json::from_str(&serde_json::to_string(&original).unwrap()).unwrap();
    assert_eq!(back, original);
}

#[test]
fn coverage_audit_012_cursor_rejects_a_non_uuid_id() {
    let payload = serde_json::json!({"created_at": Utc::now(), "id": "not-a-uuid"});
    assert!(serde_json::from_value::<ChangeCursor>(payload).is_err());
}
