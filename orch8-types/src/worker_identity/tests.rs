use super::*;
fn proof() -> WorkerIdentityResponse {
    let now = Utc::now();
    WorkerIdentityResponse {
        version: 1,
        nonce: uuid::Uuid::new_v4().to_string(),
        tenant_id: TenantId::new("tenant").unwrap(),
        runtime_id: RuntimeId::new(),
        kind: RuntimeKind::Mobile,
        device_id: "device".into(),
        issued_at: now,
        expires_at: now + chrono::Duration::seconds(60),
    }
}
fn valid(p: &WorkerIdentityResponse, now: DateTime<Utc>) -> bool {
    p.validate_for(&p.nonce, &p.tenant_id, p.runtime_id, &p.device_id, now)
        .is_ok()
}
#[test]
fn nonce_is_canonical_non_nil_and_bounded() {
    for nonce in [
        String::new(),
        uuid::Uuid::nil().to_string(),
        uuid::Uuid::new_v4().simple().to_string(),
        uuid::Uuid::new_v4().to_string().to_uppercase(),
        "x".repeat(4096),
    ] {
        assert!(WorkerIdentityRequest { nonce }.validate().is_err());
    }
    assert!(
        WorkerIdentityRequest {
            nonce: uuid::Uuid::new_v4().to_string()
        }
        .validate()
        .is_ok()
    );
}
#[test]
fn freshness_bounds_include_expiry_and_clock_skew() {
    let p = proof();
    let now = p.issued_at;
    assert!(valid(&p, now));
    assert!(!valid(&p, p.expires_at));
    let mut changed = p.clone();
    changed.expires_at = now;
    assert!(!valid(&changed, now));
    changed.expires_at = now + chrono::Duration::seconds(61);
    assert!(!valid(&changed, now));
    changed = p.clone();
    changed.issued_at = now + chrono::Duration::seconds(31);
    assert!(!valid(&changed, now));
    changed.issued_at = now + chrono::Duration::seconds(30);
    assert!(valid(&changed, now));
    // Deserialized extreme timestamps must fail without overflowing arithmetic.
    changed.issued_at = DateTime::<Utc>::MAX_UTC;
    assert!(!valid(&changed, DateTime::<Utc>::MIN_UTC));
}
#[test]
fn probe_is_not_replayable_across_identity_or_protocol() {
    let p = proof();
    let now = p.issued_at;
    assert!(
        p.validate_for(
            &uuid::Uuid::new_v4().to_string(),
            &p.tenant_id,
            p.runtime_id,
            "device",
            now
        )
        .is_err()
    );
    assert!(
        p.validate_for(
            &p.nonce,
            &TenantId::new("other").unwrap(),
            p.runtime_id,
            "device",
            now
        )
        .is_err()
    );
    assert!(
        p.validate_for(&p.nonce, &p.tenant_id, RuntimeId::new(), "device", now)
            .is_err()
    );
    assert!(
        p.validate_for(&p.nonce, &p.tenant_id, p.runtime_id, "other", now)
            .is_err()
    );
    let mut changed = p.clone();
    changed.version = 2;
    assert!(!valid(&changed, now));
    changed = p.clone();
    changed.kind = RuntimeKind::Browser;
    assert!(!valid(&changed, now));
    for device in [
        String::new(),
        " ".into(),
        "private\n".into(),
        "x".repeat(257),
    ] {
        changed = p.clone();
        changed.device_id = device;
        assert!(!valid(&changed, now));
    }
    let mut wire = serde_json::to_value(&p).unwrap();
    wire["future_grant"] = serde_json::json!(true);
    assert!(serde_json::from_value::<WorkerIdentityResponse>(wire).is_err());
    assert!(
        serde_json::from_value::<WorkerIdentityRequest>(
            serde_json::json!({"nonce":p.nonce,"tenant":"other"})
        )
        .is_err()
    );
}
