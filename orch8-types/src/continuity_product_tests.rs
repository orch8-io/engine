use chrono::{Duration, Utc};

use super::*;

fn capabilities(kind: RuntimeKind, trust: RuntimeTrustLevel) -> RuntimeCapabilities {
    let now = Utc::now();
    RuntimeCapabilities {
        runtime_id: RuntimeId::new(),
        kind,
        trust,
        handlers: vec!["tool.run".into(), "device.biometric.verify".into()],
        plugins: vec![],
        credentials: vec!["source_control".into()],
        regions: vec!["us".into()],
        hardware: vec!["secure_enclave".into()],
        offline_capable: true,
        connectivity: Some(RuntimeConnectivity::Wifi),
        battery_percent: Some(80),
        estimated_cost_microunits: None,
        estimated_latency_ms: None,
        draining: false,
        capsule_signing_public_key: Some("key".into()),
        observed_at: now,
        expires_at: now + Duration::hours(1),
    }
}

fn offer(now: DateTime<Utc>) -> PortableWorkOffer {
    PortableWorkOffer {
        id: Uuid::now_v7(),
        protocol: CURRENT_PROTOCOL,
        tenant_id: TenantId::new("acme").unwrap(),
        continuity_id: ContinuityId::new(),
        expected_epoch: ExecutionEpoch::initial(),
        handler: "tool.run".into(),
        input: serde_json::json!({}),
        input_schema: serde_json::json!({"type":"object"}),
        requirements: CapsuleRequirements {
            handlers: vec!["tool.run".into()],
            ..CapsuleRequirements::default()
        },
        policy: None,
        classification: DataClassification::Internal,
        idempotency_key: "once".into(),
        receipt_required: true,
        expires_at: now + Duration::minutes(5),
    }
}

#[test]
fn opportunity_01_protocol_and_03_work_offer_are_versioned_and_claimable() {
    let now = Utc::now();
    let offer = offer(now);
    assert!(offer.can_be_claimed_by(
        &capabilities(RuntimeKind::Desktop, RuntimeTrustLevel::Signed),
        now
    ));
    let mut incompatible = offer;
    incompatible.protocol.major += 1;
    assert!(matches!(
        incompatible.validate(now),
        Err(ProductContractError::UnsupportedProtocol { .. })
    ));
}

#[test]
fn work_offer_claim_enforces_locality_policy_not_only_handler_requirements() {
    let now = Utc::now();
    let mut offer = offer(now);
    offer.classification = DataClassification::Restricted;
    offer.policy = Some(
        compile_placement_policy("classification=restricted;runtime_kinds=mobile;regions=br")
            .unwrap()
            .policy,
    );
    let desktop = capabilities(RuntimeKind::Desktop, RuntimeTrustLevel::Attested);
    assert!(!offer.can_be_claimed_by(&desktop, now));
}

#[test]
fn opportunity_04_runtime_passport_binds_runtime_and_capabilities() {
    let now = Utc::now();
    let caps = capabilities(RuntimeKind::Mobile, RuntimeTrustLevel::Attested);
    let issued_at = caps.observed_at;
    let mut passport = RuntimePassport {
        protocol: CURRENT_PROTOCOL,
        runtime_id: caps.runtime_id,
        issuer: "acme-ca".into(),
        subject: "phone".into(),
        public_key: "pk".into(),
        attestation_sha256: Some("abc".into()),
        issued_at,
        expires_at: issued_at + Duration::minutes(30),
        signature: "sig".into(),
        capabilities: caps,
    };
    assert_eq!(passport.validate(now), Ok(()));
    assert_eq!(
        passport.verify_with(now, |payload, key, signature| {
            !payload.is_empty() && key == "pk" && signature == "sig"
        }),
        Ok(())
    );
    passport.runtime_id = RuntimeId::new();
    assert_eq!(
        passport.validate(now),
        Err(ProductContractError::PassportCapabilityMismatch)
    );
}

#[test]
fn opportunity_05_and_25_execution_receipt_is_tamper_evident_bill_of_execution() {
    let now = Utc::now();
    let mut receipt = ExecutionReceipt {
        protocol: CURRENT_PROTOCOL,
        receipt_id: Uuid::now_v7(),
        tenant_id: TenantId::new("acme").unwrap(),
        continuity_id: ContinuityId::new(),
        instance_id: InstanceId::new(),
        final_epoch: ExecutionEpoch::initial(),
        sequence_sha256: "sequence".into(),
        model_ids: vec!["local-model".into()],
        tool_ids: vec!["tool.run".into()],
        locations: vec![],
        effects: vec![],
        policy_outcome: PolicyOutcome::Allow,
        consent_receipt_ids: vec![],
        previous_receipt_sha256: None,
        created_at: now,
        digest_sha256: String::new(),
        signing_key_id: None,
        signature: None,
    };
    receipt.seal();
    assert_eq!(receipt.verify(), Ok(()));
    receipt.tool_ids.push("tampered".into());
    assert_eq!(
        receipt.verify(),
        Err(ProductContractError::ReceiptDigestMismatch)
    );
}

#[test]
fn opportunity_06_policy_language_compiles_to_core_requirements() {
    let compiled = compile_placement_policy("classification=restricted;runtime_kinds=mobile;min_trust=attested;human_ui=true;regions=us,br;handlers=device.biometric.verify").unwrap();
    assert_eq!(compiled.classification, DataClassification::Restricted);
    assert_eq!(
        compiled.requirements.minimum_trust,
        Some(RuntimeTrustLevel::Attested)
    );
    assert!(compiled.requirements.requires_human_ui);
    assert_eq!(
        compiled.policy.rules[0].allowed_runtime_kinds,
        vec![RuntimeKind::Mobile]
    );
}

#[test]
fn opportunity_10_11_12_14_gateway_wrap_mcp_and_local_worker_share_one_manifest() {
    for adapter in [
        GatewayAdapter::GenericHttp,
        GatewayAdapter::Mcp,
        GatewayAdapter::LocalProcess,
        GatewayAdapter::MobileWorker,
    ] {
        let manifest = GatewayManifest {
            protocol: CURRENT_PROTOCOL,
            name: "wrapped-agent".into(),
            adapter,
            entrypoint: "./agent".into(),
            arguments: vec![],
            handler: "tool.run".into(),
            policy_source: "classification=internal;handlers=tool.run".into(),
            environment_allowlist: vec!["PATH".into()],
            secret_references: vec!["vault://token".into()],
            receipt_required: true,
        };
        assert!(manifest.validate().is_ok());
    }
}

fn passing_results() -> Vec<ConformanceCheckResult> {
    [
        ConformanceCheck::ProtocolCompatibility,
        ConformanceCheck::AtomicOwnership,
        ConformanceCheck::StaleOwnerRejected,
        ConformanceCheck::DuplicateEffectFenced,
        ConformanceCheck::OfflineResume,
        ConformanceCheck::PolicyEnforced,
        ConformanceCheck::ReceiptVerifiable,
        ConformanceCheck::TenantIsolation,
    ]
    .into_iter()
    .map(|check| ConformanceCheckResult {
        check,
        passed: true,
        evidence_sha256: sha256_hex(format!("{check:?}")),
        duration_ms: 1,
        finding: None,
    })
    .collect()
}

#[test]
fn opportunity_15_16_17_gauntlet_chaos_and_score_cover_continuity_invariants() {
    let results = passing_results();
    let score = score_conformance(&results);
    assert_eq!(score.score_millipoints, 1_000);
    assert!(
        results
            .iter()
            .any(|item| item.check == ConformanceCheck::DuplicateEffectFenced)
    );
    assert!(
        results
            .iter()
            .any(|item| item.check == ConformanceCheck::OfflineResume)
    );
}

#[test]
fn opportunity_18_and_42_certification_and_conformance_cloud_reject_mandatory_failure() {
    let now = Utc::now();
    let certificate = issue_conformance_certificate(
        "sdk".into(),
        "orch8".into(),
        &passing_results(),
        now,
        now + Duration::days(30),
    )
    .unwrap();
    assert!(certificate.badge_svg().contains("certified 100%"));
    let mut failed = passing_results();
    failed[1].passed = false;
    assert_eq!(
        issue_conformance_certificate(
            "sdk".into(),
            "orch8".into(),
            &failed,
            now,
            now + Duration::days(30)
        ),
        Err(ProductContractError::CertificationThresholdNotMet)
    );
}

#[test]
fn certification_badge_escapes_untrusted_subject() {
    let now = Utc::now();
    let certificate = issue_conformance_certificate(
        "<script>alert(1)</script>".into(),
        "orch8".into(),
        &passing_results(),
        now,
        now + Duration::days(30),
    )
    .unwrap();
    let svg = certificate.badge_svg();
    assert!(!svg.contains("<script>"));
    assert!(svg.contains("&lt;script&gt;"));
}

#[test]
fn certification_cannot_be_gamed_by_omitting_mandatory_checks() {
    let only_one = vec![passing_results().remove(0)];
    let score = score_conformance(&only_one);
    assert_eq!(score.total, ConformanceCheck::ALL.len());
    assert!(!score.mandatory_failures.is_empty());
}

#[test]
fn opportunities_21_22_23_private_rag_biometric_and_residency_are_compilable() {
    for profile in [
        TrustBoundaryProfile::PrivateRag,
        TrustBoundaryProfile::BiometricApproval,
        TrustBoundaryProfile::DataResidency,
    ] {
        let contract = profile.contract();
        assert!(profile.compile().is_ok());
        assert!(contract.requires_signed_receipt);
        assert!(!contract.forbidden_payload_classes.is_empty());
    }
}

#[test]
fn opportunities_29_31_32_audit_coding_and_onboarding_are_deployable_profiles() {
    for profile in [
        TrustBoundaryProfile::AuditEvidence,
        TrustBoundaryProfile::SecretSafeCoding,
        TrustBoundaryProfile::RegulatedOnboarding,
    ] {
        assert!(profile.compile().is_ok());
        assert!(!profile.contract().required_evidence.is_empty());
    }
}

#[test]
fn opportunities_35_36_40_fraud_airlock_and_vault_require_attested_trust() {
    for profile in [
        TrustBoundaryProfile::FraudChallenge,
        TrustBoundaryProfile::ExecutiveAirlock,
        TrustBoundaryProfile::PersonalDataVault,
    ] {
        let compiled = profile.compile().unwrap();
        assert_eq!(
            compiled.requirements.minimum_trust,
            Some(RuntimeTrustLevel::Attested)
        );
    }
}

#[test]
fn every_vertical_profile_builds_a_valid_portable_work_offer() {
    let now = Utc::now();
    for profile in TrustBoundaryProfile::ALL {
        let offer = profile
            .work_offer(
                TenantId::new("acme").unwrap(),
                ContinuityId::new(),
                ExecutionEpoch::initial(),
                serde_json::json!({}),
                format!("{profile:?}"),
                now,
            )
            .unwrap();
        assert!(offer.receipt_required);
        assert!(offer.policy.is_some());
    }
}

#[test]
fn opportunities_41_42_hosted_relay_and_conformance_plan_enforce_safety() {
    let plan = CommercialContinuityPlan {
        deployment: RelayDeployment::Hosted,
        tenant_isolation: true,
        signed_receipts: true,
        conformance_required: true,
        evidence_retention_days: 30,
        allowed_protocol_majors: vec![1],
        oem_product_id: None,
        usage_meter: Some("verified_handoff".into()),
    };
    assert_eq!(plan.validate(), Ok(()));
}

#[test]
fn opportunity_44_oem_contract_requires_product_identity() {
    let mut plan = CommercialContinuityPlan {
        deployment: RelayDeployment::OemEmbedded,
        tenant_isolation: true,
        signed_receipts: true,
        conformance_required: true,
        evidence_retention_days: 30,
        allowed_protocol_majors: vec![1],
        oem_product_id: None,
        usage_meter: Some("handoff".into()),
    };
    assert!(plan.validate().is_err());
    plan.oem_product_id = Some("com.example.agent".into());
    assert_eq!(plan.validate(), Ok(()));
}

#[test]
fn all_high_score_profiles_have_unique_handlers_and_valid_policies() {
    let profiles = [
        TrustBoundaryProfile::PrivateRag,
        TrustBoundaryProfile::BiometricApproval,
        TrustBoundaryProfile::DataResidency,
        TrustBoundaryProfile::BillOfExecution,
        TrustBoundaryProfile::AuditEvidence,
        TrustBoundaryProfile::SecretSafeCoding,
        TrustBoundaryProfile::RegulatedOnboarding,
        TrustBoundaryProfile::FraudChallenge,
        TrustBoundaryProfile::ExecutiveAirlock,
        TrustBoundaryProfile::PersonalDataVault,
    ];
    let handlers = profiles
        .iter()
        .map(|profile| profile.contract().handler)
        .collect::<BTreeSet<_>>();
    assert_eq!(handlers.len(), profiles.len());
    assert!(
        profiles
            .into_iter()
            .all(|profile| profile.compile().is_ok())
    );
}

#[test]
fn work_offer_rejects_expired_oversized_and_unsafe_contracts() {
    let now = Utc::now();
    let mut candidate = offer(now);
    candidate.expires_at = now;
    assert_eq!(
        candidate.validate(now),
        Err(ProductContractError::OfferExpired)
    );

    candidate = offer(now);
    candidate.expires_at = now + Duration::hours(25);
    assert!(matches!(
        candidate.validate(now),
        Err(ProductContractError::InvalidPolicy(message)) if message.contains("24 hours")
    ));

    candidate = offer(now);
    candidate.input_schema = serde_json::json!("object");
    assert!(matches!(
        candidate.validate(now),
        Err(ProductContractError::InvalidPolicy(_))
    ));

    candidate = offer(now);
    candidate.input =
        serde_json::json!({"payload": "x".repeat(PortableWorkOffer::MAX_INPUT_BYTES)});
    assert!(matches!(
        candidate.validate(now),
        Err(ProductContractError::InvalidPolicy(_))
    ));
}

#[test]
fn runtime_passport_rejects_bad_signature_and_invalid_lifetime() {
    let now = Utc::now();
    let caps = capabilities(RuntimeKind::Mobile, RuntimeTrustLevel::Attested);
    let mut passport = RuntimePassport {
        protocol: CURRENT_PROTOCOL,
        runtime_id: caps.runtime_id,
        issuer: "issuer".into(),
        subject: "device".into(),
        public_key: "public-key".into(),
        attestation_sha256: None,
        issued_at: caps.observed_at,
        expires_at: caps.observed_at + Duration::minutes(30),
        signature: "signature".into(),
        capabilities: caps,
    };
    assert_eq!(
        passport.verify_with(now, |_, _, _| false),
        Err(ProductContractError::InvalidSignature)
    );
    passport.expires_at = passport.issued_at + Duration::hours(25);
    assert_eq!(
        passport.validate(now),
        Err(ProductContractError::PassportCapabilityMismatch)
    );
}

#[test]
fn execution_receipt_signature_and_effect_resolution_are_enforced() {
    let now = Utc::now();
    let mut receipt = ExecutionReceipt {
        protocol: CURRENT_PROTOCOL,
        receipt_id: Uuid::now_v7(),
        tenant_id: TenantId::new("acme").unwrap(),
        continuity_id: ContinuityId::new(),
        instance_id: InstanceId::new(),
        final_epoch: ExecutionEpoch::initial(),
        sequence_sha256: "ab".repeat(32),
        model_ids: vec![],
        tool_ids: vec![],
        locations: vec![],
        effects: vec![],
        policy_outcome: PolicyOutcome::Allow,
        consent_receipt_ids: vec![],
        previous_receipt_sha256: None,
        created_at: now,
        digest_sha256: String::new(),
        signing_key_id: Some("key-1".into()),
        signature: Some("signature".into()),
    };
    receipt.seal();
    // `seal` deliberately drops stale signatures; signing must happen after sealing.
    receipt.signature = Some("signature".into());
    assert_eq!(
        receipt.verify_signature_with(|digest, signature| {
            digest == receipt.digest_sha256.as_bytes() && signature == "signature"
        }),
        Ok(())
    );
    assert_eq!(
        receipt.verify_signature_with(|_, _| false),
        Err(ProductContractError::InvalidSignature)
    );

    receipt.effects.push(crate::continuity::EffectReceipt {
        id: crate::continuity::EffectId::new(),
        tenant_id: receipt.tenant_id.clone(),
        continuity_id: receipt.continuity_id,
        epoch: receipt.final_epoch,
        instance_id: receipt.instance_id,
        block_id: crate::ids::BlockId::new("external-effect"),
        kind: crate::continuity::EffectKind::Http,
        state: crate::continuity::EffectState::Unknown,
        destination_fingerprint: "provider".into(),
        idempotency_key: Some("once".into()),
        request_sha256: "cd".repeat(32),
        provider_receipt_id: None,
        attempt: 1,
        created_at: now,
        updated_at: now,
    });
    receipt.seal();
    assert_eq!(
        receipt.verify(),
        Err(ProductContractError::ReceiptIncomplete)
    );
}

#[test]
fn policy_parser_rejects_unknown_duplicate_and_malformed_values() {
    for source in [
        "classification=internal;unknown=true",
        "classification=internal;classification=restricted",
        "human_ui=maybe",
        "minimum_battery=full",
        "runtime_kinds=teleporter",
        "connectivity=carrier_pigeon",
        "min_trust=absolute",
    ] {
        assert!(
            matches!(
                compile_placement_policy(source),
                Err(ProductContractError::InvalidPolicy(_))
            ),
            "policy should fail closed: {source}"
        );
    }
}

#[test]
fn policy_claim_requires_live_security_sensitive_facts() {
    let now = Utc::now();
    let mut candidate = offer(now);
    let compiled = compile_placement_policy(
        "classification=restricted;runtime_kinds=desktop;connectivity=wifi;minimum_battery=50;maximum_cost=100;maximum_latency_ms=20",
    )
    .unwrap();
    candidate.classification = compiled.classification;
    candidate.policy = Some(compiled.policy);
    let mut runtime = capabilities(RuntimeKind::Desktop, RuntimeTrustLevel::Attested);
    runtime.estimated_cost_microunits = Some(80);
    runtime.estimated_latency_ms = Some(10);
    assert!(candidate.can_be_claimed_by(&runtime, now));
    runtime.battery_percent = None;
    assert!(!candidate.can_be_claimed_by(&runtime, now));
}

#[test]
fn gateway_manifest_rejects_secret_values_duplicates_and_unbounded_arguments() {
    let base = GatewayManifest {
        protocol: CURRENT_PROTOCOL,
        name: "agent".into(),
        adapter: GatewayAdapter::LocalProcess,
        entrypoint: "./agent".into(),
        arguments: vec![],
        handler: "tool.run".into(),
        policy_source: "classification=internal".into(),
        environment_allowlist: vec![],
        secret_references: vec!["vault://token".into()],
        receipt_required: true,
    };
    let mut duplicate = base.clone();
    duplicate.secret_references.push("vault://token".into());
    assert!(duplicate.validate().is_err());

    let mut raw_secret = base.clone();
    raw_secret.secret_references = vec!["plaintext-token".into()];
    assert!(raw_secret.validate().is_err());

    let mut oversized = base;
    oversized.arguments = vec!["x".repeat(4_097)];
    assert!(oversized.validate().is_err());
}

#[test]
fn conformance_rejects_duplicate_checks_bad_evidence_and_invalid_certificate_windows() {
    let mut duplicate = passing_results();
    duplicate.push(duplicate[0].clone());
    let score = score_conformance(&duplicate);
    assert!(score.mandatory_failures.contains(&duplicate[0].check));

    let mut malformed = passing_results();
    malformed[0].evidence_sha256 = "z".repeat(64);
    assert!(score_conformance(&malformed).score_millipoints < 1_000);

    let now = Utc::now();
    assert!(
        issue_conformance_certificate(
            "sdk".into(),
            "orch8".into(),
            &passing_results(),
            now,
            now + Duration::days(367),
        )
        .is_err()
    );
}

#[test]
fn profile_names_and_each_commercial_invariant_are_fail_closed() {
    assert_eq!(
        "private_rag".parse::<TrustBoundaryProfile>().unwrap(),
        TrustBoundaryProfile::PrivateRag
    );
    assert!("private-rag".parse::<TrustBoundaryProfile>().is_err());

    let valid = CommercialContinuityPlan {
        deployment: RelayDeployment::Hosted,
        tenant_isolation: true,
        signed_receipts: true,
        conformance_required: true,
        evidence_retention_days: 30,
        allowed_protocol_majors: vec![CURRENT_PROTOCOL.major],
        oem_product_id: None,
        usage_meter: Some("handoff".into()),
    };
    let mut candidates = Vec::new();
    let mut plan = valid.clone();
    plan.tenant_isolation = false;
    candidates.push(plan);
    let mut plan = valid.clone();
    plan.signed_receipts = false;
    candidates.push(plan);
    let mut plan = valid.clone();
    plan.conformance_required = false;
    candidates.push(plan);
    let mut plan = valid.clone();
    plan.evidence_retention_days = 0;
    candidates.push(plan);
    let mut plan = valid;
    plan.allowed_protocol_majors.clear();
    candidates.push(plan);
    assert!(candidates.into_iter().all(|plan| plan.validate().is_err()));
}
