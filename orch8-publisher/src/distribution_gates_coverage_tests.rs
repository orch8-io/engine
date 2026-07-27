//! Strict opportunity-delivery gate coverage for `ReleaseChannel::select`.
//!
//! Count contract: 21 independently named unit tests.

use chrono::Duration;
use chrono::TimeZone as _;
use orch8_types::continuity::{RuntimeConnectivity, RuntimeKind, RuntimeTrustLevel};

use super::*;

fn hash(byte: char) -> String {
    std::iter::repeat_n(byte, 64).collect()
}

fn runtime() -> RuntimeCapabilities {
    let now = Utc::now();
    RuntimeCapabilities {
        runtime_id: RuntimeId::new(),
        kind: RuntimeKind::Mobile,
        trust: RuntimeTrustLevel::Attested,
        handlers: vec![],
        plugins: vec![],
        credentials: vec![],
        regions: vec![],
        hardware: vec![],
        offline_capable: true,
        connectivity: None,
        battery_percent: Some(90),
        estimated_cost_microunits: None,
        estimated_latency_ms: None,
        draining: false,
        capsule_signing_public_key: None,
        observed_at: now,
        expires_at: now + Duration::minutes(5),
    }
}

fn channel_with(requirements: CapsuleRequirements) -> ReleaseChannel {
    let mut channel = ReleaseChannel::new("tenant-a", ReleaseChannelName::Stable);
    channel
        .promote(ChannelRelease {
            package_name: "acme/app".into(),
            version: "1.0.0".into(),
            content_hash: hash('a'),
            package_url: "/full".into(),
            requirements,
            selected_runtime_id: None,
            promoted_at: Utc::now(),
        })
        .unwrap();
    channel
}

fn network_channel() -> ReleaseChannel {
    channel_with(CapsuleRequirements {
        requires_network: true,
        ..CapsuleRequirements::default()
    })
}

fn human_ui_channel() -> ReleaseChannel {
    channel_with(CapsuleRequirements {
        requires_human_ui: true,
        ..CapsuleRequirements::default()
    })
}

macro_rules! connectivity_case {
    ($name:ident, $connectivity:expr, $accepted:expr) => {
        #[test]
        fn $name() {
            let mut runtime = runtime();
            runtime.connectivity = $connectivity;
            let channel = network_channel();
            let result = channel.select(&runtime, Utc::now());
            if $accepted {
                assert_eq!(result.map(|release| release.version.as_str()), Ok("1.0.0"));
            } else {
                assert!(
                    matches!(result, Err(DistributionError::Incompatible(_))),
                    "expected an Incompatible rejection, got: {result:?}"
                );
            }
        }
    };
}

connectivity_case!(
    coverage_distribution_gate_001_metered_satisfies_network_gate,
    Some(RuntimeConnectivity::Metered),
    true
);
connectivity_case!(
    coverage_distribution_gate_002_wifi_satisfies_network_gate,
    Some(RuntimeConnectivity::Wifi),
    true
);
connectivity_case!(
    coverage_distribution_gate_003_ethernet_satisfies_network_gate,
    Some(RuntimeConnectivity::Ethernet),
    true
);
connectivity_case!(
    coverage_distribution_gate_004_offline_fails_network_gate,
    Some(RuntimeConnectivity::Offline),
    false
);
connectivity_case!(
    coverage_distribution_gate_005_unknown_connectivity_fails_network_gate,
    None,
    false
);

macro_rules! human_ui_case {
    ($name:ident, $kind:expr, $accepted:expr) => {
        #[test]
        fn $name() {
            let mut runtime = runtime();
            runtime.kind = $kind;
            let channel = human_ui_channel();
            let result = channel.select(&runtime, Utc::now());
            if $accepted {
                assert_eq!(result.map(|release| release.version.as_str()), Ok("1.0.0"));
            } else {
                assert!(
                    matches!(result, Err(DistributionError::Incompatible(_))),
                    "expected an Incompatible rejection, got: {result:?}"
                );
            }
        }
    };
}

human_ui_case!(
    coverage_distribution_gate_006_mobile_satisfies_human_ui_gate,
    RuntimeKind::Mobile,
    true
);
human_ui_case!(
    coverage_distribution_gate_007_browser_satisfies_human_ui_gate,
    RuntimeKind::Browser,
    true
);
human_ui_case!(
    coverage_distribution_gate_008_server_fails_human_ui_gate,
    RuntimeKind::Server,
    false
);
human_ui_case!(
    coverage_distribution_gate_009_edge_fails_human_ui_gate,
    RuntimeKind::Edge,
    false
);
human_ui_case!(
    coverage_distribution_gate_010_desktop_fails_human_ui_gate,
    RuntimeKind::Desktop,
    false
);

#[test]
fn coverage_distribution_gate_011_relaxed_network_gate_accepts_offline() {
    let mut runtime = runtime();
    runtime.connectivity = Some(RuntimeConnectivity::Offline);
    let channel = channel_with(CapsuleRequirements::default());
    let release = channel.select(&runtime, Utc::now()).unwrap();
    assert_eq!(release.version, "1.0.0");
}

#[test]
fn coverage_distribution_gate_012_relaxed_network_gate_accepts_unknown_connectivity() {
    let runtime = runtime();
    let channel = channel_with(CapsuleRequirements::default());
    let release = channel.select(&runtime, Utc::now()).unwrap();
    assert_eq!(release.version, "1.0.0");
}

#[test]
fn coverage_distribution_gate_013_network_gate_reports_incompatible() {
    let mut runtime = runtime();
    runtime.connectivity = Some(RuntimeConnectivity::Offline);
    let error = network_channel().select(&runtime, Utc::now()).unwrap_err();
    assert!(
        matches!(
            error,
            DistributionError::Incompatible(ref message) if message.contains("online")
        ),
        "unexpected error: {error}"
    );
}

#[test]
fn coverage_distribution_gate_014_equal_trust_passes_minimum() {
    let requirements = CapsuleRequirements {
        minimum_trust: Some(RuntimeTrustLevel::Attested),
        ..CapsuleRequirements::default()
    };
    let runtime = runtime();
    let channel = channel_with(requirements);
    let release = channel.select(&runtime, Utc::now()).unwrap();
    assert_eq!(release.version, "1.0.0");
}

#[test]
fn coverage_distribution_gate_015_higher_trust_passes_minimum() {
    let requirements = CapsuleRequirements {
        minimum_trust: Some(RuntimeTrustLevel::Signed),
        ..CapsuleRequirements::default()
    };
    let runtime = runtime();
    let channel = channel_with(requirements);
    let release = channel.select(&runtime, Utc::now()).unwrap();
    assert_eq!(release.version, "1.0.0");
}

#[test]
fn coverage_distribution_gate_016_lower_trust_fails_minimum() {
    let requirements = CapsuleRequirements {
        minimum_trust: Some(RuntimeTrustLevel::Attested),
        ..CapsuleRequirements::default()
    };
    let mut runtime = runtime();
    runtime.trust = RuntimeTrustLevel::Signed;
    let error = channel_with(requirements)
        .select(&runtime, Utc::now())
        .unwrap_err();
    assert!(
        matches!(
            error,
            DistributionError::Incompatible(ref message) if message.contains("trust")
        ),
        "unexpected error: {error}"
    );
}

#[test]
fn coverage_distribution_gate_017_absent_minimum_accepts_unverified() {
    let mut runtime = runtime();
    runtime.trust = RuntimeTrustLevel::Unverified;
    let channel = channel_with(CapsuleRequirements::default());
    let release = channel.select(&runtime, Utc::now()).unwrap();
    assert_eq!(release.version, "1.0.0");
}

#[test]
fn coverage_distribution_gate_018_draining_runtime_is_rejected_first() {
    let mut runtime = runtime();
    runtime.draining = true;
    runtime.connectivity = Some(RuntimeConnectivity::Wifi);
    let error = network_channel().select(&runtime, Utc::now()).unwrap_err();
    assert!(
        matches!(
            error,
            DistributionError::Incompatible(ref message) if message.contains("draining")
        ),
        "unexpected error: {error}"
    );
}

#[test]
fn coverage_distribution_gate_019_expired_advertisement_is_rejected() {
    let mut runtime = runtime();
    let now = Utc::now();
    runtime.expires_at = now - Duration::seconds(1);
    let error = channel_with(CapsuleRequirements::default())
        .select(&runtime, now)
        .unwrap_err();
    assert!(
        matches!(
            error,
            DistributionError::Incompatible(ref message) if message.contains("expired")
        ),
        "unexpected error: {error}"
    );
}

#[test]
fn coverage_distribution_gate_020_expiry_boundary_is_exclusive() {
    let mut runtime = runtime();
    let now = Utc.with_ymd_and_hms(2026, 7, 25, 12, 0, 0).unwrap();
    runtime.expires_at = now;
    let error = channel_with(CapsuleRequirements::default())
        .select(&runtime, now)
        .unwrap_err();
    assert!(
        matches!(
            error,
            DistributionError::Incompatible(ref message) if message.contains("expired")
        ),
        "unexpected error: {error}"
    );
}

#[test]
fn coverage_distribution_gate_021_unexpired_advertisement_one_second_out_is_accepted() {
    let mut runtime = runtime();
    let now = Utc.with_ymd_and_hms(2026, 7, 25, 12, 0, 0).unwrap();
    runtime.expires_at = now + Duration::seconds(1);
    let channel = channel_with(CapsuleRequirements::default());
    let release = channel.select(&runtime, now).unwrap();
    assert_eq!(release.version, "1.0.0");
}
