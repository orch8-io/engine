//! Release-selection gates of `ReleaseChannel::select`.
//!
//! Rule under test: a runtime receives the channel head only if it is live
//! (not draining, advertisement strictly in the future), is the runtime that
//! placement selected (when one was), holds every required handler, plugin,
//! credential and hardware item (exact, case-sensitive names), is in at least
//! one allowed region (any-of), meets the minimum trust, is online when the
//! release needs network, and offers a human UI when the release needs one.
//! Every refusal is `Incompatible` with a message naming the failed gate; an
//! empty channel is `UnknownRelease`.

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
        labels: std::collections::BTreeMap::new(),
        observed_at: now,
        expires_at: now + Duration::minutes(5),
    }
}

fn release(requirements: CapsuleRequirements) -> ChannelRelease {
    ChannelRelease {
        package_name: "acme/app".into(),
        version: "1.0.0".into(),
        content_hash: hash('a'),
        package_url: "/full".into(),
        requirements,
        selected_runtime_id: None,
        promoted_at: Utc::now(),
    }
}

fn channel_with_release(release: ChannelRelease) -> ReleaseChannel {
    let mut channel = ReleaseChannel::new("tenant-a", ReleaseChannelName::Stable);
    channel.promote(release).unwrap();
    channel
}

fn channel_with(requirements: CapsuleRequirements) -> ReleaseChannel {
    channel_with_release(release(requirements))
}

fn strings(items: &[&str]) -> Vec<String> {
    items.iter().map(ToString::to_string).collect()
}

/// `Ok(version)` or the `Incompatible` message, so table rows can state the
/// exact outcome.
fn outcome(channel: &ReleaseChannel, runtime: &RuntimeCapabilities) -> Result<String, String> {
    match channel.select(runtime, Utc::now()) {
        Ok(release) => Ok(release.version.clone()),
        Err(DistributionError::Incompatible(message)) => Err(message),
        Err(other) => panic!("expected Ok or Incompatible, got {other:?}"),
    }
}

#[test]
fn network_gate_admits_only_online_connectivity() {
    let channel = channel_with(CapsuleRequirements {
        requires_network: true,
        ..CapsuleRequirements::default()
    });
    let offline = Err("release requires a currently-online runtime".to_string());
    let cases = [
        (
            "metered",
            Some(RuntimeConnectivity::Metered),
            Ok("1.0.0".to_string()),
        ),
        (
            "wifi",
            Some(RuntimeConnectivity::Wifi),
            Ok("1.0.0".to_string()),
        ),
        (
            "ethernet",
            Some(RuntimeConnectivity::Ethernet),
            Ok("1.0.0".to_string()),
        ),
        (
            "offline",
            Some(RuntimeConnectivity::Offline),
            offline.clone(),
        ),
        ("unknown", None, offline),
    ];
    for (case, connectivity, expected) in cases {
        let mut candidate = runtime();
        candidate.connectivity = connectivity;
        assert_eq!(outcome(&channel, &candidate), expected, "case {case}");
    }
}

#[test]
fn release_without_network_requirement_is_delivered_to_offline_runtime() {
    let mut candidate = runtime();
    candidate.connectivity = Some(RuntimeConnectivity::Offline);

    let selected = outcome(&channel_with(CapsuleRequirements::default()), &candidate);

    assert_eq!(selected, Ok("1.0.0".to_string()));
}

#[test]
fn human_ui_gate_rejects_edge_runtime() {
    // Desktop/Mobile/Browser/Server are covered by
    // `channel_region_and_ui_selection_agrees_with_task_claiming`.
    let channel = channel_with(CapsuleRequirements {
        requires_human_ui: true,
        ..CapsuleRequirements::default()
    });
    let mut candidate = runtime();
    candidate.kind = RuntimeKind::Edge;

    assert_eq!(
        outcome(&channel, &candidate),
        Err("release requires a human UI runtime".to_string())
    );
}

#[test]
fn trust_gate_is_a_minimum_not_an_exact_match() {
    let below = Err("runtime trust is below the release minimum".to_string());
    let cases = [
        (
            "equal",
            Some(RuntimeTrustLevel::Attested),
            RuntimeTrustLevel::Attested,
            Ok("1.0.0".to_string()),
        ),
        (
            "higher",
            Some(RuntimeTrustLevel::Signed),
            RuntimeTrustLevel::Attested,
            Ok("1.0.0".to_string()),
        ),
        (
            "lower",
            Some(RuntimeTrustLevel::Attested),
            RuntimeTrustLevel::Signed,
            below,
        ),
        (
            "no minimum",
            None,
            RuntimeTrustLevel::Unverified,
            Ok("1.0.0".to_string()),
        ),
    ];
    for (case, minimum, trust, expected) in cases {
        let channel = channel_with(CapsuleRequirements {
            minimum_trust: minimum,
            ..CapsuleRequirements::default()
        });
        let mut candidate = runtime();
        candidate.trust = trust;
        assert_eq!(outcome(&channel, &candidate), expected, "case {case}");
    }
}

#[test]
fn draining_runtime_is_rejected_before_any_capability_gate() {
    let channel = channel_with(CapsuleRequirements {
        requires_network: true,
        ..CapsuleRequirements::default()
    });
    let mut candidate = runtime();
    candidate.draining = true;
    candidate.connectivity = Some(RuntimeConnectivity::Wifi);

    assert_eq!(
        outcome(&channel, &candidate),
        Err("runtime is draining or its advertisement expired".to_string())
    );
}

#[test]
fn advertisement_expiry_is_exclusive() {
    let now = Utc.with_ymd_and_hms(2026, 7, 25, 12, 0, 0).unwrap();
    let channel = channel_with(CapsuleRequirements::default());
    let cases = [
        ("expired a second ago", now - Duration::seconds(1), false),
        ("expires exactly now", now, false),
        ("expires in a second", now + Duration::seconds(1), true),
    ];
    for (case, expires_at, admitted) in cases {
        let mut candidate = runtime();
        candidate.expires_at = expires_at;
        let result = channel.select(&candidate, now);
        if admitted {
            assert_eq!(
                result.map(|r| r.version.as_str()),
                Ok("1.0.0"),
                "case {case}"
            );
        } else {
            assert_eq!(
                result,
                Err(DistributionError::Incompatible(
                    "runtime is draining or its advertisement expired".into()
                )),
                "case {case}"
            );
        }
    }
}

#[test]
fn placement_bound_release_is_delivered_only_to_the_selected_runtime() {
    let selected = runtime();
    let other = runtime();
    let channel = channel_with_release(ChannelRelease {
        selected_runtime_id: Some(selected.runtime_id),
        ..release(CapsuleRequirements::default())
    });

    assert_eq!(outcome(&channel, &selected), Ok("1.0.0".to_string()));
    assert_eq!(
        outcome(&channel, &other),
        Err("placement selected a different runtime".to_string())
    );
}

#[test]
fn empty_channel_has_no_release_to_select() {
    let channel = ReleaseChannel::new("tenant-a", ReleaseChannelName::Stable);

    assert_eq!(
        channel.select(&runtime(), Utc::now()),
        Err(DistributionError::UnknownRelease)
    );
}

/// (case, release requirements, what the runtime advertises, outcome).
type CapabilityCase = (
    &'static str,
    CapsuleRequirements,
    fn(&mut RuntimeCapabilities),
    Result<String, String>,
);

#[test]
fn every_required_capability_must_be_advertised_by_exact_name() {
    let cases: [CapabilityCase; 9] = [
        (
            "no requirements",
            CapsuleRequirements::default(),
            |_| {},
            Ok("1.0.0".into()),
        ),
        (
            "handler among extras",
            CapsuleRequirements {
                handlers: strings(&["noop"]),
                ..CapsuleRequirements::default()
            },
            |r| r.handlers = strings(&["log", "noop", "http"]),
            Ok("1.0.0".into()),
        ),
        (
            "missing handler",
            CapsuleRequirements {
                handlers: strings(&["noop"]),
                ..CapsuleRequirements::default()
            },
            |_| {},
            Err("missing handler noop".into()),
        ),
        (
            "one of two handlers missing",
            CapsuleRequirements {
                handlers: strings(&["noop", "log"]),
                ..CapsuleRequirements::default()
            },
            |r| r.handlers = strings(&["noop"]),
            Err("missing handler log".into()),
        ),
        (
            "handler names are case-sensitive",
            CapsuleRequirements {
                handlers: strings(&["noop"]),
                ..CapsuleRequirements::default()
            },
            |r| r.handlers = strings(&["NOOP"]),
            Err("missing handler noop".into()),
        ),
        (
            "handler names are not prefix-matched",
            CapsuleRequirements {
                handlers: strings(&["noop"]),
                ..CapsuleRequirements::default()
            },
            |r| r.handlers = strings(&["noop-v2"]),
            Err("missing handler noop".into()),
        ),
        (
            "missing plugin",
            CapsuleRequirements {
                plugins: strings(&["ocr"]),
                ..CapsuleRequirements::default()
            },
            |r| r.plugins = strings(&["vision"]),
            Err("missing plugin ocr".into()),
        ),
        (
            "missing credential",
            CapsuleRequirements {
                credentials: strings(&["vault"]),
                ..CapsuleRequirements::default()
            },
            |_| {},
            Err("missing credential vault".into()),
        ),
        (
            "missing hardware",
            CapsuleRequirements {
                hardware: strings(&["gpu"]),
                ..CapsuleRequirements::default()
            },
            |r| r.hardware = strings(&["cpu"]),
            Err("missing hardware gpu".into()),
        ),
    ];
    for (case, requirements, advertise, expected) in cases {
        let mut candidate = runtime();
        advertise(&mut candidate);
        assert_eq!(
            outcome(&channel_with(requirements), &candidate),
            expected,
            "case {case}"
        );
    }
}

#[test]
fn region_requirement_is_satisfied_by_any_one_allowed_region() {
    let not_allowed = Err("runtime is not in any allowed region".to_string());
    let cases = [
        (
            "no region restriction",
            vec![],
            vec!["us"],
            Ok("1.0.0".to_string()),
        ),
        (
            "runtime in one allowed region",
            vec!["br", "eu"],
            vec!["us", "eu"],
            Ok("1.0.0".to_string()),
        ),
        (
            "runtime in no allowed region",
            vec!["br", "eu"],
            vec!["us"],
            not_allowed.clone(),
        ),
        (
            "runtime advertises no region",
            vec!["br"],
            vec![],
            not_allowed,
        ),
    ];
    for (case, allowed, advertised, expected) in cases {
        let channel = channel_with(CapsuleRequirements {
            regions: strings(&allowed),
            ..CapsuleRequirements::default()
        });
        let mut candidate = runtime();
        candidate.regions = strings(&advertised);
        assert_eq!(outcome(&channel, &candidate), expected, "case {case}");
    }
}
