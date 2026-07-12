//! Extensive unit tests for `orch8_publisher::package::*`.
//!
//! Goes deeper than the inline module tests: full validation matrices for
//! names/versions/upgrades, per-field content-hash sensitivity, every
//! tamper axis for verification, trust policy combinations, file-selection
//! helpers, error `Display` strings, and reproducibility guarantees.

use std::collections::BTreeMap;

use base64::{Engine as _, engine::general_purpose::STANDARD as BASE64};
use chrono::{TimeZone, Utc};
use ed25519_dalek::{Signer, SigningKey};
use rand_core::OsRng;

use orch8_publisher::package::{
    PACKAGE_FORMAT_VERSION, PackageArchive, PackageError, PackageManifest, PackageRequirements,
    SignedPackage, TrustLevel, TrustPolicy, build_package, check_trust, check_upgrade,
    content_hash, contract_files, install_namespace, parse_version, sequence_files,
    validate_package_name, verify_package,
};

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

fn key() -> SigningKey {
    SigningKey::generate(&mut OsRng)
}

fn manifest(name: &str, version: &str) -> PackageManifest {
    PackageManifest {
        name: name.into(),
        version: version.into(),
        description: "test package".into(),
        publisher: "Acme Corp".into(),
        requirements: PackageRequirements {
            handlers: vec!["charge_card".into(), "send_email".into()],
            credentials: vec!["stripe_key".into()],
            plugins: vec!["metrics".into()],
            queues: vec!["billing".into()],
            min_engine_version: Some("0.6.0".into()),
        },
        created_at: Utc.with_ymd_and_hms(2026, 7, 1, 12, 0, 0).unwrap(),
    }
}

fn files() -> BTreeMap<String, String> {
    let mut f = BTreeMap::new();
    f.insert(
        "sequences/checkout.json".to_string(),
        r#"{"name":"checkout"}"#.to_string(),
    );
    f.insert(
        "contracts/checkout.contracts.json".to_string(),
        r#"{"cases":[]}"#.to_string(),
    );
    f.insert("README.md".to_string(), "# Checkout".to_string());
    f
}

fn build() -> SignedPackage {
    build_package(manifest("acme/checkout", "1.0.0"), files(), &key()).unwrap()
}

fn build_with(m: PackageManifest) -> SignedPackage {
    build_package(m, files(), &key()).unwrap()
}

fn archive_with_paths(paths: &[&str]) -> PackageArchive {
    let mut f = BTreeMap::new();
    for p in paths {
        f.insert((*p).to_string(), "{}".to_string());
    }
    PackageArchive {
        format_version: PACKAGE_FORMAT_VERSION,
        manifest: manifest("acme/x", "1.0.0"),
        files: f,
    }
}

fn assert_invalid(result: Result<(), PackageError>) {
    assert!(
        matches!(result, Err(PackageError::Invalid(_))),
        "expected Invalid, got {result:?}"
    );
}

// ---------------------------------------------------------------------------
// validate_package_name — valid inputs
// ---------------------------------------------------------------------------

#[test]
fn name_valid_simple() {
    validate_package_name("acme/checkout").unwrap();
}

#[test]
fn name_valid_with_digits() {
    validate_package_name("a1/b2").unwrap();
}

#[test]
fn name_valid_all_digits() {
    validate_package_name("123/456").unwrap();
}

#[test]
fn name_valid_hyphens() {
    validate_package_name("my-publisher/my-package").unwrap();
}

#[test]
fn name_valid_underscores() {
    validate_package_name("my_publisher/my_package").unwrap();
}

#[test]
fn name_valid_mixed_separators() {
    validate_package_name("a-1_b/c_2-d").unwrap();
}

#[test]
fn name_valid_single_char_segments() {
    validate_package_name("a/b").unwrap();
}

#[test]
fn name_valid_only_hyphen_segments() {
    // Actual semantics: no requirement that a segment contain a letter.
    validate_package_name("-/-").unwrap();
}

#[test]
fn name_valid_only_underscore_segments() {
    validate_package_name("_/_").unwrap();
}

#[test]
fn name_valid_leading_and_trailing_hyphens() {
    validate_package_name("-acme-/-pkg-").unwrap();
}

#[test]
fn name_valid_digit_leading_segments() {
    validate_package_name("1abc/2def").unwrap();
}

#[test]
fn name_valid_long_segments() {
    let long = "a".repeat(200);
    validate_package_name(&format!("{long}/{long}")).unwrap();
}

// ---------------------------------------------------------------------------
// validate_package_name — invalid inputs
// ---------------------------------------------------------------------------

#[test]
fn name_rejects_uppercase_in_publisher() {
    assert_invalid(validate_package_name("Acme/checkout"));
}

#[test]
fn name_rejects_uppercase_in_package() {
    assert_invalid(validate_package_name("acme/Checkout"));
}

#[test]
fn name_rejects_uppercase_mid_segment() {
    assert_invalid(validate_package_name("acme/chEckout"));
}

#[test]
fn name_rejects_all_uppercase() {
    assert_invalid(validate_package_name("ACME/CHECKOUT"));
}

#[test]
fn name_rejects_space_in_publisher() {
    assert_invalid(validate_package_name("ac me/checkout"));
}

#[test]
fn name_rejects_space_in_package() {
    assert_invalid(validate_package_name("acme/check out"));
}

#[test]
fn name_rejects_leading_space() {
    assert_invalid(validate_package_name(" acme/checkout"));
}

#[test]
fn name_rejects_trailing_space() {
    assert_invalid(validate_package_name("acme/checkout "));
}

#[test]
fn name_rejects_dots_in_publisher() {
    assert_invalid(validate_package_name("a.b/c"));
}

#[test]
fn name_rejects_dots_in_package() {
    assert_invalid(validate_package_name("a/b.c"));
}

#[test]
fn name_rejects_unicode_cyrillic_lookalike() {
    // U+0430 CYRILLIC SMALL LETTER A — visually identical to 'a'.
    assert_invalid(validate_package_name("\u{430}cme/checkout"));
}

#[test]
fn name_rejects_unicode_accented() {
    assert_invalid(validate_package_name("caf\u{e9}/menu"));
}

#[test]
fn name_rejects_emoji() {
    assert_invalid(validate_package_name("acme/\u{1f680}"));
}

#[test]
fn name_rejects_empty_string() {
    assert_invalid(validate_package_name(""));
}

#[test]
fn name_rejects_missing_slash() {
    assert_invalid(validate_package_name("checkout"));
}

#[test]
fn name_rejects_empty_publisher() {
    assert_invalid(validate_package_name("/checkout"));
}

#[test]
fn name_rejects_empty_package() {
    assert_invalid(validate_package_name("acme/"));
}

#[test]
fn name_rejects_lone_slash() {
    assert_invalid(validate_package_name("/"));
}

#[test]
fn name_rejects_three_segments() {
    assert_invalid(validate_package_name("a/b/c"));
}

#[test]
fn name_rejects_four_segments() {
    assert_invalid(validate_package_name("a/b/c/d"));
}

#[test]
fn name_rejects_double_slash() {
    // "a//b" splits into three parts — treated as 3+ segments.
    assert_invalid(validate_package_name("a//b"));
}

#[test]
fn name_rejects_trailing_slash() {
    assert_invalid(validate_package_name("a/b/"));
}

#[test]
fn name_rejects_leading_slash_two_segments() {
    assert_invalid(validate_package_name("/a/b"));
}

#[test]
fn name_error_message_mentions_offender() {
    let err = validate_package_name("Acme/checkout").unwrap_err();
    let msg = err.to_string();
    assert!(msg.contains("Acme"), "message should name the bad segment: {msg}");
}

#[test]
fn name_error_for_shape_mentions_full_name() {
    let err = validate_package_name("no-slash-here").unwrap_err();
    let msg = err.to_string();
    assert!(msg.contains("no-slash-here"), "{msg}");
    assert!(msg.contains("publisher/package"), "{msg}");
}

// ---------------------------------------------------------------------------
// install_namespace
// ---------------------------------------------------------------------------

#[test]
fn namespace_basic_shape() {
    assert_eq!(install_namespace("acme/checkout"), "pkg.acme.checkout");
}

#[test]
fn namespace_preserves_digits() {
    assert_eq!(install_namespace("a1/b2"), "pkg.a1.b2");
}

#[test]
fn namespace_preserves_hyphens_and_underscores() {
    assert_eq!(install_namespace("my-pub/my_pkg"), "pkg.my-pub.my_pkg");
}

#[test]
fn namespace_always_starts_with_pkg_prefix() {
    for name in ["acme/checkout", "x/y", "a-b/c_d"] {
        assert!(install_namespace(name).starts_with("pkg."));
    }
}

#[test]
fn namespace_distinct_for_distinct_names() {
    assert_ne!(install_namespace("acme/a"), install_namespace("acme/b"));
    assert_ne!(install_namespace("a/pkg"), install_namespace("b/pkg"));
}

#[test]
fn namespace_replaces_every_slash() {
    // The helper itself does not validate; it replaces all slashes.
    assert_eq!(install_namespace("a/b/c"), "pkg.a.b.c");
}

// ---------------------------------------------------------------------------
// parse_version
// ---------------------------------------------------------------------------

#[test]
fn version_single_segment() {
    assert_eq!(parse_version("1").unwrap(), vec![1]);
}

#[test]
fn version_two_segments() {
    assert_eq!(parse_version("1.2").unwrap(), vec![1, 2]);
}

#[test]
fn version_three_segments() {
    assert_eq!(parse_version("1.2.3").unwrap(), vec![1, 2, 3]);
}

#[test]
fn version_four_segments() {
    assert_eq!(parse_version("1.2.3.4").unwrap(), vec![1, 2, 3, 4]);
}

#[test]
fn version_many_segments() {
    assert_eq!(
        parse_version("1.2.3.4.5.6.7.8").unwrap(),
        vec![1, 2, 3, 4, 5, 6, 7, 8]
    );
}

#[test]
fn version_zero() {
    assert_eq!(parse_version("0").unwrap(), vec![0]);
}

#[test]
fn version_all_zeros() {
    assert_eq!(parse_version("0.0.0").unwrap(), vec![0, 0, 0]);
}

#[test]
fn version_u64_max_segment() {
    assert_eq!(
        parse_version("18446744073709551615.1").unwrap(),
        vec![u64::MAX, 1]
    );
}

#[test]
fn version_overflowing_segment_is_rejected() {
    // u64::MAX + 1
    assert!(parse_version("18446744073709551616").is_err());
}

#[test]
fn version_leading_zeros_parse_numerically() {
    // Actual semantics: "01" is just 1 — leading zeros are accepted.
    assert_eq!(parse_version("01.002.3").unwrap(), vec![1, 2, 3]);
}

#[test]
fn version_leading_zeros_equal_their_numeric_value() {
    assert_eq!(parse_version("1.02"), parse_version("1.2"));
}

#[test]
fn version_empty_string_rejected() {
    assert!(parse_version("").is_err());
}

#[test]
fn version_letters_rejected() {
    assert!(parse_version("abc").is_err());
}

#[test]
fn version_letter_segment_rejected() {
    assert!(parse_version("1.a").is_err());
}

#[test]
fn version_letter_suffix_rejected() {
    assert!(parse_version("1.2v").is_err());
}

#[test]
fn version_semver_prerelease_rejected() {
    assert!(parse_version("1.2.3-beta").is_err());
}

#[test]
fn version_double_dot_rejected() {
    assert!(parse_version("1..2").is_err());
}

#[test]
fn version_trailing_dot_rejected() {
    assert!(parse_version("1.2.").is_err());
}

#[test]
fn version_leading_dot_rejected() {
    assert!(parse_version(".1").is_err());
}

#[test]
fn version_lone_dot_rejected() {
    assert!(parse_version(".").is_err());
}

#[test]
fn version_negative_rejected() {
    assert!(parse_version("-1").is_err());
}

#[test]
fn version_negative_segment_rejected() {
    assert!(parse_version("1.-2").is_err());
}

#[test]
fn version_plus_prefix_parses() {
    // Actual semantics: Rust's u64 parser accepts a leading '+'.
    assert_eq!(parse_version("+1").unwrap(), vec![1]);
}

#[test]
fn version_whitespace_rejected() {
    assert!(parse_version(" 1").is_err());
    assert!(parse_version("1 ").is_err());
    assert!(parse_version("1. 2").is_err());
}

#[test]
fn version_error_is_invalid_and_names_the_version() {
    let err = parse_version("nope").unwrap_err();
    assert!(matches!(err, PackageError::Invalid(_)));
    assert!(err.to_string().contains("nope"));
}

// ---------------------------------------------------------------------------
// check_upgrade
// ---------------------------------------------------------------------------

#[test]
fn upgrade_equal_versions_rejected() {
    assert!(matches!(
        check_upgrade("1.0.0", "1.0.0"),
        Err(PackageError::Downgrade { .. })
    ));
}

#[test]
fn upgrade_patch_bump_ok() {
    check_upgrade("1.0.0", "1.0.1").unwrap();
}

#[test]
fn upgrade_minor_bump_ok() {
    check_upgrade("1.0.9", "1.1.0").unwrap();
}

#[test]
fn upgrade_major_bump_ok() {
    check_upgrade("1.9.9", "2.0.0").unwrap();
}

#[test]
fn upgrade_patch_downgrade_rejected() {
    assert!(matches!(
        check_upgrade("1.0.1", "1.0.0"),
        Err(PackageError::Downgrade { .. })
    ));
}

#[test]
fn upgrade_minor_downgrade_rejected() {
    assert!(matches!(
        check_upgrade("1.1.0", "1.0.9"),
        Err(PackageError::Downgrade { .. })
    ));
}

#[test]
fn upgrade_major_downgrade_rejected() {
    assert!(matches!(
        check_upgrade("2.0.0", "1.9.9"),
        Err(PackageError::Downgrade { .. })
    ));
}

#[test]
fn upgrade_numeric_not_lexical() {
    // Lexically "1.10.0" < "1.9.0"; numerically it is greater.
    check_upgrade("1.9.0", "1.10.0").unwrap();
}

#[test]
fn upgrade_numeric_not_lexical_downgrade() {
    assert!(matches!(
        check_upgrade("1.10.0", "1.9.0"),
        Err(PackageError::Downgrade { .. })
    ));
}

#[test]
fn upgrade_large_numeric_components() {
    check_upgrade("1.999999999999", "1.1000000000000").unwrap();
}

#[test]
fn upgrade_longer_version_beats_shorter_prefix() {
    // Vec ordering: [1,2] < [1,2,0] (prefix is smaller).
    check_upgrade("1.2", "1.2.0").unwrap();
}

#[test]
fn upgrade_shorter_prefix_is_downgrade_from_longer() {
    // [1,2,0] > [1,2] so going 1.2.0 -> 1.2 is a downgrade.
    assert!(matches!(
        check_upgrade("1.2.0", "1.2"),
        Err(PackageError::Downgrade { .. })
    ));
}

#[test]
fn upgrade_even_zero_extension_counts_as_greater() {
    // Semantically "1.2" == "1.2.0" but Vec ordering says otherwise.
    // Assert the actual (lexicographic Vec) semantics.
    assert!(parse_version("1.2").unwrap() < parse_version("1.2.0").unwrap());
    check_upgrade("1.2", "1.2.0").unwrap();
}

#[test]
fn upgrade_different_segment_counts_numeric_comparison() {
    check_upgrade("1.2", "1.3").unwrap();
    check_upgrade("1.2.9.9", "1.3").unwrap();
    assert!(matches!(
        check_upgrade("1.3", "1.2.9.9"),
        Err(PackageError::Downgrade { .. })
    ));
}

#[test]
fn upgrade_single_vs_triple() {
    check_upgrade("1", "1.0.0").unwrap(); // [1] < [1,0,0]
    check_upgrade("1.9.9", "2").unwrap(); // [1,9,9] < [2]
}

#[test]
fn upgrade_invalid_installed_is_invalid_not_downgrade() {
    let err = check_upgrade("garbage", "1.0.0").unwrap_err();
    assert!(matches!(err, PackageError::Invalid(_)));
}

#[test]
fn upgrade_invalid_incoming_is_invalid_not_downgrade() {
    let err = check_upgrade("1.0.0", "garbage").unwrap_err();
    assert!(matches!(err, PackageError::Invalid(_)));
}

#[test]
fn upgrade_downgrade_error_carries_both_versions() {
    let err = check_upgrade("2.0.0", "1.0.0").unwrap_err();
    match err {
        PackageError::Downgrade { installed, incoming } => {
            assert_eq!(installed, "2.0.0");
            assert_eq!(incoming, "1.0.0");
        }
        other => panic!("expected Downgrade, got {other:?}"),
    }
}

#[test]
fn upgrade_tiny_patch_at_zero() {
    check_upgrade("0.0.1", "0.0.2").unwrap();
}

// ---------------------------------------------------------------------------
// content_hash — determinism and sensitivity
// ---------------------------------------------------------------------------

fn base_archive() -> PackageArchive {
    PackageArchive {
        format_version: PACKAGE_FORMAT_VERSION,
        manifest: manifest("acme/checkout", "1.0.0"),
        files: files(),
    }
}

#[test]
fn hash_is_deterministic() {
    let a = base_archive();
    assert_eq!(content_hash(&a).unwrap(), content_hash(&a).unwrap());
}

#[test]
fn hash_is_64_lowercase_hex_chars() {
    let h = content_hash(&base_archive()).unwrap();
    assert_eq!(h.len(), 64);
    assert!(h.chars().all(|c| c.is_ascii_hexdigit() && !c.is_ascii_uppercase()));
}

#[test]
fn hash_ignores_file_insertion_order() {
    // BTreeMap sorts keys, so insertion order cannot leak into the hash.
    let mut forward = BTreeMap::new();
    forward.insert("a.md".to_string(), "1".to_string());
    forward.insert("b.md".to_string(), "2".to_string());
    forward.insert("c.md".to_string(), "3".to_string());
    let mut reverse = BTreeMap::new();
    reverse.insert("c.md".to_string(), "3".to_string());
    reverse.insert("b.md".to_string(), "2".to_string());
    reverse.insert("a.md".to_string(), "1".to_string());
    let mut arch_a = base_archive();
    arch_a.files = forward;
    let mut arch_b = base_archive();
    arch_b.files = reverse;
    assert_eq!(content_hash(&arch_a).unwrap(), content_hash(&arch_b).unwrap());
}

fn hash_of(mutate: impl FnOnce(&mut PackageArchive)) -> String {
    let mut a = base_archive();
    mutate(&mut a);
    content_hash(&a).unwrap()
}

#[test]
fn hash_sensitive_to_manifest_name() {
    assert_ne!(hash_of(|_| {}), hash_of(|a| a.manifest.name = "acme/other".into()));
}

#[test]
fn hash_sensitive_to_manifest_version() {
    assert_ne!(hash_of(|_| {}), hash_of(|a| a.manifest.version = "1.0.1".into()));
}

#[test]
fn hash_sensitive_to_description() {
    assert_ne!(hash_of(|_| {}), hash_of(|a| a.manifest.description = "x".into()));
}

#[test]
fn hash_sensitive_to_publisher_label() {
    assert_ne!(hash_of(|_| {}), hash_of(|a| a.manifest.publisher = "Evil".into()));
}

#[test]
fn hash_sensitive_to_handlers_requirement() {
    assert_ne!(
        hash_of(|_| {}),
        hash_of(|a| a.manifest.requirements.handlers.push("extra".into()))
    );
}

#[test]
fn hash_sensitive_to_credentials_requirement() {
    assert_ne!(
        hash_of(|_| {}),
        hash_of(|a| a.manifest.requirements.credentials.clear())
    );
}

#[test]
fn hash_sensitive_to_plugins_requirement() {
    assert_ne!(
        hash_of(|_| {}),
        hash_of(|a| a.manifest.requirements.plugins = vec!["other".into()])
    );
}

#[test]
fn hash_sensitive_to_queues_requirement() {
    assert_ne!(
        hash_of(|_| {}),
        hash_of(|a| a.manifest.requirements.queues.clear())
    );
}

#[test]
fn hash_sensitive_to_min_engine_version() {
    assert_ne!(
        hash_of(|_| {}),
        hash_of(|a| a.manifest.requirements.min_engine_version = Some("0.7.0".into()))
    );
    assert_ne!(
        hash_of(|_| {}),
        hash_of(|a| a.manifest.requirements.min_engine_version = None)
    );
}

#[test]
fn hash_sensitive_to_created_at() {
    assert_ne!(
        hash_of(|_| {}),
        hash_of(|a| a.manifest.created_at = Utc.with_ymd_and_hms(2026, 7, 2, 0, 0, 0).unwrap())
    );
}

#[test]
fn hash_sensitive_to_file_content() {
    assert_ne!(
        hash_of(|_| {}),
        hash_of(|a| {
            a.files.insert("README.md".into(), "# Different".into());
        })
    );
}

#[test]
fn hash_sensitive_to_single_byte_file_change() {
    assert_ne!(
        hash_of(|_| {}),
        hash_of(|a| {
            a.files.insert("README.md".into(), "# Checkout!".into());
        })
    );
}

#[test]
fn hash_sensitive_to_file_path_rename() {
    assert_ne!(
        hash_of(|_| {}),
        hash_of(|a| {
            let content = a.files.remove("README.md").unwrap();
            a.files.insert("README2.md".into(), content);
        })
    );
}

#[test]
fn hash_sensitive_to_file_addition() {
    assert_ne!(
        hash_of(|_| {}),
        hash_of(|a| {
            a.files.insert("LICENSE".into(), "MIT".into());
        })
    );
}

#[test]
fn hash_sensitive_to_file_removal() {
    assert_ne!(
        hash_of(|_| {}),
        hash_of(|a| {
            a.files.remove("README.md");
        })
    );
}

#[test]
fn hash_sensitive_to_requirement_list_order() {
    // Requirement lists are Vecs: order is part of the signed content.
    let ab = hash_of(|a| a.manifest.requirements.handlers = vec!["a".into(), "b".into()]);
    let ba = hash_of(|a| a.manifest.requirements.handlers = vec!["b".into(), "a".into()]);
    assert_ne!(ab, ba);
}

#[test]
fn hash_sensitive_to_format_version() {
    assert_ne!(hash_of(|_| {}), hash_of(|a| a.format_version = 2));
}

#[test]
fn hash_of_empty_vs_missing_description_differ_only_if_value_differs() {
    // Description defaults to "" — two archives with the same empty value hash equal.
    let a = hash_of(|a| a.manifest.description = String::new());
    let b = hash_of(|a| a.manifest.description = String::new());
    assert_eq!(a, b);
}

// ---------------------------------------------------------------------------
// build_package — validations
// ---------------------------------------------------------------------------

#[test]
fn build_rejects_bad_name() {
    let err = build_package(manifest("BadName/x", "1.0.0"), files(), &key()).unwrap_err();
    assert!(matches!(err, PackageError::Invalid(_)));
}

#[test]
fn build_rejects_name_without_slash() {
    assert!(build_package(manifest("noslash", "1.0.0"), files(), &key()).is_err());
}

#[test]
fn build_rejects_bad_version() {
    let err = build_package(manifest("acme/x", "1.0.0-beta"), files(), &key()).unwrap_err();
    assert!(matches!(err, PackageError::Invalid(_)));
}

#[test]
fn build_rejects_empty_version() {
    assert!(build_package(manifest("acme/x", ""), files(), &key()).is_err());
}

#[test]
fn build_rejects_empty_file_map() {
    let err = build_package(manifest("acme/x", "1.0.0"), BTreeMap::new(), &key()).unwrap_err();
    assert!(matches!(err, PackageError::Invalid(msg) if msg.contains("no files")));
}

#[test]
fn build_rejects_top_level_traversal() {
    let mut f = BTreeMap::new();
    f.insert("../etc/passwd".to_string(), "evil".to_string());
    assert!(build_package(manifest("acme/x", "1.0.0"), f, &key()).is_err());
}

#[test]
fn build_rejects_nested_traversal() {
    let mut f = BTreeMap::new();
    f.insert("a/../b".to_string(), "evil".to_string());
    assert!(build_package(manifest("acme/x", "1.0.0"), f, &key()).is_err());
}

#[test]
fn build_rejects_deep_traversal() {
    let mut f = BTreeMap::new();
    f.insert("a/b/../../../c".to_string(), "evil".to_string());
    assert!(build_package(manifest("acme/x", "1.0.0"), f, &key()).is_err());
}

#[test]
fn build_rejects_trailing_dotdot() {
    let mut f = BTreeMap::new();
    f.insert("a/..".to_string(), "evil".to_string());
    assert!(build_package(manifest("acme/x", "1.0.0"), f, &key()).is_err());
}

#[test]
fn build_rejects_lone_dotdot() {
    let mut f = BTreeMap::new();
    f.insert("..".to_string(), "evil".to_string());
    assert!(build_package(manifest("acme/x", "1.0.0"), f, &key()).is_err());
}

#[test]
fn build_rejects_absolute_path() {
    let mut f = BTreeMap::new();
    f.insert("/abs/path".to_string(), "evil".to_string());
    assert!(build_package(manifest("acme/x", "1.0.0"), f, &key()).is_err());
}

#[test]
fn build_rejects_root_path() {
    let mut f = BTreeMap::new();
    f.insert("/".to_string(), "evil".to_string());
    assert!(build_package(manifest("acme/x", "1.0.0"), f, &key()).is_err());
}

#[test]
fn build_rejects_mixed_good_and_bad_paths() {
    let mut f = files();
    f.insert("../sneaky".to_string(), "evil".to_string());
    assert!(build_package(manifest("acme/x", "1.0.0"), f, &key()).is_err());
}

#[test]
fn build_path_error_names_the_path() {
    let mut f = BTreeMap::new();
    f.insert("../oops".to_string(), "x".to_string());
    let err = build_package(manifest("acme/x", "1.0.0"), f, &key()).unwrap_err();
    assert!(err.to_string().contains("../oops"));
}

#[test]
fn build_allows_single_dot_segment() {
    // Actual semantics: only ".." segments and absolute paths are rejected.
    let mut f = BTreeMap::new();
    f.insert("a/./b".to_string(), "ok".to_string());
    build_package(manifest("acme/x", "1.0.0"), f, &key()).unwrap();
}

#[test]
fn build_allows_leading_dot_slash() {
    let mut f = BTreeMap::new();
    f.insert("./a".to_string(), "ok".to_string());
    build_package(manifest("acme/x", "1.0.0"), f, &key()).unwrap();
}

#[test]
fn build_allows_dotdot_prefixed_filename() {
    // "..hidden" is a filename, not a traversal segment.
    let mut f = BTreeMap::new();
    f.insert("..hidden".to_string(), "ok".to_string());
    build_package(manifest("acme/x", "1.0.0"), f, &key()).unwrap();
}

#[test]
fn build_allows_dotdot_inside_filename() {
    let mut f = BTreeMap::new();
    f.insert("a..b".to_string(), "ok".to_string());
    build_package(manifest("acme/x", "1.0.0"), f, &key()).unwrap();
}

#[test]
fn build_allows_empty_path_key() {
    // Actual semantics: the empty path has no ".." segment and is not
    // absolute, so validation lets it through.
    let mut f = BTreeMap::new();
    f.insert(String::new(), "ok".to_string());
    build_package(manifest("acme/x", "1.0.0"), f, &key()).unwrap();
}

#[test]
fn build_allows_double_slash_in_path() {
    let mut f = BTreeMap::new();
    f.insert("a//b".to_string(), "ok".to_string());
    build_package(manifest("acme/x", "1.0.0"), f, &key()).unwrap();
}

#[test]
fn build_sets_current_format_version() {
    assert_eq!(build().archive.format_version, PACKAGE_FORMAT_VERSION);
}

#[test]
fn build_embeds_matching_content_hash() {
    let pkg = build();
    assert_eq!(pkg.content_hash, content_hash(&pkg.archive).unwrap());
}

#[test]
fn build_embeds_decodable_signature_and_key() {
    let pkg = build();
    assert_eq!(BASE64.decode(&pkg.signature).unwrap().len(), 64);
    assert_eq!(BASE64.decode(&pkg.public_key).unwrap().len(), 32);
}

#[test]
fn build_public_key_matches_signing_key() {
    let k = key();
    let pkg = build_package(manifest("acme/x", "1.0.0"), files(), &k).unwrap();
    assert_eq!(pkg.public_key, BASE64.encode(k.verifying_key().to_bytes()));
}

#[test]
fn build_preserves_manifest_and_files() {
    let pkg = build();
    assert_eq!(pkg.archive.manifest.name, "acme/checkout");
    assert_eq!(pkg.archive.manifest.version, "1.0.0");
    assert_eq!(pkg.archive.files.len(), 3);
    assert_eq!(pkg.archive.files["README.md"], "# Checkout");
}

// ---------------------------------------------------------------------------
// verify_package
// ---------------------------------------------------------------------------

#[test]
fn verify_round_trip_ok() {
    verify_package(&build()).unwrap();
}

#[test]
fn verify_detects_tampered_file_content() {
    let mut pkg = build();
    pkg.archive
        .files
        .insert("sequences/checkout.json".into(), r#"{"name":"evil"}"#.into());
    assert_eq!(verify_package(&pkg), Err(PackageError::Tampered));
}

#[test]
fn verify_detects_added_file() {
    let mut pkg = build();
    pkg.archive.files.insert("payload.sh".into(), "rm -rf /".into());
    assert_eq!(verify_package(&pkg), Err(PackageError::Tampered));
}

#[test]
fn verify_detects_removed_file() {
    let mut pkg = build();
    pkg.archive.files.remove("README.md");
    assert_eq!(verify_package(&pkg), Err(PackageError::Tampered));
}

#[test]
fn verify_detects_renamed_file() {
    let mut pkg = build();
    let v = pkg.archive.files.remove("README.md").unwrap();
    pkg.archive.files.insert("README.txt".into(), v);
    assert_eq!(verify_package(&pkg), Err(PackageError::Tampered));
}

#[test]
fn verify_detects_tampered_manifest_name() {
    let mut pkg = build();
    pkg.archive.manifest.name = "evil/checkout".into();
    assert_eq!(verify_package(&pkg), Err(PackageError::Tampered));
}

#[test]
fn verify_detects_tampered_manifest_version() {
    let mut pkg = build();
    pkg.archive.manifest.version = "9.9.9".into();
    assert_eq!(verify_package(&pkg), Err(PackageError::Tampered));
}

#[test]
fn verify_detects_tampered_description() {
    let mut pkg = build();
    pkg.archive.manifest.description = "totally safe".into();
    assert_eq!(verify_package(&pkg), Err(PackageError::Tampered));
}

#[test]
fn verify_detects_tampered_publisher_label() {
    let mut pkg = build();
    pkg.archive.manifest.publisher = "Trusted Vendor Inc".into();
    assert_eq!(verify_package(&pkg), Err(PackageError::Tampered));
}

#[test]
fn verify_detects_hidden_handler_requirement() {
    let mut pkg = build();
    pkg.archive.manifest.requirements.handlers.clear();
    assert_eq!(verify_package(&pkg), Err(PackageError::Tampered));
}

#[test]
fn verify_detects_added_credential_requirement() {
    let mut pkg = build();
    pkg.archive
        .manifest
        .requirements
        .credentials
        .push("aws_root_key".into());
    assert_eq!(verify_package(&pkg), Err(PackageError::Tampered));
}

#[test]
fn verify_detects_tampered_plugins() {
    let mut pkg = build();
    pkg.archive.manifest.requirements.plugins = vec!["backdoor".into()];
    assert_eq!(verify_package(&pkg), Err(PackageError::Tampered));
}

#[test]
fn verify_detects_tampered_queues() {
    let mut pkg = build();
    pkg.archive.manifest.requirements.queues.clear();
    assert_eq!(verify_package(&pkg), Err(PackageError::Tampered));
}

#[test]
fn verify_detects_tampered_min_engine_version() {
    let mut pkg = build();
    pkg.archive.manifest.requirements.min_engine_version = Some("99.0.0".into());
    assert_eq!(verify_package(&pkg), Err(PackageError::Tampered));
}

#[test]
fn verify_detects_tampered_created_at() {
    let mut pkg = build();
    pkg.archive.manifest.created_at = Utc.with_ymd_and_hms(2020, 1, 1, 0, 0, 0).unwrap();
    assert_eq!(verify_package(&pkg), Err(PackageError::Tampered));
}

#[test]
fn verify_detects_tampered_hash_field() {
    let mut pkg = build();
    pkg.content_hash = "0".repeat(64);
    assert_eq!(verify_package(&pkg), Err(PackageError::Tampered));
}

#[test]
fn verify_forged_hash_with_stale_signature_is_bad_signature() {
    let mut pkg = build();
    pkg.archive.files.insert("README.md".into(), "# Evil".into());
    pkg.content_hash = content_hash(&pkg.archive).unwrap();
    assert_eq!(verify_package(&pkg), Err(PackageError::BadSignature));
}

#[test]
fn verify_signature_from_wrong_key_rejected() {
    let pkg = build();
    let other = key();
    let mut forged = pkg.clone();
    forged.signature = BASE64.encode(other.sign(pkg.content_hash.as_bytes()).to_bytes());
    assert_eq!(verify_package(&forged), Err(PackageError::BadSignature));
}

#[test]
fn verify_signature_over_wrong_message_rejected() {
    let k = key();
    let mut pkg = build_package(manifest("acme/x", "1.0.0"), files(), &k).unwrap();
    pkg.signature = BASE64.encode(k.sign(b"something else entirely").to_bytes());
    assert_eq!(verify_package(&pkg), Err(PackageError::BadSignature));
}

#[test]
fn verify_swapped_public_key_rejected() {
    // Replace the embedded key with another real key: hash still matches,
    // but the signature was made by the original key.
    let mut pkg = build();
    pkg.public_key = BASE64.encode(key().verifying_key().to_bytes());
    assert_eq!(verify_package(&pkg), Err(PackageError::BadSignature));
}

#[test]
fn verify_rejects_format_version_zero() {
    let mut pkg = build();
    pkg.archive.format_version = 0;
    assert_eq!(verify_package(&pkg), Err(PackageError::UnsupportedFormat(0)));
}

#[test]
fn verify_rejects_format_version_two() {
    let mut pkg = build();
    pkg.archive.format_version = 2;
    assert_eq!(verify_package(&pkg), Err(PackageError::UnsupportedFormat(2)));
}

#[test]
fn verify_rejects_format_version_max() {
    let mut pkg = build();
    pkg.archive.format_version = u32::MAX;
    assert_eq!(
        verify_package(&pkg),
        Err(PackageError::UnsupportedFormat(u32::MAX))
    );
}

#[test]
fn verify_format_check_precedes_tamper_check() {
    // Both format and content tampered: the format error wins.
    let mut pkg = build();
    pkg.archive.format_version = 7;
    pkg.archive.files.insert("evil".into(), "x".into());
    assert_eq!(verify_package(&pkg), Err(PackageError::UnsupportedFormat(7)));
}

#[test]
fn verify_rejects_non_base64_public_key() {
    let mut pkg = build();
    pkg.public_key = "not base64!!!".into();
    assert!(matches!(verify_package(&pkg), Err(PackageError::Invalid(_))));
}

#[test]
fn verify_rejects_wrong_length_public_key() {
    let mut pkg = build();
    pkg.public_key = BASE64.encode([0u8; 16]); // valid base64, wrong length
    let err = verify_package(&pkg).unwrap_err();
    assert!(matches!(&err, PackageError::Invalid(msg) if msg.contains("32 bytes")));
}

#[test]
fn verify_rejects_empty_public_key() {
    let mut pkg = build();
    pkg.public_key = String::new();
    assert!(matches!(verify_package(&pkg), Err(PackageError::Invalid(_))));
}

#[test]
fn verify_rejects_non_base64_signature() {
    let mut pkg = build();
    pkg.signature = "%%%not-base64%%%".into();
    assert!(matches!(verify_package(&pkg), Err(PackageError::Invalid(_))));
}

#[test]
fn verify_rejects_wrong_length_signature() {
    let mut pkg = build();
    pkg.signature = BASE64.encode([0u8; 32]); // valid base64, wrong length
    let err = verify_package(&pkg).unwrap_err();
    assert!(matches!(&err, PackageError::Invalid(msg) if msg.contains("64 bytes")));
}

#[test]
fn verify_rejects_empty_signature() {
    let mut pkg = build();
    pkg.signature = String::new();
    assert!(matches!(verify_package(&pkg), Err(PackageError::Invalid(_))));
}

#[test]
fn verify_rejects_garbage_32_byte_public_key() {
    // Correct length, but not the signer's key — must fail one way or another.
    let mut pkg = build();
    pkg.public_key = BASE64.encode([0xAB_u8; 32]);
    assert!(verify_package(&pkg).is_err());
}

#[test]
fn verify_rejects_zeroed_64_byte_signature() {
    let mut pkg = build();
    pkg.signature = BASE64.encode([0u8; 64]);
    assert_eq!(verify_package(&pkg), Err(PackageError::BadSignature));
}

#[test]
fn verify_is_pure_and_repeatable() {
    let pkg = build();
    verify_package(&pkg).unwrap();
    verify_package(&pkg).unwrap();
    verify_package(&pkg).unwrap();
}

// ---------------------------------------------------------------------------
// check_trust
// ---------------------------------------------------------------------------

#[test]
fn trust_single_trusted_key() {
    let pkg = build();
    let policy = TrustPolicy {
        trusted_keys: vec![pkg.public_key.clone()],
        allow_untrusted: false,
    };
    assert_eq!(check_trust(&pkg, &policy), Ok(TrustLevel::Trusted));
}

#[test]
fn trust_key_among_multiple_trusted() {
    let pkg = build();
    let policy = TrustPolicy {
        trusted_keys: vec![
            BASE64.encode(key().verifying_key().to_bytes()),
            pkg.public_key.clone(),
            BASE64.encode(key().verifying_key().to_bytes()),
        ],
        allow_untrusted: false,
    };
    assert_eq!(check_trust(&pkg, &policy), Ok(TrustLevel::Trusted));
}

#[test]
fn trust_untrusted_denied_by_default_policy() {
    let pkg = build();
    let policy = TrustPolicy {
        trusted_keys: vec![],
        allow_untrusted: false,
    };
    assert_eq!(check_trust(&pkg, &policy), Err(PackageError::UntrustedPublisher));
}

#[test]
fn trust_untrusted_allowed_with_opt_in() {
    let pkg = build();
    let policy = TrustPolicy {
        trusted_keys: vec![],
        allow_untrusted: true,
    };
    assert_eq!(check_trust(&pkg, &policy), Ok(TrustLevel::UntrustedAllowed));
}

#[test]
fn trust_wrong_keys_only_denied() {
    let pkg = build();
    let policy = TrustPolicy {
        trusted_keys: vec![
            BASE64.encode(key().verifying_key().to_bytes()),
            BASE64.encode(key().verifying_key().to_bytes()),
        ],
        allow_untrusted: false,
    };
    assert_eq!(check_trust(&pkg, &policy), Err(PackageError::UntrustedPublisher));
}

#[test]
fn trust_wrong_keys_with_opt_in_is_untrusted_allowed() {
    let pkg = build();
    let policy = TrustPolicy {
        trusted_keys: vec![BASE64.encode(key().verifying_key().to_bytes())],
        allow_untrusted: true,
    };
    assert_eq!(check_trust(&pkg, &policy), Ok(TrustLevel::UntrustedAllowed));
}

#[test]
fn trust_trusted_wins_over_untrusted_opt_in() {
    let pkg = build();
    let policy = TrustPolicy {
        trusted_keys: vec![pkg.public_key.clone()],
        allow_untrusted: true,
    };
    assert_eq!(check_trust(&pkg, &policy), Ok(TrustLevel::Trusted));
}

#[test]
fn trust_key_comparison_is_exact_string_match() {
    // A key with different (invalid) padding/whitespace is NOT trusted.
    let pkg = build();
    let policy = TrustPolicy {
        trusted_keys: vec![format!(" {}", pkg.public_key)],
        allow_untrusted: false,
    };
    assert_eq!(check_trust(&pkg, &policy), Err(PackageError::UntrustedPublisher));
}

// ---------------------------------------------------------------------------
// sequence_files / contract_files selection
// ---------------------------------------------------------------------------

#[test]
fn selection_picks_sequences_json() {
    let a = archive_with_paths(&["sequences/a.json", "sequences/b.json"]);
    let picked = sequence_files(&a);
    assert_eq!(picked.len(), 2);
}

#[test]
fn selection_excludes_non_json_in_sequences() {
    let a = archive_with_paths(&["sequences/notes.txt", "sequences/a.yaml"]);
    assert!(sequence_files(&a).is_empty());
}

#[test]
fn selection_includes_nested_sequence_paths() {
    let a = archive_with_paths(&["sequences/nested/deep/a.json"]);
    assert_eq!(sequence_files(&a).len(), 1);
}

#[test]
fn selection_requires_sequences_prefix() {
    let a = archive_with_paths(&["seq/a.json", "sequencesX/a.json", "a.json"]);
    assert!(sequence_files(&a).is_empty());
}

#[test]
fn selection_is_case_sensitive() {
    let a = archive_with_paths(&["Sequences/a.json", "sequences/A.JSON"]);
    assert!(sequence_files(&a).is_empty());
}

#[test]
fn selection_edge_bare_dot_json_in_sequences_is_included() {
    // Actual semantics: "sequences/.json" starts with the prefix and ends
    // with ".json", so it is selected.
    let a = archive_with_paths(&["sequences/.json"]);
    assert_eq!(sequence_files(&a).len(), 1);
}

#[test]
fn selection_contracts_require_contracts_json_suffix() {
    let a = archive_with_paths(&[
        "contracts/a.contracts.json",
        "contracts/b.json",
        "contracts/c.contracts.json.bak",
    ]);
    let picked = contract_files(&a);
    assert_eq!(picked.len(), 1);
    assert_eq!(picked[0].0, "contracts/a.contracts.json");
}

#[test]
fn selection_contracts_require_contracts_prefix() {
    let a = archive_with_paths(&["sequences/a.contracts.json", "a.contracts.json"]);
    assert!(contract_files(&a).is_empty());
}

#[test]
fn selection_includes_nested_contract_paths() {
    let a = archive_with_paths(&["contracts/team/a.contracts.json"]);
    assert_eq!(contract_files(&a).len(), 1);
}

#[test]
fn selection_excludes_readme_from_both() {
    let a = archive_with_paths(&["README.md", "sequences/a.json", "contracts/a.contracts.json"]);
    let seq_paths: Vec<&String> = sequence_files(&a).into_iter().map(|(p, _)| p).collect();
    let con_paths: Vec<&String> = contract_files(&a).into_iter().map(|(p, _)| p).collect();
    assert!(!seq_paths.iter().any(|p| p.as_str() == "README.md"));
    assert!(!con_paths.iter().any(|p| p.as_str() == "README.md"));
}

#[test]
fn selection_contract_files_are_not_sequence_files() {
    let a = archive_with_paths(&["contracts/a.contracts.json", "sequences/a.json"]);
    assert_eq!(sequence_files(&a).len(), 1);
    assert_eq!(sequence_files(&a)[0].0, "sequences/a.json");
    assert_eq!(contract_files(&a).len(), 1);
}

#[test]
fn selection_sequence_files_include_contract_style_names_under_sequences() {
    // A "*.contracts.json" under sequences/ still ends with ".json".
    let a = archive_with_paths(&["sequences/a.contracts.json"]);
    assert_eq!(sequence_files(&a).len(), 1);
}

#[test]
fn selection_results_are_sorted_by_path() {
    let a = archive_with_paths(&[
        "sequences/z.json",
        "sequences/a.json",
        "sequences/m.json",
    ]);
    let picked: Vec<&str> = sequence_files(&a).into_iter().map(|(p, _)| p.as_str()).collect();
    assert_eq!(picked, vec!["sequences/a.json", "sequences/m.json", "sequences/z.json"]);
}

#[test]
fn selection_empty_when_no_matching_files() {
    let a = archive_with_paths(&["README.md", "docs/guide.md"]);
    assert!(sequence_files(&a).is_empty());
    assert!(contract_files(&a).is_empty());
}

#[test]
fn selection_returns_file_content() {
    let mut f = BTreeMap::new();
    f.insert("sequences/a.json".to_string(), r#"{"name":"a"}"#.to_string());
    let a = PackageArchive {
        format_version: PACKAGE_FORMAT_VERSION,
        manifest: manifest("acme/x", "1.0.0"),
        files: f,
    };
    let picked = sequence_files(&a);
    assert_eq!(picked[0].1, r#"{"name":"a"}"#);
}

// ---------------------------------------------------------------------------
// PackageError Display
// ---------------------------------------------------------------------------

#[test]
fn display_invalid() {
    let e = PackageError::Invalid("boom".into());
    assert_eq!(e.to_string(), "invalid package: boom");
}

#[test]
fn display_unsupported_format_mentions_both_versions() {
    let e = PackageError::UnsupportedFormat(9);
    let msg = e.to_string();
    assert!(msg.contains('9'), "{msg}");
    assert!(msg.contains(&PACKAGE_FORMAT_VERSION.to_string()), "{msg}");
    assert_eq!(
        msg,
        "unsupported package format version 9 (this tool supports 1)"
    );
}

#[test]
fn display_tampered() {
    assert_eq!(
        PackageError::Tampered.to_string(),
        "content hash mismatch — the archive was modified after signing"
    );
}

#[test]
fn display_bad_signature() {
    assert_eq!(
        PackageError::BadSignature.to_string(),
        "signature verification failed — not signed by the embedded key"
    );
}

#[test]
fn display_untrusted_publisher() {
    assert_eq!(
        PackageError::UntrustedPublisher.to_string(),
        "publisher key is not trusted; pass the key explicitly or allow untrusted installs"
    );
}

#[test]
fn display_downgrade_includes_versions() {
    let e = PackageError::Downgrade {
        installed: "2.0.0".into(),
        incoming: "1.5.0".into(),
    };
    assert_eq!(
        e.to_string(),
        "downgrade rejected: installed version 2.0.0 >= incoming 1.5.0"
    );
}

#[test]
fn package_format_version_is_one() {
    assert_eq!(PACKAGE_FORMAT_VERSION, 1);
}

// ---------------------------------------------------------------------------
// Reproducibility
// ---------------------------------------------------------------------------

#[test]
fn same_key_and_content_produce_identical_serialized_bytes() {
    let k = key();
    let a = build_package(manifest("acme/checkout", "1.0.0"), files(), &k).unwrap();
    let b = build_package(manifest("acme/checkout", "1.0.0"), files(), &k).unwrap();
    assert_eq!(a, b);
    assert_eq!(
        serde_json::to_vec(&a).unwrap(),
        serde_json::to_vec(&b).unwrap()
    );
}

#[test]
fn same_content_different_key_same_hash_different_signature() {
    let a = build_package(manifest("acme/checkout", "1.0.0"), files(), &key()).unwrap();
    let b = build_package(manifest("acme/checkout", "1.0.0"), files(), &key()).unwrap();
    assert_eq!(a.content_hash, b.content_hash);
    assert_ne!(a.signature, b.signature);
    assert_ne!(a.public_key, b.public_key);
}

#[test]
fn different_content_same_key_different_hash() {
    let k = key();
    let a = build_package(manifest("acme/checkout", "1.0.0"), files(), &k).unwrap();
    let mut f2 = files();
    f2.insert("EXTRA.md".into(), "extra".into());
    let b = build_package(manifest("acme/checkout", "1.0.0"), f2, &k).unwrap();
    assert_ne!(a.content_hash, b.content_hash);
    assert_ne!(a.signature, b.signature);
    assert_eq!(a.public_key, b.public_key);
}

#[test]
fn ed25519_signing_is_deterministic_per_key() {
    // Same key, same content — signature identical across builds (no nonce
    // randomness leaks into the artifact).
    let k = key();
    let sigs: Vec<String> = (0..3)
        .map(|_| {
            build_package(manifest("acme/checkout", "2.3.4"), files(), &k)
                .unwrap()
                .signature
        })
        .collect();
    assert_eq!(sigs[0], sigs[1]);
    assert_eq!(sigs[1], sigs[2]);
}

#[test]
fn serde_round_trip_preserves_package_equality_and_verifiability() {
    let pkg = build();
    let json = serde_json::to_string(&pkg).unwrap();
    let back: SignedPackage = serde_json::from_str(&json).unwrap();
    assert_eq!(back, pkg);
    verify_package(&back).unwrap();
}

#[test]
fn hash_stable_across_serde_round_trip() {
    let pkg = build();
    let json = serde_json::to_string(&pkg.archive).unwrap();
    let back: PackageArchive = serde_json::from_str(&json).unwrap();
    assert_eq!(content_hash(&back).unwrap(), pkg.content_hash);
}

#[test]
fn manifests_with_default_requirements_build_and_verify() {
    let m = PackageManifest {
        name: "acme/minimal".into(),
        version: "0.1.0".into(),
        description: String::new(),
        publisher: String::new(),
        requirements: PackageRequirements::default(),
        created_at: Utc.with_ymd_and_hms(2026, 1, 1, 0, 0, 0).unwrap(),
    };
    let pkg = build_with(m);
    verify_package(&pkg).unwrap();
}

#[test]
fn default_requirements_are_all_empty() {
    let r = PackageRequirements::default();
    assert!(r.handlers.is_empty());
    assert!(r.credentials.is_empty());
    assert!(r.plugins.is_empty());
    assert!(r.queues.is_empty());
    assert!(r.min_engine_version.is_none());
}
