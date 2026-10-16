//! `orch8 package build|verify`: a package never leaves the publisher's
//! machine unless its manifest, every sequence, every contract suite and its
//! requirements are valid and each archive path is provided exactly once; the
//! signing seed must be exactly 32 base64 bytes; verification reports an
//! untrusted publisher to the caller.

use super::*;

use ed25519_dalek::SigningKey;

fn seed_bytes(seed: u8) -> [u8; 32] {
    [seed; 32]
}

fn seed_base64(seed: u8) -> String {
    BASE64.encode(seed_bytes(seed))
}

fn test_manifest(version: &str) -> PackageManifest {
    PackageManifest {
        name: "acme/billing".into(),
        version: version.into(),
        description: "billing workflows".into(),
        publisher: "acme".into(),
        requirements: PackageRequirements::default(),
        created_at: chrono::DateTime::parse_from_rfc3339("2026-07-25T00:00:00Z")
            .unwrap()
            .to_utc(),
    }
}

fn test_package(seed: u8) -> SignedPackage {
    build_package(
        test_manifest("1.2.0"),
        BTreeMap::from([("sequences/billing.json".to_string(), "{}".to_string())]),
        &SigningKey::from_bytes(&seed_bytes(seed)),
    )
    .unwrap()
}

/// A minimal but complete sequence definition the builder accepts.
const VALID_SEQUENCE_JSON: &str = r#"{
  "id": "0191e4f2-a1b2-7c3d-8e4f-a5b6c7d8e9f0",
  "tenant_id": "demo",
  "namespace": "default",
  "name": "billing",
  "version": 1,
  "blocks": [
    { "type": "step", "id": "charge", "handler": "charge_card" }
  ],
  "created_at": "2026-07-25T00:00:00Z"
}"#;

const VALID_SEQUENCE_YAML: &str = "id: 0191e4f2-a1b2-7c3d-8e4f-a5b6c7d8e9f0\ntenant_id: demo\n\
     namespace: default\nname: billing\nversion: 1\ncreated_at: '2026-07-25T00:00:00Z'\n\
     blocks:\n  - type: step\n    id: charge\n    handler: charge_card\n";

const MANIFEST: &str = r#"{"name": "acme/billing", "version": "1.2.0", "description": "billing", "publisher": "acme"}"#;

/// A package source directory with `package.json` plus the given files.
fn package_dir(files: &[(&str, &str)]) -> tempfile::TempDir {
    let dir = tempfile::tempdir().unwrap();
    for (path, content) in files {
        let path = dir.path().join(path);
        std::fs::create_dir_all(path.parent().unwrap()).unwrap();
        std::fs::write(path, content).unwrap();
    }
    dir
}

fn build_in(dir: &tempfile::TempDir) -> Result<PathBuf> {
    let out = dir.path().join("out.orch8pkg");
    build(dir.path(), &seed_base64(5), Some(&out)).map(|()| out)
}

#[test]
fn signing_key_accepts_an_inline_or_at_file_base64_seed() {
    let dir = tempfile::tempdir().unwrap();
    let plain = dir.path().join("plain.key");
    std::fs::write(&plain, seed_base64(9)).unwrap();
    let padded = dir.path().join("padded.key");
    std::fs::write(&padded, format!("  {}\n", seed_base64(11))).unwrap();
    let cases = [
        (seed_base64(7), 7),
        (format!("@{}", plain.display()), 9),
        (format!("@{}", padded.display()), 11),
    ];
    for (arg, seed) in cases {
        let key = load_signing_key(&arg).unwrap();
        assert_eq!(key.to_bytes(), seed_bytes(seed), "case {arg}");
    }
}

#[test]
fn signing_key_rejects_non_base64_short_or_unreadable_seeds() {
    let cases = [
        ("not!base64!".to_string(), "key must be base64"),
        (
            BASE64.encode([1u8; 16]),
            "key seed must be exactly 32 bytes",
        ),
        (
            "@/nonexistent/publisher.key".to_string(),
            "reading key file /nonexistent/publisher.key",
        ),
    ];
    for (arg, expected) in cases {
        let error = load_signing_key(&arg).unwrap_err();
        assert!(format!("{error:#}").contains(expected), "{arg}: {error:#}");
    }
}

#[test]
fn reading_a_missing_or_non_package_file_fails() {
    let dir = tempfile::tempdir().unwrap();
    let absent = dir.path().join("absent.orch8pkg");
    let garbage = dir.path().join("bad.orch8pkg");
    std::fs::write(&garbage, b"{nope").unwrap();
    let cases = [
        (absent.clone(), format!("reading {}", absent.display())),
        (garbage, "file is not a signed orch8 package".to_string()),
    ];
    for (path, expected) in cases {
        let error = read_package(&path).unwrap_err();
        assert!(format!("{error:#}").contains(&expected), "{error:#}");
    }
}

/// `(case, source files, expected error fragment)`.
type BuildRejection = (
    &'static str,
    Vec<(&'static str, &'static str)>,
    &'static str,
);

#[test]
fn build_rejects_invalid_sources_without_writing_a_package() {
    let suite_without_cases = r#"{"cases": []}"#;
    let cases: [BuildRejection; 9] = [
        ("no package.json", vec![], "package.json"),
        (
            "manifest not json",
            vec![("package.json", "{not json")],
            "package.json is not valid JSON",
        ),
        (
            "manifest without name",
            vec![("package.json", r#"{"version": "1.0.0"}"#)],
            "'name' is required",
        ),
        (
            "manifest without version",
            vec![("package.json", r#"{"name": "acme/billing"}"#)],
            "'version' is required",
        ),
        (
            "invalid requirements",
            vec![(
                "package.json",
                r#"{"name": "acme/billing", "version": "1.0.0", "requirements": {"handlers": "charge"}}"#,
            )],
            "package.json: invalid 'requirements'",
        ),
        (
            "broken sequence",
            vec![
                ("package.json", MANIFEST),
                ("sequences/billing.json", "{broken"),
            ],
            "sequences/billing.json is not a valid sequence definition",
        ),
        (
            "unparseable contract suite",
            vec![
                ("package.json", MANIFEST),
                ("contracts/billing.contracts.json", "{broken"),
            ],
            "contracts/billing.contracts.json is not a valid contract suite",
        ),
        (
            "contract suite failing validation",
            vec![
                ("package.json", MANIFEST),
                ("contracts/billing.contracts.json", suite_without_cases),
            ],
            "contracts/billing.contracts.json: contract suite has no cases",
        ),
        (
            "same sequence as JSON and YAML",
            vec![
                ("package.json", MANIFEST),
                ("sequences/billing.json", VALID_SEQUENCE_JSON),
                ("sequences/billing.yaml", VALID_SEQUENCE_YAML),
            ],
            "sequences/billing.json is provided twice",
        ),
    ];
    for (case, files, expected) in cases {
        let dir = package_dir(&files);

        let error = build_in(&dir).unwrap_err();

        assert!(format!("{error:#}").contains(expected), "{case}: {error:#}");
        assert!(!dir.path().join("out.orch8pkg").exists(), "{case}");
    }
}

#[test]
fn built_package_verifies_and_carries_manifest_sequences_and_readme() {
    let dir = package_dir(&[
        ("package.json", MANIFEST),
        ("sequences/billing.json", VALID_SEQUENCE_JSON),
        ("README.md", "# Billing"),
    ]);

    let out = build_in(&dir).unwrap();

    verify(&out, &[]).unwrap();
    let pkg = read_package(&out).unwrap();
    assert_eq!(pkg.archive.manifest.name, "acme/billing");
    assert_eq!(pkg.archive.manifest.version, "1.2.0");
    assert_eq!(
        pkg.archive.files.keys().collect::<Vec<_>>(),
        ["README.md", "sequences/billing.json"]
    );
    verify_package(&pkg).unwrap();
}

#[test]
fn verify_accepts_a_package_signed_by_a_trusted_publisher_key() {
    let dir = package_dir(&[
        ("package.json", MANIFEST),
        ("sequences/billing.json", VALID_SEQUENCE_JSON),
    ]);
    let out = build_in(&dir).unwrap();
    let pkg = read_package(&out).unwrap();

    verify(&out, std::slice::from_ref(&pkg.public_key)).unwrap();
}

#[test]
fn verify_returns_untrusted_error_to_caller() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("package.orch8pkg");
    std::fs::write(&path, serde_json::to_vec(&test_package(1)).unwrap()).unwrap();
    let unrelated = BASE64.encode(
        SigningKey::from_bytes(&seed_bytes(2))
            .verifying_key()
            .to_bytes(),
    );

    let error = verify(&path, &[unrelated]).unwrap_err();

    assert!(error.to_string().contains("package is not trusted"));
}

// The two tests below exercise `orch8_publisher::package` helpers and belong
// in that crate; they stay here until orch8-publisher gains equivalents
// (its `upgrade_monotonicity` / `file_helpers_pick_the_right_entries` do not
// cover these inputs).

#[test]
fn upgrade_rejects_unparseable_versions() {
    for (installed, incoming) in [("1.2.0", "latest"), ("", "1.0.0")] {
        assert!(
            check_upgrade(installed, incoming).is_err(),
            "{installed:?} -> {incoming:?}"
        );
    }
}

#[test]
fn file_filters_keep_only_json_sequences_and_contract_suites() {
    let archive = orch8_publisher::package::PackageArchive {
        format_version: 1,
        manifest: test_manifest("1.0.0"),
        files: BTreeMap::from([
            ("sequences/a.json".to_string(), "{}".to_string()),
            ("sequences/b.txt".to_string(), "x".to_string()),
            ("contracts/a.contracts.json".to_string(), "{}".to_string()),
            ("contracts/c.json".to_string(), "{}".to_string()),
            ("README.md".to_string(), "# docs".to_string()),
        ]),
    };

    let sequences: Vec<&str> = sequence_files(&archive)
        .iter()
        .map(|(path, _)| path.as_str())
        .collect();
    let contracts: Vec<&str> = contract_files(&archive)
        .iter()
        .map(|(path, _)| path.as_str())
        .collect();

    assert_eq!(sequences, ["sequences/a.json"]);
    assert_eq!(contracts, ["contracts/a.contracts.json"]);
}
