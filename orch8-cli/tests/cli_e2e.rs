//! End-to-end integration tests for the compiled `orch8` CLI binary.
//!
//! Each test runs the real binary via `env!("CARGO_BIN_EXE_orch8")` in its
//! own tempdir with a scrubbed environment: every `ORCH8_*` variable is
//! removed and `ORCH8_CONTEXTS_FILE` (the CLI's fleet-context path override)
//! is pointed into the tempdir so no host state is read or written.
//!
//! `tempfile` / `serde_json` come from the crate's regular `[dependencies]`,
//! which integration tests may use; no new dev-dependencies were added.

use std::path::{Path, PathBuf};
use std::process::Command;

/// A minimal but complete sequence definition the package builder accepts
/// (mirrors `VALID_SEQUENCE_JSON` in package_cmd_coverage_tests.rs).
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

/// Base64 of the 32-byte Ed25519 seed `[5u8; 32]` (same value the unit
/// coverage tests derive via `seed_base64(5)`).
const TEST_SEED_BASE64: &str = "BQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQU=";

/// Per-test sandbox: a tempdir plus the contexts-file path inside it.
struct Sandbox {
    _dir: tempfile::TempDir,
    root: PathBuf,
    contexts_file: PathBuf,
}

impl Sandbox {
    fn new() -> Self {
        let dir = tempfile::tempdir().expect("create tempdir");
        let root = dir.path().to_path_buf();
        let contexts_file = root.join("contexts.json");
        Self {
            _dir: dir,
            root,
            contexts_file,
        }
    }

    /// Build a `Command` for the compiled binary with an isolated env:
    /// everything cleared (no inherited `ORCH8_*`, credentials, or proxy
    /// vars), only the essentials restored, and the contexts file redirected
    /// into the sandbox. The process also runs with the sandbox as cwd so
    /// relative paths (`orch8-packages.lock`, `.orch8-contexts.json`) never
    /// touch the developer's working tree.
    fn cmd(&self) -> Command {
        let mut cmd = Command::new(env!("CARGO_BIN_EXE_orch8"));
        cmd.env_clear();
        for key in ["PATH", "TMPDIR", "HOME", "SystemRoot"] {
            if let Some(value) = std::env::var_os(key) {
                cmd.env(key, value);
            }
        }
        cmd.env("ORCH8_CONTEXTS_FILE", &self.contexts_file);
        cmd.current_dir(&self.root);
        cmd
    }

    /// Run the binary with `args`, assert it exits 0, return stdout.
    fn run_ok(&self, args: &[&str]) -> String {
        let output = self
            .cmd()
            .args(args)
            .output()
            .expect("spawn orch8 binary");
        let stdout = String::from_utf8_lossy(&output.stdout).into_owned();
        let stderr = String::from_utf8_lossy(&output.stderr).into_owned();
        assert!(
            output.status.success(),
            "orch8 {args:?} should succeed\nstdout:\n{stdout}\nstderr:\n{stderr}"
        );
        stdout
    }

    /// Run the binary with `args`, assert it fails, return stderr.
    fn run_err(&self, args: &[&str]) -> String {
        let output = self
            .cmd()
            .args(args)
            .output()
            .expect("spawn orch8 binary");
        let stderr = String::from_utf8_lossy(&output.stderr).into_owned();
        assert!(
            !output.status.success(),
            "orch8 {args:?} should fail\nstdout:\n{}\nstderr:\n{stderr}",
            String::from_utf8_lossy(&output.stdout)
        );
        stderr
    }
}

#[cfg(unix)]
fn file_mode(path: &Path) -> u32 {
    use std::os::unix::fs::PermissionsExt as _;
    std::fs::metadata(path).unwrap().permissions().mode() & 0o777
}

// ---------------------------------------------------------------------------
// Smoke: --help
// ---------------------------------------------------------------------------

#[test]
fn help_exits_zero_and_lists_subcommands() {
    let sb = Sandbox::new();
    let stdout = sb.run_ok(&["--help"]);
    for sub in ["context", "init", "package", "health"] {
        assert!(
            stdout.contains(sub),
            "--help should mention `{sub}`\n{stdout}"
        );
    }
}

// ---------------------------------------------------------------------------
// Fleet context lifecycle
// ---------------------------------------------------------------------------

#[test]
fn context_lifecycle_set_list_use_remove() {
    let sb = Sandbox::new();

    // Add two contexts. `context set` (the add/replace subcommand) validates
    // name + URL and persists with secure permissions.
    let stdout = sb.run_ok(&[
        "context",
        "set",
        "prod",
        "--url",
        "https://engine.example/api/v1",
        "--tenant-id",
        "tenant-a",
        "--api-key",
        "secret-a",
    ]);
    assert!(stdout.contains("Saved fleet context prod"), "{stdout}");

    sb.run_ok(&[
        "context",
        "set",
        "staging",
        "--url",
        "https://staging.example/api/v1/",
        "--tenant-id",
        "tenant-b",
        "--api-key",
        "secret-b",
    ]);

    // The persisted file must not be group/world readable.
    #[cfg(unix)]
    assert_eq!(
        file_mode(&sb.contexts_file),
        0o600,
        "contexts file holds credentials and must be 0600"
    );

    // List shows both names; nothing selected yet, so no `*` marker.
    let stdout = sb.run_ok(&["context", "list"]);
    assert!(stdout.contains("prod"), "{stdout}");
    assert!(stdout.contains("staging"), "{stdout}");
    assert!(!stdout.contains('*'), "nothing selected yet:\n{stdout}");

    // `context use` switches the selected context.
    let stdout = sb.run_ok(&["context", "use", "prod"]);
    assert!(stdout.contains("Selected fleet context prod"), "{stdout}");

    let stdout = sb.run_ok(&["context", "list"]);
    assert!(stdout.contains("* prod"), "{stdout}");
    assert!(stdout.contains(" staging"), "{stdout}");
    // Credentials are never printed by list.
    assert!(!stdout.contains("secret-a"), "{stdout}");
    assert!(!stdout.contains("secret-b"), "{stdout}");

    // Remove the non-selected context; it disappears from the listing.
    let stdout = sb.run_ok(&["context", "remove", "staging"]);
    assert!(stdout.contains("Removed fleet context staging"), "{stdout}");

    let stdout = sb.run_ok(&["context", "list"]);
    assert!(!stdout.contains("staging"), "{stdout}");
    assert!(stdout.contains("* prod"), "{stdout}");
}

#[test]
fn context_set_rejects_invalid_name() {
    let sb = Sandbox::new();
    let stderr = sb.run_err(&[
        "context",
        "set",
        "bad name!",
        "--url",
        "https://engine.example/api/v1",
        "--tenant-id",
        "t",
        "--api-key",
        "k",
    ]);
    assert!(
        stderr.contains("context name must be 1-64 ASCII letters, digits, '-' or '_'"),
        "{stderr}"
    );
    // The failed write must not leave a contexts file behind.
    assert!(!sb.contexts_file.exists());
}

#[test]
fn context_set_rejects_invalid_url() {
    let sb = Sandbox::new();

    // Not parseable as a URL at all.
    let stderr = sb.run_err(&[
        "context",
        "set",
        "prod",
        "--url",
        "not-a-url",
        "--tenant-id",
        "t",
        "--api-key",
        "k",
    ]);
    assert!(stderr.contains("context URL is invalid"), "{stderr}");

    // Absolute but not http(s).
    let stderr = sb.run_err(&[
        "context",
        "set",
        "prod",
        "--url",
        "ftp://engine.example/api/v1",
        "--tenant-id",
        "t",
        "--api-key",
        "k",
    ]);
    assert!(
        stderr.contains("context URL must be an absolute http(s) URL"),
        "{stderr}"
    );
}

#[test]
fn context_use_and_remove_unknown_context_fail() {
    let sb = Sandbox::new();
    let stderr = sb.run_err(&["context", "use", "ghost"]);
    assert!(
        stderr.contains("fleet context 'ghost' does not exist"),
        "{stderr}"
    );
    let stderr = sb.run_err(&["context", "remove", "ghost"]);
    assert!(
        stderr.contains("fleet context 'ghost' does not exist"),
        "{stderr}"
    );
}

/// Fail-closed credential guard: a contexts file that is group/world
/// readable must be refused by every command that loads it.
#[cfg(unix)]
#[test]
fn context_file_with_insecure_permissions_is_refused() {
    let sb = Sandbox::new();
    std::fs::write(
        &sb.contexts_file,
        r#"{"selected":null,"contexts":{}}"#,
    )
    .unwrap();
    std::fs::set_permissions(
        &sb.contexts_file,
        std::os::unix::fs::PermissionsExt::from_mode(0o644),
    )
    .unwrap();

    let stderr = sb.run_err(&["context", "list"]);
    assert!(
        stderr.contains("must not be accessible by group/others"),
        "{stderr}"
    );
}

// ---------------------------------------------------------------------------
// init scaffolding
// ---------------------------------------------------------------------------

#[test]
fn init_scaffolds_project_and_never_clobbers() {
    let sb = Sandbox::new();
    let project = sb.root.join("proj");
    let project_arg = project.to_str().unwrap();

    let stdout = sb.run_ok(&["init", project_arg]);
    assert!(
        stdout.contains("Initialized Orch8 project in"),
        "{stdout}"
    );
    for file in ["orch8.toml", "sequence.json", "docker-compose.yml"] {
        assert!(
            project.join(file).exists(),
            "init should create {file}\n{stdout}"
        );
    }

    // orch8.toml contains generated secrets and must be 0600.
    #[cfg(unix)]
    assert_eq!(
        file_mode(&project.join("orch8.toml")),
        0o600,
        "generated secrets must not be group/world readable"
    );

    // A second init must not clobber existing files.
    let toml_before = std::fs::read_to_string(project.join("orch8.toml")).unwrap();
    let seq_before = std::fs::read_to_string(project.join("sequence.json")).unwrap();
    let stdout = sb.run_ok(&["init", project_arg]);
    assert!(
        stdout.contains("skip orch8.toml (already exists)"),
        "{stdout}"
    );
    assert!(
        stdout.contains("skip sequence.json (already exists)"),
        "{stdout}"
    );
    assert_eq!(
        std::fs::read_to_string(project.join("orch8.toml")).unwrap(),
        toml_before,
        "second init must not rewrite orch8.toml (it embeds generated keys)"
    );
    assert_eq!(
        std::fs::read_to_string(project.join("sequence.json")).unwrap(),
        seq_before,
        "second init must not rewrite sequence.json"
    );
}

// ---------------------------------------------------------------------------
// package build/verify round trip (fully offline — no server or key service)
// ---------------------------------------------------------------------------

#[test]
fn package_build_verify_round_trip_and_tamper_detection() {
    let sb = Sandbox::new();
    let pkg_src = sb.root.join("pkg");
    std::fs::create_dir_all(pkg_src.join("sequences")).unwrap();
    std::fs::write(
        pkg_src.join("package.json"),
        r#"{"name": "acme/billing", "version": "1.2.0", "description": "billing", "publisher": "acme"}"#,
    )
    .unwrap();
    std::fs::write(pkg_src.join("sequences/billing.json"), VALID_SEQUENCE_JSON).unwrap();
    let out = sb.root.join("billing.orch8pkg");

    // Build + sign with a fixed test seed.
    let stdout = sb.run_ok(&[
        "package",
        "build",
        pkg_src.to_str().unwrap(),
        "--key",
        TEST_SEED_BASE64,
        "--out",
        out.to_str().unwrap(),
    ]);
    assert!(stdout.contains("built acme/billing v1.2.0"), "{stdout}");
    assert!(out.exists(), "package file should be written");

    // Verify passes for the untouched package.
    let stdout = sb.run_ok(&["package", "verify", out.to_str().unwrap()]);
    assert!(stdout.contains("integrity: OK"), "{stdout}");
    assert!(stdout.contains("signature: OK"), "{stdout}");
    assert!(stdout.contains("trust:     not checked"), "{stdout}");

    // Tamper with the packaged manifest; verification must fail.
    let raw = std::fs::read_to_string(&out).unwrap();
    let mut pkg: serde_json::Value = serde_json::from_str(&raw).unwrap();
    pkg["archive"]["manifest"]["version"] = serde_json::json!("9.9.9");
    let tampered = sb.root.join("tampered.orch8pkg");
    std::fs::write(&tampered, serde_json::to_string_pretty(&pkg).unwrap()).unwrap();

    let stderr = sb.run_err(&["package", "verify", tampered.to_str().unwrap()]);
    assert!(
        stderr.contains("content hash mismatch"),
        "{stderr}"
    );
}
