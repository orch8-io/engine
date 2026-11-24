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
/// (mirrors `VALID_SEQUENCE_JSON` in `package_cmd_build_tests.rs`).
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
        self.command(env!("CARGO_BIN_EXE_orch8"))
    }

    fn command(&self, program: &str) -> Command {
        let mut cmd = Command::new(program);
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
        let output = self.cmd().args(args).output().expect("spawn orch8 binary");
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
        let output = self.cmd().args(args).output().expect("spawn orch8 binary");
        let stderr = String::from_utf8_lossy(&output.stderr).into_owned();
        assert!(
            !output.status.success(),
            "orch8 {args:?} should fail\nstdout:\n{}\nstderr:\n{stderr}",
            String::from_utf8_lossy(&output.stdout)
        );
        stderr
    }

    fn write_sequence(&self, name: &str, definition: &str) -> PathBuf {
        let path = self.root.join(name);
        std::fs::write(&path, definition).expect("write sequence fixture");
        path
    }
}

#[cfg(unix)]
fn file_mode(path: &Path) -> u32 {
    use std::os::unix::fs::PermissionsExt as _;
    std::fs::metadata(path).unwrap().permissions().mode() & 0o777
}

#[tokio::test]
async fn sqlite_maintenance_reports_bounded_progress_under_a_live_reader() {
    use sqlx::sqlite::SqliteConnectOptions;
    use sqlx::{Connection as _, SqliteConnection};

    let sandbox = Sandbox::new();
    let path = sandbox.root.join("maintenance.db");
    let options = SqliteConnectOptions::new()
        .filename(&path)
        .create_if_missing(true)
        .pragma("wal_autocheckpoint", "0");
    let mut writer = SqliteConnection::connect_with(&options).await.unwrap();
    sqlx::query("PRAGMA auto_vacuum=INCREMENTAL")
        .execute(&mut writer)
        .await
        .unwrap();
    sqlx::query("PRAGMA journal_mode=WAL")
        .fetch_all(&mut writer)
        .await
        .unwrap();
    sqlx::query("CREATE TABLE payloads(id INTEGER PRIMARY KEY, blob BLOB)")
        .execute(&mut writer)
        .await
        .unwrap();
    sqlx::query(
        "WITH RECURSIVE ids(n) AS (VALUES(1) UNION ALL SELECT n+1 FROM ids WHERE n<64)
        INSERT INTO payloads SELECT n, zeroblob(8000) FROM ids",
    )
    .execute(&mut writer)
    .await
    .unwrap();

    let mut reader = SqliteConnection::connect_with(&options).await.unwrap();
    let mut read = reader.begin().await.unwrap();
    let rows: i64 = sqlx::query_scalar("SELECT count(*) FROM payloads")
        .fetch_one(&mut *read)
        .await
        .unwrap();
    assert_eq!(rows, 64);
    sqlx::query("DELETE FROM payloads WHERE id > 2")
        .execute(&mut writer)
        .await
        .unwrap();
    let before: i64 = sqlx::query_scalar("PRAGMA freelist_count")
        .fetch_one(&mut writer)
        .await
        .unwrap();
    assert!(before > 3);

    let started = std::time::Instant::now();
    let stdout = sandbox.run_ok(&[
        "--output",
        "json",
        "sqlite-schema",
        "maintain",
        "--database",
        path.to_str().unwrap(),
        "--max-ms",
        "1000",
        "--max-pages",
        "3",
    ]);
    assert!(started.elapsed() < std::time::Duration::from_secs(2));
    let report: serde_json::Value = serde_json::from_str(&stdout).unwrap();
    assert_eq!(report["reclaimed_pages"], 3, "{report}");
    assert_eq!(report["busy"], true, "{report}");
    assert_eq!(report["deadline_reached"], false, "{report}");
    let checkpointed = report["checkpointed_frames"].as_u64().unwrap();
    assert!(checkpointed > 0 && checkpointed < report["wal_frames"].as_u64().unwrap());

    let after: i64 = sqlx::query_scalar("PRAGMA freelist_count")
        .fetch_one(&mut writer)
        .await
        .unwrap();
    assert_eq!(before - after, 3);
    let still_visible: i64 = sqlx::query_scalar("SELECT count(*) FROM payloads")
        .fetch_one(&mut *read)
        .await
        .unwrap();
    assert_eq!(still_visible, 64);
    read.commit().await.unwrap();
}

// ---------------------------------------------------------------------------
// Smoke: --help
// ---------------------------------------------------------------------------

#[test]
fn help_exits_zero_and_lists_subcommands() {
    let sb = Sandbox::new();
    let stdout = sb.run_ok(&["--help"]);
    for sub in ["context", "init", "package", "portable", "health"] {
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
    std::fs::write(&sb.contexts_file, r#"{"selected":null,"contexts":{}}"#).unwrap();
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
    assert!(stdout.contains("Initialized Orch8 project in"), "{stdout}");
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
// Local terminal execution (`orch8 dev --once`)
// ---------------------------------------------------------------------------

#[test]
fn dev_once_runs_a_yaml_sequence() {
    let sb = Sandbox::new();
    let sequence = sb.write_sequence(
        "flow.yaml",
        "name: yaml-flow\nblocks:\n  - type: step\n    id: emit\n    handler: probe\n",
    );
    let stdout = sb.run_ok(&[
        "dev",
        sequence.to_str().unwrap(),
        "--no-server",
        "--once",
        "--mock",
        r#"probe={"outcome":"from-yaml"}"#,
    ]);
    assert!(stdout.contains("from-yaml"), "{stdout}");
    assert!(stdout.contains("instance completed"), "{stdout}");

    let broken = sb.write_sequence("broken.yaml", "name: x\nblocks:\n  - id: [oops\n");
    let stderr = sb.run_err(&["dev", broken.to_str().unwrap(), "--no-server", "--once"]);
    assert!(stderr.contains("invalid YAML"), "{stderr}");
    assert!(stderr.contains("[ORCH8-V001]"), "{stderr}");
}

#[test]
fn import_n8n_writes_yaml_sequence_that_dev_accepts() {
    let sb = Sandbox::new();
    let fixture =
        Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/import/n8n-daily-report.json");
    let out = sb.root.join("report.yaml");
    let report = sb.root.join("report.json");
    let output = sb
        .cmd()
        .args([
            "import",
            "n8n",
            fixture.to_str().unwrap(),
            "--out",
            out.to_str().unwrap(),
            "--report",
            report.to_str().unwrap(),
        ])
        .output()
        .unwrap();
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(output.status.success(), "{stderr}");
    assert!(stderr.contains("TODO"), "{stderr}");
    let yaml = std::fs::read_to_string(&out).unwrap();
    assert!(yaml.contains("handler: http_request"), "{yaml}");
    let report: serde_json::Value =
        serde_json::from_str(&std::fs::read_to_string(&report).unwrap()).unwrap();
    assert_eq!(report["triggers"][0]["body"]["cron_expr"], "0 7 * * 1-5");
}

#[test]
fn learn_prints_steps_without_a_tty_and_runs_real_checks() {
    let sb = Sandbox::new();
    let list = sb.run_ok(&["learn", "--list"]);
    assert!(list.contains("Level 1"), "{list}");
    assert!(list.contains("0/"), "{list}");

    // No TTY: prints the current step only.
    let shown = sb.run_ok(&["learn"]);
    assert!(shown.contains("Create a workspace"), "{shown}");
    assert!(shown.contains("no TTY"), "{shown}");
    assert!(!sb.root.join(".orch8/learn.json").exists());

    // Step 1 writes the guide's sequence; step 2 runs it on a local dev
    // engine through the real binary; step 4 proves validation rejects it.
    sb.run_ok(&["learn", "--step", "1", "--check"]);
    let ran = sb.run_ok(&["learn", "--step", "2", "--check"]);
    assert!(ran.contains("check passed"), "{ran}");
    let rejected = sb.run_ok(&["learn", "--step", "4", "--check"]);
    assert!(rejected.contains("duplicate block id"), "{rejected}");

    let progress: serde_json::Value =
        serde_json::from_str(&std::fs::read_to_string(sb.root.join(".orch8/learn.json")).unwrap())
            .unwrap();
    assert_eq!(
        progress["completed"],
        serde_json::json!(["1.1", "1.2", "1.4"])
    );

    // A manual step has no automated check.
    let err = sb.run_err(&["learn", "--step", "3", "--check"]);
    assert!(err.contains("no automated check"), "{err}");

    sb.run_ok(&["learn", "--reset"]);
    assert!(!sb.root.join(".orch8/learn.json").exists());
}

#[test]
fn learn_publishes_to_a_local_dev_engine() {
    let sb = Sandbox::new();
    sb.run_ok(&["learn", "--step", "1", "--check"]);
    let list = sb.run_ok(&["learn", "--list"]);
    let index = |id: &str| -> String {
        list.lines()
            .find(|l| l.contains(&format!("({id})")))
            .and_then(|l| l.split('.').next())
            .and_then(|n| n.split_whitespace().last())
            .map_or_else(|| panic!("step {id} not listed:\n{list}"), str::to_string)
    };
    sb.run_ok(&["learn", "--step", &index("2.1"), "--check"]);
    let published = sb.run_ok(&["learn", "--step", &index("3.4"), "--check"]);
    assert!(
        published.contains("published to the dev engine"),
        "{published}"
    );
}

#[test]
fn dev_once_completes_sequence_and_prints_mocked_output() {
    let sb = Sandbox::new();
    let sequence = sb.write_sequence(
        "success.json",
        r#"{"name":"terminal-success","blocks":[{"type":"step","id":"emit","handler":"probe"}]}"#,
    );

    let stdout = sb.run_ok(&[
        "dev",
        sequence.to_str().unwrap(),
        "--no-server",
        "--once",
        "--mock",
        r#"probe={"outcome":"accepted"}"#,
    ]);

    assert!(stdout.contains("emit"), "step output missing:\n{stdout}");
    assert!(
        stdout.contains("accepted"),
        "mocked result missing:\n{stdout}"
    );
    assert!(stdout.contains("instance completed"), "{stdout}");
    assert!(stdout.contains("1 step(s) executed"), "{stdout}");
}

#[test]
fn dev_once_returns_failure_and_prints_handler_error() {
    let sb = Sandbox::new();
    let sequence = sb.write_sequence(
        "failure.json",
        r#"{"name":"terminal-failure","blocks":[{"type":"step","id":"boom","handler":"fail","params":{"message":"terminal boom"}}]}"#,
    );

    let output = sb
        .cmd()
        .args(["dev", sequence.to_str().unwrap(), "--no-server", "--once"])
        .output()
        .expect("spawn orch8 dev failure scenario");
    let stdout = String::from_utf8_lossy(&output.stdout);
    let stderr = String::from_utf8_lossy(&output.stderr);

    assert!(
        !output.status.success(),
        "failure scenario must exit non-zero"
    );
    assert!(stdout.contains("instance failed"), "{stdout}");
    assert!(stdout.contains("step failed: boom"), "{stdout}");
    assert!(
        stderr.contains("instance ended in state Failed"),
        "{stderr}"
    );
}

#[test]
fn dev_once_fast_forwards_delayed_sequence_with_virtual_time() {
    let sb = Sandbox::new();
    let sequence = sb.write_sequence(
        "delayed.json",
        r#"{"name":"terminal-delay","blocks":[{"type":"step","id":"wait","handler":"noop","delay":{"duration":259200000}},{"type":"step","id":"after","handler":"probe"}]}"#,
    );
    let started = std::time::Instant::now();

    let stdout = sb.run_ok(&[
        "dev",
        sequence.to_str().unwrap(),
        "--no-server",
        "--once",
        "--skip-timers",
        "--mock",
        r#"probe={"timer":"advanced"}"#,
    ]);

    assert!(started.elapsed() < std::time::Duration::from_secs(5));
    assert!(stdout.contains("wait"), "{stdout}");
    assert!(stdout.contains("after"), "{stdout}");
    assert!(stdout.contains("advanced"), "{stdout}");
    assert!(stdout.contains("instance completed"), "{stdout}");
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
    assert!(stderr.contains("content hash mismatch"), "{stderr}");
}

// ---------------------------------------------------------------------------
// Portable agent product: compiled-binary and checked-in example journeys
// ---------------------------------------------------------------------------

fn repository_root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .expect("CLI crate has repository parent")
        .to_path_buf()
}

#[test]
fn portable_protocol_policy_and_all_profiles_are_machine_readable() {
    let sb = Sandbox::new();
    let protocol = sb.run_ok(&["--output", "json", "portable", "protocol"]);
    let protocol: serde_json::Value = serde_json::from_str(&protocol).unwrap();
    assert_eq!(protocol["version"]["major"], 1);
    assert_eq!(protocol["adapters"].as_array().unwrap().len(), 4);

    let source = std::fs::read_to_string(
        repository_root().join("examples/portable-agent-product/private-rag.policy"),
    )
    .unwrap();
    let compiled = sb.run_ok(&[
        "--output",
        "json",
        "portable",
        "compile-policy",
        source.trim(),
    ]);
    let compiled: serde_json::Value = serde_json::from_str(&compiled).unwrap();
    assert_eq!(compiled["classification"], "restricted");
    assert_eq!(compiled["requirements"]["minimum_trust"], "signed");

    for profile in [
        "private-rag",
        "biometric-approval",
        "data-residency",
        "bill-of-execution",
        "audit-evidence",
        "secret-safe-coding",
        "regulated-onboarding",
        "fraud-challenge",
        "executive-airlock",
        "personal-data-vault",
    ] {
        let output = sb.run_ok(&["--output", "json", "portable", "profile", profile]);
        let contract: serde_json::Value = serde_json::from_str(&output).unwrap();
        assert_eq!(contract["requires_signed_receipt"], true, "{profile}");
        assert!(!contract["required_evidence"].as_array().unwrap().is_empty());
    }
}

#[test]
fn portable_wrap_and_score_are_real_offline_cli_workflows() {
    let sb = Sandbox::new();
    let output = sb.root.join("wrapped.json");
    let stdout = sb.run_ok(&[
        "portable",
        "wrap",
        "example-agent",
        "--adapter",
        "mcp",
        "--entrypoint",
        "https://agent.example/mcp",
        "--handler",
        "private_rag.query",
        "--policy",
        "classification=restricted;min_trust=signed;handlers=private_rag.query",
        "--allow-env",
        "PATH",
        "--secret-ref",
        "vault://model/token",
        "--manifest-output",
        output.to_str().unwrap(),
    ]);
    assert!(stdout.contains("wrote"), "{stdout}");
    let manifest: serde_json::Value =
        serde_json::from_slice(&std::fs::read(&output).unwrap()).unwrap();
    assert_eq!(manifest["adapter"], "mcp");
    assert_eq!(manifest["receipt_required"], true);
    assert_eq!(
        manifest["secret_references"],
        serde_json::json!(["vault://model/token"])
    );

    let results =
        repository_root().join("examples/portable-agent-product/conformance-results.json");
    let score = sb.run_ok(&[
        "--output",
        "json",
        "portable",
        "score",
        results.to_str().unwrap(),
    ]);
    let score: serde_json::Value = serde_json::from_str(&score).unwrap();
    assert_eq!(score["score_millipoints"], 1_000);
    assert_eq!(score["mandatory_failures"], serde_json::json!([]));

    let failed = sb.root.join("failed-results.json");
    std::fs::write(
        &failed,
        r#"[{"check":"atomic_ownership","passed":false,"evidence_sha256":"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa","duration_ms":1,"finding":"two owners"}]"#,
    )
    .unwrap();
    let stderr = sb.run_err(&["portable", "score", failed.to_str().unwrap()]);
    assert!(
        stderr.contains("mandatory continuity checks failed"),
        "{stderr}"
    );
}

#[test]
fn checked_in_portable_examples_parse_and_the_local_worker_runs() {
    use std::io::Write as _;
    use std::process::Stdio;

    let example = repository_root().join("examples/portable-agent-product");
    let manifest: orch8_types::continuity_product::GatewayManifest =
        serde_json::from_slice(&std::fs::read(example.join("local-worker.manifest.json")).unwrap())
            .unwrap();
    manifest.validate().unwrap();

    let results: Vec<orch8_types::continuity_product::ConformanceCheckResult> =
        serde_json::from_slice(&std::fs::read(example.join("conformance-results.json")).unwrap())
            .unwrap();
    assert_eq!(
        orch8_types::continuity_product::score_conformance(&results).score_millipoints,
        1_000
    );
    let _: serde_json::Value =
        serde_json::from_slice(&std::fs::read(example.join("profile-offer-request.json")).unwrap())
            .unwrap();
    let plan: orch8_types::continuity_product::CommercialContinuityPlan =
        serde_json::from_slice(&std::fs::read(example.join("commercial-plan.json")).unwrap())
            .unwrap();
    plan.validate().unwrap();

    let mut child = Command::new("/bin/sh")
        .arg(example.join("local-worker.sh"))
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .spawn()
        .unwrap();
    child
        .stdin
        .take()
        .unwrap()
        .write_all(br#"{"params":{"query":"hello"},"context":{}}"#)
        .unwrap();
    let output = child.wait_with_output().unwrap();
    assert!(output.status.success());
    let value: serde_json::Value = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(
        value,
        serde_json::json!({
            "ok": true,
            "adapter": "local_process",
            "input_received": true
        })
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn portable_worker_once_polls_the_real_api_and_exits_cleanly() {
    let server = orch8_api::test_harness::spawn_test_server().await;
    let sb = Sandbox::new();
    let manifest =
        repository_root().join("examples/portable-agent-product/local-worker.manifest.json");
    let output = sb
        .cmd()
        .args([
            "--url",
            &server.v1_url(),
            "portable",
            "worker",
            manifest.to_str().unwrap(),
            "--runtime-id",
            "0191e4f2-a1b2-7c3d-8e4f-a5b6c7d8e9f0",
            "--once",
        ])
        .output()
        .expect("spawn portable worker");
    assert!(
        output.status.success(),
        "worker should poll once and exit\nstdout:\n{}\nstderr:\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
}

#[test]
fn contract_coverage_reports_unexecuted_nested_steps() {
    let sandbox = Sandbox::new();
    std::fs::write(
        sandbox.root.join("choice.json"),
        r#"{"name":"choice","blocks":[{"type":"router","id":"choice","routes":[
        {"condition":"kind == 'a'","blocks":[{"type":"step","id":"a","handler":"work",
        "params":{"example":{"id":"not_a_block","handler":"arbitrary_data"}}}]},
        {"condition":"kind == 'b'","blocks":[{"type":"step","id":"b","handler":"work"}]}
        ]}]}"#,
    )
    .unwrap();
    std::fs::write(
        sandbox.root.join("choice.contracts.json"),
        r#"{"cases":[{"name":"takes a","input":{"kind":"a"},
        "mocks":[{"handler":"work","type":"success","output":{}}],
        "expect":{"terminal_state":"completed","path":{"traversed":["a"]}}}]}"#,
    )
    .unwrap();

    let report = sandbox.run_ok(&[
        "test",
        "run",
        "choice.contracts.json",
        "--report",
        "coverage",
    ]);
    let report: serde_json::Value = serde_json::from_str(&report).unwrap();
    assert_eq!(report["coverage"]["covered"], serde_json::json!(["a"]));
    assert_eq!(report["coverage"]["uncovered"], serde_json::json!(["b"]));
    assert_eq!(report["coverage"]["total"], 2);

    let error = sandbox.run_err(&["test", "run", "choice.contracts.json", "--require-coverage"]);
    assert!(
        error.contains("1 sequence step(s) were not covered"),
        "{error}"
    );
}

#[test]
fn verified_recipes_run_their_bundled_contracts_offline() {
    let sandbox = Sandbox::new();
    for recipe in [
        "field-inspection",
        "delivery-evidence",
        "maintenance-approval",
    ] {
        let output = sandbox.run_ok(&["templates", "verify", recipe]);
        let report: serde_json::Value = serde_json::from_str(&output).unwrap();
        assert_eq!(report["suite"]["sequence_name"], recipe);
        assert_eq!(report["suite"]["passed"], true);
        assert_eq!(report["coverage"]["uncovered"], serde_json::json!([]));
        assert!(report["required_handlers"].as_array().unwrap().len() >= 2);
    }
    let error = sandbox.run_err(&["templates", "verify", "default"]);
    assert!(error.contains("has no bundled contract"), "{error}");
}

#[test]
fn importer_generates_runnable_parity_fixture_from_source_evidence() {
    let sandbox = Sandbox::new();
    std::fs::write(
        sandbox.root.join("original.json"),
        r#"{"StartAt":"Wait","States":{"Wait":{"Type":"Wait","Seconds":1,"End":true}}}"#,
    )
    .unwrap();
    std::fs::write(
        sandbox.root.join("source-run.json"),
        r#"{"cases":[{"name":"original completed","input":{"order":"A"},
        "observed_nodes":["Wait"],"mocks":[{"handler":"noop","type":"success","output":{}}],
        "terminal_state":"completed"}]}"#,
    )
    .unwrap();
    sandbox.run_ok(&[
        "import",
        "sfn",
        "original.json",
        "--out",
        "converted.json",
        "--report",
        "report.json",
        "--parity-from",
        "source-run.json",
        "--parity-out",
        "converted.contracts.json",
    ]);
    let output = sandbox.run_ok(&[
        "test",
        "run",
        "converted.contracts.json",
        "--report",
        "json",
    ]);
    let report: serde_json::Value = serde_json::from_str(&output).unwrap();
    assert_eq!(
        report["cases"][0]["executed_blocks"],
        serde_json::json!(["wait"])
    );
    let conversion: serde_json::Value =
        serde_json::from_slice(&std::fs::read(sandbox.root.join("report.json")).unwrap()).unwrap();
    assert_eq!(conversion["translation"]["mapped"][0]["node"], "Wait");

    std::fs::write(
        sandbox.root.join("unmapped.json"),
        r#"{"cases":[{"name":"missing step","observed_nodes":["Unknown"],"terminal_state":"completed"}]}"#,
    )
    .unwrap();
    let error = sandbox.run_err(&[
        "import",
        "sfn",
        "original.json",
        "--out",
        "other.json",
        "--parity-from",
        "unmapped.json",
        "--parity-out",
        "other.contracts.json",
    ]);
    assert!(
        error.contains("cannot map observed node 'Unknown'"),
        "{error}"
    );
    assert!(!sandbox.root.join("other.json").exists());
    assert!(!sandbox.root.join("other.contracts.json").exists());
}

#[test]
fn sequence_scaffolds_a_typed_worker_handler_and_contract() {
    let sandbox = Sandbox::new();
    std::fs::write(
        sandbox.root.join("receipt.json"),
        r#"{"name":"receipt","blocks":[{"type":"step","id":"capture","handler":"scan",
        "output_schema":{"type":"object","properties":{"receipt":{"type":"string"}},"required":["receipt"]}}]}"#,
    )
    .unwrap();
    std::fs::write(sandbox.root.join("output.json"), r#"{"receipt":"r-42"}"#).unwrap();
    let command = [
        "sequence",
        "scaffold-handler",
        "receipt.json",
        "--block",
        "capture",
        "--example-output",
        "output.json",
        "--out-dir",
        "generated",
    ];
    sandbox.run_ok(&command);
    let stub = std::fs::read_to_string(sandbox.root.join("generated/handler.ts")).unwrap();
    assert!(stub.contains(r#"StepOutputs["capture"]"#));
    assert!(stub.contains("Promise<HandlerOutput>"));
    let types = std::fs::read_to_string(sandbox.root.join("generated/types.ts")).unwrap();
    assert!(types.contains(r#""receipt": string"#));
    sandbox.run_ok(&[
        "test",
        "run",
        "generated/handler.contracts.json",
        "--sequence",
        "receipt.json",
        "--require-coverage",
    ]);
    let error = sandbox.run_err(&command);
    assert!(error.contains("generated"), "{error}");
}

#[test]
fn mobile_sequence_preflight_distinguishes_published_and_development_handlers() {
    let sandbox = Sandbox::new();
    std::fs::write(
        sandbox.root.join("offline.json"),
        r#"{"name":"offline","blocks":[
        {"type":"step","id":"save","handler":"save_inspection"},
        {"type":"step","id":"shape","handler":"transform"}
        ]}"#,
    )
    .unwrap();
    let blocked = sandbox.run_err(&[
        "--output",
        "json",
        "sequence",
        "preflight-mobile",
        "offline.json",
        "--sdk",
        "published-0-7-1",
        "--handler",
        "save_inspection",
    ]);
    assert!(
        blocked.contains("mobile compatibility checks failed"),
        "{blocked}"
    );
    let report = sandbox.run_ok(&[
        "--output",
        "json",
        "sequence",
        "preflight-mobile",
        "offline.json",
        "--sdk",
        "development",
        "--handler",
        "save_inspection",
    ]);
    let report: serde_json::Value = serde_json::from_str(&report).unwrap();
    assert_eq!(report["ready"], true);
    assert_eq!(report["checks"][0]["source"], "host");
    assert_eq!(report["checks"][1]["source"], "builtin");
}

#[test]
fn failure_pack_contract_counts_failed_attempt_and_compensation_as_covered() {
    let sandbox = Sandbox::new();
    let fixture = concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/../examples/verified-sequences/compensation.contracts.json"
    );
    let output = sandbox.run_ok(&[
        "test",
        "run",
        fixture,
        "--report",
        "coverage",
        "--require-coverage",
    ]);
    let report: serde_json::Value = serde_json::from_str(&output).unwrap();
    assert_eq!(
        report["coverage"]["covered"],
        serde_json::json!(["confirm", "release", "reserve"])
    );
}

#[test]
fn handler_scaffold_supports_native_and_python_output_types() {
    let sandbox = Sandbox::new();
    std::fs::write(
        sandbox.root.join("formation.json"),
        r#"{"name":"formation","blocks":[
        {"type":"step","id":"leaf","handler":"scan","output_schema":{"type":"string"}}
    ]}"#,
    )
    .unwrap();
    std::fs::write(sandbox.root.join("result.json"), r#""completed""#).unwrap();
    for (language, stub, types) in [
        ("python", "handler.py", "handler_types.py"),
        ("swift", "Handler.swift", "HandlerTypes.swift"),
        ("kotlin", "Handler.kt", "HandlerTypes.kt"),
    ] {
        sandbox.run_ok(&[
            "sequence",
            "scaffold-handler",
            "formation.json",
            "--block",
            "leaf",
            "--example-output",
            "result.json",
            "--out-dir",
            language,
            "--language",
            language,
        ]);
        let directory = sandbox.root.join(language);
        assert!(
            std::fs::read_to_string(directory.join(stub))
                .unwrap()
                .contains("HandlerOutput")
        );
        assert!(
            std::fs::read_to_string(directory.join(types))
                .unwrap()
                .contains("SequenceInput")
        );
        assert!(directory.join("handler.contracts.json").is_file());
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[allow(clippy::too_many_lines)] // Exercise both integrity failures in one database.
async fn verify_externalized_integrity_reports_missing_and_wrong_owner_without_exposing_data() {
    use orch8_storage::encrypting::EncryptingStorage;
    use orch8_storage::externalizing::ref_key;
    use orch8_storage::sqlite::SqliteStorage;
    use orch8_storage::testing::fixtures::{InstanceBuilder, SequenceBuilder};
    use orch8_storage::{
        InstanceStore as _, OutputStore as _, ResourceStore as _, SequenceStore as _,
        StorageBackend,
    };
    use orch8_types::encryption::FieldEncryptor;
    use orch8_types::ids::BlockId;
    use orch8_types::output::BlockOutput;
    use serde_json::{Value, json};
    use std::sync::Arc;

    let sandbox = Sandbox::new();
    let path = sandbox.root.join("integrity.db");
    let raw = Arc::new(
        SqliteStorage::file_path(path.to_str().unwrap())
            .await
            .unwrap(),
    );
    let sequence = SequenceBuilder::new("demo").step("one", "noop").build();
    raw.create_sequence(&sequence).await.unwrap();
    let key = "00112233445566778899aabbccddeeff00112233445566778899aabbccddeeff";
    let backend: Arc<dyn StorageBackend> = raw.clone();
    let sealed = EncryptingStorage::new(backend, FieldEncryptor::from_hex_key(key).unwrap());
    let owner = InstanceBuilder::new("demo", "default", sequence.id)
        .data(json!({"first":"missing payload secret", "second":"wrong owner payload secret"}))
        .build();
    let other = InstanceBuilder::new("demo", "default", sequence.id).build();
    sealed
        .create_instance_externalized(&owner, 1)
        .await
        .unwrap();
    sealed.create_instance(&other).await.unwrap();
    let output_ref = format!("{}:output:one:0", owner.id);
    sealed
        .save_externalized_state(owner.id, &output_ref, &json!("historical payload secret"))
        .await
        .unwrap();
    sealed
        .save_block_output(&BlockOutput {
            id: uuid::Uuid::now_v7(),
            instance_id: owner.id,
            block_id: BlockId::new("one"),
            output: Value::Null,
            output_ref: Some(output_ref),
            output_size: 25,
            attempt: 0,
            created_at: chrono::Utc::now(),
        })
        .await
        .unwrap();
    let command = || {
        let mut command = sandbox.cmd();
        command
            .env("ORCH8_ENCRYPTION_KEY", key)
            .args(["--output", "json", "verify-externalized", "--database-url"])
            .arg(&path);
        command
    };

    let healthy = command().output().expect("healthy integrity scan");
    assert!(healthy.status.success(), "{healthy:?}");
    let healthy_json: Value = serde_json::from_slice(&healthy.stdout).unwrap();
    assert_eq!(healthy_json["issue_count"], 0);
    assert_eq!(healthy_json["references"], 3);
    let stored = sealed.get_instance(owner.id).await.unwrap().unwrap();
    let first_ref = stored.context.data["first"]["_ref"]
        .as_str()
        .unwrap()
        .to_owned();
    let second_ref = stored.context.data["second"]["_ref"]
        .as_str()
        .unwrap()
        .to_owned();
    sqlx::query("DELETE FROM externalized_state WHERE ref_key = ?")
        .bind(&first_ref)
        .execute(raw.pool())
        .await
        .unwrap();
    sqlx::query("UPDATE externalized_state SET instance_id = ? WHERE ref_key = ?")
        .bind(other.id.to_string())
        .bind(&second_ref)
        .execute(raw.pool())
        .await
        .unwrap();

    let damaged = command().output().expect("damaged integrity scan");
    assert!(!damaged.status.success());
    let stdout = String::from_utf8(damaged.stdout).unwrap();
    let stderr = String::from_utf8(damaged.stderr).unwrap();
    assert!(!stdout.contains("payload secret") && !stderr.contains("payload secret"));
    assert!(!stdout.contains(&first_ref) && !stdout.contains(&second_ref));
    let report: Value = serde_json::from_str(&stdout).unwrap();
    assert_eq!(report["issue_count"], 2);
    let issues = report["issues"].as_array().unwrap();
    for expected in ["missing", "wrong_owner"] {
        assert!(
            issues.iter().any(|issue| {
                issue["kind"] == expected && issue["instance_id"] == owner.id.to_string()
            }),
            "{issues:?}"
        );
    }
    let stored = sealed.get_instance(owner.id).await.unwrap().unwrap();
    assert_eq!(
        ref_key(&stored.context.data["first"], false),
        Some(first_ref.as_str())
    );
    assert_eq!(
        ref_key(&stored.context.data["second"], false),
        Some(second_ref.as_str())
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn signed_backup_integrity_cli_uses_an_out_of_band_trusted_key() {
    use base64::{Engine as _, engine::general_purpose::STANDARD as BASE64};
    use ed25519_dalek::SigningKey;
    use orch8_storage::SequenceStore as _;
    use orch8_storage::sqlite::SqliteStorage;
    use orch8_storage::testing::fixtures::SequenceBuilder;

    let sandbox = Sandbox::new();
    let source = sandbox.root.join("source.db");
    let store = SqliteStorage::file_path(source.to_str().unwrap())
        .await
        .unwrap();
    let sequence = SequenceBuilder::new("demo").step("one", "noop").build();
    store.create_sequence(&sequence).await.unwrap();
    store.close().await;
    let signing = SigningKey::from_bytes(&[3; 32]);
    std::fs::write(
        sandbox.root.join("key.seed"),
        BASE64.encode(signing.to_bytes()),
    )
    .unwrap();
    std::fs::write(
        sandbox.root.join("key.pub"),
        BASE64.encode(signing.verifying_key().to_bytes()),
    )
    .unwrap();
    let wrong = SigningKey::from_bytes(&[4; 32]).verifying_key();
    std::fs::write(
        sandbox.root.join("wrong.pub"),
        BASE64.encode(wrong.to_bytes()),
    )
    .unwrap();

    sandbox.run_ok(&[
        "backup",
        "--database-url",
        "source.db",
        "--out",
        "archive.tar.gz",
        "--signing-key-file",
        "key.seed",
    ]);
    let error = sandbox.run_err(&[
        "--yes",
        "restore",
        "archive.tar.gz",
        "--database-url",
        "target.db",
        "--trusted-key-file",
        "wrong.pub",
    ]);
    assert!(error.contains("signature verification failed"), "{error}");
    assert!(!sandbox.root.join("target.db").exists());
    sandbox.run_ok(&[
        "--yes",
        "restore",
        "archive.tar.gz",
        "--database-url",
        "target.db",
        "--trusted-key-file",
        "key.pub",
    ]);
    let target = sandbox.root.join("target.db");
    let restored = SqliteStorage::file_path(target.to_str().unwrap())
        .await
        .unwrap();
    assert_eq!(
        restored
            .get_sequence(sequence.id)
            .await
            .unwrap()
            .unwrap()
            .name,
        sequence.name
    );
}

#[tokio::test]
#[allow(clippy::too_many_lines)]
async fn recipient_encrypted_backup_round_trips_only_with_an_authorized_key() {
    use orch8_storage::AdminStore as _;
    use orch8_storage::sqlite::SqliteStorage;
    use orch8_types::credential::{CredentialDef, CredentialKind};

    const SECRET: &str = "test-only backup credential containing punctuation!F~2";
    let sandbox = Sandbox::new();
    let age_available = ["age", "age-keygen"].into_iter().all(|tool| {
        sandbox
            .command(tool)
            .arg("--version")
            .output()
            .is_ok_and(|result| result.status.success())
    });
    if !age_available {
        assert!(
            std::env::var_os("ORCH8_REQUIRE_AGE").is_none(),
            "age and age-keygen are required in CI"
        );
        eprintln!("skipping recipient backup fixture because age is unavailable");
        return;
    }
    let recipient = |file: &str| {
        assert!(
            sandbox
                .command("age-keygen")
                .args(["-o", file])
                .output()
                .unwrap()
                .status
                .success()
        );
        let output = sandbox
            .command("age-keygen")
            .args(["-y", file])
            .output()
            .unwrap();
        assert!(output.status.success());
        String::from_utf8(output.stdout).unwrap().trim().to_owned()
    };
    let first = recipient("first.agekey");
    let second = recipient("second.agekey");
    let _other = recipient("wrong.agekey");
    let source = sandbox.root.join("source.db");
    let store = SqliteStorage::file_path(source.to_str().unwrap())
        .await
        .unwrap();
    let now = chrono::Utc::now();
    store
        .create_credential(&CredentialDef {
            id: "backup-test".into(),
            tenant_id: "demo".into(),
            name: "backup test".into(),
            kind: CredentialKind::default(),
            value: SECRET.to_owned().into(),
            expires_at: None,
            refresh_url: None,
            refresh_token: None,
            enabled: true,
            description: None,
            created_at: now,
            updated_at: now,
        })
        .await
        .unwrap();
    store.close().await;

    sandbox.run_ok(&[
        "backup",
        "--database-url",
        "source.db",
        "--out",
        "encrypted.age",
        "--encrypt-recipient",
        &first,
        "--encrypt-recipient",
        &second,
    ]);
    let encrypted = std::fs::read(sandbox.root.join("encrypted.age")).unwrap();
    assert!(encrypted.starts_with(b"age-encryption.org/v1\n"));
    assert!(
        !encrypted
            .windows(SECRET.len())
            .any(|bytes| bytes == SECRET.as_bytes())
    );
    let restore = |archive, identity, database| {
        [
            "--yes",
            "restore",
            archive,
            "--identity",
            identity,
            "--database-url",
            database,
        ]
    };
    let error = sandbox.run_err(&restore("encrypted.age", "wrong.agekey", "wrong.db"));
    assert!(error.contains("could not decrypt or verify"), "{error}");
    assert!(!sandbox.root.join("wrong.db").exists());
    std::fs::write(
        sandbox.root.join("corrupt.age"),
        &encrypted[..encrypted.len() - 1],
    )
    .unwrap();
    let error = sandbox.run_err(&restore("corrupt.age", "first.agekey", "corrupt.db"));
    assert!(error.contains("age backup"), "{error}");
    assert!(!sandbox.root.join("corrupt.db").exists());
    sandbox.run_ok(&restore("encrypted.age", "first.agekey", "first.db"));
    sandbox.run_ok(&restore("encrypted.age", "second.agekey", "second.db"));
    for name in ["first.db", "second.db"] {
        let path = sandbox.root.join(name);
        let restored = SqliteStorage::file_path(path.to_str().unwrap())
            .await
            .unwrap();
        let credential = restored
            .get_credential(None, "backup-test")
            .await
            .unwrap()
            .unwrap();
        assert_eq!(credential.value.expose(), SECRET);
        restored.close().await;
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn streamed_restore_integrity_rejects_a_damaged_trailer_before_opening_target() {
    use orch8_storage::SequenceStore as _;
    use orch8_storage::sqlite::SqliteStorage;
    use orch8_storage::testing::fixtures::SequenceBuilder;

    let sandbox = Sandbox::new();
    let source = sandbox.root.join("source.db");
    let store = SqliteStorage::file_path(source.to_str().unwrap())
        .await
        .unwrap();
    store
        .create_sequence(&SequenceBuilder::new("demo").step("one", "noop").build())
        .await
        .unwrap();
    store.close().await;
    sandbox.run_ok(&[
        "backup",
        "--database-url",
        "source.db",
        "--out",
        "archive.tar.gz",
    ]);

    let archive = sandbox.root.join("archive.tar.gz");
    let mut bytes = std::fs::read(&archive).unwrap();
    // Gzip stores the uncompressed CRC in the first half of the trailer.
    let crc_start = bytes.len() - 8;
    bytes[crc_start] ^= 1;
    std::fs::write(archive, bytes).unwrap();
    let error = sandbox.run_err(&[
        "--yes",
        "restore",
        "archive.tar.gz",
        "--database-url",
        "target.db",
    ]);
    assert!(
        error.contains("gzip trailer") || error.contains("corrupt archive"),
        "{error}"
    );
    assert!(!sandbox.root.join("target.db").exists());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[allow(clippy::too_many_lines)]
async fn streamed_backup_integrity_archives_many_large_outputs() {
    use orch8_storage::sqlite::SqliteStorage;
    use orch8_storage::testing::fixtures::{InstanceBuilder, SequenceBuilder};
    use orch8_storage::{
        ExecutionTreeStore as _, InstanceStore as _, OutputStore as _, SequenceStore as _,
    };
    use orch8_types::execution::{BlockType, ExecutionNode, NodeState};
    use orch8_types::ids::{BlockId, ExecutionNodeId};
    use orch8_types::output::BlockOutput;
    use std::io::{BufRead as _, BufReader};

    #[cfg(target_os = "linux")]
    const OUTPUTS: u32 = 160;
    #[cfg(not(target_os = "linux"))]
    const OUTPUTS: u32 = 48;
    const SIZE: u32 = 2 * 1024 * 1024;
    let sandbox = Sandbox::new();
    let source = sandbox.root.join("source.db");
    let storage = SqliteStorage::file_path(source.to_str().unwrap())
        .await
        .unwrap();
    let sequence = SequenceBuilder::new("demo").step("large", "noop").build();
    storage.create_sequence(&sequence).await.unwrap();
    let instance = InstanceBuilder::for_sequence(&sequence).build();
    storage.create_instance(&instance).await.unwrap();
    let nodes: Vec<_> = (0..501)
        .map(|index| ExecutionNode {
            id: ExecutionNodeId::new(),
            instance_id: instance.id,
            block_id: BlockId::new(format!("node-{index}")),
            parent_id: None,
            block_type: BlockType::Step,
            branch_index: None,
            state: NodeState::Pending,
            started_at: None,
            completed_at: None,
        })
        .collect();
    for batch in nodes.chunks(100) {
        storage.create_execution_nodes_batch(batch).await.unwrap();
    }
    let payload = "z".repeat(SIZE as usize);
    for index in 0..OUTPUTS {
        storage
            .save_block_output(&BlockOutput {
                id: uuid::Uuid::now_v7(),
                instance_id: instance.id,
                block_id: BlockId::new("large"),
                output: serde_json::json!({"index": index, "data": payload}),
                output_ref: None,
                output_size: SIZE,
                attempt: 0,
                created_at: chrono::Utc::now(),
            })
            .await
            .unwrap();
    }
    storage.close().await;

    let arguments = [
        "--output",
        "json",
        "backup",
        "--database-url",
        "source.db",
        "--out",
        "archive.tar.gz",
        "--include-instances",
    ];
    #[cfg(target_os = "linux")]
    let stdout = {
        // The archive contains over 320 MiB of output records. The child
        // process is limited to 256 MiB of virtual address space.
        let child = sandbox
            .command("sh")
            .args([
                "-c",
                "ulimit -v 262144 || exit 97; exec \"$@\"",
                "memory-limited-backup",
                env!("CARGO_BIN_EXE_orch8"),
            ])
            .env("TOKIO_WORKER_THREADS", "2")
            // glibc can reserve a large address range for each thread arena.
            // Keep the limit focused on backup's memory use.
            .env("MALLOC_ARENA_MAX", "1")
            .args(arguments)
            .output()
            .unwrap();
        assert!(child.status.success(), "{child:?}");
        String::from_utf8(child.stdout).unwrap()
    };
    #[cfg(not(target_os = "linux"))]
    let stdout = sandbox.run_ok(&arguments);
    let manifest: serde_json::Value = serde_json::from_str(&stdout).unwrap();
    assert!(
        manifest["files"]
            .as_array()
            .unwrap()
            .iter()
            .any(|member| { member["name"] == "execution_tree.jsonl" && member["records"] == 501 })
    );
    let outputs = manifest["files"]
        .as_array()
        .unwrap()
        .iter()
        .find(|member| member["name"] == "block_outputs.jsonl")
        .unwrap();
    assert_eq!(outputs["records"], OUTPUTS);
    assert!(outputs["bytes"].as_u64().unwrap() > u64::from(SIZE) * u64::from(OUTPUTS));

    let file = std::fs::File::open(sandbox.root.join("archive.tar.gz")).unwrap();
    let mut archive = tar::Archive::new(flate2::read::GzDecoder::new(file));
    let mut found = false;
    for entry in archive.entries().unwrap() {
        let entry = entry.unwrap();
        if entry.path().unwrap().as_ref() != Path::new("block_outputs.jsonl") {
            continue;
        }
        found = true;
        let mut count = 0;
        for line in BufReader::new(entry).split(b'\n') {
            let row: BlockOutput = serde_json::from_slice(&line.unwrap()).unwrap();
            assert_eq!(row.instance_id, instance.id);
            assert_eq!(row.output["index"], count);
            assert_eq!(row.output["data"].as_str().unwrap().len(), SIZE as usize);
            count += 1;
        }
        assert_eq!(count, OUTPUTS);
    }
    assert!(found);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn checkpoint_backup_integrity_preserves_latest_and_historical_selection() {
    use orch8_storage::encrypting::EncryptingStorage;
    use orch8_storage::sqlite::SqliteStorage;
    use orch8_storage::testing::fixtures::{InstanceBuilder, SequenceBuilder};
    use orch8_storage::{InstanceStore as _, ResourceStore as _, SequenceStore as _};
    use orch8_types::checkpoint::Checkpoint;
    use orch8_types::encryption::FieldEncryptor;
    use serde_json::json;
    use std::sync::Arc;
    use uuid::Uuid;

    let sandbox = Sandbox::new();
    let store = Arc::new(
        SqliteStorage::file_path(sandbox.root.join("source.db").to_str().unwrap())
            .await
            .unwrap(),
    );
    let key = "00112233445566778899aabbccddeeff00112233445566778899aabbccddeeff";
    let source = EncryptingStorage::new(store.clone(), FieldEncryptor::from_hex_key(key).unwrap());
    let sequence = SequenceBuilder::new("demo")
        .version(7)
        .step("work", "noop")
        .build();
    source.create_sequence(&sequence).await.unwrap();
    let instance = InstanceBuilder::for_sequence(&sequence).build();
    source.create_instance(&instance).await.unwrap();
    let now = chrono::Utc::now();
    for number in 1..=2 {
        source
            .save_checkpoint(&Checkpoint {
                id: Uuid::from_u128(number),
                instance_id: instance.id,
                checkpoint_data: json!({"checkpoint_seq": number, "sequence_version": sequence.version, "owner_epoch": 4}),
                created_at: now,
            })
            .await
            .unwrap();
    }
    let original_ciphertext = store
        .get_latest_checkpoint(instance.id)
        .await
        .unwrap()
        .unwrap()
        .checkpoint_data;
    store.close().await;
    sandbox.run_ok(&[
        "backup",
        "--database-url",
        "source.db",
        "--out",
        "with-checkpoints.tar.gz",
        "--include-instances",
    ]);
    let restore = || {
        sandbox.run_ok(&[
            "--yes",
            "--output",
            "json",
            "restore",
            "with-checkpoints.tar.gz",
            "--database-url",
            "target.db",
            "--include-instances",
        ])
    };
    let report: serde_json::Value = serde_json::from_str(&restore()).unwrap();
    assert_eq!(report["counts"]["checkpoints"]["created"], 2);
    let repeated: serde_json::Value = serde_json::from_str(&restore()).unwrap();
    assert_eq!(repeated["counts"]["checkpoints"]["existing"], 2);

    let store = Arc::new(
        SqliteStorage::file_path(sandbox.root.join("target.db").to_str().unwrap())
            .await
            .unwrap(),
    );
    let raw = store
        .get_latest_checkpoint(instance.id)
        .await
        .unwrap()
        .unwrap();
    assert!(FieldEncryptor::is_encrypted(&raw.checkpoint_data));
    assert_eq!(raw.checkpoint_data, original_ciphertext);
    let target = EncryptingStorage::new(store, FieldEncryptor::from_hex_key(key).unwrap());
    let latest = target
        .get_latest_checkpoint(instance.id)
        .await
        .unwrap()
        .unwrap();
    let history = target.list_checkpoints(instance.id, 100).await.unwrap();
    assert_eq!(latest.id, Uuid::from_u128(2));
    assert_eq!(history.len(), 2);
    assert_eq!(history[0].id, latest.id);
    assert_eq!(history[1].id, Uuid::from_u128(1));
    assert_eq!(history[1].instance_id, instance.id);
    assert_eq!(history[1].created_at, now);
    assert_eq!(
        history[1].checkpoint_data,
        json!({"checkpoint_seq": 1, "sequence_version": 7, "owner_epoch": 4})
    );
}

#[tokio::test]
#[allow(clippy::too_many_lines)]
async fn signal_inbox_backup_preserves_delivery_order_and_deduplication() {
    use chrono::{Duration, Utc};
    use orch8_storage::sqlite::SqliteStorage;
    use orch8_storage::testing::fixtures::{InstanceBuilder, SequenceBuilder};
    use orch8_storage::{InstanceStore as _, SequenceStore as _, SignalStore as _};
    use orch8_types::signal::{Signal, SignalType};
    use serde_json::{Value, json};
    use uuid::Uuid;

    let sandbox = Sandbox::new();
    let source = SqliteStorage::file_path(sandbox.root.join("source.db").to_str().unwrap())
        .await
        .unwrap();
    let sequence = SequenceBuilder::new("signals").step("work", "noop").build();
    source.create_sequence(&sequence).await.unwrap();
    let now = Utc::now();
    let mut instance = InstanceBuilder::for_sequence(&sequence).build();
    instance.next_fire_at = Some(now);
    source.create_instance(&instance).await.unwrap();
    // Insert out of delivery order and assign two signals the same timestamp.
    let signals = [4, 3, 1, 2].map(|id| Signal {
        id: Uuid::from_u128(id),
        instance_id: instance.id,
        signal_type: SignalType::Custom("wake".into()),
        payload: json!({"value": id}),
        delivered: id == 1,
        created_at: if id == 4 {
            now
        } else {
            now - Duration::seconds(2)
        },
        delivered_at: (id == 1).then_some(now - Duration::seconds(1)),
    });
    for signal in &signals {
        source.enqueue_signal(signal).await.unwrap();
    }
    source.close().await;

    sandbox.run_ok(&[
        "backup",
        "--database-url",
        "source.db",
        "--out",
        "inbox.tar.gz",
        "--include-instances",
    ]);
    let restore = || {
        let result = sandbox.run_ok(&[
            "--yes",
            "--output",
            "json",
            "restore",
            "inbox.tar.gz",
            "--database-url",
            "target.db",
            "--include-instances",
        ]);
        serde_json::from_str::<Value>(&result).unwrap()
    };
    assert_eq!(restore()["counts"]["signals"]["created"], 4);

    let target = SqliteStorage::file_path(sandbox.root.join("target.db").to_str().unwrap())
        .await
        .unwrap();
    let pending = target.get_pending_signals(instance.id).await.unwrap();
    assert_eq!(
        pending
            .iter()
            .map(|signal| signal.id.as_u128())
            .collect::<Vec<_>>(),
        [2, 3, 4]
    );
    let already_consumed = &signals[2];
    let history = target
        .list_signal_history_page(instance.id, None, 4)
        .await
        .unwrap();
    assert!(history[0].delivered);
    assert_eq!(history[0].delivered_at, already_consumed.delivered_at);
    assert!(target.enqueue_signal(already_consumed).await.is_err());

    let claimed = target.claim_due_instances(Utc::now(), 1, 0).await.unwrap();
    assert_eq!(claimed[0].id, instance.id);
    for signal in pending {
        assert!(
            target
                .mark_signal_delivered_claimed(instance.id, signal.id, claimed[0].updated_at)
                .await
                .unwrap()
        );
    }
    assert!(
        target
            .get_pending_signals(instance.id)
            .await
            .unwrap()
            .is_empty()
    );
    assert_eq!(restore()["counts"]["signals"]["existing"], 4);
    assert!(
        target
            .get_pending_signals(instance.id)
            .await
            .unwrap()
            .is_empty()
    );
    let history = target
        .list_signal_history_page(instance.id, None, 4)
        .await
        .unwrap();
    assert_eq!(history.len(), 4);
    assert!(
        history
            .iter()
            .all(|signal| signal.delivered && signal.delivered_at.is_some())
    );
    assert_eq!(history[0].delivered_at, already_consumed.delivered_at);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[allow(clippy::too_many_lines)]
async fn instance_kv_backup_resumes_waiting_workflow_with_original_state() {
    use chrono::{Duration, Utc};
    use orch8::{Clock, CreateInstanceOptions, Engine, ManualClock, SharedClock, Storage};
    use orch8_storage::ResourceStore as _;
    use orch8_storage::sqlite::SqliteStorage;
    use orch8_types::instance::InstanceState;
    use orch8_types::sequence::SequenceDefinition;
    use serde_json::{Value, json};
    use std::sync::Arc;

    let sandbox = Sandbox::new();
    let source_db = sandbox.root.join("source.db");
    let target_db = sandbox.root.join("target.db");
    let clock = Arc::new(ManualClock::new(Utc::now()));
    let start_engine = |path| {
        Engine::builder()
            .storage(Storage::sqlite(path))
            .clock(SharedClock::from_arc(clock.clone()))
            .build()
    };

    let source = start_engine(&source_db).await.unwrap();
    let mut raw: Value = serde_json::from_str(VALID_SEQUENCE_JSON).unwrap();
    raw["blocks"] = json!([
        {"type":"step","id":"set","handler":"set_state","params":{"key":"token","value":{"from":"source"}}},
        {"type":"step","id":"read","handler":"get_state","params":{"key":"token"},"delay":{"duration":60000}},
        {"type":"step","id":"expired","handler":"get_state","params":{"key":"gone"}},
        {"type":"step","id":"live","handler":"get_state","params":{"key":"recent"}}
    ]);
    let sequence: SequenceDefinition = serde_json::from_value(raw).unwrap();
    let sequence = source.upsert_sequence(sequence).await.unwrap();
    let id = source
        .create_instance(sequence, CreateInstanceOptions::default())
        .await
        .unwrap();
    for _ in 0..8 {
        source.tick_once().await.unwrap();
        if source
            .get_instance(id)
            .await
            .unwrap()
            .next_fire_at
            .is_some_and(|wake| wake > clock.now())
        {
            break;
        }
    }
    let waiting = source.get_instance(id).await.unwrap();
    assert!(waiting.next_fire_at.is_some_and(|wake| wake > clock.now()));
    assert!(!waiting.state.is_terminal());
    assert!(
        source
            .block_outputs(id)
            .await
            .unwrap()
            .iter()
            .all(|output| output.block_id.as_str() != "read")
    );
    source.close().await;

    let fixture = SqliteStorage::file_path(source_db.to_str().unwrap())
        .await
        .unwrap();
    assert_eq!(
        fixture.get_instance_kv(id, "token").await.unwrap(),
        Some(json!({"from":"source"}))
    );
    fixture
        .set_instance_kv_expiring(
            id,
            "gone",
            &json!("already expired"),
            clock.now() - Duration::minutes(1),
        )
        .await
        .unwrap();
    fixture
        .set_instance_kv_expiring(id, "recent", &json!(42), clock.now() + Duration::days(1))
        .await
        .unwrap();
    fixture.close().await;

    sandbox.run_ok(&[
        "backup",
        "--database-url",
        "source.db",
        "--out",
        "kv.tar.gz",
        "--include-instances",
    ]);
    let report = sandbox.run_ok(&[
        "--yes",
        "--output",
        "json",
        "restore",
        "kv.tar.gz",
        "--database-url",
        "target.db",
        "--include-instances",
    ]);
    let report: Value = serde_json::from_str(&report).unwrap();
    assert_eq!(report["counts"]["instance_kv"]["created"], 3);

    let target = start_engine(&target_db).await.unwrap();
    clock.advance(Duration::minutes(2));
    for _ in 0..12 {
        target.tick_once().await.unwrap();
        if target.get_instance(id).await.unwrap().state.is_terminal() {
            break;
        }
    }
    assert_eq!(
        target.get_instance(id).await.unwrap().state,
        InstanceState::Completed
    );
    let outputs = target.block_outputs(id).await.unwrap();
    for (block, value) in [
        ("read", json!({"from":"source"})),
        ("expired", Value::Null),
        ("live", json!(42)),
    ] {
        let output = outputs
            .iter()
            .find(|output| output.block_id.as_str() == block)
            .unwrap();
        assert_eq!(output.output["value"], value);
    }
    target.close().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[allow(clippy::too_many_lines)]
async fn worker_task_backup_resumes_progress_under_fresh_lease_authority() {
    use orch8::{CreateInstanceOptions, Engine, Storage};
    use orch8_storage::{InstanceStore, WorkerStore, sqlite::SqliteStorage};
    use orch8_types::{
        instance::InstanceState,
        sequence::SequenceDefinition,
        worker::{WorkerClaim, WorkerTaskState},
    };
    use serde_json::{Value, json};

    let sandbox = Sandbox::new();
    let source_db = sandbox.root.join("source.db");
    let target_db = sandbox.root.join("target.db");
    let source = Engine::builder()
        .storage(Storage::sqlite(&source_db))
        .build()
        .await
        .unwrap();
    let source_storage = SqliteStorage::file_path(source_db.to_str().unwrap())
        .await
        .unwrap();
    let mut definition: Value = serde_json::from_str(VALID_SEQUENCE_JSON).unwrap();
    let blocks = definition["blocks"].take();
    definition["blocks"] = json!([{"type":"parallel","id":"fanout","branches":[blocks]}]);
    let sequence: SequenceDefinition = serde_json::from_value(definition).unwrap();
    let sequence = source.upsert_sequence(sequence).await.unwrap();
    let running = source
        .create_instance(sequence, CreateInstanceOptions::default())
        .await
        .unwrap();
    source.tick_once().await.unwrap();
    let original = source_storage
        .claim_worker_tasks("charge_card", "same-worker", 1)
        .await
        .unwrap()
        .pop()
        .unwrap();
    assert_eq!(original.instance_id, running);
    assert!(original.effect_id.is_some());
    let old_claim = WorkerClaim::new("same-worker", original.claim_epoch);
    let queued = source
        .create_instance(sequence, CreateInstanceOptions::default())
        .await
        .unwrap();
    source.tick_once().await.unwrap();
    assert_eq!(
        source.get_instance(queued).await.unwrap().state,
        InstanceState::Waiting
    );
    source.close().await;

    let backup = [
        "backup",
        "--database-url",
        "source.db",
        "--out",
        "workers.tar.gz",
        "--include-instances",
    ];
    let error = sandbox.run_err(&backup);
    assert!(error.contains("no resumable checkpoint"), "{error}");
    assert!(!sandbox.root.join("workers.tar.gz").exists());
    let checkpoint = json!({"uploaded_parts": [1, 2], "cursor": "part-3"});
    assert_eq!(
        source_storage
            .checkpoint_worker_task(original.id, &old_claim, 0, &checkpoint)
            .await
            .unwrap(),
        Some(1)
    );
    let original_binding = source_storage
        .get_worker_invocation(
            &source_storage
                .get_instance(running)
                .await
                .unwrap()
                .unwrap()
                .tenant_id,
            original.id,
        )
        .await
        .unwrap()
        .unwrap();
    sandbox.run_ok(&backup);
    assert_eq!(
        source_storage
            .get_worker_task(original.id)
            .await
            .unwrap()
            .unwrap()
            .state,
        WorkerTaskState::Claimed
    );
    source_storage.close().await;

    let restore = || {
        serde_json::from_str::<Value>(&sandbox.run_ok(&[
            "--yes",
            "--output",
            "json",
            "restore",
            "workers.tar.gz",
            "--database-url",
            "target.db",
            "--include-instances",
        ]))
        .unwrap()
    };
    assert_eq!(restore()["counts"]["worker_tasks"]["created"], 2);
    assert_eq!(restore()["counts"]["worker_tasks"]["existing"], 2);

    let target_storage = SqliteStorage::file_path(target_db.to_str().unwrap())
        .await
        .unwrap();
    let tasks = target_storage
        .claim_worker_tasks("charge_card", "same-worker", 2)
        .await
        .unwrap();
    assert_eq!(tasks.len(), 2);
    let resumed = tasks
        .iter()
        .find(|task| task.instance_id == running)
        .unwrap();
    let pending = tasks
        .iter()
        .find(|task| task.instance_id == queued)
        .unwrap();
    assert_ne!(resumed.id, original.id);
    assert!(resumed.claim_epoch > old_claim.claim_epoch);
    assert_eq!(resumed.attempt, original.attempt);
    assert_eq!(resumed.effect_id, original.effect_id);
    assert_eq!(resumed.resume_checkpoint.as_ref(), Some(&checkpoint));
    assert_eq!(resumed.checkpoint_seq, 1);
    assert_eq!(pending.checkpoint_seq, 0);
    let binding = target_storage
        .get_worker_invocation(&original_binding.tenant_id, resumed.id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(binding.target, original_binding.target);
    assert_eq!(binding.run_id, original_binding.run_id);
    assert!(
        target_storage
            .get_worker_dispatch_admission(&binding.tenant_id, resumed.id)
            .await
            .unwrap()
            .is_some()
    );

    let output = json!({"resumed": true});
    let api = orch8_api::test_harness::spawn_test_server_on(
        std::sync::Arc::new(
            SqliteStorage::file_path(target_db.to_str().unwrap())
                .await
                .unwrap(),
        ),
        None,
    )
    .await;
    let rejected = reqwest::Client::new()
        .post(format!("{}/workers/tasks/{}/complete", api.v1_url(), original.id))
        .json(&json!({"worker_id": old_claim.worker_id, "claim_epoch": old_claim.claim_epoch, "output": output}))
        .send().await.unwrap();
    assert_eq!(rejected.status(), reqwest::StatusCode::NOT_FOUND);
    drop(api);
    assert!(
        !target_storage
            .complete_worker_task(original.id, &old_claim, &output)
            .await
            .unwrap()
    );
    assert!(
        !target_storage
            .complete_worker_task(resumed.id, &old_claim, &output)
            .await
            .unwrap()
    );
    let fresh = WorkerClaim::new("same-worker", resumed.claim_epoch);
    assert_eq!(
        target_storage
            .checkpoint_worker_task(resumed.id, &fresh, 1, &json!({"uploaded_parts": [1, 2, 3]}))
            .await
            .unwrap(),
        Some(2)
    );
    assert!(
        target_storage
            .complete_worker_task(resumed.id, &fresh, &output)
            .await
            .unwrap()
    );
    target_storage.close().await;

    let target = Engine::builder()
        .storage(Storage::sqlite(&target_db))
        .build()
        .await
        .unwrap();
    for _ in 0..8 {
        target.tick_once().await.unwrap();
        if target
            .get_instance(running)
            .await
            .unwrap()
            .state
            .is_terminal()
        {
            break;
        }
    }
    assert_eq!(
        target.get_instance(running).await.unwrap().state,
        InstanceState::Completed
    );
    let results = target.block_outputs(running).await.unwrap();
    let saved = results
        .iter()
        .find(|result| result.block_id.as_str() == "charge")
        .unwrap();
    assert_eq!(saved.output, output);
    target.close().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[allow(clippy::too_many_lines)]
async fn effect_backup_restores_committed_compensated_and_unresolved_receipts() {
    use chrono::{Duration, Utc};
    use orch8::{Clock, CreateInstanceOptions, Engine, ManualClock, SharedClock, Storage};
    use orch8_storage::ContinuityStore as _;
    use orch8_storage::sqlite::SqliteStorage;
    use orch8_types::continuity::{EffectId, EffectReceipt, EffectState};
    use orch8_types::ids::BlockId;
    use orch8_types::instance::InstanceState;
    use orch8_types::sequence::SequenceDefinition;
    use serde_json::{Value, json};
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};

    let sandbox = Sandbox::new();
    let source_db = sandbox.root.join("source.db");
    let target_db = sandbox.root.join("target.db");
    let clock = Arc::new(ManualClock::new(Utc::now()));
    let calls = Arc::new(AtomicUsize::new(0));
    let engine = |path| {
        let calls = calls.clone();
        Engine::builder()
            .storage(Storage::sqlite(path))
            .clock(SharedClock::from_arc(clock.clone()))
            .effect_handler("http", move |_| {
                calls.fetch_add(1, Ordering::SeqCst);
                async { Ok(json!({"status":200, "receipt":"provider-id"})) }
            })
            .build()
    };

    let source = engine(&source_db).await.unwrap();
    let mut raw: Value = serde_json::from_str(VALID_SEQUENCE_JSON).unwrap();
    raw["blocks"] = json!([
        {"type":"step","id":"committed","handler":"http","params":{"url":"https://provider.invalid/first","method":"POST"}},
        {"type":"step","id":"unsettled","handler":"http","params":{"url":"https://provider.invalid/second","method":"POST"},"delay":{"duration":60000}}
    ]);
    let sequence: SequenceDefinition = serde_json::from_value(raw).unwrap();
    let sequence = source.upsert_sequence(sequence).await.unwrap();
    let id = source
        .create_instance(sequence, CreateInstanceOptions::default())
        .await
        .unwrap();
    for _ in 0..8 {
        source.tick_once().await.unwrap();
        if source
            .get_instance(id)
            .await
            .unwrap()
            .next_fire_at
            .is_some_and(|wake| wake > clock.now())
        {
            break;
        }
    }
    let instance = source.get_instance(id).await.unwrap();
    assert!(instance.next_fire_at.is_some_and(|wake| wake > clock.now()));
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    source.close().await;

    let store = SqliteStorage::file_path(source_db.to_str().unwrap())
        .await
        .unwrap();
    let owner = store
        .get_continuity_execution_by_instance(&instance.tenant_id, id)
        .await
        .unwrap()
        .unwrap();
    let before = store
        .list_effect_receipts(&owner.tenant_id, owner.continuity_id, 10)
        .await
        .unwrap();
    assert_eq!(before.len(), 1);
    let committed = &before[0];
    assert_eq!(committed.state, EffectState::Committed);
    let unresolved = EffectReceipt {
        id: EffectId::new(),
        block_id: BlockId::new("unsettled"),
        state: EffectState::Unknown,
        ..committed.clone()
    };
    let compensated = EffectReceipt {
        id: EffectId::new(),
        block_id: BlockId::new("old-compensated"),
        state: EffectState::Compensated,
        ..committed.clone()
    };
    store.create_effect_receipt(&unresolved).await.unwrap();
    store.create_effect_receipt(&compensated).await.unwrap();
    let attempts = store.export_effect_attempts(&owner).await.unwrap();
    assert!(
        attempts
            .0
            .iter()
            .any(|binding| binding.effect_id == committed.id)
    );
    store.close().await;

    sandbox.run_ok(&[
        "backup",
        "--database-url",
        "source.db",
        "--out",
        "effects.tar.gz",
        "--include-instances",
    ]);
    let report = sandbox.run_ok(&[
        "--yes",
        "--output",
        "json",
        "restore",
        "effects.tar.gz",
        "--database-url",
        "target.db",
        "--include-instances",
    ]);
    let report: Value = serde_json::from_str(&report).unwrap();
    assert_eq!(report["counts"]["effect_receipts"]["created"], 3);
    let store = SqliteStorage::file_path(target_db.to_str().unwrap())
        .await
        .unwrap();
    assert_eq!(
        store.export_effect_attempts(&owner).await.unwrap(),
        attempts
    );
    for expected in [committed, &compensated, &unresolved] {
        assert_eq!(
            store
                .get_effect_receipt(&owner.tenant_id, expected.id)
                .await
                .unwrap()
                .as_ref(),
            Some(expected)
        );
    }
    store.close().await;

    let target = engine(&target_db).await.unwrap();
    clock.advance(Duration::minutes(2));
    let mut advanced = false;
    for _ in 0..5 {
        advanced |= target.tick_once().await.unwrap().instances_advanced > 0;
        if target.get_instance(id).await.unwrap().state.is_terminal() {
            break;
        }
    }
    assert!(
        advanced,
        "restored workflow did not reach the unresolved effect"
    );
    assert_eq!(
        target.get_instance(id).await.unwrap().state,
        InstanceState::Failed
    );
    assert_eq!(
        calls.load(Ordering::SeqCst),
        1,
        "restore let an effect run again"
    );
    let outputs = target.block_outputs(id).await.unwrap();
    assert!(
        outputs
            .iter()
            .any(|output| output.block_id.as_str() == "committed")
    );
    assert!(!outputs.iter().any(|output| output.block_id.as_str() == "unsettled" && output.output["status"] == 200));
    target.close().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn sqlite_schema_plan_leaves_source_untouched_and_apply_is_idempotent() {
    use orch8_storage::sqlite::SqliteStorage;
    use serde_json::Value;
    use std::time::Duration;

    let sandbox = Sandbox::new();
    let path = sandbox.root.join("schema.db");
    let path_str = path.to_str().unwrap();
    let source = SqliteStorage::file_path(path_str).await.unwrap();
    sqlx::query("ALTER TABLE instance_kv_state DROP COLUMN expires_at")
        .execute(source.pool())
        .await
        .unwrap();
    sqlx::query("DROP INDEX idx_signal_inbox_history")
        .execute(source.pool())
        .await
        .unwrap();
    source.close().await;
    let original = std::fs::read(&path).unwrap();
    let reconcile = |action| {
        let output = sandbox.run_ok(&[
            "--output",
            "json",
            "sqlite-schema",
            action,
            "--database",
            "schema.db",
        ]);
        serde_json::from_str::<Value>(&output).unwrap()
    };
    let plan = reconcile("plan");
    let changes = plan["changes"].as_array().unwrap();
    assert!(changes.iter().any(|change| {
        change["action"] == "add"
            && change["kind"] == "column"
            && change["name"] == "instance_kv_state.expires_at"
    }));
    assert!(
        changes
            .iter()
            .any(|change| change["name"] == "idx_signal_inbox_history")
    );
    assert_eq!(std::fs::read(&path).unwrap(), original);
    assert_eq!(reconcile("apply"), plan);
    let unchanged = reconcile("apply");
    assert!(unchanged["changes"].as_array().unwrap().is_empty());
    assert_eq!(
        unchanged["previous_version"],
        unchanged["reconciled_version"]
    );
    assert_eq!(reconcile("plan"), unchanged);

    let owner = SqliteStorage::file_server_owned(path_str, 1, Duration::from_secs(5))
        .await
        .unwrap();
    let failure = sandbox.run_err(&["sqlite-schema", "apply", "--database", "schema.db"]);
    assert!(failure.contains("already owned"), "{failure}");
    assert_eq!(reconcile("plan"), unchanged);
    owner.close().await;
}

fn imported_definition(sandbox: &Sandbox) -> orch8_types::sequence::SequenceDefinition {
    let mut value: serde_json::Value =
        serde_json::from_slice(&std::fs::read(sandbox.root.join("converted.json")).unwrap())
            .unwrap();
    value["id"] = serde_json::json!(uuid::Uuid::now_v7());
    value["created_at"] = serde_json::json!(chrono::Utc::now());
    let definition = orch8_types::sequence::deserialize_sequence_strict(&value).unwrap();
    definition.validate().unwrap();
    definition
}

#[tokio::test]
#[allow(clippy::too_many_lines)]
async fn step_functions_routes_typed_errors_through_ordered_catchers() {
    use orch8::{CreateInstanceOptions, Engine, Storage};
    use orch8_types::{context::ExecutionContext, error::StepError, instance::InstanceState};
    use serde_json::json;

    let sandbox = Sandbox::new();
    let source = json!({
        "StartAt":"Work",
        "States":{
            "Work":{"Type":"Task","Resource":"arn:aws:states:us-east-1:111111111111:activity:typed-failure",
                "Parameters":{"failure.$":"$.failure"},
                "Catch":[
                    {"ErrorEquals":["ValidationError"],"Next":"Validation","ResultPath":"$.failure_details"},
                    {"ErrorEquals":["ServiceError","ValidationError"],"Next":"Service"},
                    {"ErrorEquals":["States.ALL"],"Next":"Fallback"}
                ],"End":true},
            "Validation":{"Type":"Pass","Parameters":{"route":"validation",
                "error.$":"$.failure_details.Error","source.$":"$.source"},"End":true},
            "Service":{"Type":"Pass","Result":{"route":"service"},"End":true},
            "Fallback":{"Type":"Pass","Result":{"route":"fallback"},"End":true}
        }
    });
    sandbox.write_sequence("catch.json", &source.to_string());
    sandbox.run_ok(&[
        "import",
        "stepfunctions",
        "catch.json",
        "--out",
        "converted.json",
    ]);
    let definition = imported_definition(&sandbox);
    let storage = sandbox.root.join("catch.db");
    let engine = Engine::builder()
        .storage(Storage::sqlite(&storage))
        .handler("typed_failure", |context| async move {
            Err(StepError::Permanent {
                message: "typed failure".into(),
                details: Some(json!({"error_code": context.params["failure"]})),
            })
        })
        .build()
        .await
        .unwrap();
    let sequence = engine.upsert_sequence(definition).await.unwrap();
    for (failure, expected) in [
        (
            "ValidationError",
            Some(json!({"route":"validation","error":"ValidationError","source":"original"})),
        ),
        ("ServiceError", Some(json!({"route":"service"}))),
        ("UnexpectedError", Some(json!({"route":"fallback"}))),
        ("States.Runtime", None),
    ] {
        let id = engine
            .create_instance(
                sequence,
                CreateInstanceOptions {
                    context: ExecutionContext {
                        data: json!({"failure":failure,"source":"original"}),
                        ..ExecutionContext::default()
                    },
                    ..CreateInstanceOptions::default()
                },
            )
            .await
            .unwrap();
        for _ in 0..35 {
            engine.tick_once().await.unwrap();
            if engine.get_instance(id).await.unwrap().state.is_terminal() {
                break;
            }
        }
        let state = engine.get_instance(id).await.unwrap().state;
        let routed: Vec<_> = engine
            .block_outputs(id)
            .await
            .unwrap()
            .into_iter()
            .filter_map(|output| {
                output
                    .output
                    .get("route")
                    .is_some()
                    .then_some(output.output)
            })
            .collect();
        if let Some(expected) = expected {
            assert_eq!(state, InstanceState::Completed, "{failure}");
            assert_eq!(routed, vec![expected]);
        } else {
            assert_eq!(state, InstanceState::Failed, "{failure}");
            assert!(routed.is_empty());
        }
    }
    engine.close().await;
}

#[tokio::test]
async fn step_functions_ordered_retries_keep_independent_budgets_after_reopen() {
    use chrono::{Duration, Timelike, Utc};
    use orch8::{CreateInstanceOptions, Engine, ManualClock, SharedClock, Storage};
    use orch8_types::{error::StepError, instance::InstanceState};
    use serde_json::json;
    use std::sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    };

    let sandbox = Sandbox::new();
    let source = json!({
        "StartAt":"Work", "States":{"Work":{
            "Type":"Task",
            "Resource":"arn:aws:states:us-east-1:111111111111:activity:ordered-retry",
            "Retry":[
                {"ErrorEquals":["A"],"MaxAttempts":1,"IntervalSeconds":1},
                {"ErrorEquals":["B","A"],"MaxAttempts":2,"IntervalSeconds":4},
                {"ErrorEquals":["States.ALL"],"MaxAttempts":4,"IntervalSeconds":20}
            ],"End":true
        }}
    });
    sandbox.write_sequence("retry.json", &source.to_string());
    sandbox.run_ok(&[
        "import",
        "stepfunctions",
        "retry.json",
        "--out",
        "converted.json",
    ]);
    let database = sandbox.root.join("ordered-retry.db");
    let mut now = Utc::now().with_nanosecond(0).unwrap();
    let clock = Arc::new(ManualClock::new(now));
    let calls = Arc::new(AtomicUsize::new(0));
    let open = || {
        let calls = calls.clone();
        Engine::builder()
            .storage(Storage::sqlite(&database))
            .clock(SharedClock::from_arc(clock.clone()))
            .handler("ordered_retry", move |_| {
                let call = calls.fetch_add(1, Ordering::SeqCst);
                async move {
                    let code = ["A", "B", "B", "A"][call];
                    Err(StepError::Permanent {
                        message: format!("failure {code}"),
                        details: Some(json!({"error_code":code})),
                    })
                }
            })
            .build()
    };
    let mut engine = open().await.unwrap();
    let sequence = engine
        .upsert_sequence(imported_definition(&sandbox))
        .await
        .unwrap();
    let id = engine
        .create_instance(sequence, CreateInstanceOptions::default())
        .await
        .unwrap();
    for (attempt, seconds) in [1, 4, 8].into_iter().enumerate() {
        engine.tick_once().await.unwrap();
        assert_eq!(calls.load(Ordering::SeqCst), attempt + 1);
        let deadline = engine.get_instance(id).await.unwrap().next_fire_at.unwrap();
        assert_eq!(deadline, now + Duration::seconds(seconds));
        engine.close().await;
        engine = open().await.unwrap();
        now = deadline;
        clock.set(now);
    }
    engine.tick_once().await.unwrap();
    assert_eq!(calls.load(Ordering::SeqCst), 4);
    assert_eq!(
        engine.get_instance(id).await.unwrap().state,
        InstanceState::Failed
    );
    engine.close().await;
}

#[tokio::test]
#[allow(clippy::too_many_lines)]
async fn step_functions_full_jitter_restores_the_chosen_retry_deadline() {
    use chrono::{Duration, Timelike, Utc};
    use orch8::{CreateInstanceOptions, Engine, ManualClock, SharedClock, Storage};
    use orch8_storage::{InstanceStore as _, sqlite::SqliteStorage};
    use orch8_types::error::StepError;
    use orch8_types::instance::InstanceState;
    use serde_json::json;
    use std::sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    };

    let sandbox = Sandbox::new();
    let source = json!({
        "StartAt": "Try",
        "States": {"Try": {
            "Type":"Task", "Resource":"arn:aws:states:us-east-1:111111111111:activity:retry-jitter",
            "Retry":[{"ErrorEquals":["States.ALL"],"MaxAttempts":1,"IntervalSeconds":20,
                "MaxDelaySeconds":10,"JitterStrategy":"FULL"}],
            "End":true
        }}
    });
    sandbox.write_sequence("retry.json", &source.to_string());
    sandbox.run_ok(&[
        "import",
        "stepfunctions",
        "retry.json",
        "--out",
        "converted.json",
    ]);
    let definition = imported_definition(&sandbox);
    let policy = serde_json::to_value(&definition).unwrap()["blocks"][0]["retry"].clone();
    assert_eq!(policy["jitter"], "full");
    assert_eq!(policy["max_attempts"], 1);

    let database = sandbox.root.join("retry.db");
    let start = Utc::now().with_nanosecond(0).unwrap();
    let clock = Arc::new(ManualClock::new(start));
    let attempts = Arc::new(AtomicUsize::new(0));
    let open = || {
        let calls = attempts.clone();
        Engine::builder()
            .storage(Storage::sqlite(&database))
            .clock(SharedClock::from_arc(clock.clone()))
            .handler("retry_jitter", move |_| {
                let call = calls.fetch_add(1, Ordering::SeqCst);
                async move {
                    if call == 0 {
                        Err(StepError::Retryable {
                            message: "transient".into(),
                            details: None,
                        })
                    } else {
                        Ok(json!({"ok":true}))
                    }
                }
            })
            .build()
    };
    let engine = open().await.unwrap();
    let sequence = engine.upsert_sequence(definition).await.unwrap();
    let id = engine
        .create_instance(sequence, CreateInstanceOptions::default())
        .await
        .unwrap();
    engine.tick_once().await.unwrap();
    let deadline = engine.get_instance(id).await.unwrap().next_fire_at.unwrap();
    assert!((start..=start + Duration::seconds(10)).contains(&deadline));
    assert_eq!(attempts.load(Ordering::SeqCst), 1);
    let outputs = engine.block_outputs(id).await.unwrap();
    let marker = outputs
        .iter()
        .find(|entry| entry.output_ref.as_deref() == Some("__retry__"))
        .unwrap();
    let marker_deadline: chrono::DateTime<Utc> =
        serde_json::from_value(marker.output["_retry_fire_at"].clone()).unwrap();
    assert_eq!(marker_deadline, deadline);
    engine.close().await;

    let before = deadline - Duration::microseconds(1);
    clock.set(before);
    let writer = SqliteStorage::file_path(database.to_str().unwrap())
        .await
        .unwrap();
    writer
        .update_instance_state(id, InstanceState::Scheduled, Some(before))
        .await
        .unwrap();
    writer.close().await;
    let engine = open().await.unwrap();
    engine.tick_once().await.unwrap();
    assert_eq!(
        engine.get_instance(id).await.unwrap().next_fire_at,
        Some(deadline)
    );
    assert_eq!(attempts.load(Ordering::SeqCst), 1);
    clock.set(deadline + Duration::milliseconds(1));
    for _ in 0..5 {
        engine.tick_once().await.unwrap();
        if engine.get_instance(id).await.unwrap().state.is_terminal() {
            break;
        }
    }
    assert_eq!(
        engine.get_instance(id).await.unwrap().state,
        InstanceState::Completed
    );
    assert_eq!(attempts.load(Ordering::SeqCst), 2);
    engine.close().await;
}

#[tokio::test]
#[allow(clippy::too_many_lines)]
async fn step_functions_item_selector_materializes_values_and_indexes_per_iteration() {
    use orch8::{CreateInstanceOptions, Engine, Storage};
    use orch8_types::context::ExecutionContext;
    use orch8_types::instance::InstanceState;
    use serde_json::{Value, json};

    let sandbox = Sandbox::new();
    let source = json!({
        "StartAt":"Walk",
        "States":{"Walk":{
            "Type":"Map","ItemsPath":"$.items","MaxConcurrency":1,"End":true,
            "ItemSelector":{
                "kind":"order",
                "details":[{"value.$":"$$.Map.Item.Value","index.$":"$$.Map.Item.Index"}],
                "region.$":"$.region"
            },
            "ItemProcessor":{
                "ProcessorConfig":{"Mode":"INLINE"},
                "StartAt":"Record",
                "States":{"Record":{"Type":"Pass","Parameters":{"selection.$":"$"},"End":true}}
            }
        }}
    });
    sandbox.write_sequence("map.json", &source.to_string());
    sandbox.run_ok(&[
        "import",
        "stepfunctions",
        "map.json",
        "--out",
        "converted.json",
    ]);
    let definition = imported_definition(&sandbox);
    let engine = Engine::builder()
        .storage(Storage::sqlite(sandbox.root.join("map.db")))
        .build()
        .await
        .unwrap();
    let sequence = engine.upsert_sequence(definition).await.unwrap();
    let id = engine
        .create_instance(
            sequence,
            CreateInstanceOptions {
                context: ExecutionContext {
                    data: json!({"items":["same","same","different"],"region":"south"}),
                    ..ExecutionContext::default()
                },
                ..CreateInstanceOptions::default()
            },
        )
        .await
        .unwrap();
    for _ in 0..20 {
        engine.tick_once().await.unwrap();
        if engine.get_instance(id).await.unwrap().state.is_terminal() {
            break;
        }
    }
    assert_eq!(
        engine.get_instance(id).await.unwrap().state,
        InstanceState::Completed
    );
    let outputs = engine.block_outputs(id).await.unwrap();
    let selected: Vec<Value> = outputs
        .iter()
        .filter(|row| row.block_id.as_str() == "record")
        .map(|row| row.output["selection"].clone())
        .collect();
    assert_eq!(
        selected,
        [
            json!({"kind":"order","details":[{"value":"same","index":0}],"region":"south"}),
            json!({"kind":"order","details":[{"value":"same","index":1}],"region":"south"}),
            json!({"kind":"order","details":[{"value":"different","index":2}],"region":"south"})
        ]
    );
    engine.close().await;
}

#[tokio::test]
#[allow(clippy::too_many_lines)]
async fn step_functions_result_selector_runs_before_result_path() {
    use orch8::{CreateInstanceOptions, Engine, StepContext, Storage};
    use orch8_types::context::ExecutionContext;
    use orch8_types::instance::InstanceState;
    use serde_json::json;

    let sandbox = Sandbox::new();
    let definition = json!({
        "StartAt": "Lookup",
        "States": {
            "Lookup": {
                "Type":"Task", "Resource":"arn:aws:lambda:us-east-1:123456789012:function:lookup",
                "InputPath":"$.request", "ResultPath":"$.response", "Next":"Read",
                "ResultSelector": {
                    "chosen":[{"code.$":"$.records[1].code"},null,"{{data.original}}"],
                    "nested":{"optional.$":"$.records[0].note"}
                }
            },
            "Read": {
                "Type":"Pass", "End":true,
                "Parameters": {
                    "chosen.$":"$.response.chosen", "from_null.$":"$.response.nested.optional",
                    "source_sibling.$":"$.original", "whole.$":"$"
                }
            }
        }
    });
    sandbox.write_sequence("selector.json", &definition.to_string());
    sandbox.run_ok(&[
        "import",
        "stepfunctions",
        "selector.json",
        "--out",
        "converted.json",
    ]);
    let sequence = imported_definition(&sandbox);
    let engine = Engine::builder()
        .storage(Storage::sqlite(sandbox.root.join("selector.db")))
        .handler("lookup", |ctx: StepContext| async move {
            assert_eq!(ctx.params, json!({"input": {"query":"invoice"}}));
            Ok(json!({"records":[{"note":null},{"code":12}], "ignored":"raw payload"}))
        })
        .build()
        .await
        .unwrap();
    let sequence = engine.upsert_sequence(sequence).await.unwrap();
    let id = engine
        .create_instance(
            sequence,
            CreateInstanceOptions {
                context: ExecutionContext {
                    data: json!({"request":{"query":"invoice"},"original":"from original input"}),
                    ..ExecutionContext::default()
                },
                ..CreateInstanceOptions::default()
            },
        )
        .await
        .unwrap();
    for _ in 0..8 {
        engine.tick_once().await.unwrap();
        if engine.get_instance(id).await.unwrap().state.is_terminal() {
            break;
        }
    }
    assert_eq!(
        engine.get_instance(id).await.unwrap().state,
        InstanceState::Completed
    );
    let outputs = engine.block_outputs(id).await.unwrap();
    let read = &outputs
        .iter()
        .find(|output| output.block_id.as_str() == "read")
        .unwrap()
        .output;
    assert_eq!(
        *read,
        json!({
            "chosen":[{"code":12},null,"{{data.original}}"],
            "from_null":null,
            "source_sibling":"from original input",
            "whole":{
                "request":{"query":"invoice"},
                "original":"from original input",
                "response":{
                    "chosen":[{"code":12},null,"{{data.original}}"],
                    "nested":{"optional":null}
                }
            }
        })
    );
    engine.close().await;
}

#[tokio::test]
#[allow(clippy::too_many_lines)]
async fn step_functions_intrinsics_import_and_run_with_typed_results() {
    use orch8::{CreateInstanceOptions, Engine, Storage};
    use orch8_types::context::ExecutionContext;
    use orch8_types::instance::InstanceState;
    use serde_json::json;

    let sandbox = Sandbox::new();
    let file = sandbox.root.join("source.json");
    let source = json!({
        "StartAt": "Compose",
        "States": {
            "Compose": {
                "Type": "Pass",
                "Parameters": {
                    "content.$": r"States.Array(States.Format('\{\} {} \'{}\' \\', $.name, 7), States.StringToJson($.document), true, null)",
                    "empty.$": "States.Array()"
                },
                "End": true
            }
        }
    });
    std::fs::write(&file, serde_json::to_vec(&source).unwrap()).unwrap();
    sandbox.run_ok(&[
        "import",
        "stepfunctions",
        "source.json",
        "--out",
        "converted.json",
    ]);
    let definition = imported_definition(&sandbox);
    let engine = Engine::builder()
        .storage(Storage::sqlite_in_memory())
        .build()
        .await
        .unwrap();
    let sequence = engine.upsert_sequence(definition).await.unwrap();
    for (document, expected) in [
        (r#"{"size":42}"#, InstanceState::Completed),
        ("invalid JSON", InstanceState::Failed),
    ] {
        let id = engine
            .create_instance(
                sequence,
                CreateInstanceOptions {
                    context: ExecutionContext {
                        data: json!({"name":"blue", "document":document}),
                        ..ExecutionContext::default()
                    },
                    ..CreateInstanceOptions::default()
                },
            )
            .await
            .unwrap();
        for _ in 0..4 {
            engine.tick_once().await.unwrap();
            if engine.get_instance(id).await.unwrap().state.is_terminal() {
                break;
            }
        }
        assert_eq!(engine.get_instance(id).await.unwrap().state, expected);
        if expected == InstanceState::Completed {
            let outputs = engine.block_outputs(id).await.unwrap();
            let output = &outputs
                .iter()
                .find(|output| output.block_id.as_str() == "compose")
                .unwrap()
                .output;
            assert_eq!(
                output["content"],
                json!(["{} blue '7' \\", {"size":42}, true, null])
            );
            assert_eq!(output["empty"], json!([]));
        } else {
            assert!(
                engine
                    .block_outputs(id)
                    .await
                    .unwrap()
                    .iter()
                    .all(|output| output.output.get("content").is_none())
            );
        }
    }
    engine.close().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[allow(clippy::too_many_lines)]
async fn step_functions_dynamic_wait_keeps_original_deadline_across_reopen() {
    use chrono::{Duration, Timelike, Utc};
    use orch8::{Clock, CreateInstanceOptions, Engine, ManualClock, SharedClock, Storage};
    use orch8_storage::InstanceStore as _;
    use orch8_storage::sqlite::SqliteStorage;
    use orch8_types::context::ExecutionContext;
    use orch8_types::instance::InstanceState;
    use serde_json::{Value, json};
    use std::sync::Arc;

    let start = Utc::now().with_nanosecond(0).unwrap();
    let wake = start + Duration::seconds(120);
    let timestamp =
        (wake + Duration::milliseconds(789)).to_rfc3339_opts(chrono::SecondsFormat::Millis, true);
    for (path_parameter, original) in [
        ("SecondsPath", json!(120)),
        ("TimestampPath", json!(timestamp)),
    ] {
        let sandbox = Sandbox::new();
        let db = sandbox.root.join("durable.db");
        let clock = Arc::new(ManualClock::new(start));
        let open = || {
            Engine::builder()
                .storage(Storage::sqlite(&db))
                .clock(SharedClock::from_arc(clock.clone()))
                .build()
        };
        let source = json!({
            "StartAt": "Wait",
            "States": {
                "Wait": {"Type":"Wait", (path_parameter):"$.waiting", "Next":"Complete"},
                "Complete": {"Type":"Pass","Result":{"ok":true},"End":true}
            }
        });
        std::fs::write(
            sandbox.root.join("wait.json"),
            serde_json::to_vec(&source).unwrap(),
        )
        .unwrap();
        sandbox.run_ok(&[
            "import",
            "stepfunctions",
            "wait.json",
            "--out",
            "converted.json",
        ]);
        let definition = imported_definition(&sandbox);

        let engine = open().await.unwrap();
        let sequence = engine.upsert_sequence(definition).await.unwrap();
        let id = engine
            .create_instance(
                sequence,
                CreateInstanceOptions {
                    context: ExecutionContext {
                        data: json!({"waiting":original}),
                        ..ExecutionContext::default()
                    },
                    ..CreateInstanceOptions::default()
                },
            )
            .await
            .unwrap();
        engine.tick_once().await.unwrap();
        assert_eq!(
            engine.get_instance(id).await.unwrap().next_fire_at,
            Some(wake)
        );
        assert!(engine.block_outputs(id).await.unwrap().is_empty());
        engine.close().await;

        let writer = SqliteStorage::file_path(db.to_str().unwrap())
            .await
            .unwrap();
        let mut task = writer.get_instance(id).await.unwrap().unwrap();
        task.context.data["waiting"] = Value::Null;
        writer
            .update_instance_context(id, &task.context)
            .await
            .unwrap();
        clock.advance(Duration::seconds(60));
        writer
            .update_instance_state(id, InstanceState::Scheduled, Some(clock.now()))
            .await
            .unwrap();
        writer.close().await;

        let reopened = open().await.unwrap();
        reopened.tick_once().await.unwrap();
        assert_eq!(
            reopened.get_instance(id).await.unwrap().next_fire_at,
            Some(wake)
        );
        assert!(reopened.block_outputs(id).await.unwrap().is_empty());
        clock.advance(Duration::seconds(60));
        for _ in 0..5 {
            reopened.tick_once().await.unwrap();
            if reopened.get_instance(id).await.unwrap().state.is_terminal() {
                break;
            }
        }
        assert_eq!(
            reopened.get_instance(id).await.unwrap().state,
            InstanceState::Completed
        );
        let outputs = reopened.block_outputs(id).await.unwrap();
        assert!(
            outputs
                .iter()
                .any(|output| output.block_id.as_str() == "complete"
                    && output.output == json!({"ok":true}))
        );
        reopened.close().await;
    }
}

#[tokio::test]
async fn state_schema_fences_rejected_writes_before_they_commit() {
    use orch8::{CreateInstanceOptions, Engine, Storage};
    use orch8_storage::{ResourceStore as _, sqlite::SqliteStorage};
    use orch8_types::instance::InstanceState;
    use serde_json::json;

    let sandbox = Sandbox::new();
    let database = sandbox.root.join("typed-state.db");
    let engine = Engine::builder()
        .storage(Storage::sqlite(&database))
        .build()
        .await
        .unwrap();
    let sequence = serde_json::from_value(json!({
        "id":uuid::Uuid::now_v7(),"name":"typed-state","tenant_id":"default","namespace":"default",
        "version":1,"created_at":chrono::Utc::now(),
        "input_schema":{"x-orch8-state-schema":{
            "type":"object","additionalProperties":false,"properties":{"count":{"type":"integer"}}
        }},
        "blocks":[
            {"type":"step","id":"valid","handler":"set_state","params":{"key":"count","value":8}},
            {"type":"step","id":"invalid","handler":"set_state","params":{"key":"count","value":"8"}}
        ]
    })).unwrap();
    let sequence = engine.upsert_sequence(sequence).await.unwrap();
    let id = engine
        .create_instance(sequence, CreateInstanceOptions::default())
        .await
        .unwrap();
    engine.tick_once().await.unwrap();
    assert_eq!(
        engine.get_instance(id).await.unwrap().state,
        InstanceState::Failed
    );
    engine.close().await;

    let reopened = SqliteStorage::file_path(database.to_str().unwrap())
        .await
        .unwrap();
    assert_eq!(
        reopened.get_instance_kv(id, "count").await.unwrap(),
        Some(json!(8))
    );
    reopened.close().await;
}

#[tokio::test]
#[allow(clippy::too_many_lines)] // Verify the complete export and empty-store restore.
async fn artifact_backup_restores_original_output_references_into_an_empty_store() {
    use orch8::{CreateInstanceOptions, Engine, Storage};
    use orch8_storage::{
        InstanceStore as _, OutputStore as _, artifacts::ObjectArtifactStore, sqlite::SqliteStorage,
    };
    use orch8_types::{ids::BlockId, output::BlockOutput};

    let sandbox = Sandbox::new();
    let database = sandbox.root.join("source.db");
    let engine = Engine::builder()
        .storage(Storage::sqlite(&database))
        .build()
        .await
        .unwrap();
    let sequence = serde_json::from_str(VALID_SEQUENCE_JSON).unwrap();
    let sequence = engine.upsert_sequence(sequence).await.unwrap();
    let instance = engine
        .create_instance(sequence, CreateInstanceOptions::default())
        .await
        .unwrap();
    engine.close().await;

    let source_dir = sandbox.root.join("source-artifacts");
    let source = ObjectArtifactStore::local(source_dir.to_str().unwrap()).unwrap();
    let file = b"first line\n\x00\xff\r\nlast line";
    let mut references = Vec::new();
    for _ in 0..2 {
        references.push(
            source
                .put(
                    &instance.to_string(),
                    "application/octet-stream",
                    file.to_vec().into(),
                )
                .await
                .unwrap(),
        );
    }
    let storage = SqliteStorage::file_path(database.to_str().unwrap())
        .await
        .unwrap();
    storage
        .save_block_output(&BlockOutput {
            id: uuid::Uuid::now_v7(),
            instance_id: instance,
            block_id: BlockId::new("attachment"),
            output: references[0].clone().into_output(),
            output_ref: None,
            output_size: 512,
            attempt: 0,
            created_at: chrono::Utc::now(),
        })
        .await
        .unwrap();
    storage.close().await;

    let manifest = sandbox.run_ok(&[
        "--output",
        "json",
        "backup",
        "--database-url",
        "source.db",
        "--out",
        "backup.tar.gz",
        "--include-instances",
        "--artifacts-dir",
        "source-artifacts",
    ]);
    let manifest: serde_json::Value = serde_json::from_str(&manifest).unwrap();
    let members = manifest["files"].as_array().unwrap();
    assert_eq!(
        members
            .iter()
            .filter(|member| member["name"]
                .as_str()
                .unwrap()
                .starts_with("artifact-sha256-"))
            .count(),
        1
    );
    let restore = [
        "--output",
        "json",
        "--yes",
        "restore",
        "backup.tar.gz",
        "--database-url",
        "target.db",
        "--include-instances",
        "--artifacts-dir",
        "target-artifacts",
    ];
    for (created, existing) in [(2, 0), (0, 2)] {
        let report: serde_json::Value = serde_json::from_str(&sandbox.run_ok(&restore)).unwrap();
        assert_eq!(report["counts"]["artifacts"]["created"], created);
        assert_eq!(report["counts"]["artifacts"]["existing"], existing);
    }
    let destination =
        ObjectArtifactStore::local(sandbox.root.join("target-artifacts").to_str().unwrap())
            .unwrap();
    let storage = SqliteStorage::file_path(sandbox.root.join("target.db").to_str().unwrap())
        .await
        .unwrap();
    assert!(storage.get_instance(instance).await.unwrap().is_some());
    let output = storage
        .get_block_output(instance, &BlockId::new("attachment"))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(output.output["artifact"]["key"], references[0].key);
    for reference in &references {
        assert_eq!(
            destination.get(&reference.key).await.unwrap().unwrap(),
            file
        );
    }
}

#[tokio::test]
#[allow(clippy::too_many_lines)] // Compare renewed and missed deadlines in one workflow fixture.
async fn step_functions_heartbeat_renews_durable_eligibility_and_catches_missed_deadline() {
    use orch8::{CreateInstanceOptions, Engine, Storage};
    use orch8_storage::{WorkerStore, sqlite::SqliteStorage};
    use orch8_types::{
        context::ExecutionContext,
        instance::InstanceState,
        worker::{WorkerAttemptEventKind, WorkerClaim, WorkerTaskState},
    };
    use serde_json::json;
    use std::time::Duration;

    let sandbox = Sandbox::new();
    let source = json!({
        "StartAt": "Work",
        "States": {
            "Work": {
                "Type": "Task",
                "Resource": "arn:aws:states:us-east-1:111111111111:activity:heartbeat-work",
                "Parameters": {"job.$": "$.job"},
                "HeartbeatSecondsPath": "$.lease.seconds",
                "TimeoutSeconds": 7200,
                "Catch": [
                    {"ErrorEquals": ["States.Timeout"], "Next": "Timed out"},
                    {"ErrorEquals": ["States.ALL"], "Next": "Wrong branch"}
                ],
                "End": true
            },
            "Timed out": {
                "Type": "Pass",
                "Parameters": {"route": "timeout", "error.$": "$.Error"},
                "End": true
            },
            "Wrong branch": {"Type": "Fail", "Error": "Unexpected"}
        }
    });
    sandbox.write_sequence("heartbeat.json", &source.to_string());
    sandbox.run_ok(&[
        "import",
        "stepfunctions",
        "heartbeat.json",
        "--out",
        "converted.json",
    ]);

    let database = sandbox.root.join("heartbeat.db");
    let engine = Engine::builder()
        .storage(Storage::sqlite(&database))
        .build()
        .await
        .unwrap();
    let sequence = engine
        .upsert_sequence(imported_definition(&sandbox))
        .await
        .unwrap();
    let instance = engine
        .create_instance(
            sequence,
            CreateInstanceOptions {
                context: ExecutionContext {
                    data: json!({"job": "reconcile", "lease": {"seconds": 3600}}),
                    ..ExecutionContext::default()
                },
                ..CreateInstanceOptions::default()
            },
        )
        .await
        .unwrap();
    for _ in 0..10 {
        engine.tick_once().await.unwrap();
        if engine.get_instance(instance).await.unwrap().state == InstanceState::Waiting {
            break;
        }
    }
    let store = SqliteStorage::file_path(database.to_str().unwrap())
        .await
        .unwrap();
    let claimed = store
        .claim_worker_tasks("heartbeat_work", "worker", 1)
        .await
        .unwrap();
    let [task] = claimed.as_slice() else {
        panic!("expected one worker task")
    };
    assert_eq!(task.heartbeat_timeout_secs, Some(3600));
    assert_eq!(task.params, json!({"job": "reconcile"}));
    let task_id = task.id;
    let claim = WorkerClaim::new("worker", task.claim_epoch);
    let old = (chrono::Utc::now() - chrono::Duration::minutes(59)).to_rfc3339();
    sqlx::query("UPDATE worker_tasks SET claimed_at=?1,heartbeat_at=?1 WHERE id=?2")
        .bind(&old)
        .bind(task_id.to_string())
        .execute(store.pool())
        .await
        .unwrap();
    assert!(store.heartbeat_worker_task(task_id, &claim).await.unwrap());

    // Move the claim beyond its original window while retaining the renewal.
    let expired = (chrono::Utc::now() - chrono::Duration::minutes(61)).to_rfc3339();
    sqlx::query("UPDATE worker_tasks SET claimed_at=?1 WHERE id=?2")
        .bind(&expired)
        .bind(task_id.to_string())
        .execute(store.pool())
        .await
        .unwrap();
    store.close().await;
    let store = SqliteStorage::file_path(database.to_str().unwrap())
        .await
        .unwrap();
    let durable = store.get_worker_task(task_id).await.unwrap().unwrap();
    assert_eq!(durable.heartbeat_timeout_secs, Some(3600));
    assert!(durable.heartbeat_at > durable.claimed_at);
    let reaper_lease = Duration::from_secs(1);
    let active = orch8_engine::worker_lease::reap_worker_tasks(&store, reaper_lease)
        .await
        .unwrap();
    assert_eq!(active.total(), 0);
    assert_eq!(durable.state, WorkerTaskState::Claimed);

    sqlx::query("UPDATE worker_tasks SET heartbeat_at=?1 WHERE id=?2")
        .bind(&expired)
        .bind(task_id.to_string())
        .execute(store.pool())
        .await
        .unwrap();
    assert!(!store.heartbeat_worker_task(task_id, &claim).await.unwrap());
    assert!(
        !store
            .complete_worker_task(task_id, &claim, &json!({"late": true}))
            .await
            .unwrap()
    );
    let expired = orch8_engine::worker_lease::reap_worker_tasks(&store, reaper_lease)
        .await
        .unwrap();
    assert_eq!(expired.timed_out, 1);
    assert_eq!(expired.requeued, 0);
    assert!(
        store
            .list_worker_task_attempt_events(task_id, 20)
            .await
            .unwrap()
            .iter()
            .any(|event| {
                event.event == WorkerAttemptEventKind::TimedOut
                    && event.reason.as_deref()
                        == Some(orch8_engine::worker_lease::HEARTBEAT_TIMED_OUT_REASON)
            })
    );
    for _ in 0..15 {
        engine.tick_once().await.unwrap();
        if engine
            .get_instance(instance)
            .await
            .unwrap()
            .state
            .is_terminal()
        {
            break;
        }
    }
    assert_eq!(
        engine.get_instance(instance).await.unwrap().state,
        InstanceState::Completed
    );
    assert!(
        engine
            .block_outputs(instance)
            .await
            .unwrap()
            .iter()
            .any(|output| {
                output.output == json!({"route": "timeout", "error": "States.Timeout"})
            })
    );
    store.close().await;
    engine.close().await;
}
#[tokio::test]
#[allow(clippy::too_many_lines)]
async fn step_functions_callback_token_is_durable_tenant_bound_and_single_use() {
    use orch8::{CreateInstanceOptions, Engine, Storage};
    use orch8_storage::{WorkerStore, sqlite::SqliteStorage};
    use orch8_types::instance::InstanceState;
    use reqwest::StatusCode;
    use serde_json::{Value, json};
    use std::{sync::Arc, time::Duration};

    let sandbox = Sandbox::new();
    let source = json!({
        "StartAt": "Wait",
        "States": {
            "Wait": {
                "Type": "Task",
                "Resource": "arn:aws:states:::sqs:sendMessage.waitForTaskToken",
                "Parameters": {"items": [{
                    "token.$": "$$.Task.Token",
                    "message/parts.$": r#"States.Array(States.Format('{}:{}', $.label, $$.Task.Token), States.StringToJson(States.Format('\{"ticket":"{}"\}', $$.Task.Token)))"#,
                    "untrusted.$": "$.untrusted"
                }]},
                "TimeoutSecondsPath": "$.callback_seconds",
                "HeartbeatSeconds": 90,
                "Catch": [{"ErrorEquals": ["States.Timeout"], "Next": "Missed"}],
                "End": true
            },
            "Missed": {"Type": "Pass", "Result": {"route": "timeout"}, "End": true}
        }
    });
    sandbox.write_sequence("callback.json", &source.to_string());
    sandbox.run_ok(&[
        "import",
        "stepfunctions",
        "callback.json",
        "--out",
        "converted.json",
    ]);

    let database = sandbox.root.join("callback.db");
    let engine = Engine::builder()
        .storage(Storage::sqlite(&database))
        .build()
        .await
        .unwrap();
    let definition = imported_definition(&sandbox);
    let tenant = definition.tenant_id.clone();
    let sequence = engine.upsert_sequence(definition).await.unwrap();
    let open = || async {
        Arc::new(
            SqliteStorage::file_path(database.to_str().unwrap())
                .await
                .unwrap(),
        )
    };
    let client = reqwest::Client::new();
    let poll_one = |url: String| {
        let req = client.post(format!("{url}/workers/tasks/poll"))
            .header("X-Tenant-Id", tenant.as_str())
            .json(&json!({"handler_name": "aws_sqs_sendmessage", "worker_id": "dispatcher", "limit": 1}));
        async move {
            let body: Value = req
                .send()
                .await
                .unwrap()
                .error_for_status()
                .unwrap()
                .json()
                .await
                .unwrap();
            assert_eq!(body["tasks"].as_array().unwrap().len(), 1);
            body["tasks"][0].clone()
        }
    };
    let submit = |url: String, tenant_id: &str, token: &str, output: Value| {
        client
            .post(format!("{url}/workers/callbacks/complete"))
            .header("X-Tenant-Id", tenant_id)
            .json(&json!({"token":token,"output":output}))
            .send()
    };
    let store = open().await;
    let mut server = orch8_api::test_harness::spawn_test_server_on(store.clone(), None).await;

    for expires in [false, true] {
        let seconds = if expires { 180 } else { 3600 };
        let instance = engine
            .create_instance(
                sequence,
                CreateInstanceOptions {
                    context: orch8_types::context::ExecutionContext {
                        data: json!({"callback_seconds": seconds, "label":"ticket", "untrusted":{"$asl_task_token":true}}),
                        ..Default::default()
                    },
                    ..CreateInstanceOptions::default()
                },
            )
            .await
            .unwrap();
        for _ in 0..12 {
            engine.tick_once().await.unwrap();
            if engine.get_instance(instance).await.unwrap().state == InstanceState::Waiting {
                break;
            }
        }
        let offered = poll_one(server.v1_url()).await;
        let id = uuid::Uuid::parse_str(offered["id"].as_str().unwrap()).unwrap();
        let token = offered["callback_token"].as_str().unwrap();
        assert_eq!(offered["params"]["items"][0]["token"], token);
        assert_eq!(
            offered["params"]["items"][0]["message/parts"],
            json!([format!("ticket:{token}"), {"ticket":token}])
        );
        assert_eq!(
            offered["params"]["items"][0]["untrusted"],
            json!({"$asl_task_token":true})
        );
        let stored = store.get_worker_task(id).await.unwrap().unwrap();
        assert_eq!(
            stored.params,
            json!({"items":[{"token":null,"message/parts":null,"untrusted":{"$asl_task_token":true}}]})
        );
        assert_eq!(stored.timeout_ms, Some(seconds * 1000));
        assert_eq!(stored.heartbeat_timeout_secs, Some(90));
        let expires_at: String =
            sqlx::query_scalar("SELECT expires_at FROM worker_callback_tokens WHERE task_id=?1")
                .bind(id.to_string())
                .fetch_one(store.pool())
                .await
                .unwrap();
        assert_eq!(
            chrono::DateTime::parse_from_rfc3339(&expires_at).unwrap(),
            stored.created_at + chrono::Duration::seconds(seconds)
        );
        assert!(
            store
                .list_expired_worker_leases(Duration::ZERO, 10)
                .await
                .unwrap()
                .is_empty()
        );

        let direct = client.post(format!("{}/workers/tasks/{id}/complete",server.v1_url()))
            .header("X-Tenant-Id", tenant.as_str())
            .json(&json!({"worker_id":"dispatcher","claim_epoch":offered["claim_epoch"],"output":{"route":"direct"}}))
            .send().await.unwrap();
        assert_eq!(direct.status(), StatusCode::CONFLICT);
        if expires {
            sqlx::query("UPDATE worker_callback_tokens SET expires_at='2000-01-01T00:00:00+00:00' WHERE task_id=?1")
                .bind(id.to_string()).execute(store.pool()).await.unwrap();
            sqlx::query(
                "UPDATE worker_tasks SET created_at='2000-01-01T00:00:00+00:00' WHERE id=?1",
            )
            .bind(id.to_string())
            .execute(store.pool())
            .await
            .unwrap();
            let expired = submit(
                server.v1_url(),
                tenant.as_str(),
                token,
                json!({"route":"late"}),
            )
            .await
            .unwrap();
            assert_eq!(expired.status(), StatusCode::NOT_FOUND);
            assert_eq!(
                orch8_engine::worker_lease::reap_worker_tasks(store.as_ref(), Duration::ZERO)
                    .await
                    .unwrap()
                    .timed_out,
                1
            );
        } else {
            // A logical recovery carries progress and callback mode, then issues
            // independent callback authority for a worker's new claim.
            use orch8_types::worker::WorkerClaim;
            use std::io::Read as _;

            let old_claim =
                WorkerClaim::new("dispatcher", offered["claim_epoch"].as_u64().unwrap());
            let progress = json!({"dispatched_message": "job-7"});
            assert_eq!(
                store
                    .checkpoint_worker_task(id, &old_claim, 0, &progress)
                    .await
                    .unwrap(),
                Some(1),
            );
            sandbox.run_ok(&[
                "backup",
                "--database-url",
                "callback.db",
                "--out",
                "callback.tar.gz",
                "--include-instances",
            ]);
            let archive = std::fs::File::open(sandbox.root.join("callback.tar.gz")).unwrap();
            let mut expanded = Vec::new();
            flate2::read::GzDecoder::new(archive)
                .read_to_end(&mut expanded)
                .unwrap();
            assert!(
                !expanded
                    .windows(token.len())
                    .any(|chunk| chunk == token.as_bytes())
            );

            sandbox.run_ok(&[
                "--yes",
                "restore",
                "callback.tar.gz",
                "--database-url",
                "restored.db",
                "--include-instances",
            ]);
            let restored_db = sandbox.root.join("restored.db");
            let restored_store = Arc::new(
                SqliteStorage::file_path(restored_db.to_str().unwrap())
                    .await
                    .unwrap(),
            );
            let restored_api =
                orch8_api::test_harness::spawn_test_server_on(restored_store, None).await;
            let fresh = poll_one(restored_api.v1_url()).await;
            assert_ne!(fresh["id"], offered["id"]);
            assert_eq!(fresh["wait_for_callback"], true);
            assert_eq!(fresh["resume_checkpoint"], progress);
            assert_eq!(fresh["checkpoint_seq"], 1);
            let fresh_token = fresh["callback_token"].as_str().unwrap();
            assert_ne!(fresh_token, token);
            assert_eq!(fresh["params"]["items"][0]["token"], fresh_token);
            assert_eq!(
                fresh["params"]["items"][0]["message/parts"],
                json!([format!("ticket:{fresh_token}"), {"ticket":fresh_token}])
            );
            let stale = submit(
                restored_api.v1_url(),
                tenant.as_str(),
                token,
                json!({"stale": true}),
            )
            .await
            .unwrap();
            assert_eq!(stale.status(), StatusCode::NOT_FOUND);
            let completed = submit(
                restored_api.v1_url(),
                tenant.as_str(),
                fresh_token,
                json!({"restored": true}),
            )
            .await
            .unwrap();
            assert_eq!(completed.status(), StatusCode::OK);
            drop(restored_api);

            drop(server);
            server = orch8_api::test_harness::spawn_test_server_on(open().await, None).await;
            let foreign = submit(
                server.v1_url(),
                "other-tenant",
                token,
                json!({"route":"wrong"}),
            )
            .await
            .unwrap();
            assert_eq!(foreign.status(), StatusCode::NOT_FOUND);
            let accepted = submit(
                server.v1_url(),
                tenant.as_str(),
                token,
                json!({"route":"accepted"}),
            )
            .await
            .unwrap();
            assert_eq!(accepted.status(), StatusCode::OK);
            let duplicate = submit(
                server.v1_url(),
                tenant.as_str(),
                token,
                json!({"route":"duplicate"}),
            )
            .await
            .unwrap();
            assert_eq!(duplicate.status(), StatusCode::OK);
        }
        for _ in 0..15 {
            engine.tick_once().await.unwrap();
            if engine
                .get_instance(instance)
                .await
                .unwrap()
                .state
                .is_terminal()
            {
                break;
            }
        }
        assert_eq!(
            engine.get_instance(instance).await.unwrap().state,
            InstanceState::Completed
        );
        let outputs = engine.block_outputs(instance).await.unwrap();
        let routes: Vec<_> = outputs
            .iter()
            .filter_map(|out| out.output["route"].as_str())
            .collect();
        assert_eq!(routes, vec![if expires { "timeout" } else { "accepted" }]);
    }
    drop(server);
    store.close().await;
    engine.close().await;
}
