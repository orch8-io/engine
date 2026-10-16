//! Fleet contexts: names are 1-64 ASCII `[A-Za-z0-9_-]`, URLs must be absolute
//! http(s), an explicit `--context` beats the stored selection, a missing
//! context is an error (never a silent fallback), and the credentials file is
//! owner-only — a group/world-accessible file fails closed.

use super::*;

fn set_cmd(name: &str, url: &str, tenant: &str, key: &str) -> ContextCmd {
    ContextCmd::Set {
        name: name.into(),
        url: url.into(),
        tenant_id: tenant.into(),
        api_key: key.into(),
    }
}

fn use_cmd(name: &str) -> ContextCmd {
    ContextCmd::Use { name: name.into() }
}

fn remove_cmd(name: &str) -> ContextCmd {
    ContextCmd::Remove { name: name.into() }
}

fn seed_store(path: &Path) {
    run(
        path,
        set_cmd(
            "prod",
            "https://prod.example.com/api/v1",
            "tenant-prod",
            "prod-key",
        ),
    )
    .unwrap();
    run(
        path,
        set_cmd(
            "staging",
            "https://staging.example.com/api/v1",
            "tenant-stg",
            "stg-key",
        ),
    )
    .unwrap();
}

fn store_path(dir: &tempfile::TempDir) -> PathBuf {
    dir.path().join("contexts.json")
}

#[cfg(unix)]
fn chmod(path: &Path, mode: u32) {
    use std::os::unix::fs::PermissionsExt as _;
    std::fs::set_permissions(path, std::fs::Permissions::from_mode(mode)).unwrap();
}

#[cfg(not(unix))]
fn chmod(_path: &Path, _mode: u32) {}

#[test]
fn context_names_are_bounded_ascii_identifiers() {
    let long_ok = "a".repeat(64);
    let too_long = "a".repeat(65);
    let cases: [(&str, bool); 14] = [
        ("", false),
        ("a", true),
        (&long_ok, true),
        (&too_long, false),
        ("eu-west", true),
        ("prod_eu", true),
        ("12345", true),
        ("Prod-EU_1", true),
        ("prod eu", false),
        ("prod.eu", false),
        ("prod/eu", false),
        ("prodüction", false),
        ("prod\neu", false),
        ("prod@eu", false),
    ];
    for (name, valid) in cases {
        assert_eq!(validate_name(name).is_ok(), valid, "case {name:?}");
    }
}

#[test]
fn context_requires_absolute_http_url_tenant_and_key() {
    let cases = [
        ("https://engine.example.com/api/v1", "tenant-a", "key", true),
        ("http://127.0.0.1:8080/api/v1", "tenant-a", "key", true),
        (
            "https://engine.example.com:8443/api/v1",
            "tenant-a",
            "key",
            true,
        ),
        (
            "https://engine.example.com/api/v1/",
            "tenant-a",
            "key",
            true,
        ),
        ("ftp://engine.example.com", "tenant-a", "key", false),
        ("wss://engine.example.com", "tenant-a", "key", false),
        ("engine.example.com/api/v1", "tenant-a", "key", false),
        ("https://", "tenant-a", "key", false),
        ("https://engine example.com", "tenant-a", "key", false),
        ("", "tenant-a", "key", false),
        ("https://engine.example.com", "", "key", false),
        ("https://engine.example.com", "tenant-a", "", false),
    ];
    for (url, tenant, key, valid) in cases {
        assert_eq!(
            validate_context(url, tenant, key).is_ok(),
            valid,
            "case url={url:?} tenant={tenant:?} key={key:?}"
        );
    }
}

#[test]
fn trailing_slashes_are_trimmed_from_saved_url() {
    let dir = tempfile::tempdir().unwrap();
    let path = store_path(&dir);

    run(
        &path,
        set_cmd("prod", "https://prod.example.com/api/v1///", "t", "k"),
    )
    .unwrap();

    assert_eq!(
        load(&path).unwrap().contexts["prod"].url,
        "https://prod.example.com/api/v1"
    );
}

#[test]
fn set_replaces_an_existing_context_of_the_same_name() {
    let dir = tempfile::tempdir().unwrap();
    let path = store_path(&dir);
    run(
        &path,
        set_cmd("prod", "https://old.example.com", "t1", "k1"),
    )
    .unwrap();

    run(
        &path,
        set_cmd("prod", "https://new.example.com", "t2", "k2"),
    )
    .unwrap();

    let store = load(&path).unwrap();
    assert_eq!(store.contexts.len(), 1);
    assert_eq!(store.contexts["prod"].url, "https://new.example.com");
    assert_eq!(store.contexts["prod"].tenant_id, "t2");
}

#[test]
fn use_selects_the_named_context() {
    let dir = tempfile::tempdir().unwrap();
    let path = store_path(&dir);
    seed_store(&path);

    run(&path, use_cmd("staging")).unwrap();

    assert_eq!(load(&path).unwrap().selected.as_deref(), Some("staging"));
}

#[test]
fn use_of_a_missing_context_fails_without_changing_the_selection() {
    let dir = tempfile::tempdir().unwrap();
    let path = store_path(&dir);
    seed_store(&path);

    let error = run(&path, use_cmd("ghost")).unwrap_err();

    assert_eq!(error.to_string(), "fleet context 'ghost' does not exist");
    assert_eq!(load(&path).unwrap().selected, None);
}

#[test]
fn remove_of_a_missing_context_fails_and_keeps_the_others() {
    let dir = tempfile::tempdir().unwrap();
    let path = store_path(&dir);
    seed_store(&path);

    let error = run(&path, remove_cmd("ghost")).unwrap_err();

    assert_eq!(error.to_string(), "fleet context 'ghost' does not exist");
    assert_eq!(load(&path).unwrap().contexts.len(), 2);
}

#[test]
fn removing_the_selected_context_clears_the_selection() {
    let dir = tempfile::tempdir().unwrap();
    let path = store_path(&dir);
    seed_store(&path);
    run(&path, use_cmd("prod")).unwrap();

    run(&path, remove_cmd("prod")).unwrap();

    let store = load(&path).unwrap();
    assert_eq!(store.selected, None);
    assert!(!store.contexts.contains_key("prod"));
    assert!(store.contexts.contains_key("staging"));
}

#[test]
fn removing_another_context_keeps_the_selection() {
    let dir = tempfile::tempdir().unwrap();
    let path = store_path(&dir);
    seed_store(&path);
    run(&path, use_cmd("prod")).unwrap();

    run(&path, remove_cmd("staging")).unwrap();

    assert_eq!(load(&path).unwrap().selected.as_deref(), Some("prod"));
}

#[test]
fn removing_the_only_context_leaves_nothing_to_resolve() {
    let dir = tempfile::tempdir().unwrap();
    let path = store_path(&dir);
    run(
        &path,
        set_cmd("prod", "https://prod.example.com/api/v1", "t", "k"),
    )
    .unwrap();
    run(&path, use_cmd("prod")).unwrap();

    run(&path, remove_cmd("prod")).unwrap();

    assert!(load(&path).unwrap() == ContextStore::default());
    assert!(resolve(&path, None).unwrap().is_none());
}

#[test]
fn explicit_context_overrides_the_selection() {
    let dir = tempfile::tempdir().unwrap();
    let path = store_path(&dir);
    seed_store(&path);
    run(&path, use_cmd("prod")).unwrap();

    let resolved = resolve(&path, Some("staging")).unwrap().unwrap();

    assert_eq!(resolved.tenant_id, "tenant-stg");
}

#[test]
fn explicit_missing_or_empty_context_name_is_an_error() {
    let dir = tempfile::tempdir().unwrap();
    let path = store_path(&dir);
    seed_store(&path);
    for name in ["ghost", ""] {
        let error = resolve(&path, Some(name)).err().expect("must fail");
        assert_eq!(
            error.to_string(),
            format!("fleet context '{name}' does not exist"),
            "case {name:?}"
        );
    }
}

#[test]
fn stale_selection_pointing_at_a_removed_context_is_an_error() {
    let dir = tempfile::tempdir().unwrap();
    let path = store_path(&dir);
    let store = ContextStore {
        selected: Some("ghost".into()),
        contexts: BTreeMap::new(),
    };
    std::fs::write(&path, serde_json::to_vec_pretty(&store).unwrap()).unwrap();
    chmod(&path, 0o600);

    let error = resolve(&path, None).err().expect("must fail");

    assert_eq!(error.to_string(), "fleet context 'ghost' does not exist");
}

#[test]
fn no_selection_resolves_to_no_context() {
    let dir = tempfile::tempdir().unwrap();
    let path = store_path(&dir);
    seed_store(&path);

    assert!(resolve(&path, None).unwrap().is_none());
}

#[test]
fn missing_file_resolves_to_none_but_a_named_context_still_errors() {
    let dir = tempfile::tempdir().unwrap();
    let path = store_path(&dir);

    assert!(resolve(&path, None).unwrap().is_none());
    let error = resolve(&path, Some("prod")).err().expect("must fail");
    assert_eq!(error.to_string(), "fleet context 'prod' does not exist");
}

#[test]
fn corrupt_file_fails_naming_the_file() {
    let dir = tempfile::tempdir().unwrap();
    let path = store_path(&dir);
    std::fs::write(&path, b"{not json").unwrap();
    chmod(&path, 0o600);

    let error = load(&path).err().expect("must fail");

    assert!(
        format!("{error:#}").contains(&format!("parse {}", path.display())),
        "{error:#}"
    );
}

#[test]
fn save_creates_missing_parent_directories() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir
        .path()
        .join("nested")
        .join("deeper")
        .join("contexts.json");

    run(&path, set_cmd("prod", "https://prod.example.com", "t", "k")).unwrap();

    assert_eq!(load(&path).unwrap().contexts["prod"].tenant_id, "t");
}

#[cfg(unix)]
#[test]
fn group_or_world_accessible_store_fails_closed() {
    for mode in [0o640, 0o604] {
        let dir = tempfile::tempdir().unwrap();
        let path = store_path(&dir);
        std::fs::write(&path, b"{}").unwrap();
        chmod(&path, mode);

        let error = load(&path).err().expect("must fail");

        assert!(
            format!("{error:#}").contains("must not be accessible by group/others"),
            "mode {mode:o}: {error:#}"
        );
    }
}

#[cfg(unix)]
#[test]
fn owner_only_store_loads() {
    let dir = tempfile::tempdir().unwrap();
    let path = store_path(&dir);
    std::fs::write(&path, br#"{"selected": null, "contexts": {}}"#).unwrap();
    chmod(&path, 0o600);

    assert!(load(&path).unwrap() == ContextStore::default());
}

#[test]
fn invalid_set_writes_no_file() {
    let cases = [
        (
            set_cmd("bad name", "https://prod.example.com", "t", "k"),
            "context name",
        ),
        (
            set_cmd("prod", "not-a-url", "t", "k"),
            "context URL is invalid",
        ),
    ];
    for (cmd, expected) in cases {
        let dir = tempfile::tempdir().unwrap();
        let path = store_path(&dir);

        let error = run(&path, cmd).unwrap_err();

        assert!(format!("{error:#}").contains(expected), "{error:#}");
        assert!(!path.exists(), "case {expected}");
    }
}

#[test]
fn listing_marks_the_selection_and_never_includes_credentials() {
    let dir = tempfile::tempdir().unwrap();
    let path = store_path(&dir);
    seed_store(&path);
    run(&path, use_cmd("staging")).unwrap();

    let lines = listing(&load(&path).unwrap());

    assert_eq!(lines, ["  prod", "* staging"]);
    let joined = lines.join("\n");
    for secret in ["prod-key", "stg-key", "tenant-prod", "example.com"] {
        assert!(!joined.contains(secret), "listing leaked {secret}");
    }
}

#[test]
fn explicit_contexts_file_beats_xdg_and_legacy_locations() {
    let dir = tempfile::tempdir().unwrap();
    let legacy = dir.path().join("legacy.json");
    std::fs::write(&legacy, b"{}").unwrap();

    let path = select_path(
        Some("/explicit/contexts.json".into()),
        Some(dir.path().as_os_str()),
        None,
        &legacy,
    );

    assert_eq!(path, PathBuf::from("/explicit/contexts.json"));
}

#[test]
fn existing_xdg_file_beats_the_legacy_file() {
    let dir = tempfile::tempdir().unwrap();
    let xdg = dir.path().join("orch8").join("contexts.json");
    std::fs::create_dir_all(xdg.parent().unwrap()).unwrap();
    std::fs::write(&xdg, b"{}").unwrap();
    let legacy = dir.path().join("legacy.json");
    std::fs::write(&legacy, b"{}").unwrap();

    let path = select_path(None, Some(dir.path().as_os_str()), None, &legacy);

    assert_eq!(path, xdg);
}

#[test]
fn legacy_file_is_used_only_while_no_xdg_file_exists() {
    let dir = tempfile::tempdir().unwrap();
    let xdg = dir.path().join("orch8").join("contexts.json");
    let legacy = dir.path().join("legacy.json");

    let without_legacy = select_path(None, Some(dir.path().as_os_str()), None, &legacy);
    std::fs::write(&legacy, b"{}").unwrap();
    let with_legacy = select_path(None, Some(dir.path().as_os_str()), None, &legacy);

    assert_eq!(without_legacy, xdg);
    assert_eq!(with_legacy, legacy);
}

#[test]
fn no_xdg_or_home_falls_back_to_the_legacy_location() {
    let legacy = Path::new("/nonexistent/legacy.json");

    assert_eq!(select_path(None, None, None, legacy), legacy);
}
