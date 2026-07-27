//! Coverage tests for named fleet contexts: validation, selection, and
//! secure persistence.
//!
//! Count contract: 53 independently named unit tests.

use super::*;

macro_rules! name_case {
    ($name:ident, $value:expr, $valid:expr) => {
        #[test]
        fn $name() {
            assert_eq!(validate_name(&$value).is_ok(), $valid);
        }
    };
}

name_case!(coverage_context_001_empty_name_is_rejected, "", false);
name_case!(
    coverage_context_002_single_letter_name_is_allowed,
    "a",
    true
);
name_case!(
    coverage_context_003_64_byte_name_is_allowed,
    "a".repeat(64),
    true
);
name_case!(
    coverage_context_004_65_byte_name_is_rejected,
    "a".repeat(65),
    false
);
name_case!(coverage_context_005_hyphen_name_is_allowed, "eu-west", true);
name_case!(
    coverage_context_006_underscore_name_is_allowed,
    "prod_eu",
    true
);
name_case!(coverage_context_007_digit_name_is_allowed, "12345", true);
name_case!(
    coverage_context_008_mixed_name_is_allowed,
    "Prod-EU_1",
    true
);
name_case!(
    coverage_context_009_space_name_is_rejected,
    "prod eu",
    false
);
name_case!(coverage_context_010_dot_name_is_rejected, "prod.eu", false);
name_case!(
    coverage_context_011_slash_name_is_rejected,
    "prod/eu",
    false
);
name_case!(
    coverage_context_012_unicode_name_is_rejected,
    "prodüction",
    false
);
name_case!(
    coverage_context_013_newline_name_is_rejected,
    "prod\neu",
    false
);
name_case!(coverage_context_014_at_name_is_rejected, "prod@eu", false);

macro_rules! context_case {
    ($name:ident, $url:expr, $tenant:expr, $key:expr, $valid:expr) => {
        #[test]
        fn $name() {
            assert_eq!(validate_context($url, $tenant, $key).is_ok(), $valid);
        }
    };
}

context_case!(
    coverage_context_015_https_url_is_allowed,
    "https://engine.example.com/api/v1",
    "tenant-a",
    "key",
    true
);
context_case!(
    coverage_context_016_http_url_is_allowed,
    "http://127.0.0.1:8080/api/v1",
    "tenant-a",
    "key",
    true
);
context_case!(
    coverage_context_017_ftp_scheme_is_rejected,
    "ftp://engine.example.com",
    "tenant-a",
    "key",
    false
);
context_case!(
    coverage_context_018_relative_url_is_rejected,
    "engine.example.com/api/v1",
    "tenant-a",
    "key",
    false
);
context_case!(
    coverage_context_019_hostless_url_is_rejected,
    "https://",
    "tenant-a",
    "key",
    false
);
context_case!(
    coverage_context_020_empty_tenant_is_rejected,
    "https://engine.example.com",
    "",
    "key",
    false
);
context_case!(
    coverage_context_021_empty_api_key_is_rejected,
    "https://engine.example.com",
    "tenant-a",
    "",
    false
);
context_case!(
    coverage_context_022_url_with_port_is_allowed,
    "https://engine.example.com:8443/api/v1",
    "tenant-a",
    "key",
    true
);
context_case!(
    coverage_context_023_url_with_trailing_slash_is_allowed,
    "https://engine.example.com/api/v1/",
    "tenant-a",
    "key",
    true
);
context_case!(
    coverage_context_024_url_with_embedded_space_is_rejected,
    "https://engine example.com",
    "tenant-a",
    "key",
    false
);
context_case!(
    coverage_context_025_empty_url_is_rejected,
    "",
    "tenant-a",
    "key",
    false
);
context_case!(
    coverage_context_026_websocket_scheme_is_rejected,
    "wss://engine.example.com",
    "tenant-a",
    "key",
    false
);

fn set_cmd(name: &str, url: &str, tenant: &str, key: &str) -> ContextCmd {
    ContextCmd::Set {
        name: name.into(),
        url: url.into(),
        tenant_id: tenant.into(),
        api_key: key.into(),
    }
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

#[test]
fn coverage_context_027_set_then_resolve_returns_full_context() {
    let dir = tempfile::tempdir().unwrap();
    let path = store_path(&dir);
    run(
        &path,
        set_cmd("prod", "https://prod.example.com/api/v1", "t", "k"),
    )
    .unwrap();
    run(
        &path,
        ContextCmd::Use {
            name: "prod".into(),
        },
    )
    .unwrap();
    let resolved = resolve(&path, None).unwrap().unwrap();
    assert_eq!(resolved.url, "https://prod.example.com/api/v1");
    assert_eq!(resolved.tenant_id, "t");
    assert_eq!(resolved.api_key, "k");
}

#[test]
fn coverage_context_028_trailing_slashes_are_trimmed_from_url() {
    let dir = tempfile::tempdir().unwrap();
    let path = store_path(&dir);
    run(
        &path,
        set_cmd("prod", "https://prod.example.com/api/v1///", "t", "k"),
    )
    .unwrap();
    let store = load(&path).unwrap();
    assert_eq!(
        store.contexts["prod"].url,
        "https://prod.example.com/api/v1"
    );
}

#[test]
fn coverage_context_029_set_replaces_existing_context() {
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
fn coverage_context_030_use_selects_the_named_context() {
    let dir = tempfile::tempdir().unwrap();
    let path = store_path(&dir);
    seed_store(&path);
    run(
        &path,
        ContextCmd::Use {
            name: "staging".into(),
        },
    )
    .unwrap();
    assert_eq!(load(&path).unwrap().selected.as_deref(), Some("staging"));
}

#[test]
fn coverage_context_031_use_missing_context_fails_without_changes() {
    let dir = tempfile::tempdir().unwrap();
    let path = store_path(&dir);
    seed_store(&path);
    let error = run(
        &path,
        ContextCmd::Use {
            name: "ghost".into(),
        },
    )
    .unwrap_err();
    assert!(
        error
            .to_string()
            .contains("fleet context 'ghost' does not exist"),
        "{error:#}"
    );
    assert_eq!(load(&path).unwrap().selected, None);
}

#[test]
fn coverage_context_032_remove_missing_context_fails() {
    let dir = tempfile::tempdir().unwrap();
    let path = store_path(&dir);
    seed_store(&path);
    let error = run(
        &path,
        ContextCmd::Remove {
            name: "ghost".into(),
        },
    )
    .unwrap_err();
    assert!(
        error
            .to_string()
            .contains("fleet context 'ghost' does not exist"),
        "{error:#}"
    );
    assert_eq!(load(&path).unwrap().contexts.len(), 2);
}

#[test]
fn coverage_context_033_removing_selected_context_clears_selection() {
    let dir = tempfile::tempdir().unwrap();
    let path = store_path(&dir);
    seed_store(&path);
    run(
        &path,
        ContextCmd::Use {
            name: "prod".into(),
        },
    )
    .unwrap();
    run(
        &path,
        ContextCmd::Remove {
            name: "prod".into(),
        },
    )
    .unwrap();
    let store = load(&path).unwrap();
    assert_eq!(store.selected, None);
    assert!(!store.contexts.contains_key("prod"));
    assert!(store.contexts.contains_key("staging"));
}

#[test]
fn coverage_context_034_removing_other_context_keeps_selection() {
    let dir = tempfile::tempdir().unwrap();
    let path = store_path(&dir);
    seed_store(&path);
    run(
        &path,
        ContextCmd::Use {
            name: "prod".into(),
        },
    )
    .unwrap();
    run(
        &path,
        ContextCmd::Remove {
            name: "staging".into(),
        },
    )
    .unwrap();
    assert_eq!(load(&path).unwrap().selected.as_deref(), Some("prod"));
}

#[test]
fn coverage_context_035_explicit_name_overrides_selection() {
    let dir = tempfile::tempdir().unwrap();
    let path = store_path(&dir);
    seed_store(&path);
    run(
        &path,
        ContextCmd::Use {
            name: "prod".into(),
        },
    )
    .unwrap();
    let resolved = resolve(&path, Some("staging")).unwrap().unwrap();
    assert_eq!(resolved.tenant_id, "tenant-stg");
}

#[test]
fn coverage_context_036_explicit_missing_context_is_an_error() {
    let dir = tempfile::tempdir().unwrap();
    let path = store_path(&dir);
    seed_store(&path);
    let error = resolve(&path, Some("ghost"))
        .err()
        .expect("resolve must fail");
    assert!(
        error
            .to_string()
            .contains("fleet context 'ghost' does not exist"),
        "{error:#}"
    );
}

#[test]
fn coverage_context_037_selected_missing_context_is_an_error() {
    // A store whose selection points at a name that no longer exists can
    // only be constructed by editing the file — resolve must still fail.
    let dir = tempfile::tempdir().unwrap();
    let path = store_path(&dir);
    let store = ContextStore {
        selected: Some("ghost".into()),
        contexts: BTreeMap::new(),
    };
    std::fs::write(&path, serde_json::to_vec_pretty(&store).unwrap()).unwrap();
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt as _;
        std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o600)).unwrap();
    }
    let error = resolve(&path, None).err().expect("resolve must fail");
    assert!(
        error
            .to_string()
            .contains("fleet context 'ghost' does not exist"),
        "{error:#}"
    );
}

#[test]
fn coverage_context_038_no_selection_resolves_to_none() {
    let dir = tempfile::tempdir().unwrap();
    let path = store_path(&dir);
    seed_store(&path);
    assert!(resolve(&path, None).unwrap().is_none());
}

#[test]
fn coverage_context_039_missing_file_resolves_to_none() {
    let dir = tempfile::tempdir().unwrap();
    let path = store_path(&dir);
    assert!(resolve(&path, None).unwrap().is_none());
    let error = resolve(&path, Some("prod"))
        .err()
        .expect("resolve must fail");
    assert_eq!(error.to_string(), "fleet context 'prod' does not exist");
}

#[test]
fn coverage_context_040_missing_file_loads_as_empty_store() {
    let dir = tempfile::tempdir().unwrap();
    let store = load(&store_path(&dir)).unwrap();
    assert!(store == ContextStore::default());
}

#[test]
fn coverage_context_041_corrupt_file_is_a_parse_error() {
    let dir = tempfile::tempdir().unwrap();
    let path = store_path(&dir);
    std::fs::write(&path, b"{not json").unwrap();
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt as _;
        std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o600)).unwrap();
    }
    let error = load(&path).err().expect("load must fail");
    assert!(format!("{error:#}").contains("parse"), "{error:#}");
}

#[test]
fn coverage_context_042_save_creates_missing_parent_directories() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir
        .path()
        .join("nested")
        .join("deeper")
        .join("contexts.json");
    run(&path, set_cmd("prod", "https://prod.example.com", "t", "k")).unwrap();
    assert!(path.exists());
    assert_eq!(load(&path).unwrap().contexts["prod"].tenant_id, "t");
}

#[cfg(unix)]
#[test]
fn coverage_context_043_saved_store_is_owner_only() {
    use std::os::unix::fs::PermissionsExt as _;
    let dir = tempfile::tempdir().unwrap();
    let path = store_path(&dir);
    run(&path, set_cmd("prod", "https://prod.example.com", "t", "k")).unwrap();
    assert_eq!(
        std::fs::metadata(&path).unwrap().permissions().mode() & 0o777,
        0o600
    );
}

#[cfg(unix)]
#[test]
fn coverage_context_044_group_readable_store_fails_closed() {
    use std::os::unix::fs::PermissionsExt as _;
    let dir = tempfile::tempdir().unwrap();
    let path = store_path(&dir);
    std::fs::write(&path, b"{}").unwrap();
    std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o640)).unwrap();
    let error = load(&path).err().expect("load must fail");
    assert!(
        format!("{error:#}").contains("must not be accessible by group/others"),
        "{error:#}"
    );
}

#[cfg(unix)]
#[test]
fn coverage_context_045_world_readable_store_fails_closed() {
    use std::os::unix::fs::PermissionsExt as _;
    let dir = tempfile::tempdir().unwrap();
    let path = store_path(&dir);
    std::fs::write(&path, b"{}").unwrap();
    std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o604)).unwrap();
    let error = load(&path).err().expect("load must fail");
    assert!(
        format!("{error:#}").contains("must not be accessible by group/others"),
        "{error:#}"
    );
}

#[cfg(unix)]
#[test]
fn coverage_context_046_owner_only_store_loads() {
    use std::os::unix::fs::PermissionsExt as _;
    let dir = tempfile::tempdir().unwrap();
    let path = store_path(&dir);
    std::fs::write(&path, br#"{"selected": null, "contexts": {}}"#).unwrap();
    std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o600)).unwrap();
    assert!(load(&path).unwrap() == ContextStore::default());
}

#[test]
fn coverage_context_047_set_with_invalid_name_saves_nothing() {
    let dir = tempfile::tempdir().unwrap();
    let path = store_path(&dir);
    let error = run(
        &path,
        set_cmd("bad name", "https://prod.example.com", "t", "k"),
    )
    .unwrap_err();
    assert!(error.to_string().contains("context name"), "{error:#}");
    assert!(!path.exists());
}

#[test]
fn coverage_context_048_set_with_invalid_url_saves_nothing() {
    let dir = tempfile::tempdir().unwrap();
    let path = store_path(&dir);
    let error = run(&path, set_cmd("prod", "not-a-url", "t", "k")).unwrap_err();
    assert!(
        format!("{error:#}").contains("context URL is invalid"),
        "{error:#}"
    );
    assert!(!path.exists());
}

#[test]
fn coverage_context_049_store_file_is_sorted_and_complete() {
    let dir = tempfile::tempdir().unwrap();
    let path = store_path(&dir);
    seed_store(&path);
    let raw: serde_json::Value = serde_json::from_slice(&std::fs::read(&path).unwrap()).unwrap();
    let keys: Vec<&String> = raw["contexts"].as_object().unwrap().keys().collect();
    assert_eq!(keys, ["prod", "staging"]);
}

#[test]
fn coverage_context_050_store_serde_round_trip() {
    let mut contexts = BTreeMap::new();
    contexts.insert(
        "prod".to_string(),
        FleetContext {
            url: "https://prod.example.com".into(),
            tenant_id: "t".into(),
            api_key: "k".into(),
        },
    );
    let store = ContextStore {
        selected: Some("prod".into()),
        contexts,
    };
    let bytes = serde_json::to_vec(&store).unwrap();
    assert!(serde_json::from_slice::<ContextStore>(&bytes).unwrap() == store);
}

#[test]
fn coverage_context_051_default_store_has_no_selection_or_contexts() {
    let store = ContextStore::default();
    assert_eq!(store.selected, None);
    assert!(store.contexts.is_empty());
}

#[test]
fn coverage_context_052_explicit_empty_name_is_an_error() {
    let dir = tempfile::tempdir().unwrap();
    let path = store_path(&dir);
    seed_store(&path);
    let error = resolve(&path, Some("")).err().expect("resolve must fail");
    assert!(
        error
            .to_string()
            .contains("fleet context '' does not exist"),
        "{error:#}"
    );
}

#[test]
fn coverage_context_053_removing_the_only_context_leaves_an_empty_store() {
    let dir = tempfile::tempdir().unwrap();
    let path = store_path(&dir);
    run(
        &path,
        set_cmd("prod", "https://prod.example.com/api/v1", "t", "k"),
    )
    .unwrap();
    run(
        &path,
        ContextCmd::Use {
            name: "prod".into(),
        },
    )
    .unwrap();
    run(
        &path,
        ContextCmd::Remove {
            name: "prod".into(),
        },
    )
    .unwrap();
    assert!(load(&path).unwrap() == ContextStore::default());
    assert!(resolve(&path, None).unwrap().is_none());
}
