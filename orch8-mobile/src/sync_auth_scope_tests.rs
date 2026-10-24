use super::*;

#[test]
fn normalized_https_origin_is_the_bearer_authority() {
    let auth = SyncAuth::Bearer("manifest_secret".into());
    let scoped = auth
        .for_manifest("https://API.EXAMPLE:443/root?manifest_token=private#fragment")
        .unwrap();
    for (target, expected) in [
        ("https://api.example/seq.json", true),
        ("https://api.example:443/seq.sig?entry_token=private", true),
        ("https://API.EXAMPLE/other/path#fragment", true),
        ("https://api.example/path/https://foreign.example", true),
        ("http://api.example/seq.json", false),
        ("https://api.example:444/seq.json", false),
        ("https://api.example.evil.test/seq.json", false),
        ("https://child.api.example/seq.json", false),
        ("https://api.example@foreign.example/seq.json", false),
    ] {
        let target = reqwest::Url::parse(target).unwrap();
        assert_eq!(scoped.bearer_for(&target).is_some(), expected, "{target}");
    }
}

#[test]
fn auth_scope_errors_do_not_include_provider_or_url_secrets() {
    for token in ["", " ", "\t\r\n"] {
        let auth = SyncAuth::Bearer(token.into());
        assert!(matches!(
            auth.for_manifest("https://api.example/manifest"),
            Err(MobileError::InvalidInput { .. })
        ));
    }
    let auth = SyncAuth::Bearer("private_provider_token".into());
    let error = auth
        .for_manifest("not a URL?private_signed_token")
        .err()
        .unwrap()
        .to_string();
    assert!(!error.contains("private_provider_token") && !error.contains("private_signed_token"));
}

#[test]
fn retry_diagnostics_do_not_reveal_url_authority_or_fragments() {
    for url in [
        "https://user:password@cdn.example/seq.json#private_fragment",
        "https://cdn.example/seq.json#private_fragment",
        "not-a-valid-url#private_fragment",
    ] {
        let safe = redacted_url(url);
        for secret in ["user:", "password", "private_fragment"] {
            assert!(
                !safe.contains(secret),
                "diagnostic URL retained a credential component"
            );
        }
    }
}
