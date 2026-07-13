//! Deep-coverage unit tests for `orch8_types::redaction`.
//!
//! Goes far beyond the module's inline tests: exhaustive matrices over
//! every sensitive key fragment, every exact key, every secret value
//! prefix, JWT shapes, plus full behavioural coverage of `redact_value`,
//! `redacted`, `redact_headers`, `redact_url`, and `safe_excerpt`.
//!
//! Where fragment-substring matching produces surprising results for
//! lookalike keys (e.g. `tokenizer` contains `token`, `secretary`
//! contains `secret`, `footprint` contains `otp`), the tests encode the
//! ACTUAL behaviour deliberately, so any future change to the matching
//! strategy shows up as an explicit test failure here.

use orch8_types::redaction::{REDACTED, RedactionPolicy};
use serde_json::{Value, json};

fn policy() -> RedactionPolicy {
    RedactionPolicy::default()
}

fn swap_case(s: &str) -> String {
    s.chars()
        .map(|c| {
            if c.is_ascii_uppercase() {
                c.to_ascii_lowercase()
            } else {
                c.to_ascii_uppercase()
            }
        })
        .collect()
}

// ===========================================================================
// is_sensitive_key: every fragment in SENSITIVE_KEY_FRAGMENTS, with the
// fragment placed bare / as a prefix / as a suffix / in the middle, plus
// uppercase and mixed-case variants.
// ===========================================================================

macro_rules! sensitive_fragment_tests {
    ($($name:ident : $frag:expr;)+) => {$(
        #[test]
        fn $name() {
            let p = policy();
            let frag: &str = $frag;
            assert!(p.is_sensitive_key(frag), "bare fragment `{frag}`");
            assert!(
                p.is_sensitive_key(&format!("{frag}_extra")),
                "fragment as prefix: `{frag}_extra`"
            );
            assert!(
                p.is_sensitive_key(&format!("user_{frag}")),
                "fragment as suffix: `user_{frag}`"
            );
            assert!(
                p.is_sensitive_key(&format!("x_{frag}_y")),
                "fragment in the middle: `x_{frag}_y`"
            );
            let upper = frag.to_ascii_uppercase();
            assert!(p.is_sensitive_key(&upper), "uppercase `{upper}`");
            let mixed: String = frag
                .chars()
                .enumerate()
                .map(|(i, c)| {
                    if i % 2 == 0 {
                        c.to_ascii_uppercase()
                    } else {
                        c
                    }
                })
                .collect();
            assert!(p.is_sensitive_key(&mixed), "mixed case `{mixed}`");
        }
    )+};
}

sensitive_fragment_tests! {
    fragment_password: "password";
    fragment_passwd: "passwd";
    fragment_secret: "secret";
    fragment_token: "token";
    fragment_api_key_underscore: "api_key";
    fragment_api_key_dash: "api-key";
    fragment_apikey: "apikey";
    fragment_authorization: "authorization";
    fragment_credential: "credential";
    fragment_private_key_underscore: "private_key";
    fragment_privatekey: "privatekey";
    fragment_access_key_underscore: "access_key";
    fragment_accesskey: "accesskey";
    fragment_client_secret: "client_secret";
    fragment_session_id: "session_id";
    fragment_cookie: "cookie";
    fragment_signature: "signature";
    fragment_signing_key: "signing_key";
    fragment_bearer: "bearer";
    fragment_refresh: "refresh";
    fragment_otp: "otp";
    fragment_pin_code: "pin_code";
}

// ===========================================================================
// is_sensitive_key: every exact key in SENSITIVE_EXACT_KEYS, incl. case.
// ===========================================================================

macro_rules! exact_key_tests {
    ($($name:ident : $key:expr;)+) => {$(
        #[test]
        fn $name() {
            let p = policy();
            let key: &str = $key;
            assert!(p.is_sensitive_key(key), "exact key `{key}`");
            let upper = key.to_ascii_uppercase();
            assert!(p.is_sensitive_key(&upper), "uppercase exact key `{upper}`");
            let mixed: String = key
                .chars()
                .enumerate()
                .map(|(i, c)| {
                    if i % 2 == 1 {
                        c.to_ascii_uppercase()
                    } else {
                        c
                    }
                })
                .collect();
            assert!(p.is_sensitive_key(&mixed), "mixed case exact key `{mixed}`");
        }
    )+};
}

exact_key_tests! {
    exact_key_auth: "auth";
    exact_key_key: "key";
    exact_key_pass: "pass";
    exact_key_pwd: "pwd";
    exact_key_jwt: "jwt";
    exact_key_sid: "sid";
}

// ===========================================================================
// is_sensitive_key: lookalikes. Some DO match (fragments are substrings)
// and some do NOT (exact keys never substring-match). Encode reality.
// ===========================================================================

#[test]
fn lookalike_author_not_sensitive() {
    // "auth" is exact-only; no fragment is contained in "author".
    assert!(!policy().is_sensitive_key("author"));
}

#[test]
fn lookalike_keyboard_not_sensitive() {
    // "key" is exact-only.
    assert!(!policy().is_sensitive_key("keyboard"));
}

#[test]
fn lookalike_monkey_not_sensitive() {
    assert!(!policy().is_sensitive_key("monkey"));
}

#[test]
fn lookalike_passport_not_sensitive() {
    // "pass" is exact-only; fragments are "password"/"passwd".
    assert!(!policy().is_sensitive_key("passport"));
}

#[test]
fn lookalike_authentic_not_sensitive() {
    assert!(!policy().is_sensitive_key("authentic"));
}

#[test]
fn lookalike_passenger_not_sensitive() {
    assert!(!policy().is_sensitive_key("passenger"));
}

#[test]
fn lookalike_tokenizer_is_sensitive() {
    // Surprising but real: "tokenizer" contains the fragment "token".
    assert!(policy().is_sensitive_key("tokenizer"));
    assert!(policy().is_sensitive_key("TOKENIZER"));
}

#[test]
fn lookalike_secretary_is_sensitive() {
    // "secretary" contains the fragment "secret".
    assert!(policy().is_sensitive_key("secretary"));
}

#[test]
fn lookalike_footprint_is_sensitive() {
    // "footprint" = fo·otp·rint — contains the fragment "otp".
    assert!(policy().is_sensitive_key("footprint"));
}

#[test]
fn lookalike_totp_is_sensitive() {
    // "totp" contains "otp" (and happens to genuinely be sensitive).
    assert!(policy().is_sensitive_key("totp"));
}

#[test]
fn lookalike_laptop_not_sensitive() {
    // "laptop" does NOT contain "otp" as a contiguous substring.
    assert!(!policy().is_sensitive_key("laptop"));
}

#[test]
fn lookalike_oauth_not_sensitive() {
    // "oauth" contains "auth" as a substring, but "auth" is an EXACT key
    // only — substring matching does not apply to the exact list.
    assert!(!policy().is_sensitive_key("oauth"));
}

#[test]
fn lookalike_keys_plural_not_sensitive() {
    assert!(!policy().is_sensitive_key("keys"));
}

#[test]
fn lookalike_passcode_not_sensitive() {
    // Neither "password" nor "passwd" is contained; "pass" is exact-only.
    assert!(!policy().is_sensitive_key("passcode"));
}

#[test]
fn lookalike_sessionid_without_underscore_not_sensitive() {
    // The fragment is "session_id" (with underscore); "sessionid" does not
    // contain it, and does not contain "sid" contiguously either.
    assert!(!policy().is_sensitive_key("sessionid"));
}

#[test]
fn lookalike_side_and_cwd_not_sensitive() {
    let p = policy();
    assert!(!p.is_sensitive_key("side")); // "sid" is exact-only
    assert!(!p.is_sensitive_key("cwd")); // not "pwd"
    assert!(!p.is_sensitive_key("pwds")); // "pwd" is exact-only
}

#[test]
fn lookalike_auth_header_not_sensitive() {
    // "auth_header" contains neither the exact key "auth" (exact match
    // required) nor the fragment "authorization".
    assert!(!policy().is_sensitive_key("auth_header"));
}

#[test]
fn lookalike_refreshing_is_sensitive_but_fresh_is_not() {
    let p = policy();
    assert!(p.is_sensitive_key("refreshing")); // contains "refresh"
    assert!(!p.is_sensitive_key("fresh"));
}

#[test]
fn lookalike_jwt_token_sensitive_via_fragment() {
    // "jwt_token" is not the exact key "jwt", but contains "token".
    assert!(policy().is_sensitive_key("jwt_token"));
}

#[test]
fn ordinary_business_keys_not_sensitive() {
    let p = policy();
    for key in [
        "email",
        "name",
        "amount",
        "url",
        "customer_id",
        "status",
        "description",
        "created_at",
        "count",
        "id",
    ] {
        assert!(!p.is_sensitive_key(key), "`{key}` must not be sensitive");
    }
}

// ===========================================================================
// protected_sections extension
// ===========================================================================

#[test]
fn protected_section_extends_policy() {
    let mut p = policy();
    p.protected_sections.push("medical_record".into());
    assert!(p.is_sensitive_key("medical_record"));
}

#[test]
fn protected_section_case_insensitive_both_directions() {
    let mut p = policy();
    p.protected_sections.push("MEDICAL_RECORD".into());
    assert!(p.is_sensitive_key("medical_record"));
    assert!(p.is_sensitive_key("Medical_Record"));
    assert!(p.is_sensitive_key("MEDICAL_RECORD"));

    let mut p2 = policy();
    p2.protected_sections.push("medical_record".into());
    assert!(p2.is_sensitive_key("MEDICAL_RECORD"));
}

#[test]
fn protected_section_matches_exactly_not_as_substring() {
    let mut p = policy();
    p.protected_sections.push("medical_record".into());
    assert!(!p.is_sensitive_key("medical_record_id"));
    assert!(!p.is_sensitive_key("my_medical_record"));
    assert!(!p.is_sensitive_key("medical"));
}

#[test]
fn protected_section_absent_from_default_policy() {
    assert!(!policy().is_sensitive_key("medical_record"));
    assert!(policy().protected_sections.is_empty());
}

#[test]
fn protected_sections_multiple_entries_all_match() {
    let mut p = policy();
    p.protected_sections.push("hr_notes".into());
    p.protected_sections.push("payroll".into());
    assert!(p.is_sensitive_key("hr_notes"));
    assert!(p.is_sensitive_key("payroll"));
    assert!(p.is_sensitive_key("PAYROLL"));
    assert!(!p.is_sensitive_key("payroll_summary"));
}

#[test]
fn protected_sections_do_not_interfere_with_builtin_policy() {
    let mut p = policy();
    p.protected_sections.push("payroll".into());
    // Built-in matching still fully applies…
    assert!(p.is_sensitive_key("password"));
    assert!(p.is_sensitive_key("auth"));
    // …and benign keys stay benign.
    assert!(!p.is_sensitive_key("email"));
    assert!(!p.is_sensitive_key("author"));
}

#[test]
fn protected_section_applies_in_redact_value_at_any_depth() {
    let mut p = policy();
    p.protected_sections.push("payroll".into());
    let mut v = json!({
        "payroll": {"salary": 100},
        "nested": {"payroll": [1, 2, 3], "ok": "x"}
    });
    p.redact_value(&mut v);
    assert_eq!(v["payroll"], REDACTED);
    assert_eq!(v["nested"]["payroll"], REDACTED);
    assert_eq!(v["nested"]["ok"], "x");
}

#[test]
fn protected_section_applies_in_redact_url() {
    let mut p = policy();
    p.protected_sections.push("payroll".into());
    assert_eq!(
        p.redact_url("https://x.com/p?payroll=42&other=1"),
        format!("https://x.com/p?payroll={REDACTED}&other=1")
    );
}

#[test]
fn protected_section_applies_in_redact_headers() {
    let mut p = policy();
    p.protected_sections.push("x-payroll".into());
    let out = p.redact_headers(vec![("X-Payroll", "42"), ("X-Other", "1")]);
    assert_eq!(out[0].1, REDACTED);
    assert_eq!(out[1].1, "1");
}

// ===========================================================================
// is_secret_shaped: every prefix in SECRET_VALUE_PREFIXES.
// Prefixes are checked case-SENSITIVELY and only at the very start.
// ===========================================================================

macro_rules! secret_prefix_tests {
    ($($name:ident : $prefix:expr;)+) => {$(
        #[test]
        fn $name() {
            let p = policy();
            let prefix: &str = $prefix;
            assert!(
                p.is_secret_shaped(&format!("{prefix}abc123XYZ")),
                "`{prefix}abc123XYZ` should be secret-shaped"
            );
            assert!(
                p.is_secret_shaped(prefix),
                "the bare prefix `{prefix}` itself is secret-shaped"
            );
            assert!(
                !p.is_secret_shaped(&format!("x{prefix}abc123")),
                "prefix not at the start must not match"
            );
            assert!(
                !p.is_secret_shaped(&format!(" {prefix}abc123")),
                "leading whitespace defeats the prefix match"
            );
            let swapped = swap_case(prefix);
            assert!(
                !p.is_secret_shaped(&format!("{swapped}abc123")),
                "prefix matching is case-sensitive: `{swapped}` must not match"
            );
        }
    )+};
}

secret_prefix_tests! {
    prefix_stripe_sk_live: "sk_live_";
    prefix_stripe_sk_test: "sk_test_";
    prefix_stripe_rk_live: "rk_live_";
    prefix_stripe_rk_test: "rk_test_";
    prefix_github_ghp: "ghp_";
    prefix_github_gho: "gho_";
    prefix_github_ghu: "ghu_";
    prefix_github_ghs: "ghs_";
    prefix_github_pat: "github_pat_";
    prefix_slack_xoxb: "xoxb-";
    prefix_slack_xoxp: "xoxp-";
    prefix_slack_xoxa: "xoxa-";
    prefix_slack_xoxs: "xoxs-";
    prefix_aws_akia: "AKIA";
    prefix_anthropic_sk_ant: "sk-ant-";
    prefix_openai_sk_proj: "sk-proj-";
    prefix_gitlab_glpat: "glpat-";
    prefix_bearer_with_space: "Bearer ";
}

// ===========================================================================
// is_secret_shaped: JWT shapes.
// ===========================================================================

#[test]
fn jwt_three_dot_segments_is_secret() {
    let p = policy();
    assert!(p.is_secret_shaped("eyJhbGciOiJIUzI1NiJ9.eyJzdWIiOiIxIn0.sig"));
    assert!(p.is_secret_shaped("eyJa.eyJb.c"));
}

#[test]
fn jwt_two_dot_segments_not_secret() {
    // Only exactly three dot-separated segments count as a JWT.
    assert!(!policy().is_secret_shaped("eyJhbGci.eyJzdWIi"));
}

#[test]
fn jwt_four_dot_segments_not_secret() {
    assert!(!policy().is_secret_shaped("eyJa.b.c.d"));
}

#[test]
fn jwt_eyj_prefix_without_dots_not_secret() {
    assert!(!policy().is_secret_shaped("eyJhbGciOiJIUzI1NiJ9"));
}

#[test]
fn jwt_three_segments_without_eyj_prefix_not_secret() {
    assert!(!policy().is_secret_shaped("abc.def.ghi"));
}

#[test]
fn jwt_eyj_with_empty_segments_is_secret() {
    // "eyJ.." splits into ["eyJ", "", ""] — three segments, so the shape
    // check fires even though the trailing segments are empty. Encode it.
    assert!(policy().is_secret_shaped("eyJ.."));
}

#[test]
fn bearer_without_trailing_space_not_secret() {
    // The prefix is "Bearer " (with a space); the bare word is not shaped.
    assert!(!policy().is_secret_shaped("Bearer"));
    assert!(!policy().is_secret_shaped("Bearer"));
}

#[test]
fn empty_string_not_secret_shaped() {
    assert!(!policy().is_secret_shaped(""));
}

#[test]
fn ordinary_values_not_secret_shaped() {
    let p = policy();
    for v in [
        "hello world",
        "customer-42",
        "https://example.com",
        "eyJ but not a jwt",
        "sk_livid_", // not a real prefix
        "akia-lowercase",
        "gh_notoken",
    ] {
        assert!(!p.is_secret_shaped(v), "`{v}` must not be secret-shaped");
    }
}

// ===========================================================================
// redact_value
// ===========================================================================

#[test]
fn redact_value_five_levels_deep() {
    let mut v = json!({
        "l1": {"l2": {"l3": {"l4": {"l5": {"password": "x", "ok": 1}}}}}
    });
    policy().redact_value(&mut v);
    assert_eq!(v["l1"]["l2"]["l3"]["l4"]["l5"]["password"], REDACTED);
    assert_eq!(v["l1"]["l2"]["l3"]["l4"]["l5"]["ok"], 1);
}

#[test]
fn redact_value_arrays_of_objects() {
    let mut v = json!([
        {"token": "a", "n": 1},
        {"token": "b", "n": 2},
        {"safe": "c"}
    ]);
    policy().redact_value(&mut v);
    assert_eq!(v[0]["token"], REDACTED);
    assert_eq!(v[1]["token"], REDACTED);
    assert_eq!(v[0]["n"], 1);
    assert_eq!(v[2]["safe"], "c");
}

#[test]
fn redact_value_nested_arrays() {
    let mut v = json!({"list": [[{"secret": 1}], ["sk_live_x", "fine"]]});
    policy().redact_value(&mut v);
    assert_eq!(v["list"][0][0]["secret"], REDACTED);
    assert_eq!(v["list"][1][0], REDACTED);
    assert_eq!(v["list"][1][1], "fine");
}

#[test]
fn redact_value_sensitive_key_replaces_whole_object_subtree() {
    let mut v = json!({"credentials": {"user": "a", "nested": {"deep": "b"}}});
    policy().redact_value(&mut v);
    // The whole subtree collapses to a single string, not per-field.
    assert_eq!(v["credentials"], Value::String(REDACTED.to_string()));
}

#[test]
fn redact_value_sensitive_key_replaces_array_value() {
    let mut v = json!({"pin_code": [1, 2, 3, 4]});
    policy().redact_value(&mut v);
    assert_eq!(v["pin_code"], REDACTED);
}

#[test]
fn redact_value_sensitive_number_replaced() {
    let mut v = json!({"otp": 123_456});
    policy().redact_value(&mut v);
    assert_eq!(v["otp"], REDACTED);
}

#[test]
fn redact_value_sensitive_bool_replaced() {
    let mut v = json!({"secret": true});
    policy().redact_value(&mut v);
    assert_eq!(v["secret"], REDACTED);
}

#[test]
fn redact_value_sensitive_null_replaced() {
    // Even a null under a sensitive key is replaced with the marker — the
    // policy never confirms absence/presence distinctions.
    let mut v = json!({"password": null});
    policy().redact_value(&mut v);
    assert_eq!(v["password"], REDACTED);
}

#[test]
fn redact_value_root_scalar_secret_shaped_string_replaced() {
    let mut v = Value::String("sk_live_topsecret".into());
    policy().redact_value(&mut v);
    assert_eq!(v, Value::String(REDACTED.to_string()));
}

#[test]
fn redact_value_root_scalar_benign_string_unchanged() {
    let mut v = Value::String("hello".into());
    policy().redact_value(&mut v);
    assert_eq!(v, Value::String("hello".into()));
}

#[test]
fn redact_value_root_scalars_non_string_unchanged() {
    let mut n = json!(42);
    policy().redact_value(&mut n);
    assert_eq!(n, json!(42));
    let mut b = json!(true);
    policy().redact_value(&mut b);
    assert_eq!(b, json!(true));
    let mut nul = Value::Null;
    policy().redact_value(&mut nul);
    assert_eq!(nul, Value::Null);
}

#[test]
fn redact_value_empty_object_unchanged() {
    let mut v = json!({});
    policy().redact_value(&mut v);
    assert_eq!(v, json!({}));
}

#[test]
fn redact_value_empty_array_unchanged() {
    let mut v = json!([]);
    policy().redact_value(&mut v);
    assert_eq!(v, json!([]));
}

#[test]
fn redact_value_mixed_tree_only_sensitive_parts_touched() {
    let mut v = json!({
        "name": "checkout",
        "amount": 12.5,
        "flags": [true, false],
        "config": {
            "api_key": "k",
            "retries": 3,
            "note": "ghp_leaked000000"
        },
        "items": [{"token": "t"}, {"amount": 5, "label": "ok"}]
    });
    policy().redact_value(&mut v);
    assert_eq!(v["name"], "checkout");
    assert_eq!(v["amount"], 12.5);
    assert_eq!(v["flags"], json!([true, false]));
    assert_eq!(v["config"]["api_key"], REDACTED);
    assert_eq!(v["config"]["retries"], 3);
    assert_eq!(v["config"]["note"], REDACTED); // secret-shaped value
    assert_eq!(v["items"][0]["token"], REDACTED);
    assert_eq!(v["items"][1]["amount"], 5);
    assert_eq!(v["items"][1]["label"], "ok");
}

#[test]
fn redact_value_secret_shaped_string_inside_array_replaced() {
    let mut v = json!({"notes": ["fine", "xoxb-123-abc", "also fine"]});
    policy().redact_value(&mut v);
    assert_eq!(v["notes"][0], "fine");
    assert_eq!(v["notes"][1], REDACTED);
    assert_eq!(v["notes"][2], "also fine");
}

#[test]
fn redact_value_keys_preserved_only_values_replaced() {
    let mut v = json!({"password": "x", "email": "a@b.c"});
    policy().redact_value(&mut v);
    let obj = v.as_object().unwrap();
    assert!(obj.contains_key("password"), "key name must survive");
    assert!(obj.contains_key("email"));
    assert_eq!(obj.len(), 2);
}

#[test]
fn redact_value_jwt_under_innocent_key_replaced() {
    let mut v = json!({"trace": "eyJhbGci.eyJzdWIi.sig"});
    policy().redact_value(&mut v);
    assert_eq!(v["trace"], REDACTED);
}

#[test]
fn redact_value_tokenizer_key_redacted_by_fragment_overmatch() {
    // Documenting the substring over-match end-to-end: an innocent
    // "tokenizer" config value is wiped because the key contains "token".
    let mut v = json!({"tokenizer": "cl100k_base"});
    policy().redact_value(&mut v);
    assert_eq!(v["tokenizer"], REDACTED);
}

// ===========================================================================
// redacted() — clone semantics
// ===========================================================================

#[test]
fn redacted_does_not_mutate_deep_original() {
    let original = json!({
        "config": {"password": "hunter2", "nested": {"token": [1, 2]}},
        "note": "sk_live_abc"
    });
    let snapshot = original.clone();
    let out = policy().redacted(&original);
    assert_eq!(original, snapshot, "original must be untouched");
    assert_eq!(out["config"]["password"], REDACTED);
    assert_eq!(out["config"]["nested"]["token"], REDACTED);
    assert_eq!(out["note"], REDACTED);
}

#[test]
fn redacted_equals_original_when_nothing_sensitive() {
    let original = json!({"a": 1, "b": ["x", {"c": true}]});
    let out = policy().redacted(&original);
    assert_eq!(out, original);
}

#[test]
fn redacted_of_root_scalar_secret() {
    let original = Value::String("ghp_abcdef0123456789".into());
    let out = policy().redacted(&original);
    assert_eq!(out, Value::String(REDACTED.to_string()));
    assert_eq!(original, Value::String("ghp_abcdef0123456789".into()));
}

// ===========================================================================
// redact_headers
// ===========================================================================

#[test]
fn headers_always_redacted_lowercase_names() {
    let out = policy().redact_headers(vec![
        ("authorization", "Basic x"),
        ("proxy-authorization", "y"),
        ("cookie", "a=1"),
        ("set-cookie", "b=2"),
    ]);
    for (name, value) in &out {
        assert_eq!(value, REDACTED, "header `{name}` must be redacted");
    }
}

#[test]
fn headers_always_redacted_uppercase_names() {
    let out = policy().redact_headers(vec![
        ("AUTHORIZATION", "x"),
        ("PROXY-AUTHORIZATION", "x"),
        ("COOKIE", "x"),
        ("SET-COOKIE", "x"),
    ]);
    for (_, value) in &out {
        assert_eq!(value, REDACTED);
    }
}

#[test]
fn headers_always_redacted_mixed_case_names() {
    let out = policy().redact_headers(vec![
        ("Authorization", "x"),
        ("Proxy-Authorization", "x"),
        ("Cookie", "x"),
        ("Set-Cookie", "x"),
        ("sEt-CoOkIe", "x"),
    ]);
    for (_, value) in &out {
        assert_eq!(value, REDACTED);
    }
}

#[test]
fn headers_always_redacted_even_with_benign_values() {
    // The four always-redacted headers are wiped regardless of value shape.
    let out = policy().redact_headers(vec![("Cookie", "just-a-plain-string")]);
    assert_eq!(out[0].1, REDACTED);
}

#[test]
fn headers_name_sensitive_by_fragment() {
    let out = policy().redact_headers(vec![
        ("X-Api-Key", "k"),
        ("X-Session-Token", "t"),
        ("X-Client-Secret", "s"),
    ]);
    for (_, value) in &out {
        assert_eq!(value, REDACTED);
    }
}

#[test]
fn headers_x_auth_passes_through() {
    // "x-auth" is neither an exact key ("auth" must match the whole name)
    // nor contains any fragment — it passes through un-redacted. Encode.
    let out = policy().redact_headers(vec![("X-Auth", "v")]);
    assert_eq!(out[0].1, "v");
}

#[test]
fn headers_x_auth_token_redacted_via_token_fragment() {
    let out = policy().redact_headers(vec![("X-Auth-Token", "v")]);
    assert_eq!(out[0].1, REDACTED);
}

#[test]
fn headers_value_shaped_secret_redacted_under_benign_name() {
    let out = policy().redact_headers(vec![
        ("X-Debug", "ghp_leakedtoken000"),
        ("X-Trace", "eyJa.eyJb.sig"),
        ("X-Upstream", "Bearer abc123"),
    ]);
    for (_, value) in &out {
        assert_eq!(value, REDACTED);
    }
}

#[test]
fn headers_benign_passthrough() {
    let out = policy().redact_headers(vec![
        ("Content-Type", "application/json"),
        ("X-Request-Id", "req-1"),
        ("Accept", "*/*"),
        ("User-Agent", "orch8-test"),
    ]);
    assert_eq!(out[0].1, "application/json");
    assert_eq!(out[1].1, "req-1");
    assert_eq!(out[2].1, "*/*");
    assert_eq!(out[3].1, "orch8-test");
}

#[test]
fn headers_empty_list_yields_empty_vec() {
    let out = policy().redact_headers(Vec::<(&str, &str)>::new());
    assert!(out.is_empty());
}

#[test]
fn headers_duplicate_names_each_processed_independently() {
    let out = policy().redact_headers(vec![
        ("Cookie", "a=1"),
        ("Cookie", "b=2"),
        ("X-Request-Id", "r1"),
        ("X-Request-Id", "r2"),
    ]);
    assert_eq!(out.len(), 4);
    assert_eq!(out[0].1, REDACTED);
    assert_eq!(out[1].1, REDACTED);
    assert_eq!(out[2].1, "r1");
    assert_eq!(out[3].1, "r2");
}

#[test]
fn headers_original_names_and_order_preserved() {
    let out = policy().redact_headers(vec![("X-ZZZ", "1"), ("Authorization", "x"), ("X-AAA", "2")]);
    let names: Vec<&str> = out.iter().map(|(n, _)| n.as_str()).collect();
    // Names keep their original casing and their original order.
    assert_eq!(names, vec!["X-ZZZ", "Authorization", "X-AAA"]);
}

// ===========================================================================
// redact_url
// ===========================================================================

#[test]
fn url_without_query_unchanged() {
    let p = policy();
    assert_eq!(
        p.redact_url("https://example.com/path/to/x"),
        "https://example.com/path/to/x"
    );
}

#[test]
fn url_without_query_but_with_fragment_unchanged() {
    // No '?': the whole thing (fragment included) passes through verbatim.
    let p = policy();
    assert_eq!(
        p.redact_url("https://x.com/p#token=abc"),
        "https://x.com/p#token=abc"
    );
}

#[test]
fn url_empty_query_preserved() {
    let p = policy();
    assert_eq!(p.redact_url("https://x.com/p?"), "https://x.com/p?");
}

#[test]
fn url_multiple_params_mixed_sensitivity() {
    let p = policy();
    assert_eq!(
        p.redact_url("https://api.example.com/v1?amount=5&api_key=sk_live_x&user=bob&token=t"),
        format!("https://api.example.com/v1?amount=5&api_key={REDACTED}&user=bob&token={REDACTED}")
    );
}

#[test]
fn url_valueless_benign_param_preserved() {
    let p = policy();
    assert_eq!(
        p.redact_url("https://x.com/p?flag&a=1"),
        "https://x.com/p?flag&a=1"
    );
}

#[test]
fn url_valueless_sensitive_param_passes_through() {
    // A bare "?token" pair has no '=' so no value exists to redact — the
    // textual pass leaves it untouched. Encode the actual behaviour.
    let p = policy();
    assert_eq!(
        p.redact_url("https://x.com/p?token"),
        "https://x.com/p?token"
    );
}

#[test]
fn url_fragment_preserved_after_redaction() {
    let p = policy();
    assert_eq!(
        p.redact_url("https://x.com/p?token=abc#section-2"),
        format!("https://x.com/p?token={REDACTED}#section-2")
    );
}

#[test]
fn url_fragment_content_never_redacted() {
    // Fragments are preserved verbatim even if they look like params.
    let p = policy();
    assert_eq!(
        p.redact_url("https://x.com/p?a=1#password=hunter2"),
        "https://x.com/p?a=1#password=hunter2"
    );
}

#[test]
fn url_sensitive_param_names_matrix() {
    let p = policy();
    for name in ["password", "key", "sid", "pwd", "auth", "client_secret"] {
        assert_eq!(
            p.redact_url(&format!("https://x.com/p?{name}=v")),
            format!("https://x.com/p?{name}={REDACTED}"),
            "param `{name}` should be redacted"
        );
    }
}

#[test]
fn url_secret_shaped_value_redacted_under_benign_name() {
    let p = policy();
    assert_eq!(
        p.redact_url("https://x.com/p?ref=ghp_secret123456"),
        format!("https://x.com/p?ref={REDACTED}")
    );
    assert_eq!(
        p.redact_url("https://x.com/p?trace=eyJa.eyJb.sig"),
        format!("https://x.com/p?trace={REDACTED}")
    );
}

#[test]
fn url_leading_ampersand_preserved() {
    let p = policy();
    assert_eq!(p.redact_url("https://x.com/p?&a=1"), "https://x.com/p?&a=1");
}

#[test]
fn url_double_ampersand_preserved() {
    let p = policy();
    assert_eq!(
        p.redact_url("https://x.com/p?a=1&&token=t"),
        format!("https://x.com/p?a=1&&token={REDACTED}")
    );
}

#[test]
fn url_without_scheme_still_works() {
    let p = policy();
    assert_eq!(
        p.redact_url("example.com/p?pwd=1&ok=2"),
        format!("example.com/p?pwd={REDACTED}&ok=2")
    );
}

#[test]
fn url_param_name_case_matched_but_preserved() {
    // Matching is case-insensitive; the emitted key keeps original casing.
    let p = policy();
    assert_eq!(
        p.redact_url("https://x.com/p?TOKEN=x&Amount=2"),
        format!("https://x.com/p?TOKEN={REDACTED}&Amount=2")
    );
}

#[test]
fn url_value_containing_equals_preserved() {
    let p = policy();
    assert_eq!(
        p.redact_url("https://x.com/p?filter=a=b"),
        "https://x.com/p?filter=a=b"
    );
}

#[test]
fn url_second_question_mark_becomes_part_of_param_name() {
    // Textual behaviour: only the FIRST '?' splits base/query, so a second
    // one lands inside the first param's key — which here contains "token"
    // and therefore gets redacted. Encode the actual behaviour.
    let p = policy();
    assert_eq!(
        p.redact_url("https://x.com/a?b?token=1"),
        format!("https://x.com/a?b?token={REDACTED}")
    );
}

#[test]
fn url_all_params_sensitive() {
    let p = policy();
    assert_eq!(
        p.redact_url("https://x.com/p?token=a&secret=b&api_key=c"),
        format!("https://x.com/p?token={REDACTED}&secret={REDACTED}&api_key={REDACTED}")
    );
}

// ===========================================================================
// safe_excerpt
// ===========================================================================

#[test]
fn excerpt_json_structural_redaction_nested() {
    let p = policy();
    let out = p.safe_excerpt(r#"{"error":"denied","detail":{"api_key":"sk_live_x","code":403}}"#);
    assert!(out.contains("denied"));
    assert!(out.contains("403"));
    assert!(!out.contains("sk_live_x"));
    assert!(out.contains(REDACTED));
}

#[test]
fn excerpt_json_secret_shaped_value_under_innocent_key() {
    let p = policy();
    let out = p.safe_excerpt(r#"{"note":"xoxb-1234-abcd","n":1}"#);
    assert!(!out.contains("xoxb-1234-abcd"));
    assert!(out.contains(REDACTED));
}

#[test]
fn excerpt_json_array_body_redacted() {
    let p = policy();
    let out = p.safe_excerpt(r#"["sk_live_x", "fine", 3]"#);
    assert!(!out.contains("sk_live_x"));
    assert!(out.contains("fine"));
    assert!(out.contains(REDACTED));
}

#[test]
fn excerpt_invalid_json_scans_whitespace_tokens() {
    let p = policy();
    let out = p.safe_excerpt("upstream failed with ghp_tok123456789 retrying");
    assert!(!out.contains("ghp_tok123456789"));
    assert!(out.contains(REDACTED));
    assert!(out.contains("upstream failed with"));
}

#[test]
fn excerpt_text_bearer_split_across_tokens_leaks_nothing_but_word() {
    // "Bearer abc" tokenizes to ["Bearer", "abc"]: neither token matches
    // the "Bearer " (with space) prefix, so plain-text bearer values are
    // NOT caught by the token scan. Encode this real limitation.
    let p = policy();
    let out = p.safe_excerpt("auth used Bearer abc123 today");
    assert!(out.contains("Bearer"));
    assert!(out.contains("abc123"));
    assert!(!out.contains(REDACTED));
}

#[test]
fn excerpt_text_whitespace_collapsed() {
    // Tokens are re-joined with single spaces — runs of whitespace and
    // newlines collapse. Encode the actual behaviour.
    let p = policy();
    assert_eq!(p.safe_excerpt("a  b\n\tc"), "a b c");
}

#[test]
fn excerpt_text_jwt_token_redacted() {
    let p = policy();
    let out = p.safe_excerpt("token was eyJhbGci.eyJzdWIi.sig indeed");
    assert!(!out.contains("eyJhbGci.eyJzdWIi.sig"));
    assert_eq!(out, format!("token was {REDACTED} indeed"));
}

#[test]
fn excerpt_bounded_at_custom_length() {
    let mut p = policy();
    p.max_excerpt_len = 16;
    let out = p.safe_excerpt(&"y".repeat(500));
    assert!(out.ends_with('…'));
    assert!(out.chars().count() <= 17); // 16 chars + ellipsis
    assert!(out.starts_with(&"y".repeat(16)));
}

#[test]
fn excerpt_exactly_at_limit_no_ellipsis() {
    let mut p = policy();
    p.max_excerpt_len = 5;
    let out = p.safe_excerpt("abcde"); // rendered length == limit
    assert_eq!(out, "abcde");
    assert!(!out.ends_with('…'));
}

#[test]
fn excerpt_one_over_limit_truncated_with_ellipsis() {
    let mut p = policy();
    p.max_excerpt_len = 5;
    let out = p.safe_excerpt("abcdef");
    assert_eq!(out, "abcde…");
}

#[test]
fn excerpt_truncation_respects_utf8_boundaries() {
    let mut p = policy();
    // 'é' is 2 bytes: a limit of 5 lands mid-char and must back off.
    p.max_excerpt_len = 5;
    let out = p.safe_excerpt("ééééé not json");
    assert!(out.ends_with('…'));
    assert!(out.starts_with("éé"));
    // Never panics, and never exceeds limit+ellipsis in bytes.
    assert!(out.len() <= 5 + '…'.len_utf8());
}

#[test]
fn excerpt_empty_body_yields_empty_string() {
    assert_eq!(policy().safe_excerpt(""), "");
}

#[test]
fn excerpt_default_limit_is_2048() {
    let p = policy();
    assert_eq!(p.max_excerpt_len, 2048);
    let out = p.safe_excerpt(&"x".repeat(5000));
    assert!(out.ends_with('…'));
    // 2048 payload bytes plus the 3-byte ellipsis.
    assert_eq!(out.len(), 2048 + '…'.len_utf8());
}

#[test]
fn excerpt_truncates_after_redaction_not_before() {
    // The secret sits early in a long JSON body: redaction happens on the
    // full parsed value first, then the rendered string is bounded — so
    // the marker survives and the secret never leaks.
    let mut p = policy();
    p.max_excerpt_len = 64;
    let body = format!(
        r#"{{"api_key":"sk_live_supersecret","pad":"{}"}}"#,
        "z".repeat(500)
    );
    let out = p.safe_excerpt(&body);
    assert!(out.contains(REDACTED));
    assert!(!out.contains("sk_live_supersecret"));
    assert!(out.ends_with('…'));
    assert!(out.len() <= 64 + '…'.len_utf8());
}

#[test]
fn excerpt_short_body_returned_intact() {
    let p = policy();
    assert_eq!(p.safe_excerpt("plain short body"), "plain short body");
}

#[test]
fn excerpt_json_number_body_is_valid_json_passthrough() {
    // A bare number is valid JSON: it takes the structural path and is
    // re-rendered as-is.
    let p = policy();
    assert_eq!(p.safe_excerpt("12345"), "12345");
}

#[test]
fn excerpt_nested_json_secrets_all_redacted() {
    let p = policy();
    let out = p.safe_excerpt(
        r#"{"a":{"password":"p1"},"b":[{"token":"t1"},{"c":{"client_secret":"s1"}}]}"#,
    );
    assert!(!out.contains("p1"));
    assert!(!out.contains("t1"));
    assert!(!out.contains("s1"));
    // Three separate redactions.
    assert_eq!(out.matches(REDACTED).count(), 3);
}
