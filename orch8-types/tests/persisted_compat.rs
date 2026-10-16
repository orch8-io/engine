//! Golden-JSON compatibility for persisted types (T2-GAP-TY-01/02).
//!
//! Each fixture pair under `tests/fixtures/persisted/` is a payload as this
//! release writes it (`<name>.json`) and the same payload as a *newer*
//! release might write it, with an extra field (`<name>.future.json`). A
//! binary rollback must keep reading rows a newer binary wrote, so every
//! persisted type tolerates unknown fields on read.

use orch8_types::ai::PromptRef;
use orch8_types::release::ReleaseTarget;
use orch8_types::sub_tenant::{EmbedTheme, SubTenantLimits};
use serde::Serialize;
use serde::de::DeserializeOwned;

fn fixture(name: &str) -> serde_json::Value {
    let path = format!(
        "{}/tests/fixtures/persisted/{name}",
        env!("CARGO_MANIFEST_DIR")
    );
    serde_json::from_str(&std::fs::read_to_string(&path).unwrap()).unwrap()
}

fn decode<T: DeserializeOwned>(name: &str) -> Result<T, serde_json::Error> {
    serde_json::from_value(fixture(name))
}

fn reencode<T: DeserializeOwned + Serialize>(name: &str) -> serde_json::Value {
    serde_json::to_value(decode::<T>(name).unwrap()).unwrap()
}

macro_rules! persisted_type_compat {
    ($current_test:ident, $future_test:ident, $ty:ty, $stem:literal) => {
        #[test]
        fn $current_test() {
            let reencoded = reencode::<$ty>(concat!($stem, ".json"));

            assert_eq!(reencoded, fixture(concat!($stem, ".json")));
        }

        #[test]
        fn $future_test() {
            let future = decode::<$ty>(concat!($stem, ".future.json"));

            assert_eq!(
                future.unwrap(),
                decode::<$ty>(concat!($stem, ".json")).unwrap()
            );
        }
    };
}

persisted_type_compat!(
    prompt_ref_golden_payload_round_trips,
    prompt_ref_from_a_newer_writer_decodes,
    PromptRef,
    "prompt_ref"
);
persisted_type_compat!(
    release_target_golden_payload_round_trips,
    release_target_from_a_newer_writer_decodes,
    ReleaseTarget,
    "release_target"
);
persisted_type_compat!(
    sub_tenant_limits_golden_payload_round_trips,
    sub_tenant_limits_from_a_newer_writer_decodes,
    SubTenantLimits,
    "sub_tenant_limits"
);
persisted_type_compat!(
    embed_theme_golden_payload_round_trips,
    embed_theme_from_a_newer_writer_decodes,
    EmbedTheme,
    "embed_theme"
);
