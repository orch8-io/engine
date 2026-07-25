use orch8_api::{
    API_V1_PREFIX, UNVERSIONED_DEPRECATION, UNVERSIONED_SUNSET, test_harness::spawn_test_server,
};

#[tokio::test]
async fn legacy_routes_advertise_versioned_successor_and_sunset() {
    let server = spawn_test_server().await;
    let client = reqwest::Client::new();
    let legacy = client
        .get(format!("{}/sequences?limit=1", server.base_url))
        .send()
        .await
        .expect("legacy response");
    assert!(legacy.status().is_success());
    assert_eq!(legacy.headers()["deprecation"], UNVERSIONED_DEPRECATION);
    assert_eq!(legacy.headers()["sunset"], UNVERSIONED_SUNSET);
    assert_eq!(
        legacy.headers()["link"],
        format!("<{API_V1_PREFIX}/sequences?limit=1>; rel=\"successor-version\"")
    );
    let legacy_body = legacy.text().await.expect("legacy JSON body");

    let canonical = client
        .get(format!("{}/api/v1/sequences?limit=1", server.base_url))
        .send()
        .await
        .expect("canonical response");
    assert!(canonical.status().is_success());
    assert!(!canonical.headers().contains_key("deprecation"));
    assert!(!canonical.headers().contains_key("sunset"));
    assert!(!canonical.headers().contains_key("link"));
    let canonical_body = canonical.text().await.expect("canonical JSON body");
    assert_eq!(legacy_body, canonical_body);
}
