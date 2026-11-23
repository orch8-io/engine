use orch8_wasm::preflight;
use serde_json::Value;

#[test]
fn offline_preflight_reports_the_server_code_for_an_invalid_schema_path() {
    let sequence = include_str!("../../orch8-engine/tests/fixtures/typed_dataflow_preview.json");
    let report: Value = serde_json::from_str(&preflight(sequence)).unwrap();
    assert_eq!(report["overall"], "fail", "{report}");
    let check = report["checks"]
        .as_array()
        .unwrap()
        .iter()
        .find(|check| check["id"] == "typed_dataflow_compatible")
        .unwrap();
    assert_eq!(check["status"], "fail");
    assert_eq!(check["findings"][0]["code"], "SCHEMA_PATH_MISSING");
    assert!(
        check["findings"][0]["summary"]
            .as_str()
            .unwrap()
            .contains("data.regon")
    );
}

#[test]
fn offline_preflight_reports_producer_order_and_branch_availability() {
    let sequence = include_str!("../../orch8-engine/tests/fixtures/typed_dataflow_order.json");
    let report: Value = serde_json::from_str(&preflight(sequence)).unwrap();
    let check = report["checks"]
        .as_array()
        .unwrap()
        .iter()
        .find(|check| check["id"] == "typed_dataflow_compatible")
        .unwrap();
    assert_eq!(check["status"], "fail");
    let findings = check["findings"].as_array().unwrap();
    assert_eq!(findings[0]["code"], "PRODUCER_MAY_NOT_RUN");
    assert_eq!(findings[0]["severity"], "warning");
    assert_eq!(findings[1]["code"], "PRODUCER_RUNS_LATER");
    assert_eq!(findings[1]["severity"], "error");
}

#[test]
fn offline_preflight_exposes_the_same_source_location_as_native_preflight() {
    let sequence = include_str!("../../orch8-engine/tests/fixtures/typed_dataflow_source.json");
    let report: Value = serde_json::from_str(&preflight(sequence)).unwrap();
    let check = report["checks"]
        .as_array()
        .unwrap()
        .iter()
        .find(|check| check["id"] == "typed_dataflow_compatible")
        .unwrap();
    let evidence = &check["findings"][0]["evidence"][0];
    assert_eq!(evidence["label"], "dataflow_source");
    assert_eq!(
        evidence["data"]["sequence_pointer"],
        "/blocks/0/branches/0/1/params/value~1~0/0"
    );
    assert_eq!(
        evidence["data"]["producer_schema_pointer"],
        "/blocks/0/branches/0/0/output_schema"
    );
}
