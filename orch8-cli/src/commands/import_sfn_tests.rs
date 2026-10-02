//! Fixture-driven tests for `orch8 import stepfunctions`. Every converted
//! sequence must decode strictly, validate, and pass preflight.

use super::super::tests::{assert_passes_preflight, find_block};
use super::*;

fn load(name: &str) -> (String, String, Value) {
    let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("tests/fixtures/import")
        .join(name);
    let raw = std::fs::read_to_string(&path).unwrap();
    let value = serde_json::from_str(&raw).unwrap();
    (name.to_string(), raw, value)
}

fn convert(name: &str) -> Conversion {
    let (file, raw, value) = load(name);
    let conversion =
        convert_stepfunctions(&value, Some((&file, &raw)), &ConvertOptions::default()).unwrap();
    assert_passes_preflight(&conversion);
    conversion
}

fn unmapped_mentions(conversion: &Conversion, needle: &str) -> bool {
    conversion
        .report
        .unmapped
        .iter()
        .any(|u| u.construct.contains(needle) || u.reason.contains(needle))
}

#[test]
fn order_pipeline_translates_structure() {
    let conversion = convert("sfn-order-pipeline.asl.json");
    let seq = &conversion.sequence;
    assert_eq!(seq["name"], "order-pipeline");
    let blocks = seq["blocks"].as_array().unwrap();

    // Validate Order: lambda:invoke with Retry + Catch → try_catch + router
    // (the catch path ends in Fail, the success path continues).
    assert_eq!(blocks[0]["type"], "try_catch");
    let validate = &blocks[0]["try_block"][0];
    assert_eq!(validate["handler"], "validate_order");
    assert_eq!(validate["timeout"], 30_000);
    assert_eq!(validate["retry"]["max_attempts"], 4);
    assert_eq!(validate["retry"]["initial_backoff"], 2_000);
    assert_eq!(validate["retry"]["max_backoff"], 20_000);
    assert_eq!(validate["params"]["orderId"], "{{data.order.id}}");
    assert_eq!(validate["params"]["customer"], "{{data.customer}}");
    assert_eq!(validate["params"]["source"], "sfn");
    let catch = blocks[0]["catch_block"].as_array().unwrap();
    assert_eq!(catch[0]["handler"], "noop", "error marker");
    assert_eq!(catch[1]["handler"], "aws_sns_publish");
    assert_eq!(catch[2]["handler"], "fail");
    assert_eq!(catch[2]["params"]["error"], "OrderFailed");

    assert_eq!(blocks[1]["type"], "router");
    let ok = &blocks[1]["routes"][0];
    assert_eq!(
        ok["condition"],
        format!("outputs.{} == null", catch[0]["id"].as_str().unwrap())
    );
    let ok_blocks = ok["blocks"].as_array().unwrap();

    // Is Express? → router whose branches rejoin at Reserve And Charge.
    let choice = &ok_blocks[0];
    assert_eq!(choice["type"], "router");
    assert_eq!(
        choice["routes"][0]["condition"],
        "(outputs.validate_order.valid == true) && (data.order.shipping == \"express\")"
    );
    assert_eq!(choice["routes"][1]["condition"], "data.order.total > 1000");
    assert_eq!(choice["routes"][0]["blocks"][0]["handler"], "transform");
    assert_eq!(
        choice["routes"][1]["blocks"][0]["delay"]["duration"],
        3_600_000
    );
    // Default goes straight to the join: an empty (noop) branch.
    assert_eq!(choice["default"][0]["handler"], "noop");

    // The join follows the router exactly once.
    let parallel = &ok_blocks[1];
    assert_eq!(parallel["type"], "parallel");
    assert_eq!(parallel["branches"][0][0]["handler"], "reserve_inventory");
    assert_eq!(
        parallel["branches"][0][0]["params"]["items"],
        "{{data.order.items}}"
    );
    assert_eq!(parallel["branches"][1][0]["handler"], "charge_card");
    let map = &ok_blocks[2];
    assert_eq!(map["type"], "for_each");
    assert_eq!(map["collection"], "{{data.order.items}}");
    assert_eq!(map["body"][0]["params"]["sku"], "{{item.sku}}");
    assert_eq!(ok_blocks.len(), 3, "Succeed adds no block");

    let workers = &conversion.report.worker_handlers;
    for h in [
        "validate_order",
        "reserve_inventory",
        "charge_card",
        "ship_item",
        "aws_sns_publish",
    ] {
        assert!(
            workers.iter().any(|w| w == h),
            "missing worker {h}: {workers:?}"
        );
    }
    // Nothing silently dropped: the one untranslated construct is reported.
    assert!(
        conversion.report.unmapped.is_empty(),
        "{:#?}",
        conversion.report.unmapped
    );
    assert!(
        conversion
            .report
            .mapped
            .iter()
            .any(|m| m.node == "Done" && m.mapped_to == "end of chain")
    );
}

#[test]
fn polling_loop_becomes_loop_block() {
    let conversion = convert("sfn-poll-job.asl.json");
    let blocks = conversion.sequence["blocks"].as_array().unwrap();
    let kinds: Vec<&str> = blocks.iter().map(|b| b["type"].as_str().unwrap()).collect();
    assert_eq!(kinds, ["step", "step", "step", "loop", "router"]);
    assert_eq!(blocks[0]["handler"], "start_job");
    assert_eq!(blocks[1]["delay"]["duration"], 30_000);
    // ResultPath $.job → the check reads the start job's output.
    assert_eq!(blocks[2]["params"]["jobId"], "{{outputs.start_job.id}}");

    let lp = &blocks[3];
    assert_eq!(
        lp["condition"],
        "!((data.status.state == \"SUCCEEDED\") || (data.status.state == \"FAILED\"))"
    );
    let body = lp["body"].as_array().unwrap();
    assert_eq!(body.len(), 2, "wait + check again");
    assert_eq!(body[0]["delay"]["duration"], 30_000);
    assert_eq!(body[1]["handler"], "check_job");

    let exit = &blocks[4];
    assert_eq!(
        exit["routes"][0]["condition"],
        "outputs.check_status.state == \"SUCCEEDED\""
    );
    let publish = &exit["routes"][0]["blocks"][0];
    assert_eq!(publish["handler"], "http_request");
    assert_eq!(publish["params"]["method"], "POST");
    assert_eq!(
        publish["params"]["body"]["state"],
        "{{outputs.check_status.state}}"
    );
    assert_eq!(exit["routes"][1]["blocks"][0]["handler"], "fail");
    assert!(unmapped_mentions(&conversion, "polling loop"));
}

#[test]
fn describe_output_reports_every_gap() {
    let conversion = convert("sfn-describe-account-sync.json");
    let seq = &conversion.sequence;
    assert_eq!(seq["name"], "account-sync");
    let map = find_block(seq, "fan_out").unwrap();
    assert_eq!(map["type"], "for_each");
    assert_eq!(map["collection"], "{{data.accounts}}");
    let child = &map["body"][0];
    assert_eq!(child["type"], "sub_sequence");
    assert_eq!(child["sequence_name"], "sync-account");
    assert_eq!(child["input"]["accountId"], "{{item.id}}");
    assert_eq!(child["input"]["region"], "{{data.region}}");

    let approve = find_block(seq, "approve").unwrap();
    assert_eq!(approve["handler"], "aws_sqs_sendmessage");
    assert_eq!(approve["retry"]["max_attempts"], 6);

    let wait_until = find_block(seq, "wait_until").unwrap();
    assert_eq!(wait_until["delay"]["fire_at_local"], "2026-12-01T09:30:00");
    assert_eq!(wait_until["delay"]["timezone"], "UTC");
    let dynamic = find_block(seq, "dynamic_wait").unwrap();
    assert_eq!(dynamic["handler"], "log");

    for needle in [
        "DISTRIBUTED",
        "$$.Task.Token",
        "States.Format",
        "Retry[1]",
        "Retry[0].ErrorEquals",
        "Catch[1]",
        "HeartbeatSeconds",
        "StringMatches",
        "waitForTaskToken",
        "SecondsPath",
        "Fan Out.Retry",
        "ItemSelector",
    ] {
        assert!(
            unmapped_mentions(&conversion, needle),
            "`{needle}` not reported: {:#?}",
            conversion.report.unmapped
        );
    }
    // Untranslatable Choice rule is gated on an explicit TODO flag.
    let route = find_block(seq, "route").unwrap();
    assert_eq!(
        route["routes"][0]["condition"],
        "data.todo_route_rule_0 == true"
    );
    assert_eq!(
        route["routes"][1]["condition"],
        "outputs.approve.decision == null"
    );
    // Line numbers point into the (pretty-printed) definition string's file.
    assert!(conversion.report.unmapped.iter().all(|u| {
        std::path::Path::new(&u.file)
            .extension()
            .is_some_and(|e| e.eq_ignore_ascii_case("json"))
    }));
}

#[test]
fn missing_default_fails_like_step_functions() {
    let def = json!({
        "StartAt": "C",
        "States": {
            "C": {"Type": "Choice", "Choices": [{"Variable": "$.x", "NumericEquals": 1, "Next": "A"}]},
            "A": {"Type": "Pass", "Result": {"ok": true}, "End": true}
        }
    });
    let conversion = convert_stepfunctions(&def, None, &ConvertOptions::default()).unwrap();
    assert_passes_preflight(&conversion);
    let router = &conversion.sequence["blocks"][0];
    assert_eq!(router["default"][0]["handler"], "fail");
    assert_eq!(
        router["default"][0]["params"]["error"],
        "States.NoChoiceMatched"
    );
}

#[test]
fn non_definitions_are_rejected() {
    assert!(convert_stepfunctions(&json!({"foo": 1}), None, &ConvertOptions::default()).is_err());
    let dangling = json!({"StartAt": "A", "States": {"A": {"Type": "Pass", "Next": "B"}}});
    let err = convert_stepfunctions(&dangling, None, &ConvertOptions::default()).unwrap_err();
    assert!(err.to_string().contains("`B`"), "{err}");
}
