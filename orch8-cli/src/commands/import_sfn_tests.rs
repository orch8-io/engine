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
fn composite_retries_are_retained_in_source_order() {
    let child = json!({"Type":"Task", "Resource":"arn:aws:states:::states:startExecution.sync:2",
        "Parameters":{"StateMachineArn":"arn:aws:states:us-east-1:111111111111:stateMachine:child-flow"},
        "End":true});
    let policy = json!([
        {"ErrorEquals":["Fixture.Exhausted"],"MaxAttempts":0},
        {"ErrorEquals":["States.ALL"],"MaxAttempts":2,"IntervalSeconds":2}
    ]);
    let conversion = convert_stepfunctions(
        &json!({
            "StartAt":"Fan Out",
            "States":{
                "Fan Out":{"Type":"Parallel","Branches":[
                    {"StartAt":"Branch","States":{"Branch":child}}
                ],"Retry":policy,"Next":"Inline"},
                "Inline":{"Type":"Map","ItemsPath":"$.items","MaxConcurrency":1,
                    "ItemProcessor":{"ProcessorConfig":{"Mode":"INLINE"},
                        "StartAt":"Item","States":{"Item":child}},
                    "Retry":policy,"End":true}
            }
        }),
        None,
        &ConvertOptions::default(),
    )
    .unwrap();
    assert_passes_preflight(&conversion);
    assert!(!unmapped_mentions(&conversion, "Retry"));
    let policies = &conversion.sequence["input_schema"]["x-orch8-composite-retry"];
    for id in ["fan_out", "inline"] {
        let rules = &policies[id]["rules"];
        assert_eq!(rules[0]["error_equals"], json!(["Fixture.Exhausted"]));
        assert_eq!(rules[0]["max_attempts"], 0);
        assert_eq!(rules[1]["error_equals"], json!(["States.ALL"]));
        assert_eq!(rules[1]["max_attempts"], 2);
        assert_eq!(rules[1]["initial_backoff"], 2000);
    }
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
    assert_eq!(validate["retry"]["max_attempts"], 3);
    assert_eq!(validate["retry"]["jitter"], "none");
    assert_eq!(validate["retry"]["initial_backoff"], 2_000);
    assert_eq!(validate["retry"]["max_backoff"], 20_000);
    assert_eq!(validate["params"]["orderId"], "{{data.order.id}}");
    assert_eq!(validate["params"]["customer"], "{{data.customer}}");
    assert_eq!(validate["params"]["source"], "sfn");
    assert_eq!(
        blocks[0]["try_block"][1]["id"],
        "validate_order_result_selector"
    );
    let catch = blocks[0]["catch_block"].as_array().unwrap();
    assert_eq!(catch[0]["handler"], "noop", "error marker");
    let route = catch[1]["routes"][0]["blocks"].as_array().unwrap();
    assert!(
        route
            .iter()
            .any(|step| step["handler"] == "aws_sns_publish")
    );
    let fail = route.iter().find(|step| step["handler"] == "fail").unwrap();
    assert_eq!(fail["params"]["error"], "OrderFailed");

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
        "(outputs.validate_order_result_path.validation.Payload.valid == true) && (outputs.validate_order_result_path.order.shipping == \"express\")"
    );
    assert_eq!(
        choice["routes"][1]["condition"],
        "outputs.validate_order_result_path.order.total > 1000"
    );
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
        "{{outputs.validate_order_result_path.order.items}}"
    );
    assert_eq!(parallel["branches"][1][0]["handler"], "charge_card");
    let map = &ok_blocks[2];
    assert_eq!(map["type"], "for_each");
    assert_eq!(
        map["collection"],
        "{{outputs.validate_order_result_path.order.items}}"
    );
    let item_var = map["item_var"].as_str().unwrap();
    assert_eq!(
        map["body"][0]["params"]["sku"],
        format!("{{{{data.{item_var}.sku}}}}")
    );
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
    // Every construct in this fixture is translated.
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
    assert_eq!(map["body"][0]["id"], "fan_out_item_selector");
    let child = &map["body"][1];
    assert_eq!(child["type"], "sub_sequence");
    assert_eq!(child["sequence_name"], "sync-account");
    assert_eq!(
        child["input"]["accountId"],
        "{{outputs.fan_out_item_selector.accountId}}"
    );
    assert_eq!(
        child["input"]["region"],
        "{{outputs.fan_out_item_selector.region}}"
    );

    let approve = find_block(seq, "approve").unwrap();
    assert_eq!(approve["handler"], "aws_sqs_sendmessage");
    assert_eq!(approve["retry"]["max_attempts"], 5);
    assert_eq!(approve["params"]["$asl_heartbeat"]["seconds"], 600);
    assert_eq!(
        approve["params"]["$asl_heartbeat"]["params"]["$asl_callback"]["params"]["MessageBody"]["token"],
        json!({"$asl_task_token": true})
    );

    let wait_until = find_block(seq, "wait_until").unwrap();
    assert_eq!(wait_until["delay"]["fire_at_local"], "2026-12-01T09:30:00");
    assert_eq!(wait_until["delay"]["timezone"], "UTC");
    let dynamic = find_block(seq, "dynamic_wait").unwrap();
    assert_eq!(dynamic["handler"], "asl_wait");
    assert!(dynamic["params"]["seconds"].is_string());

    for needle in [
        "DISTRIBUTED",
        "States.Format",
        "StringMatches",
        "Fan Out.Retry",
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

#[test]
fn heartbeat_path_uses_narrowed_state_input_and_preserves_payload() {
    let source = json!({
        "StartAt": "Work",
        "States": {
            "Work": {
                "Type": "Task",
                "Resource": "arn:aws:states:::lambda:invoke",
                "InputPath": "$.request",
                "Parameters": {"FunctionName": "work", "Payload.$": "$.value"},
                "HeartbeatSecondsPath": "$.options.heartbeat",
                "TimeoutSeconds": 300,
                "End": true
            }
        }
    });
    let conversion = convert_stepfunctions(&source, None, &ConvertOptions::default()).unwrap();
    assert_passes_preflight(&conversion);
    let work = find_block(&conversion.sequence, "work").unwrap();
    assert_eq!(work["timeout"], 300_000);
    assert_eq!(
        work["params"]["$asl_heartbeat"],
        json!({
            "seconds": "{{data.request.options.heartbeat}}",
            "params": {"input": "{{data.request.value}}"}
        })
    );
    assert!(!unmapped_mentions(&conversion, "HeartbeatSecondsPath"));
}

#[test]
fn malformed_heartbeat_configuration_is_rejected() {
    let mut source = json!({
        "StartAt":"Work",
        "States":{"Work":{
            "Type":"Task",
            "Resource":"arn:aws:states:us-east-1:111111111111:activity:work",
            "HeartbeatSeconds":0,
            "End":true
        }}
    });
    let error = convert_stepfunctions(&source, None, &ConvertOptions::default())
        .expect_err("zero heartbeat interval must be rejected");
    assert!(error.to_string().contains("positive integer"));
    source["States"]["Work"]["HeartbeatSeconds"] = json!(1);
    source["States"]["Work"]["HeartbeatSecondsPath"] = json!("$.heartbeat");
    let error = convert_stepfunctions(&source, None, &ConvertOptions::default())
        .expect_err("mutually exclusive heartbeat fields must be rejected");
    assert!(
        error
            .to_string()
            .contains("both HeartbeatSeconds and HeartbeatSecondsPath")
    );
}

#[test]
fn worker_timeout_path_wraps_callback_parameters_and_rejects_a_second_deadline() {
    let mut source = json!({
        "StartAt": "Work",
        "States": {"Work": {
            "Type": "Task", "Resource": "arn:aws:states:::sqs:sendMessage.waitForTaskToken",
            "TimeoutSecondsPath": "$.limits.seconds", "HeartbeatSeconds": 5,
            "Parameters": {"token.$": "$$.Task.Token"}, "End": true
        }}
    });
    let conversion = convert_stepfunctions(&source, None, &ConvertOptions::default()).unwrap();
    assert!(!unmapped_mentions(&conversion, "TimeoutSecondsPath"));
    let params = &conversion.sequence["blocks"][0]["params"];
    assert_eq!(
        params,
        &json!({"$asl_timeout": {
            "seconds": "{{data.limits.seconds}}", "params": {"$asl_heartbeat": {
                "seconds": 5, "params": {"$asl_callback": {"params": {"token": {"$asl_task_token": true}}}}
            }}
        }})
    );
    source["States"]["Work"]["TimeoutSeconds"] = json!(10);
    assert!(
        convert_stepfunctions(&source, None, &ConvertOptions::default())
            .unwrap_err()
            .to_string()
            .contains("both TimeoutSeconds and TimeoutSecondsPath")
    );
}

#[test]
fn callback_intrinsics_are_deferred_only_for_token_tasks() {
    let intrinsic = "States.Format('{}:{}', $.label, $$.Task.Token)";
    let mut source = json!({"StartAt":"Work", "States":{"Work":{
        "Type":"Task", "Resource":"arn:aws:states:::sqs:sendMessage.waitForTaskToken",
        "Parameters":{"message.$":intrinsic}, "End":true
    }}});
    let callback = convert_stepfunctions(&source, None, &ConvertOptions::default()).unwrap();
    assert!(!unmapped_mentions(&callback, "States.Format"));
    let bound = callback.sequence["blocks"][0]["params"]["$asl_callback"]["params"]["message"]
        .as_str()
        .unwrap();
    assert!(
        orch8_types::asl_intrinsic::Intrinsic::from_callback_template(bound)
            .unwrap()
            .is_ok()
    );
    source["States"]["Work"]["Resource"] = json!("arn:aws:states:::sqs:sendMessage");
    let normal = convert_stepfunctions(&source, None, &ConvertOptions::default()).unwrap();
    assert!(unmapped_mentions(&normal, "States.Format"));
}
