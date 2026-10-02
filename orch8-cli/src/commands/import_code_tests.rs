//! Fixture-driven tests for `orch8 import temporal|inngest|bullmq`. Every
//! converted sequence must decode strictly, validate, and pass preflight;
//! every construct the extractor cannot translate must be reported with its
//! `file:line`.

use super::super::tests::{assert_passes_preflight, find_block};
use super::super::{ImportArgs, ImportCmd, run};
use super::*;
use crate::seqdoc::FormatArg;

fn fixture_path(name: &str) -> std::path::PathBuf {
    std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("tests/fixtures/import")
        .join(name)
}

fn load(name: &str) -> Vec<SourceFile> {
    load_sources(&fixture_path(name)).unwrap()
}

/// Line of the first occurrence of `needle` in a fixture.
fn line_of(name: &str, needle: &str) -> usize {
    std::fs::read_to_string(fixture_path(name))
        .unwrap()
        .lines()
        .position(|l| l.contains(needle))
        .map(|i| i + 1)
        .unwrap_or_else(|| panic!("`{needle}` not in {name}"))
}

fn unmapped_at<'c>(conversion: &'c Conversion, needle: &str) -> &'c UnmappedConstruct {
    conversion
        .report
        .unmapped
        .iter()
        .find(|u| u.construct.contains(needle) || u.reason.contains(needle))
        .unwrap_or_else(|| panic!("`{needle}` not reported: {:#?}", conversion.report.unmapped))
}

#[test]
fn durations_parse() {
    assert_eq!(parse_duration("1d"), Some(86_400_000));
    assert_eq!(parse_duration("24 hours"), Some(86_400_000));
    assert_eq!(parse_duration("1h30m"), Some(5_400_000));
    assert_eq!(parse_duration("2s"), Some(2_000));
    assert_eq!(parse_duration("1 minute"), Some(60_000));
    assert_eq!(parse_duration("500"), Some(500));
    assert_eq!(parse_duration("soon"), None);
}

#[test]
fn tokenizer_tracks_lines_strings_and_comments() {
    let file = SourceFile {
        path: "x.ts".into(),
        text: "// c1\nconst a = 'x\\'y'; /* multi\nline */ const b = `t ${a}`;\nfoo?.bar".into(),
    };
    let src = Src::new(&file);
    let kinds: Vec<(Tok, usize)> = src.toks.iter().map(|t| (t.tok.clone(), t.line)).collect();
    assert_eq!(kinds[3], (Tok::Str("x'y".into()), 2));
    assert!(kinds.contains(&(Tok::Tpl("t ${a}".into(), true), 3)));
    assert!(kinds.contains(&(Tok::Punct("?."), 4)));
}

#[test]
fn inngest_function_becomes_sequence_skeleton() {
    let conversion =
        convert_inngest(&load("inngest-onboarding.ts"), &ConvertOptions::default()).unwrap();
    assert_passes_preflight(&conversion);
    let seq = &conversion.sequence;
    let report = &conversion.report;
    assert_eq!(seq["name"], "user-onboarding");
    assert!(report.warnings.iter().any(|w| w.contains("nightly-report")));

    let fetch = find_block(seq, "fetch_user").unwrap();
    assert_eq!(fetch["handler"], "fetch_user");
    assert_eq!(fetch["retry"]["max_attempts"], 3, "retries: 2 → 3 attempts");
    assert_eq!(
        find_block(seq, "wait_a_day").unwrap()["delay"]["duration"],
        86_400_000
    );

    // waitForEvent with a timeout: wrapped so a timeout continues (null).
    let tc = find_block(seq, "wait_for_activation_or_timeout").unwrap();
    assert_eq!(tc["type"], "try_catch");
    let wait = &tc["try_block"][0];
    assert_eq!(wait["handler"], "wait_for_event");
    assert_eq!(wait["params"]["events"][0], "app/user.activated");
    assert_eq!(wait["params"]["correlation_key"], "{{data.data.userId}}");
    assert_eq!(wait["wait_for_input"]["timeout"], 3 * 86_400_000_u64);

    // if/else around steps → router with a translated condition.
    let line = line_of("inngest-onboarding.ts", "user.plan ===");
    let router = find_block(seq, &format!("if_l{line}")).unwrap();
    assert_eq!(
        router["routes"][0]["condition"],
        "outputs.fetch_user.plan == \"pro\" && data.data.source != \"import\""
    );
    let par = &router["routes"][0]["blocks"][0];
    assert_eq!(par["type"], "parallel");
    assert_eq!(par["branches"][0][0]["handler"], "provision_workspace");
    assert_eq!(par["branches"][1][0]["handler"], "notify_sales");
    assert_eq!(router["default"][0]["handler"], "send_free_tips");

    // for..of over the event → for_each.
    let for_line = line_of("inngest-onboarding.ts", "for (const team");
    let each = find_block(seq, &format!("for_team_l{for_line}")).unwrap();
    assert_eq!(each["collection"], "{{data.data.teams}}");
    assert_eq!(each["item_var"], "team");
    assert_eq!(each["body"][0]["handler"], "invite_team");

    // Untranslatable condition: gated on a TODO flag and reported with its line.
    let random_line = line_of("inngest-onboarding.ts", "Math.random");
    let lottery = find_block(seq, &format!("if_l{random_line}")).unwrap();
    assert_eq!(
        lottery["routes"][0]["condition"],
        format!("data.todo_if_condition_l{random_line} == true")
    );
    let u = unmapped_at(&conversion, "Math.random");
    assert_eq!(u.line, random_line);
    assert!(u.file.ends_with("inngest-onboarding.ts"));

    let emit = find_block(seq, "emit_onboarded").unwrap();
    assert_eq!(emit["handler"], "emit_event");
    assert_eq!(emit["params"]["trigger_slug"], "app-user-onboarded");
    assert_eq!(
        emit["params"]["data"]["data"]["userId"],
        "{{data.data.userId}}"
    );

    let invoke = find_block(seq, "score_lead").unwrap();
    assert_eq!(invoke["type"], "sub_sequence");
    assert_eq!(invoke["sequence_name"], "score-lead");
    assert_eq!(
        invoke["input"]["data"]["email"],
        "{{outputs.fetch_user.email}}"
    );

    // Unknown step method → visible TODO stub + unmapped entry with its line.
    let signal = unmapped_at(&conversion, "step.waitForSignal");
    assert_eq!(
        signal.line,
        line_of("inngest-onboarding.ts", "step.waitForSignal")
    );
    assert_eq!(
        unmapped_at(&conversion, "concurrency").line,
        line_of("inngest-onboarding.ts", "concurrency:")
    );
    unmapped_at(&conversion, "cancelOn");

    assert_eq!(report.triggers.len(), 1);
    assert_eq!(report.triggers[0].kind, "event");
    assert_eq!(report.triggers[0].body["trigger_type"], "event");

    let nightly = convert_inngest(
        &load("inngest-onboarding.ts"),
        &ConvertOptions {
            workflow: Some("nightly-report".into()),
            ..ConvertOptions::default()
        },
    )
    .unwrap();
    assert_passes_preflight(&nightly);
    assert_eq!(nightly.report.triggers[0].kind, "cron");
    assert_eq!(nightly.report.triggers[0].body["cron_expr"], "0 6 * * *");
    assert_eq!(nightly.report.triggers[0].body["timezone"], "Europe/Paris");
}

#[test]
fn temporal_workflow_maps_activities_and_control_flow() {
    let conversion = convert_temporal(&load("temporal-order"), &ConvertOptions::default()).unwrap();
    assert_passes_preflight(&conversion);
    let seq = &conversion.sequence;
    assert_eq!(seq["name"], "order-workflow");
    let blocks = seq["blocks"].as_array().unwrap();

    let reserve = &blocks[0];
    assert_eq!(reserve["handler"], "reserve_inventory");
    assert_eq!(reserve["params"]["args"][0], "{{data.id}}");
    assert_eq!(reserve["timeout"], 60_000);
    assert_eq!(reserve["retry"]["max_attempts"], 5);
    assert_eq!(reserve["retry"]["initial_backoff"], 2_000);
    assert_eq!(reserve["retry"]["max_backoff"], 60_000);
    assert_eq!(reserve["retry"]["non_retryable_codes"][0], "CardDeclined");

    // if (order.total > 1000) { condition(...) ; if (!ok) {...; return} }
    let big = &blocks[1];
    assert_eq!(big["type"], "router");
    assert_eq!(big["routes"][0]["condition"], "data.total > 1000");
    let gate_tc = &big["routes"][0]["blocks"][0];
    assert_eq!(gate_tc["type"], "try_catch");
    let gate = &gate_tc["try_block"][0];
    assert_eq!(gate["wait_for_input"]["timeout"], 86_400_000);
    let expired = &big["routes"][0]["blocks"][1];
    assert_eq!(
        expired["routes"][0]["blocks"][0]["handler"],
        "notify_customer"
    );
    assert_eq!(
        expired["routes"][0]["blocks"][0]["queue_name"],
        "notifications"
    );
    unmapped_at(&conversion, "early return");
    unmapped_at(&conversion, "human_input:");

    assert_eq!(blocks[2]["handler"], "charge_card");
    let tc = &blocks[3];
    assert_eq!(tc["type"], "try_catch");
    assert_eq!(tc["try_block"][0]["handler"], "ship_order");
    assert_eq!(tc["catch_block"][0]["handler"], "refund_card");
    assert_eq!(
        tc["catch_block"][0]["params"]["args"][0],
        "{{outputs.charge_card.chargeId}}"
    );
    assert_eq!(tc["catch_block"][1]["handler"], "fail", "rethrow preserved");

    assert_eq!(blocks[4]["delay"]["duration"], 2 * 86_400_000_u64);
    assert_eq!(blocks[5]["type"], "cancellation_scope");
    assert_eq!(blocks[5]["blocks"][0]["handler"], "notify_customer");
    assert_eq!(blocks[6]["type"], "sub_sequence");
    assert_eq!(blocks[6]["sequence_name"], "loyalty-workflow");
    assert_eq!(blocks[6]["input"], "{{data.email}}");
    assert_eq!(blocks.len(), 7);

    let workflows = "temporal-order/workflows.ts";
    assert_eq!(
        unmapped_at(&conversion, "setHandler").line,
        line_of(workflows, "setHandler(")
    );
    assert_eq!(
        unmapped_at(&conversion, "defineSignal").line,
        line_of(workflows, "defineSignal<")
    );
    assert_eq!(
        unmapped_at(&conversion, "retries activities forever").line,
        line_of(workflows, "const notify")
    );
    unmapped_at(&conversion, "workflowId");
    assert!(
        conversion
            .report
            .warnings
            .iter()
            .any(|w| w.contains("loyaltyWorkflow"))
    );

    let loyalty = convert_temporal(
        &load("temporal-order"),
        &ConvertOptions {
            workflow: Some("loyaltyWorkflow".into()),
            ..ConvertOptions::default()
        },
    )
    .unwrap();
    assert_passes_preflight(&loyalty);
    assert_eq!(loyalty.sequence["blocks"][0]["delay"]["duration"], 1_000);
}

#[test]
fn bullmq_flow_tree_becomes_children_then_parent() {
    let conversion = convert_bullmq(&load("bullmq-flow.ts"), &ConvertOptions::default()).unwrap();
    assert_passes_preflight(&conversion);
    let seq = &conversion.sequence;
    assert_eq!(seq["name"], "renovate-interior");
    let blocks = seq["blocks"].as_array().unwrap();
    assert_eq!(blocks.len(), 2);
    let children = &blocks[0];
    assert_eq!(children["type"], "parallel");
    assert_eq!(children["branches"][0][0]["params"]["place"], "ceiling");
    assert_eq!(children["branches"][0][0]["queue_name"], "steps");
    assert_eq!(children["branches"][1][0]["delay"]["duration"], 5_000);
    // Grandchild runs before its parent inside the branch.
    assert_eq!(children["branches"][2][0]["handler"], "buy_materials");
    assert_eq!(children["branches"][2][0]["queue_name"], "shopping");
    assert_eq!(children["branches"][2][1]["handler"], "fix");

    let root = &blocks[1];
    assert_eq!(root["handler"], "renovate_interior");
    assert_eq!(root["queue_name"], "renovate");
    assert_eq!(root["retry"]["max_attempts"], 3);
    assert_eq!(root["retry"]["initial_backoff"], 1_000);
    assert_eq!(root["retry"]["backoff_multiplier"], 2.0);

    let file = "bullmq-flow.ts";
    assert_eq!(
        unmapped_at(&conversion, "emails.add").line,
        line_of(file, "emails.add")
    );
    assert_eq!(
        unmapped_at(&conversion, "opts.jobId").line,
        line_of(file, "jobId")
    );
    unmapped_at(&conversion, "failParentOnFailure");
    assert_eq!(
        unmapped_at(&conversion, "houseId").line,
        line_of(file, "house: houseId")
    );
    assert!(
        conversion
            .report
            .warnings
            .iter()
            .any(|w| w.contains("Worker for queue steps"))
    );
    for h in ["paint", "fix", "buy_materials", "renovate_interior"] {
        assert!(
            conversion.report.worker_handlers.iter().any(|w| w == h),
            "{h}"
        );
    }
}

#[test]
fn run_dispatches_code_importers() {
    let dir = tempfile::tempdir().unwrap();
    let out = dir.path().join("seq.json");
    let report = dir.path().join("report.json");
    run(
        ImportCmd::Temporal(ImportArgs {
            file: fixture_path("temporal-order"),
            out: Some(out.clone()),
            format: FormatArg::Json,
            name: Some("orders".into()),
            namespace: "default".into(),
            report: Some(report.clone()),
            zap: None,
            workflow: None,
        }),
        None,
    )
    .unwrap();
    assert_eq!(
        crate::seqdoc::read_document(&out).unwrap()["name"],
        "orders"
    );
    let report: Value = serde_json::from_str(&std::fs::read_to_string(report).unwrap()).unwrap();
    assert_eq!(report["source"], "temporal");
    assert!(
        report["unmapped"]
            .as_array()
            .unwrap()
            .iter()
            .all(|u| u["line"].as_u64().unwrap() > 0)
    );
}

#[test]
fn sources_without_workflows_are_rejected() {
    let file = SourceFile {
        path: "x.ts".into(),
        text: "export const x = 1;".into(),
    };
    let files = [file];
    assert!(convert_inngest(&files, &ConvertOptions::default()).is_err());
    assert!(convert_temporal(&files, &ConvertOptions::default()).is_err());
    assert!(convert_bullmq(&files, &ConvertOptions::default()).is_err());
    let err = convert_inngest(
        &load("inngest-onboarding.ts"),
        &ConvertOptions {
            workflow: Some("nope".into()),
            ..ConvertOptions::default()
        },
    )
    .unwrap_err();
    assert!(err.to_string().contains("user-onboarding"), "{err}");
}
