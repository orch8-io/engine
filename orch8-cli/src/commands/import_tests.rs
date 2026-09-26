//! Fixture-driven tests for `orch8 import n8n|zapier`. Every converted
//! sequence must decode strictly, validate, and pass preflight.

use super::*;
use orch8_engine::preflight::{RuntimeInventory, run_preflight};
use orch8_types::preflight::PreflightStatus;
use orch8_types::worker::WorkerRegistration;

fn fixture(name: &str) -> Value {
    let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("tests/fixtures/import")
        .join(name);
    serde_json::from_str(&std::fs::read_to_string(&path).unwrap()).unwrap()
}

fn inventory(workers: &[String]) -> RuntimeInventory {
    RuntimeInventory {
        worker_registrations: Some(
            workers
                .iter()
                .map(|h| WorkerRegistration {
                    worker_id: format!("w-{h}"),
                    handler_name: h.clone(),
                    queue_name: None,
                    version: None,
                    tenant_id: None,
                    last_seen_at: chrono::Utc::now(),
                })
                .collect(),
        ),
        version_pins: Some(vec![]),
        credentials: Some(vec![]),
        plugins: Some(vec![]),
        queue_dispatch: Some(vec![]),
        routing_rules: Some(vec![]),
        sequences: Some(vec![]),
    }
}

/// Collect every router condition in a sequence.
fn conditions(value: &Value, out: &mut Vec<String>) {
    match value {
        Value::Object(map) => {
            if let Some(c) = map.get("condition").and_then(Value::as_str) {
                out.push(c.to_string());
            }
            map.values().for_each(|v| conditions(v, out));
        }
        Value::Array(items) => items.iter().for_each(|v| conditions(v, out)),
        _ => {}
    }
}

/// Strict decode + structural validation + preflight: ready once the stub
/// workers exist, and without them only the worker check fails.
fn assert_passes_preflight(conversion: &Conversion) {
    let mut value = conversion.sequence.clone();
    value["id"] = json!(uuid::Uuid::now_v7());
    value["created_at"] = json!(chrono::Utc::now());
    let seq = orch8_types::sequence::deserialize_sequence_strict(&value)
        .unwrap_or_else(|e| panic!("strict decode failed: {e}\n{value:#}"));
    seq.validate().unwrap();

    for condition in {
        let mut all = Vec::new();
        conditions(&value, &mut all);
        all
    } {
        orch8_engine::expression::try_evaluate(
            &condition,
            &orch8_types::context::ExecutionContext::default(),
            &json!({}),
        )
        .unwrap_or_else(|e| panic!("condition `{condition}` does not parse: {e}"));
    }

    let now = chrono::Utc::now();
    let report = run_preflight(&seq, &inventory(&conversion.report.worker_handlers), now);
    assert!(
        report.is_ready(),
        "preflight not ready: {:#}",
        serde_json::to_value(&report).unwrap()
    );

    let bare = run_preflight(&seq, &inventory(&[]), now);
    for check in &bare.checks {
        if check.id == "handlers_have_workers" {
            let flagged: Vec<String> = check
                .findings
                .iter()
                .filter_map(|f| f.affected_resource.as_ref().map(|r| r.id.clone()))
                .collect();
            for handler in &flagged {
                assert!(
                    conversion.report.worker_handlers.contains(handler),
                    "unexpected external handler {handler}"
                );
            }
        } else {
            assert!(
                matches!(
                    check.status,
                    PreflightStatus::Pass | PreflightStatus::Warning
                ),
                "check {} is {:?}: {:?}",
                check.id,
                check.status,
                check.findings
            );
        }
    }
}

fn find_block<'a>(value: &'a Value, id: &str) -> Option<&'a Value> {
    match value {
        Value::Object(map) => {
            if map.get("id").and_then(Value::as_str) == Some(id) && map.contains_key("type") {
                return Some(value);
            }
            map.values().find_map(|v| find_block(v, id))
        }
        Value::Array(items) => items.iter().find_map(|v| find_block(v, id)),
        _ => None,
    }
}

#[test]
fn n8n_lead_intake_converts_and_passes_preflight() {
    let conversion =
        convert_n8n(&fixture("n8n-lead-intake.json"), &ConvertOptions::default()).unwrap();
    let seq = &conversion.sequence;
    let report = &conversion.report;
    assert_eq!(seq["name"], "lead-intake-and-routing");
    assert_eq!(seq["tenant_id"], "default");

    // Webhook trigger → trigger definition, not a block.
    assert_eq!(report.triggers.len(), 1);
    assert_eq!(report.triggers[0].kind, "webhook");
    assert_eq!(report.triggers[0].body["slug"], "lead-intake");

    // Set node → transform with translated expressions (webhook body → data).
    let normalize = find_block(seq, "normalize_lead").unwrap();
    assert_eq!(normalize["handler"], "transform");
    assert_eq!(normalize["params"]["email"], "{{data.email}}");
    assert_eq!(normalize["params"]["source"], "website");

    // IF → router on the Set output; both branches converge on Merge.
    let router = find_block(seq, "is_enterprise").unwrap();
    assert_eq!(router["type"], "router");
    assert_eq!(
        router["routes"][0]["condition"],
        "outputs.normalize_lead.employees >= 200"
    );
    let true_branch = &router["routes"][0]["blocks"];
    assert_eq!(true_branch[0]["id"], "create_hubspot_deal");
    assert_eq!(true_branch[0]["handler"], "http_request");
    assert_eq!(true_branch[0]["params"]["method"], "POST");
    assert!(
        true_branch[0]["params"]["body"]
            .as_str()
            .unwrap()
            .contains("{{outputs.normalize_lead.company}}")
    );
    assert_eq!(true_branch[0]["params"]["timeout_ms"], 15000);
    assert_eq!(true_branch[1]["handler"], "slack_post_message");
    assert_eq!(
        true_branch[1]["params"]["text"],
        "New enterprise lead: {{outputs.normalize_lead.company}} ({{outputs.normalize_lead.email}})"
    );
    let false_branch = &router["default"];
    assert_eq!(false_branch[0]["delay"]["duration"], 7_200_000);
    assert_eq!(false_branch[1]["handler"], "send_email");

    // After the join: code → worker stub with the original code; unmapped → TODO log stub.
    let top: Vec<&str> = seq["blocks"]
        .as_array()
        .unwrap()
        .iter()
        .map(|b| b["id"].as_str().unwrap())
        .collect();
    assert_eq!(
        top,
        [
            "normalize_lead",
            "is_enterprise",
            "score_lead",
            "log_to_sheet"
        ]
    );
    let code = find_block(seq, "score_lead").unwrap();
    assert_eq!(code["handler"], "n8n_code_score_lead");
    assert!(
        code["params"]["code"]
            .as_str()
            .unwrap()
            .contains("Score the lead")
    );
    let todo = find_block(seq, "log_to_sheet").unwrap();
    assert_eq!(todo["handler"], "log");
    assert_eq!(
        todo["params"]["original_type"],
        "n8n-nodes-base.googleSheets"
    );

    let mut handlers = report.worker_handlers.clone();
    handlers.sort();
    assert_eq!(
        handlers,
        ["n8n_code_score_lead", "send_email", "slack_post_message"]
    );
    assert!(
        report
            .todos
            .iter()
            .any(|t| t.node == "Log to Sheet" && t.handler == "log")
    );
    assert!(
        report
            .warnings
            .iter()
            .any(|w| w.contains("credentials are not exported"))
    );
    assert_passes_preflight(&conversion);
}

#[test]
fn n8n_schedule_switch_and_legacy_function_convert() {
    let conversion = convert_n8n(
        &fixture("n8n-daily-report.json"),
        &ConvertOptions {
            tenant_id: "acme".into(),
            name: Some("ops-report".into()),
            ..ConvertOptions::default()
        },
    )
    .unwrap();
    let seq = &conversion.sequence;
    assert_eq!(seq["name"], "ops-report");
    assert_eq!(seq["tenant_id"], "acme");
    let cron = &conversion.report.triggers[0];
    assert_eq!(cron.kind, "cron");
    assert_eq!(cron.body["cron_expr"], "0 7 * * 1-5");

    let router = find_block(seq, "route_by_status").unwrap();
    assert_eq!(router["routes"].as_array().unwrap().len(), 2);
    assert_eq!(
        router["routes"][0]["condition"],
        "outputs.fetch_status.status.indicator == \"none\""
    );
    assert_eq!(router["routes"][1]["blocks"][0]["id"], "page_on_call");
    assert_eq!(router["default"][0]["id"], "degraded_summary");
    let page = find_block(seq, "page_on_call").unwrap();
    assert!(
        page["params"]["body"]
            .as_str()
            .unwrap()
            .contains("{{outputs.fetch_status.status.description}}")
    );
    // The legacy Function node after the join runs once, after the router.
    let last = seq["blocks"].as_array().unwrap().last().unwrap();
    assert_eq!(last["handler"], "n8n_code_format_line_legacy");
    assert!(
        conversion
            .report
            .warnings
            .iter()
            .any(|w| w.contains("output 1 ends without reaching")),
        "{:?}",
        conversion.report.warnings
    );
    assert_passes_preflight(&conversion);
}

#[test]
fn zapier_filter_paths_delay_and_webhooks_convert() {
    let conversion = convert_zapier(
        &fixture("zapier-order-alerts.json"),
        &ConvertOptions::default(),
    )
    .unwrap();
    let seq = &conversion.sequence;
    let report = &conversion.report;
    assert_eq!(seq["name"], "large-shopify-orders-slack-fulfilment-api");
    assert_eq!(report.triggers[0].kind, "webhook");
    assert!(report.warnings.iter().any(|w| w.contains("holds 2 zaps")));

    // Filter → router whose only route holds the rest of the zap.
    let filter = &seq["blocks"][0];
    assert_eq!(filter["type"], "router");
    assert_eq!(
        filter["routes"][0]["condition"],
        "(data.total_price > 500) && (data.currency == \"USD\")"
    );
    assert!(filter.get("default").is_none());
    let rest = filter["routes"][0]["blocks"].as_array().unwrap();
    let ids: Vec<&str> = rest.iter().map(|b| b["id"].as_str().unwrap()).collect();
    assert_eq!(
        ids,
        [
            "format_total",
            "post_to_big_orders",
            "create_fulfilment_request",
            "wait_30_minutes",
            "split_by_country",
            "log_order"
        ]
    );
    assert_eq!(rest[0]["handler"], "transform");
    assert_eq!(
        rest[1]["params"]["text"],
        "Order {{data.order_number}} from {{data.customer.email}} for {{outputs.format_total.output}}"
    );
    assert_eq!(rest[2]["handler"], "http_request");
    assert_eq!(rest[2]["params"]["method"], "POST");
    assert_eq!(rest[2]["params"]["headers"]["X-Source"], "zapier");
    assert_eq!(
        serde_json::from_str::<Value>(rest[2]["params"]["body"].as_str().unwrap()).unwrap(),
        json!({"order_id": "{{data.id}}", "priority": "high"})
    );
    assert_eq!(rest[3]["delay"]["duration"], 1_800_000);
    let paths = &rest[4];
    assert_eq!(paths["type"], "router");
    assert_eq!(
        paths["routes"][0]["condition"],
        "data.shipping_address.country_code == \"US\""
    );
    assert_eq!(
        paths["routes"][0]["blocks"][0]["handler"],
        "zapier_code_compute_carrier"
    );
    assert_eq!(
        paths["routes"][1]["condition"],
        "data.shipping_address.country_code != \"US\""
    );
    assert_eq!(paths["routes"][1]["blocks"][0]["handler"], "send_email");
    assert_eq!(rest[5]["handler"], "log");
    assert_passes_preflight(&conversion);
}

#[test]
fn zapier_schedule_zap_is_selectable_and_becomes_cron() {
    let conversion = convert_zapier(
        &fixture("zapier-order-alerts.json"),
        &ConvertOptions {
            zap: Some("Weekly digest".into()),
            ..ConvertOptions::default()
        },
    )
    .unwrap();
    let report = &conversion.report;
    assert_eq!(report.triggers[0].kind, "cron");
    assert_eq!(report.triggers[0].body["cron_expr"], "0 9 * * 1");
    let blocks = conversion.sequence["blocks"].as_array().unwrap();
    assert_eq!(blocks[0]["handler"], "http_request");
    assert_eq!(
        blocks[0]["params"]["url"],
        "https://kpi.example.com/weekly?format=json"
    );
    assert_eq!(blocks[0]["params"]["method"], "GET");
    assert_eq!(
        blocks[1]["params"]["text"],
        "Weekly KPIs: {{outputs.fetch_kpis.summary}}"
    );
    assert_passes_preflight(&conversion);

    let err = convert_zapier(
        &fixture("zapier-order-alerts.json"),
        &ConvertOptions {
            zap: Some("nope".into()),
            ..ConvertOptions::default()
        },
    )
    .unwrap_err();
    assert!(err.to_string().contains("no zap"));
}

#[test]
fn yaml_output_round_trips_to_the_same_sequence() {
    let conversion =
        convert_n8n(&fixture("n8n-lead-intake.json"), &ConvertOptions::default()).unwrap();
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("lead.yaml");
    crate::seqdoc::write_document(&path, &conversion.sequence).unwrap();
    assert_eq!(
        crate::seqdoc::read_document(&path).unwrap(),
        conversion.sequence
    );
}

#[test]
fn run_writes_sequence_and_report_files() {
    let dir = tempfile::tempdir().unwrap();
    let out = dir.path().join("zap.yml");
    let report = dir.path().join("report.json");
    run(
        ImportCmd::Zapier(ImportArgs {
            file: std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
                .join("tests/fixtures/import/zapier-order-alerts.json"),
            out: Some(out.clone()),
            format: FormatArg::Json,
            name: None,
            namespace: "sales".into(),
            report: Some(report.clone()),
            zap: None,
        }),
        Some("tenant-a"),
    )
    .unwrap();
    let seq = crate::seqdoc::read_document(&out).unwrap();
    assert_eq!(seq["tenant_id"], "tenant-a");
    assert_eq!(seq["namespace"], "sales");
    let report: Value = serde_json::from_str(&std::fs::read_to_string(report).unwrap()).unwrap();
    assert_eq!(report["source"], "zapier");
    assert!(!report["todos"].as_array().unwrap().is_empty());
}

#[test]
fn non_exports_are_rejected() {
    assert!(convert_n8n(&json!({"foo": 1}), &ConvertOptions::default()).is_err());
    assert!(convert_zapier(&json!({"foo": 1}), &ConvertOptions::default()).is_err());
}

#[test]
fn expression_translation_handles_common_shapes() {
    let options = ConvertOptions::default();
    let conv = N8n {
        nodes: HashMap::new(),
        edges: HashMap::new(),
        in_degree: HashMap::new(),
        refs: HashMap::from([("Fetch".to_string(), "fetch".to_string())]),
        triggers: HashSet::new(),
        visited: HashSet::new(),
        options: &options,
    };
    assert_eq!(
        conv.translate_expr("$json.a.b", Some("prev")).as_deref(),
        Some("outputs.prev.a.b")
    );
    assert_eq!(
        conv.translate_expr("$json[\"a\"][0]", Some("data"))
            .as_deref(),
        Some("data.a.0")
    );
    assert_eq!(
        conv.translate_expr("$('Fetch').item.json.x", None)
            .as_deref(),
        Some("outputs.fetch.x")
    );
    assert_eq!(
        conv.translate_expr("$node[\"Fetch\"].json.y", None)
            .as_deref(),
        Some("outputs.fetch.y")
    );
    assert!(
        conv.translate_expr("$json.a.toUpperCase()", Some("p"))
            .is_none()
    );
    assert!(conv.translate_expr("DateTime.now()", Some("p")).is_none());
}
