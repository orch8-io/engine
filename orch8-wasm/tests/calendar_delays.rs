use orch8_wasm::run_dry_with_options;
use serde_json::{Value, json};

fn timed_step(delay: &Value, options: &Value) -> Value {
    let graph = json!({"blocks":[{
        "type":"step","id":"wake","handler":"transform","delay":delay,
        "params":{"instant":"{{ now() }}"}
    }]});
    serde_json::from_str(&run_dry_with_options(
        &graph.to_string(),
        &options.to_string(),
    ))
    .unwrap()
}

#[test]
fn skips_a_calendar_holiday_and_the_following_weekend() {
    let run = timed_step(
        &json!({"duration":86_400_000,"business_days_only":true}),
        &json!({
            "start_at":"2026-12-24T15:00:00Z", "timezone":"America/New_York",
            "config":{"holidays":["2026-12-25"]}
        }),
    );
    assert_eq!(run["status"], "completed", "{run}");
    assert_eq!(run["virtual_duration_ms"], 345_600_000);
    assert_eq!(
        run["outputs"]["wake"]["instant"],
        "2026-12-28T15:00:00+00:00"
    );
}

#[test]
fn schedules_spring_gaps_and_autumn_overlaps_using_the_server_policy() {
    for (start, local, expected) in [
        (
            "2026-03-08T06:00:00Z",
            "2026-03-08T02:30:00",
            "2026-03-08T07:30:00+00:00",
        ),
        (
            "2026-11-01T04:00:00Z",
            "2026-11-01T01:30:00",
            "2026-11-01T05:30:00+00:00",
        ),
    ] {
        let run = timed_step(
            &json!({"duration":0,"fire_at_local":local,"timezone":"America/New_York"}),
            &json!({"start_at":start}),
        );
        assert_eq!(run["status"], "completed", "{run}");
        assert_eq!(run["virtual_duration_ms"], 5_400_000);
        assert_eq!(run["outputs"]["wake"]["instant"], expected);
    }
}
