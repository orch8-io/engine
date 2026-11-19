use std::sync::Arc;

use orch8_mobile::{
    HandlerError, InstanceStateKind, MobileEngine, MobileEngineConfig, StepHandler,
};

struct SaveInspection;

impl StepHandler for SaveInspection {
    fn execute(&self, _step_name: String, _input: String) -> Result<String, HandlerError> {
        Ok("{}".into())
    }
}

#[test]
fn bundled_inspection_resumes_after_reopening_the_database() {
    let directory = tempfile::tempdir().unwrap();
    let database = directory.path().join("inspection.db");
    let workflow = std::fs::read_to_string(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/../examples/mobile-inspection/Sources/Inspection/Resources/field-inspection-offline.json"
    ))
    .expect("starter bundles a workflow");
    let open = || {
        let engine = MobileEngine::new(
            database.to_str().unwrap().to_owned(),
            MobileEngineConfig {
                telemetry_enabled: false,
                ..Default::default()
            },
        )
        .unwrap();
        engine
            .register_handler("save_inspection".into(), Arc::new(SaveInspection))
            .unwrap();
        engine
    };

    let engine = open();
    engine.load_sequence_from_json(workflow).unwrap();
    let id = engine
        .start(
            "field-inspection-offline".into(),
            r#"{"site_id":"A-12"}"#.into(),
            Some("inspection:A-12".into()),
        )
        .unwrap();
    engine.run_until_idle(20, 5_000).unwrap();
    assert_eq!(
        engine.get_instance(id.clone()).unwrap().state,
        InstanceStateKind::Waiting
    );
    engine.shutdown();
    drop(engine);

    let recovered = open();
    assert_eq!(recovered.active_instances().unwrap()[0].instance_id, id);
    recovered
        .complete_step(
            id.clone(),
            "record_inspection".into(),
            r#"{"value":"passed"}"#.into(),
        )
        .unwrap();
    recovered.run_until_idle(20, 5_000).unwrap();
    let instance = recovered.get_instance(id).unwrap();
    assert_eq!(instance.state, InstanceStateKind::Completed);
    let context: serde_json::Value = serde_json::from_str(&instance.context).unwrap();
    assert_eq!(context["inspection_result"], "passed");
    recovered.shutdown();
}
