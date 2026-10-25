//! Real no-op catalog admission for cross-module ownership regressions.
use super::runs::{
    CatalogRunLimits, CatalogRunRequest,
    materialize::{InstanceLimits, Preparation},
};
use super::*;
use orch8_storage::testing::fixtures::{SequenceBuilder, step_with_params};
use orch8_types::{ids::InstanceId, sequence::BlockDefinition};

pub(crate) async fn scoped_run(mobile: &MobileStorage, account: &str, catalog: &str) -> InstanceId {
    let account = mobile
        .open_account_scope(account, "issuer", account)
        .await
        .unwrap();
    let scope = mobile
        .open_catalog_scope(&account, catalog, "source", "authority")
        .await
        .unwrap();
    let seq = SequenceBuilder::new("publisher")
        .name("flow")
        .block(BlockDefinition::Step(Box::new(step_with_params(
            "work",
            "noop",
            serde_json::json!({}),
        ))))
        .build();
    let digest = "b".repeat(64);
    mobile
        .accept_catalog_manifest(&scope, &[], 1, &digest)
        .await
        .unwrap();
    mobile
        .install_catalog_workflow(&scope, &serde_json::to_string(&seq).unwrap(), 1, &digest)
        .await
        .unwrap();
    sqlx::query("INSERT INTO mobile_catalog_execution_policy(account_id,catalog_id,name,version,manifest_version,manifest_digest,min_sdk_version,required_handlers) VALUES (?1,?2,'flow',1,1,?3,'0.0.0','[]')")
        .bind(scope.account_id()).bind(scope.catalog_id()).bind(&digest).execute(mobile.pool()).await.unwrap();
    mobile
        .mark_catalog_reconciled(&scope, 1, &digest)
        .await
        .unwrap();
    let run = mobile
        .prepare_catalog_run(
            &scope,
            CatalogRunRequest {
                workflow: "flow",
                version: None,
                input_json: r#"{"private":"SCOPED_CANARY"}"#,
                dedup_key: None,
            },
            CatalogRunLimits {
                global_records: 64,
                account_records: 32,
                catalog_records: 16,
                global_input_bytes: 1_048_576,
                account_input_bytes: 1_048_576,
                catalog_input_bytes: 1_048_576,
            },
        )
        .await
        .unwrap();
    let Preparation::Pending(snapshot) = mobile
        .prepare_materialization(&scope, run.id)
        .await
        .unwrap()
    else {
        panic!("pending")
    };
    let checked = snapshot
        .check(
            &std::collections::HashSet::from(["noop".to_string()]),
            env!("CARGO_PKG_VERSION"),
        )
        .unwrap();
    mobile
        .commit_materialization(
            &scope,
            checked,
            InstanceLimits {
                global: 64,
                account: 32,
                catalog: 16,
            },
            &tokio_util::sync::CancellationToken::new(),
        )
        .await
        .unwrap();
    run.id
}
