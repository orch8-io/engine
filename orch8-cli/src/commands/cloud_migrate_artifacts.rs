use std::collections::BTreeSet;

use anyhow::{Context, Result, bail};
use orch8_storage::StorageBackend;
use orch8_types::ids::InstanceId;
use orch8_types::migration::{MAX_MIGRATION_ARTIFACT_BYTES, MigratedInstance, MigrationArtifact};
use serde_json::Value;
use uuid::Uuid;

use super::MigrationDestination;

pub(super) fn reachable_artifacts(instance: &MigratedInstance) -> BTreeSet<Uuid> {
    let prefix = format!("artifact://{}/", instance.instance.id);
    let mut artifacts = BTreeSet::new();
    let mut visit = |value| collect(value, &prefix, &mut artifacts);
    visit(&instance.instance.context.data);
    visit(&instance.instance.context.config);
    visit(&instance.instance.metadata);
    for output in &instance.block_outputs {
        visit(&output.output);
    }
    for payload in &instance.externalized_payloads {
        visit(&payload.payload);
    }
    for signal in &instance.pending_signals {
        visit(&signal.payload);
    }
    for row in &instance.durable_state.instance_kv {
        visit(&row.value);
    }
    for checkpoint in &instance.durable_state.checkpoints {
        visit(&checkpoint.checkpoint_data);
    }
    artifacts
}

fn collect(value: &Value, prefix: &str, artifacts: &mut BTreeSet<Uuid>) {
    match value {
        Value::String(uri) => {
            if let Some(id) = uri
                .strip_prefix(prefix)
                .and_then(|id| Uuid::parse_str(id).ok())
            {
                artifacts.insert(id);
            }
        }
        Value::Array(values) => {
            for value in values {
                collect(value, prefix, artifacts);
            }
        }
        Value::Object(object) => {
            for value in object.values() {
                collect(value, prefix, artifacts);
            }
        }
        _ => {}
    }
}

pub(super) async fn transfer(
    storage: &dyn StorageBackend,
    destination: &MigrationDestination<'_>,
    instance: InstanceId,
    artifacts: BTreeSet<Uuid>,
) -> Result<Vec<MigrationArtifact>> {
    let mut records = Vec::with_capacity(artifacts.len());
    for artifact_id in artifacts {
        let key = format!("{instance}/{artifact_id}");
        let bytes = storage
            .get_artifact(&key)
            .await?
            .with_context(|| format!("referenced artifact {key} is missing"))?;
        if bytes.len() > MAX_MIGRATION_ARTIFACT_BYTES {
            bail!("artifact {key} exceeds the migration size limit");
        }
        let digest = orch8_types::plugin::WasmArtifactDigest::for_bytes(&bytes);
        destination.stage_artifact(digest.as_str(), &bytes).await?;
        records.push(MigrationArtifact {
            artifact_id,
            sha256: digest.as_str().to_owned(),
            length: bytes.len(),
        });
    }
    Ok(records)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn references_are_deduplicated_and_scoped_to_the_instance() {
        let instance = InstanceId::new();
        let object = Uuid::now_v7();
        let uri = format!("artifact://{instance}/{object}");
        let foreign = format!("artifact://{}/{object}", InstanceId::new());
        let mut found = BTreeSet::new();
        collect(
            &json!([{"nested":{"uri":uri}}, uri, foreign, "artifact://not-an-id"]),
            &format!("artifact://{instance}/"),
            &mut found,
        );
        assert_eq!(found, BTreeSet::from([object]));
    }
}
