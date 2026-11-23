use std::collections::BTreeSet;
use std::path::Path;

use anyhow::{Context, Result, bail};
use orch8_storage::artifacts::ObjectArtifactStore;
use orch8_types::{ids::InstanceId, instance::TaskInstance};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use super::{Archive, RestoreReport};

pub(super) const REFERENCES: &str = "artifact_references.jsonl";
const PREFIX: &str = "artifact-sha256-";
const SUFFIX: &str = ".bin";
pub(super) const MAX_BLOB_BYTES: u64 = 100 * 1024 * 1024;

#[derive(Debug, Deserialize, Serialize)]
pub(super) struct Reference {
    pub instance_id: InstanceId,
    pub artifact_id: Uuid,
    pub sha256: String,
    pub bytes: u64,
}

pub(super) fn member_name(digest: &str) -> String {
    format!("{PREFIX}{digest}{SUFFIX}")
}

pub(super) fn member_digest(name: &str) -> Option<&str> {
    name.strip_prefix(PREFIX)?
        .strip_suffix(SUFFIX)
        .filter(|digest| {
            digest.len() == 64
                && digest
                    .bytes()
                    .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
        })
}

pub(super) fn verify(archive: &Archive) -> Result<()> {
    if !archive.members.contains_key(REFERENCES) {
        return Ok(());
    }
    let mut owned_instances = BTreeSet::new();
    for instance in archive.records::<TaskInstance>("instances.jsonl")? {
        owned_instances.insert(instance?.id);
    }
    let mut identities = BTreeSet::new();
    let mut referenced = BTreeSet::new();
    for reference in archive.records::<Reference>(REFERENCES)? {
        let reference = reference?;
        if !owned_instances.contains(&reference.instance_id)
            || !identities.insert((reference.instance_id, reference.artifact_id))
        {
            bail!("artifact reference belongs to an unknown instance or repeats an identifier");
        }
        let name = member_name(&reference.sha256);
        let member = archive
            .members
            .get(&name)
            .context("archive artifact payload is missing")?;
        if member_digest(&name) != Some(reference.sha256.as_str())
            || member.sha256 != reference.sha256
            || member.bytes != reference.bytes
            || member.bytes > MAX_BLOB_BYTES
        {
            bail!("archive artifact length or digest mismatch");
        }
        referenced.insert(name);
    }
    if archive
        .members
        .keys()
        .any(|name| member_digest(name).is_some() && !referenced.contains(name))
    {
        bail!("archive contains an unreferenced artifact payload");
    }
    Ok(())
}

pub(super) fn open(path: &Path) -> Result<ObjectArtifactStore> {
    Ok(ObjectArtifactStore::local(
        path.to_str()
            .context("artifact directory path is not UTF-8")?,
    )?)
}

pub(super) async fn restore(
    archive: &Archive,
    destination: Option<&ObjectArtifactStore>,
    dry_run: bool,
) -> Result<RestoreReport> {
    let mut report = RestoreReport::default();
    for reference in archive.records::<Reference>(REFERENCES)? {
        let reference = reference?;
        let key = format!("{}/{}", reference.instance_id, reference.artifact_id);
        let mut exists = false;
        if let Some(destination) = destination {
            if let Some(existing) = destination.get(&key).await? {
                if existing.len() as u64 != reference.bytes
                    || super::sha256_hex(&existing) != reference.sha256
                {
                    bail!("destination artifact {key} has different content");
                }
                exists = true;
            } else if !dry_run {
                let member = &archive.members[&member_name(&reference.sha256)];
                let bytes = std::fs::read(&member.path)?;
                destination
                    .put_with_id(
                        &reference.instance_id.to_string(),
                        reference.artifact_id,
                        "application/octet-stream",
                        bytes.into(),
                    )
                    .await?;
            }
        }
        report.tally("artifacts", exists);
    }
    Ok(report)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn artifact_reference_rejects_a_valid_archive_member_with_a_different_length() {
        use crate::commands::backup::{Manifest, StagedMember};
        let directory = tempfile::tempdir().unwrap();
        let instance = orch8_storage::testing::fixtures::InstanceBuilder::new(
            "archive_test",
            "default",
            orch8_types::ids::SequenceId::new(),
        )
        .build();
        let instance_path = directory.path().join("instances.jsonl");
        std::fs::write(&instance_path, serde_json::to_vec(&instance).unwrap()).unwrap();
        let reference_path = directory.path().join(REFERENCES);
        let digest = "a".repeat(64);
        std::fs::write(
            &reference_path,
            serde_json::to_vec(&Reference {
                instance_id: instance.id,
                artifact_id: Uuid::now_v7(),
                sha256: digest.clone(),
                bytes: 17,
            })
            .unwrap(),
        )
        .unwrap();
        let member = |path, bytes, sha256| StagedMember {
            path,
            bytes,
            sha256,
            records: 1,
        };
        let archive = Archive {
            manifest: Manifest {
                format: "orch8-backup".into(),
                format_version: 4,
                created_at: String::new(),
                orch8_version: String::new(),
                source_backend: "sqlite".into(),
                storage_schema_version: 0,
                include_instances: true,
                files: Vec::new(),
                warnings: Vec::new(),
            },
            members: [
                (
                    "instances.jsonl".into(),
                    member(instance_path, 0, String::new()),
                ),
                (REFERENCES.into(), member(reference_path, 0, String::new())),
                (
                    member_name(&digest),
                    member(directory.path().join("blob"), 18, digest),
                ),
            ]
            .into(),
            _staging: directory,
        };
        let error = verify(&archive).unwrap_err();
        assert!(error.to_string().contains("length or digest mismatch"));
    }

    #[test]
    fn blob_members_require_canonical_full_digests() {
        let digest = "a".repeat(64);
        assert_eq!(member_digest(&member_name(&digest)), Some(digest.as_str()));
        for invalid in [
            "../../artifact",
            "AB",
            "artifact-sha256-b.bin",
            "artifact-sha256-abc.jsonl",
        ] {
            assert!(member_digest(invalid).is_none());
        }
    }
}
