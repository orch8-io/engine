//! Page a consistent source into private JSONL files, then stream a tar.gz.
use std::collections::BTreeMap;
use std::fs::File;
use std::io::{self, BufWriter, Seek as _, Write};
use std::path::{Path, PathBuf};

use anyhow::{Context as _, Result, bail};
use ed25519_dalek::{Signer as _, SigningKey};
use orch8_storage::{StorageBackend, artifacts::ObjectArtifactStore, restore::EffectArchive};
use orch8_types::filter::{InstanceFilter, Pagination};
use serde::Serialize;
use sha2::{Digest as _, Sha256};

use super::{
    CREDENTIAL_WARNING, FORMAT, FORMAT_VERSION, MANIFEST_SIGNATURE, MAX_ARCHIVE_BYTES,
    MAX_MANIFEST_BYTES, MAX_MEMBER_BYTES, MAX_TOTAL_UNCOMPRESSED_BYTES, Manifest, ManifestFile,
    PAGE, SourceGuard, WORKER_TASK_FORMAT_VERSION, WORKER_TASKS, artifacts,
    create_private_archive_member, credential_json, hex_bytes, private_archive_staging,
    read_archive_from, signature_message, trigger_json,
};

const CATALOGS: [&str; 5] = [
    "sequences.jsonl",
    "triggers.jsonl",
    "cron.jsonl",
    "queue_routing.jsonl",
    "credentials.jsonl",
];
const EXECUTION: [&str; 10] = [
    "instances.jsonl",
    "execution_tree.jsonl",
    "block_outputs.jsonl",
    "externalized_state.jsonl",
    "checkpoints.jsonl",
    "signals.jsonl",
    "instance_kv.jsonl",
    "effect_scopes.jsonl",
    "effect_receipts.jsonl",
    WORKER_TASKS,
];
// Payload-bearing records can be large. A single-row page puts a bound on
// their aggregate memory without reducing the supported maximum record size.
const PAYLOAD_PAGE: u32 = 1;

struct Member {
    name: &'static str,
    path: PathBuf,
    writer: BufWriter<File>,
    hash: Sha256,
    written: u64,
    records: u64,
}

pub(super) struct BackupSpool {
    members: Vec<Member>,
    blobs: BTreeMap<String, (ManifestFile, PathBuf)>,
    expanded_bytes: u64,
    include_instances: bool,
    directory: tempfile::TempDir,
}

impl BackupSpool {
    pub(super) fn new(include_instances: bool) -> Result<Self> {
        let directory = private_archive_staging()?;
        let names = CATALOGS.iter().chain(if include_instances {
            &EXECUTION[..]
        } else {
            &[]
        });
        let members = names
            .map(|name| new_member(&directory, name))
            .collect::<io::Result<_>>()?;
        Ok(Self {
            members,
            blobs: BTreeMap::new(),
            expanded_bytes: 0,
            include_instances,
            directory,
        })
    }

    pub(super) fn enable_artifacts(&mut self) -> Result<()> {
        self.members
            .push(new_member(&self.directory, artifacts::REFERENCES)?);
        Ok(())
    }

    async fn collect_artifacts(
        &mut self,
        store: &ObjectArtifactStore,
        instance: orch8_types::ids::InstanceId,
    ) -> Result<()> {
        let owner = instance.to_string();
        let prefix = format!("{owner}/");
        for listed in store.list(&owner).await? {
            let stored_id = listed
                .key
                .strip_prefix(&prefix)
                .context("invalid source artifact owner")?;
            let artifact_id = stored_id.parse()?;
            if listed.size > artifacts::MAX_BLOB_BYTES {
                bail!("artifact {} exceeds the backup size limit", listed.key);
            }
            let bytes = store
                .get(&listed.key)
                .await?
                .context("source artifact disappeared during capture")?;
            if bytes.len() as u64 != listed.size {
                bail!("artifact {} changed during capture", listed.key);
            }
            let digest = super::sha256_hex(&bytes);
            let name = artifacts::member_name(&digest);
            if !self.blobs.contains_key(&name) {
                if listed.size > MAX_TOTAL_UNCOMPRESSED_BYTES - self.expanded_bytes {
                    bail!("artifact payloads exceed the expanded backup size limit");
                }
                let path = self.directory.path().join(&name);
                create_private_archive_member(&path)?.write_all(&bytes)?;
                self.blobs.insert(
                    name.clone(),
                    (
                        ManifestFile {
                            name,
                            records: 1,
                            bytes: listed.size,
                            sha256: digest.clone(),
                        },
                        path,
                    ),
                );
                self.expanded_bytes += listed.size;
            }
            self.append(
                artifacts::REFERENCES,
                &artifacts::Reference {
                    instance_id: instance,
                    artifact_id,
                    sha256: digest,
                    bytes: listed.size,
                },
            )?;
        }
        Ok(())
    }

    fn append(&mut self, name: &str, value: &impl Serialize) -> Result<()> {
        let member = self
            .members
            .iter_mut()
            .find(|member| member.name == name)
            .with_context(|| format!("unconfigured backup member {name}"))?;
        let mut writer = RecordWriter {
            member,
            expanded_bytes: &mut self.expanded_bytes,
        };
        serde_json::to_writer(&mut writer, value).with_context(|| format!("stage {name}"))?;
        writer.write_all(b"\n")?;
        member.records += 1;
        Ok(())
    }

    pub(super) async fn collect(
        &mut self,
        storage: &dyn StorageBackend,
        source: &SourceGuard,
        artifacts: Option<&ObjectArtifactStore>,
    ) -> Result<()> {
        let mut offset = 0;
        loop {
            let page = storage
                .list_sequences(None, None, PAYLOAD_PAGE, offset)
                .await?;
            if page.is_empty() {
                break;
            }
            for sequence in &page {
                self.append("sequences.jsonl", sequence)?;
            }
            offset += PAYLOAD_PAGE;
        }

        for catalog in [
            Catalog::Triggers,
            Catalog::Cron,
            Catalog::QueueRouting,
            Catalog::Credentials,
        ] {
            let mut offset = 0;
            loop {
                let ids = catalog.page(source, offset).await?;
                if ids.is_empty() {
                    break;
                }
                for id in &ids {
                    catalog.append(storage, id, self).await?;
                }
                offset += u64::try_from(ids.len())?;
            }
        }

        if self.include_instances {
            let mut offset = 0;
            loop {
                let page = storage
                    .list_instances(
                        &InstanceFilter::default(),
                        &Pagination {
                            offset,
                            limit: PAYLOAD_PAGE,
                            sort_ascending: true,
                        },
                    )
                    .await?;
                if page.is_empty() {
                    break;
                }
                for instance in &page {
                    if orch8_storage::delegation_publication::is_proxy_record(storage, instance)
                        .await?
                    {
                        bail!(
                            "instance backup cannot preserve delegation relay authority; use a full database backup for relay state"
                        );
                    }
                    self.append("instances.jsonl", instance)?;
                    self.collect_execution(storage, instance).await?;
                    if let Some(artifacts) = artifacts {
                        self.collect_artifacts(artifacts, instance.id).await?;
                    }
                }
                offset += u64::from(PAYLOAD_PAGE);
            }
        }
        Ok(())
    }

    async fn collect_execution(
        &mut self,
        storage: &dyn StorageBackend,
        instance: &orch8_types::instance::TaskInstance,
    ) -> Result<()> {
        let id = instance.id;
        let mut after = None;
        loop {
            let page = storage.get_execution_tree_page(id, after, PAGE).await?;
            for node in &page {
                self.append("execution_tree.jsonl", node)?;
            }
            if page.len() < PAGE as usize {
                break;
            }
            after = page.last().map(|node| node.id);
        }

        let mut offset = 0;
        loop {
            let outputs = storage.get_outputs_page(id, PAYLOAD_PAGE, offset).await?;
            if outputs.is_empty() {
                break;
            }
            for output in &outputs {
                self.append("block_outputs.jsonl", output)?;
            }
            offset += u64::from(PAYLOAD_PAGE);
        }

        let mut after = None;
        loop {
            let payloads = storage
                .list_externalized_entries(id, after.as_deref(), PAYLOAD_PAGE)
                .await?;
            if payloads.is_empty() {
                break;
            }
            for payload in &payloads {
                self.append("externalized_state.jsonl", payload)?;
            }
            after = payloads.last().map(|payload| payload.ref_key.clone());
        }
        let mut after = None;
        loop {
            let checkpoints = storage
                .list_checkpoint_page(id, after, PAYLOAD_PAGE)
                .await?;
            if checkpoints.is_empty() {
                break;
            }
            for checkpoint in &checkpoints {
                self.append("checkpoints.jsonl", checkpoint)?;
            }
            after = checkpoints.last().map(|checkpoint| checkpoint.id);
        }
        let mut after = None;
        loop {
            let signals = storage
                .list_signal_history_page(id, after, PAYLOAD_PAGE)
                .await?;
            if signals.is_empty() {
                break;
            }
            for signal in &signals {
                self.append("signals.jsonl", signal)?;
            }
            after = signals.last().map(|signal| signal.id);
        }
        let mut after = None;
        loop {
            let entries = storage
                .list_instance_kv_page(id, after.as_deref(), PAYLOAD_PAGE)
                .await?;
            if entries.is_empty() {
                break;
            }
            for entry in &entries {
                self.append("instance_kv.jsonl", entry)?;
            }
            after = entries.last().map(|entry| entry.key.clone());
        }
        let scope = super::effect_archive(storage, instance).await?;
        self.collect_effects(storage, scope.as_ref()).await?;
        let mut after = None;
        loop {
            let page = super::worker_task_archive_page(
                storage,
                instance,
                scope.as_ref(),
                after,
                PAYLOAD_PAGE,
            )
            .await?;
            if page.is_empty() {
                break;
            }
            for record in &page {
                self.append(WORKER_TASKS, record)?;
            }
            after = page.last().map(|record| record.task.id);
        }
        Ok(())
    }

    async fn collect_effects(
        &mut self,
        storage: &dyn StorageBackend,
        scope: Option<&EffectArchive>,
    ) -> Result<()> {
        if let Some(scope) = scope {
            self.append("effect_scopes.jsonl", scope)?;
            let mut after = None;
            loop {
                let page = storage
                    .page_effect_receipts(
                        &scope.owner.tenant_id,
                        scope.owner.continuity_id,
                        after.as_ref(),
                        PAYLOAD_PAGE,
                    )
                    .await?;
                for receipt in &page.receipts {
                    self.append("effect_receipts.jsonl", receipt)?;
                }
                after = page.next;
                if after.is_none() {
                    break;
                }
            }
        }
        Ok(())
    }

    pub(super) fn publish(
        self,
        destination: &Path,
        source_backend: &str,
        signing_key: Option<&SigningKey>,
    ) -> Result<(Manifest, u64)> {
        let format_version = if self.include_instances {
            WORKER_TASK_FORMAT_VERSION
        } else {
            FORMAT_VERSION
        };
        let mut files = Vec::with_capacity(self.members.len());
        let mut bodies = Vec::with_capacity(self.members.len());
        let mut warnings = Vec::new();
        for member in self.members {
            member.writer.into_inner()?;
            if member.name == "credentials.jsonl" && member.records > 0 {
                warnings.push(CREDENTIAL_WARNING.into());
            }
            files.push(ManifestFile {
                name: member.name.into(),
                records: member.records,
                bytes: member.written,
                sha256: hex_bytes(&member.hash.finalize()),
            });
            bodies.push(member.path);
        }
        for (metadata, path) in self.blobs.into_values() {
            files.push(metadata);
            bodies.push(path);
        }
        if self.include_instances {
            warnings.push("instance backups exclude issued worker claims and audit history".into());
        }
        let manifest = Manifest {
            format: FORMAT.into(),
            format_version,
            created_at: chrono::Utc::now().to_rfc3339(),
            orch8_version: env!("CARGO_PKG_VERSION").into(),
            source_backend: source_backend.into(),
            storage_schema_version: orch8_storage::STORAGE_SCHEMA_VERSION,
            include_instances: self.include_instances,
            files,
            warnings,
        };
        let manifest_bytes = serde_json::to_vec_pretty(&manifest)?;
        let signature =
            signing_key.map(|key| key.sign(&signature_message(&manifest_bytes)).to_bytes());
        let header_bytes = manifest_bytes.len() as u64 + if signature.is_some() { 64 } else { 0 };
        if manifest_bytes.len() as u64 > MAX_MANIFEST_BYTES
            || self.expanded_bytes + header_bytes > MAX_TOTAL_UNCOMPRESSED_BYTES
        {
            bail!("backup manifest exceeds the archive size limit");
        }

        let mut compressed_size = 0;
        crate::atomic_write_private_with(destination, |file| {
            let encoder = flate2::write::GzEncoder::new(
                LimitedOutput {
                    file: &mut *file,
                    written: 0,
                },
                flate2::Compression::default(),
            );
            let mut archive = tar::Builder::new(encoder);
            write_member(
                &mut archive,
                "manifest.json",
                manifest_bytes.len() as u64,
                manifest_bytes.as_slice(),
            )?;
            if let Some(signature) = &signature {
                write_member(&mut archive, MANIFEST_SIGNATURE, 64, signature.as_slice())?;
            }
            for (member, path) in manifest.files.iter().zip(&bodies) {
                write_member(&mut archive, &member.name, member.bytes, File::open(path)?)?;
            }
            let output = archive.into_inner()?.finish()?;
            compressed_size = output.written;
            // Drop the source spool before independent verification to avoid
            // holding two copies of the expanded archive on disk.
            drop(self.directory);
            file.rewind()?;
            let trusted = signing_key.map(SigningKey::verifying_key);
            read_archive_from(file, trusted.as_ref())?;
            Ok(())
        })?;
        Ok((manifest, compressed_size))
    }
}

fn new_member(directory: &tempfile::TempDir, name: &'static str) -> io::Result<Member> {
    let path = directory.path().join(name);
    Ok(Member {
        name,
        writer: BufWriter::new(create_private_archive_member(&path)?),
        path,
        hash: Sha256::new(),
        written: 0,
        records: 0,
    })
}

fn write_member(
    archive: &mut tar::Builder<impl Write>,
    name: &str,
    bytes: u64,
    reader: impl io::Read,
) -> Result<()> {
    let mut header = tar::Header::new_gnu();
    header.set_size(bytes);
    header.set_mode(0o600);
    header.set_entry_type(tar::EntryType::Regular);
    header.set_cksum();
    archive.append_data(&mut header, name, reader)?;
    Ok(())
}

struct RecordWriter<'a> {
    member: &'a mut Member,
    expanded_bytes: &'a mut u64,
}

impl Write for RecordWriter<'_> {
    fn write(&mut self, data: &[u8]) -> io::Result<usize> {
        let requested = data.len() as u64;
        if requested > MAX_MEMBER_BYTES - self.member.written
            || requested > MAX_TOTAL_UNCOMPRESSED_BYTES - *self.expanded_bytes
        {
            return Err(io::Error::other(
                "expanded backup exceeds the archive size limit",
            ));
        }
        let count = self.member.writer.write(data)?;
        self.member.hash.update(&data[..count]);
        self.member.written += count as u64;
        *self.expanded_bytes += count as u64;
        Ok(count)
    }

    fn flush(&mut self) -> io::Result<()> {
        self.member.writer.flush()
    }
}

struct LimitedOutput<'a> {
    file: &'a mut File,
    written: u64,
}

impl Write for LimitedOutput<'_> {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        if bytes.len() as u64 > MAX_ARCHIVE_BYTES as u64 - self.written {
            return Err(io::Error::other(
                "compressed backup exceeds the archive size limit",
            ));
        }
        let count = self.file.write(bytes)?;
        self.written += count as u64;
        Ok(count)
    }

    fn flush(&mut self) -> io::Result<()> {
        self.file.flush()
    }
}

#[derive(Clone, Copy)]
enum Catalog {
    Triggers,
    Cron,
    QueueRouting,
    Credentials,
}

impl Catalog {
    async fn page(self, source: &SourceGuard, offset: u64) -> Result<Vec<String>> {
        // Enumerate only IDs. Domain store getters decode a single record
        // without changing how stored secrets and backend types are handled.
        let offset = i64::try_from(offset)?;
        if let Some(pool) = &source.sqlite_pool {
            let query = match self {
                Self::Triggers => "SELECT slug FROM triggers ORDER BY slug LIMIT ? OFFSET ?",
                Self::Cron => "SELECT id FROM cron_schedules ORDER BY id LIMIT ? OFFSET ?",
                Self::QueueRouting => {
                    "SELECT id FROM queue_routing_rules ORDER BY id LIMIT ? OFFSET ?"
                }
                Self::Credentials => "SELECT id FROM credentials ORDER BY id LIMIT ? OFFSET ?",
            };
            return Ok(sqlx::query_scalar(query)
                .bind(i64::from(PAGE))
                .bind(offset)
                .fetch_all(pool)
                .await?);
        }
        if let Some(pool) = &source.postgres_pool {
            let query = match self {
                Self::Triggers => "SELECT slug FROM triggers ORDER BY slug LIMIT $1 OFFSET $2",
                Self::Cron => {
                    "SELECT id::text FROM cron_schedules ORDER BY cron_schedules.id LIMIT $1 OFFSET $2"
                }
                Self::QueueRouting => {
                    "SELECT id::text FROM queue_routing_rules ORDER BY queue_routing_rules.id LIMIT $1 OFFSET $2"
                }
                Self::Credentials => "SELECT id FROM credentials ORDER BY id LIMIT $1 OFFSET $2",
            };
            return Ok(sqlx::query_scalar(query)
                .bind(i64::from(PAGE))
                .bind(offset)
                .fetch_all(pool)
                .await?);
        }
        bail!("missing backup source")
    }

    async fn append(
        self,
        storage: &dyn StorageBackend,
        id: &str,
        spool: &mut BackupSpool,
    ) -> Result<()> {
        let missing = || anyhow::anyhow!("catalog record disappeared from the backup snapshot");
        match self {
            Self::Triggers => {
                let trigger = storage.get_trigger(None, id).await?.ok_or_else(missing)?;
                spool.append("triggers.jsonl", &trigger_json(&trigger)?)
            }
            Self::Cron => {
                let cron = storage
                    .get_cron_schedule(id.parse()?)
                    .await?
                    .ok_or_else(missing)?;
                spool.append("cron.jsonl", &cron)
            }
            Self::QueueRouting => {
                let rule = storage
                    .get_queue_routing_rule(id.parse()?)
                    .await?
                    .ok_or_else(missing)?;
                spool.append("queue_routing.jsonl", &rule)
            }
            Self::Credentials => {
                let credential = storage
                    .get_credential(None, id)
                    .await?
                    .ok_or_else(missing)?;
                spool.append("credentials.jsonl", &credential_json(&credential)?)
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn archive_budget_failure_keeps_existing_backup() {
        let directory = tempfile::tempdir().unwrap();
        let destination = directory.path().join("last-good.tar.gz");
        std::fs::write(&destination, b"last good").unwrap();
        let mut spool = BackupSpool::new(true).unwrap();
        spool.expanded_bytes = MAX_TOTAL_UNCOMPRESSED_BYTES - 1;
        assert!(spool.append("block_outputs.jsonl", &"too large").is_err());
        assert!(spool.publish(&destination, "sqlite", None).is_err());
        assert_eq!(std::fs::read(&destination).unwrap(), b"last good");
    }
}
