# Logical backup and restore

```sh
orch8 backup --database-url sqlite:orch8.db --out backup.tar.gz --include-instances
orch8 restore backup.tar.gz --database-url sqlite:restored.db --include-instances
```

The archive includes sequences, triggers, cron schedules, queue routing rules,
and credentials. `--include-instances` adds instances, execution trees, every
historical block output, and the instance's externalized payload catalog.
Payload ownership, reference keys, creation times, and expiration times survive
the round trip. Restoring again preserves instances already present in the target.

## Source consistency

SQLite uses a consistent temporary snapshot, including committed WAL frames.
PostgreSQL reads every catalog, instance, tree, output, and payload through one
read-only repeatable-read transaction. Concurrent commits cannot change the view
partway through an archive. If the PostgreSQL session is lost or its transaction
ends, backup fails instead of reconnecting with a different snapshot. Retry the
backup command after correcting the connection problem.

The PostgreSQL snapshot retains the normal 30-second statement and 60-second
idle transaction guards. The CLI explicitly closes the dedicated backup source
on read success or failure, before encoding the archive. Ordinary server pools
retain their normal recycling behavior.

## Failed writes and retries

Each new instance, its execution tree, outputs, and payloads commit together.
A failed write rolls back that entire instance. Retry the same archive after
correcting the failure: completed instance imports are skipped and the failed
instance can be imported in full. Concurrent imports of one instance id publish
one complete copy.

Existing target instances remain unchanged. Partial instances left by older
binaries are also existing instances; use a verified clean target when recovering
from those older failed imports. This operation does not infer which existing
rows an operator intended to replace.

## Format compatibility

New archives use format 2 and include `externalized_state.jsonl` when instances
are included. Restore rejects an incomplete format 2 archive. Upgrade the restore
binary before using a format 2 archive with an older installation.

Format 1 archives remain readable. They omitted externalized payloads, so their
context or output references can be incomplete. Restore reports that limitation;
missing historical payloads cannot be reconstructed from those archives.

## Encryption and scope

Archives preserve values as stored, including ciphertexts. Use the source
`ORCH8_ENCRYPTION_KEY` when reading restored encrypted data. Credentials can be
plaintext when the source had no encryption key; keep the archive private.
External artifact stores and BYOK vault contents require their own backups.

Signals, worker tasks, KV state, checkpoints, continuity/effect ledgers, and audit
history are outside this logical archive. See [cloud migration](MIGRATING_TO_CLOUD.md)
for the continuity migration workflow.

## Custom Rust backends

`ResourceStore::list_externalized_entries` uses an exclusive reference-key cursor
and returns up to 1,000 records per page. `restore_externalized_entry` preserves
metadata atomically and rejects reference keys owned by another instance without
changing their payloads. Native and encrypted backends implement both operations.

`InstanceStore::restore_instance_snapshot` atomically publishes a validated
`InstanceSnapshot`. It returns `false` for an existing id and must leave the
entire instance absent after any failed write. Node ownership, duplicate
identities, missing parents, and cycles are validated before import; native
backends insert parents before children, including archives with UUIDv4 node ids.
