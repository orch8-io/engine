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

Each new instance, its execution tree, outputs, payloads, and execution
checkpoints commit together with the signal inbox, instance KV state,
effect ledger, and queued worker tasks.
A failed write rolls back that entire instance. Retry the same archive after
correcting the failure: completed instance imports are skipped and the failed
instance can be imported in full. Concurrent imports of one instance id publish
one complete copy.

Existing target instances remain unchanged. Partial instances left by older
binaries are also existing instances; use a verified clean target when recovering
from those older failed imports. This operation does not infer which existing
rows an operator intended to replace.

## Format compatibility

Configuration archives use format 3. With `--include-instances`, format 5 archives include
`externalized_state.jsonl`, `checkpoints.jsonl`, `signals.jsonl`,
`instance_kv.jsonl`, `effect_scopes.jsonl`, `effect_receipts.jsonl`, and
`worker_tasks.jsonl`. Checkpoints retain
their IDs, instance IDs, timestamps, and exact stored payloads, including any
sequence or ownership metadata recorded in those payloads. Restore rejects
an incomplete archive and preserves checkpoint ordering for lookups. Signals retain
their IDs, queue timestamps, delivery flags, and delivery timestamps.
Restored pending signals are processed by queue timestamp and then by ID.
Instance KV records preserve the owner, key, value, last update, and absolute
expiry time. Expired records remain invisible to workflows after recovery.

Effect ledgers retain receipt identities, outcomes, attempt bindings, and commit
guards. A restored unresolved receipt still blocks dispatch, and a committed
step keeps its saved execution result. Use a full database backup for instances
in an active continuity transfer or compensation run.

Formats 1 through 4 remain readable. Formats 1 through 4 omit queued worker
tasks. Formats 1 and 2 omit execution checkpoints,
the signal inbox, instance KV state, and the effect ledger.
Format 1 also omits externalized payloads, so its context or output references
can be incomplete. Restore reports those omissions.

## Worker recovery

Restored worker tasks wait for a new claim. They retain their dispatch
bindings, attempt numbers, queue and placement settings, and the last
durable checkpoint and its sequence number. The target generates a new
task ID for each restored task and issues a fresh lease when a worker
claims it. A completion or checkpoint carrying a source lease is rejected
on the target.

If an in-flight effect has no resumable checkpoint, backup reports the
affected task and stops. Settle that task on the source before retrying.
Stop the source workers and scheduler before resuming work on the recovery
target. A logical backup reads the source database without changing its
leases.

## Encryption and scope

Archives preserve values as stored, including ciphertexts. Use the source
`ORCH8_ENCRYPTION_KEY` when reading restored encrypted data. Credentials can be
plaintext when the source had no encryption key; keep the archive private.
BYOK vault contents require their own backups.

### Artifact bytes

Add `--artifacts-dir PATH` with `--include-instances` to capture the local
artifact store's bytes. The archive stores each SHA-256 digest once
and preserves every instance and artifact ID in its reference index. Use
the flag on restore to install those bytes into the target store. Restore
verifies all references, lengths, and digests before opening the target.
Use an object-store backup for deployments backed by S3-compatible storage.

### Recipient-encrypted archives

With the [reference `age` executable](https://github.com/FiloSottile/age) on
`PATH`, repeat `--encrypt-recipient` to encrypt the entire compressed archive
to one or more native `age1…` public keys. A matching private identity can
restore or dry-run the archive.

```sh
age-keygen -o backup.agekey
recipient="$(age-keygen -y backup.agekey)"
orch8 backup --database-url sqlite:orch8.db --out backup.tar.gz.age \
  --encrypt-recipient "$recipient" --include-instances
orch8 restore backup.tar.gz.age --identity backup.agekey \
  --database-url sqlite:restored.db --include-instances
```

The age envelope is independent of `ORCH8_ENCRYPTION_KEY` and can be combined
with `--signing-key-file` and `--trusted-key-file`. The CLI builds and verifies
the inner tar.gz in a private temporary directory, then atomically publishes
the encrypted file. Restore authenticates the age stream and verifies the
archive before opening the target database.

Issued worker claims, continuity handoffs, and audit history are outside this
logical archive.
See [cloud migration](MIGRATING_TO_CLOUD.md) for the continuity migration
workflow.

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

### Signed manifests

Use `orch8 backup --signing-key-file backup.seed` to sign the manifest with
Ed25519. The file contains the base64-encoded 32-byte signing seed. Keep it
private. The detached signature authenticates the exact manifest bytes and
the manifest records the SHA-256 digest of each archive member.

Pass `--trusted-key-file backup.pub` to `orch8 restore`, including with
`--dry-run`. The file contains the corresponding base64-encoded 32-byte
public key obtained independently of the archive. Supplying a trusted key
requires a valid signature. A signed archive requires an explicit trusted
key. Signature and member checks complete before restore opens the target.
Legacy unsigned archives can still be restored without a trusted key.

### Backup creation and temporary storage

Backup reads a consistent snapshot in pages and writes JSONL members to a
private temporary directory. It releases the source before streaming those
members into the compressed archive. The complete archive is verified in a
temporary file beside the destination before it replaces the previous
backup. Memory use scales with the largest record and the bounded page
sizes. The temporary member bodies and compressed archive are each limited
to 512 MiB.

### Restore verification and temporary storage

Restore reads the compressed archive incrementally and stages its members in
a private temporary directory. The compressed input and total expanded
member bodies each have a 512 MiB limit. Restore checks the gzip trailer,
manifest, member checksums, and JSON record types before opening the target.
It removes the staging directory when the operation finishes. Restoring
instances still groups their execution data in memory for atomic import.
