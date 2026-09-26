# Single-node production on SQLite + Litestream

> **Stability: beta**, shipped and tested; may change in a minor release with a changelog note. Note: SQLite itself is a supported server backend; this Litestream topology is newer than the PostgreSQL path.

Orch8 can run in production on SQLite. For a small team this can be the cheapest
durable setup: one container, one volume, and continuous off-host replication to
S3-compatible storage through [Litestream](https://litestream.io). This page covers
what that setup guarantees, what it doesn't, and how to rehearse a restore.

If any of the [limits](#limits-read-these-first) are a problem for you, use PostgreSQL
([Deployment](DEPLOYMENT.md)).

## Is SQLite allowed outside dev?

Yes. `orch8-server` accepts `database.backend = "sqlite"` (or
`ORCH8_STORAGE_BACKEND=sqlite`) in every mode. It doesn't need a special flag and doesn't
print a "dev only" warning. The published container image defaults to SQLite at
`/data/orch8.db`. The secure-by-default rules still apply: startup fails without
`ORCH8_API_KEY` and `ORCH8_ENCRYPTION_KEY` unless you pass the explicit `--insecure*`
flags.

What the engine does with a file-backed SQLite database (`orch8-storage/src/sqlite/mod.rs`):

| Setting | Value | Consequence |
|---|---|---|
| Journal mode | WAL | Readers don't block the writer. Litestream requires WAL. |
| Busy timeout | 5 s | A writer waits up to 5 s for the lock before failing with `SQLITE_BUSY`. |
| Connection pool | 8 connections, fixed | `database.max_connections` doesn't apply to file-backed SQLite. |
| Foreign keys | on | Cascading deletes work as they do on PostgreSQL. |
| Schema | created and reconciled at boot | A database from an older binary gains new tables and columns on startup. `ORCH8_RUN_MIGRATIONS` and `orch8 migrate` apply only to PostgreSQL. |

## Limits (read these first)

- **One engine process per database file.** Run exactly one `orch8-server` against the file.
  Don't put the file on NFS, SMB, EFS, or any other network filesystem, and don't point two
  containers at the same volume. SQLite locking isn't reliable across network
  filesystems, and a Litestream replica must have exactly one writer.
- **No scale-out.** The split `control` / `executor` [node roles](NODE_ROLES.md) and
  multiple replicas need a shared database, which means PostgreSQL. On SQLite, run the
  `all_in_one` role with `replicas: 1`. The Helm chart refuses SQLite with more than one replica.
- **Single writer.** Every state transition goes through one SQLite write lock. Throughput
  depends on your disk's fsync latency and your workload. We don't publish a number for it.
  Measure your workload with the [load generator](../loadgen/README.md) before you commit.
- **Downtime during restarts and upgrades.** With one node, a deploy stops the engine.
  Timers and in-flight steps resume after restart: running instances are recovered by the
  stale-instance reaper after `stale_instance_threshold_secs`, which defaults to 300 s.
- **Your recovery point is Litestream's, not SQLite's.** Litestream ships WAL frames
  asynchronously (`sync-interval: 1s` in the example). If the host is lost, you lose the
  writes that hadn't been shipped yet. A step that completed in that window is still
  pending after a restore, so it runs again. Make side-effecting steps idempotent with
  idempotency keys, as you would for any at-least-once system.
- **Litestream replicates only the database file.** It doesn't copy:
  - the encryption key. Without the same `ORCH8_ENCRYPTION_KEY`, encrypted context,
    credentials, and outputs in a restored database can't be read. Store the key in a
    secret manager separate from the bucket.
  - artifacts on the `local` backend. Use `ORCH8_ARTIFACT_BACKEND=s3` or `none`, or back
    up the artifact directory separately.
  - your config file and environment.
- **Triggers.** NATS and file-watch triggers are fine here because there's only one node.
  The duplicate-fan-out warning in [Deployment](DEPLOYMENT.md#high-availability) applies
  only to multi-replica setups.

## Topology

```
          ┌──────────── volume: orch8-data (/data) ────────────┐
restore ─▶│ orch8.db (+ -wal, -shm)                            │
(init)    └───────▲───────────────────────────▲────────────────┘
                  │ read/write                 │ read WAL
             engine (all_in_one, uid 999)   litestream replicate ──▶ S3 / R2 / MinIO
```

1. `restore` runs once at start. If `/data/orch8.db` is missing and a replica exists, it
   restores the database. Otherwise it does nothing.
2. `engine` opens the database in WAL mode.
3. `litestream` sidecar continuously ships WAL frames and daily snapshots.

## Run it

The files are in [`deploy/sqlite-litestream/`](../deploy/sqlite-litestream/):
`docker-compose.yml` and `litestream.yml`.

```bash
cd deploy/sqlite-litestream
export ORCH8_API_KEY=$(openssl rand -hex 32)
export ORCH8_ENCRYPTION_KEY=$(openssl rand -hex 32)   # store this in your secret manager
export LITESTREAM_BUCKET=my-orch8-backups
export LITESTREAM_ACCESS_KEY_ID=...  LITESTREAM_SECRET_ACCESS_KEY=...
# For R2 / B2 / MinIO also set:
# export LITESTREAM_ENDPOINT=https://<account>.r2.cloudflarestorage.com LITESTREAM_FORCE_PATH_STYLE=true
docker compose up -d
docker compose exec engine orch8 --url http://127.0.0.1:8080 health
```

The compose file pins `litestream/litestream:0.3.13`. Override it with
`LITESTREAM_VERSION`. The engine image tag comes from `ORCH8_VERSION`, which defaults to
`latest`. Pin it in production.

### Kubernetes

Use the [Helm chart](../deploy/helm/orch8/README.md) with `storage.backend=sqlite`
(`deploy/helm/orch8/ci/sqlite-values.yaml` is a working example). The chart creates a PVC,
refuses more than one replica, autoscaling, split mode, or a gateway, and switches the
Deployment to a `Recreate` strategy so the old and new pods never write the file at the
same time. The chart doesn't include Litestream. Add it the same way as here, for example
through a Kustomize patch or a post-renderer: an init container running
`litestream restore -if-db-not-exists -if-replica-exists` and a sidecar running
`litestream replicate`, both mounting the data PVC and running as uid/gid 999.

## Monitoring

- `GET /health/ready` returns 503 when the database ping fails or the engine loop has stopped.
- Watch Litestream's logs for `wal segment written`. If none appear for more than a few
  minutes while the engine is busy, replication has stalled.
- Litestream can expose Prometheus metrics with `addr: ":9090"` in `litestream.yml`.
  Alert when replication lag grows.
- Watch the database file size. Set `ORCH8_INSTANCE_RETENTION_SECS` so that terminal
  instances are swept. See the production checklist in [Deployment](DEPLOYMENT.md#production-checklist).

## Restore drill

Rehearse this before you rely on it. The procedure below was run against the files in
`deploy/sqlite-litestream/` with the MinIO profile. After the volume was deleted, the
restored engine reported the same instances in the same states.

```bash
cd deploy/sqlite-litestream
export ORCH8_API_KEY=drill-key ORCH8_ENCRYPTION_KEY=$(openssl rand -hex 32)
export LITESTREAM_BUCKET=orch8-drill LITESTREAM_ACCESS_KEY_ID=minioadmin LITESTREAM_SECRET_ACCESS_KEY=minioadmin123
export LITESTREAM_ENDPOINT=http://minio:9000 LITESTREAM_FORCE_PATH_STYLE=true

# 1. Start local S3, create the bucket, start the stack.
docker compose --profile local-s3 up -d --wait minio
docker compose exec -T minio mc alias set local http://127.0.0.1:9000 minioadmin minioadmin123
docker compose exec -T minio mc mb --ignore-existing local/orch8-drill
docker compose --profile local-s3 up -d --wait engine litestream

# 2. Write data: create a sequence and some instances, then record what you see.
orch8 --url http://127.0.0.1:8080/api/v1 --api-key "$ORCH8_API_KEY" --tenant-id drill instance list

# 3. Wait a few seconds for Litestream to ship WAL, then simulate losing the host.
docker compose --profile local-s3 stop engine litestream
docker compose --profile local-s3 rm -f engine litestream restore volume-init
docker volume rm orch8-sqlite_orch8-data

# 4. Start again. `restore` sees an empty volume and pulls the replica.
docker compose --profile local-s3 up -d --wait engine litestream
docker compose --profile local-s3 logs restore | tail   # "renaming database from temporary location"

# 5. Compare the instance list with step 2. Then clean up.
docker compose --profile local-s3 down -v
```

To restore a specific point in time on a scratch host instead of the latest state, run
`litestream restore -config /etc/litestream.yml -timestamp 2026-09-01T12:00:00Z -o /data/orch8.db /data/orch8.db`
with the engine stopped. Start the engine with the **same** encryption key.

Keep a record of each drill: date, restored size, time to restore, and whether the
instance counts matched.

## When to move to PostgreSQL

Move when you need any of these: a second engine replica, split control/executor roles,
zero-downtime deploys, a recovery point tighter than Litestream's sync interval, or more
write throughput than one SQLite writer gives you. This guide doesn't cover copying data
between backends. Plan the move as a cutover: stop starting new instances and let running
ones finish, or recreate them on the new backend.
