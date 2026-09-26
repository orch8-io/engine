# Triggers

> **Stability: beta**, shipped and tested; may change in a minor release with a changelog note.

A trigger maps an event source to a sequence. Every event creates one
instance of the trigger's sequence in the trigger's tenant and namespace.
The event body becomes `context.data`, and source details are stored under
`metadata._trigger_event`.

```bash
curl -X POST http://localhost:8080/api/v1/triggers \
  -H 'Content-Type: application/json' -H 'X-Tenant-Id: acme' \
  -d '{"slug": "orders-in", "sequence_name": "process-order", "tenant_id": "acme",
       "trigger_type": "kafka",
       "config": {"brokers": ["kafka:9092"], "topic": "orders"}}'
```

| `trigger_type` | Source | Engine feature | In the default `orch8-server` build |
|---|---|---|---|
| `webhook` | `POST /webhooks/{slug}` (HMAC) | — | yes |
| `event` | `POST /triggers/{slug}/fire`, `emit_event` | — | yes |
| `nats` | NATS queue-group subscription | `nats` | yes |
| `file_watch` | filesystem events | `file-watch` | yes |
| `activepieces_poll` | Activepieces sidecar polling | — | yes |
| `sqs` | AWS SQS queue | `sqs` | yes |
| `pubsub` | GCP Pub/Sub pull subscription | `pubsub` | yes |
| `postgres_rows` | row changes in a Postgres table | `postgres-rows` | yes |
| `kafka` | Kafka topic | `kafka` | no: build with `--features trigger-kafka` |
| `redis_streams` | Redis Streams consumer group | `redis-streams` | no: build with `--features trigger-redis-streams` |

`sqs`, `pubsub` and `postgres_rows` are compiled into the server by default
because they reuse crates the server already links (`reqwest`, `hmac`,
`jsonwebtoken`, `sqlx`), so they cost no extra dependencies. Kafka and Redis
Streams each bring in a new client crate tree, so they are opt-in:
`cargo build -p orch8-server --features all-triggers`. If a trigger's type is
not compiled into a build, the engine logs a warning and skips it. The API
still validates its config.

A string value in a message-source config can be a `credentials://<id>[/<key>]`
reference ([credentials](API.md)). They are resolved per tenant when the
listener starts, so broker passwords, AWS keys, and service-account JSON never
need to be stored inline.

## Delivery guarantees (message sources)

All message sources (`kafka`, `sqs`, `pubsub`, `redis_streams`,
`postgres_rows`) share one contract:

- **Ack after a durable create.** A message is acknowledged only after its
  instance row has committed. That means the Kafka or Postgres cursor is
  persisted, SQS runs `DeleteMessage`, Pub/Sub runs `acknowledge`, or Redis
  runs `XACK`.
- **Idempotent by message id.** Each instance gets a tenant-scoped
  idempotency key `trigger:<slug>:<message id>`. A redelivery is acknowledged
  and does not start a second run. Redeliveries come from a crash between
  create and ack, a visibility timeout, a rebalance, or a second engine node.
- **No ack on failure.** A failed create leaves the message unacknowledged.
  The broker redelivers it, or the listener re-reads it from the persisted
  cursor after an exponential backoff (1 s to 60 s).
- **Consumers across nodes.** SQS, Pub/Sub and Redis consumer groups balance
  consumption across engine nodes. Kafka (through the pure-Rust `rskafka`
  client, which has no consumer groups) and Postgres rows use a per-trigger
  lease in `trigger_poll_state`, so exactly one node consumes at a time. If
  that node dies, another node takes over within about a minute.

For `kafka` and `postgres_rows`, the cursor and the last error are stored in
`trigger_poll_state` and returned as `poll_state` by `GET /triggers/{slug}`. The cursor is tied to the Kafka
`topic` or the Postgres `outbox_table`. If you edit either one, the listener
starts again from `start_from`.

> **Multi-tenant deployments:** a message-source trigger makes the engine
> connect to an address from the trigger config. The same is already true of
> `nats`. Restrict who can create triggers, or run the engine where it cannot
> reach internal brokers it should not read.

## `kafka`

```json
{
  "brokers": ["kafka-1:9092", "kafka-2:9092"],
  "topic": "orders",
  "partitions": [0, 1],
  "start_from": "latest",
  "tls": true,
  "sasl": {"mechanism": "scram-sha-512", "username": "orch8", "password": "credentials://kafka/password"},
  "max_wait_ms": 500,
  "max_batch_bytes": 1048576
}
```

- `partitions` defaults to every partition of the topic. `start_from` applies
  only when a partition has no stored offset.
- Offsets live in the engine, in `trigger_poll_state`, not in a Kafka consumer
  group. A partition's offset is advanced only past records whose instance
  committed. If retention deleted the stored offset, consumption resumes at
  the earliest kept record.
- SASL mechanisms: `plain`, `scram-sha-256`, `scram-sha-512`. TLS uses the
  Mozilla root store.
- Record compression: none, gzip, and snappy. lz4 and zstd need C libraries
  and are not compiled in.
- `data` is the record value (JSON when it parses, otherwise a string). The
  event metadata carries `topic`, `partition`, `offset`, `key`, `headers`, and
  `timestamp`. The message id is `topic/partition/offset`.

## `sqs`

```json
{
  "queue_url": "https://sqs.us-east-1.amazonaws.com/123456789012/orders",
  "access_key_id": "credentials://aws/access_key_id",
  "secret_access_key": "credentials://aws/secret_access_key",
  "wait_time_seconds": 20,
  "max_messages": 10,
  "visibility_timeout": 60
}
```

- The listener uses the SQS JSON protocol with a built-in SigV4 signer, so no
  AWS SDK is needed. `region` is parsed from the queue URL, or set it for
  custom endpoints such as LocalStack or ElasticMQ (`http://localhost:4566/...`).
- Credentials come from the config, or from `AWS_ACCESS_KEY_ID`,
  `AWS_SECRET_ACCESS_KEY` and `AWS_SESSION_TOKEN`. Instance-profile, IRSA and
  SSO credential providers are not supported yet. Use static or injected
  environment credentials.
- A message is deleted (`DeleteMessageBatch`) only after its instance
  commits. A failed create is redelivered after the visibility timeout, and
  the queue's own redrive policy moves poison messages to your SQS DLQ.
- The message id is the SQS `MessageId`. Metadata includes `attributes` and
  `message_attributes`.

## `pubsub`

```json
{
  "subscription": "projects/my-project/subscriptions/orders-orch8",
  "credentials_json": "credentials://gcp-sa",
  "max_messages": 50
}
```

- Pulls through the REST API (`subscriptions.pull`, `acknowledge`,
  `modifyAckDeadline`).
- Auth, in order: `credentials_json` (a service-account key), the file at
  `GOOGLE_APPLICATION_CREDENTIALS`, then the GCE/GKE metadata server.
- Emulator: set `"endpoint": "http://localhost:8085"` (or
  `PUBSUB_EMULATOR_HOST`). Auth is then skipped unless `credentials_json` is
  set.
- A failed create is nacked (`ackDeadlineSeconds: 0`), so the subscription's
  retry and dead-letter policy applies.
- The message id is the Pub/Sub `messageId`. Metadata includes `attributes`,
  `publish_time`, `ordering_key`, and `delivery_attempt`.

## `redis_streams`

```json
{
  "url": "credentials://redis/url",
  "stream": "orders",
  "group": "orch8",
  "start_id": "$",
  "payload_field": "payload",
  "batch_size": 50,
  "block_ms": 5000,
  "claim_idle_ms": 60000
}
```

- `url` is `redis://` or `rediss://` (TLS). Because credential references
  replace a whole string value, store a URL that contains a password as a
  credential and reference it, as in the example above.
- Reads with `XREADGROUP`, using the group `orch8-<slug>` by default and one
  consumer per engine process. The group is created if missing: `start_id: "$"`
  reads new entries only, and `"0"` reads the backlog.
- An entry is `XACK`ed after its instance commits. On (re)start the listener
  first re-reads its own pending entries. Entries left pending by a crashed
  consumer are taken over with `XAUTOCLAIM` once they have been idle for
  `claim_idle_ms`.
- `data` is the `payload_field` value (parsed as JSON when possible). If that
  field is absent, `data` is the whole field map. The message id is
  `stream/entry-id`.

## `postgres_rows`

This trigger starts a workflow when rows in one of **your** tables are
inserted, updated, or deleted.

### 1. Install capture in the application database

```bash
orch8 triggers pg-install --table public.orders --events insert,update > capture.sql
psql "$APP_DATABASE_URL" -f capture.sql
# or apply directly:
orch8 triggers pg-install --table public.orders --events insert,update \
  --apply --database-url "$APP_DATABASE_URL"
```

The script is idempotent and requires PostgreSQL 13 or later. It creates:

- The outbox table `public.orch8_row_changes`. It has one row per change,
  written **in the same transaction** as the change, so a committed change is
  never lost, even while the engine is down.
- The `plpgsql` function `orch8_row_changes_capture()`. It appends to the
  outbox and calls `pg_notify('orch8_row_changes', …)` as a wake-up.
- An `AFTER INSERT OR UPDATE … FOR EACH ROW` trigger named
  `orch8_row_change` on the watched table.

`--uninstall` drops the table trigger and keeps the shared outbox.
`--outbox-table` and `--channel` override the defaults.

### 2. Create the trigger

```json
{
  "slug": "order-changed", "sequence_name": "on-order-change", "tenant_id": "acme",
  "trigger_type": "postgres_rows",
  "config": {
    "database_url": "credentials://app-db/url",
    "table": "public.orders",
    "events": ["insert", "update"],
    "start_from": "now",
    "poll_interval_ms": 5000
  }
}
```

The workflow receives:

```json
{"op": "update", "table": "public.orders",
 "new": {"id": 7, "status": "paid"}, "old": {"id": 7, "status": "new"}}
```

On insert, `old` is null. On delete, `new` is null.

### How delivery works

- The engine `LISTEN`s on the channel **and** polls every `poll_interval_ms`.
  A lost notification, for example while the engine was down, only delays
  delivery.
- The engine reads outbox rows in `(txid, id)` order. A row is eligible only
  when its transaction is older than the oldest in-flight transaction
  (`pg_snapshot_xmin(pg_current_snapshot())`). That set of rows is final, so
  the durable cursor can never skip a row that a long transaction commits
  late. This avoids the classic `BIGSERIAL` outbox gap. The trade-off: a very
  long-running transaction anywhere in the database delays delivery until it
  ends.
- The cursor is stored in the engine's `trigger_poll_state` and advanced only
  past rows whose instance committed. The instance idempotency key is the
  outbox row id, which is the same replay-safe rule as the
  [outbox intake relay](POSTGRES_OUTBOX_INTAKE.md).
- `start_from: "beginning"` also processes changes that were already in the
  outbox when the trigger first ran.
- The listener filters on `table` and `events`, so one outbox can serve many
  tables and triggers. Prune old rows yourself, for example:
  `DELETE FROM orch8_row_changes WHERE created_at < now() - interval '7 days'`.
