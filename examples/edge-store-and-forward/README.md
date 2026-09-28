# Offline edge: store-and-forward for retail POS and field service

This template is for operators who run workflows **in stores, vans or on
sites** where the WAN link drops. Checkout, stock, job sheets and end-of-day
still have to work. Anything that must reach HQ is queued durably on site and
forwarded exactly once when the link returns.

It uses only what the engine ships today (`0.7.x`). Where something doesn't
exist, this page says so instead of inventing a setting.

## Pick the right piece for each device

| Device | Orch8 component | Runs offline | How data reaches HQ |
|---|---|---|---|
| POS terminal, handheld or tablet (Android/iOS) | Embedded engine, [Mobile SDK](../../docs/MOBILE_SDK.md) (Swift, Kotlin, Flutter, React Native, KMP), local SQLite | Everything: sequences, timers, retries, human steps | Built-in sync channel: `syncUrl` → HQ `POST /api/v1/mobile/sync` |
| Store back-office box or van gateway (Linux, Docker) | `orch8-server` with `[node].role = "edge"`, SQLite | Scheduled and triggered workflows using built-in steps | A workflow step that retries until HQ answers, deduplicated by an idempotency key (`sequences/store-eod-forward.json`) |
| HQ | `orch8-server` (`all_in_one`, or `control` + `executor` on PostgreSQL) with `ORCH8_MOBILE_SYNC_ENABLED=true` | n/a | Receives syncs, forwarded instances, and optionally the edge nodes' control sessions |

```
 store (LAN)                                              HQ
 ┌──────────────────────────────────────┐   WAN (may be down)   ┌─────────────────────────┐
 │ POS terminals — embedded engine      │── /mobile/sync ──────▶│ orch8 (mobile sync on)  │
 │   local SQLite, start(dedupKey)      │◀─ commands ───────────│ Postgres                │
 │                                      │                        │                         │
 │ back-office box — orch8 role=edge    │── POST /instances ───▶│ hq-store-eod-ingest     │
 │   SQLite, cron → http_request (LAN)  │   idempotency_key      │                         │
 │   no inbound API / no gRPC           │── managed control ───▶│ (optional: liveness,    │
 └──────────────────────────────────────┘   (control only)       │  drain; no payloads)    │
                                                                 └─────────────────────────┘
```

## What works offline

**POS terminals (embedded engine):** everything. The engine runs on the device
against its own SQLite file. Sequences are stored locally after an
Ed25519-verified manifest sync, so a terminal that has synced once keeps
selling with no network at all. `start(sequenceName, input, dedupKey)` is
idempotent locally. Use the receipt number as `dedupKey`, and a double-tap or
an app restart can't create a second sale workflow.

**Edge box (`role = "edge"`):** the full engine (scheduler, timers, retries,
built-in handlers such as `http_request`, `log`, state and events, cron
schedules and triggers) runs against local SQLite. Two things are
**deliberately absent** on this role ([Node roles](../../docs/NODE_ROLES.md)):

- **There's no inbound API and no gRPC.** Only `/health/*` answers. POS
  terminals and external workers can't submit work to an edge node. Work
  enters through its **own cron schedules and triggers**, loaded during
  provisioning (below). If your terminals must start workflows over the LAN,
  run the store box as `all_in_one` with the API bound to the store LAN
  instead. That is a supported role on SQLite, with a larger attack surface.
- **The HQ control session carries no workflow data.** With
  `managed_control_*` set, the node opens an outbound HTTPS gRPC session that
  reports liveness and obeys `drain`. It never ships contexts, outputs or
  instances, so it isn't a replication channel.

## How store-and-forward reconciles

### Terminals → HQ (built-in sync)

With `syncUrl`, `deviceId` and `syncApiKey` set, the embedded engine does the
following each time the link is up (every 30 s by default, and immediately on
a silent push):

1. **Uploads** instance status summaries, pending approval requests, and
   *step delegations*: steps that need a server-side secret, resolved at HQ
   through `credentials://` and executed on the device. Each array is capped at
   500 items per request. The telemetry buffer holds up to 1,000 events offline
   and trims to 900.
2. **Downloads** pending commands (`complete_step`, `cancel_instance`,
   `start_workflow`, delegated `step_result`), up to 50 per sync.
3. **Acknowledges** executed commands on the next sync. The device writes a
   durable idempotency record **before** running a command, so a command
   redelivered after the app was killed mid-sync runs at most once.

HQ enforces device ownership per tenant. A `deviceId` registered to another
tenant is rejected.

### Edge box → HQ (durable forward)

`sequences/store-eod-forward.json` runs nightly on the edge node:

1. `export`: `GET` the store back-office export over the LAN. This works
   offline. `ORCH8_ALLOW_INTERNAL_URLS=true` is required because the SSRF
   guard blocks private addresses by default.
2. `forward`: `POST {HQ}/api/v1/instances` with the export as
   `context.data.export` and
   `idempotency_key = "store:<store_id>:eod:{{runtime.instance_id}}"`.
   Network errors and HTTP 5xx are retryable, so the step retries with
   exponential backoff capped at 1 hour, for up to 500 attempts (several weeks
   of outage). The workflow state and its retry timer live in the store's
   SQLite, so a power cut resumes the retry schedule after reboot.
3. HQ answers `201 {"deduplicated": false}` the first time and
   `{"deduplicated": true}` for any repeat. A repeat happens when the request
   landed but the response was lost, or when the step re-ran after a crash.

HTTP 4xx from HQ (bad key, bad payload) is **permanent**: the instance fails
instead of retrying forever. Alert on failed `store-eod-forward` instances.

## Conflicts and duplicates

- **Duplicates are handled by idempotency keys at every hop.** Terminal →
  local engine uses `dedupKey` (receipt id). Edge → HQ uses the instance
  `idempotency_key` (store id plus edge instance id). Any side-effecting call
  you make yourself must pass a business key downstream. `pos-sale.json` shows
  `loyalty:<store>:<receipt>`, and the worker/effect `effect_id` is available to
  external handlers ([Distributed runtimes](../../docs/DISTRIBUTED_RUNTIMES.md)).
  At-least-once delivery plus an idempotent receiver gives you effectively
  once.
- **There's no multi-writer merge.** Each workflow instance is owned by exactly
  one engine: the terminal that started it, or the store box. HQ receives
  copies and status, and never edits the same instance concurrently. Model
  shared business state (stock levels, loyalty balances) as **events forwarded
  to HQ** and reconciled there, not as a record both sides overwrite. The
  engine provides no CRDT or last-writer-wins replication.
- **Commands to an offline device** wait in HQ's queue until the device
  syncs. A command that no longer applies (for example cancelling a finished
  instance) is a no-op on the device.

## Setup

### 1. HQ (local stand-in)

```bash
export HQ_API_KEY=$(openssl rand -hex 32) HQ_ENCRYPTION_KEY=$(openssl rand -hex 32)
docker compose --profile hq up -d
# register sequences/hq-store-eod-ingest.json in tenant "acme", note its id → HQ_SEQUENCE_ID
```

In production, point stores at your real HQ deployment over HTTPS. For
`/mobile/*`, HQ must be an `all_in_one` or `control` node with
`ORCH8_MOBILE_SYNC_ENABLED=true`.

### 2. Store box

```bash
export STORE_ID=store-042
export STORE_API_KEY=$(openssl rand -hex 32)          # local only
export STORE_ENCRYPTION_KEY=$(openssl rand -hex 32)   # BACK THIS UP off the box
export POS_EXPORT_URL=http://backoffice.store.lan/api/eod-export   # your system
export HQ_URL=https://hq.example.com HQ_TENANT=acme HQ_SEQUENCE_ID=<uuid>
export HQ_FORWARDER_KEY=<HQ key allowed to create instances in HQ_TENANT>

docker compose --profile provision up -d store-provision   # full API on 127.0.0.1:18080
./provision.sh                                             # credential, sequence, nightly cron
docker compose --profile provision stop store-provision
docker compose up -d store-edge                            # edge role from here on
```

Re-run the same four steps to change a store's workflows. SQLite allows
**one** engine process per file, so `store-provision` and `store-edge` must
never run at the same time.

Optional fleet visibility: set `MANAGED_CONTROL_ENDPOINT` (must be `https://`),
`MANAGED_CONTROL_API_KEY`, `MANAGED_CONTROL_TENANT_ID` and `STORE_RUNTIME_ID`
(a UUID, stable per store). These map to the documented
`ORCH8_MANAGED_CONTROL_*` variables.

### 3. POS terminals

Load `sequences/pos-sale.json` (or sync it from a signed manifest), register
the app-native handlers (`print_receipt`, `decrement_local_stock`,
`award_loyalty_points`), and open the engine with sync pointed at HQ:

```kotlin
val engine = Orch8Engine.open(dbPath, EngineConfig(
    syncUrl = "https://hq.example.com", deviceId = terminalId, syncApiKey = deviceKey))
engine.resume()
engine.start("pos-sale", saleJson, dedupKey = "sale:$storeId:$receiptId")
```

`syncUrl` must be public HTTPS on port 443. Private and loopback addresses are
rejected at engine construction, so terminals can't sync to the store box.

## Sizing

- **Edge box:** SQLite in WAL mode with a fixed pool of 8 connections, and one
  writer. A store's workload (hundreds to low thousands of instances a day) is
  far below what a single SQLite writer handles, but measure yours with the
  [load generator](../../loadgen/README.md) before you commit. Any small x86 or
  ARM box with an SSD works. Avoid SD cards for the database, and never put the
  file on a network share.
- **Disk:** set `ORCH8_INSTANCE_RETENTION_SECS` (30 days in the compose file)
  so terminal instances are garbage-collected. Size for (instances per day) ×
  (context size) × (retention days) plus the WAL.
- **Outage budget:** `forward` retries for roughly 500 hours at the 1-hour
  cap. Raise `max_attempts` if stores can be offline longer. Nightly exports
  queue up as separate instances, one per night, and drain in order of their
  next retry once the link returns.
- **Terminals:** the SDK's local caps (`max_stored_sequences`,
  `max_concurrent_instances`, `memory_budget_bytes`) are in the
  [Mobile SDK configuration](../../docs/MOBILE_SDK.md).

## Failure modes

| Failure | What happens | Operator action |
|---|---|---|
| WAN down | Terminals and edge keep running. Syncs and `forward` retries back off. | None. Watch the backlog after recovery. |
| WAN down longer than the retry budget | `forward` instance fails permanently | Re-run it (retry from DLQ) once the link is back. The idempotency key prevents a double import. |
| HQ rejects with 4xx | Permanent failure, no retry storm | Fix the key or payload, then retry the instance. |
| Store box power loss | SQLite WAL survives. Running instances are recovered after `stale_instance_threshold_secs` (default 300 s). | None |
| Store box disk lost | Everything not yet forwarded is lost. There is no replica. | Add [Litestream](../../docs/SQLITE_PRODUCTION.md) to a store-local or cloud bucket if the RPO matters. Keep `STORE_ENCRYPTION_KEY` off the box, or the backup is unreadable. |
| Clock skew on the box | Cron fires at the local clock's idea of 23:30. Retry timers are relative. | Run NTP. Choose `STORE_TZ` explicitly. |
| `store-provision` left running with `store-edge` | Two writers on one SQLite file. This is **unsupported** and can corrupt state. | Never do this. The compose profile exists to make it a deliberate step. |
| Terminal app killed mid-sync | Command idempotency records prevent re-execution, and unsent status is retried on the next sync. | None |
| Terminal lost or wiped | Instances that existed only on the device are gone. HQ keeps the last synced status. | Keep terminal-only workflows short. Forward business events to HQ early. |

## Files

| File | Purpose |
|---|---|
| `docker-compose.yml` | `store-edge` (role `edge`, SQLite), `store-provision` (temporary `all_in_one` on loopback, same volume), and the `hq` profile (Postgres + mobile sync) |
| `provision.sh` | Loads the credential, the forward sequence (with store values substituted) and the nightly cron into the store database. Needs `curl`, `jq`, `uuidgen`. |
| `sequences/store-eod-forward.json` | Edge workflow: LAN export, then idempotent forward to HQ with long retries |
| `sequences/hq-store-eod-ingest.json` | HQ workflow that receives forwarded exports |
| `sequences/pos-sale.json` | Terminal workflow shape for the embedded engine (app-native handlers) |
