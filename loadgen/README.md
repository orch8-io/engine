# @orch8/loadgen

Continuous workflow load generator for the orch8 engine. Drives the
dashboard with realistic traffic (instances in every state, mixed block
types, cron triggers, signals) and doubles as a stress tool.

## Quick start

The load generator does not currently send Orch8 API-key or tenant headers.
Run it only against an isolated local engine started with `--insecure` and
both tenant-isolation overrides shown below, never against a shared or
production environment.

```bash
# isolated engine terminal
ORCH8_HTTP_ADDR=127.0.0.1:18080 \
ORCH8_API_KEY="" \
ORCH8_REQUIRE_TENANT_HEADER=false \
ORCH8_ALLOW_NO_TENANT_ISOLATION=1 \
orch8-server --config orch8.toml --insecure

# load-generator terminal, from repo root
cd loadgen
npm install
npm run light     # ~1 instance/sec, 20 concurrent, 2 workers
npm run steady    # ~5/sec, 200 concurrent, 8 workers  (default)
npm run stress    # ~50/sec, 2000 concurrent, 32 workers
```

Stop with Ctrl-C — the process traps SIGINT, drains in-flight HTTP, and
prints a final counters snapshot.

## Presets

| Preset  | rate (inst/s) | concurrency | workers | signal rate |
|---------|---------------|-------------|---------|-------------|
| light   | 1             | 20          | 2       | 1 / 30s     |
| steady  | 5             | 200         | 8       | 0.1 / s     |
| stress  | 50            | 2000        | 32      | 0.5 / s     |

Any preset field can be overridden via CLI flag: `--rate=10 --concurrency=500`.

## Flags & env vars

| Flag              | Env var             | Default                         |
|-------------------|---------------------|---------------------------------|
| `--server=URL`    | `LOADGEN_SERVER`    | `http://localhost:18080`        |
| `--preset=NAME`   | `LOADGEN_PRESET`    | `steady`                        |
| `--rate=N`        | —                   | preset                          |
| `--concurrency=N` | —                   | preset                          |
| `--workers=N`     | —                   | preset                          |
| `--signal-rate=N` | —                   | preset                          |
| `--duration=SEC`  | `LOADGEN_DURATION`  | forever                         |
| `--seed=N`        | `LOADGEN_SEED`      | `Date.now()`                    |
| `--tenants=a,b`   | `LOADGEN_TENANTS`   | `loadgen-a,loadgen-b,loadgen-c` |
| `--namespaces=…`  | `LOADGEN_NAMESPACES`| `prod,staging`                  |
| `--enable-llm`    | `LOADGEN_ENABLE_LLM=1` | off                          |
| `--verbose`       | `LOADGEN_VERBOSE=1` | off                             |

## Catalog

Eight always-on templates plus one gated LLM template:

- `order-fulfillment` — parallel validate/charge/reserve → ship → notify
- `approval-flow` — kickoff → wait-for-input signal → router on approval
- `data-pipeline` — for_each over N items → summarize
- `eta-escalation` — try_catch with timeout → catch escalate → finish
- `ab-split-rollout` — ab_split 70/30 control/treatment → converge
- `loop-aggregator` — loop while remaining > 0 → finalize
- `race-fetch` — race (first-to-resolve) primary/mirror/cache → converge
- `nested-subsequence` — prepare → sub_sequence `order-fulfillment` → finalize
- `llm-chain` *(gated)* — two `llm_call` steps (generate → summarize)

## Cleanup

Everything the load generator creates uses `loadgen-*` tenants. There is no
public tenant-cleanup endpoint. Use a disposable SQLite database or dedicated
PostgreSQL database for a run, then delete that database through your normal
database lifecycle. Do not issue ad-hoc cross-table deletes against a shared
Orch8 database.
