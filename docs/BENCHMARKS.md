# Benchmarks

> **Stability: experimental**, may change or be removed in any release. Note: only the Orch8 harness has been run end to end; the other systems are marked UNVERIFIED.

This page describes a reproducible, head-to-head benchmark of Orch8 against
Temporal, Inngest, Trigger.dev, and Hatchet, and how to run it yourself. The
harness lives in [`loadgen/bench/`](../loadgen/bench/README.md).

**No results are published yet.** The tables at the bottom are intentionally
empty. Numbers will only be added together with the raw result files that
produced them, the exact image digests, and the hardware they ran on. If you
see an Orch8 benchmark number that does not link to such files, treat it as
unverified.

## What is measured

Two scenarios, each run against every system on the same machine.

### Throughput

- **Workload:** N workflows, each with **3 sequential steps**; each step is an
  activity that sleeps **50 ms** and appends one line to an activity log. The
  driver keeps at most `--concurrency` workflows in flight.
- **Throughput:** completed workflows ÷ (last completion − first submission).
- **End-to-end latency:** p50 / p95 / p99 / max of workflow start → close.
  When the system exposes server-side start and close timestamps for every
  workflow, those are used (`latency_source: "server"`); otherwise the driver's
  submit → observed-completion time is used (`latency_source: "driver"`),
  which includes polling granularity. The source is recorded in every result
  and results with different sources are not compared directly.

### Crash recovery

Same workload, but `--kill-after` seconds into the measured run the harness
sends `SIGKILL` to the orchestrator container (`docker compose kill`), waits
`--down-for` seconds, and starts it again. Workers and the database keep
running. Recorded per run:

| Field | Definition |
|---|---|
| `in_flight_at_kill` | Workflows submitted before the kill that had not completed at the kill instant |
| `recovered_count` | Workflows submitted before the kill that completed after the restart |
| `lost_count` | Workflows that never completed before the driver deadline |
| `duplicate_activity_executions` | Activity-log lines minus distinct (workflow, step) pairs — how many activities ran more than once |
| `time_to_recover_ms` | Restart → first workflow completion observed after the restart |

Duplicate executions are **expected** for every system here: all of them
provide at-least-once activity execution, so an activity whose completion was
not durably recorded before the crash may run again. The metric quantifies
that re-execution; it is not by itself a correctness bug. A workflow that
completes twice, or completes with a step missing, would be — the activity log
lets you check for both.

The driver deadline (`--deadline`, default 600 s) must be longer than every
system's recovery timeouts at default settings, or `lost_count` measures the
deadline rather than the system. For Orch8, instances held by a dead node are
reclaimed after `stale_instance_threshold_secs` (default 300 s) and worker
tasks after `worker_reaper_stale_secs` (default 60 s); see
[Configuration](CONFIGURATION.md#engine). Look up the equivalent timeouts for
the pinned version of each other system and record them in the result `notes`.

## Fairness rules

1. **Same hardware, same Docker daemon, one system at a time.** Nothing else
   heavy runs on the host. Results record CPU model, core count, memory, OS,
   Docker version, and the CPUs/memory visible to Docker.
2. **Fresh state every run.** Each run starts with `docker compose down -v`
   and a new stack, so no run benefits from a warm database or cache left by
   the previous one.
3. **Same backing store class.** Every system uses PostgreSQL 16 in the same
   compose network (plus Redis where the system requires it). No system gets
   in-memory persistence while another pays for durable writes.
4. **Default settings.** Each system runs with its documented defaults, except
   what is required to start at all (credentials, migrations, connection
   strings). Any change is written into the result `notes` and the table
   footnotes. Worker concurrency is set to the same value (`WORKER_SLOTS`,
   default 100) everywhere.
5. **Same workload shape.** Three durable, sequential steps of 50 ms each,
   using each system's native durable-step primitive (Orch8 external worker
   step, Temporal activity, Inngest `step.run`, Hatchet DAG task, Trigger.dev
   child task via `triggerAndWait`). Where the closest primitive carries extra
   overhead (Trigger.dev child tasks), the table says so.
6. **Warm-up.** `--warmup` workflows (default 50) run and are discarded before
   the measured run.
7. **Three runs, report the median.** `--runs 3` (the default) produces three
   result files; publish the median run per metric and keep all three.
8. **Pinned versions.** Pin each image to an exact tag or digest before a
   publishable run. Every result file records the resolved image digests and
   SDK versions regardless, so an unpinned run is still traceable.
9. **Publish everything.** Result files, samples, and activity logs are
   published alongside any number. Results from a system whose harness is
   marked unverified are labelled as such.

## What is not measured

- Multi-node or horizontally scaled deployments, managed/cloud offerings, and
  cost.
- Long-running workflows, timers, signals, fan-out, large payloads, or
  activities that do real I/O.
- Developer experience, feature coverage, or operational effort.
- Database failure, network partitions, or worker crashes (only the
  orchestrator process is killed).
- Tail behaviour under sustained overload beyond the chosen concurrency.

A benchmark this narrow says how each system behaves for this workload on
this machine. It does not rank systems in general.

## Durability at default settings (what "completed" means)

Check each vendor's documentation for the pinned version before publishing;
the harness does not change these behaviours.

| System | Durable unit in this workload | Activity execution guarantee |
|---|---|---|
| Orch8 | Instance state and each step's output persisted in PostgreSQL before the next step is scheduled | At least once (worker tasks are leased and re-offered after lease expiry) |
| Temporal | Workflow event history persisted by the server before progress is acknowledged | At least once (activities retried per retry policy) |
| Inngest | Each `step.run` result memoized by the server | At least once |
| Hatchet | Each task run and result persisted in PostgreSQL | At least once |
| Trigger.dev | Parent checkpointed at each `triggerAndWait`; each child run persisted | At least once |

## Harness status

| System | Stack | Driver/worker | Run end-to-end by the harness |
|---|---|---|---|
| Orch8 | Complete | Complete (REST worker, no SDK) | Yes (smoke-sized runs only) |
| Temporal | Complete (`temporalio/auto-setup`) | Complete, `UNVERIFIED` SDK usage | No |
| Inngest | Complete (self-hosted `inngest start`), `UNVERIFIED` flags | Complete, `UNVERIFIED` REST run-status endpoint | No |
| Hatchet | Complete (`hatchet-lite`), `UNVERIFIED` env | Complete, `UNVERIFIED` SDK usage | No |
| Trigger.dev | **Skeleton** — self-hosted stack incomplete | Skeleton | No (`run.sh` refuses it) |

`UNVERIFIED` markers in the files name exactly which image settings or SDK
calls have not been confirmed against a running system.

## Reproduce

Requirements: Docker with Compose v2, Node.js 22+, bash.

```bash
# Throughput, 3 runs of 1000 workflows at 100 in flight
loadgen/bench/run.sh --system orch8 --scenario throughput --n 1000 --concurrency 100

# Crash recovery: kill the orchestrator 5 s in, restart it 5 s later
loadgen/bench/run.sh --system orch8 --scenario crash_recovery \
  --n 500 --concurrency 50 --kill-after 5 --down-for 5

# Other systems (drivers install their SDKs with npm on first use)
loadgen/bench/run.sh --system temporal --scenario throughput --n 1000 --concurrency 100
loadgen/bench/run.sh --system inngest  --scenario throughput --n 1000 --concurrency 100
loadgen/bench/run.sh --system hatchet  --scenario throughput --n 1000 --concurrency 100

# Validate result files against the schema
node loadgen/bench/validate-result.mjs loadgen/bench/results/*/result.json
```

Each run writes `loadgen/bench/results/<system>-<scenario>-<UTC stamp>-run<i>/`
containing `result.json` (schema:
[`loadgen/bench/results.schema.json`](../loadgen/bench/results.schema.json)),
`samples.jsonl` (one line per workflow), `activity.jsonl` (one line per
activity execution), `images.json` (resolved image digests), and the raw
driver output. Pin images with `ORCH8_IMAGE`, `TEMPORAL_IMAGE`,
`INNGEST_IMAGE`, `HATCHET_LITE_IMAGE`, and `TRIGGER_VERSION`.

## Results

Not yet published. Each cell will hold the median of three runs with a link
to the raw files.

### Throughput (N = —, concurrency = —)

| System | Version | Throughput (wf/s) | p50 (ms) | p95 (ms) | p99 (ms) | Latency source | Failed |
|---|---|---|---|---|---|---|---|
| Orch8 | — | — | — | — | — | — | — |
| Temporal | — | — | — | — | — | — | — |
| Inngest | — | — | — | — | — | — | — |
| Trigger.dev | — | — | — | — | — | — | — |
| Hatchet | — | — | — | — | — | — | — |

### Crash recovery (N = —, concurrency = —, kill after — s, down for — s)

| System | Version | In flight at kill | Recovered | Lost | Duplicate activity executions | Time to recover (ms) |
|---|---|---|---|---|---|---|
| Orch8 | — | — | — | — | — | — |
| Temporal | — | — | — | — | — | — |
| Inngest | — | — | — | — | — | — |
| Trigger.dev | — | — | — | — | — | — |
| Hatchet | — | — | — | — | — | — |

Hardware: —
