# Distributed benchmark

> **Stability: experimental**, may change or be removed in any release. Note: no multi-host result has been published. The only recorded run is a single-host smoke test (see [Results](#results)). The `compose` and `ssh` modes have not been run yet.

This page describes how to measure Orch8 when the roles run as separate
processes on separate machines: one `control` node, N `executor` nodes and W
REST worker processes on a shared PostgreSQL. It also covers how to read the
result file. The harness is
[`loadgen/bench/distributed/`](../loadgen/bench/distributed/run.sh). It reuses
the workload, driver, worker, summarizer and result schema of the
[comparative benchmark](BENCHMARKS.md), so a distributed result can be set
next to a single-node one.

## Topology

```
             driver (this machine)            ── POST /instances, GET /instances/{id}
                    │
                    ▼
   control  (role=control: API, no scheduler) ◀── workers poll / complete over HTTP
                    │ shared PostgreSQL ▲
   executor-1 … executor-N  (role=executor: scheduler, dispatch to worker queue)
   worker-1  … worker-W     (REST worker, WORKER_SLOTS concurrent 50 ms activities)
```

The roles are the ones documented in [Node roles](NODE_ROLES.md). Executors
claim instances from PostgreSQL. Workers claim `bench_activity` tasks through
the control node's worker API with `FOR UPDATE SKIP LOCKED`.

## Workload

It is identical to [Benchmarks → Throughput](BENCHMARKS.md#throughput): N
workflows of **3 sequential steps**, each a **50 ms** external-worker activity,
with at most `--concurrency` workflows in flight and `--warmup` workflows
discarded first. The measures are throughput (completed ÷ last completion −
first submission) and end-to-end latency p50/p95/p99/max. The latency comes
from server timestamps (`created_at` → `updated_at`) when every sample has
them.

## Modes

| Mode | Where the roles run | What it can tell you |
|---|---|---|
| `ssh --hosts FILE` | Each role on the host named in the file. The driver runs locally. | Real multi-host scaling, including network and database latency. **The only mode that yields a distributed result.** |
| `compose` | Each role in its own container on one Docker host, optionally capped with `--cpus-per-node` and `--mem-per-node` | Process-level scaling and coordination overhead under CPU isolation. It's still one kernel, one disk and one network namespace. |
| `local --server-bin --database-url` | Each role as a plain process on this machine, with no Docker | A smoke test that the split roles work end to end under load. The numbers mostly reflect contention on one machine. |

Any result where every role shared one machine gets
`topology.single_host = true`. Label such a result **"single-host smoke,
not a distributed result"** wherever it is quoted.

## Running it

Requirements: bash and Node.js 22+ on the driver machine. `compose` needs
Docker with Compose v2. `local` needs an `orch8-server` binary, `psql`, and a
PostgreSQL server where the harness may drop and recreate one database. `ssh`
needs key-based `ssh`/`scp` to every host, `orch8-server` on `PATH` on control
and executor hosts, Node.js 22 on worker hosts, and `psql` on the control host.

```bash
# compose: 1 control + 3 executors + 4 workers, 2 CPUs / 2 GiB per container
loadgen/bench/distributed/run.sh --mode compose --executors 3 --workers 4 \
  --cpus-per-node 2 --mem-per-node 2g --n 2000 --concurrency 200 --runs 3 \
  --image ghcr.io/orch8-io/engine:<exact tag>

# ssh: roles placed by a hosts file (see loadgen/bench/distributed/hosts.example)
loadgen/bench/distributed/run.sh --mode ssh --hosts hosts.txt --n 5000 --concurrency 500 --runs 3

# local processes (single-host smoke)
loadgen/bench/distributed/run.sh --mode local --server-bin ./target/release/orch8-server \
  --database-url postgres://orch8@127.0.0.1:5432/orch8_bench --executors 2 --workers 2 --n 300

node loadgen/bench/validate-result.mjs loadgen/bench/results/distributed-*/result.json
```

The database named in `--database-url` or the hosts file is **dropped and
recreated** on every run. The harness refuses any database whose name doesn't
contain `bench`.

Scaling experiments vary one thing at a time. For example, hold workers at
`W × WORKER_SLOTS ≥ concurrency × 3` so workers aren't the bottleneck, then
sweep `--executors 1, 2, 4, 8`. Then hold executors and sweep workers. A
throughput plateau while CPU on the executors stays low points at the database
or the control node's worker API. Record `pg_stat_statements` and host CPU if
you want to say which.

## Result file

Each run writes `loadgen/bench/results/distributed-<mode>-<UTC stamp>-run<i>/`:

| File | Content |
|---|---|
| `result.json` | The standard result ([schema](../loadgen/bench/results.schema.json)) plus a `topology` block |
| `topology.json` | The same block on its own |
| `samples.jsonl` | One line per measured workflow (submit, completion, server start/close) |
| `activity-*.jsonl` | One line per activity execution, per worker |
| `driver.out`, `control.log`, `executor-*.log`, `worker-*.log` | Raw process output (`local` mode). In `ssh` mode, logs stay in `/tmp/orch8-bench-*` on each host. |
| `compose.yml`, `images.json` | Generated stack and resolved image digests (`compose` mode) |

`topology` records `mode`, `single_host`, `executors`, `workers`,
`worker_slots`, `engine_build` (image reference, or binary path with its
`--version`), and for the control host and every executor and worker host:
hostname, OS, CPU model, cores, memory, `hw_model` (`sysctl hw.model` on macOS,
DMI product name on Linux) and the number of role processes on it. The
top-level `hardware` block describes the **driver** machine.

## Fairness and honesty rules

The rules in [Benchmarks → Fairness rules](BENCHMARKS.md#fairness-rules)
apply: fresh database per run, documented defaults, warm-up, three runs with
the median reported, pinned versions, and every raw file published. In
addition:

1. **Never quote a `single_host: true` result as distributed.** Quote it with
   its label and the hardware line.
2. **Debug builds aren't benchmarks.** `engine_build` shows whether a local
   binary was a debug build. Use release binaries or published images for any
   number you intend to compare.
3. **State the database machine.** In `ssh` mode the harness doesn't log in
   to the database host. Put its hardware and PostgreSQL version in `notes`.
4. **Network matters.** Record where the driver ran relative to the control
   node (same rack, same region, across the internet), because the driver's
   polling adds to driver-side latency. Server-side latency doesn't include it.

## What this does not measure

- Control-plane failover, database failover or network partitions. Only
  steady-state throughput is measured. The crash scenario of the comparative
  harness applies to the single-node stack.
- `edge` nodes, managed-control sessions, mobile runtimes or gRPC workers.
- Workloads other than 3 × 50 ms external-worker steps.

## Results

No distributed (multi-host) result has been recorded. Results will be added
here only together with their raw result directories.

### Single-host smoke (not a distributed result)

One `local`-mode run was recorded while the harness was being developed, to
confirm that the split `control`/`executor` roles complete the workload end to
end. Every process shared one laptop: PostgreSQL, the control node, both
executors, both workers and the driver. The numbers say **nothing** about
distributed scale-out, and they aren't comparable to a release build or to the
[comparative benchmark](BENCHMARKS.md).

Raw files:
[`loadgen/bench/distributed/published/20260928T191204Z-local-smoke/`](../loadgen/bench/distributed/published/20260928T191204Z-local-smoke/result.json)
(`result.json` with its `topology` block, and gzipped `samples.jsonl`).

| Mode | Executors × workers × slots | N / concurrency | Completed / failed | Throughput (wf/s) | p50 / p95 / p99 (ms) | Latency source | Engine build | Hardware |
|---|---|---|---|---|---|---|---|---|
| local, single host | 2 × 2 × 100 | 2000 / 200 (warm-up 100) | 2000 / 0 | 194.4 | 920 / 1152 / 1207 (max 1232) | server | `orch8-server` 0.7.1, `dev` profile built with host rustflags `-C opt-level=3 -C target-cpu=native` (not the `release` profile, no LTO), worktree at `21a4255` plus uncommitted changes | Apple M3 Max (`hw.model` Mac15,11), 14 cores, 36 GiB RAM, macOS (Darwin 25.3.0 arm64); PostgreSQL 14.23 (Homebrew) on the same host |
