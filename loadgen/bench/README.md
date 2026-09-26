# Comparative benchmark harness

Runs the same workload — N workflows × 3 sequential steps, each step a 50 ms
activity — against Orch8, Temporal, Inngest, Trigger.dev, and Hatchet, plus a
crash-recovery scenario that kills the orchestrator mid-run.

Methodology, fairness rules, and the (intentionally empty) result tables are
in [docs/BENCHMARKS.md](../../docs/BENCHMARKS.md).

```bash
# from the repository root
loadgen/bench/run.sh --system orch8 --scenario throughput --n 1000 --concurrency 100
loadgen/bench/run.sh --system orch8 --scenario crash_recovery --n 500 --concurrency 50 --kill-after 5
node loadgen/bench/validate-result.mjs loadgen/bench/results/*/result.json
```

| Path | Purpose |
|---|---|
| `run.sh` | Fresh stack per run → driver → optional kill/restart → result file → teardown |
| `systems/<name>/docker-compose.yml` | The system under test plus its worker |
| `systems/<name>/driver.mjs` | Submits workflows and records per-workflow samples |
| `lib/summarize.mjs` | Builds `result.json` (percentiles, throughput, crash metrics) |
| `results.schema.json` / `validate-result.mjs` | Result contract and dependency-free validator |

Status per system: **orch8** has been run end-to-end by the harness.
**temporal**, **inngest**, and **hatchet** are complete but not yet exercised
end-to-end; SDK/API usage and image settings are marked `UNVERIFIED` in the
files. **trigger** is a skeleton — `run.sh` refuses it until the self-hosted
stack is completed (see its compose file).

This harness is separate from the continuous traffic generator in
`loadgen/src`, which is unchanged.
