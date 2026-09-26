// Hatchet benchmark driver: runs N bench-3step workflows (bounded in flight)
// and awaits each result. Latency is driver-observed (submit -> result).
// UNVERIFIED: `workflow.run(input)` semantics per Hatchet TypeScript SDK v1.
// Needs HATCHET_CLIENT_TOKEN etc. from $BENCH_OUT_DIR/driver.env (setup.sh).
// Crash scenario caveat: a run() whose gRPC call fails while hatchet-lite is
// down is reported as failed rather than re-attached; see docs/BENCHMARKS.md.
import { main } from "../../lib/driver-common.mjs";
import { bench3Step } from "./workflow.mjs";

async function setup() {
  return { runTag: Date.now() };
}

async function runOne(index, { runTag }) {
  const key = `bench-${runTag}-${index}`;
  await bench3Step.run({ key });
  return { id: key, status: "completed", completed_at_ms: Date.now() };
}

await main(runOne, { setup });
