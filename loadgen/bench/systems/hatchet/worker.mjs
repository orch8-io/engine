// Hatchet worker hosting bench-3step. UNVERIFIED (see workflow.mjs).
import { bench3Step, hatchet } from "./workflow.mjs";

const worker = await hatchet.worker("bench-worker", {
  workflows: [bench3Step],
  slots: Number(process.env.WORKER_SLOTS ?? 100),
});
await worker.start();
