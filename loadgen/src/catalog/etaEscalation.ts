/**
 * eta-escalation — TryCatch wraps a slow external step with a short
 * timeout; the catch branch dispatches an escalation. Exercises timeouts,
 * the TryCatch branch visualizer, and retries on the slow step.
 */

import type { Template } from "./index.ts";

const HANDLERS = ["eta_slow_task", "eta_escalate", "eta_finish"] as const;

export const etaEscalation: Template = {
  name: "eta-escalation",
  handlers: HANDLERS,
  weight: 2,
  buildBlocks: () => [
    {
      type: "try_catch",
      id: "tc",
      try_block: [
        {
          type: "step",
          id: "slow",
          handler: "eta_slow_task",
          params: {},
          timeout: 8_000,
          retry: {
            max_attempts: 2,
            initial_backoff: 500,
            max_backoff: 2_000,
            backoff_multiplier: 2.0,
          },
        },
      ],
      catch_block: [
        { type: "step", id: "escalate", handler: "eta_escalate", params: {} },
      ],
    },
    { type: "step", id: "done", handler: "eta_finish", params: {} },
  ],
  buildContext: (rng) => ({
    data: {
      task_id: rng.uuid(),
      deadline_budget_ms: rng.int(5_000, 30_000),
      severity: rng.weighted(["low", "medium", "high"], [0.6, 0.3, 0.1]),
    },
  }),
};
