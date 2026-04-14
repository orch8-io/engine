/**
 * loop-aggregator — iterates a handler N times driven by a loop
 * condition, then aggregates. Shows repeated block instances in the
 * execution tree.
 */

import type { Template } from "./index.ts";

const HANDLERS = ["loop_accumulate", "loop_finalize"] as const;

export const loopAggregator: Template = {
  name: "loop-aggregator",
  handlers: HANDLERS,
  weight: 1,
  buildBlocks: () => [
    {
      type: "loop",
      id: "iter",
      condition: "context.data.remaining > 0",
      max_iterations: 15,
      body: [
        {
          type: "step",
          id: "accumulate",
          handler: "loop_accumulate",
          params: {},
        },
      ],
    },
    { type: "step", id: "finalize", handler: "loop_finalize", params: {} },
  ],
  buildContext: (rng) => ({
    data: { remaining: rng.int(5, 12), total: 0 },
  }),
};
