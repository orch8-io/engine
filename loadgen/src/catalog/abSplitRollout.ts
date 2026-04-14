/**
 * ab-split-rollout — deterministic A/B split with 70/30 weights.
 * Shows up in the dashboard as a single block with two possible branch
 * sub-trees; instance outputs record which variant fired.
 */

import type { Template } from "./index.ts";

const HANDLERS = ["ab_variant_a", "ab_variant_b", "ab_converge"] as const;

export const abSplitRollout: Template = {
  name: "ab-split-rollout",
  handlers: HANDLERS,
  weight: 1,
  buildBlocks: () => [
    {
      type: "a_b_split",
      id: "split",
      variants: [
        {
          name: "control",
          weight: 70,
          blocks: [{ type: "step", id: "variant_a", handler: "ab_variant_a", params: {} }],
        },
        {
          name: "treatment",
          weight: 30,
          blocks: [{ type: "step", id: "variant_b", handler: "ab_variant_b", params: {} }],
        },
      ],
    },
    { type: "step", id: "converge", handler: "ab_converge", params: {} },
  ],
  buildContext: (rng) => ({
    data: { experiment_id: "exp-2026-q2", user_id: rng.uuid() },
  }),
};
