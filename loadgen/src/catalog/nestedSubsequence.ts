/**
 * nested-subsequence — invokes `order-fulfillment` as a child workflow.
 * Exercises parent/child linking and the "waiting on child" state.
 */

import type { Template } from "./index.ts";

const HANDLERS = ["nested_prepare", "nested_finalize"] as const;

export const nestedSubsequence: Template = {
  name: "nested-subsequence",
  handlers: HANDLERS,
  weight: 1,
  buildBlocks: () => [
    { type: "step", id: "prepare", handler: "nested_prepare", params: {} },
    {
      type: "sub_sequence",
      id: "child",
      sequence_name: "order-fulfillment",
      input: { from_parent: true },
    },
    { type: "step", id: "finalize", handler: "nested_finalize", params: {} },
  ],
  buildContext: (rng) => ({
    data: { parent_id: rng.uuid() },
  }),
};
