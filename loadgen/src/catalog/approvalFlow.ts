/**
 * approval-flow — kicks off, waits for a human signal (`approval_result`),
 * routes to ship or reject, ends with a notification. Instances stay in
 * `waiting` until the signaller posts an update_context with the
 * decision. Exercises: paused/waiting states, signal UI, context merge.
 */

import type { Template } from "./index.ts";
import type { Block } from "../types.ts";

const HANDLERS = ["approval_kickoff", "approval_ship", "approval_reject", "approval_notify"] as const;

export const approvalFlow: Template = {
  name: "approval-flow",
  handlers: HANDLERS,
  weight: 2,
  buildBlocks: () => [
    step("kickoff", "approval_kickoff"),
    {
      type: "step",
      id: "await",
      handler: "human_review",
      params: {
        instructions: "Approve or reject this request",
        reviewer: "loadgen",
      },
      wait_for_input: {
        prompt: "Loadgen approval prompt",
        timeout: 120_000,
      },
    },
    {
      type: "router",
      id: "route",
      routes: [
        {
          condition: 'context.data.approved == "yes"',
          blocks: [step("ship", "approval_ship")],
        },
      ],
      default: [step("reject", "approval_reject")],
    },
    step("notify", "approval_notify"),
  ],
  buildContext: (rng) => ({
    data: {
      request_id: rng.uuid(),
      requester: rng.pick(["alice", "bob", "carol", "dave"]),
      approved: null,
    },
  }),
};

function step(
  id: string,
  handler: string,
  params: Record<string, unknown> = {},
): Block {
  return { type: "step", id, handler, params };
}
