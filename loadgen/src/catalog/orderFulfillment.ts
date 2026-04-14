/**
 * order-fulfillment — Parallel(validate, charge, reserve) → ship → notify.
 *
 * Showcases the Parallel block with three independent branches all
 * dispatched to external workers, then a sequential tail with a
 * heavier-weight ship step (longer timeout) and a fast notify step.
 */

import type { Template } from "./index.ts";
import type { Block } from "../types.ts";

const HANDLERS = [
  "order_validate",
  "order_charge",
  "order_reserve",
  "order_ship",
  "order_notify",
] as const;

export const orderFulfillment: Template = {
  name: "order-fulfillment",
  handlers: HANDLERS,
  weight: 3,
  buildBlocks: () => [
    {
      type: "parallel",
      id: "preflight",
      branches: [
        [step("validate", "order_validate")],
        [step("charge", "order_charge", { amount: 0 })],
        [step("reserve", "order_reserve")],
      ],
    },
    step("ship", "order_ship", {}, { timeout: 30_000 }),
    step("notify", "order_notify"),
  ],
  buildContext: (rng) => ({
    data: {
      order_id: rng.uuid(),
      amount_cents: rng.int(500, 50_000),
      currency: rng.pick(["USD", "EUR", "GBP"]),
      customer_tier: rng.weighted(["bronze", "silver", "gold"], [0.6, 0.3, 0.1]),
    },
  }),
};

function step(
  id: string,
  handler: string,
  params: Record<string, unknown> = {},
  extra: Record<string, unknown> = {},
): Block {
  return { type: "step", id, handler, params, ...extra };
}
