/**
 * race-fetch — three concurrent fetchers, first-to-resolve wins.
 * Exercises Race cancellation: slower branches get cancelled when a
 * faster one returns, and that should show in the execution tree.
 */

import type { Template } from "./index.ts";

const HANDLERS = [
  "race_fetch_primary",
  "race_fetch_mirror",
  "race_fetch_cache",
  "race_converge",
] as const;

export const raceFetch: Template = {
  name: "race-fetch",
  handlers: HANDLERS,
  weight: 1,
  buildBlocks: () => [
    {
      type: "race",
      id: "race",
      semantics: "first_to_resolve",
      branches: [
        [{ type: "step", id: "primary", handler: "race_fetch_primary", params: {} }],
        [{ type: "step", id: "mirror", handler: "race_fetch_mirror", params: {} }],
        [{ type: "step", id: "cache", handler: "race_fetch_cache", params: {} }],
      ],
    },
    { type: "step", id: "converge", handler: "race_converge", params: {} },
  ],
  buildContext: (rng) => ({
    data: { request_id: rng.uuid(), region: rng.pick(["us-east", "eu-west", "ap-south"]) },
  }),
};
