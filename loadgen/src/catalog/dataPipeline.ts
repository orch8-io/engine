/**
 * data-pipeline — ForEach over a collection of items, each iteration
 * runs a transform via an external worker. Exercises fan-out and
 * queue depth on the Tasks dashboard.
 */

import type { Template } from "./index.ts";

const HANDLERS = ["pipeline_transform", "pipeline_summarize"] as const;

export const dataPipeline: Template = {
  name: "data-pipeline",
  handlers: HANDLERS,
  weight: 2,
  buildBlocks: () => [
    {
      type: "for_each",
      id: "loop",
      collection: "{{context.data.items}}",
      item_var: "item",
      max_iterations: 200,
      body: [
        {
          type: "step",
          id: "transform",
          handler: "pipeline_transform",
          params: { item: "{{item}}" },
          queue_name: "pipeline",
          retry: { max_attempts: 2, initial_backoff: 500, max_backoff: 2000 },
        },
      ],
    },
    {
      type: "step",
      id: "summarize",
      handler: "pipeline_summarize",
      params: {},
    },
  ],
  buildContext: (rng) => {
    const n = rng.int(10, 50);
    return {
      data: {
        items: Array.from({ length: n }, (_, i) => ({
          idx: i,
          payload: rng.uuid(),
        })),
      },
    };
  },
};
