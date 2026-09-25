/**
 * llm-chain — calls `llm_call` built-in twice: first generation, then
 * summarization. GATED: only included in the active catalog when
 * `LOADGEN_ENABLE_LLM=1` (needs real API keys on the server).
 *
 * The template itself is defined for completeness — the catalog loader
 * decides whether to use it based on the `enableLlm` flag in config.
 */

import type { Template } from "./index.ts";

export const llmChain: Template = {
  name: "llm-chain",
  handlers: [], // all built-ins, no external worker queues
  weight: 0,    // set to non-zero by the catalog loader when LLM is enabled
  buildBlocks: () => [
    {
      type: "step",
      id: "generate",
      handler: "llm_call",
      params: {
        provider: "openai",
        api_key_env: "OPENAI_API_KEY",
        model: "gpt-4o-mini",
        max_tokens: 256,
        messages: [
          { role: "system", content: "You are a concise assistant." },
          { role: "user", content: "Write one sentence about Saturn's rings." },
        ],
      },
      timeout: 30_000,
      retry: { max_attempts: 2, initial_backoff: 1000, max_backoff: 4000 },
    },
    {
      type: "step",
      id: "summarize",
      handler: "llm_call",
      params: {
        provider: "openai",
        api_key_env: "OPENAI_API_KEY",
        model: "gpt-4o-mini",
        max_tokens: 128,
        messages: [
          { role: "system", content: "Summarize in 5 words." },
          {
            role: "user",
            content: "{{outputs.generate.choices[0].message.content|default:hello}}",
          },
        ],
      },
      timeout: 20_000,
    },
  ],
  buildContext: (rng) => ({
    data: { topic: rng.pick(["space", "ocean", "forest", "desert"]) },
  }),
};
