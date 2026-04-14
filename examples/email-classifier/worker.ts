/**
 * External worker for the email-classifier pipeline.
 *
 * Two handlers:
 *   check_subject_prefix — gate: only "VIVA ORCH8:" emails pass
 *   search_and_format    — web search + Slack message formatting
 *
 * Classification is handled by the engine's built-in `llm_call` handler
 * (no worker involvement needed for that step).
 *
 * Run: npm run worker
 */
import { Orch8Worker } from "../../worker-sdk-node/dist/index.js";

const ORCH8_URL = process.env.ORCH8_URL ?? "http://localhost:8080";

const worker = new Orch8Worker({
  engineUrl: ORCH8_URL,
  workerId: "email-classifier-worker-1",
  handlers: {
    /**
     * Gate: checks if subject starts with the required prefix.
     */
    check_subject_prefix: async (task) => {
      const { subject, prefix } = task.params as {
        subject: string;
        prefix: string;
      };

      const accepted = subject.startsWith(prefix);
      const clean_subject = accepted
        ? subject.slice(prefix.length).trim()
        : subject;

      return { accepted, clean_subject };
    },

    /**
     * Search the web for context about the email, then format a Slack message.
     * Receives the LLM classification result as input.
     */
    search_and_format: async (task) => {
      const { classification, subject, from } = task.params as {
        classification: string; // JSON string from llm_call output
        subject: string;
        from: string;
      };

      // Parse classification (llm_call returns content as a string)
      let category = "unknown";
      let priority = "medium";
      let reasoning = "";
      let search_query = subject;

      try {
        const parsed = JSON.parse(classification);
        category = parsed.category ?? category;
        priority = parsed.priority ?? priority;
        reasoning = parsed.reasoning ?? reasoning;
        search_query = parsed.search_query ?? search_query;
      } catch {
        console.warn("  Could not parse classification, using defaults");
      }

      console.log(`  Classification: ${category} / ${priority}`);
      console.log(`  Search query: ${search_query}`);

      // ─── Web search via DuckDuckGo ────────────────────────────────
      let searchResults: { title: string; snippet: string; url: string }[] = [];
      try {
        const encoded = encodeURIComponent(`${search_query}`);
        const searchRes = await fetch(
          `https://html.duckduckgo.com/html/?q=${encoded}`,
          {
            headers: {
              "User-Agent":
                "Mozilla/5.0 (compatible; orch8-example/1.0; +https://orch8.io)",
            },
          },
        );

        if (searchRes.ok) {
          const html = await searchRes.text();
          const resultBlocks = html.split('<div class="result ');
          for (
            let i = 1;
            i < resultBlocks.length && searchResults.length < 3;
            i++
          ) {
            const block = resultBlocks[i];
            const titleMatch = block.match(
              /<a class="result__a"[^>]*href="([^"]*)"[^>]*>([\s\S]*?)<\/a>/,
            );
            const snippetMatch = block.match(
              /<a class="result__snippet"[^>]*>([\s\S]*?)<\/a>/,
            );
            if (titleMatch && snippetMatch) {
              searchResults.push({
                url: titleMatch[1],
                title: titleMatch[2].replace(/<[^>]+>/g, "").trim(),
                snippet: snippetMatch[1].replace(/<[^>]+>/g, "").trim(),
              });
            }
          }
        }
      } catch {
        // Search is best-effort
      }

      console.log(`  Search results: ${searchResults.length}`);

      // ─── Format Slack message ─────────────────────────────────────
      const emoji =
        priority === "high"
          ? ":rotating_light:"
          : priority === "medium"
            ? ":large_yellow_circle:"
            : ":white_circle:";

      const searchContext =
        searchResults.length > 0
          ? searchResults.map((r) => `  • <${r.url}|${r.title}>`).join("\n")
          : "_No web context found._";

      const slack_message = [
        `${emoji} *New email — ${category}* (${priority})`,
        "",
        `*From:* ${from}`,
        `*Subject:* ${subject}`,
        `*AI reasoning:* ${reasoning}`,
        "",
        `*Web context:*`,
        searchContext,
      ].join("\n");

      return { slack_message, category, priority, reasoning };
    },
  },
});

await worker.start();
console.log(
  `Worker started (${ORCH8_URL}). Polling for: check_subject_prefix, search_and_format`,
);
console.log("Press Ctrl+C to stop.");

process.on("SIGINT", async () => {
  console.log("\nShutting down...");
  await worker.stop();
  process.exit(0);
});

function requiredEnv(key: string): string {
  const val = process.env[key];
  if (!val) {
    console.error(`ERROR: Missing env var ${key}`);
    console.error("Copy .env.example to .env and fill in your keys.");
    process.exit(1);
  }
  return val;
}
