/**
 * Deploy the email-classifier workflow to the orch8 engine.
 *
 * Only emails whose subject starts with "VIVA ORCH8:" are processed.
 *
 * Uses the built-in `llm_call` handler for DeepSeek classification (runs
 * inline in the engine — no worker needed for that step).
 *
 * Run: npm run deploy
 */
const ORCH8_URL = process.env.ORCH8_URL ?? "http://localhost:8080";
const DEEPSEEK_API_KEY = process.env.DEEPSEEK_API_KEY ?? "";
const SLACK_CHANNEL_ID = process.env.SLACK_CHANNEL_ID ?? "C0123456789";
const SLACK_ACCESS_TOKEN = process.env.SLACK_ACCESS_TOKEN ?? "";

const sequence = {
  id: crypto.randomUUID(),
  tenant_id: "example",
  namespace: "default",
  name: "email-classifier",
  version: 8,
  created_at: new Date().toISOString(),
  blocks: [
    // Step 1: Gate — check "VIVA ORCH8:" prefix (worker handler)
    {
      type: "step",
      id: "gate",
      handler: "check_subject_prefix",
      params: {
        subject: "{{context.data.subject}}",
        prefix: "VIVA ORCH8:",
      },
    },
    // Step 2: Router — only process emails that passed the gate
    {
      type: "router",
      id: "filter",
      routes: [
        {
          condition: "outputs.gate.accepted == true",
          blocks: [
            // Built-in llm_call — runs INLINE in engine (not worker queue).
            // Completes before subsequent steps in this branch are evaluated.
            {
              type: "step",
              id: "classify",
              handler: "llm_call",
              params: {
                provider: "openai",
                base_url: "https://api.deepseek.com",
                api_key: DEEPSEEK_API_KEY,
                model: "deepseek-chat",
                temperature: 0.1,
                response_format: { type: "json_object" },
                messages: [
                  {
                    role: "system",
                    content: [
                      "You classify inbound business emails. Analyze sender, subject, and body.",
                      "Respond with JSON:",
                      '{',
                      '  "category": "support" | "sales" | "billing" | "partnership" | "hiring" | "spam" | "other",',
                      '  "priority": "high" | "medium" | "low",',
                      '  "search_query": "concise web search query (max 10 words) about the sender or topic",',
                      '  "reasoning": "one sentence explanation"',
                      '}',
                      '',
                      'Priority guide:',
                      '- high: revenue >$10k, outage, legal, executive sender',
                      '- medium: standard inquiry, feature request',
                      '- low: newsletters, automated notifications',
                    ].join("\n"),
                  },
                  {
                    role: "user",
                    content:
                      "From: {{context.data.from}}\nSubject: {{outputs.gate.clean_subject}}\n\n{{context.data.body}}",
                  },
                ],
              },
            },
            // Worker step: search web + format Slack message.
            // By the time this is dispatched, classify has completed (inline).
            {
              type: "step",
              id: "format",
              handler: "search_and_format",
              params: {
                classification: "{{outputs.classify.message.content}}",
                subject: "{{outputs.gate.clean_subject}}",
                from: "{{context.data.from}}",
              },
            },
          ],
        },
      ],
      default: [
        {
          type: "step",
          id: "skip",
          handler: "noop",
          params: {},
        },
      ],
    },
    // Step 3: Notify — runs AFTER router completes (top-level = sequential)
    {
      type: "router",
      id: "notify_gate",
      routes: [
        {
          condition: "outputs.gate.accepted == true",
          blocks: [
            {
              type: "step",
              id: "notify",
              handler: "ap://slack.send_channel_message",
              params: {
                auth: { access_token: SLACK_ACCESS_TOKEN },
                props: {
                  channel: SLACK_CHANNEL_ID,
                  text: "{{outputs.format.slack_message}}",
                  sendAsBot: true,
                },
              },
            },
          ],
        },
      ],
      default: [
        {
          type: "step",
          id: "done",
          handler: "noop",
          params: {},
        },
      ],
    },
  ],
};

const res = await fetch(`${ORCH8_URL}/sequences`, {
  method: "POST",
  headers: { "Content-Type": "application/json" },
  body: JSON.stringify(sequence),
});

if (!res.ok) {
  console.error(`Failed to deploy: ${res.status} ${await res.text()}`);
  process.exit(1);
}

console.log(`Deployed sequence: ${sequence.name} (id: ${sequence.id})`);
