/**
 * Trigger an email-classifier instance.
 *
 * Simulates what Resend's inbound webhook would send. In production you'd
 * configure Resend to POST directly to:
 *   POST https://your-orch8.com/instances
 *
 * Run: npm run trigger
 * Run with non-matching subject: npm run trigger -- --skip
 */
const ORCH8_URL = process.env.ORCH8_URL ?? "http://localhost:8080";
const shouldSkip = process.argv.includes("--skip");

const emailPayload = shouldSkip
  ? {
      // This email does NOT have "VIVA ORCH8:" prefix — will be skipped
      from: "newsletter@marketing.io",
      subject: "Weekly digest: top 10 AI tools",
      body: "Here are this week's trending AI tools...",
      to: "enquire@auto.orch8.io",
      message_id: "<skip123@marketing.io>",
    }
  : {
      // This email HAS "VIVA ORCH8:" prefix — will be fully processed
      from: "sarah.chen@bigcorp.com",
      subject: "VIVA ORCH8: Enterprise pricing for 500 seats",
      body: `Hi,

We're evaluating workflow orchestration tools for our platform team
(~500 engineers). We're currently using Temporal but looking for
something with less operational overhead.

Questions:
1. Do you support on-premise deployment?
2. What's the pricing for 500+ seats?
3. Do you have SOC2 compliance?

We'd like to start a pilot in Q2.

Thanks,
Sarah Chen
VP Engineering, BigCorp`,
      to: "enquire@auto.orch8.io",
      message_id: "<abc123@mail.bigcorp.com>",
    };

// Get the sequence ID by name
const seqRes = await fetch(
  `${ORCH8_URL}/sequences/by-name?tenant_id=example&namespace=default&name=email-classifier`,
);
if (!seqRes.ok) {
  console.error(
    `Sequence not found. Run 'npm run deploy' first. (${seqRes.status})`,
  );
  process.exit(1);
}
const seq = (await seqRes.json()) as { id: string };

// Create instance
const res = await fetch(`${ORCH8_URL}/instances`, {
  method: "POST",
  headers: { "Content-Type": "application/json" },
  body: JSON.stringify({
    sequence_id: seq.id,
    tenant_id: "example",
    namespace: "default",
    context: {
      data: {
        ...emailPayload,
        received_at: new Date().toISOString(),
      },
    },
  }),
});

if (!res.ok) {
  console.error(`Failed to create instance: ${res.status} ${await res.text()}`);
  process.exit(1);
}

const instance = (await res.json()) as { id: string };
console.log(`Created instance: ${instance.id}`);
console.log(
  shouldSkip
    ? "Subject has no 'VIVA ORCH8:' prefix — will be skipped (noop)."
    : "Subject has 'VIVA ORCH8:' prefix — full pipeline will run.",
);
console.log(`\nTrack: curl ${ORCH8_URL}/instances/${instance.id}`);
