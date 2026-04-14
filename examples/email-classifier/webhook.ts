/**
 * Webhook adapter: Resend inbound → orch8 instance.
 *
 * Receives Resend's `email.received` POST, transforms it into an orch8
 * instance creation request, and forwards to the engine.
 *
 * Run: npm run webhook
 */
import { createServer } from "node:http";

const ORCH8_URL = process.env.ORCH8_URL ?? "http://localhost:8080";
const PORT = parseInt(process.env.WEBHOOK_PORT ?? "3333", 10);

// You must deploy the sequence first (`npm run deploy`) and paste the ID here,
// or we look it up by name on startup.
let sequenceId: string | null = null;

async function resolveSequenceId(): Promise<string> {
  const res = await fetch(
    `${ORCH8_URL}/sequences/by-name?tenant_id=example&namespace=default&name=email-classifier`,
  );
  if (!res.ok) {
    throw new Error(`Sequence not found. Run 'npm run deploy' first.`);
  }
  const seq = (await res.json()) as { id: string };
  return seq.id;
}

const server = createServer(async (req, res) => {
  if (req.method !== "POST") {
    res.writeHead(405).end("Method Not Allowed");
    return;
  }

  const chunks: Buffer[] = [];
  for await (const chunk of req) chunks.push(chunk as Buffer);
  const body = JSON.parse(Buffer.concat(chunks).toString());

  // Resend payload: { type: "email.received", data: { from, to, subject, text, ... } }
  if (body.type !== "email.received") {
    res.writeHead(200).end("ignored");
    return;
  }

  const email = body.data;
  console.log(`\nInbound email from: ${email.from}`);
  console.log(`  Subject: ${email.subject}`);

  if (!sequenceId) {
    sequenceId = await resolveSequenceId();
  }

  const instance = await fetch(`${ORCH8_URL}/instances`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({
      sequence_id: sequenceId,
      tenant_id: "example",
      namespace: "default",
      context: {
        data: {
          from: email.from,
          subject: email.subject,
          body: email.text ?? email.html ?? "",
          to: Array.isArray(email.to) ? email.to[0] : email.to,
          message_id: email.message_id,
          received_at: email.created_at ?? new Date().toISOString(),
        },
      },
    }),
  });

  if (!instance.ok) {
    const err = await instance.text();
    console.error(`  ERROR creating instance: ${instance.status} ${err}`);
    res.writeHead(500).end(err);
    return;
  }

  const result = (await instance.json()) as { id: string };
  console.log(`  Created instance: ${result.id}`);

  res.writeHead(200).end(JSON.stringify({ instance_id: result.id }));
});

server.listen(PORT, () => {
  console.log(`Webhook adapter listening on http://localhost:${PORT}`);
  console.log(`Forwarding to orch8 at ${ORCH8_URL}`);
  console.log(`\nPoint ngrok here: ngrok http ${PORT}`);
});
