# Deliver signed lifecycle webhooks

Use outbound webhooks when another service needs low-latency notification that
an Orch8 instance completed, failed, or breached an SLA. Delivery is
asynchronous and at-least-once, so the receiver must verify the raw body and
deduplicate retries.

**Starting point:** Node.js 20 or newer and the Level 3 project directory.
This guide restarts the server to add webhook configuration.

**Result:** a local receiver validates `X-Orch8-Signature`, records one event,
and acknowledges duplicates safely.

## 1. Create a raw-body receiver

Create `webhook-receiver.mjs` in the Level 3 project:

```js
import { createServer } from "node:http";
import { createHmac, timingSafeEqual } from "node:crypto";

const secret = process.env.WEBHOOK_SECRET;
if (!secret) throw new Error("WEBHOOK_SECRET is required");
const seen = new Set();

createServer((req, res) => {
  const chunks = [];
  req.on("data", (chunk) => chunks.push(chunk));
  req.on("end", () => {
    const body = Buffer.concat(chunks);
    const timestamp = req.headers["x-orch8-timestamp"] ?? "";
    const supplied = req.headers["x-orch8-signature"] ?? "";
    const expected = `sha256=${createHmac("sha256", secret)
      .update(`${timestamp}.`)
      .update(body)
      .digest("hex")}`;

    const valid = supplied.length === expected.length &&
      timingSafeEqual(Buffer.from(supplied), Buffer.from(expected));
    if (!valid) {
      res.writeHead(401).end("invalid signature");
      return;
    }

    const event = JSON.parse(body.toString("utf8"));
    const key = `${event.instance_id}:${event.event_type}:${event.timestamp}`;
    if (!seen.has(key)) {
      seen.add(key);
      console.log(JSON.stringify(event));
    }
    res.writeHead(202).end();
  });
}).listen(3000, "127.0.0.1", () => {
  console.log("receiver listening on http://127.0.0.1:3000/orch8-events");
});
```

Signature verification happens before JSON parsing because the signature covers
the exact request bytes, not a reconstructed object.

## 2. Start the receiver

In terminal A:

```bash
umask 077
openssl rand -hex 32 > /tmp/orch8-webhook-secret
export WEBHOOK_SECRET=$(sed -n '1p' /tmp/orch8-webhook-secret)
node webhook-receiver.mjs
```

Keep the same secret available for the Orch8 process. In a separate terminal,
read the same temporary file; do not generate a second value:

```bash
export WEBHOOK_SECRET=$(sed -n '1p' /tmp/orch8-webhook-secret)
```

## 3. Restart Orch8 with outbound delivery enabled

Stop the Level 3 server. In terminal B, restore its existing API and encryption
variables, then add:

```bash
export ORCH8_WEBHOOK_URLS=http://127.0.0.1:3000/orch8-events
export ORCH8_WEBHOOK_SECRET="$WEBHOOK_SECRET"
orch8-server --config orch8.toml
```

Webhook targets are operator-controlled configuration. Use HTTPS and a private
network or authenticated gateway for production receivers.

## 4. Complete an instance

Resolve the sequence and start one run:

```bash
SEQUENCE_ID=$(orch8 --output json sequence lookup \
  demo default durable-welcome | jq -r '.id')
INSTANCE_ID=$(orch8 --output json instance create \
  --sequence-id "$SEQUENCE_ID" \
  --tenant-id demo \
  --context '{"data":{"customer":"Webhook Receiver"}}' | jq -r '.id')
orch8 instance get "$INSTANCE_ID" --watch
```

Terminal A prints an `instance.completed` event. The receiver returns quickly;
real work should be queued rather than performed inside the request.

## 5. Inspect delivery health

Confirm a success metric exists:

```bash
curl -fsS http://127.0.0.1:8080/metrics |
  grep -E 'orch8_webhooks_(sent|failed|parked)_total'
```

List exhausted deliveries waiting for operator action:

```bash
curl -fsS \
  -H "x-api-key: $ORCH8_API_KEY" \
  -H "x-tenant-id: demo" \
  "$ORCH8_URL/webhooks/outbox" | jq
```

An empty outbox after a successful delivery is expected. Failed deliveries are
retried and then parked; they are never silently discarded.

After the exercise, stop the receiver and remove its temporary secret:

```bash
rm -f /tmp/orch8-webhook-secret
```

## Checkpoint

You are done when the receiver prints the event, the instance ID matches, the
signature is accepted from raw bytes, and the sent counter increases.

For retry timing, redelivery endpoints, and subscriber rules, use the
[webhooks guide](../../WEBHOOKS.md).

## If something failed

| Symptom | Likely cause | Fix |
|---|---|---|
| Receiver reports invalid signature | Receiver and engine secrets differ | Export the same secret in both processes and restart Orch8 |
| No event arrives | Server was not restarted with webhook variables | Inspect the startup environment and trigger a new instance |
| Duplicate event appears | At-least-once retry occurred | Persist the composite deduplication key instead of using memory in production |
