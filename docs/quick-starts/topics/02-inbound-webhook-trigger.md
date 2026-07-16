# Accept a signed inbound webhook

Use a public webhook trigger when an external system should start a workflow
without receiving your Orch8 API key. The trigger owns a separate shared
secret, verifies an exact-body HMAC, and rejects stale or replayed requests.

**Starting point:** the Level 3 server and `durable-welcome` sequence are
running. `openssl`, `curl`, and `jq` are installed.

**Result:** `POST /webhooks/customer-created` starts one authenticated workflow
instance, while replaying the same signed request is rejected.

## 1. Create the public trigger

Generate a secret without printing it and register the trigger through the
authenticated management API:

```bash
export TRIGGER_SECRET=$(openssl rand -hex 32)

curl -fsS -X POST "$ORCH8_URL/triggers" \
  -H "x-api-key: $ORCH8_API_KEY" \
  -H "x-tenant-id: demo" \
  -H "content-type: application/json" \
  -d "{
    \"slug\": \"customer-created\",
    \"sequence_name\": \"durable-welcome\",
    \"tenant_id\": \"demo\",
    \"namespace\": \"default\",
    \"trigger_type\": \"webhook\",
    \"secret\": \"$TRIGGER_SECRET\"
  }" | jq '{slug, sequence_name, trigger_type, enabled}'
```

The management request uses the root API key. Public deliveries do not; they
authenticate with the trigger-specific signature.

## 2. Build one exact request body

Do not pretty-print or reserialize the body after signing it. The bytes sent by
`curl` must be the same bytes used for the HMAC:

```bash
BODY='{"customer":"Webhook User"}'
TIMESTAMP=$(date +%s)
NONCE=$(openssl rand -hex 16)
```

The signed message is:

```text
timestamp.nonce.exact-request-body
```

Compute HMAC-SHA256 and encode it as unpadded base64url:

```bash
SIGNATURE=$(printf '%s.%s.%s' "$TIMESTAMP" "$NONCE" "$BODY" |
  openssl dgst -sha256 -hmac "$TRIGGER_SECRET" -binary |
  openssl base64 -A |
  tr '+/' '-_' |
  tr -d '=')
```

## 3. Deliver the webhook

Public webhook ingestion is rooted at the server origin, not `/api/v1`:

```bash
WEBHOOK_RESPONSE=$(curl -fsS -X POST \
  http://127.0.0.1:8080/webhooks/customer-created \
  -H "content-type: application/json" \
  -H "x-trigger-timestamp: $TIMESTAMP" \
  -H "x-trigger-nonce: $NONCE" \
  -H "x-orch8-signature: v1=$SIGNATURE" \
  --data-binary "$BODY")

INSTANCE_ID=$(printf '%s' "$WEBHOOK_RESPONSE" | jq -r '.instance_id')
printf '%s\n' "$INSTANCE_ID"
```

The request body becomes the new instance's `context.data`. Watch it finish:

```bash
orch8 instance get "$INSTANCE_ID" --watch
```

## 4. Prove replay protection

Send the exact same timestamp, nonce, signature, and body again:

```bash
curl -i -X POST \
  http://127.0.0.1:8080/webhooks/customer-created \
  -H "content-type: application/json" \
  -H "x-trigger-timestamp: $TIMESTAMP" \
  -H "x-trigger-nonce: $NONCE" \
  -H "x-orch8-signature: v1=$SIGNATURE" \
  --data-binary "$BODY"
```

The server rejects the replay with `401`. A legitimate retry must use a fresh
nonce and recompute the signature. Timestamps may be at most five minutes old
and no more than 60 seconds ahead of the server clock.

## 5. Inspect and remove the trigger

```bash
curl -fsS \
  -H "x-api-key: $ORCH8_API_KEY" \
  -H "x-tenant-id: demo" \
  "$ORCH8_URL/triggers/customer-created" |
  jq '{slug, trigger_type, enabled, sequence_name}'

curl -fsS -X DELETE \
  -H "x-api-key: $ORCH8_API_KEY" \
  -H "x-tenant-id: demo" \
  "$ORCH8_URL/triggers/customer-created"
```

## Checkpoint

You are done when the first request returns `202` with an instance ID, the
workflow sees `data.customer`, and the exact replay returns `401`.

Use [Sequence triggers](../../SEQUENCES.md#use-triggers) for internal event and
Activepieces polling triggers. Do not confuse this inbound endpoint with
[outbound lifecycle webhooks](03-outbound-lifecycle-webhooks.md).

## If something failed

| Symptom | Likely cause | Fix |
|---|---|---|
| `401` on the first delivery | Signed bytes differ from sent bytes | Use `--data-binary "$BODY"` and do not reformat after signing |
| `401` after a long pause | Timestamp is outside the replay window | Create a fresh timestamp, nonce, and signature |
| `404` | Slug is wrong, disabled, or not a webhook trigger | Inspect it through the authenticated management endpoint |
| `400 invalid webhook JSON` | The signed body is not valid JSON | Validate with `printf '%s' "$BODY" | jq` before signing |

