# Stripe: `payment_intent.succeeded`

Starts `stripe-payment-succeeded` for every verified Stripe delivery. The trigger
uses the `stripe` signature preset, so Orch8 checks the provider's own
signature over the raw request body before any instance is created; bad,
stale, or replayed deliveries get `401` and never reach the workflow.

## 1. Store the secrets

```bash
export ORCH8_URL=http://127.0.0.1:8080/api/v1
export ORCH8_TENANT_ID=demo

# The webhook signing secret: the endpoint's signing secret (`whsec_...`).
curl -s -X POST "$ORCH8_URL/credentials" -H "X-Tenant-Id: demo" -H "Content-Type: application/json" \
  -d '{"id": "stripe-webhook", "name": "Stripe webhook", "kind": "api_key", "tenant_id": "demo", "value": "REPLACE_ME"}'
```

Also create the credentials the steps reference (see `sequence.json`), e.g.
`resend`, `aws-ses`, `smtp`, or the chat webhook URLs.

## 2. Upload the sequence and create the trigger

```bash
orch8 sequence create --file examples/recipes/stripe-payment-succeeded/sequence.json

curl -s -X POST "$ORCH8_URL/triggers" -H "X-Tenant-Id: demo" -H "Content-Type: application/json" \
  -d '{"slug": "stripe-payment-succeeded", "sequence_name": "stripe-payment-succeeded", "tenant_id": "demo",
       "trigger_type": "webhook",
       "config": {"verify": {"preset": "stripe", "secret_ref": "credentials://stripe-webhook"}}}'
```

## 3. Point Stripe at Orch8

```
stripe listen --forward-to "$ORCH8_PUBLIC_URL/webhooks/stripe-payment-succeeded"   # or add the endpoint in the Stripe dashboard
```

The public endpoint is `POST /webhooks/stripe-payment-succeeded` (no API key; the signature is
the authentication).

## How verification works

- Stripe signs `"{t}.{raw body}"` with HMAC-SHA256 and sends `Stripe-Signature: t=...,v1=...`. Orch8 verifies every `v1` candidate in constant time and rejects timestamps more than 300 s from the server clock (`tolerance_secs` to change it).
- Each `(t, signature)` pair is claimed once, so a captured delivery cannot be replayed inside the tolerance window.
- The whole Stripe event becomes `context.data`, so `context.data.type` and `context.data.data.object` are available to templates.
- Secrets live only in the tenant's credential store; nothing secret is written to logs or instance data.
