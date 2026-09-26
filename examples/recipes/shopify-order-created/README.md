# Shopify: `orders/create`

Starts `shopify-order-created` for every verified Shopify delivery. The trigger
uses the `shopify` signature preset, so Orch8 checks the provider's own
signature over the raw request body before any instance is created; bad,
stale, or replayed deliveries get `401` and never reach the workflow.

## 1. Store the secrets

```bash
export ORCH8_URL=http://127.0.0.1:8080/api/v1
export ORCH8_TENANT_ID=demo

# The webhook signing secret: the app's client secret / the webhook signing key shown by Shopify.
curl -s -X POST "$ORCH8_URL/credentials" -H "X-Tenant-Id: demo" -H "Content-Type: application/json" \
  -d '{"id": "shopify-webhook", "name": "Shopify webhook", "kind": "api_key", "tenant_id": "demo", "value": "REPLACE_ME"}'
```

Also create the credentials the steps reference (see `sequence.json`), e.g.
`resend`, `aws-ses`, `smtp`, or the chat webhook URLs.

## 2. Upload the sequence and create the trigger

```bash
orch8 sequence create --file examples/recipes/shopify-order-created/sequence.json

curl -s -X POST "$ORCH8_URL/triggers" -H "X-Tenant-Id: demo" -H "Content-Type: application/json" \
  -d '{"slug": "shopify-order-created", "sequence_name": "shopify-order-created", "tenant_id": "demo",
       "trigger_type": "webhook",
       "config": {"verify": {"preset": "shopify", "secret_ref": "credentials://shopify-webhook"}}}'
```

## 3. Point Shopify at Orch8

```
Shopify admin -> Settings -> Notifications -> Webhooks (or the Admin API) -> event Order creation, format JSON, URL $ORCH8_PUBLIC_URL/webhooks/shopify-order-created
```

The public endpoint is `POST /webhooks/shopify-order-created` (no API key; the signature is
the authentication).

## How verification works

- Shopify sends `X-Shopify-Hmac-Sha256: base64(HMAC-SHA256(secret, raw body))`.
- Replay protection keys on `X-Shopify-Webhook-Id` (72 h). `X-Shopify-Topic` is recorded as `provider_event`.
- `aws` resolves to a credential holding `{"access_key_id", "secret_access_key", "region"}`.
- Secrets live only in the tenant's credential store; nothing secret is written to logs or instance data.
