# Clerk (Svix): `user.created`

Starts `clerk-user-created` for every verified Clerk (Svix) delivery. The trigger
uses the `svix` signature preset, so Orch8 checks the provider's own
signature over the raw request body before any instance is created; bad,
stale, or replayed deliveries get `401` and never reach the workflow.

## 1. Store the secrets

```bash
export ORCH8_URL=http://127.0.0.1:8080/api/v1
export ORCH8_TENANT_ID=demo

# The webhook signing secret: the endpoint signing secret (`whsec_...`, used verbatim).
curl -s -X POST "$ORCH8_URL/credentials" -H "X-Tenant-Id: demo" -H "Content-Type: application/json" \
  -d '{"id": "clerk-webhook", "name": "Clerk (Svix) webhook", "kind": "api_key", "tenant_id": "demo", "value": "REPLACE_ME"}'
```

Also create the credentials the steps reference (see `sequence.json`), e.g.
`resend`, `aws-ses`, `smtp`, or the chat webhook URLs.

## 2. Upload the sequence and create the trigger

```bash
orch8 sequence create --file examples/recipes/clerk-user-created/sequence.json

curl -s -X POST "$ORCH8_URL/triggers" -H "X-Tenant-Id: demo" -H "Content-Type: application/json" \
  -d '{"slug": "clerk-user-created", "sequence_name": "clerk-user-created", "tenant_id": "demo",
       "trigger_type": "webhook",
       "config": {"verify": {"preset": "svix", "secret_ref": "credentials://clerk-webhook"}}}'
```

## 3. Point Clerk (Svix) at Orch8

```
Clerk dashboard -> Webhooks -> Add endpoint: $ORCH8_PUBLIC_URL/webhooks/clerk-user-created, subscribe to user.created
```

The public endpoint is `POST /webhooks/clerk-user-created` (no API key; the signature is
the authentication).

## How verification works

- Clerk (like Resend and every other Svix sender) signs `"{svix-id}.{svix-timestamp}.{raw body}"`; the `whsec_` secret is base64-decoded to the HMAC key. Standard Webhooks `webhook-id/-timestamp/-signature` headers are accepted too.
- Timestamps more than 300 s from the server clock are refused and each `svix-id` is accepted once.
- `smtp` resolves to a credential holding `{"host", "port", "username", "password", "tls": "starttls"}`.
- Secrets live only in the tenant's credential store; nothing secret is written to logs or instance data.
