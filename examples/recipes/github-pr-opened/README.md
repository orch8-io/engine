# GitHub: `pull_request` with `action: opened`

Starts `github-pr-opened` for every verified GitHub delivery. The trigger
uses the `github` signature preset, so Orch8 checks the provider's own
signature over the raw request body before any instance is created; bad,
stale, or replayed deliveries get `401` and never reach the workflow.

## 1. Store the secrets

```bash
export ORCH8_URL=http://127.0.0.1:8080/api/v1
export ORCH8_TENANT_ID=demo

# The webhook signing secret: the webhook secret you typed into GitHub.
curl -s -X POST "$ORCH8_URL/credentials" -H "X-Tenant-Id: demo" -H "Content-Type: application/json" \
  -d '{"id": "github-webhook", "name": "GitHub webhook", "kind": "api_key", "tenant_id": "demo", "value": "REPLACE_ME"}'
```

Also create the credentials the steps reference (see `sequence.json`), e.g.
`resend`, `aws-ses`, `smtp`, or the chat webhook URLs.

## 2. Upload the sequence and create the trigger

```bash
orch8 sequence create --file examples/recipes/github-pr-opened/sequence.json

curl -s -X POST "$ORCH8_URL/triggers" -H "X-Tenant-Id: demo" -H "Content-Type: application/json" \
  -d '{"slug": "github-pr-opened", "sequence_name": "github-pr-opened", "tenant_id": "demo",
       "trigger_type": "webhook",
       "config": {"verify": {"preset": "github", "secret_ref": "credentials://github-webhook"}}}'
```

## 3. Point GitHub at Orch8

```
Repository -> Settings -> Webhooks -> Add webhook: Payload URL = $ORCH8_PUBLIC_URL/webhooks/github-pr-opened, Content type = application/json, events = Pull requests
```

The public endpoint is `POST /webhooks/github-pr-opened` (no API key; the signature is
the authentication).

## How verification works

- GitHub signs the raw body with HMAC-SHA256 and sends `X-Hub-Signature-256: sha256=<hex>`.
- GitHub does not sign a timestamp, so replay protection keys on `X-GitHub-Delivery` (remembered for 72 h). The instance metadata records `provider_event` from `X-GitHub-Event`.
- The content type must be `application/json` (form-encoded payloads are not JSON bodies).
- Secrets live only in the tenant's credential store; nothing secret is written to logs or instance data.
