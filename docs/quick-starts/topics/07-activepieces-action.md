# Run an Activepieces action

Use the Activepieces sidecar when a workflow needs a community integration but
the engine should remain a small Rust runtime. An `ap://piece.action` handler
is dispatched over HTTP to the Node sidecar, which loads the installed piece
and returns its output.

This guide uses the credential-free HTTP `parse_url` action. You can prove the
integration without creating a Slack, Stripe, or Gmail account.

**Starting point:** Node.js 20 or newer, the repository checkout, and the
Level 3 project. This guide restarts the engine.

**Result:** an Orch8 step executes an installed Activepieces community action
and a downstream step reads its structured result.

## 1. Build the sidecar

From the repository root:

```bash
cd activepieces
npm install
npm run build
```

The checked-in dependencies include `@activepieces/piece-http`. Additional
pieces are made available by installing their `@activepieces/piece-*` package
in this directory and restarting the sidecar.

## 2. Start and verify the sidecar

In terminal A:

```bash
cd activepieces
export ORCH8_AP_ALLOWLIST=http
node dist/index.js
```

Verify its public health endpoint:

```bash
curl -fsS http://127.0.0.1:50052/health | jq
```

The allowlist is optional, but limiting a production sidecar to approved piece
names reduces accidental integration surface.

## 3. Restart Orch8 with the sidecar endpoint

Stop the Level 3 server. In its terminal, restore the existing API and
encryption variables, then start:

```bash
export ORCH8_ACTIVEPIECES_URL=http://127.0.0.1:50052/execute
orch8-server --config orch8.toml
```

The URL is read once on first use. Changing it after the engine has dispatched
an `ap://` step requires a restart.

## 4. Author a piece-backed sequence

Create `activepieces-sequence.json` in the Level 3 project:

```json
{
  "id": "019a0000-0000-7000-8000-000000000107",
  "tenant_id": "demo",
  "namespace": "default",
  "name": "parse-campaign-url",
  "version": 1,
  "input_schema": {
    "type": "object",
    "required": ["url"],
    "properties": { "url": { "type": "string" } },
    "additionalProperties": false
  },
  "blocks": [
    {
      "type": "step",
      "id": "parse_url",
      "handler": "ap://http.parse_url",
      "params": {
        "props": {
          "url": "{{data.url}}",
          "returnArrays": true
        }
      },
      "retry": {
        "max_attempts": 2,
        "initial_backoff": 500,
        "max_backoff": 2000
      }
    },
    {
      "type": "step",
      "id": "record_domain",
      "handler": "log",
      "params": {
        "message": "Parsed domain {{outputs.parse_url.domain}}"
      }
    }
  ],
  "created_at": "2026-01-01T00:00:00Z"
}
```

`auth` is omitted because the HTTP piece declares no authentication for this
action. Credentialed pieces receive `params.auth` and `params.props` through
the same envelope.

## 5. Publish and run it

```bash
orch8 sequence preflight --file activepieces-sequence.json
orch8 sequence apply activepieces-sequence.json
SEQUENCE_ID=$(orch8 --output json sequence lookup \
  demo default parse-campaign-url | jq -r '.id')

INSTANCE_ID=$(orch8 --output json instance create \
  --sequence-id "$SEQUENCE_ID" \
  --tenant-id demo \
  --context '{"data":{"url":"https://example.com/pricing?utm_source=orch8&tag=a&tag=b"}}' |
  jq -r '.id')
orch8 instance get "$INSTANCE_ID" --watch
orch8 --output json instance outputs "$INSTANCE_ID" |
  jq '.[] | select(.block_id == "parse_url") | .output'
```

The output includes `domain: "example.com"`, `path: "/pricing"`, and query
parameter arrays. `record_domain` proves a later block can use the result.

## Checkpoint

You are done when both health checks pass, the instance completes, the piece
output has the parsed URL fields, and the downstream log block executes.

Use the [Activepieces sidecar reference](../../../activepieces/README.md) for
credentials, polling triggers, error classification, and unsupported webhook
strategy triggers.

## If something failed

| Symptom | Likely cause | Fix |
|---|---|---|
| Sidecar returns piece not found | Package is not installed or allowlisted | Install the package and include its piece name in the allowlist |
| Engine says sidecar unreachable | Sidecar is stopped or URL is wrong | Check `/health`, then restart Orch8 with the correct `/execute` URL |
| `parse_url` is a permanent failure | URL is not absolute | Include a scheme such as `https://` |
| Credentialed action returns `401` upstream | Token is absent or expired | Resolve a current tenant-scoped credential into `params.auth` |

