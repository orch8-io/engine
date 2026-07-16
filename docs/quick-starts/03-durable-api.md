# Level 3: Run the durable API server

Local development mode is intentionally ephemeral. This level starts the real
server with SQLite, authentication, encrypted storage, and canonical HTTP API
routes. You will publish a sequence, create an instance, inspect its outputs,
and restart the server without losing the stored records.

**Starting point:** the `orch8` and `orch8-server` binaries are in `PATH`.

**Result:** a secured local engine stores workflows and instances in
`orch8.db`, and the CLI operates it through `/api/v1`.

## 1. Scaffold a server project

From `quickstart-work`:

```bash
mkdir -p level-3
orch8 init level-3
cd level-3
```

The scaffold contains:

```text
orch8.toml          server configuration; SQLite by default
sequence.json       example authoring definition
docker-compose.yml  PostgreSQL and engine services for later use
```

Replace `sequence.json` with a definition that uses only built-in handlers:

```json
{
  "id": "019a0000-0000-7000-8000-000000000003",
  "tenant_id": "demo",
  "namespace": "default",
  "name": "durable-welcome",
  "version": 1,
  "input_schema": {
    "type": "object",
    "required": ["customer"],
    "properties": {
      "customer": { "type": "string", "minLength": 1 }
    },
    "additionalProperties": false
  },
  "blocks": [
    {
      "type": "step",
      "id": "remember_customer",
      "handler": "set_state",
      "params": {
        "key": "customer",
        "value": "{{data.customer}}"
      }
    },
    {
      "type": "step",
      "id": "wait",
      "handler": "noop",
      "delay": { "duration": 2000 }
    },
    {
      "type": "step",
      "id": "read_customer",
      "handler": "get_state",
      "params": { "key": "customer" }
    },
    {
      "type": "step",
      "id": "complete",
      "handler": "log",
      "params": {
        "message": "Durable run completed for {{outputs.read_customer.value}}"
      }
    }
  ],
  "created_at": "2026-01-01T00:00:00Z"
}
```

The file includes placeholder identity fields so draft preflight can parse the
complete sequence contract. `sequence apply` replaces the ID, version, and
timestamp with the new immutable server version.

## 2. Export the local credentials

`orch8 init` generated an API key in `orch8.toml`. Read it into the shell
without printing it:

```bash
export ORCH8_API_KEY=$(sed -n 's/^api_key = "\(.*\)"/\1/p' orch8.toml)
export ORCH8_TENANT_ID=demo
export ORCH8_URL=http://127.0.0.1:8080/api/v1
export ORCH8_ENCRYPTION_KEY=$(openssl rand -hex 32)
```

The API key protects requests. The 32-byte hex encryption key protects stored
context and credentials. Keep the same encryption key across server restarts
for this level. In production, both belong in a secrets manager.

## 3. Start the server

In terminal A, from `quickstart-work/level-3`:

```bash
orch8-server --config orch8.toml
```

Migrations run automatically because the scaffold enables them. Wait for the
server to listen on port 8080.

In terminal B, return to the same directory and restore the four exported
variables if it is a new shell. Check readiness; health probes intentionally do
not require authentication:

```bash
curl -fsS http://127.0.0.1:8080/health/ready | jq
```

## 4. Preflight and publish the sequence

Preflight checks the local definition before it becomes an immutable stored
version:

```bash
orch8 sequence preflight --file sequence.json
```

Apply it:

```bash
orch8 sequence apply sequence.json
```

`apply` is GitOps-friendly. It looks up the latest version by tenant,
namespace, and name. If content is unchanged it does nothing; if behavior
changed it publishes the next immutable version.

Look up the stored version and capture its server-assigned ID:

```bash
SEQUENCE_ID=$(orch8 --output json sequence lookup \
  demo default durable-welcome | jq -r '.id')
printf '%s\n' "$SEQUENCE_ID"
```

## 5. Create and watch an instance

Create one instance and capture its ID:

```bash
INSTANCE_ID=$(orch8 --output json instance create \
  --sequence-id "$SEQUENCE_ID" \
  --tenant-id demo \
  --context '{"data":{"customer":"Margaret"}}' | jq -r '.id')
printf '%s\n' "$INSTANCE_ID"
```

The CLI's `--context` value is the complete execution context for the HTTP
API, so the per-run input is nested under `data`. This differs from `orch8 dev
--context`, which accepts the data object directly as a local convenience.

Watch until the state is terminal:

```bash
orch8 instance get "$INSTANCE_ID" --watch
```

Then inspect the execution tree and outputs:

```bash
orch8 instance tree "$INSTANCE_ID"
orch8 instance outputs "$INSTANCE_ID"
```

The instance record, block progress, state value, and outputs now live in
SQLite rather than process memory.

## 6. Make the same request with curl

The CLI is a client of the same API available to every language. Fetch the
instance directly:

```bash
curl -fsS \
  -H "x-api-key: $ORCH8_API_KEY" \
  -H "x-tenant-id: demo" \
  "$ORCH8_URL/instances/$INSTANCE_ID" | jq '{id, state, sequence_id}'
```

Explore the complete live contract at `http://127.0.0.1:8080/swagger-ui`.
Swagger is authoritative for request and response fields; these tutorials
select only the fields needed for their outcome.

## 7. Prove the data is durable

Stop terminal A with `Ctrl-C`. Confirm `orch8.db` exists:

```bash
ls -lh orch8.db
```

Restart the server in terminal A with the same environment, especially the
same `ORCH8_ENCRYPTION_KEY`:

```bash
orch8-server --config orch8.toml
```

Fetch the old instance again in terminal B:

```bash
orch8 instance get "$INSTANCE_ID"
orch8 instance outputs "$INSTANCE_ID"
```

The record is still present. This is the boundary between a workflow evaluator
and a durable orchestration service.

## 8. Check authentication explicitly

An unauthenticated product request should fail:

```bash
curl -i "$ORCH8_URL/instances/$INSTANCE_ID"
```

The same request with both headers succeeds:

```bash
curl -fsS \
  -H "x-api-key: $ORCH8_API_KEY" \
  -H "x-tenant-id: demo" \
  "$ORCH8_URL/instances/$INSTANCE_ID" | jq '.state'
```

Do not use `--insecure` as a shortcut for application development. It is
appropriate only for an intentionally throwaway process with no protected
data.

## Checkpoint

You are ready for Level 4 when:

- Readiness succeeds, while an unauthenticated product request fails.
- `sequence preflight`, `sequence apply`, and `sequence lookup` succeed.
- The instance reaches `completed` and its outputs remain available after a
  server restart.
- You understand why server-mode context is `{"data": {...}}`, while local
  `orch8 dev --context` accepts the inner data object.

Keep the server running for the next level.

Next: [Execute application code in a worker](04-external-worker.md).

## If something failed

| Symptom | Likely cause | Fix |
|---|---|---|
| Server refuses to start without an encryption key | The variable is absent in terminal A | Export a 64-character hex key before starting |
| CLI returns `401` | `ORCH8_API_KEY` is missing or does not match `orch8.toml` | Re-run the `sed` export in the CLI terminal |
| CLI returns a tenant error | `ORCH8_TENANT_ID` is absent or differs from `demo` | Export it and retry |
| `jq` captures `null` | A preceding command returned an error object | Run that command without command substitution and inspect the full response |
| Old encrypted context cannot be read after restart | A new encryption key was generated | Restart with the original key used to write the database |
| Port 8080 is occupied | Another server is running | Stop it or change `[api].http_addr` and `ORCH8_URL` together |
