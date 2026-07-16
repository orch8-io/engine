# Connect the dashboard and Prometheus metrics

Use the operator dashboard for interactive diagnosis and `/metrics` for
automated monitoring. Both management surfaces should use the same environment
identity and authenticated engine connection.

**Starting point:** Node.js 20 or newer, `pnpm`, and the Level 3 server. This
guide restarts the server with a local CORS origin and environment banner.

**Result:** the dashboard connects with an API key and tenant, while a
Prometheus-compatible scrape returns Orch8 counters and histograms.

## 1. Restart the engine for browser access

Stop the Level 3 server. Restore its API and encryption variables, then add:

```bash
export ORCH8_CORS_ORIGINS=http://localhost:5173
export ORCH8_ENV_LABEL=local-demo
export ORCH8_ENV_COLOR='#1d4ed8'
orch8-server --config orch8.toml
```

Do not use `*` with API-key authentication. Production CORS should name only
the dashboard origins that operators actually use.

## 2. Start the dashboard

In terminal A, from the repository root:

```bash
cd dashboard
pnpm install
pnpm dev
```

Open the URL printed by Vite, normally `http://localhost:5173`.

In **Settings**, enter:

```text
Engine API URL: http://localhost:8080
API key:        the value of ORCH8_API_KEY
Tenant ID:      demo
```

The current dashboard uses the server origin and its backward-compatible bare
management routes. CLI and new application integrations should continue using
the canonical `/api/v1` prefix.

Select **Test connection**. The local-demo environment banner should remain
visible on every operator page.

## 3. Create activity to observe

From a client terminal in the Level 3 project:

```bash
SEQUENCE_ID=$(orch8 --output json sequence lookup \
  demo default durable-welcome | jq -r '.id')
INSTANCE_ID=$(orch8 --output json instance create \
  --sequence-id "$SEQUENCE_ID" \
  --tenant-id demo \
  --context '{"data":{"customer":"Dashboard User"}}' | jq -r '.id')
orch8 instance get "$INSTANCE_ID" --watch
```

Use **Executions** to find the instance, then open its tree, timeline, logs,
context, and outputs. The UI is an operator client of the same API, not a
separate source of state.

## 4. Scrape metrics with authentication

`/metrics` is rooted at the server origin and is protected as a management
surface. Request it with both headers:

```bash
curl -fsS \
  -H "x-api-key: $ORCH8_API_KEY" \
  -H "x-tenant-id: demo" \
  http://127.0.0.1:8080/metrics > /tmp/orch8-metrics.txt

grep '^orch8_' /tmp/orch8-metrics.txt | sed -n '1,40p'
```

An unauthenticated scrape should fail:

```bash
curl -i http://127.0.0.1:8080/metrics
```

Configure Prometheus to send the same headers and scrape every 15 seconds. Do
not put the API key directly in a committed Prometheus file; inject it from the
deployment's secret mechanism.

## 5. Start with four operational signals

Use the provided [Grafana dashboard](../../grafana-dashboard.json) for the full
surface. At minimum, monitor:

- failed instance rate;
- scheduler tick duration;
- pending worker-task depth; and
- parked webhook count.

Correlate a metric change with the execution tree and logs before taking a
mutation such as retry, redelivery, or worker drain.

## Checkpoint

You are done when the dashboard test connects, the environment banner is
visible, the new instance appears, an authenticated metrics scrape succeeds,
and an unauthenticated scrape is rejected.

Use [Operator dashboard](../../DASHBOARD.md), [Metrics API](../../API.md#metrics),
and [Deployment observability](../../DEPLOYMENT.md#observability) for the full
contracts.

## If something failed

| Symptom | Likely cause | Fix |
|---|---|---|
| Browser reports a CORS error | Dashboard origin differs from configured origin | Copy the exact Vite origin and restart Orch8 |
| Dashboard returns `401` | API key in browser storage is absent or wrong | Re-enter the current Level 3 key in Settings |
| Dashboard data is empty | Tenant is not `demo` | Set the tenant and create one known instance |
| Metrics returns `401` | Scrape omitted management headers | Send API key and tenant headers |
