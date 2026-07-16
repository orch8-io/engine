# Topic quick starts

These guides solve focused integration and operations tasks outside the
six-level [progressive learning path](../README.md). They are independent: pick
the outcome you need instead of reading them in order.

## Choose an outcome

| Area | Time | Quick start | You finish with |
|---|---:|---|---|
| Scheduling | 15 min | [Schedule recurring runs](01-cron-schedules.md) | A timezone-aware cron schedule with overlap control |
| Inbound integration | 20 min | [Accept a signed inbound webhook](02-inbound-webhook-trigger.md) | A public HMAC-protected endpoint that starts workflows |
| Outbound integration | 25 min | [Deliver signed lifecycle webhooks](03-outbound-lifecycle-webhooks.md) | A receiver that verifies raw-body signatures and deduplicates retries |
| Human operations | 20 min | [Add a human approval gate](04-human-approval.md) | A waiting approval that resumes through a validated signal |
| Type safety | 15 min | [Generate typed dataflow bindings](05-typed-dataflow.md) | Deterministic TypeScript, Python, schema, and report files |
| Large payloads | 20 min | [Externalize large workflow state](06-large-payloads.md) | Compact hot rows with transparently hydrated outputs |
| Integrations | 20 min | [Run an Activepieces action](07-activepieces-action.md) | A community-piece action executed by the Node sidecar |
| Observability | 20 min | [Connect the dashboard and metrics](08-dashboard-and-metrics.md) | An authenticated operator UI and Prometheus scrape target |
| Performance | 20 min | [Generate disposable local load](09-load-testing.md) | Repeatable seeded traffic against an isolated database |

## Shared starting point

Unless a guide says otherwise, complete
[Level 3: Run the durable API server](../03-durable-api.md) first and keep that
server running. The topic guides assume these variables exist in every client
terminal:

```bash
export ORCH8_URL=http://127.0.0.1:8080/api/v1
export ORCH8_TENANT_ID=demo
export ORCH8_API_KEY=$(sed -n 's/^api_key = "\(.*\)"/\1/p' orch8.toml)
```

Direct API examples use both authentication headers:

```text
-H "x-api-key: $ORCH8_API_KEY" \
-H "x-tenant-id: $ORCH8_TENANT_ID"
```

Health probes, `/metrics`, and signed public webhook ingestion are rooted at
the server origin rather than under `/api/v1`. Health and inbound webhook
routes have their own public trust boundaries; `/metrics` still requires the
management API key and tenant headers.

## Boundaries

Each article contains only the shortest complete path to one outcome. Use the
linked reference after the checkpoint for every option, failure mode, or
production requirement. The live server contract remains authoritative at
`http://127.0.0.1:8080/swagger-ui`.
