# Operator dashboard

The dashboard is an operational decision interface embedded in the server build.
It groups surfaces by the action an operator is trying to take: monitor, build and
ship, manage runtime capacity, or administer credentials and integrations.

## Connect

Run the dashboard locally during frontend development:

```bash
cd dashboard
pnpm install
pnpm dev
```

By default it connects to `http://localhost:8080`. Open **Settings** to store a
different engine URL, API key, and tenant ID in browser local storage. The
tenant is sent as `X-Tenant-Id` on JSON API requests, so those requests work
when `ORCH8_REQUIRE_TENANT_HEADER` is enabled. Browser-native event streams
and artifact downloads must also satisfy the deployment's URL-auth policy. For a build-time
default, set `VITE_ORCH8_API_URL`.

The engine must allow the dashboard origin with `ORCH8_CORS_ORIGINS`. Example:

```bash
export ORCH8_CORS_ORIGINS=http://localhost:5173
orch8-server --config orch8.toml
```

## Current surfaces

| Group | Surface | Decision or action |
|---|---|---|
| Monitor | Overview | Is the engine healthy, and what needs attention? |
| Monitor | Executions | Which run is blocked or failed, and what happened? |
| Monitor | Approvals | Which human decisions are waiting? |
| Monitor | Operations | Retry DLQ work, recover webhooks, reset breakers, or drain nodes. |
| Build & ship | Sequences | Inspect versions and start schema-driven runs. |
| Build & ship | Releases | Diff, validate, canary, evaluate, promote, pause, or roll back. |
| Build & ship | Cron / Triggers / Sessions | Manage invocation and coordination. |
| Runtime | Tasks / Workers / Queues / Pools | Diagnose and control dispatch capacity. |
| Runtime | Mobile / Usage | Inspect device sync and resource usage. |
| Administration | Plugins / Credentials / API keys | Manage integration and access material. |

An execution detail page includes the execution tree, unified timeline, artifacts,
audit entries, context patching, checkpoints, resume-from-block, and fork preview.
Potentially destructive actions ask for confirmation and keep their scope visible.

## Environment identity

Set an environment label so operators do not confuse production with staging:

```bash
export ORCH8_ENV_LABEL=production
export ORCH8_ENV_COLOR='#b91c1c'
```

The label appears as a persistent banner. Choose a color with at least 4.5:1
contrast against white text.

## Verify dashboard changes

```bash
cd dashboard
pnpm typecheck
pnpm test
pnpm lint
pnpm build
```

The interface supports narrow screens by moving navigation above the content and
reducing page gutters. Tables remain horizontally scrollable instead of dropping
columns or changing their meaning.
