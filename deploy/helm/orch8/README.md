# Orch8 Helm chart

Deploys the [Orch8](https://orch8.io) durable workflow engine
(`ghcr.io/orch8-io/engine`) on Kubernetes.

> **License.** The engine is distributed under the Business Source License 1.1
> (BUSL-1.1), not an OSI open-source license. Read [`LICENSE`](../../../LICENSE)
> before offering Orch8 to third parties. The chart templates carry the same
> license as the repository.

This chart lives in the engine repository and is not yet published to a Helm
repository or Artifact Hub. Install it from a checkout:

```bash
helm dependency build deploy/helm/orch8   # fetches the optional postgresql subchart
helm install orch8 deploy/helm/orch8 \
  --namespace orch8 --create-namespace \
  --set externalDatabase.url='postgres://orch8:secret@db:5432/orch8?sslmode=require'
```

`helm dependency build` is required even when `postgresql.enabled=false`:
Helm refuses to render a chart whose declared dependencies are missing from
`charts/`.

## Topologies

| `mode` | Deployments | Notes |
|---|---|---|
| `allInOne` (default) | `<release>-all-in-one` running role `all_in_one` | API, metrics, public webhooks, engine, push outbox in one process. |
| `split` | `<release>-control` (role `control`) + `<release>-executor` (role `executor`) | Control serves the full API and metrics without an engine; executors run the engine and expose only health over HTTP plus the worker-lifecycle gRPC surface. Scale them independently. |

`gateway.enabled=true` adds a `<release>-gateway` Deployment (role `gateway`)
for continuity capsule/handoff traffic. The server refuses to start a gateway
unless the root API key, mandatory tenant headers, the encryption key and full
gRPC mTLS material (server cert, key, client CA) are configured and the HTTP
listener is loopback-only. The chart therefore binds its HTTP listener to
`127.0.0.1:8080`, probes it with `orch8 --url http://127.0.0.1:8080 health`
exec probes, never routes it through the Ingress, and publishes only gRPC.
Put the PEM files in a Secret and set `gateway.tls.existingSecret`.

The `edge` role is not offered: it is meant for device and edge hosts that run
outside the cluster and connect to a managed control plane. See
[docs/NODE_ROLES.md](../../../docs/NODE_ROLES.md).

## Secrets

The server refuses to start without an API key and a 64-hex-character
encryption key. Either:

- set `secrets.existingSecret` (keys named by `secrets.apiKeyKey` and
  `secrets.encryptionKeyKey`), or
- let the chart manage a Secret. Explicit `secrets.apiKey` /
  `secrets.encryptionKey` values are used when set; otherwise values are
  generated on first install and reused on upgrade via `lookup`.

The chart-managed Secret is annotated `helm.sh/resource-policy: keep`, so
`helm uninstall` leaves it (and the encryption key) in place. Reinstalling
under the same release name then fails because Helm will not adopt an
unowned resource: either delete the Secret deliberately, adopt it with the
`meta.helm.sh/release-name`/`release-namespace` annotations and
`app.kubernetes.io/managed-by=Helm` label, or point `secrets.existingSecret`
at it. **Back up the encryption key**: encrypted context, credentials and
artifacts are unreadable without it. `helm template` (no cluster access)
generates fresh random values on every run; do not apply its output over an
existing install.

## Database

| Setting | Behaviour |
|---|---|
| `externalDatabase.url` | Stored in the chart Secret as `database-url`. |
| `externalDatabase.existingSecret` / `existingSecretKey` | Read from your Secret. Recommended. |
| `postgresql.enabled=true` | Bitnami `postgresql` subchart; URL assembled from its generated password. For evaluation only; Bitnami has changed image distribution terms, so check that the images it references are pullable in your environment. |
| `storage.backend=sqlite` | Single all-in-one replica on a PVC (`sqlite.persistence`). Rendering fails for `mode=split`, more than one replica, autoscaling, or a gateway. No PDB is created for the single pod. |

## Migrations (Postgres)

The server does not migrate Postgres by default (`run_migrations` defaults to
`false`). `migrations.mode` chooses who does:

- `job` (default): a Helm hook Job runs `orch8 migrate` (`pre-install,pre-upgrade`;
  `post-install,pre-upgrade` with the bundled subchart, whose database does not
  exist before install). An inline URL is copied into a short-lived hook Secret
  because the chart Secret does not exist yet when pre-install hooks run.
- `server`: every engine pod sets `ORCH8_RUN_MIGRATIONS=true`.
- `none`: run `orch8 migrate --database-url <url>` yourself before rolling out.

SQLite databases are migrated by the server at boot; no Job is rendered.

## Metrics and alerts

`/metrics` is served only by `all_in_one` and `control` pods and requires the
`x-api-key` header (no tenant header).

- `metrics.serviceMonitor.enabled` renders a prometheus-operator
  ServiceMonitor. **Caveat:** a ServiceMonitor cannot send a custom
  `x-api-key` header, so scrapes return 401 unless a proxy in front of the pods
  injects the header. It is disabled by default for that reason.
- `metrics.scrapeConfigSecret.enabled` renders a Secret containing a raw
  Prometheus scrape config that sends `x-api-key` via the `http_headers`
  option, reading the key from `/etc/prometheus/secrets/<engine-secret>/<key>`.
  Use it with `additionalScrapeConfigs` and list the engine Secret in the
  Prometheus `spec.secrets`. It requires a Prometheus version that supports
  `http_headers`.
- `metrics.prometheusRule.enabled` renders the alerts from
  [docs/prometheus-alerts.yml](../../../docs/prometheus-alerts.yml) with the
  `job` label set to `metrics.jobName` (default: the API Service name, which is
  also the ServiceMonitor job label).

## Security defaults

Pods run as uid/gid 999 (the image's `orch8` user) with `runAsNonRoot`,
`RuntimeDefault` seccomp, a read-only root filesystem, no privilege
escalation and all capabilities dropped. `/tmp` is an `emptyDir`; SQLite data
is on `/data`. The ServiceAccount token is not mounted.
`terminationGracePeriodSeconds` defaults to 45 so the engine's 30-second
shutdown grace can drain in-flight steps.

## High availability caveat

Instance claiming is safe at any replica count, but NATS and file-watch
triggers are not lease-gated: every engine node subscribes, so one event
starts one instance per node. If you use those triggers, run a single engine
replica. See [docs/DEPLOYMENT.md](../../../docs/DEPLOYMENT.md#high-availability).

## Values

| Key | Default | Description |
|---|---|---|
| `image.repository` / `image.tag` | `ghcr.io/orch8-io/engine` / appVersion | Engine image. |
| `mode` | `allInOne` | `allInOne` or `split`. |
| `allInOne.*`, `control.*`, `executor.*` | see values.yaml | `replicas`, `resources`, `autoscaling` (HPA, CPU), `pdb`, scheduling, `podAnnotations`, `extraEnv`. |
| `gateway.enabled` | `false` | Continuity gateway Deployment; needs `gateway.tls.existingSecret`. |
| `config.logLevel` / `config.logJson` | `info` / `true` | `ORCH8_LOG_LEVEL` / `ORCH8_LOG_JSON`. |
| `config.requireTenantHeader` | `true` | `ORCH8_REQUIRE_TENANT_HEADER`. Disabling also needs `ORCH8_ALLOW_NO_TENANT_ISOLATION=1` in `extraEnv`. |
| `config.corsOrigins` | `""` | `ORCH8_CORS_ORIGINS`; `*` is rejected (the server refuses it with auth on). |
| `config.toml` | `""` | Optional `orch8.toml`, mounted and passed with `--config`. Env vars still win. |
| `extraEnv` | `[]` | Extra env for every engine container. |
| `storage.backend` | `postgres` | `postgres` or `sqlite`. |
| `externalDatabase.*` | | See Database. |
| `postgresql.enabled` | `false` | Bundled Bitnami subchart. |
| `sqlite.persistence.*` | `5Gi`, RWO | PVC for SQLite (`existingClaim` supported). |
| `secrets.*` | generated | See Secrets. |
| `migrations.mode` | `job` | `job`, `server`, `none`. |
| `service.type` / `httpPort` / `grpcPort` | `ClusterIP` / 8080 / 50051 | Services per component. |
| `ingress.*` | disabled | Routes to the API Service (all-in-one or control). |
| `metrics.*` | disabled | See Metrics and alerts. |
| `serviceAccount.*` | created, no token | |
| `podSecurityContext`, `securityContext` | hardened | See Security defaults. |
| `terminationGracePeriodSeconds` | `45` | Must exceed 30 (schema-enforced minimum 31). |
| `probes.*` | | Startup/readiness/liveness timings. Readiness uses `/health/ready` (DB reachable and engine loop alive); liveness uses `/health/live`. |

## Testing

```bash
scripts/helm-test.sh
```

Runs `helm lint --strict` and `helm template` for every `ci/*-values.yaml`
(chart-testing layout), checks rendered content, and asserts that invalid
combinations (SQLite with two replicas, unknown mode, malformed encryption
key, gateway without TLS, wildcard CORS, ...) are rejected by the template
guards or `values.schema.json`.
