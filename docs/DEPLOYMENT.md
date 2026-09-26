# Deployment

> **Stability: stable**, covered by the [1.0 stability contract](../STABILITY.md). Note: Docker and plain Kubernetes manifests are stable; the Helm chart and one-click templates are **beta**.

The engine ships as a single binary (`orch8-server`) or a container image (`ghcr.io/orch8-io/engine`). You need Postgres (or SQLite for single-node deployments) and a way to run the process.

This page is the index. Each cloud target has its own section below with a copy-pasteable starting point.

---

## Which backend?

| Scenario | Backend | Notes |
|---|---|---|
| Local dev, tests, demos | SQLite | Zero dependencies, single file on disk |
| Single-node production | SQLite + Litestream | One engine process, no scale-out; see [SQLite in production](SQLITE_PRODUCTION.md) |
| HA / multi-replica production | Postgres | Multiple engine replicas coordinate via `FOR UPDATE SKIP LOCKED` |

SQLite is a supported backend for small single-node deployments. Pair it with Litestream for off-host durability ([guide](SQLITE_PRODUCTION.md)). Once you need more than one engine replica, split node roles, or zero-downtime deploys, move to Postgres.

> **Postgres migrations are opt-in.** `ORCH8_RUN_MIGRATIONS` defaults to `false`. On a fresh database, set it to `true` or run `orch8 migrate --database-url ...` (for example as a pre-deploy job) before starting the server. Without that the server boots, `/health/ready` returns 503, and API calls fail with `relation "..." does not exist`. SQLite creates and reconciles its schema at boot.

Upgrades are safe on both backends: Postgres migrations are checksum-verified and never edited in place (CI-enforced), and a file-backed SQLite database created by an older binary is migrated forward at boot with versioned schema deltas.

---

## High availability

The engine is stateless. Run N replicas pointing at the same Postgres — `FOR UPDATE SKIP LOCKED` guarantees no double-claiming. No leader election, no peer discovery, no sidecar coordinator.

> **Warning — triggers are not lease-gated.** Instance *claiming* is safe at
> any replica count, but the trigger processor is not: every node runs the
> trigger loop and spawns its own NATS subscription / file watcher for each
> enabled trigger, so one event fans out into **one workflow instance per
> node** (duplicate executions). The other background loops (cron, GC,
> stale-instance reaper) also run on every node but coordinate through the
> database, so they stay single-fire. Until trigger lease-gating ships: if you
> use NATS or file-watch triggers, run a **single engine node**. Workloads
> without those triggers can scale API serving + execution to 2+ replicas
> behind a load balancer as described below.

Recommended:

- **2+ replicas** for availability (single node if you use NATS/file triggers — see the warning above).
- **PodDisruptionBudget** (or equivalent) of `maxUnavailable: 1`.
- **`[engine].shutdown_grace_period_secs = 30`** (the default) and an orchestrator
  termination grace period longer than that — lets in-flight steps drain before SIGKILL.
- **Readiness gate** on `GET /health/ready` (returns 200 only when the DB is reachable **and** the engine tick loop is alive; 503 otherwise, so a pod whose engine died is pulled from rotation instead of accepting work it will never run).
- **Rolling updates** are safe — the stale-instance reaper recovers any instance held by a pod that exits ungracefully. Healthy pods heartbeat their claimed instances, so the reaper never yanks a long-running step from a live node.

---

## Docker

The container image defaults to SQLite. The server is secure by default: it refuses to start without both `ORCH8_API_KEY` and `ORCH8_ENCRYPTION_KEY` set (pass `--insecure` explicitly for a local throwaway container). For real deployments, override:

```bash
docker run --rm -p 8080:8080 -p 50051:50051 \
  -e ORCH8_STORAGE_BACKEND=postgres \
  -e ORCH8_DATABASE_URL=postgres://orch8:secret@db:5432/orch8?sslmode=require \
  -e ORCH8_RUN_MIGRATIONS=true \
  -e ORCH8_API_KEY=$ORCH8_API_KEY \
  -e ORCH8_ENCRYPTION_KEY=$ORCH8_ENCRYPTION_KEY \
  -e ORCH8_LOG_JSON=true \
  ghcr.io/orch8-io/engine:latest
```

The included `docker-compose.yml` runs both Postgres and the engine. Export
`ORCH8_API_KEY` and `ORCH8_ENCRYPTION_KEY`, then run `docker compose up`.

---

## Kubernetes

Committed manifests live in `deploy/kubernetes/`; copy
`secret.example.yaml`, replace every placeholder, and apply the directory.

A minimal `Deployment + Service + Secret` pattern:

```yaml
apiVersion: v1
kind: Secret
metadata:
  name: orch8-secrets
type: Opaque
stringData:
  ORCH8_DATABASE_URL: postgres://orch8:secret@postgres.svc:5432/orch8?sslmode=require
  ORCH8_API_KEY: "<generate-a-random-string>"
  ORCH8_ENCRYPTION_KEY: "<64 hex chars from openssl rand -hex 32>"
---
apiVersion: apps/v1
kind: Deployment
metadata:
  name: orch8-engine
spec:
  replicas: 2
  strategy:
    type: RollingUpdate
    rollingUpdate:
      maxUnavailable: 1
  selector: { matchLabels: { app: orch8-engine } }
  template:
    metadata: { labels: { app: orch8-engine } }
    spec:
      terminationGracePeriodSeconds: 45
      containers:
        - name: orch8
          image: ghcr.io/orch8-io/engine:latest
          ports:
            - { containerPort: 8080, name: http }
            - { containerPort: 50051, name: grpc }
          env:
            - { name: ORCH8_STORAGE_BACKEND, value: postgres }
            - { name: ORCH8_RUN_MIGRATIONS, value: "true" }
            - { name: ORCH8_LOG_JSON,       value: "true" }
            - { name: ORCH8_HTTP_ADDR,      value: 0.0.0.0:8080 }
          envFrom:
            - secretRef: { name: orch8-secrets }
          readinessProbe:
            httpGet: { path: /health/ready, port: 8080 }
            periodSeconds: 5
          livenessProbe:
            httpGet: { path: /health/live, port: 8080 }
            periodSeconds: 10
          resources:
            requests: { cpu: "200m", memory: "256Mi" }
            limits:   { cpu: "2",    memory: "1Gi" }
---
apiVersion: v1
kind: Service
metadata: { name: orch8-engine }
spec:
  selector: { app: orch8-engine }
  ports:
    - { name: http, port: 8080,  targetPort: 8080 }
    - { name: grpc, port: 50051, targetPort: 50051 }
```

### Helm chart

The chart is in this repository at [`deploy/helm/orch8`](../deploy/helm/orch8/README.md).
It isn't published to a Helm repository or Artifact Hub yet, so install it from a checkout:

```bash
helm dependency build deploy/helm/orch8
helm install orch8 deploy/helm/orch8 \
  --set externalDatabase.url='postgres://orch8:secret@db:5432/orch8?sslmode=require'
```

What the chart does:

- **Node roles.** `mode: allInOne` runs one Deployment with the `all_in_one` role.
  `mode: split` runs a `control` Deployment and an `executor` Deployment, each with its
  own replicas, HPA, and PDB. The optional `gateway` Deployment is for the continuity gateway
  role. It needs gRPC mTLS material and binds its HTTP listener to loopback only, so it uses
  exec probes. The chart doesn't offer the `edge` role.
- **Secrets.** Pass `secrets.existingSecret`, or let the chart generate the API key and
  the 64-hex encryption key once. Generated keys are reused on upgrade and the chart
  keeps the Secret on uninstall.
- **Database.** Use `externalDatabase.url` or `externalDatabase.existingSecret`, or turn on
  the bundled Bitnami `postgresql` subchart for evaluation with `postgresql.enabled=true`.
  SQLite (`storage.backend=sqlite`) is allowed only with one all-in-one replica on a PVC.
- **Migrations.** `migrations.mode: job` (the default) runs `orch8 migrate` as a Helm
  pre-install/pre-upgrade hook. `server` sets `ORCH8_RUN_MIGRATIONS=true` on the pods, and
  `none` leaves migrations to you.
- **Pods.** Readiness and liveness probes use `/health/ready` and `/health/live`. Pods run
  as non-root uid 999 with a read-only root filesystem and all capabilities dropped.
- **Extras.** Ingress, HPA, and PDB are available. So are a PrometheusRule (the same rules
  as [`prometheus-alerts.yml`](prometheus-alerts.yml)) and a ServiceMonitor.
- **Metrics caveat.** `/metrics` requires the `x-api-key` header, and a prometheus-operator
  ServiceMonitor can't send custom headers. The chart README describes the
  `metrics.scrapeConfigSecret` alternative.

`scripts/helm-test.sh` lints and renders every `deploy/helm/orch8/ci/*-values.yaml` case.

---

## AWS

**Compute:** ECS Fargate is the path of least resistance. EKS if you're already running Kubernetes.

**Database:** RDS Postgres 15 or Aurora Postgres. Enable automated backups and Multi-AZ.

**Secrets:** Store `ORCH8_DATABASE_URL`, `ORCH8_API_KEY`, and `ORCH8_ENCRYPTION_KEY` in Secrets Manager and inject as environment variables via the task definition.

**Ingress:** ALB with HTTPS listener, target group pointing at port 8080 with `/health/ready` as the health check. If you need gRPC, use NLB on port 50051 — ALB doesn't handle gRPC cleanly.

**IAM:** Task role needs `secretsmanager:GetSecretValue` on the specific secret ARN. That's it — the engine does not call any AWS APIs directly.

---

## Google Cloud

**Compute:** Cloud Run for burst-y workloads, GKE for sustained throughput.

**Database:** Cloud SQL for Postgres. Use the Cloud SQL Auth Proxy as a sidecar (or Private IP if you're in a VPC).

**Secrets:** Secret Manager, mounted as env vars.

**Ingress:** HTTPS Load Balancer. Cloud Run handles TLS termination natively.

**Scheduler tick caveat:** Cloud Run idle-scales to zero by default. That breaks the scheduler loop — set `--min-instances=1` (or 2 for HA) so at least one instance is always running to drive the tick loop.

---

## One-click and PaaS templates

Every template below meets the requirements for a secure container: a storage backend,
`ORCH8_API_KEY`, and `ORCH8_ENCRYPTION_KEY`, which must be 64 hex characters from
`openssl rand -hex 32`. Postgres templates also set `ORCH8_RUN_MIGRATIONS=true` so that a
fresh database gets its schema. Back up the encryption key outside the platform, because data
encrypted with it can't be read without it. All templates run **one** engine instance. See
[High availability](#high-availability) before you scale out.

| Platform | File | Storage | Button / command |
|---|---|---|---|
| Render | [`render.yaml`](../render.yaml) | Render Postgres | [![Deploy to Render](https://render.com/images/deploy-to-render-button.svg)](https://render.com/deploy?repo=https://github.com/orch8-io/engine) |
| DigitalOcean App Platform | [`.do/deploy.template.yaml`](../.do/deploy.template.yaml) | DO dev database (PG) | [![Deploy to DO](https://www.deploytodo.com/do-btn-blue.svg)](https://cloud.digitalocean.com/apps/new?repo=https://github.com/orch8-io/engine/tree/main) |
| Railway | [`deploy/railway/`](../deploy/railway/) | Railway Postgres | Template not published yet (see below) |
| Fly.io | [`fly.toml`](../fly.toml) | SQLite on a Fly volume | `fly launch --copy-config` (no button) |
| Coolify | [`deploy/coolify/docker-compose.yml`](../deploy/coolify/docker-compose.yml) | Postgres container | Paste as a Docker Compose resource |

The Render and DigitalOcean buttons read the template from the default branch of the
public repository. A template added on a feature branch only takes effect after it merges.

### Render

`render.yaml` creates a `basic-256mb` Render Postgres and a web service from
`ghcr.io/orch8-io/engine:latest`. Render generates `ORCH8_API_KEY`. `ORCH8_ENCRYPTION_KEY`
is `sync: false`, so Render asks for it during setup: paste the output of
`openssl rand -hex 32`. The health check is `/health/ready`.

### DigitalOcean App Platform

`.do/deploy.template.yaml` pulls the image from GHCR, attaches a dev PostgreSQL database
through `${db.DATABASE_URL}`, and declares `ORCH8_API_KEY` and `ORCH8_ENCRYPTION_KEY` as
empty `SECRET` values. Fill both in on the review screen, or the server exits at startup.
For production, attach a managed database cluster instead of the dev database. If the
first deploy logs `permission denied for schema public`, the database user can't create
tables. Grant it rights on the schema, or use a database the user owns.

### Railway

A Railway template is created in the Railway dashboard, not from a file, so there's no
button until a maintainer publishes one. The repository side is ready:
`deploy/railway/railway.json` builds `deploy/railway/Dockerfile`, which is just
`FROM ghcr.io/orch8-io/engine`, and sets the `/health/ready` health check. To create the template:

1. New project, then add a **PostgreSQL** database.
2. Add a service from the GitHub repo. In **Settings → Config-as-code**, set the path to
   `deploy/railway/railway.json`.
3. Variables:
   `ORCH8_STORAGE_BACKEND=postgres`, `ORCH8_DATABASE_URL=${{Postgres.DATABASE_URL}}`,
   `ORCH8_RUN_MIGRATIONS=true`, `ORCH8_HTTP_ADDR=0.0.0.0:8080`, `ORCH8_REQUIRE_TENANT_HEADER=true`,
   `ORCH8_API_KEY=<random>`, `ORCH8_ENCRYPTION_KEY=<64 hex>`. In a published template, the
   template variable functions can generate these values. Check in the template editor
   that the encryption key comes out as exactly 64 hex characters.
4. Networking: generate a domain that targets port **8080**.

### Fly.io

Single-region SQLite on a volume is the quickest way to run on Fly. Fly has no deploy
button. From a checkout:

```bash
fly launch --copy-config --no-deploy          # reuses fly.toml; pick your own app name
fly volumes create orch8_data --size 1
fly secrets set ORCH8_API_KEY=$(openssl rand -hex 32) ORCH8_ENCRYPTION_KEY=$(openssl rand -hex 32)
fly deploy
```

`fly.toml` keeps one machine running (`auto_stop_machines = false`, because the scheduler
must stay up). A Fly volume lives on one host. Add Litestream as described in
[SQLite in production](SQLITE_PRODUCTION.md) for off-host copies. If you need more than
one machine, switch to Postgres. Several machines can't share one SQLite volume.

### Coolify

Create a **Docker Compose** resource and paste `deploy/coolify/docker-compose.yml`.
Coolify fills `SERVICE_FQDN_ENGINE_8080`, `SERVICE_USER_POSTGRES`,
`SERVICE_PASSWORD_POSTGRES`, and `SERVICE_PASSWORD_64_APIKEY` (used as the API key) itself.
Set `ORCH8_ENCRYPTION_KEY` in the resource's environment. The compose file refuses to
start without it. The same file was smoke-tested with plain `docker compose` using
substituted values: Postgres came up healthy, migrations ran, and the engine turned ready.

---

## Production checklist

Before opening traffic:

- [ ] `ORCH8_API_KEY` is set to a random 32+ char value.
- [ ] `ORCH8_ENCRYPTION_KEY` is set (64 hex chars) and backed up in a separate secret store.
- [ ] `ORCH8_CORS_ORIGINS` is restricted to the domains that need it — the server refuses to start with `*` while API-key auth is on.
- [ ] `ORCH8_REQUIRE_TENANT_HEADER=true` (the default). Only disable for single-tenant deployments, which additionally requires `ORCH8_ALLOW_NO_TENANT_ISOLATION=1`.
- [ ] Postgres SSL is enforced (`sslmode=require` in the DSN).
- [ ] Postgres automated backups are on. **Test the restore.**
- [ ] `/metrics` is scraped by your Prometheus — it sits behind API-key auth, so configure the scrape job to send the `x-api-key` header (and `x-tenant-id` when tenant enforcement is on).
- [ ] Log level is `info` or `warn`, format is `json`.
- [ ] Webhook subscribers handle dedup on `instance_id + event_type` (see [WEBHOOKS.md](WEBHOOKS.md)).
- [ ] Replicas ≥ 2 behind a load balancer with health checks — unless you use NATS/file triggers (see the warning in [High availability](#high-availability)).
- [ ] Retention is configured — set `instance_retention_secs` (e.g. 30–90 days) so terminal instances are swept; `0` (the default) keeps them forever. Trade-off: the sweep deletes queryable instance history (`orch8 instance get`, outputs, execution tree), so pick a window your debugging/audit needs can live with.
- [ ] Table growth is monitored — track `pg_total_relation_size` on `task_instances`, `block_outputs`, `audit_log`, and `step_logs`; they grow with every execution and dominate storage.
- [ ] PostgreSQL migration 074 has been applied — it proactively creates 16 fixed hash partitions for `block_outputs` (by instance) and `audit_log` (by tenant), avoiding a high-risk conversion after those tables reach millions of rows. Monitor partition skew; fixed hash partitions require no monthly maintenance.
- [ ] Backlog/lag alerts — alert when `orch8_queue_depth` sits at the `batch_size` ceiling for several minutes (work is arriving faster than it is claimed) and on `orch8_recovery_stale_instances_total` increments (a node died holding work).
- [ ] Graceful shutdown verified — send SIGTERM, in-flight steps complete, no orphaned tasks.

---

## Observability

Minimum viable:

- **Metrics:** scrape `/metrics` every 15s with the `x-api-key` header set (see [API.md — Metrics](API.md#metrics)).
- **Logs:** `ORCH8_LOG_JSON=true`, ship to your log aggregator.
- **Dashboard:** import [`docs/grafana-dashboard.json`](grafana-dashboard.json) into Grafana.
- **Alerts:** at minimum, alert on `orch8_instances_failed_total` rate and `orch8_tick_duration_seconds` p99.

---

## See also

- [Configuration](CONFIGURATION.md) — every env var and TOML field
- [Architecture](ARCHITECTURE.md) — why `SKIP LOCKED` makes multi-replica safe
- [Webhooks](WEBHOOKS.md) — terminal-state event wiring
