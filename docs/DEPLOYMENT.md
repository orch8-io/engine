# Deployment

The engine ships as a single binary (`orch8-server`) or a container image (`ghcr.io/orch8-io/engine`). You need Postgres (or SQLite for single-node deployments) and a way to run the process.

This page is the index. Each cloud target has its own section below with a copy-pasteable starting point.

---

## Which backend?

| Scenario | Backend | Notes |
|---|---|---|
| Local dev, tests, demos | SQLite | Zero dependencies, single file on disk |
| Single-node production | SQLite | Fine up to ~hundreds of instances/sec — beyond that, switch |
| HA / multi-replica production | Postgres | Multiple engine replicas coordinate via `FOR UPDATE SKIP LOCKED` |

SQLite is a first-class backend for small deployments. Once you need more than one engine replica **or** want off-host durability, move to Postgres.

---

## High availability

The engine is stateless. Run N replicas pointing at the same Postgres — `FOR UPDATE SKIP LOCKED` guarantees no double-claiming. No leader election, no peer discovery, no sidecar coordinator.

Recommended:

- **2+ replicas** for availability.
- **PodDisruptionBudget** (or equivalent) of `maxUnavailable: 1`.
- **`ORCH8_SHUTDOWN_GRACE_PERIOD_SECS=30`** — lets in-flight steps drain before SIGKILL.
- **Readiness gate** on `GET /health/ready` (returns 200 only when the DB is reachable).
- **Rolling updates** are safe — the stale-instance reaper recovers any instance held by a pod that exits ungracefully.

---

## Docker

The container image defaults to SQLite. For real deployments, override:

```bash
docker run --rm -p 8080:8080 -p 50051:50051 \
  -e ORCH8_STORAGE_BACKEND=postgres \
  -e ORCH8_DATABASE_URL=postgres://orch8:secret@db:5432/orch8?sslmode=require \
  -e ORCH8_API_KEY=$ORCH8_API_KEY \
  -e ORCH8_ENCRYPTION_KEY=$ORCH8_ENCRYPTION_KEY \
  -e ORCH8_LOG_JSON=true \
  ghcr.io/orch8-io/engine:latest
```

The included `docker-compose.yml` runs Postgres on port 5434 for local dev — it does **not** launch the engine. For a local stack that runs both, add an `orch8` service pointing at `postgres://orch8:orch8@postgres:5432/orch8`.

---

## Kubernetes

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
            - { name: ORCH8_LOG_JSON,       value: "true" }
            - { name: ORCH8_HTTP_ADDR,      value: 0.0.0.0:8080 }
            - { name: ORCH8_SHUTDOWN_GRACE_PERIOD_SECS, value: "30" }
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

The production path is the Helm chart at [orch8-io/helm-charts](https://github.com/orch8-io/helm-charts). Install and tune values there — this repo does not duplicate chart documentation.

```bash
helm repo add orch8 https://orch8-io.github.io/helm-charts
helm install orch8 orch8/orch8-engine
```

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

## Fly.io

Single-region, SQLite-on-volume is the fastest path:

```toml
# fly.toml
app = "orch8-engine"

[build]
  image = "ghcr.io/orch8-io/engine:latest"

[mounts]
  source = "orch8_data"
  destination = "/data"

[env]
  ORCH8_STORAGE_BACKEND = "sqlite"
  ORCH8_DATABASE_URL    = "sqlite:///data/orch8.db"
  ORCH8_LOG_JSON        = "true"

[http_service]
  internal_port = 8080
  force_https = true
  auto_stop_machines = false   # scheduler needs to stay up
  min_machines_running = 1
```

For HA or multi-region, use Fly Postgres and bump `min_machines_running = 2`.

---

## Railway / Render

Both support the container image directly. Steps:

1. Provision managed Postgres.
2. Create a service from `ghcr.io/orch8-io/engine:latest`.
3. Set env vars: `ORCH8_STORAGE_BACKEND=postgres`, `ORCH8_DATABASE_URL=<managed db url>`, `ORCH8_API_KEY=<random>`, `ORCH8_ENCRYPTION_KEY=<64 hex>`.
4. Expose port 8080. Set health check to `/health/ready`.
5. Scale to 2+ replicas for HA.

---

## Production checklist

Before opening traffic:

- [ ] `ORCH8_API_KEY` is set to a random 32+ char value.
- [ ] `ORCH8_ENCRYPTION_KEY` is set (64 hex chars) and backed up in a separate secret store.
- [ ] `ORCH8_CORS_ORIGINS` is restricted to the domains that need it — not `*`.
- [ ] `ORCH8_REQUIRE_TENANT_HEADER=true` if you run multi-tenant.
- [ ] Postgres SSL is enforced (`sslmode=require` in the DSN).
- [ ] Postgres automated backups are on. **Test the restore.**
- [ ] `/metrics` is scraped by your Prometheus.
- [ ] Log level is `info` or `warn`, format is `json`.
- [ ] Webhook subscribers handle dedup on `instance_id + event_type` (see [WEBHOOKS.md](WEBHOOKS.md)).
- [ ] Replicas ≥ 2 behind a load balancer with health checks.
- [ ] Graceful shutdown verified — send SIGTERM, in-flight steps complete, no orphaned tasks.

---

## Observability

Minimum viable:

- **Metrics:** scrape `/metrics` every 15s (see [API.md — Metrics](API.md#metrics)).
- **Logs:** `ORCH8_LOG_JSON=true`, ship to your log aggregator.
- **Dashboard:** import [`docs/grafana-dashboard.json`](grafana-dashboard.json) into Grafana.
- **Alerts:** at minimum, alert on `orch8_instances_failed_total` rate and `orch8_tick_duration_seconds` p99.

---

## See also

- [Configuration](CONFIGURATION.md) — every env var and TOML field
- [Architecture](ARCHITECTURE.md) — why `SKIP LOCKED` makes multi-replica safe
- [Webhooks](WEBHOOKS.md) — terminal-state event wiring
