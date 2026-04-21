# Orch8 Engine — Configuration Reference

Configuration is layered: `orch8.toml` is the base, environment variables override individual fields, and CLI flags (where supported) take final precedence.

The `orch8.toml` file has four sections: `[database]`, `[engine]`, `[api]`, and `[logging]`.

---

## [database]

Controls the storage backend and connection pool.

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `backend` | string | `"postgres"` | Storage backend. `"sqlite"` or `"postgres"` |
| `url` | string | `"postgres://orch8:orch8@localhost:5432/orch8"` | Connection URL. For SQLite: `"sqlite://orch8.db"` |
| `max_connections` | integer | `64` | Connection pool size |
| `run_migrations` | bool | `true` | Automatically apply schema migrations on startup |

**SQLite connection strings:**

```
sqlite://orch8.db            # relative path (file in cwd)
sqlite:///data/orch8.db      # absolute path
sqlite://:memory:            # in-memory (testing only)
```

**Postgres connection strings:**

```
postgres://user:password@host:5432/dbname
postgres://user:password@host:5432/dbname?sslmode=require
postgresql://user:password@host/dbname?connect_timeout=10
```

---

## [engine]

Controls the scheduler tick loop, concurrency, crash recovery, webhooks, and encryption.

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `tick_interval_ms` | integer | `100` | How often the scheduler claims and advances instances (milliseconds) |
| `batch_size` | integer | `256` | Maximum instances claimed per tick across all tenants |
| `max_concurrent_steps` | integer | `128` | Semaphore limit — max step handlers executing simultaneously |
| `shutdown_grace_period_secs` | integer | `30` | Seconds to wait for in-flight steps to finish before forced shutdown |
| `stale_instance_threshold_secs` | integer | `300` | Instances in `running` state longer than this are considered stale and recovered |
| `max_instances_per_tenant` | integer | `0` | Max instances a single tenant can claim per tick (0 = no limit) |
| `externalize_output_threshold` | integer | `0` | Step outputs larger than this (bytes) are stored in `externalized_state` instead of inline (0 = disabled) |
| `encryption_key` | string | `""` | AES-256-GCM key for encrypting sensitive context fields at rest. Must be exactly 64 hex characters. Empty means no encryption. |

### [engine.webhooks]

Webhook events are fired on instance state transitions.

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `urls` | array of strings | `[]` | Endpoint URLs to POST event payloads to |
| `timeout_secs` | integer | `10` | Per-request timeout for webhook delivery |
| `max_retries` | integer | `3` | Number of delivery retries before giving up |

---

## [api]

Controls the HTTP and gRPC servers, authentication, CORS, and rate limiting.

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `http_addr` | string | `"0.0.0.0:8080"` | HTTP API listen address |
| `grpc_addr` | string | `"0.0.0.0:50051"` | gRPC API listen address |
| `cors_origins` | string | `"*"` | CORS `Access-Control-Allow-Origin` value. Use `*` to allow all origins or a comma-separated list of specific origins |
| `api_key` | string | `""` | If set, all requests must include `Authorization: Bearer <key>`. Empty means no authentication |
| `require_tenant_header` | bool | `false` | If `true`, all requests must include `X-Tenant-Id`. Requests without it receive `400 Bad Request` |
| `max_concurrent_requests` | integer | `0` | Global cap on in-flight HTTP requests (0 = unlimited). This is a concurrency limit, not an RPS limiter. Accepts the legacy alias `rate_limit_rps`. |

---

## [logging]

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `level` | string | `"info"` | Log verbosity. One of: `trace`, `debug`, `info`, `warn`, `error` |
| `json` | bool | `false` | If `true`, emit structured JSON logs. If `false`, emit human-readable text |

---

## Environment Variables

All config fields can be set via `ORCH8_*` environment variables. Environment variables override values in `orch8.toml`.

### Database

| Variable | Default | Description |
|----------|---------|-------------|
| `ORCH8_DATABASE_BACKEND` | `postgres` | `sqlite` or `postgres` |
| `ORCH8_DATABASE_URL` | `postgres://orch8:orch8@localhost:5432/orch8` | Connection string |
| `ORCH8_DATABASE_MAX_CONNECTIONS` | `64` | Connection pool size |
| `ORCH8_DATABASE_RUN_MIGRATIONS` | `true` | Auto-run migrations on startup |

### Engine

| Variable | Default | Description |
|----------|---------|-------------|
| `ORCH8_TICK_INTERVAL_MS` | `100` | Scheduler tick interval (ms) |
| `ORCH8_MAX_INSTANCES_PER_TICK` | `256` | Batch size per tick |
| `ORCH8_MAX_CONCURRENT_STEPS` | `128` | Semaphore limit for step execution |
| `ORCH8_SHUTDOWN_GRACE_PERIOD_SECS` | `30` | Graceful shutdown timeout |
| `ORCH8_STALE_THRESHOLD_SECS` | `300` | Stale instance recovery threshold |
| `ORCH8_ENCRYPTION_KEY` | — | 64 hex chars for AES-256-GCM encryption at rest |
| `ORCH8_CRON_TICK_SECS` | `10` | Cron loop check interval (seconds) |

### API

| Variable | Default | Description |
|----------|---------|-------------|
| `ORCH8_HTTP_ADDR` | `0.0.0.0:8080` | HTTP listen address |
| `ORCH8_GRPC_ADDR` | `0.0.0.0:50051` | gRPC listen address |
| `ORCH8_CORS_ORIGINS` | `*` | CORS allowed origins |
| `ORCH8_API_KEY` | — | Set to enable API key authentication |
| `ORCH8_REQUIRE_TENANT_HEADER` | `false` | Enforce `X-Tenant-Id` header |
| `ORCH8_MAX_CONCURRENT_REQUESTS` | `0` | Global in-flight request cap (0 = unlimited). Legacy `ORCH8_RATE_LIMIT_RPS` still accepted. |

### Logging

| Variable | Default | Description |
|----------|---------|-------------|
| `ORCH8_LOG_LEVEL` | `info` | Log level: `trace`, `debug`, `info`, `warn`, `error` |
| `ORCH8_LOG_JSON` | `false` | Set to `true` or `1` for structured JSON logs; any other value (or unset) uses human-readable pretty logs |

---

## Example Configurations

### Development — SQLite

No external dependencies. Suitable for local development and CI.

```toml
[database]
backend = "sqlite"
url = "sqlite://orch8.db"
max_connections = 16
run_migrations = true

[engine]
tick_interval_ms = 100
batch_size = 64
max_concurrent_steps = 32
shutdown_grace_period_secs = 10
stale_instance_threshold_secs = 60

[api]
http_addr = "127.0.0.1:8080"
grpc_addr = "127.0.0.1:50051"
cors_origins = "*"
require_tenant_header = false
max_concurrent_requests = 0

[logging]
level = "debug"
json = false
```

---

### Production — Postgres

Encrypted at rest, API key authentication, JSON logs, restricted CORS.

```toml
[database]
backend = "postgres"
url = "postgres://orch8:s3cr3t@db.internal:5432/orch8?sslmode=require"
max_connections = 64
run_migrations = true

[engine]
tick_interval_ms = 100
batch_size = 256
max_concurrent_steps = 128
shutdown_grace_period_secs = 30
stale_instance_threshold_secs = 300
max_instances_per_tenant = 0
encryption_key = ""  # set via ORCH8_ENCRYPTION_KEY env var

[engine.webhooks]
urls = ["https://hooks.internal/orch8-events"]
timeout_secs = 10
max_retries = 3

[api]
http_addr = "0.0.0.0:8080"
grpc_addr = "0.0.0.0:50051"
cors_origins = "https://app.example.com,https://admin.example.com"
api_key = ""  # set via ORCH8_API_KEY env var
require_tenant_header = true
max_concurrent_requests = 500

[logging]
level = "info"
json = true
```

For the secrets, pass them as environment variables at deploy time:

```bash
ORCH8_ENCRYPTION_KEY=<64-hex-chars>
ORCH8_API_KEY=<your-api-key>
ORCH8_DATABASE_URL=postgres://orch8:s3cr3t@db.internal:5432/orch8?sslmode=require
```

---

### High-Throughput

Tuned for high volume: larger batch sizes, higher concurrency, coarser tick interval to reduce database load.

```toml
[database]
backend = "postgres"
url = "postgres://orch8:s3cr3t@db.internal:5432/orch8"
max_connections = 128
run_migrations = false  # run migrations separately in CI/CD

[engine]
tick_interval_ms = 50
batch_size = 512
max_concurrent_steps = 256
shutdown_grace_period_secs = 60
stale_instance_threshold_secs = 300
max_instances_per_tenant = 64   # prevent a single tenant from starving others
externalize_output_threshold = 65536  # externalize outputs > 64 KiB

[api]
http_addr = "0.0.0.0:8080"
grpc_addr = "0.0.0.0:50051"
cors_origins = "*"
max_concurrent_requests = 0

[logging]
level = "warn"
json = true
```

**Tuning notes:**

- `tick_interval_ms = 50` doubles the claim rate but increases DB IOPS. Test with your Postgres instance before lowering further.
- `batch_size = 512` with `max_concurrent_steps = 256` means up to 256 steps run per tick, with the rest queued in the next tick.
- Set `max_instances_per_tenant` to a non-zero value when running multiple tenants to prevent one tenant's burst from blocking others.
- `externalize_output_threshold` keeps hot rows in `task_instances` small when step outputs can be large (e.g., LLM responses).

---

### Minimal

Smallest possible footprint. Single-tenant, no auth, no encryption, no webhooks.

```toml
[database]
backend = "sqlite"
url = "sqlite://orch8.db"

[engine]
tick_interval_ms = 200
batch_size = 32
max_concurrent_steps = 16

[api]
http_addr = "127.0.0.1:8080"
grpc_addr = "127.0.0.1:50051"

[logging]
level = "info"
```

Any field not listed here uses the default value shown in the reference tables above.

---

## Generating an Encryption Key

```bash
# macOS / Linux
openssl rand -hex 32

# Node.js
node -e "console.log(require('crypto').randomBytes(32).toString('hex'))"

# Python
python3 -c "import secrets; print(secrets.token_hex(32))"
```

The output is exactly 64 hex characters, which is the required format for `encryption_key` / `ORCH8_ENCRYPTION_KEY`.

> Store the key outside the repository. Rotating the key requires re-encrypting all existing context fields — there is no automatic migration.

---

## Configuration Precedence

1. Default values (compiled in)
2. `orch8.toml` fields
3. `ORCH8_*` environment variables (highest priority)

Environment variables always win. This means you can commit a `orch8.toml` with safe defaults and inject secrets at runtime without modifying the file.
