# Node Roles

`[node].role` selects the process assembly. Unknown roles fail during config
parsing; a role never silently falls back to the all-in-one surface.

| Role | HTTP surface | gRPC surface | Engine | Push outbox |
|---|---|---|---:|---:|
| `all_in_one` | Full API, metrics, docs, public webhooks, health | Full | yes | yes |
| `control` | Full control API, metrics, docs, public webhooks, health | Full | no | yes |
| `executor` | Health only | Worker lifecycle, runtime session, artifact transfer, telemetry, health | yes | no |
| `gateway` | Canonical `/api/v1/continuity/*` plus health | Artifact transfer and health only | no | no |
| `edge` | Health only | Disabled | yes | no |

Executor and gateway gRPC restrictions are enforced on the fully-qualified RPC
path before handler dispatch. Disabled methods return `PERMISSION_DENIED`; they
are not merely hidden from documentation. Services omitted by a role are not
spawned and graceful shutdown joins only the selected assembly.

## Continuity gateway hardening

Gateway mode is a narrow trust boundary for capsule/handoff verification,
federation trust, disclosure minimization, residency policy, continuity
streams, and artifact transport. Startup fails unless all of these hold:

- root API-key authentication is configured;
- tenant headers are mandatory;
- the engine master encryption key is configured, enabling stable continuity
  signing and payload encryption keys;
- gRPC server certificate, private key, and client CA paths are all configured;
- the HTTP listener is loopback-only, intended to sit behind a local TLS
  reverse proxy.

The gateway does not mount the general management API, legacy unversioned
aliases, Swagger, metrics, mobile sync, circuit-breaker administration, or
public webhook ingestion. Its gRPC listener requires trusted client
certificates and exposes only health and resumable artifact transfer.

```toml
[node]
role = "gateway"

[api]
http_addr = "127.0.0.1:8080"
api_key = "replace-with-secret"
require_tenant_header = true
grpc_tls_cert_path = "/run/orch8/tls/server.crt"
grpc_tls_key_path = "/run/orch8/tls/server.key"
grpc_tls_client_ca_path = "/run/orch8/tls/client-ca.crt"

[engine]
encryption_key = "64-hex-characters"
```

`ORCH8_NODE_ROLE` overrides the TOML role and accepts exactly `all_in_one`,
`control`, `executor`, `gateway`, or `edge`.

## Managed-cloud outbound control

Executor and edge roles can establish a control-only outbound gRPC session to
a managed control plane. The endpoint must use HTTPS and requires a dedicated
API key, tenant, stable worker ID, and UUID runtime ID:

```toml
[node]
role = "edge"
managed_control_endpoint = "https://control.example.com"
managed_control_api_key = "replace-with-dedicated-key"
managed_control_tenant_id = "acme"
managed_control_worker_id = "edge-factory-1"
managed_control_runtime_id = "018f5f2d-58ef-7a61-9b4f-21f77aa1f005"
```

The session authenticates outbound, advertises only coarse runtime identity
and connectivity with empty plugin, credential, region, hardware, and signing
key lists, then refreshes a 45-second lease. It never sends task demand and
therefore never exports workflow contexts, params, outputs, artifacts, logs,
or credential bindings. `ping` and `reload` commands are acknowledged; a
`drain` command triggers local graceful shutdown. Placement commands remain
pending because this control-only channel will not accept workload payloads.

Disconnects retry with bounded 1–30 second exponential backoff. The dedicated
managed API key is removed from the long-lived engine config after the client
task receives it. Environment equivalents are
`ORCH8_MANAGED_CONTROL_ENDPOINT`, `ORCH8_MANAGED_CONTROL_API_KEY`,
`ORCH8_MANAGED_CONTROL_TENANT_ID`, `ORCH8_MANAGED_CONTROL_WORKER_ID`, and
`ORCH8_MANAGED_CONTROL_RUNTIME_ID`.

## Auditable fleet draining

An operator drain transition now persists four distinct facts on the cluster
node record: `drain_started_at`, capability withdrawal, `stopped_at`, and
execution handoff evidence. Setting drain immediately changes status to
`draining` and withdraws new claim/placement capability. The engine then stops
scheduling, waits its bounded in-flight/background drain, and only on graceful
deregistration writes:

```text
scheduler_drained; in_flight_work_completed_or_durably_recoverable
```

Stale-node reaping records `stopped_at` but never fabricates capability or
handoff evidence, so operators can distinguish a graceful transfer boundary
from a crashed node. Managed-control nodes additionally publish one final
`draining=true` capability heartbeat with a bounded flush window before the
outbound session closes. The cluster-node row remains as durable evidence
rather than being deleted at shutdown.
