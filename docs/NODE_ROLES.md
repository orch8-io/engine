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
