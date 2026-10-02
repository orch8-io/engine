# Federation and BYOK externalization

> **Stability: beta**, shipped and tested; may change in a minor release with a changelog note.

This page covers two related capabilities:

1. **Federation transport**: run a sequence on another engine and wait for its result. The other engine can belong to another organization (cross-organization federation) or be another cluster of your own organization (cross-cluster child workflows).
2. **BYOK externalization**: store externalized payloads in a bucket you own, encrypted under a key you control.

Both are **off by default**. Nothing leaves an engine until an operator registers a peer or configures a vault.

---

## 1. Federation transport

### What already existed and what this adds

The engine already had the cryptographic primitives: ed25519-signed envelopes that live at most 300 s, are bound to a payload SHA-256 and to the receiving tenant, and are deduplicated through `federation_receipts`. Until now it had no network transport ([ENGINE_FEATURE_PRIORITIES](ENGINE_FEATURE_PRIORITIES.md): "network transport remains explicit"). This release adds an explicit, opt-in transport on top of the same envelopes. It adds:

- a per-tenant **trust registry** (`/api/v1/federation/peers`);
- an **HTTPS delivery** of signed envelopes to `POST {peer}/api/v1/federation/inbound`;
- a **`federate` step handler** that calls a sequence at a peer and parks until that run finishes;
- **inbound acceptance**, which maps a verified envelope to a local instance and answers with a signed response envelope.

It does **not** add a second spawn/join primitive. On the receiving side, a federated run is an ordinary instance. On the sending side, the step parks and resumes through the same `wait_for_input` gate and `human_input:<block>` resume signal that `wait_for_event` uses.

### Identity and trust

Each engine's federation identity comes from its continuity signing key, which is itself derived from `ORCH8_ENCRYPTION_KEY`:

```http
GET /api/v1/federation/identity
→ { "peer_id": "…uuid…", "public_key": "<base64 32 bytes>", "trust_root_sha256": "…" }
```

`peer_id` is a UUID derived from the public key. The registry rejects an entry whose `peer_id` does not match its key, so one identity cannot be bound to two keys. When the master key rotates, the identity rotates with it, and every peer has to re-register you.

Each side registers the other. Writes require the **root/admin API key**, because a peer entry lets another party start your sequences and receive declared outputs. Reads are tenant-scoped.

```http
POST /api/v1/federation/peers        (X-Tenant-Id: acme, root key)
{
  "peer_id": "<globex peer_id>",
  "name": "globex",
  "relationship": "organization",           // or "cluster" (same organization)
  "endpoint": "https://federation.globex.example",
  "public_key": "<globex public_key>",
  "remote_tenant_id": "globex-prod",        // tenant at the peer this link is bound to
  "outbound": {                              // what acme may do at globex
    "sequences": ["kyc-check"],
    "disclosed_fields": ["customer_id", "country"]
  },
  "inbound": {                               // what globex may do at acme
    "sequences": ["invoice-verify"],
    "handlers": ["http_request", "transform"],   // optional
    "returned_outputs": ["verdict"]
  },
  "expires_at": "2027-01-01T00:00:00Z"
}
```

`GET|PUT|DELETE /api/v1/federation/peers/{peer_id}` read, replace, and remove an entry. Setting `revoked_at` revokes it immediately.

**Mutual allowlists.** A call succeeds only when the caller's `outbound.sequences` **and** the callee's `inbound.sequences` both list the sequence. If the callee sets `inbound.handlers`, it also refuses any sequence that reaches a handler outside that list, or that contains a `sub_sequence`, which cannot be inspected statically.

**Disclosure minimization.** The step input is filtered down to `outbound.disclosed_fields` before it is stored or sent. Undeclared top-level fields never leave; the engine keeps only their SHA-256 as evidence. On the way back, only the outputs of the block ids in the callee's `inbound.returned_outputs` are returned. Context data, errors, logs, and other outputs stay with the callee. A failed remote run is reported as `failed` with no detail. The `"*"` wildcard is allowed only for `relationship: "cluster"`.

**Tenant binding.** Each envelope names the receiver's tenant, and the receiver accepts it only if its registry entry for the sender belongs to that tenant. Each response envelope names the caller's tenant in the same way.

### The `federate` step

```json
{
  "id": "kyc",
  "handler": "federate",
  "params": {
    "peer": "globex",
    "sequence": "kyc-check",
    "version": 3,
    "input": { "customer_id": "{{ context.data.customer_id }}", "country": "{{ context.data.country }}" },
    "call_key": "{{ context.data.customer_id }}"
  },
  "wait_for_input": { "prompt": "waiting for globex kyc", "timeout": 86400000 }
}
```

| Param | Required | Meaning |
|---|---|---|
| `peer` | yes | Registered peer name or peer id |
| `sequence` | yes | Sequence name at the peer (in the peer tenant's `default` namespace) |
| `version` | no | Pin a version; default is the latest |
| `input` | no | Object. Filtered by `disclosed_fields` |
| `call_key` | no | Distinguishes calls from the same block, for example inside `for_each`/loops |

`wait_for_input` is **required** (the linter warns without it) and must not define custom `choices`. Templates resolve against context only, the same as `wait_for_event`, because the call is registered at the gate. When the run completes, the block output is:

```json
{ "call_id": "…", "peer_id": "…", "sequence": "kyc-check", "remote_instance_id": "…",
  "state": "completed", "withheld_fields": 1, "outputs": { "verdict": { … } } }
```

A remote failure or cancellation, a peer refusal (4xx), or a revoked or expired peer fails the step permanently. A `wait_for_input.timeout` uses the normal gate timeout and escalation behavior.

### How it runs

```
sender engine                                         receiver engine
─────────────                                         ───────────────
gate parks step → federation_calls row (pending)
poller ── signed start ─────────────────────────────▶ verify peer + signature + freshness
                                                      allowlists → instance (idempotency key
                                                      federation:<peer>:<call>) + receipt
        ◀──────────────────────── signed {running} ─
poller ── signed status (1 s → 30 s backoff) ──────▶
        ◀─────────── signed {completed, outputs} ──── (only returned_outputs)
store result, send human_input:<block> → gate opens → handler returns result
```

- **No network I/O on the scheduler path.** The gate hook only inserts the call row. All transport happens in the federation poller, which runs every 1 s on every engine node that has a signing key.
- **Idempotency.** The call id is derived from `(tenant, instance, block, call_key)`, so step recovery reuses it. The receiver creates at most one instance per `(peer, call)` through the ordinary instance idempotency key. Concurrent pollers on several nodes, retries, and replayed envelopes all converge on one remote run. Each accepted envelope is recorded once in `federation_receipts` under a continuity scope bound to the call. A replay of an identical envelope gets the current status, never a second run.
- **Retries.** A network error, a 5xx or 429, or an unverifiable response is retried with exponential backoff capped at 64 s, for as long as the local instance is waiting. A 4xx is a refusal and fails the call.
- **Cancellation propagates.** If the local instance becomes terminal (cancelled, failed, or completed through another path) while the call is outstanding, the poller sends a signed `cancel`. The receiver enqueues a normal `cancel` signal, which also cascades to the remote instance's children. Cancel delivery is retried up to 10 times.
- **Cross-cluster children** (`relationship: "cluster"`) use the same path. The request additionally carries the parent `{instance_id, block_id}`, which the child records under `metadata.federation.parent`. Organization peers never receive it.

### Transport security

- Peer endpoints must be `https://`. `ORCH8_FEDERATION_ALLOW_HTTP=1` allows `http://`, for loopback tests only.
- `POST /api/v1/federation/inbound` sits **outside** API-key auth. The only credential it accepts is a signed envelope from a registered, unrevoked, unexpired peer, verified against the pinned key. Every refusal returns the same `403 federation request denied`. The body limit is 4 MiB.
- Envelopes live 60 s (the primitive caps them at 300 s). The payload digest, tenant, and call binding are all verified before any lookup that depends on the payload.
- The outbound client does not follow redirects and uses a 15 s timeout.
- `node.role = "gateway"` mounts the registry and the inbound route. The gateway only creates instances; engines that share its database execute them.
- Federation needs the engine encryption key. Without it, `/federation/identity` returns 503 and the poller does not start.

### Not included

- No push callback: the caller polls, with bounded backoff.
- No streaming or partial results.
- No payload encryption beyond TLS. Envelope signatures authenticate the payload; TLS keeps it confidential in transit.
- No automatic key rotation handshake: rotating the master key means re-registering with every peer.

---

## 2. BYOK externalization

### Which backends externalization supports

Externalization ([EXTERNALIZATION.md](EXTERNALIZATION.md)) stores large context fields and block outputs in the `externalized_state` table. Before this release, that table was the only backend. BYOK adds a customer-owned object-store backend to the encrypting storage layer.

### How it works

With a vault configured, every externalized payload goes through these steps:

1. The payload is encrypted with AES-256-GCM under a data-encryption key (DEK). The associated data binds it to `(instance id, ref key, object path)`.
2. The ciphertext is written to your bucket (S3, R2, MinIO, or any S3-compatible store) at `<prefix>/<instance>/<hash(ref)>-<uuid>`.
3. `externalized_state` stores only a reference:
   `{"_o8vault": {"v":1,"object":"…","kid":"<key id / KMS ARN>","dek":"<DEK wrapped by your key>","alg":"A256GCM"}}`.

The DEK is wrapped by your key provider:

| Provider | Configuration | Notes |
|---|---|---|
| AWS KMS | `ORCH8_BYOK_KMS_KEY_ARN`, optional `ORCH8_BYOK_KMS_REGION`, `ORCH8_BYOK_KMS_ENDPOINT` | `Encrypt`/`Decrypt` with encryption context `orch8=orch8-externalized-payload-v1`, SigV4-signed, no AWS SDK. Credentials come from `AWS_ACCESS_KEY_ID` / `AWS_SECRET_ACCESS_KEY` / `AWS_SESSION_TOKEN`. |
| Static | `ORCH8_BYOK_STATIC_KEY` (64 hex), `ORCH8_BYOK_STATIC_KEY_ID` | Local AES-256-GCM key wrap. For tests, or for operators who inject a key from their own secret manager. |

Bucket settings: `ORCH8_BYOK_BUCKET`, `ORCH8_BYOK_PREFIX` (default `orch8`), `ORCH8_BYOK_REGION`, `ORCH8_BYOK_ENDPOINT`, and `ORCH8_BYOK_ACCESS_KEY_ID`/`ORCH8_BYOK_SECRET_ACCESS_KEY`. If you leave the keys unset, the standard AWS chain is used (environment, web identity/IRSA, instance metadata). `ORCH8_BYOK_ALLOW_HTTP` allows plain HTTP. `ORCH8_BYOK_LOCAL_PATH` uses a local directory, for development only.

One DEK is reused for at most 5 minutes or 10,000 payloads, so KMS sees roughly one `Encrypt` call per 5 minutes per node. Unwrapped DEKs are cached in memory for 10 minutes. BYOK requires encryption at rest (`ORCH8_ENCRYPTION_KEY`), and the server refuses to start with a half-configured vault.

To store every context field and output in the vault instead of only large ones, set:

```toml
[engine]
externalization_mode = { type = "threshold", bytes = 0 }
```

`always_outputs` covers outputs only.

### What "Cloud never sees your data" guarantees

Assume a managed control plane runs **without** your bucket credentials and your KMS permission, and your executors run **with** both. Under that split:

- **Guaranteed:** the plaintext of every externalized payload is unreadable to the control plane and to anyone holding only a database dump. They see references: object path, key id, and a wrapped DEK that only your KMS key can unwrap. Revoking the key (KMS key policy or key disable) makes every payload unreadable, including to Orch8.
- **Guaranteed:** a reference cannot be replayed onto another instance or ref key, and an object cannot be swapped. AEAD associated data binds each reference to its location.
- **Operates on references only:** reads through a node without a vault return the reference unchanged instead of failing, so listing, retention, and inspection keep working on metadata.

### What it does not guarantee

- **Data below the externalization threshold stays in the database.** Use `bytes = 0` to externalize every *top-level context field*. Encryption at rest still protects it, but with the engine key.
- **Some data is never externalized:** sequence definitions, step `params` after template resolution (including anything interpolated into them), signal payloads, error messages, step logs, audit entries, checkpoints, instance metadata, and small outputs under `threshold`. It is also never externalized when `externalization_mode = never`. It is protected only by engine-key encryption at rest, where that applies.
- **The node that executes a step sees plaintext.** Only nodes you operate should hold vault access.
- **In hybrid mode** ([HYBRID.md](HYBRID.md#byok-keep-large-outputs-in-your-bucket)), remote executors configured with the vault seal large output fields themselves (`ORCH8_EXECUTOR_EXTERNALIZE_BYTES`) and report only references; the Cloud engine needs no vault or KMS access. Step params and context rendered by the engine still pass through it.
- **Instance deletion does not delete objects.** The database cascade removes references, but bucket objects stay behind. Use a bucket lifecycle rule to expire them, for example after your retention period. A crash between the object write and the database write can also leave an orphan object; the same lifecycle rule covers that.
- **Credential limits:** KMS credentials must be static or session env credentials. IMDS and web-identity credentials are supported for the bucket but **not** for KMS in this release.
