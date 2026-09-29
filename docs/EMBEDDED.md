# Embedded Orch8: sub-tenants, embed tokens, rollouts, licensing

> **Stability: beta**, shipped and tested; may change in a minor release with a changelog note.

This guide is for vendors who run Orch8 inside their own product and serve
workflows to *their* customers. It covers:

1. [Sub-tenants](#sub-tenants): scoping, caps and metering per end customer.
2. [Embed tokens](#embed-tokens): short-lived browser credentials for the
   `/api/v1/embed/*` routes used by `@orch8/embed`.
3. [Theme](#theme): tenant branding served to the embed kit.
4. [Staged rollouts per sub-tenant](#staged-rollouts-per-sub-tenant).
5. [License keys](#license-keys): offline verification, soft enforcement only.
6. [Security model](#security-model).

The terms used below:

| Term | Meaning |
| --- | --- |
| Tenant | An engine tenant (`X-Tenant-Id` or a per-tenant API key). The vendor. |
| Sub-tenant | One of the vendor's customers, scoped inside a tenant. |
| Vendor backend | The vendor's server. It holds the tenant API key and mints embed tokens. |
| Embed kit | The `@orch8/embed` web components running in the end customer's browser. |

## Sub-tenants

A sub-tenant id is 1–128 characters of `[A-Za-z0-9._:-]`. The vendor backend
sends it as a header:

```http
POST /api/v1/instances
x-api-key: <tenant key>
X-Orch8-Sub-Tenant: acme-corp
```

- **Instances** created with the header carry `sub_tenant: "acme-corp"`. The
  body field `sub_tenant` is accepted too; when both are present they must
  match (403 otherwise). Without the header or field, behaviour is unchanged:
  the instance is tenant-level (`sub_tenant` absent).
- **Sequences** created with the header (`POST /api/v1/sequences`) are *owned*
  by that sub-tenant: only that sub-tenant's embedded builder may add versions.
- **Children** spawned by a `sub_sequence` block inherit the parent's
  sub-tenant. Forks keep the source's sub-tenant.
- **Lists and reads.** `GET /api/v1/instances?sub_tenant=acme-corp` filters by
  sub-tenant. With the header present, the header wins over the query, and
  `GET /api/v1/instances/{id}` answers 404 for an instance of another (or no)
  sub-tenant.
- **Batches.** `POST /api/v1/instances/batch` may target a single
  `(tenant, sub-tenant)`; mixing sub-tenants (or sub-tenant and tenant-level
  items) in one batch is a 400.

The header is a *scoping* input for the vendor backend, which already holds a
tenant key that can act on every sub-tenant. It is **not** an authorization
boundary for end customers; embed tokens are.

### Caps (pooled limits)

```http
PUT /api/v1/sub-tenants/acme-corp/limits
{ "max_executions_per_month": 10000, "max_concurrent": 25 }
```

`GET` returns the stored caps (`null` = uncapped; a sub-tenant without a row is
uncapped). Admission of every sub-tenant instance happens in one transaction,
under the same tenant lock as plan admission:

1. **Tenant pool.** The tenant's plan limit (`max_active_instances` from the
   [entitlement catalog](API_ENTITLEMENTS_AND_CLIENT_GATE.md)) covers all of
   its sub-tenants together. Exhausting it is `429 rate_limited`.
2. **Per-sub-tenant caps.** `max_concurrent` counts the sub-tenant's
   non-terminal instances; `max_executions_per_month` counts executions it
   started since the first of the current UTC month. Exceeding either is
   `429` with error code `sub_tenant_quota_exceeded`.

Caps apply to executions started through the API (`POST /instances`,
`/instances/batch`, `POST /embed/runs`). Child instances of a running workflow
are part of their parent's execution and are not admitted separately.

### Metering

```http
GET /api/v1/usage/sub-tenants?from=2026-09-01T00:00:00Z&to=2026-10-01T00:00:00Z
```

```json
{
  "items": [
    { "sub_tenant": "acme-corp", "executions_started": 412, "executions_completed": 398,
      "steps_executed": 2261, "last_active_at": "2026-09-28T13:02:11Z" }
  ],
  "active_sub_tenants": 1,
  "from": "2026-09-01T00:00:00Z",
  "to": "2026-10-01T00:00:00Z"
}
```

- The window is `[from, to)`; `from` defaults to the start of the current UTC
  month, `to` to now, and a window may span at most 400 days.
- `executions_started` comes from an append-only execution ledger written in
  the same transaction as the instance, so instance retention and pruning never
  change billed numbers. `active_sub_tenants` (sub-tenants with at least one
  started execution in the window) is the number Embedded billing uses.
- `executions_completed` and `steps_executed` are read from live instance and
  step-output rows and therefore only cover data that has not been pruned yet.

## Embed tokens

Embedding is off until a secret is configured; until then every
`/api/v1/embed/*` route answers 404.

```toml
[embed]
token_secret = "<64+ hex chars>"          # ORCH8_EMBED_TOKEN_SECRET
allowed_origins = "https://app.vendor.example"  # ORCH8_EMBED_ALLOWED_ORIGINS
```

The secret must decode to at least 32 bytes (`openssl rand -hex 32`). An
invalid secret stops the server at startup; it is never silently ignored.

### Minting

The vendor backend mints a token per end-customer session with a tenant API
key that has the Operator capability:

```http
POST /api/v1/embed/tokens
x-api-key: <tenant key>
x-tenant-id: <tenant>

{ "sub_tenant": "acme-corp",
  "scopes": ["runs:read", "runs:start", "approvals:resolve"],
  "sequences": ["onboarding"],
  "ttl_seconds": 900 }
```

Response: `{ "token": "o8e1.…", "expires_at": "…" }`. `ttl_seconds` defaults to
900 and may not exceed 3600. There is no refresh; mint a new token.

| Scope | Grants |
| --- | --- |
| `runs:read` | `GET /embed/runs`, `GET /embed/runs/{id}` |
| `runs:start` | `POST /embed/runs` (and sequence discovery) |
| `approvals:resolve` | `GET /embed/approvals`, `POST /embed/approvals/{id}` |
| `sequences:read` | `GET /embed/sequences`, `GET /embed/sequences/{name}` |
| `builder:edit` | `PUT /embed/sequences/{name}` (sub-tenant-owned sequences only) |

A missing scope is `403` with error code `embed_scope_denied`.

### Wire format

```text
o8e1.<b64url(payload)>.<b64url(HMAC-SHA256(secret, "o8e1." + b64url(payload)))>
payload = { "v":1, "tid":"<tenant>", "sub":"<sub-tenant>", "scp":[…],
            "seq":[…]|null, "iat":<unix>, "exp":<unix>, "jti":"<uuid>" }
```

`secret` is the hex-decoded `token_secret` bytes; base64url has no padding. A
vendor may mint tokens itself with the same secret. The engine rejects tokens
with unknown claims or scopes, `exp - iat > 3600`, `iat` more than 60 seconds
in the future, or an invalid sub-tenant id. The signature is compared in
constant time.

### Routes

All take `Authorization: Bearer o8e1…` and are bound to the token's tenant and
sub-tenant.

| Route | Returns |
| --- | --- |
| `GET /embed/runs?limit=&cursor=` | `{ items: [{ id, sequence, state, created_at, updated_at, current_step }], next_cursor }` |
| `GET /embed/runs/{id}` | `{ id, sequence, state, created_at, updated_at, steps: [{ id, name, state, started_at, finished_at, output? }] }` |
| `POST /embed/runs` `{ sequence, input, namespace?, idempotency_key? }` | `201 { id }` |
| `GET /embed/approvals` | `{ items: [{ id, instance_id, step_id, prompt, choices: [{label, value}], created_at }] }` |
| `POST /embed/approvals/{id}` `{ choice, comment? }` | `202`; `409` when already resolved or the run finished |
| `GET /embed/sequences` | `{ items: [{ id, name, namespace, version, description, input_schema, owned, gallery, title, template }], handlers: [{ name }] }` |
| `GET /embed/sequences/{name}?namespace=` | summary fields + `definition` + `redacted` |
| `PUT /embed/sequences/{name}?namespace=` | `201 { id, version }`: a new version owned by the sub-tenant |
| `GET /embed/theme` | `{ css_vars, logo_url?, hide_badge }` |

What an embedded viewer sees:

- **Runs.** Only runs of the token's sub-tenant, and only of sequences the
  token may run. A run of another sub-tenant is a 404, never a 403.
- **Outputs.** No context, metadata or step output is ever returned, except
  outputs of steps the sequence lists in `embed.visible_outputs`:

  ```json
  { "name": "onboarding", "embed": { "visible_outputs": ["summary"] }, "blocks": [ … ] }
  ```

- **Sequences.** A token sees its sub-tenant's own sequences, gallery
  templates (see below), and tenant-level sequences its `sequences` list admits
  (all of them when `sequences` is `null`). Owned sequences and gallery
  templates return the full `definition`. Other tenant-level sequences return
  a definition with every step's `params` emptied (`"redacted": true`) so
  URLs, prompts and credential references are not disclosed.
- **Gallery templates.** Tenant-level sequences in namespace `embed-gallery`
  with `"embed": { "gallery": true, "title": …, "template": … }` are listed to
  every sub-tenant as read-only. A builder copies one by reading it and writing
  it under a new name in its own namespace. `PUT` into `embed-gallery` is
  refused.
- **Builder.** `builder:edit` writes only names that no tenant-level sequence
  and no other sub-tenant already uses (409 `sequence name is not available`
  otherwise). Identity fields in the body (`id`, `tenant_id`, `name`,
  `version`, `sub_tenant`, …) are ignored and assigned by the server. The same
  validation as `POST /sequences` applies.

`builder:edit` lets an end customer author workflows that run with the
tenant's handlers and credentials. Grant it only to customers you trust with
that, the same way you would grant them a sequence editor in your own product.

### CORS

Origins listed in `[embed] allowed_origins` get CORS access to the embed-token
routes only; they get no access to the management API or to
`/embed/tokens`. `api.cors_origins` keeps governing the rest of the API.
Responses expose the `X-Orch8-License` header to browsers.

## Theme

```http
PUT /api/v1/embed/theme
x-api-key: <tenant key>

{ "css_vars": { "accent": "#0a66ff", "--orch8-radius": "6px" },
  "logo_url": "https://cdn.vendor.example/logo.svg",
  "hide_badge": true }
```

- `css_vars` keys may be sent with or without the `--orch8-` prefix and are
  stored and served without it. Names are `[A-Za-z0-9_-]` (up to 64 bytes);
  values may not contain `; { } < > \ " '` backticks, control characters,
  `url(` or `expression(`. At most 128 properties.
- `logo_url` must be `https`.
- `hide_badge` is stored as sent but reported `true` only while the engine has
  a valid license with the `white_label` feature.

`GET /api/v1/embed/theme` serves the effective theme to an embed token or to
the tenant's API key.

## Staged rollouts per sub-tenant

[Releases](RELEASES.md) accept an optional target that rolls a candidate out
by sub-tenant instead of by per-instance cohort:

```http
POST /api/v1/releases
{ "tenant_id": "vendor", "baseline_sequence_id": "…", "candidate_sequence_id": "…",
  "target": { "sub_tenants": ["design-partner-1"], "percentage": 10 } }
```

```http
PUT /api/v1/releases/{id}/target
{ "sub_tenants": ["design-partner-1", "acme-corp"], "percentage": 25 }
```

While the release routes traffic (canary):

- a sub-tenant instance gets the candidate when its sub-tenant is listed, or
  when a stable hash of `(release, sub-tenant)` falls under `percentage`.
  Raising the percentage only adds sub-tenants; a sub-tenant never flips back
  and forth;
- a tenant-level instance (no sub-tenant) stays on the baseline, the default
  release;
- after promotion every new instance gets the candidate, and after rollback
  every new instance gets the baseline.

`PUT …/target` with `null` returns to per-instance cohorts. Retargeting is
refused once a release is promoted or rolled back, and every change is
recorded in the release's decision trail.

## License keys

```toml
[license]
key = "o8l1.…"   # ORCH8_LICENSE_KEY
```

```text
o8l1.<b64url(payload)>.<b64url(ed25519 signature over "o8l1." + b64url(payload))>
payload = { "v":1, "licensee":"…", "edition":"oem"|"embedded"|"hybrid",
            "features":["white_label","sub_tenants",…], "max_sub_tenants":n|null,
            "issued_at":<unix>, "expires_at":<unix> }
```

The engine verifies keys offline against a public key compiled into the
binary. `GET /api/v1/license` reports
`{ status: "licensed"|"unlicensed"|"expired"|"invalid", edition?, licensee?, expires_at?, features, max_sub_tenants? }`.
`features` is empty unless the status is `licensed`. The engine adds no grace
period of its own; Orch8 Cloud builds any grace into `expires_at`.

Enforcement is **soft only**. A missing, invalid or expired license never
blocks an execution or a request. The engine counts sub-tenants that started
an execution in the last 30 days, across all tenants, and refreshes the count
at most once a minute:

- without a valid license that includes `sub_tenants`, more than 3 active
  sub-tenants adds `X-Orch8-License: unlicensed` to API responses;
- with such a license, more than `max_sub_tenants` adds
  `X-Orch8-License: over_limit`.

Either case also logs a warning at most once an hour.

### Rotating the verification key

The compiled-in key (`EMBEDDED_PUBLIC_KEY_B64` in `orch8-api/src/license.rs`)
is a placeholder in this release line: its private half was discarded, so no
license verifies against it until the production key is rotated in. To rotate:

1. Generate the production ed25519 keypair offline, on a trusted machine.
2. Store the PKCS#8 private key only in Orch8 Cloud's secret store as
   `ORCH8_LICENSE_SIGNING_KEY` (base64). Never commit it, and never put it in
   engine configuration.
3. Replace `EMBEDDED_PUBLIC_KEY_B64` with the base64 of the raw 32-byte public
   key and ship an engine release. Keys signed by the previous key stop
   verifying on that release, so re-issue licenses before customers upgrade.

`ORCH8_LICENSE_PUBLIC_KEY` (base64 of the raw 32-byte key) overrides the
compiled key. It exists for tests and private deployments; tests generate
ephemeral keypairs and never use the production key.

## Security model

| Boundary | Enforced by |
| --- | --- |
| Tenant ↔ tenant | API keys bind the tenant; embed tokens carry a signed `tid`. Resources of another tenant are 404. |
| Sub-tenant ↔ sub-tenant (browser) | Embed tokens carry a signed `sub`; every embed read and write checks the resource's `sub_tenant`. Foreign resources are 404. |
| End customer ↔ vendor configuration | No context, metadata or unlisted outputs on embed routes; tenant-level definitions are redacted. |
| Browser ↔ management API | `o8e1` bearers are accepted only on the embed-token route allowlist; everywhere else they are not a credential. Embed CORS origins are limited to those routes. |
| Token forgery and replay window | HMAC-SHA256 with a secret of at least 32 bytes, constant-time comparison, `exp - iat ≤ 3600`, strict claim schema. |
| Minting | `POST /embed/tokens` requires a tenant API key with the Operator capability. |
| License tampering | ed25519 signature over the exact token prefix and payload; any change makes the key `invalid`. |

Embed tokens are stateless: there is no per-token revocation. Keep TTLs short,
and rotate `token_secret` to invalidate every outstanding token at once.
