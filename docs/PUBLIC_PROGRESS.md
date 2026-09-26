# Public Progress Links

> **Stability: beta**, shipped and tested; may change in a minor release with a changelog note.

Share a read-only, live progress view of one workflow instance with someone
who has no Orch8 account — a customer watching their order, a candidate
watching an onboarding checklist — without exposing the workflow's data.

A share is a revocable bearer link:

- the token is 256 bits of randomness, returned **once**; Orch8 stores only
  its SHA-256;
- it expires (default 7 days, 60 s – 90 days);
- it is scoped to one instance of one tenant;
- the public view contains step labels, statuses, counts, and timestamps —
  **no `context.data` at all** unless you allowlist specific top-level keys.

## Create, list, revoke (authenticated)

```bash
# Create (Operator capability, tenant-scoped)
curl -s -X POST "$ORCH8_URL/instances/$INSTANCE_ID/share" \
  -H "X-Api-Key: $ORCH8_API_KEY" -H "X-Tenant-Id: acme" -H "Content-Type: application/json" \
  -d '{"expires_in_secs": 86400, "allowed_fields": ["order_id"]}'
```

```json
{
  "id": "0192…",
  "token": "Qm9…43 chars…",
  "url": "https://orch8.acme.com/public/progress/Qm9…",
  "embed_url": "https://orch8.acme.com/public/progress/Qm9…/embed",
  "embed_snippet": "<script src=\"https://orch8.acme.com/public/progress/embed.js\" data-token=\"Qm9…\" async></script>",
  "expires_at": "2026-09-27T10:00:00Z",
  "allowed_fields": ["order_id"]
}
```

URLs are absolute when the server has `api.public_url` (`ORCH8_PUBLIC_URL`),
otherwise relative paths.

```bash
curl -s "$ORCH8_URL/instances/$INSTANCE_ID/shares" -H "X-Tenant-Id: acme"          # never returns tokens
curl -s -X DELETE "$ORCH8_URL/instances/$INSTANCE_ID/share/$SHARE_ID" -H "X-Tenant-Id: acme"  # 204
```

CLI: `orch8 share create <instance> [--expires-in 86400] [--field order_id]`,
`orch8 share list <instance>`, `orch8 share revoke <instance> <share_id>`.

`allowed_fields` accepts at most 20 plain top-level keys (`[A-Za-z0-9_-]`).
Values larger than 4 KiB are omitted from the public view.

## Public endpoints (no authentication)

| Route | Returns |
|---|---|
| `GET /public/progress/{token}` | Redacted JSON (below) |
| `GET /public/progress/{token}/embed` | Self-contained HTML progress bar |
| `GET /public/progress/embed.js` | Loader for the `<script>` snippet |

These routes live outside the API-key/tenant middleware (like
`/webhooks/{slug}`) and are served by nodes that expose public webhooks
(`all_in_one`, `control`).

```json
{
  "status": "running",
  "completed": 2,
  "total": 4,
  "percent": 50,
  "steps": [
    {"label": "Charge card", "status": "completed", "started_at": "…", "completed_at": "…"},
    {"label": "Reserve inventory", "status": "completed"},
    {"label": "Ship order", "status": "running", "started_at": "…"},
    {"label": "Send receipt", "status": "pending"}
  ],
  "created_at": "…",
  "updated_at": "…",
  "link_expires_at": "…",
  "data": {"order_id": "ord_42"}
}
```

Step `status` is one of `pending`, `running`, `waiting`, `completed`,
`failed`, `skipped`, `cancelled`. Labels are the step ids humanized
(`send_receipt` → "Send receipt"); name steps accordingly. `data` is present
only when the share has an allowlist.

### Security properties

- **Fail-closed and uniform.** Unknown, malformed, expired, and revoked
  tokens, a missing instance, a share/instance tenant mismatch, and storage
  errors all return the same `404`.
- **Tenant isolation.** A share is created only for an instance the caller's
  tenant owns, and at read time the share's tenant must still own the
  instance.
- **Rate limited** per client IP (20 requests/second across the public
  progress routes; excess gets `429`).
- **No caching / leaking.** Responses carry `Cache-Control: no-store`,
  `Referrer-Policy: no-referrer`, `X-Robots-Tag: noindex`, and
  `X-Content-Type-Options: nosniff`.

## Embedding

Paste the snippet where the progress bar should appear:

```html
<script src="https://orch8.acme.com/public/progress/embed.js"
        data-token="Qm9…" data-height="72" async></script>
```

The loader validates the token's shape and inserts a sandboxed iframe
(`sandbox="allow-scripts allow-same-origin"`, `referrerpolicy="no-referrer"`)
pointing at `/public/progress/{token}/embed`. You can also iframe the embed
URL directly.

The embed page is fully self-contained: inline CSS and JS with **no external
dependencies**, pinned by a strict Content-Security-Policy —
`default-src 'none'; style-src 'sha256-…'; script-src 'sha256-…';
connect-src 'self'; base-uri 'none'; form-action 'none'; frame-ancestors *`.
The script contains no token (it derives the JSON URL from its own path),
renders with `textContent` only, polls every 5 s, and stops once the instance
is `completed`, `failed`, or `cancelled`.

## Operational notes

- Revocation is immediate; an already-loaded embed shows "Progress
  unavailable" on its next poll.
- Tokens are bearer credentials: anyone with the link can see the redacted
  view until it expires or is revoked. Prefer short expiries for sensitive
  workflows.
- Sub-sequence (child) instances are not expanded; share the child instance
  separately if needed.
