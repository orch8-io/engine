# Embed kit

> **Stability: beta.** The widgets follow the embed API in the engine
> (`/api/v1/embed/*`). Attribute and event names may still change before 1.0.

`@orch8/embed` puts Orch8 inside your product. Your customers see their own
workflow runs, resolve approvals and edit workflow steps in your UI, under your
brand, without an Orch8 account.

| Element | Shows | Scopes it uses |
| --- | --- | --- |
| `<orch8-runs>` | Recent runs, "load more", optional **Start** button | `runs:read`, `runs:start` |
| `<orch8-run-timeline run-id>` | One run's steps, live until it finishes | `runs:read` |
| `<orch8-approvals>` | Pending approvals: choose an option, add a comment, submit | `approvals:resolve` |
| `<orch8-builder sequence>` | List-based step editor: add, reorder, remove, edit params as JSON | `sequences:read`, `builder:edit` |
| `<orch8-badge>` | "Powered by Orch8" link | none |

The fastest way to see all of them is the Next.js starter. It runs on a
built-in mock engine, so you don't need Orch8 installed:

```bash
npx create-orch8-embed my-app && cd my-app
cp .env.example .env.local && pnpm install && pnpm dev
```

See [examples/create-orch8-embed](../examples/create-orch8-embed/README.md).

## How it fits together

```text
your server ──x-api-key──> POST /api/v1/embed/tokens  { sub_tenant, scopes, sequences, ttl_seconds }
     │                          <── { token: "o8e1…", expires_at }
     ▼
browser widgets ──Authorization: Bearer o8e1…──> /api/v1/embed/runs | approvals | sequences | theme
```

1. **Sub-tenant per customer.** Each of your customer accounts maps to one
   Orch8 sub-tenant. Every embed token is bound to exactly one sub-tenant, so a
   widget can only see that customer's runs.
2. **Mint on the server.** Your backend exchanges its tenant API key for a
   short-lived token (TTL up to 3600 s). The API key never reaches the browser.
3. **Scopes per role.** Grant `builder:edit` only to your customers' admins,
   for example. Widgets hide actions the token can't perform, and the engine
   enforces the same scopes.

## Enable the embed API on the engine

```bash
export ORCH8_EMBED_TOKEN_SECRET=$(openssl rand -hex 32)          # ≥32 bytes; embed routes return 404 without it
export ORCH8_EMBED_ALLOWED_ORIGINS=https://app.example.com       # CORS: your app's origin(s), comma-separated
```

The same settings exist in `orch8.toml` as `[embed] token_secret` and
`[embed] allowed_origins`.

## 1. Mint a token (server)

```ts
// e.g. app/api/orch8/embed-token/route.ts (Next.js), or any backend route
export async function POST() {
  const user = await requireSession();                      // your auth
  const res = await fetch(`${process.env.ORCH8_URL}/api/v1/embed/tokens`, {
    method: "POST",
    headers: {
      "content-type": "application/json",
      "x-api-key": process.env.ORCH8_API_KEY!,             // server only
      "x-tenant-id": process.env.ORCH8_TENANT_ID!,
    },
    body: JSON.stringify({
      sub_tenant: `org:${user.orgId}`,                      // [A-Za-z0-9._:-], 1–128 chars
      scopes: user.isAdmin
        ? ["runs:read", "runs:start", "approvals:resolve", "sequences:read", "builder:edit"]
        : ["runs:read", "runs:start", "approvals:resolve", "sequences:read"],
      sequences: null,                                      // or ["customer-onboarding"] to restrict
      ttl_seconds: 900,
    }),
  });
  if (!res.ok) return new Response("token mint failed", { status: 502 });
  return Response.json(await res.json(), { headers: { "cache-control": "no-store" } });
}
```

## 2. Use the widgets (browser)

### Plain HTML / any framework

```html
<script type="module">
  import "https://cdn.jsdelivr.net/npm/@orch8/embed/dist/cdn/orch8-embed.esm.min.js";

  // One provider for every widget: fetches from your route, refreshes on expiry and on 401.
  let cached = null;
  async function tokenProvider({ reason }) {
    if (cached && reason !== "unauthorized" && Date.parse(cached.expires_at) - Date.now() > 60_000) return cached.token;
    const res = await fetch("/api/orch8/embed-token", { method: "POST", credentials: "same-origin" });
    cached = await res.json();
    return cached.token;
  }
  for (const el of document.querySelectorAll("orch8-runs, orch8-run-timeline, orch8-approvals, orch8-builder")) {
    el.tokenProvider = tokenProvider;
  }
</script>

<orch8-runs base-url="https://orch8.example.com" start-sequence="customer-onboarding" href-template="/runs/{id}"></orch8-runs>
<orch8-run-timeline base-url="https://orch8.example.com" run-id="0192…"></orch8-run-timeline>
<orch8-approvals base-url="https://orch8.example.com"></orch8-approvals>
<orch8-builder base-url="https://orch8.example.com" sequence="customer-onboarding"></orch8-builder>
```

For a quick test you can pass a token directly with the `token="o8e1…"`
attribute. It won't refresh.

With a bundler: `pnpm add @orch8/embed`, then `import "@orch8/embed";`.
Importing registers the elements and is safe during SSR.

### React / Next.js

```tsx
"use client";
import { useRouter } from "next/navigation";
import { Orch8Approvals, Orch8Runs, type TokenProvider } from "@orch8/embed/react";

const baseUrl = "https://orch8.example.com";

export function Automations({ tokenProvider }: { tokenProvider: TokenProvider }) {
  const router = useRouter();
  return (
    <>
      <Orch8Runs baseUrl={baseUrl} tokenProvider={tokenProvider} startSequence="customer-onboarding"
                 onRunSelect={({ id }) => router.push(`/runs/${id}`)} />
      <Orch8Approvals baseUrl={baseUrl} tokenProvider={tokenProvider}
                      onApprovalResolved={({ choice }) => console.log(`Recorded ${choice}`)} />
    </>
  );
}
```

The wrappers are thin and typed. On the server they render the bare tag and
its attributes. After mount they register the elements and assign JS-only
props (`tokenProvider`, `strings`, `fetchImpl`).

## Attributes, properties and events

Every widget takes these:

| Attribute / prop | |
| --- | --- |
| `base-url` / `baseUrl` | Engine origin. `/api/v1/embed` is appended. Defaults to the page origin. |
| `token` | Static embed token. |
| `tokenProvider` (JS property) | `({ reason: "initial" \| "expiring" \| "unauthorized" }) => string \| Promise<string>`. Called when there is no token, 30 s before `exp`, and once on a 401. |
| `theme` | JSON object of `--orch8-*` variables (with or without the prefix). Overrides the server theme. |
| `color-scheme` | `light`, `dark` or `auto` (default: follow the OS). |
| `locale` | BCP 47 tag for dates and relative times. |
| `vendor` | Your slug in the badge link. Defaults to the token's tenant. |
| `strings` (JS property) | Partial map of UI strings, for i18n. See `defaultStrings`. |
| `fetchImpl` (JS property) | Custom `fetch`, e.g. for a proxy. |
| `refresh()` (method) | Reload now. |

| Element | Extra attributes | Events (`detail`) |
| --- | --- | --- |
| `orch8-runs` | `page-size` (20), `poll-interval` ms (10000, `0` = off), `href-template` (`/runs/{id}` renders links), `start-sequence` | `orch8-run-select` `{id, run}`, `orch8-run-started` `{id, sequence}` |
| `orch8-run-timeline` | `run-id`, `poll-interval` ms (3000). Polling stops at completed, failed or cancelled. | `orch8-run-update` `{run}` |
| `orch8-approvals` | `instance-id` (one run only), `poll-interval` ms (15000) | `orch8-approval-resolved` `{id, instance_id, choice, comment?}` |
| `orch8-builder` | `sequence` | `orch8-sequence-saved` `{name, definition}`, `orch8-dirty-change` `{dirty}` |
| `orch8-badge` | `vendor` | none |

Events bubble and cross the shadow boundary (`composed: true`).

## Builder behaviour

- Top-level `step` blocks can be edited: step ID, action (handler) and params
  as JSON. The action list comes from `GET /api/v1/embed/sequences` (`handlers`).
  An action with `default_params` pre-fills new steps.
- Composite blocks (`parallel`, `loop`, `router`, …) are shown locked. You can
  move them but not edit them.
- Other step fields, such as `retry`, `timeout` and `wait_for_input`, are kept
  unchanged on save.
- The builder checks, before saving: IDs are present, match
  `[A-Za-z0-9_.-]`, and are unique (including IDs nested in locked blocks);
  params are a JSON object.
- Save sends `PUT /api/v1/embed/sequences/{name}` with the full definition.
  The engine only accepts it for sequences owned by the token's sub-tenant.
  Otherwise the widget shows the 403.
- Without `builder:edit` the builder is read-only.

## Theming and white-label

Theme values are applied in this order, later wins:

1. Built-in defaults (WCAG AA contrast, light and dark).
2. The server theme: `GET /api/v1/embed/theme` → `{ css_vars, logo_url, hide_badge }`,
   fetched once per page per sub-tenant.
3. The `theme` attribute.
4. Your page CSS on the element, e.g. `orch8-runs { --orch8-accent: #0f766e; }`.

| Variable | Default (light) |
| --- | --- |
| `--orch8-accent` / `--orch8-accent-fg` | `#3346c4` / `#ffffff` |
| `--orch8-bg`, `--orch8-surface`, `--orch8-fg`, `--orch8-muted`, `--orch8-border` | white, `#f6f7f9`, `#1a1d23`, `#555c69`, `#d5d9e0` |
| `--orch8-success`, `--orch8-danger`, `--orch8-warning`, `--orch8-info`, `--orch8-focus` | AA-contrast greens, reds, ambers and blues |
| `--orch8-radius`, `--orch8-space`, `--orch8-font`, `--orch8-font-size`, `--orch8-mono-font` | `8px`, `8px`, system UI, `14px`, system mono |
| `--orch8-dark-*` | Same names, used in dark mode |

Parts are exposed for deeper styling: `::part(header)`, `title`, `logo`,
`table`, `row`, `state`, `timeline`, `step`, `approval`, `badge`, `footer`,
`loading`, `empty` and `error`.

Set the vendor theme once for all your customers with your tenant API key:

```bash
curl -X PUT "$ORCH8_URL/api/v1/embed/theme" \
  -H "x-api-key: $ORCH8_API_KEY" -H "x-tenant-id: $ORCH8_TENANT_ID" \
  -H "content-type: application/json" \
  -d '{ "css_vars": { "accent": "#0f766e", "radius": "10px" }, "logo_url": "https://cdn.example.com/logo.svg", "hide_badge": true }'
```

`hide_badge: true` only takes effect with a license that includes the
`white_label` feature. Without that license it is stored but reported as
`false`, and the badge stays. Server CSS values that could break out of a
declaration or load resources (`;`, `{}`, `url(`, `@import`) are dropped.
`logo_url` must be `http(s)`.

### The "Powered by Orch8" badge

Each widget footer shows a link to `https://orch8.io/?ref=embed&v=<vendor>`,
unless the theme hides it. The link opens in a new tab with `rel="noopener"`,
and the anchor text is always the plain brand name. To show the badge
somewhere else, for example in your app footer, use `<orch8-badge vendor="acme">`.

## Accessibility

- Semantic structure: a table with column headers for runs, an ordered list
  for the timeline, and a `<form>` with `fieldset`/`legend` radios and a
  labelled comment for each approval. Builder fields have labels, and errors
  are linked with `aria-describedby`.
- State is always written as text, never shown by colour alone.
- A polite live region announces step changes, submitted decisions, and
  moves, adds and removes in the builder. Errors use `role="alert"`.
- Keyboard: all actions are native buttons, links and inputs. Focus is kept
  across polling re-renders and after reorder, remove and add. Focus rings use
  `:focus-visible`. The spinner respects `prefers-reduced-motion`.
- Approval cards are matched by ID on refresh, so polling never discards a
  comment someone is typing.

## Reliability

- **Retries.** `GET` and `PUT` retry 5xx, 429 and network errors 3 times,
  with exponential backoff and jitter. 429 follows `Retry-After`. `POST`
  (start run, resolve approval) is never retried.
- **Polling.** Polling pauses while the tab is hidden. After an error it backs
  off up to 60 s and shows "Retrying…" while keeping the last data on screen.
  It stops on 401, 403 and 404.
- **Live updates.** The engine's SSE stream (`/instances/{id}/stream`) needs
  an API key and isn't exposed to embed tokens. Widgets therefore poll.
- **Errors and empty states.** Every widget has loading, empty and error
  states. The error state for 403 includes the engine's message, for example
  which scope is missing.

## Low-level client

```ts
import { EmbedClient } from "@orch8/embed";
const client = new EmbedClient({ baseUrl, tokenProvider });
await client.startRun({ sequence: "customer-onboarding", input: { plan: "pro" } });
```

`EmbedClient` exposes `listRuns`, `getRun`, `startRun`, `listApprovals`,
`resolveApproval`, `listSequences`, `getSequence`, `putSequence` and
`getTheme`. Errors are `Orch8EmbedError` with a `kind` (`unauthorized`,
`forbidden`, `not_found`, `conflict`, `invalid`, `rate_limited`, `server`,
`network`, `config`), `status`, and the engine's `code`.

## Size

The self-registering bundle is about 16 KiB gzipped (52 KiB minified), with
no runtime dependencies. Measure it with `pnpm --dir packages/embed size`.
