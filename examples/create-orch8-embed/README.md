# create-orch8-embed

A Next.js (App Router) SaaS starter that embeds Orch8 inside your product:
your customers see their own workflow **runs**, resolve **approvals**, and
edit **workflow steps**, without leaving your app and without an Orch8
account.

```text
browser ──(session cookie)──> /api/orch8/embed-token ──(x-api-key)──> Orch8 POST /api/v1/embed/tokens
   │                                   returns a 15-min token scoped to ONE customer (sub-tenant)
   └──(Bearer o8e1…)──> Orch8 /api/v1/embed/*   ← widgets from @orch8/embed
```

The tenant API key lives only in the server route. The browser gets a
short-lived token that can only see one customer's data, with scopes that
depend on the user's role.

## 1. See it working in 2 minutes (mock engine)

```bash
npx create-orch8-embed my-app
cd my-app
cp .env.example .env.local
# Leave ORCH8_URL empty to use the built-in mock engine
pnpm install
pnpm dev
```

Open http://localhost:3000, then:

1. **Runs** → **Start customer-onboarding** → you land on the run timeline.
2. Steps tick along until **activate_approval** is *Waiting*.
3. **Approvals** → choose *Activate*, add a comment, **Submit decision**.
4. Back on the run, the remaining steps complete.
5. Switch user (top right) to **Li · Globex**: a different customer, so you
   see different runs. **Raj** is a member, so the **Builder** is read-only
   for him.

## 2. Connect a real engine (about 10 minutes)

You need the `orch8` and `orch8-server` binaries
(`brew tap orch8-io/orch8 && brew install orch8`, or see the engine README).

```bash
orch8 init orch8-local && cd orch8-local
export ORCH8_ENCRYPTION_KEY=$(openssl rand -hex 32)
export ORCH8_EMBED_TOKEN_SECRET=$(openssl rand -hex 32)      # turns the embed API on
export ORCH8_EMBED_ALLOWED_ORIGINS=http://localhost:3000     # CORS for the widgets
orch8-server --config orch8.toml
```

In `my-app/.env.local`:

```bash
ORCH8_URL=http://127.0.0.1:8080
ORCH8_API_KEY=<api_key from orch8-local/orch8.toml>
ORCH8_TENANT_ID=demo
```

Publish the sample workflow once per demo customer, then restart the app:

```bash
pnpm seed      # POSTs orch8/customer-onboarding.json with X-Orch8-Sub-Tenant
pnpm dev
```

Now **Runs → Start** creates a real Orch8 instance for the signed-in
customer's sub-tenant, and the approval step is a real `human_review` +
`wait_for_input` gate.

If the token route logs `HTTP 404`, the engine has no embed secret. If it
logs `HTTP 401`, check `ORCH8_API_KEY` and `ORCH8_TENANT_ID`. If widgets show
"Can't reach the server", add your app's origin to
`ORCH8_EMBED_ALLOWED_ORIGINS`.

## 3. Make it yours

| File | What to change |
| --- | --- |
| `lib/users.ts`, `lib/auth.ts` | **Stub auth.** Replace `getCurrentUser()` with your session. Keep one sub-tenant per customer account (`subTenantFor`) and decide scopes per role (`scopesFor`). |
| `app/api/orch8/embed-token/route.ts` | The only place the API key is used. Return 401 when there is no session. |
| `lib/token-provider.ts` | Browser token cache. Widgets call it when a token is missing, about to expire, or rejected. |
| `components/widgets.tsx` | Widget props, brand colours (`theme`), and navigation on run select. |
| `orch8/customer-onboarding.json` | The sample workflow. `embed.visible_outputs` lists the steps whose outputs customers may see. |

White-label: with a license that includes `white_label`, call
`PUT /api/v1/embed/theme` with `{ "css_vars": {...}, "logo_url": "...", "hide_badge": true }`
and every widget picks it up.

Full widget reference: [docs/EMBED_KIT.md](https://github.com/orch8-io/engine/blob/main/docs/EMBED_KIT.md).

## Scripts

| Command | |
| --- | --- |
| `pnpm dev` / `pnpm build` / `pnpm start` | Next.js |
| `pnpm seed` | Publish the sample workflow for each demo customer (real engine only) |
| `pnpm lint` / `pnpm typecheck` | ESLint / TypeScript |
