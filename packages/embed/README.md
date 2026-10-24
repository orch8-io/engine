# @orch8/embed

White-label web components that put Orch8 runs, approvals and a step editor
inside your product. Framework-free (Shadow DOM custom elements), with thin
React wrappers at `@orch8/embed/react`.

```html
<!-- Pin an exact version (never a floating tag) and add Subresource Integrity. -->
<script
  type="module"
  src="https://cdn.jsdelivr.net/npm/@orch8/embed@0.1.0/dist/cdn/orch8-embed.esm.min.js"
  integrity="sha384-REPLACE_WITH_HASH"
  crossorigin="anonymous"
></script>

<orch8-runs
  base-url="https://orch8.example.com"
  token="o8e1…"
  href-template="/runs/{id}"
></orch8-runs>
<orch8-run-timeline
  base-url="https://orch8.example.com"
  token="o8e1…"
  run-id="…"
></orch8-run-timeline>
<orch8-approvals
  base-url="https://orch8.example.com"
  token="o8e1…"
></orch8-approvals>
<orch8-builder
  base-url="https://orch8.example.com"
  token="o8e1…"
  sequence="onboarding"
></orch8-builder>
<orch8-badge vendor="acme"></orch8-badge>
```

Compute the `integrity` value for the exact file you pin, for example
`curl -s <url> | openssl dgst -sha384 -binary | openssl base64 -A` (prefix with
`sha384-`). jsDelivr also shows it in the "Copy SRI" menu. Load one version per
page: when two copies are loaded the first one to register a tag wins, and the
second logs a console warning naming both versions.

Mint embed tokens on your server (`POST /api/v1/embed/tokens` with your tenant
API key) and hand them to the browser, ideally through the `tokenProvider`
property so they refresh on their own.

`EmbedClient` shares refreshes only for the same provider, engine base URL,
application `authContext`, and initial authorization scope. Provider callbacks
receive `{ reason, baseUrl, authContext }`, so one callback can mint the correct
credential for its caller. For provider-only clients, supply a stable account or
session key as `authContext`; construct a new client with a new context when the
account changes. A refresh that changes a known tenant/customer identity fails
with `token_scope_mismatch` before sending that credential. Token decoding is
only a consistency/UX check; the server verifies signatures and permissions.

```js
const client = new EmbedClient({
  baseUrl: "https://orch8.example.com",
  authContext: currentAccount.sessionId,
  tokenProvider: ({ authContext }) => fetchEmbedTokenForSession(authContext),
});
```

At most 64 context stores are retained per provider. Eviction can cause an extra
refresh when an old context is revisited; it never authorizes reuse across
contexts. Provider errors clear the in-flight refresh so the next call can retry.
Components expose the same boundary as the `auth-context` attribute / `authContext`
property, and React wrappers accept an `authContext` prop. Change it whenever the
account/session changes, especially when using an inline provider callback. The
widget immediately clears its old model, drafts, theme, and pending actions,
then loads the new context. Update or remove any static `token` at the same time;
changing a context label does not mint a different credential. A provider or
transport function replacement also resets the view. React keeps inline provider
callbacks stable, so changing only that closure is not an account-switch signal.

Pending operations are cancelled on a boundary change or disconnection. A
mutation already accepted by the engine can still finish; cancellation prevents
its late response or events from entering the new view and does not undo server
side effects. An old builder save resolves `false`; an old programmatic run start
rejects with an `aborted` error. Theme sharing is bounded to 128 authorization
contexts per page, with a five-minute TTL.

## Content Security Policy

The widgets work under a strict CSP with no `'unsafe-inline'` for styles:

- Styles are installed as constructable stylesheets (`adoptedStyleSheets`),
  which `style-src` does not block, and no element uses an inline `style=`
  attribute.
- Browsers without constructable stylesheets (Safari before 16.4) fall back to
  `<style>` elements. Give those the page's nonce through the `nonce`
  attribute (the `nonce` prop in React): `<orch8-runs nonce="r4nd0m" …>`
  and allow it with `style-src 'nonce-r4nd0m'`.
- `connect-src` must include your engine origin (`base-url`).
- `img-src` must include the origin of the vendor logo if you configure one.
  Logos must be `https:` URLs and are fetched with `referrerpolicy="no-referrer"`.
- `script-src` needs the CDN origin (or your own origin if you self-host), plus
  the SRI hash when using the CDN.

## Notes

- React: `className` is forwarded as the element's `class`. Inline
  `tokenProvider` / `strings` literals are safe; they do not cause reloads.
- Widgets pause polling while scrolled out of view or in a background tab, and
  add jitter and exponential backoff when the engine is failing.
- Rotating a token with the same tenant, customer, permission scopes, and sequence
  restrictions keeps the current view and unsaved input. Changing authorization,
  endpoint, or account context discards old drafts and reloads; applications that
  need to preserve drafts should store them under their originating account.
- Form controls use a 3:1 border colour (`--orch8-control-border`) and the
  widgets honour `forced-colors` (Windows High Contrast).

Full guide, attributes, events, theming and a Next.js starter:
[docs/EMBED_KIT.md](https://github.com/orch8-io/engine/blob/main/docs/EMBED_KIT.md).
