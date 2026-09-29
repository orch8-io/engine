# @orch8/embed

White-label web components that put Orch8 runs, approvals and a step editor
inside your product. Framework-free (Shadow DOM custom elements), with thin
React wrappers at `@orch8/embed/react`.

```html
<script type="module" src="https://cdn.jsdelivr.net/npm/@orch8/embed/dist/cdn/orch8-embed.esm.min.js"></script>

<orch8-runs base-url="https://orch8.example.com" token="o8e1…" href-template="/runs/{id}"></orch8-runs>
<orch8-run-timeline base-url="https://orch8.example.com" token="o8e1…" run-id="…"></orch8-run-timeline>
<orch8-approvals base-url="https://orch8.example.com" token="o8e1…"></orch8-approvals>
<orch8-builder base-url="https://orch8.example.com" token="o8e1…" sequence="onboarding"></orch8-builder>
<orch8-badge vendor="acme"></orch8-badge>
```

Mint embed tokens on your server (`POST /api/v1/embed/tokens` with your tenant
API key) and hand them to the browser, ideally through the `tokenProvider`
property so they refresh on their own.

Full guide, attributes, events, theming and a Next.js starter:
[docs/EMBED_KIT.md](https://github.com/orch8-io/engine/blob/main/docs/EMBED_KIT.md).
