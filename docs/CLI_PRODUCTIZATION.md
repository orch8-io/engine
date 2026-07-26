# CLI productization commands

- `orch8 debug open` fetches bounded timeline, checkpoint, and effect evidence
  (maximum 500 rows per surface). `debug fork` creates a dry-run sandbox unless
  the operator explicitly opts into live effects. It complements rather than
  replaces the dashboard.
- `orch8 context set/use/list/remove` manages named URL, tenant, and API-key
  records. The file is atomically replaced with mode `0600` on Unix, insecure
  permissions fail closed, credentials never appear in listings, and
  `--context` explicitly overrides the selected default.
- `orch8 deploy` verifies a signed package locally, requires semantic diff and
  historical validation, starts a bounded canary, evaluates every requested
  observation, and promotes without force only when all gates succeed. Every
  failed HTTP gate stops the workflow visibly.

These commands do not provide a full-screen TUI, hide failed release gates, or
store credentials in shell arguments unless the operator explicitly passes one.

