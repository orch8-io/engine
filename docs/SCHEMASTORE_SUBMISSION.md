# SchemaStore submission (prepared, not submitted)

> **Stability: n/a**, maintainer procedure; not submitted yet.

This page records how to register the Orch8 sequence-file schema with
[SchemaStore](https://www.schemastore.org/), so editors that use the SchemaStore
catalog (VS Code with the YAML extension, JetBrains IDEs, and others) pick up
validation and completion for Orch8 sequence files automatically.

Nothing here has been submitted yet. A maintainer has to open the upstream pull
request by hand.

## Which schema to register

`contracts/` holds two schemas. Both are generated from the OpenAPI
`SequenceDefinition` by `scripts/export-contracts.mjs`:

| File | `$id` | Use |
|---|---|---|
| `sequence.schema.json` | `https://orch8.io/contracts/sequence.schema.json` | A **persisted** sequence as the API stores and returns it. It requires `id`, `tenant_id`, `namespace`, `name`, `version`, `blocks`, and `created_at`. |
| `sequence-file.schema.json` | `https://orch8.io/contracts/sequence-file.schema.json` | A **hand-written** sequence file. It has the same properties and `$defs`, but only `name` and `blocks` are required. |

Register **`sequence-file.schema.json`**. The persisted schema requires fields
that files on disk normally leave out. `orch8 dev` fills in `id`, `tenant_id`,
`version`, and `created_at` and defaults `namespace`. `orch8 sequence apply`
adds `id`, `version`, and `created_at`. With the persisted schema, an editor
would flag every scaffolded file. That includes the `orch8 init` default and
every file in `docs/agent-patterns/`, which all fail it on `id`.

The files that `orch8 init`, `orch8 generate`, `orch8 sequence upgrade-format`,
and the dashboard create keep their `"$schema"` link to `sequence.schema.json`.
The engine and the CLI treat that link as metadata only. Changing the link is a
separate decision that needs Rust and dashboard changes, so it is out of scope
here.

Draft catalog entry, kept in
[`contracts/schemastore-catalog-entry.json`](../contracts/schemastore-catalog-entry.json):

```json
{
  "name": "Orch8 sequence",
  "description": "Orch8 durable workflow sequence definition file",
  "fileMatch": ["*.orch8.json", "*.orch8.yaml", "*.orch8.yml", "orch8.sequence.json"],
  "url": "https://orch8.io/contracts/sequence-file.schema.json"
}
```

The `fileMatch` globs are opt-in names. Plain `sequence.json`, which `orch8 init`
writes, is too generic to claim in a shared catalog, so it is not listed.
Files that carry an explicit `"$schema"` are validated regardless of name.

## Prerequisites before opening the PR

1. **The schema must be served at its `$id` URL.** The `Publish API contracts`
   workflow (`.github/workflows/contracts.yml`) regenerates both schemas on
   every push to `main` and deploys the `contracts/` directory to GitHub Pages.
   After the first deploy that includes `sequence-file.schema.json`, check
   that the public URL resolves:

   ```bash
   curl -fsSI https://orch8.io/contracts/sequence-file.schema.json
   curl -fsS  https://orch8.io/contracts/sequence-file.schema.json | jq -r '."$id"'
   ```

   The response must be `200` with a JSON content type (`application/json` or
   `application/schema+json`). GitHub Pages sends
   `Access-Control-Allow-Origin: *`, which browser-based editors need. If
   `orch8.io/contracts/` is served by something other than this Pages
   deployment, check that host's path mapping and CORS headers too.
2. **YAML globs depend on YAML sequence files.** `*.orch8.yaml` and
   `*.orch8.yml` are useful only after YAML sequence-file support ships in the
   CLI. If it has not shipped when you submit, drop those two globs and add
   them in a follow-up PR.
3. **Validate locally.** The authoring schema accepts every file in
   `docs/agent-patterns/*.json` and the `orch8 init` default template. It
   rejects files that are missing `name` or `blocks`. This was checked with
   Ajv 8 in draft 2020-12 mode.

## Upstream PR steps

These steps follow SchemaStore's `CONTRIBUTING.md` as of this writing. The
repository layout and scripts change from time to time, so read the current
guide before you start.

1. Fork `https://github.com/SchemaStore/schemastore` and clone your fork.
2. Create a branch, for example `add-orch8-sequence`.
3. Edit `src/api/json/catalog.json`. Insert the catalog entry above into the
   `schemas` array in **alphabetical order by `name`**. The upstream linter
   enforces this order.
4. Keep the schema **externally hosted** by leaving `url` pointing at
   `orch8.io`. Do not copy the schema into `src/schemas/json/`. Orch8
   regenerates it on every release, and a vendored copy would drift.
   SchemaStore accepts catalog entries for externally hosted schemas.
5. Optional: if the guide asks for test files for catalog-only entries, or you
   choose to vendor the schema, add a positive test such as
   `src/test/orch8-sequence/hello.orch8.json` (copy of the `orch8 init`
   default) and a negative test such as
   `src/negative_test/orch8-sequence/missing-blocks.orch8.json`.
6. Install and run their checks, using the commands listed in their
   `CONTRIBUTING.md`. At the time of writing that is `npm ci` followed by
   `node ./cli.js check`. The upstream npm scripts wrap the same command.
7. Commit with a message like `Add Orch8 sequence schema`, push the branch,
   and open a PR against `SchemaStore/schemastore:master`. Link the
   `orch8.io` schema URL and this repository in the description.
8. After the PR merges, editors refresh the catalog on their own schedule. VS
   Code picks it up within about a day.

## Keeping it correct after registration

- `scripts/export-contracts.mjs` writes both schemas, so whenever the OpenAPI
  `SequenceDefinition` changes, the Pages deploy updates the registered schema.
- Never tighten `required` in `sequence-file.schema.json` beyond what
  authoring tools actually require. Every existing user file would turn red in
  editors.
- Renaming the `$id` or moving the URL means another SchemaStore PR. Keep the
  URL stable.
