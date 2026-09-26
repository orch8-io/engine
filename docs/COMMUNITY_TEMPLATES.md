# Community templates

> **Stability: experimental**, may change or be removed in any release. Note: the community catalog repository is not published yet.

The `orch8` CLI ships a few built-in templates. You can list them with
`orch8 templates list` or scaffold one with `orch8 init --template <name>`. It
can also read a remote **template catalog**. The community catalog lives in a
separate repository, `orch8-io/community-templates`.

> Status: the repository has been prepared locally but is **not published
> yet**. The URLs below are placeholders until it is.

## Use a catalog

```bash
CATALOG=https://raw.githubusercontent.com/orch8-io/community-templates/main/catalog.json
orch8 templates list --catalog-url "$CATALOG"
orch8 templates show payment-dunning --catalog-url "$CATALOG"
orch8 templates pull payment-dunning --catalog-url "$CATALOG" --out payment-dunning.json

# or set it once
export ORCH8_TEMPLATE_CATALOG_URL="$CATALOG"
orch8 templates list
```

`--catalog-url` and `ORCH8_TEMPLATE_CATALOG_URL` exist in CLI builds from this
source tree. The `orch8` 0.7.1 binary inside `ghcr.io/orch8-io/engine:latest`
has neither; it lists built-in templates only. Built-in names take precedence:
`show` and `pull` look up a name among the built-ins first.

Try a pulled template locally before you deploy it:

```bash
orch8 dev payment-dunning.json --no-server --dry-run --skip-timers --once \
  --context '{"invoice_id":"inv_1","email":"a@example.com","email_api_url":"https://email.example.com/send"}' \
  --mock charge_payment='{"charged":true}' --mock suspend_subscription='{"suspended":true}' \
  --mock http_request='{"status":202,"body":{}}'
```

Then publish it with `orch8 sequence apply payment-dunning.json`, which stamps
`id`, `version`, and `created_at`.

## Catalog format

The CLI (`orch8-cli/src/commands/templates.rs`) accepts either of two shapes:

- a JSON array of entries
- an object `{"templates": [ ... ]}`

The response may be at most 8 MiB. Each entry has:

| Field | Required | Meaning |
|---|---|---|
| `name` | yes | Name used by `show` and `pull`. |
| `description` | no | One line shown by `list`. |
| `sequence` | one of these two | Inline sequence JSON. |
| `download_url` | one of these two | URL of the sequence JSON. The CLI fetches it with the same 8 MiB limit. |

The CLI ignores any other fields. The community catalog inlines `sequence`,
so a single request gets everything and any static host works. It also adds
metadata for people to read: `tags`, `author`, `min_engine_version`,
`handlers` (built-in handlers and external workers), and `requires`.

## Trust boundary

A catalog is a **separate trust boundary** from your engine.

- The CLI builds a separate HTTP client for catalog requests. It **never**
  sends your engine API key (`x-api-key`) or tenant header to a catalog or to
  a `download_url`.
- A template is untrusted input. Read `sequence.json` before you deploy it,
  and check every `http_request` URL, `emit_event` trigger, and handler name.
  Templates in the community catalog read endpoints from `context.data.*`
  rather than hard-coding them. Keep it that way in your copy.
- Pin a catalog to a commit, for example a raw URL that contains a commit SHA,
  if you need reproducible pulls.

## Contributing

Contributions go to the community-templates repository. Read its
`CONTRIBUTING.md`. In brief:

- A template is `templates/<kebab-name>/sequence.json` plus `template.json`.
- `template.json` holds the description, tags, the handlers and external
  workers the template needs, its prerequisites, and a validation context and
  mocks.
- The sequence must be in authoring format, with no `id`, `version`,
  `created_at`, or `tenant_id`. It may use only block types documented in
  [SEQUENCES.md](SEQUENCES.md), and it must contain no secrets.
- `scripts/validate.sh` runs static checks. It then runs every template once
  through the real engine with
  `orch8 dev <file> --dry-run --skip-timers --once --input … --mock …`, and
  checks that `catalog.json` matches `scripts/build-catalog.mjs`. The
  repository's CI runs the same script against the published engine image.
- Validation mocks every side-effecting and worker handler. It proves that the
  definition parses, passes engine validation, and completes. It does not
  prove that your endpoints or workers behave correctly.

The seed templates are:

- `onboarding-drip`
- `payment-dunning`
- `rag-ingestion`
- `approval-chain`
- `webhook-fan-out`

The repository's license is still undecided. The maintainers must choose one
before they publish it or accept outside contributions.
