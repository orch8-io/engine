# Prompt registry

Keep prompts out of sequence JSON: push them to a versioned, tenant-scoped
registry, move labels such as `production` between versions, canary a new
version on a slice of traffic, and reference the prompt from `llm_call`.

- **Immutable versions.** Each push of new content appends version
  `latest + 1`. Pushing content identical to the latest version is a no-op that
  returns that version. Versions are never edited or deleted.
- **Labels** are movable aliases (`production`, `staging`, `canary`, …). A label
  can carry a canary split: `{version, canary_version, canary_percent}`.
- **Tenant isolation.** Every read and write is scoped to one tenant; a
  tenant-bound API key can only see its own prompts.
- **Replay determinism.** The version a step resolves is recorded in its output
  and pinned for that step of that execution, so retries and replays use the
  same text even after the label moves.

## Prompt file

```json
{
  "name": "ticket-triage",
  "description": "Route support tickets",
  "system": "You classify {{ product }} support tickets.",
  "messages": [
    { "role": "user", "content": "Ticket #{{ ticket.id }}: {{ ticket.body }}" }
  ],
  "model_params": { "provider": "openai", "model": "gpt-5.6-luna", "temperature": 0 },
  "response_schema": {
    "type": "object",
    "required": ["queue"],
    "properties": { "queue": { "enum": ["billing", "bugs", "sales"] } }
  }
}
```

- `name` and labels: 1–128 characters of letters, digits, `.`, `_`, `-`.
- `{{ path }}` placeholders are rendered at dispatch from the step's
  `prompt.variables`. Dotted paths index objects (`{{ ticket.id }}`). Strings are
  inserted verbatim; other JSON values are serialized. A missing variable fails
  the step with a permanent error that lists the missing names. No model call
  is made.
- Roles are `system`, `user`, `assistant` or `developer`.
- `model_params` are defaults. The step's own params win key by key.
  Credentials and endpoints (`api_key`, `api_key_env`, `base_url`,
  `providers`) are rejected: they stay with the step and its credential policy.
- `response_schema` must be a valid JSON Schema. A step-level
  `response_schema` overrides it.
- A version may be at most 256 KiB.

## CLI

```bash
export ORCH8_URL=http://127.0.0.1:8080/api/v1 ORCH8_TENANT_ID=acme

orch8 prompt push triage.json --label production   # v1, labelled production
orch8 prompt push triage-v2.json                   # v2 (label unchanged)
orch8 prompt list
orch8 prompt get ticket-triage                     # all versions + labels
orch8 prompt get ticket-triage --label production  # the version a label resolves to
orch8 prompt get ticket-triage --version 2

# Canary: 10% of executions get v2, the rest v1.
orch8 prompt label ticket-triage production --version 1 --canary-version 2 --canary-percent 10
# Promote: everything on v2.
orch8 prompt label ticket-triage production --version 2
orch8 prompt unlabel ticket-triage staging
```

## REST

| Method | Path | Purpose |
|---|---|---|
| `POST` | `/prompts` | Push a version (`201` created, `200` identical to latest). Optional `label` moves that label to the pushed version. |
| `GET` | `/prompts` | Registry listing: latest version, version count, labels. |
| `GET` | `/prompts/{name}` | All versions (newest first) and labels. |
| `GET` | `/prompts/{name}/versions/{version}` | One immutable version. |
| `GET` | `/prompts/{name}/resolve?label=…` or `?version=…` | The version a label or number resolves to. With neither, the latest version. |
| `PUT` | `/prompts/{name}/labels/{label}` | `{version, canary_version?, canary_percent?}` |
| `DELETE` | `/prompts/{name}/labels/{label}` | Remove a label. |

Tenant: from the `X-Tenant-Id` header or a per-tenant key. Unscoped root-key
callers pass `tenant_id` in the body or query. API keys with the `publisher`
capability may manage prompts, just as they manage sequences and releases.

## Referencing a prompt from `llm_call`

```json
{
  "type": "step", "id": "triage", "handler": "llm_call",
  "params": {
    "api_key_env": "OPENAI_API_KEY",
    "prompt": {
      "name": "ticket-triage",
      "label": "production",
      "variables": { "product": "Orch8", "ticket": "{{ context.data.ticket }}" }
    }
  }
}
```

`prompt` takes `version` (exact) or `label`, not both. With neither, the latest
version is used. Pin a `version` or use a label for production traffic.
`variables` values go through normal step templating first, so they can pull
from `context.data`, earlier outputs, and so on.

Merge rules:

1. The rendered `system` is used unless the step sets its own `system`.
2. The prompt's messages come first, followed by any `messages` the step adds.
   This is useful for few-shot prompts plus a runtime turn.
3. Each `model_params` key applies only if the step leaves it unset.
4. The prompt's `response_schema` applies unless the step sets one. Validation
   and auto-repair work as documented for `llm_call`.

The step output gains a `prompt` field:

```json
"prompt": { "name": "ticket-triage", "version": 1, "label": "production",
            "variant": "stable", "content_hash": "9f2c…" }
```

`variant` is `stable` or `canary` when the prompt was resolved through a label,
`pinned` for an explicit version, and `latest` otherwise. Each resolution also
emits a `tracing` event `orch8.prompt.resolved` (target `orch8::prompt`) with
the name, version, label and variant.

## Replay determinism

On its first real (non-dry-run) execution, a step stores the resolution in
instance state under `__prompt__:{block_id}:{name}`. Every later execution of
that step in that instance reuses the pinned version, even if the label has
moved. This covers retries, crash recovery, and loop iterations. New instances
follow the label. Release validation replays recorded outputs, and those
outputs name the exact version, so historical evidence stays accurate.

Dry-runs resolve the prompt (so a missing prompt or label is caught) but never
pin it.

## Canaries

A label's canary split is decided per execution with a deterministic hash of
`(instance_id, block_id, prompt name)`. This is the same scheme `ab_split`
uses, so an execution always sees the same side. To watch the canary, group
step outputs by `prompt.variant` / `prompt.version`, or use `GET /usage` for cost.

For a **gated** rollout with automatic rollback, put the two prompt versions in
two sequence versions (`prompt.version: 1` vs `prompt.version: 2`) and run a
[safe release](RELEASES.md). Release gates compare error rates between sequence
versions, and in-flight executions stay pinned. Label canaries are the
lightweight option: no gates, instant promotion or rollback by moving the label.

## Limitations

- Prompt templates are stored in plaintext, like sequence definitions. Do not
  put secrets in them.
- Rendering is simple substitution (`{{ path }}`). There are no filters,
  conditionals or loops.
- Labels themselves are not versioned. Label moves are not in an audit trail
  yet; outputs record what actually ran.
