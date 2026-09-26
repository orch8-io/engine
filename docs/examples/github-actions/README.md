# Orch8 GitHub Action examples

> **Stability: beta**, shipped and tested; may change in a minor release with a changelog note. Note: `mode: gate` wraps stable CLI commands; `diff` and `preview` are new.

The repository root `action.yml` is a composite action with three modes. Every
mode first installs the `orch8` CLI and validates the sequence locally with
`orch8 dev FILE --no-server --dry-run --skip-timers --once`.

| Mode | Example | What it does |
|---|---|---|
| `gate` (default) | [gate.yml](gate.yml) | Local validation. With `release-id`, runs `orch8 release diff`, `release validate`, and `release gate` against an existing release and posts a pass/fail comment. |
| `diff` | [sequence-diff.yml](sequence-diff.yml) | Semantic diff of the sequence at the PR base vs the PR head, plus a preflight report, in one PR comment that is updated in place. |
| `preview` | [preview.yml](preview.yml) | Publishes the head sequence into an ephemeral namespace, runs the server-side preflight on the stored copy, runs contract suites with `orch8 test run`, then deletes what it created. |

Copy an example into `.github/workflows/` and pin `orch8-io/engine@...` to a
release tag or commit SHA.

## Secrets and permissions

- `ORCH8_URL`: API base URL including `/api/v1`, e.g. `https://orch8.example.com/api/v1`.
- `ORCH8_API_KEY`: sent as `x-api-key`. Use a key scoped to the tenant you
  pass as `tenant-id`. The action masks it in logs.
- `diff` and `gate` comment on the PR with `github-token` (default
  `${{ github.token }}`), so the workflow needs `permissions: pull-requests: write`.
  On PRs from forks, GitHub gives a read-only token and withholds secrets, so
  these modes only work for same-repository branches.

## How `diff` works

1. It reads the baseline with `git show <base-sha>:<path>`. `base-sha` defaults
   to `github.event.pull_request.base.sha`. Check out with `fetch-depth: 0` so
   that commit is present.
2. It stamps both versions (`id`, `version`, `created_at`, `tenant_id`) into the
   ephemeral namespace `orch8-pr-<pr>-<run_id>-<attempt>`, then creates them with
   `orch8 sequence create`.
3. It calls `POST /api/v1/sequences/releases/diff`. The CLI has no subcommand
   for a diff between two sequences, so the action calls the API with `curl`.
4. It runs `orch8 sequence preflight --file` on the head version.
5. It renders the max severity, a change table, candidate lint, and the preflight
   checks. The comment carries a hidden `<!-- orch8-sequence-diff:<path> -->`
   marker, so each sequence path has one comment, edited in place.
6. A cleanup step with `if: always()` deletes the created sequences
   (`DELETE /api/v1/sequences/{id}`).

If the file is new, so there is no baseline, the comment says so and only the
preflight runs. `fail-on-severity` sets which severities fail the job:
`informational`, `behavioral`, `side_effect_risk`, `incompatible` (the default),
or `none`. `fail-on-preflight` fails the job unless preflight is `pass` or
`warning`.

## How `preview` works, and its limits

- **The namespace is a logical scope, not a separate tenant.** The preview
  sequence lives in the same tenant as your other sequences, so preflight sees
  that tenant's workers, credentials, and plugins. Set `preview-tenant-id` to
  publish into a dedicated tenant instead. That only works if the API key is
  allowed to act for that tenant.
- **`orch8 test run` is fully offline.** It mocks every handler and runs on
  virtual time. It never executes against the preview namespace or the server.
  The server is used only to publish the sequence and run preflight.
- Each suite's JUnit XML goes to the `junit-dir` output. Upload it as an
  artifact or feed it to a test reporter.
- Teardown only deletes sequences. Preview does not start instances, so the
  delete should not hit the `409 active instances` conflict. If a delete does
  fail, the action logs a warning with the id so you can remove it manually.

Sequence files must be JSON, because the scripts stamp them with `jq`.

The scripts live in [`scripts/action/`](../../../scripts/action/).
`bash scripts/action/test.sh` runs them end to end against stubbed `orch8`,
`curl`, and `gh` binaries.
