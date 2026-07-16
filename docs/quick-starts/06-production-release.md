# Level 6: Ship a guarded production release

This level moves from a single-node SQLite tutorial to the production change
model: PostgreSQL, explicit migration, authenticated service startup, immutable
sequence versions, semantic diff, historical validation, canary routing, gates,
and rollback.

It is still a local environment. “Production” here describes the topology and
release discipline, not a claim that a laptop Compose stack is production
infrastructure.

**Starting point:** Docker Compose works, and you completed the failure/recovery
level so release gates have operational meaning.

**Result:** two versions of one workflow are stored, compared, and placed in a
guarded canary without silently replacing in-flight work.

## 1. Start PostgreSQL

From the repository root:

```bash
docker compose up -d postgres
docker compose ps
```

The repository Compose file exposes PostgreSQL using the connection string
documented in the project configuration. Export it explicitly:

```bash
export ORCH8_DATABASE_URL=postgres://orch8:orch8@127.0.0.1:5434/orch8
```

Wait until the service is healthy before migrating.

## 2. Run migrations as a deployment step

```bash
orch8 migrate --database-url "$ORCH8_DATABASE_URL"
```

For a single developer process, automatic migrations are convenient. In a
rolling production deployment, run migrations from CI or an init job before
starting the new server fleet. This prevents every replica from racing to own a
schema change.

## 3. Start an authenticated PostgreSQL server

Use stable tutorial secrets for the lifetime of this disposable database:

```bash
export ORCH8_API_KEY=$(openssl rand -hex 32)
export ORCH8_ENCRYPTION_KEY=$(openssl rand -hex 32)
export ORCH8_TENANT_ID=demo
export ORCH8_URL=http://127.0.0.1:8080/api/v1
```

Record these values in a secure temporary shell session if you need to restart
the server. Losing the encryption key makes encrypted records unreadable.

In a dedicated server terminal, export the same database, API, and encryption
variables, then run:

```bash
ORCH8_STORAGE_BACKEND=postgres \
ORCH8_DATABASE_URL="$ORCH8_DATABASE_URL" \
ORCH8_API_KEY="$ORCH8_API_KEY" \
ORCH8_ENCRYPTION_KEY="$ORCH8_ENCRYPTION_KEY" \
orch8-server
```

Check readiness from the CLI terminal:

```bash
curl -fsS http://127.0.0.1:8080/health/ready | jq
```

For a real deployment, replace exported secrets with a secret manager, use TLS
at the ingress or service boundary, restrict CORS to known origins, and run more
than one server replica only after shared storage and routing are verified.

## 4. Inspect the two immutable definitions

The repository includes a deliberately small release pair:

```bash
jq '{name, version, id, blocks}' examples/safe-release/baseline.json
jq '{name, version, id, blocks}' examples/safe-release/candidate.json
```

The candidate changes a retry policy and adds a guarded step. The IDs are fixed
for a repeatable tutorial. Both files represent the same tenant, namespace, and
sequence name but different immutable versions.

## 5. Preflight before upload

```bash
orch8 sequence preflight --file examples/safe-release/baseline.json
orch8 sequence preflight --file examples/safe-release/candidate.json
```

Preflight checks definition validity, lint findings, workers, version pins,
credentials, plugins, queues, and referenced sub-sequences. In CI, treat errors
as release blockers. Review warnings in context rather than suppressing them
globally.

Upload both versions:

```bash
orch8 sequence create --file examples/safe-release/baseline.json
orch8 sequence create --file examples/safe-release/candidate.json
```

List the stored versions:

```bash
orch8 sequence versions demo default release-example
```

If the example uses a different name in your checkout, read its top-level
`name` with `jq -r '.name'` and substitute that value.

## 6. Create a guarded release

The example definitions use these stable IDs:

```bash
export BASELINE_ID=01911111-1111-7111-8111-111111111111
export CANDIDATE_ID=01922222-2222-7222-8222-222222222222
```

Create a release whose candidate may regress error rate by no more than five
percentage points, with at least five terminal samples per variant:

```bash
export RELEASE_ID=$(orch8 --output json release create \
  --tenant-id demo \
  --baseline "$BASELINE_ID" \
  --candidate "$CANDIDATE_ID" \
  --max-error-regression 0.05 \
  --min-sample 5 | jq -r '.id')
printf '%s\n' "$RELEASE_ID"
```

Creating a release does not route traffic. It creates the auditable control
object that will own validation and routing decisions.

## 7. Read the semantic diff

```bash
orch8 release diff "$RELEASE_ID"
```

Unlike a text diff, the semantic diff classifies changes such as side-effect
risk, retry behavior, compatibility, and lint findings. Review it before replay
or canary steps. A small JSON edit can be operationally large when it changes a
side-effecting handler or removes a guard.

## 8. Validate against recorded history

```bash
orch8 release validate "$RELEASE_ID" --sample 5
```

Historical validation replays recorded evidence without invoking real side
effects. Missing history should remain visible as inconclusive evidence; it is
not proof of safety.

For a brand-new tutorial database, you may not have five representative
baseline runs. That is expected. The release system should not manufacture a
pass from absent evidence.

## 9. Start a small canary

```bash
orch8 release canary "$RELEASE_ID" --percent 10
orch8 release get "$RELEASE_ID"
```

Ten percent of newly created baseline-targeted instances are now
deterministically routed to the candidate cohort. Existing instances remain
pinned to the version on which they started.

Evaluate gates:

```bash
orch8 release evaluate "$RELEASE_ID"
```

With insufficient terminal samples, the result must be inconclusive. Do not use
`--force` merely to make a tutorial advance. The refusal is the feature.

## 10. Practice pause and rollback

Pause is reversible and sends new traffic back to baseline:

```bash
orch8 release pause "$RELEASE_ID"
orch8 release canary "$RELEASE_ID" --percent 10
```

Rollback is terminal for this release object:

```bash
orch8 release rollback "$RELEASE_ID"
orch8 release decisions "$RELEASE_ID"
```

The decision log is immutable evidence of who or what moved the release through
its states. Rollback affects routing for new instances; it does not rewrite
completed evidence or silently move in-flight executions.

## 11. Translate the tutorial into a production checklist

Before real traffic, verify each boundary:

| Boundary | Production requirement |
|---|---|
| Storage | Managed PostgreSQL, backups, restore test, bounded connection pools |
| Secrets | Stable encryption key and API keys in a secret manager, with rotation procedure |
| Network | TLS, explicit ingress policy, restricted CORS, private database access |
| Schema | Migrations run once before or during a controlled deployment phase |
| Compute | Multiple engine replicas, graceful drain, resource requests and limits |
| Workers | Independent scaling, unique worker IDs, heartbeat, idempotent side effects |
| Observability | JSON logs, Prometheus metrics, alerts tied to runbooks |
| Releases | Preflight, semantic diff, historical evidence, bounded canary, automatic gates |
| Recovery | DLQ grouping, representative retry, rollback command, database restore runbook |

Continue with [Deployment](../DEPLOYMENT.md) for platform-specific setup and
[Safe workflow releases](../RELEASES.md) for full state semantics.

## Final checkpoint

You have completed the quick-start ladder when you can explain the complete
execution and change model:

```text
author JSON
  -> preflight
  -> publish immutable sequence
  -> create durable instance
  -> engine schedules built-ins and worker tasks
  -> workers return typed outputs or failures
  -> retries are bounded; exhausted work is diagnosable
  -> candidate versions earn traffic through release evidence
```

You should also be able to identify the correct response to each situation:

- Bad instance input: reject at `input_schema`.
- Bad handler output: reject at `output_schema`.
- Temporary handler outage: bounded retry.
- Exhausted instance after the outage is fixed: controlled manual retry.
- Changed workflow behavior: new immutable version and guarded release.
- Failing candidate: automatic gate failure or explicit rollback.

## If something failed

| Symptom | Likely cause | Fix |
|---|---|---|
| PostgreSQL connection is refused | Compose service is not healthy or port differs | Run `docker compose ps` and compare the repository Compose mapping |
| Migration fails with authentication error | Connection string credentials differ from Compose | Use the exact database, user, password, and mapped port from the file |
| Server starts with SQLite | Storage backend variable was omitted | Set `ORCH8_STORAGE_BACKEND=postgres` with the Postgres URL |
| Creating a sequence returns a duplicate error | The fixed example IDs already exist | Reuse the stored versions, or recreate the disposable database |
| Validation is inconclusive | The fresh database lacks historical baseline runs | Generate representative runs; do not interpret absence as pass |
| Promotion is rejected | Gates have not passed | Gather the configured samples and evaluate again; force only under an explicit audited exception |
