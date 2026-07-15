# Safe workflow releases

Use a release when a new immutable sequence version must be compared with the
current version before it receives all new traffic. A release never edits an
existing sequence and never silently migrates in-flight executions.

The state path is:

```text
draft → validating → ready → canary → promoted
  └──────────→ failed          ├──────→ paused → canary
                              └──────→ rolled_back
```

Every transition is compare-and-swap and appends an immutable decision record.
Gate evaluation is inconclusive until both variants reach the configured sample.
Inconclusive is never treated as pass.

## Before you start

Export the same connection values used by the CLI:

```bash
export ORCH8_URL=http://127.0.0.1:8080/api/v1
export ORCH8_API_KEY='replace-me'
export ORCH8_TENANT_ID=demo
```

The baseline and candidate must be different immutable versions of the same
tenant, namespace, and sequence name. Find their IDs with:

```bash
orch8 sequence versions demo default checkout
```

## CLI example: guarded canary

```bash
BASELINE_ID=01911111-1111-7111-8111-111111111111
CANDIDATE_ID=01922222-2222-7222-8222-222222222222

RELEASE_ID=$(orch8 --output json release create \
  --tenant-id demo \
  --baseline "$BASELINE_ID" \
  --candidate "$CANDIDATE_ID" \
  --max-error-regression 0.05 \
  --min-sample 20 | jq -r '.id')

# Inspect side-effect and compatibility risk before replaying history.
orch8 release diff "$RELEASE_ID"

# Effect-free replay of up to 20 recorded baseline executions.
orch8 release validate "$RELEASE_ID" --sample 20

# Route a deterministic 10% cohort of new executions to the candidate.
orch8 release canary "$RELEASE_ID" --percent 10

# Observe both variants. A failing gate rolls the canary back automatically.
orch8 release evaluate "$RELEASE_ID"

# Promotion is rejected until every configured gate passes.
orch8 release promote "$RELEASE_ID"

# The complete immutable state-transition trail remains available afterward.
orch8 release decisions "$RELEASE_ID"
```

Pause a canary without making it terminal:

```bash
orch8 release pause "$RELEASE_ID"
orch8 release canary "$RELEASE_ID" --percent 25
```

Roll back a canary or paused release permanently:

```bash
orch8 release rollback "$RELEASE_ID"
```

## REST example

The canonical API is `/api/v1`. Bare routes are compatibility aliases.

```bash
API=http://127.0.0.1:8080/api/v1
AUTH=(-H "x-api-key: $ORCH8_API_KEY" -H "x-tenant-id: demo" -H 'content-type: application/json')

curl -sS "${AUTH[@]}" -X POST "$API/releases" -d "{
  \"tenant_id\": \"demo\",
  \"baseline_sequence_id\": \"$BASELINE_ID\",
  \"candidate_sequence_id\": \"$CANDIDATE_ID\",
  \"gates\": [{
    \"metric\": \"error_rate\",
    \"max_regression\": 0.05,
    \"min_sample\": 20
  }],
  \"in_flight_policy\": \"pin\"
}"
```

Then call `POST /releases/{id}/validate`, `/canary`, `/evaluate`, `/promote`,
`/pause`, or `/rollback`. Read-only evidence is available from
`GET /releases/{id}/diff` and `GET /releases/{id}/decisions`.

## Dashboard example

Open **Releases** in the operator console. Create a release from stored versions,
select it in the release ledger, then load its semantic diff and audit trail.
Only actions legal in the current state are enabled. The console does not expose
forced promotion; use the CLI `--force` flag when an explicit, auditable override
is required.

## Operational rules

- Treat `side_effect_risk` and `incompatible` diff entries as release blockers.
- Historical validation never calls real handlers; recorded outputs are replayed
  and missing recordings become visible divergence or inconclusive evidence.
- Gate rates include terminal executions routed by that exact release since the
  canary began. Unrelated executions do not enter the denominator.
- Raising a canary percentage adds deterministic cohorts; existing cohorts do not
  flap between variants.
- Promotion changes routing for new executions. The default `pin` policy leaves
  in-flight executions on the version where they started.

See the generated `/swagger-ui` reference for exact response schemas.
