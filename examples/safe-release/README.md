# Safe release example

This pair of immutable sequence versions is intentionally small so the release
evidence is easy to read. Version 2 changes a retry policy and adds a guarded
step; both should appear in the semantic diff.

Start an engine, then configure the CLI:

```bash
export ORCH8_URL=http://127.0.0.1:8080/api/v1
export ORCH8_API_KEY='replace-me'
export ORCH8_TENANT_ID=demo
```

Preflight and upload both versions:

```bash
orch8 sequence preflight --file examples/safe-release/baseline.json
orch8 sequence preflight --file examples/safe-release/candidate.json
orch8 sequence create --file examples/safe-release/baseline.json
orch8 sequence create --file examples/safe-release/candidate.json
```

Create and inspect the guarded release:

```bash
RELEASE_ID=$(orch8 --output json release create \
  --tenant-id demo \
  --baseline 01911111-1111-7111-8111-111111111111 \
  --candidate 01922222-2222-7222-8222-222222222222 \
  --max-error-regression 0.05 \
  --min-sample 5 | jq -r '.id')

orch8 release diff "$RELEASE_ID"
orch8 release validate "$RELEASE_ID" --sample 5
orch8 release canary "$RELEASE_ID" --percent 10
orch8 release evaluate "$RELEASE_ID"
```

With no routed terminal samples, evaluation is correctly `inconclusive`. Create
executions through the baseline sequence while the canary is active, then repeat
`evaluate`. Do not force promotion merely to make the example advance: the point
of a gate is to refuse success without evidence.

For production semantics and rollback commands, continue with the
[safe releases guide](../../docs/RELEASES.md).

