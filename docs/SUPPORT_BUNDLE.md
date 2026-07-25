# Operator Support Bundle

Create a bounded, atomically written diagnostic artifact with:

```bash
orch8 --url http://127.0.0.1:8080/api/v1 \
  --api-key "$ORCH8_API_KEY" --tenant-id acme \
  support-bundle --output orch8-support-bundle.json
```

The bundle schema includes CLI/server versions, durable storage and contract
schema versions, redacted typed configuration, live/ready probe results, up to
100 context-free workload summaries, and optionally one read-only diagnosis
(`--instance UUID`). Set `--max-instances` from 1–1,000 when support needs a
different bounded sample.

Workloads are constructed from an explicit field allowlist: identifier,
sequence identifier, state, priority, and lifecycle timestamps. Context,
metadata, parameters, payloads, outputs, logs, artifacts, and audit records are
never copied. A recursive second pass replaces secret, credential, token,
password, key, context, payload, parameter, and output fields with
`[REDACTED]`. Request headers are not represented, so the API key used to
collect the bundle cannot appear in it.

The output is JSON for portability and auditability. It is written through a
same-directory temporary file, fsync, and atomic rename so an interrupted
collection cannot leave a plausible but truncated support artifact.
