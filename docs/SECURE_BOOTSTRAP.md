# Secure Production Bootstrap

`orch8 bootstrap` is the one-command path from an empty directory to a live,
verified node:

```bash
orch8 bootstrap ./my-orch8
```

It performs these gates in order:

1. scaffold missing project files without overwriting existing files;
2. generate independent 256-bit API and AES-256-GCM master keys;
3. restrict a newly generated `orch8.toml` to mode `0600` on Unix;
4. parse and validate the exact typed server configuration;
5. start `orch8-server` directly (no shell interpolation) with migrations
   enabled by the generated config;
6. poll `/health/ready` with bounded connect/request timeouts;
7. report the verified PID and remain attached until the server exits or
   Ctrl-C terminates and reaps it.

The default readiness deadline is 30 seconds and may be set from 1–300 seconds
with `--timeout-secs`. Wildcard HTTP listeners are probed through loopback.
If the child exits or readiness times out, bootstrap kills and reaps it and
returns a non-zero exit; it never reports a half-started node as successful.

Use `--server-bin /absolute/path/to/orch8-server` when the server binary is not
on `PATH`. Existing configurations are never regenerated: bootstrap validates
and uses them, and refuses to proceed when auth or encryption keys are absent.
