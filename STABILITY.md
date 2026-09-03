# Orch8 1.0 compatibility contract

Orch8 follows semantic versioning for Rust crates, published SDKs, the CLI, and
documented HTTP/gRPC contracts. A stable release supports the current minor line
and the previous two minor lines for security and data-loss fixes. Critical
security fixes may require an accelerated upgrade and will include migration and
rollback guidance.

## Stable at 1.0

- canonical `/api/v1` REST paths and the structured error envelope;
- sequence format `schema_version: 1` and block wire names;
- persisted migrations and rolling-upgrade guarantees;
- worker lease/heartbeat behavior and signed package verification;
- CLI commands not explicitly marked experimental.

Additive fields and operations are backward compatible. Removals or semantic
changes require a major release. Deprecated HTTP routes carry RFC 9745
`Deprecation`, `Sunset`, and successor links for at least two minor releases.
Stored sequences without `schema_version` decode as v1. Use
`orch8 sequence upgrade-format FILE --out NEW_FILE` before editing an older
document; the command is deterministic and never overwrites its input.

Experimental continuity-lab, fault-injection, and provider-preview surfaces may
change in a minor release and are labeled in their API documentation. Database
downgrades are supported only where a reviewed down migration exists; otherwise
restore the pre-upgrade backup.
