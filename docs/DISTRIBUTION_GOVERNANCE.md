# Governed distribution

`orch8-publisher::distribution` is the stable boundary for runtime-targeted
release channels, package deltas, private registry policy, supply-chain
attestations, and dependency locks.

## Guarantees

- Channels are limited to `stable`, `beta`, and `canary`, select packages with
  the existing `RuntimeCapabilities` and `CapsuleRequirements` vocabulary, and
  reject draining, expired, incompatible, or placement-mismatched runtimes.
- A signed channel covers its complete append-only history. Rollback selects an
  earlier immutable content hash and never mutates package bytes.
- Deltas are bounded whole-file changes. Consumers verify both base and target
  hashes and deterministically fall back to a declared full package URL.
- Private policy binds tenant, namespace, public signing roots, readers,
  publishers, and an opaque encrypted credential id. Cross-tenant access fails.
- Attestations use the in-toto Statement v1 envelope and require source,
  builder, test report, policy, and SBOM evidence tied to the package SHA-256.
- Lockfiles canonically order and hash exact connector, plugin, and model-policy
  versions. Duplicate logical dependencies and unpinned content are rejected.

## Non-guarantees

This module does not resolve arbitrary transitive ecosystems, execute install
scripts, interpret billing policy, or make an incompatible runtime compatible.
CDN transport and encrypted credential decryption remain host responsibilities.

