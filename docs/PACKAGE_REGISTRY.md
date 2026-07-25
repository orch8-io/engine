# Package Registry

`orch8-publisher` provides a tenant-scoped registry for deterministic,
Ed25519-signed workflow and connector packages. The registry is a set of static
CDN objects, so it does not introduce a control-plane service or database.

## Object layout

Publishing `acme/checkout@1.2.0` for `tenant-a` writes:

```text
tenant-a/registry/acme/
├── index.json
├── packages/checkout/1.2.0/<content-hash>.orch8pkg
└── transparency/
    ├── entries/00000000000000000042-<entry-hash>.json
    └── ledgers/<entry-hash>.json
```

The content-addressed package, numbered ledger entry, and hash-addressed ledger
snapshot are immutable and may be cached for a year. Only the discovery index
uses a 60-second cache window.

The tenant and publisher namespace are validated path segments. A publisher
for namespace `acme` rejects packages outside `acme/*`, and an index supplied
for another tenant or namespace is rejected before publication.

## Publication contract

Call `PackageRegistryPublisher::publish` with:

- a `SignedPackage` produced by `build_package`;
- the last verified `RegistryIndex` and `TransparencyLedger`;
- the package publisher's signing key; and
- an explicit publication timestamp.

The publisher verifies the package, the entire existing hash chain, the
index-to-ledger correspondence, the signing-key identity, and version
uniqueness. It builds the next state on clones, uploads immutable objects
first, then atomically advances the index with an ETag precondition. The
caller's state is only advanced after every write succeeds, making a failed
attempt safe to retry. Concurrent publishers cannot silently overwrite one
another: one wins the index compare-and-swap and the other receives
`CdnError::Conflict`, reloads the new head, and retries.

When loading an existing `index.json`, retain the response ETag with
`RegistryIndex::with_source_etag`. A freshly created index intentionally has no
source ETag and uses `If-None-Match: *` for its first publication.

## Consumer verification

Before displaying or installing a discovered version:

1. Deserialize the index and ledger and call `RegistryIndex::verify_against`.
2. If a previous ledger head was pinned, require
   `TransparencyLedger::contains_head` to return true.
3. Fetch the content-addressed package URL and call `verify_package`.
4. Apply a `TrustPolicy` to the embedded publisher key before installation.

Every transparency entry signs its canonical entry hash and commits to the
previous hash, tenant, namespace, package identity, content hash, package
signature, and publication time. This detects modified discovery metadata,
deleted or reordered records, and history forks. Pinning a previously accepted
head is required to detect a CDN serving an older but otherwise valid ledger.

The CDN is not a trust anchor. A CDN compromise can deny service, but it cannot
forge a package or an accepted continuation of a pinned publication history
without the publisher's private key.
