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
    ├── ledger.json
    └── entries/00000000000000000042-<entry-hash>.json
```

The content-addressed package and numbered ledger entry are immutable and may
be cached for a year. The discovery index and complete ledger use a 60-second
cache window.

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
first, then publishes the ledger and index heads. The caller's state is only
advanced after every write succeeds, making a failed attempt safe to retry.

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
