# Private scoped catalog sync

Status: implemented internally; public scoped SDK and execution APIs remain
under development. See [ADR-0002](adr/0002-mobile-account-and-catalog-scopes.md)
for the complete ownership contract and open gates.

## Trusted endpoint configuration

A catalog persists its manifest URL and identity URL once, under its account
and session epoch. Both endpoints require canonical public HTTPS URLs with no
userinfo, query or fragment. An endpoint change is refused instead of silently
resetting catalog identity or replay state. Credential refresh preserves the
binding and uses a fresh identity request. This private path uses bearer auth;
public catalogs and URL-token configuration require separate explicit policies.

The application's authenticated backend implements the configured identity
endpoint. This is an application integration contract, not a new login service
provided by the engine. The backend must validate the bearer credential, derive
the principal from that authentication, and authorize the requested catalog.
Client-supplied source/catalog labels and a declared account are insufficient
proof of membership. Responses should use `Cache-Control: no-store`.

The client sends a POST with `Authorization: Bearer <current credential>`,
`Cache-Control: no-store`, and this JSON shape:

```json
{
  "nonce": "a fresh unpredictable request identifier",
  "source": "stable-source-descriptor",
  "catalog": "catalog-key"
}
```

The HTTP 200 JSON response echoes that nonce and binds the authenticated
principal to the requested source/catalog:

```json
{
  "format": "orch8.catalog-identity.v1",
  "nonce": "the exact request identifier",
  "audience": {
    "source": "stable-source-descriptor",
    "catalog": "catalog-key",
    "access": {
      "kind": "principal",
      "issuer": "stable-issuer-descriptor",
      "subject": "authenticated-user-subject"
    }
  },
  "issued_at": "2026-10-24T12:00:00Z",
  "expires_at": "2026-10-24T12:01:00Z"
}
```

The timestamps above illustrate the format. The backend supplies current times.
Responses are capped at 16 KiB, must be unexpired, permit at most 30 seconds of
future clock skew, and may live no longer than five minutes. Unknown/duplicate
fields, unknown formats, nonce/audience mismatches and redirects are refused.
The authority is the explicitly configured HTTPS endpoint; this response is not
a signed portable grant and is never persisted or reused by another sync.

## Sync and acceptance

Every run verifies current identity before fetching a private manifest, including
an ETag revalidation. A rotated credential for another principal is refused before
that content request. The credential is an owned snapshot, not an account ID.
Bearer headers go only to the configured identity endpoint and manifest origin;
absolute entry URLs at another origin receive no bearer. Redirects stay disabled.

The root-signed `orch8.catalog.v1` envelope is independently checked against the
catalog's immutable principal/source/catalog/Ed25519 pin. V1 is a complete catalog
snapshot: absent cached names are withdrawn through owner-qualified storage.
Active entries and explicit removed names must be disjoint. A manifest permits
at most 1,000 entries, 1,000 removal records and 1,000 signing keys, within the
existing five MiB wire-body cap.

Acceptance atomically records the key set, original envelope digest and each
published workflow name/version/hash before any download. Failed downloads,
reopen and cache eviction cannot allow a changed body under the same workflow
version or a per-workflow version rollback. Publish a newer workflow version to
roll back its behavior. Accepted history stays qualified by account/catalog.

Unavailable or invalid entries leave evaluation pending while later valid entries
can install. Pending runs cannot advance the applied cursor or ETag. SDK/handler
compatibility skips retain the evaluation fingerprint, so an environment change
fetches/evaluates the document again. Metadata reads use pages/point lookups rather
than materializing workflow bodies. Definitions normalize remote IDs/tenant fields
after verifying their original bytes.

Identity lifetime bounds private requests, response reads, retries and sleeps
with a monotonic deadline. Cancellation/expiry stops future phases. Dispatched
SQLite writes settle under their durable epoch fence before an owned run releases
sync admission. Per-engine coordination admits two runs total and one per account;
account slot identity survives session retirement/reopen. A late identity response
cannot fetch private content after retirement.

## Remaining integration

The network and storage path is internal and tested with actual HTTP, publisher
signatures and SQLite. Production backend integration, public handles, instance
and handler ownership, node/delegation scope, legacy migration, persisted public
access policy and native artifact/platform validation remain open. Total cached
bytes/history limits and per-account physical foreign-callback admission must
also be connected before public rollout. The sync coordinator's operation slots
do not establish those callback or storage bounds.
