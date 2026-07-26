# API entitlements and generated-client gate

`PlanEntitlements` is provider-neutral: plans contain only Orch8 limits,
namespace grants, and feature names. Single and batch instance creation check
context bytes, batch size, namespace, and tenant active-instance count before
writing. Invalid plan configuration fails closed. Self-managed deployments use
the explicit unlimited provider and do not depend on a payment service.

`scripts/check-generated-clients.sh` regenerates representative Rust and
JavaScript clients from the live `ApiDoc`, compiles/runs both, then compares the
complete OpenAPI fingerprint with `orch8-api/openapi.sha256`. CI blocks missing core
operations, uncompilable output, and unreviewed contract drift.

Entitlements do not charge customers or reconcile invoices. The client gate is
representative rather than a promise that every external SDK exposes identical
ergonomics; the OpenAPI fingerprint remains the compatibility authority.
