# Acknowledge a legacy worker completion

Use this control-plane operation when your app/backend has independently
confirmed a held worker task's completion and the task has no retained
authenticated issuer. Call it from the backend with root or a same-tenant
Operator key. Browser/device sessions and Worker keys cannot approve reports;
unsecured mode cannot identify an approver.

Persist a fresh, non-nil UUID as the approval ID before sending the request.
Keep the exact task, worker, epoch, evidence reference and output for retries.
An approval ID permanently identifies one report and cannot be reused for
another task. The evidence reference is an opaque backend/provider
acknowledgement locator, not raw evidence or credentials (nonblank, NUL-free,
at most 256 UTF-8 bytes).

```http
POST /api/v1/workers/tasks/019abcde-0000-7000-8000-000000000001/reconcile-completion
X-Api-Key: <backend-operator-key>
Content-Type: application/json

{
  "worker_id": "original-worker-label",
  "claim_epoch": 7,
  "approval_id": "019abcde-0000-7000-8000-000000000002",
  "evidence_reference": "backend-ack-123",
  "output": {"provider_receipt_id": "provider-confirmed-123", "result": "ok"}
}
```

Use the actual task UUIDs; the UUID strings above are illustrative. A successful
response is `202 Accepted` with the retained task ID, acceptance time and
`decision`. The decision identifies the authenticated approver, exact claim,
logical output digest, evidence reference and original history classification
(`legacy`, `missing`, or `unsecured`). It does not rename that approver as the
original issuer. Epoch-zero history stays epoch zero.

The decision, accepted output, effect settlement and completion event commit
in one transaction. A stored key must still be an active Operator after its
key lock is acquired. An expired/revoked key cannot approve a new result; later
key revocation does not erase a fact already accepted under valid authority.

Retry the identical request after a lost response. The retained decision is
returned even after hot task/native history cleanup. Changed output, reference,
approver or approval ID conflicts with the original decision. A first-time
acceptance needs the held task and owning native instance; existing effect
bindings must still match. A changed lease or
existing authenticated issuer returns a conflict; authenticated workers use
their ordinary completion route. Existing accepted outcomes without an operator
decision cannot be rewritten through this endpoint.

`202` confirms durable evidence acceptance. The existing recovery pump applies
the result only under the retained native run, activation and ownership fences.
A deferred or superseded native execution is not advanced by approving a fact.
This operation grants no continuing heartbeat, release, reconnect or artifact
rights and does not provide a credential handoff.

PostgreSQL migration 141 / SQLite schema 71 introduce a distinct reconciled
outcome codec and permanent approval-ID index. Older readers must not silently
discard decision metadata; unsafe downgrade is refused. The complete mixed-version
coordinator write floor and broader legacy-report reconciliation remain separate
rollout requirements in the active review.
