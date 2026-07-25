# Governed execution wakes

`orch8-push::governance` defines the vendor-independent wake boundary.

- Credential routes use an exact tenant/application/topic tuple and return
  only an encrypted credential record id. Workflow payloads cannot select a
  credential.
- Wake metadata is Ed25519 signed over tenant, device, command, nonce, issue
  time, expiry, and key id. TTL is at most 15 minutes, nonce replay fails, and
  workflow state/context never enters the vendor payload.
- Collapse keys bind tenant, device, execution, and topic. Only the newest
  command in that exact scope survives; different executions are preserved.
- Provider invalid-token outcomes terminally park the wake and deactivate plus
  clear the matching tenant/device token. `TokenLifecycleState` exposes the
  quarantine reason and supports explicit reactivation after registration.

Provider acceptance is still not device acknowledgement. Push remains a hint:
devices must fetch the durable command and report its acknowledgement.

