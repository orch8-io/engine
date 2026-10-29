# Recorded gRPC reporters and reconnect selection

A real two-key transport regression reproduced gRPC heartbeat takeover of a
lease issued to a different key. Unary completion, failure, heartbeat and release
now check the immutable recorded issuer before mutation or accepted/resolution
receipt replay. Existing tenant, lease and native ownership fences remain.

Worker-stream completion/failure/heartbeat forward the authenticated actor in a
private internal request extension. Generic tenant-only requests remain separate;
forwarding a frame must not silently become an unsecured caller. Live stored-key
reauthorization still surrounds the stream and its operations. No actor field is
accepted from wire JSON/protobuf or client metadata.

Reconnect now queries held tasks by tenant, worker, requested handlers and
recorded principal before LIMIT. Filtering after the old page limit could both
disclose another principal's task and let older foreign rows hide the rightful
worker's own held task. The backend validates returned issuance against the
actual task/tenant before returning it; encrypted wrappers decrypt selected tasks
after filtering. Selection joins the current claim generation rather than any
historical claim for the task ID. Legacy/unattributed rows retain the rollout
compatibility behavior, not authenticated attribution.

SQLite compares decoded JSON key/type/value sets instead of serialized object
order. A regression on all four backends found missing versus null optional
browser device IDs also hid valid held tasks, despite typed ownership matching.
Lookup now uses the typed browser None semantics for that known optional field.
Mobile device IDs and unknown scope fields are not discarded. Explicit tests keep
phone A/B separate even when they share a runtime UUID. Issued-claim getters also
validate the indexed worker before comparing the supplied claim, so a corrupted
worker index cannot masquerade as missing historical provenance.

## Verification and remaining gates

The gRPC suite passes 159 unit and 22 real-transport cases, including rejection
of a second key's mutations/replay after hot cleanup and successful original
reporting. A reconnect test with max_in_flight=1 selects the second key's own
newer task rather than an older foreign task, then completes over the stream,
proving the original actor survives internal forwarding. The storage matrix
passes 56 cases across plain/encrypted SQLite/PostgreSQL, including handler/actor
filtering before the limit, changed claim generations, JSON reordering, optional
device fields, device isolation and corrupt-index rejection. The related HTTP
suite passes 413 cases after the getter changes.

All six in-process distributed API/scheduler cases and both real executor
process scenarios pass on SQLite and PostgreSQL. The cross-transport provenance
fixture previously claimed with an authenticated HTTP key but reported over
unauthenticated gRPC; the new guard correctly rejected it. This fixture now
runs authenticated gRPC and preserves the same stored worker key across HTTP
claim and gRPC completion. The separate negative two-key tests remain in place.
The hybrid scenarios launch rebuilt server/executor binaries, including routing
through a header-based front, process loss and draining.

Legacy/missing issuance still uses pre-journal compatibility. Durable explicit
backend/operator reconciliation and approved handoff/revocation must replace that
gap without stranding offline reports or inventing original ownership. The
complete database/coordinator write floor, claim-bound artifact reads, late facts,
acknowledged compaction, transported history/key/backup compatibility and the
remaining distributed/mobile/embedded ledger gates remain open. This work does
not authorize merging the draft.
