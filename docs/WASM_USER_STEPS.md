# Running end-user WASM steps

> **Stability: beta**, shipped and tested; may change in a minor release with a changelog note.

This guide is for a SaaS that runs Orch8 and wants **its own end users** to
upload custom step logic as WebAssembly. The engine sandbox bounds CPU, memory,
wall-clock time and output size, and grants no host access. This page covers
how to accept a module, the limits and their defaults, the ABI, and what the
sandbox does and does not protect against.

Requires an `orch8-server` built with the `wasm` feature (the default server
build and the published image include it). The limits described here were
added after `0.7.1`. On older engines there is no wall-clock or output cap and
the limits are fixed constants. For the general plugin registry, see
[Sequences: WASM Plugin](SEQUENCES.md#wasm-plugin).

## How a SaaS accepts an end-user module

End users never talk to Orch8. Plugin registration (`POST`/`PATCH`/`DELETE
/plugins`) needs an Operator key, so your backend is the only party that can
register a module:

```
end user ──upload .wasm──▶ your backend ──validate──▶ $ORCH8_WASM_PLUGIN_DIR/<tenant>/<sha256>.wasm
                                          └──POST /plugins {name, source}──▶ Orch8
sequence step  { "handler": "wasm://<name>" }  ──▶ engine loads, validates, runs it in a fresh sandbox
```

1. **Cap the upload in your HTTP layer** at `ORCH8_WASM_MAX_MODULE_BYTES` or
   lower. For end-user modules we recommend 1–2 MiB. The engine re-checks the
   size before it reads the file.
2. **Validate before accepting.** If your backend is Rust, call
   `orch8_engine::handlers::wasm_plugin::validate_module_bytes(&bytes, &limits)`
   (with the `wasm` feature). It never runs guest code, and it checks:
   - the size cap and the `\0asm` binary magic (WAT text is refused);
   - that the module compiles under the sandbox engine configuration;
   - that the module declares **no imports**;
   - the exports `memory`, `alloc: (i32) -> i32` and `handle: (i32, i32) -> i64`;
   - that the declared initial memory fits `ORCH8_WASM_MAX_MEMORY_BYTES`.

   On failure it returns a reason you can show to the end user. From another
   language, apply the same checks with `wasm-tools validate` and
   `wasm-tools print`: the import list must be empty. You can also skip this
   step. The engine applies the import, size and magic checks again on every
   load and fails the step with a permanent error.

3. **Store content-addressed** under `ORCH8_WASM_PLUGIN_DIR`, for example
   `/var/lib/orch8/plugins/<tenant>/<sha256>.wasm`, and never overwrite a file
   in place. Set `ORCH8_WASM_PLUGIN_DIR` on every engine node. Every `source`
   must then resolve inside that directory after symlinks and `..` are
   resolved, so a registration can't point at `/etc/passwd` or at another
   tenant's directory.
4. **Register an alias.** Each tenant owns its own alias namespace. An
   instance resolves an enabled alias in its tenant, then falls back to an
   alias shared across tenants when the tenant has no registration of its own.

   ```bash
   curl -s -X POST "$ORCH8_URL/api/v1/plugins" \
     -H "x-api-key: $OPERATOR_KEY" -H "x-tenant-id: acme" -H "content-type: application/json" \
     -d '{"name":"t-acme-u-42-score","plugin_type":"wasm","tenant_id":"acme",
          "source":"/var/lib/orch8/plugins/acme/9f2c…e1.wasm"}'
   ```

5. **Reference it** from a step as `"handler": "wasm://t-acme-u-42-score"`. To
   replace a module, write a new content-addressed file and `PATCH` the
   plugin's `source`. The engine tracks canonical paths and file size/mtime,
   then looks up compiled code by content digest. Concurrent loads of
   identical files share one compilation, and later loads reuse the result
   while it is cached.
6. **Disable or delete** a module with `PATCH {"enabled": false}` or `DELETE`.
   Steps that reference a disabled or missing plugin fail.

## Shared artifact storage

Configure the same artifact backend on the API server and all executors.
Upload binary WASM with an authenticated tenant header and its lowercase
SHA-256 address.

```bash
digest="$(sha256sum score.wasm | cut -d ' ' -f 1)"
curl --fail -X PUT "$ORCH8_URL/api/v1/plugins/modules/$digest" \
  -H "x-api-key: $OPERATOR_KEY" -H "x-tenant-id: acme" \
  -H "content-type: application/wasm" --data-binary @score.wasm
```

Use the returned `wasm-artifact:sha256:<digest>` as the alias's `source`.
Executors fetch from the alias owner's artifact namespace and verify size
and digest before compilation. A new executor does not need a local plugin
directory. The artifact upload limit is 16 MiB. A smaller executor module
limit still applies. Configure encryption on the artifact backend for
bytecode stored in that backend.

## Limits and defaults

Every invocation gets a **fresh store**: no guest state survives between calls
or between tenants. The limits are process-wide and read once at startup from
the environment. A value that is zero or doesn't parse falls back to the
default with a warning, so a typo can never switch a limit off.

| Variable                      | Default             | What it bounds                                                                                                                                                                     | On breach                                                                                                                                       |
| ----------------------------- | ------------------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | ----------------------------------------------------------------------------------------------------------------------------------------------- |
| `ORCH8_WASM_FUEL`             | `10000000`          | Deterministic CPU budget: 1 fuel unit ≈ 1 Wasm instruction, roughly 50–200 ms of dense arithmetic                                                                                  | Permanent step error `fuel exhausted (cpu limit)`                                                                                               |
| `ORCH8_WASM_TIMEOUT_MS`       | `2000`              | Wall clock per invocation (`alloc` plus `handle`), enforced by epoch interruption with a 10 ms tick. It catches work that fuel undercounts, such as one `memory.fill` over 64 MiB. | Permanent `wall-clock timeout exceeded`                                                                                                         |
| `ORCH8_WASM_MAX_MEMORY_BYTES` | `67108864` (64 MiB) | Linear memory, both the declared initial size and every `memory.grow`                                                                                                              | `memory.grow` returns `-1` (spec behavior). A trap afterwards, or an oversized initial memory, is reported as permanent `memory limit exceeded` |
| — (fixed)                     | `10000`             | Function-table elements                                                                                                                                                            | Permanent `table limit exceeded`                                                                                                                |
| `ORCH8_WASM_MAX_MODULE_BYTES` | `33554432` (32 MiB) | Module file size, checked before the file is read or compiled                                                                                                                      | Permanent `failed to load module` (no path or contents are echoed)                                                                              |
| `ORCH8_WASM_MAX_OUTPUT_BYTES` | `4194304` (4 MiB)   | Size of the returned output, checked before it is parsed                                                                                                                           | Permanent `output of N bytes exceeds the limit`                                                                                                 |
| `ORCH8_WASM_PLUGIN_DIR`       | unset               | Directory that every module `source` must resolve inside                                                                                                                           | Permanent `failed to load module`                                                                                                               |
| `ORCH8_WASM_COMPILE_CONCURRENCY` | `2`              | Concurrent cold compilations, from 1 to 32 | New modules wait for a compiler slot |
| `ORCH8_WASM_COMPILE_QUEUE` | `16`                    | Waiting cold compilations, from 0 to 128 | Retryable `compilation queue is busy` |
| `ORCH8_WASM_COMPILE_QUEUE_MS` | `250`                | Maximum compiler queue wait in milliseconds, from 1 to 5000 | Retryable `compilation queue is busy` |

Set `ORCH8_WASM_TENANT_LIMITS_JSON` to a JSON object keyed by tenant ID to
apply tighter fuel, memory, or output ceilings. For example,
`{"north":{"fuel":2000000,"max_memory_bytes":16777216,"max_output_bytes":262144}}`.
Each provided value must be a positive integer. The executor takes the lower
of the tenant and process ceiling for each invocation. Unconfigured tenants
use the process ceilings. Malformed policy configuration rejects plugin
execution until the executor restarts with valid configuration.

Host access is not a knob. The linker is empty, and a module that declares
**any** import is refused before instantiation, so a start function never runs.
That covers every WASI function (`fd_*`, `path_open`, `sock_*`, `clock_*`,
`random_get`), component-model interfaces and custom `env` functions. There is
no filesystem, network, clock, randomness, environment or process access, and
there's no switch that grants it.

Every limit breach and every guest trap (`unreachable`, stack overflow,
division by zero, out-of-bounds access) is a **permanent** step error, because
retrying the same input on the same module reproduces it. Only non-trap host
errors are retryable. Wrap a user step in `try_catch` if the workflow should
continue after the user's code fails.

For end-user modules we suggest lowering the defaults, for example
`ORCH8_WASM_FUEL=2000000`, `ORCH8_WASM_TIMEOUT_MS=500`,
`ORCH8_WASM_MAX_MEMORY_BYTES=16777216` and `ORCH8_WASM_MAX_MODULE_BYTES=2097152`.

## ABI

The protocol is JSON in and JSON out over the module's linear memory.

| Export               | Type                        | Contract                                                                                                                                   |
| -------------------- | --------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------ |
| `memory`             | memory                      | The module's linear memory.                                                                                                                |
| `alloc`              | `(i32 size) -> i32 ptr`     | Return a pointer to `size` writable bytes. The host checks that `ptr >= 0` and `ptr + size <= memory size`.                                |
| `handle`             | `(i32 ptr, i32 len) -> i64` | Read the input JSON at `ptr..ptr+len`, and return `(out_ptr << 32) \| out_len`. The host bounds-checks the output range and caps its size. |
| `dealloc` (optional) | `(i32 ptr, i32 len) -> ()`  | Called with the output range after it has been read. A failure is logged and otherwise ignored.                                            |

The input JSON document is:

```json
{
  "instance_id": "…",
  "block_id": "…",
  "attempt": 0,
  "params": { "…": "step params after template resolution" },
  "context": { "data": {}, "config": {} }
}
```

The output must be JSON. Anything else doesn't fail the step. It is stored as
`{"_wasm_plugin_error": "invalid_json_output", "raw": "<lossy utf-8>"}`, so
branch on that key if you need to.

The module sees **everything in `params`, `context.data` and `context.config`
of the instance that runs it**. The sandbox isolates the host from the module.
It does not decide what data you hand to the module. Never place another end
user's data, secrets or `credentials://` material into an instance that runs a
user module.

## Threat model

**Assets:** the engine host (CPU, RAM, disk, network position, credentials in
its environment), other tenants' workflows and data, the shared database, and
the availability of the scheduler.

**Attacker:** a malicious end user of your SaaS who controls the bytes of one
or more modules, and the inputs that reach them through their own workflows.
They do not have an Orch8 API key.

### What the sandbox guarantees

- **No host capabilities.** There are no imports, so the guest has no
  syscalls, files, sockets, clock, randomness or environment. Its only effect
  is the JSON it returns.
- **Memory safety at the boundary.** Every guest pointer (`alloc` result,
  output range) is checked against linear memory before the host touches it.
  A lying guest gets a permanent error. It can't crash or read out of the
  executor.
- **Bounded resources per call:** fuel, wall-clock time, linear memory,
  tables, module size and output size, as listed above. The executor thread is
  released when any limit trips.
- **No state carry-over.** Each invocation runs in a fresh `Store` and
  instance, so one call (or one tenant) can't leave data for the next.
- **Path confinement.** With `ORCH8_WASM_PLUGIN_DIR` set, a registration can't
  make the engine read files outside that directory, and load errors never echo
  paths or file contents.
- **Tenant-scoped resolution.** `wasm://name` resolves only among the running
  instance's own tenant's plugins.

### What it does not guarantee

- **Side channels.** Wasmtime mitigates Spectre-style attacks (bounds-checked
  or guard-page memory, indirect-call checks), but co-resident timing and
  cache side channels against other work in the same process aren't ruled out.
  If a module must not share a CPU with other tenants' secrets, run it in a
  separate process or machine.
- **Compilation cost DoS.** Modules are compiled with Cranelift on **first
  use** inside the executor, and compilation isn't metered by fuel or the
  wall-clock limit. Cold modules share a separate bounded compiler queue. A
  cached module uses no compiler slot. A module that takes a long time to compile can use
  seconds of CPU and hundreds of MiB of RAM per compile. Mitigations: a small
  `ORCH8_WASM_MAX_MODULE_BYTES`, compiling at upload time on a separate box
  (`validate_module_bytes` compiles), and rate-limiting uploads per user.
- **Cache churn.** The compiled-module cache retains up to 256 modules after
  maintenance and evicts individual least-recently-used entries. Set
  `ORCH8_WASM_TENANT_CACHE_SLOTS_JSON` on each executor to reserve entries for
  particular tenants, for example `{"north":32,"south":16}`. Each reservation
  retains only that tenant's least-recently-used working set. The reservations
  and the shared cache fit within 256 entries, with at least one slot left
  shared. Tenants without a reservation use the shared cache.
- **Concurrency exhaustion.** Each invocation occupies a Tokio blocking thread
  for up to `ORCH8_WASM_TIMEOUT_MS`. Many concurrent hostile invocations can
  crowd out other blocking work on the node. Bound it with the engine's
  concurrency settings and per-tenant rate limits, and isolate as described
  below.
- **Wasmtime vulnerabilities.** The sandbox is only as strong as the linked
  Wasmtime release (`wasmtime` 48 at the time of writing). Track
  [Wasmtime security advisories](https://github.com/bytecodealliance/wasmtime/security/advisories)
  and upgrade Orch8 promptly. A sandbox escape would give the attacker the
  executor's privileges.
- **Output content.** Output is size-capped and must parse as JSON, but it is
  attacker-controlled data. Downstream steps, templates and your UI must treat
  it as untrusted: escape it when rendering, and never `eval` it or feed it to a
  shell.
- **Determinism.** With no clock or randomness imports, a guest's result is a
  function of its input. The exceptions: whether a call finishes under the
  wall-clock limit depends on load; NaN bit patterns aren't canonicalized; and a
  replaced module file changes behavior. Record the module digest (the
  content-addressed file name) if you need reproducibility.
- **Infrastructure after the step.** The sandbox bounds the call, not what
  your workflow does with the output (HTTP calls, emails, database writes).
  Keep those in steps you wrote.

### Recommended deployment

Treat end-user modules like untrusted code:

- **Run them on a dedicated execution pool.** WASM plugins execute in-process
  on whichever engine node (`all_in_one` or `executor`, see
  [Node roles](NODE_ROLES.md)) advances the instance. In this release an
  in-process plugin can't be pinned to one executor, so put tenants that run
  user modules on a **separate Orch8 deployment** (its own executor nodes and
  database) with no access to other tenants' data or credentials.
- Run those nodes as an unprivileged user (the image runs as the `orch8` system user) with a
  read-only root filesystem, no cloud metadata access, egress restricted to the
  database, and container memory and CPU limits above
  `ORCH8_WASM_MAX_MEMORY_BYTES` × expected concurrency.
- Mount `ORCH8_WASM_PLUGIN_DIR` read-only on engine nodes. Only your upload
  service writes to it.
- Lower the defaults as shown above, and alert on the permanent-error rate of
  `wasm://` steps per tenant.

## Verification

The limits are covered by execution tests that compile hostile modules and
assert each limit trips (`cargo test -p orch8-engine --features wasm --lib
wasm_plugin`):

| Test                                                                                           | Proves                                                                      |
| ---------------------------------------------------------------------------------------------- | --------------------------------------------------------------------------- |
| `infinite_loop_trips_wall_clock_timeout_when_fuel_is_ample`                                    | An infinite loop with unlimited fuel is interrupted by the wall-clock limit |
| `infinite_loop_trips_fuel_limit_with_configured_budget` / `fuel_exhaustion_is_permanent_error` | Fuel exhaustion traps and is permanent                                      |
| `memory_grow_beyond_cap_is_denied_and_reported`                                                | `memory.grow` past the cap is denied and reported as a memory-limit error   |
| `initial_memory_beyond_cap_fails_instantiation`                                                | An oversized declared memory never instantiates                             |
| `wasi_filesystem_and_socket_imports_are_rejected_before_instantiation`                         | WASI fs/socket/`env` imports are refused, and the start function never runs |
| `output_beyond_cap_is_rejected_before_parsing`                                                 | Oversized output is refused                                                 |
| `module_file_beyond_size_cap_is_not_loaded`                                                    | The module size cap is enforced at load and in the validator                |
| `validator_accepts_abi_conformant_module_and_rejects_shape_errors`                             | Upload validation of the ABI and memory shape                               |
| `limits_from_env_use_defaults_and_reject_zero_or_garbage`                                      | Configuration can't disable a limit by accident                             |
# Plugin alias scope

A plugin alias belongs to one tenant. Two tenants may register the same name
with different sources. A tenant's alias takes precedence over a shared alias
with the same name. Deleting or updating an alias affects only its owner.

Upgrade all processes that read the plugin registry before creating duplicate
aliases on a shared PostgreSQL database. Rolling back the database migration
requires resolving duplicate names first.


## Module pins at publication

When publishing a sequence, the server resolves enabled artifact-backed
WASM aliases to their verified SHA-256 addresses. The revision stores the
address as `wasm://wasm-artifact:sha256:<digest>` and uses the publishing
tenant's artifact namespace. Publishing after an alias update selects the
new digest, while existing revisions continue reading their original
module. Node-local path registrations still resolve on the executor.
## Release prewarming

On each designated native executor, set `ORCH8_WASM_PREWARM_REVISIONS`
to the comma-separated UUIDs of the candidate sequence revisions. The
executor resolves and verifies their pinned artifacts, checks the module
ABI and tenant memory limit, and compiles the modules into its local cache.
It renews candidate readiness for a short window while these checks pass.

Send `ready_executor_ids` with the canary or promotion request to require
current acknowledgements from those cluster nodes. The API returns a conflict
until every named node has prepared the exact candidate revision. A draining node,
expired acknowledgement, or acknowledgement for a different revision
fails the check. Local schedulers also check pins before advancing a run.

## Shared artifact collection

Post to `/api/v1/plugins/modules/gc` for a tenant to list unreferenced
shared module digests. The default grace period is one day. Set
`grace_seconds` and `limit` to adjust the age threshold and batch size,
or set `apply: true` to record when an unreferenced module was first observed
and remove modules that have remained unreferenced through the grace period.

The collector preserves modules referenced by installed revisions,
plugin aliases and retained runs with injected blocks. This keeps old
digests available while their revisions remain installed. A short
database write fence prevents new references from racing retirement.
Re-upload the same digest before reintroducing a retired reference.
