# Orch8 progressive quick starts

This folder is a tutorial ladder: start with one local workflow, then add data,
durability, application code, recovery, and production release controls. Each
level ends with a working result and introduces one major concept.

If this is your first time using Orch8, follow the levels in order. If you
already know workflow engines, use the entry-point table to start at the first
unfamiliar concept.

## The learning path

| Level | Time | You add | You finish with |
|---|---:|---|---|
| [1. Run one local workflow](01-local-workflow.md) | 10 min | Sequence, block, handler, instance | A completed workflow without a server |
| [2. Pass data and choose a route](02-data-and-routing.md) | 20 min | Context, templates, outputs, router, input schema | One workflow that behaves differently for two inputs |
| [3. Run the durable API server](03-durable-api.md) | 25 min | SQLite persistence, authentication, CLI/API operations | A workflow that survives outside the development process |
| [4. Execute application code in a worker](04-external-worker.md) | 30 min | Worker polling, completion, outputs | A Node.js handler running outside the engine |
| [5. Observe failure and recover](05-failure-and-recovery.md) | 30 min | Retry policy, DLQ, diagnosis, manual retry | A failed instance recovered without recreating it |
| [6. Ship a guarded release](06-production-release.md) | 40 min | PostgreSQL, preflight, immutable versions, canary gates | A candidate version evaluated before promotion |

The sequence is deliberate:

```text
local evaluation
    -> data flow
    -> durable service
    -> external application code
    -> operational recovery
    -> production change control
```

## Choose your entry point

- You want to see Orch8 run immediately: start at [Level 1](01-local-workflow.md).
- You already ran `orch8 init`: start at [Level 2](02-data-and-routing.md).
- You know the sequence JSON model and need the service API: start at
  [Level 3](03-durable-api.md).
- You are integrating an existing service: start at
  [Level 4](04-external-worker.md).
- You are evaluating operations or reliability: start at
  [Level 5](05-failure-and-recovery.md).
- You are preparing a production deployment: complete Levels 3–5, then use
  [Level 6](06-production-release.md).

## What the guides assume

Levels 1–3 require:

- Rust 1.97 or newer when building from source.
- `jq`, `curl`, and a POSIX-like shell.
- A checkout of this repository, with commands run from its root unless a guide
  explicitly changes directory.

Level 4 also requires Node.js 18 or newer. Level 6 requires Docker with the
Compose plugin.

Build the two binaries once before starting:

```bash
cargo build --release --bin orch8 --bin orch8-server
export PATH="$PWD/target/release:$PATH"
orch8 --version
orch8-server --version
```

Downloaded release binaries work as well. The guides use binaries from `PATH`
so the remaining commands are the same for both installation methods.

## Conventions used throughout

Every server-based guide uses the canonical API prefix:

```bash
export ORCH8_URL=http://127.0.0.1:8080/api/v1
export ORCH8_TENANT_ID=demo
```

The CLI reads `ORCH8_URL`, `ORCH8_API_KEY`, and `ORCH8_TENANT_ID`. Direct HTTP
requests send the same API key and tenant in `x-api-key` and `x-tenant-id`.

Tutorial files are placed under a disposable `quickstart-work/` directory.
Delete that directory when finished. The repository source is not modified by
the tutorial commands.

## How to know you are ready for the next level

Do not advance merely because the commands exited successfully. At the end of
each level, check that you can explain its small mental model:

1. A **sequence** is an immutable, versioned workflow definition.
2. A **block** supplies workflow structure; a step block invokes a **handler**.
3. An **instance** is one execution of one sequence version.
4. `context.data` is run input; `outputs.<block-id>` contains completed block
   output; templates connect them.
5. The engine owns durable state and scheduling; workers own application code.
6. A retry changes an attempt. A release changes the sequence version selected
   for new instances.

## After this path

The six levels teach the engine's narrow critical path. The independent
[topic quick starts](topics/README.md) begin from a running Level 3 server and
solve a specific adjacent job without extending the learning ladder:

- schedule recurring runs;
- accept signed inbound webhooks;
- deliver signed lifecycle webhooks;
- add human approval gates;
- generate typed dataflow bindings;
- externalize large payloads;
- run Activepieces actions;
- connect the dashboard and Prometheus metrics; and
- generate disposable local load.

Continue with the reference documentation when you need the complete contract:

- [Sequence model and all block types](../SEQUENCES.md)
- [Typed dataflow compiler](../TYPED_DATAFLOW.md)
- [External worker protocol](../WORKERS.md)
- [Safe release operations](../RELEASES.md)
- [Deployment options and production checklist](../DEPLOYMENT.md)
- [Live OpenAPI](http://localhost:8080/swagger-ui) from a running server
