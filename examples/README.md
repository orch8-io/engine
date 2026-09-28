# Orch8 examples

Examples are grouped by the question they answer:

| Example | What it demonstrates | Run from |
|---|---|---|
| [Safe release](safe-release/README.md) | Immutable versions, preflight, semantic diff, historical validation, and a canary gate | repository root |
| [Portable agent product](portable-agent-product/README.md) | Placement policy, local worker wrapper, profile offer, conformance score, and OEM contract | repository root |
| [Email classifier](email-classifier/README.md) | Webhook ingestion, TypeScript worker steps, LLM classification, Activepieces, Slack, and Resend | `examples/email-classifier` |
| [Embed starter](create-orch8-embed/README.md) | Next.js SaaS app embedding runs, approvals and a step builder per customer (sub-tenant) with server-minted embed tokens; runs on a built-in mock engine | `examples/create-orch8-embed` |
| [iOS app](ios/) | Swift package integration and an observable mobile sequence | `examples/ios` |
| [Android app](android/) | Gradle integration for the mobile engine | `examples/android` |
| [Hybrid GPU executor](hybrid-gpu-executor/README.md) | Local Ollama/vLLM steps on your GPU host, placed by hardware capability, with a remote or local control plane | `examples/hybrid-gpu-executor` |
| [Offline edge store-and-forward](edge-store-and-forward/README.md) | Retail POS / field service: `edge` node on SQLite, embedded terminals with sync, idempotent forward to HQ | `examples/edge-store-and-forward` |
| [Durable functions in Node](durable-node/README.md) | `@orch8/engine-native`: in-process SQLite engine, JS handlers, resume after a crash | `examples/durable-node` |
| [Durable functions in Python](durable-python/README.md) | `orch8-engine-native`: in-process SQLite engine, Python handlers, resume after a crash | `examples/durable-python` |
| [Embedded Rust quick start](../orch8/examples/quickstart.rs) | In-process engine with no HTTP server | repository root |
| [Embedded Axum](../orch8/examples/embedded_axum.rs) | Mount Orch8 beside an application HTTP service | repository root |
| [Kill-resistant execution](../orch8/examples/kill_resistant.rs) | Durable recovery across process interruption | repository root |

For smaller copy-and-run JSON patterns, see [Agent patterns](../docs/agent-patterns/README.md)
and the built-in templates:

```bash
orch8 templates list
orch8 init ./demo --template approval-flow
```

## Portable agent continuity

Run the built-in protocol demonstration when you want to verify portable
execution without provisioning a server or attaching a physical device:

```bash
orch8 demo portable-agent
orch8 --output json demo portable-agent
```

This is not a mocked workflow template. It exports, signs, encrypts, imports,
activates, and returns one execution across isolated in-memory cloud/device
runtimes while checking ownership, trust, privacy, and redelivery invariants.
