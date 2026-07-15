# Orch8 examples

Examples are grouped by the question they answer:

| Example | What it demonstrates | Run from |
|---|---|---|
| [Safe release](safe-release/README.md) | Immutable versions, preflight, semantic diff, historical validation, and a canary gate | repository root |
| [Email classifier](email-classifier/README.md) | Webhook ingestion, TypeScript worker steps, LLM classification, Activepieces, Slack, and Resend | `examples/email-classifier` |
| [iOS app](ios/) | Swift package integration and an observable mobile sequence | `examples/ios` |
| [Android app](android/) | Gradle integration for the mobile engine | `examples/android` |
| [Embedded Rust quick start](../orch8/examples/quickstart.rs) | In-process engine with no HTTP server | repository root |
| [Embedded Axum](../orch8/examples/embedded_axum.rs) | Mount Orch8 beside an application HTTP service | repository root |
| [Kill-resistant execution](../orch8/examples/kill_resistant.rs) | Durable recovery across process interruption | repository root |

For smaller copy-and-run JSON patterns, see [Agent patterns](../docs/agent-patterns/README.md)
and the built-in templates:

```bash
orch8 templates list
orch8 init ./demo --template approval-flow
```

