# @orch8/workflow-sdk

Workflow-as-code SDK for the [orch8.io](https://orch8.io) durable task engine.
Build sequences in TypeScript, validate them with Zod, and deploy through the
orch8 REST API.

## Install

```bash
npm install @orch8/workflow-sdk
```

Requires Node 18+ (uses the global `fetch`).

## Quick start

```ts
import { workflow, Orch8Client } from "@orch8/workflow-sdk";

const onboarding = workflow("onboarding")
  .step("send_welcome", "send_email", { template: "welcome" })
  .delay({ duration: 24 * 60 * 60 * 1000 }) // 24h
  .step("day_1_nudge", "send_email", { template: "day_1" })
  .router(
    "check_activation",
    [
      {
        condition: "data.activated == true",
        blocks: (b) => b.step("celebrate", "send_email", { template: "welcome_aboard" }),
      },
    ],
    (b) => b.step("retarget", "add_to_list", { list: "unactivated" }),
  );

const client = new Orch8Client({
  baseUrl: "https://orch8.example.com",
  tenantId: "acme",
  apiKey: process.env.ORCH8_KEY,
});

await client.createSequence(onboarding);

await client.createInstance({
  sequence_name: "onboarding",
  context: { data: { user_id: "u123", email: "alice@example.com" } },
});
```

## Block types

| Method                 | Block                | Purpose                                                   |
| ---------------------- | -------------------- | --------------------------------------------------------- |
| `.step(...)`           | `step`               | A single unit of work (handler invocation).               |
| `.parallel(...)`       | `parallel`           | Run N branches concurrently; complete when all finish.    |
| `.race(...)`           | `race`               | Run N branches; complete on first resolve/success.        |
| `.loop(...)`           | `loop`               | Repeat body while expression is truthy.                   |
| `.forEach(...)`        | `for_each`           | Iterate over a context collection.                        |
| `.router(...)`         | `router`             | First matching condition wins; optional default branch.   |
| `.tryCatch(...)`       | `try_catch`          | Exception handling with optional finally.                 |
| `.subSequence(...)`    | `sub_sequence`       | Invoke another sequence as a child workflow.              |
| `.abSplit(...)`        | `ab_split`           | Deterministic weighted variant routing.                   |
| `.cancellationScope()` | `cancellation_scope` | Children cannot be cancelled until they complete.         |
| `.delay(spec)`         | *(step)*             | Convenience: insert a no-op step with a delay.            |
| `.raw(block)`          | *(any)*              | Escape hatch: append a pre-built `BlockDefinition`.       |

## Step options

```ts
wf.step("call_api", "http_request", { url }, {
  retry: { max_attempts: 3, initial_backoff: 1_000, max_backoff: 30_000 },
  timeout: 10_000,
  rate_limit_key: "third_party_api",
  queue_name: "high_priority",
  deadline: 60_000,
  on_deadline_breach: { handler: "notify_slack", params: { channel: "#ops" } },
});
```

All durations are in milliseconds.

## Validation

`build()` runs the payload through Zod before it leaves your process. Invalid
block shapes throw at build time, not on the server round-trip.

```ts
const wf = workflow("x");
wf.raw({ type: "step", id: "broken" } as any);
wf.build(); // throws ZodError
```

## License

BUSL-1.1 — same as the orch8 engine.
