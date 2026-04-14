/**
 * End-to-end approvals inbox flow.
 *
 * Proves the full stack behind the HITL dashboard:
 *   1. Author a sequence with a `wait_for_input` step carrying choices.
 *   2. Start an instance; scheduler drives it to `waiting`.
 *   3. `GET /approvals` returns the item with the author's prompt + choices.
 *   4. `POST /instances/{id}/signals` with `Custom: "human_input:{block_id}"`
 *      and payload `{value}` resumes the instance.
 *   5. Instance completes and `/approvals` is empty again.
 *
 * Signal wire shape: `signal_type: { custom: "human_input:<block_id>" }` —
 * the serde representation of `SignalType::Custom(String)`. The client's
 * `sendSignal(id, signalType, payload)` accepts a string, so we cast the
 * Custom variant through `unknown` the same way
 * `human_in_the_loop_scenarios.test.ts` does.
 */
import { describe, it, before, after } from "node:test";
import assert from "node:assert/strict";
import { Orch8Client, testSequence, step } from "../client.ts";
import { startServer, stopServer } from "../harness.ts";
import type { ServerHandle } from "../harness.ts";
import type { Block } from "../client.ts";

interface ApprovalChoice {
  label: string;
  value: string;
}

interface ApprovalItem {
  instance_id: string;
  tenant_id: string;
  namespace: string;
  sequence_id: string;
  sequence_name: string;
  block_id: string;
  prompt: string;
  choices: ApprovalChoice[];
  store_as: string | null;
  timeout_seconds: number | null;
  escalation_handler: string | null;
  waiting_since: string;
  deadline: string | null;
  metadata: unknown;
}

interface ApprovalsResponse {
  items: ApprovalItem[];
  total: number;
}

async function getApprovals(baseUrl: string): Promise<ApprovalsResponse> {
  const res = await fetch(`${baseUrl}/approvals`);
  if (!res.ok) {
    throw new Error(`GET /approvals failed: ${res.status} ${await res.text()}`);
  }
  return (await res.json()) as ApprovalsResponse;
}

describe("Approvals inbox", () => {
  let server: ServerHandle | undefined;

  before(async () => {
    server = await startServer();
  });

  after(async () => {
    await stopServer(server);
  });

  it("lists a waiting instance and resumes it after signal", async () => {
    const client = new Orch8Client(`http://localhost:${server!.port}`);

    // `/approvals` only lists instances in `state=Waiting`, which requires
    // the tree-evaluator code path (single-step flat sequences stay
    // Running/Scheduled through a Deferred outcome and never write an
    // execution tree). Wrapping the review step in a single-branch
    // `parallel` block forces `process_instance_tree`, which flips the
    // instance to Waiting when the node sits in `NodeState::Waiting`.
    const reviewStep = step(
      "review",
      "human_review",
      { prompt: "Approve?" },
      {
        wait_for_input: {
          prompt: "Approve?",
          choices: [
            { label: "Approve", value: "approve" },
            { label: "Reject", value: "reject" },
          ],
          store_as: "decision",
        },
      },
    );
    const seq = testSequence("appr-e2e", [
      {
        type: "parallel",
        id: "review_wrap",
        branches: [[reviewStep]],
      } as Block,
    ]);
    await client.createSequence(seq);
    const { id } = await client.createInstance({
      sequence_id: seq.id,
      tenant_id: seq.tenant_id,
      namespace: seq.namespace,
    });

    // Scheduler runs the step, hits wait_for_input, flips instance to waiting.
    await client.waitForState(id, "waiting", { timeoutMs: 10_000 });

    // /approvals filtered to this tenant/namespace to avoid cross-suite noise.
    const inboxUrl =
      `${client.baseUrl}/approvals` +
      `?tenant_id=${encodeURIComponent(seq.tenant_id)}` +
      `&namespace=${encodeURIComponent(seq.namespace)}`;
    const res = await fetch(inboxUrl);
    assert.ok(res.ok, `GET /approvals returned ${res.status}`);
    const approvals = (await res.json()) as ApprovalsResponse;

    // Find the item for *this* instance — other leftovers in the same
    // tenant/ns would break a blind `items[0]` lookup.
    const item = approvals.items.find((i) => i.instance_id === id);
    assert.ok(item, `instance ${id} not found in /approvals; got ${JSON.stringify(approvals)}`);
    assert.equal(item.block_id, "review");
    assert.equal(item.prompt, "Approve?");
    assert.equal(item.store_as, "decision");
    assert.equal(item.choices.length, 2);
    assert.equal(item.choices[0]!.label, "Approve");
    assert.equal(item.choices[0]!.value, "approve");
    assert.equal(item.choices[1]!.label, "Reject");
    assert.equal(item.choices[1]!.value, "reject");
    assert.equal(item.sequence_name, seq.name);

    // Resume via signal. Custom variant is `{ custom: "..." }`; cast per
    // the existing HITL test pattern.
    await client.sendSignal(
      id,
      { custom: "human_input:review" } as unknown as string,
      { value: "approve" },
    );

    const done = await client.waitForState(id, "completed", { timeoutMs: 10_000 });
    assert.equal(done.state, "completed");
    const ctx = (done.context ?? {}) as Record<string, unknown>;
    const data = (ctx.data ?? {}) as Record<string, unknown>;
    assert.equal(data.decision, "approve");

    // After resolution the instance must no longer appear in the inbox.
    const afterRes = await fetch(inboxUrl);
    assert.ok(afterRes.ok, `GET /approvals (post-resolve) returned ${afterRes.status}`);
    const afterApprovals = (await afterRes.json()) as ApprovalsResponse;
    const stillThere = afterApprovals.items.find((i) => i.instance_id === id);
    assert.equal(
      stillThere,
      undefined,
      `instance ${id} should be gone from /approvals after resume`,
    );
  });
});

// Silence unused-import warning for the helper kept in place for future
// inbox scenarios (filtering, tenant scoping, paging).
void getApprovals;
