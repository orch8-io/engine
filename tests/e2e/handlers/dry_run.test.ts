/**
 * Dry-run mode — any sequence can be run with `dry_run: true` at create time.
 *
 * Side-effecting steps (here: `tool_call`) skip their real effect and return a
 * stub tagged `dry_run: true`; control flow and templating still run. This test
 * proves the external HTTP call is NOT made in dry-run, and IS made in a normal
 * run of the very same sequence (the flag is per-run, not per-sequence).
 */
import { describe, it, before, after } from "node:test";
import assert from "node:assert/strict";
import { createServer, type IncomingMessage, type ServerResponse } from "node:http";
import type { AddressInfo } from "node:net";
import { Orch8Client, testSequence, step, uuid } from "../client.ts";
import { startServer, stopServer } from "../harness.ts";
import type { ServerHandle } from "../harness.ts";

const client = new Orch8Client();

interface CallMock {
  baseUrl: string;
  hits: () => number;
  close(): Promise<void>;
}

function startCallMock(): Promise<CallMock> {
  return new Promise((resolvePromise) => {
    let count = 0;
    const server = createServer((req: IncomingMessage, res: ServerResponse) => {
      count += 1;
      res.setHeader("Content-Type", "application/json");
      res.end(JSON.stringify({ ok: true, echoed: req.url }));
    });
    server.listen(0, "127.0.0.1", () => {
      const port = (server.address() as AddressInfo).port;
      resolvePromise({
        baseUrl: `http://127.0.0.1:${port}`,
        hits: () => count,
        close: () => new Promise<void>((r) => server.close(() => r())),
      });
    });
  });
}

describe("Dry-run mode (per-instance)", () => {
  let server: ServerHandle | undefined;
  let mock: CallMock | undefined;

  before(async () => {
    server = await startServer();
    mock = await startCallMock();
  });

  after(async () => {
    if (mock) await mock.close();
    await stopServer(server);
  });

  it("dry_run=true skips the real tool_call and returns a stub", async () => {
    const tenantId = `dry-${uuid().slice(0, 8)}`;
    const seq = testSequence(
      "dry-run-skip",
      [
        step("call", "tool_call", {
          url: `${mock!.baseUrl}/run`,
          method: "POST",
          tool_name: "search",
          arguments: { q: "rust" },
        }),
      ],
      { tenantId },
    );
    await client.createSequence(seq);

    const before = mock!.hits();
    const { id } = await client.createInstance({
      sequence_id: seq.id,
      tenant_id: tenantId,
      namespace: "default",
      dry_run: true,
    });
    await client.waitForState(id, "completed", { timeoutMs: 20_000 });

    assert.equal(mock!.hits(), before, "dry-run must not make the HTTP call");

    const out = (await client.getOutputs(id)).find((o) => o.block_id === "call")!.output as {
      dry_run: boolean;
      handler: string;
      tool_name: string;
      result: unknown;
      would: { url: string; method: string };
    };
    assert.equal(out.dry_run, true);
    assert.equal(out.handler, "tool_call");
    assert.equal(out.tool_name, "search");
    assert.equal(out.result, null);
    assert.equal(out.would.method, "POST");
  });

  it("the same sequence run normally DOES make the call", async () => {
    const tenantId = `wet-${uuid().slice(0, 8)}`;
    const seq = testSequence(
      "dry-run-control",
      [
        step("call", "tool_call", {
          url: `${mock!.baseUrl}/run`,
          method: "POST",
          tool_name: "search",
          arguments: { q: "rust" },
        }),
      ],
      { tenantId },
    );
    await client.createSequence(seq);

    const before = mock!.hits();
    const { id } = await client.createInstance({
      sequence_id: seq.id,
      tenant_id: tenantId,
      namespace: "default",
      // dry_run omitted → defaults to false
    });
    await client.waitForState(id, "completed", { timeoutMs: 20_000 });

    assert.equal(mock!.hits(), before + 1, "a normal run must make the HTTP call");

    const out = (await client.getOutputs(id)).find((o) => o.block_id === "call")!.output as {
      dry_run?: boolean;
      result: { ok: boolean };
    };
    assert.notEqual(out.dry_run, true);
    assert.equal(out.result.ok, true);
  });

  it("dry_run_auto_approve flows past a human_review gate to completion", async () => {
    const tenantId = `dry-aa-${uuid().slice(0, 8)}`;
    const seq = testSequence(
      "dry-run-auto-approve",
      [
        step("gate", "human_review", {}, {
          wait_for_input: {
            prompt: "Approve?",
            choices: [
              { label: "Yes", value: "yes" },
              { label: "No", value: "no" },
            ],
          },
        }),
        step("after", "tool_call", {
          url: `${mock!.baseUrl}/run`,
          method: "POST",
          tool_name: "post_gate",
        }),
      ],
      { tenantId },
    );
    await client.createSequence(seq);

    const before = mock!.hits();
    const { id } = await client.createInstance({
      sequence_id: seq.id,
      tenant_id: tenantId,
      namespace: "default",
      dry_run: true,
      dry_run_auto_approve: true,
    });
    // Must NOT hang at 'waiting' — the gate auto-resolves and the post-gate
    // step runs (as a dry-run stub, so no real HTTP call).
    await client.waitForState(id, "completed", { timeoutMs: 20_000 });
    assert.equal(mock!.hits(), before, "post-gate tool_call must be a dry-run stub (no HTTP)");

    const outputs = await client.getOutputs(id);
    const gate = outputs.find((o) => o.block_id === "gate")!.output as { value: string };
    assert.equal(gate.value, "yes", "gate auto-approved with the default choice");
    const after = outputs.find((o) => o.block_id === "after")!.output as { dry_run: boolean };
    assert.equal(after.dry_run, true, "post-gate step executed in dry-run");
  });

  it("dry_run without auto_approve still pauses at the gate", async () => {
    const tenantId = `dry-gate-${uuid().slice(0, 8)}`;
    const seq = testSequence(
      "dry-run-gate-waits",
      [
        step("gate", "human_review", {}, {
          wait_for_input: { prompt: "Approve?" },
        }),
      ],
      { tenantId },
    );
    await client.createSequence(seq);

    const { id } = await client.createInstance({
      sequence_id: seq.id,
      tenant_id: tenantId,
      namespace: "default",
      dry_run: true, // auto_approve omitted
    });
    const inst = await client.waitForState(id, "waiting", { timeoutMs: 15_000 });
    assert.equal(inst.state, "waiting", "dry-run alone keeps the human gate");
  });
});
