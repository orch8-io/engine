/**
 * SSE Streaming — verifies that `GET /instances/{id}/stream` emits block
 * outputs in real time as the instance progresses and closes on terminal
 * state. Closes the gap around live observability for long-running instances.
 *
 * Event schema (`orch8-api/src/streaming.rs`):
 *   - `state`  — on each instance state change (JSON: { instance_id, state }).
 *   - `output` — per new BlockOutput (full serialised BlockOutput).
 *   - `done`   — terminal state reached; server closes the stream after this.
 */
import { describe, it, before, after } from "node:test";
import assert from "node:assert/strict";
import { Orch8Client, testSequence, step, uuid } from "../client.ts";
import { startServer, stopServer } from "../harness.ts";
import type { ServerHandle } from "../harness.ts";

const client = new Orch8Client();

interface SseFrame {
  event?: string;
  data: string;
}

/** Parse SSE frames from a ReadableStream<Uint8Array>. */
async function* parseSse(
  stream: ReadableStream<Uint8Array>,
): AsyncGenerator<SseFrame, void, void> {
  const reader = stream.getReader();
  const decoder = new TextDecoder();
  let buffer = "";
  try {
    while (true) {
      const { done, value } = await reader.read();
      if (done) return;
      buffer += decoder.decode(value, { stream: true });
      // Frames are separated by double newlines per the SSE spec.
      let sep = buffer.indexOf("\n\n");
      while (sep !== -1) {
        const raw = buffer.slice(0, sep);
        buffer = buffer.slice(sep + 2);
        let event: string | undefined;
        const dataLines: string[] = [];
        for (const line of raw.split("\n")) {
          if (line.startsWith(":")) continue; // keepalive comment
          if (line.startsWith("event:")) event = line.slice(6).trim();
          else if (line.startsWith("data:")) dataLines.push(line.slice(5).trim());
        }
        if (dataLines.length > 0) {
          const frame: SseFrame = { data: dataLines.join("\n") };
          if (event !== undefined) frame.event = event;
          yield frame;
        }
        sep = buffer.indexOf("\n\n");
      }
    }
  } finally {
    reader.releaseLock();
  }
}

describe("SSE Streaming", () => {
  let server: ServerHandle | undefined;

  before(async () => {
    server = await startServer();
  });

  after(async () => {
    await stopServer(server);
  });

  it("pushes state, output, and done events over GET /instances/{id}/stream", async () => {
    const tenantId = `sse-${uuid().slice(0, 8)}`;
    const seq = testSequence(
      "sse-basic",
      [
        step("s1", "log", { message: "one" }),
        step("s2", "log", { message: "two" }),
        step("s3", "log", { message: "three" }),
      ],
      { tenantId },
    );
    await client.createSequence(seq);

    const { id } = await client.createInstance({
      sequence_id: seq.id,
      tenant_id: tenantId,
      namespace: "default",
    });

    // Connect with a short poll interval so we don't wait 500ms per tick.
    const res = await fetch(`${client.baseUrl}/instances/${id}/stream?poll_ms=100`, {
      headers: { Accept: "text/event-stream" },
    });
    assert.ok(res.ok, `stream request failed: ${res.status}`);
    assert.ok(res.body, "stream response must have a body");

    const events: SseFrame[] = [];
    const deadline = Date.now() + 15_000;
    for await (const frame of parseSse(res.body)) {
      events.push(frame);
      if (frame.event === "done") break;
      if (Date.now() > deadline) {
        throw new Error(`timed out waiting for 'done' event; got ${events.length} events`);
      }
    }

    // Must have seen a state change AND at least one block output AND a done.
    const kinds = events.map((e) => e.event);
    assert.ok(kinds.includes("state"), "expected at least one 'state' event");
    const outputEvents = events.filter((e) => e.event === "output");
    assert.ok(outputEvents.length >= 3, `expected >=3 'output' events, got ${outputEvents.length}`);
    assert.equal(kinds[kinds.length - 1], "done", "last event must be 'done'");

    // Done payload reflects a terminal state.
    const doneBody = JSON.parse(events[events.length - 1]!.data) as { state: string };
    assert.ok(
      ["completed", "failed", "cancelled"].includes(doneBody.state),
      `done state must be terminal, got ${doneBody.state}`,
    );
  });
});
