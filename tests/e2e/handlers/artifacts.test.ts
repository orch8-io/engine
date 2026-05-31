/**
 * Durable artifacts + binary I/O on `tool_call`.
 *
 * The server is started with a local-filesystem artifact backend (a temp dir)
 * via `ORCH8_ARTIFACT_BACKEND=local`. A mock serves:
 *   - GET  /image  → binary bytes (image/png) to be captured as an artifact.
 *   - PUT  /upload → records the raw request body (to verify artifact upload).
 *
 * Covered:
 *   - `response_as: "artifact"` stores the response body and returns a ref.
 *   - full lifecycle: download → artifact → upload the same bytes downstream
 *     (the ref flows through `{{steps.dl.output.artifact}}`).
 */
import { describe, it, before, after } from "node:test";
import assert from "node:assert/strict";
import { createServer, type IncomingMessage, type ServerResponse } from "node:http";
import type { AddressInfo } from "node:net";
import { rmSync } from "node:fs";
import { Orch8Client, testSequence, step, uuid } from "../client.ts";
import { startServer, stopServer } from "../harness.ts";
import type { ServerHandle } from "../harness.ts";

const client = new Orch8Client();

const IMAGE_BYTES = Buffer.from([0x89, 0x50, 0x4e, 0x47, 0x0d, 0x0a, 0x01, 0x02, 0x03]);

interface ArtMock {
  baseUrl: string;
  uploaded: Buffer[];
  close(): Promise<void>;
}

function readRaw(req: IncomingMessage): Promise<Buffer> {
  return new Promise((resolve) => {
    const chunks: Buffer[] = [];
    req.on("data", (c: Buffer) => chunks.push(c));
    req.on("end", () => resolve(Buffer.concat(chunks)));
  });
}

function startArtMock(): Promise<ArtMock> {
  return new Promise((resolvePromise) => {
    const uploaded: Buffer[] = [];
    const server = createServer((req: IncomingMessage, res: ServerResponse) => {
      void (async () => {
        const path = req.url ?? "";
        if (path.includes("/image")) {
          res.setHeader("Content-Type", "image/png");
          res.end(IMAGE_BYTES);
          return;
        }
        if (path.includes("/upload")) {
          uploaded.push(await readRaw(req));
          res.setHeader("Content-Type", "application/json");
          res.end(JSON.stringify({ ok: true }));
          return;
        }
        res.statusCode = 404;
        res.end("{}");
      })();
    });
    server.listen(0, "127.0.0.1", () => {
      const port = (server.address() as AddressInfo).port;
      resolvePromise({
        baseUrl: `http://127.0.0.1:${port}`,
        uploaded,
        close: () => new Promise<void>((r) => server.close(() => r())),
      });
    });
  });
}

describe("Artifacts — durable binary I/O (local backend)", () => {
  let server: ServerHandle | undefined;
  let mock: ArtMock | undefined;
  const artifactDir = `/tmp/o8-artifacts-e2e-${uuid().slice(0, 8)}`;

  before(async () => {
    server = await startServer({
      env: { ORCH8_ARTIFACT_BACKEND: "local", ORCH8_ARTIFACT_PATH: artifactDir },
    });
    mock = await startArtMock();
  });

  after(async () => {
    if (mock) await mock.close();
    await stopServer(server);
    try {
      rmSync(artifactDir, { recursive: true, force: true });
    } catch {
      /* ignore */
    }
  });

  it("response_as=artifact captures the response body as a durable artifact", async () => {
    const tenantId = `art-dl-${uuid().slice(0, 8)}`;
    const seq = testSequence(
      "art-download",
      [
        step("dl", "tool_call", {
          url: `${mock!.baseUrl}/image`,
          method: "GET",
          response_as: "artifact",
        }),
      ],
      { tenantId },
    );
    await client.createSequence(seq);
    const { id } = await client.createInstance({
      sequence_id: seq.id,
      tenant_id: tenantId,
      namespace: "default",
    });
    await client.waitForState(id, "completed", { timeoutMs: 20_000 });

    const out = (await client.getOutputs(id)).find((o) => o.block_id === "dl")!.output as {
      artifact: { id: string; content_type: string; size: number; uri: string; key: string };
      status: number;
    };
    assert.equal(out.status, 200);
    assert.equal(out.artifact.content_type, "image/png");
    assert.equal(out.artifact.size, IMAGE_BYTES.length);
    assert.match(out.artifact.uri, /^artifact:\/\//);
  });

  it("downloads to an artifact then uploads the same bytes downstream", async () => {
    const tenantId = `art-rt-${uuid().slice(0, 8)}`;
    const before = mock!.uploaded.length;
    const seq = testSequence(
      "art-roundtrip",
      [
        step("dl", "tool_call", {
          url: `${mock!.baseUrl}/image`,
          method: "GET",
          response_as: "artifact",
        }),
        step("up", "tool_call", {
          url: `${mock!.baseUrl}/upload`,
          method: "PUT",
          // The artifact ref from `dl` becomes the request body. Step outputs
          // are addressed as `steps.<id>.<field>`.
          body_artifact: "{{steps.dl.artifact}}",
        }),
      ],
      { tenantId },
    );
    await client.createSequence(seq);
    const { id } = await client.createInstance({
      sequence_id: seq.id,
      tenant_id: tenantId,
      namespace: "default",
    });
    await client.waitForState(id, "completed", { timeoutMs: 20_000 });

    assert.equal(mock!.uploaded.length, before + 1, "upload should have happened once");
    const received = mock!.uploaded[before]!;
    assert.deepEqual(
      Uint8Array.from(received),
      Uint8Array.from(IMAGE_BYTES),
      "uploaded bytes must equal the downloaded artifact bytes",
    );
  });

  it("HTTP endpoints list an instance's artifacts and serve their bytes (tenant-scoped)", async () => {
    const tenantId = `art-http-${uuid().slice(0, 8)}`;
    const seq = testSequence(
      "art-http",
      [step("dl", "tool_call", { url: `${mock!.baseUrl}/image`, method: "GET", response_as: "artifact" })],
      { tenantId },
    );
    await client.createSequence(seq);
    const { id } = await client.createInstance({
      sequence_id: seq.id,
      tenant_id: tenantId,
      namespace: "default",
    });
    await client.waitForState(id, "completed", { timeoutMs: 20_000 });

    const h = { "X-Tenant-Id": tenantId };

    // GET /instances/{id}/artifacts → one artifact with key + size.
    const listResp = await fetch(`${client.baseUrl}/instances/${id}/artifacts`, { headers: h });
    assert.equal(listResp.status, 200);
    const listed = (await listResp.json()) as { items: { key: string; size: number }[] };
    assert.equal(listed.items.length, 1);
    const { key, size } = listed.items[0]!;
    assert.equal(size, IMAGE_BYTES.length);

    // GET /artifacts/{key} → the raw bytes.
    const bytesResp = await fetch(`${client.baseUrl}/artifacts/${key}`, { headers: h });
    assert.equal(bytesResp.status, 200);
    const got = Buffer.from(await bytesResp.arrayBuffer());
    assert.deepEqual(Uint8Array.from(got), Uint8Array.from(IMAGE_BYTES));

    // A different tenant cannot read it — 404 (no cross-tenant probing).
    const cross = await fetch(`${client.baseUrl}/artifacts/${key}`, {
      headers: { "X-Tenant-Id": "someone-else" },
    });
    assert.equal(cross.status, 404);
  });
});
