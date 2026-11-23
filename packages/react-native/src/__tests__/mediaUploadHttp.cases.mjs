import assert from "node:assert/strict";
import test from "node:test";
import { createHttpMediaUploadTransport } from "../mediaUploadHttp.ts";

test("media HTTP adapter supplies the whole digest and checks the exact acknowledgement", async () => {
  const calls = [];
  let offset = 8;
  const source = { async read() { return new Uint8Array([4, 5]); } };
  const transport = createHttpMediaUploadTransport("instance", async (path, init) => {
    calls.push([path, init]);
    return Response.json({ offset });
  }, source);
  const fileSha256 = "a".repeat(64);
  await transport.begin("upload/id", { length: 8, chunkBytes: 6, fileSha256 });
  const bytes = await transport.read("upload/id", 6, 2);
  assert.equal(await transport.append("upload/id", 6, bytes), 8);
  assert.equal(calls[0][0], "/instances/instance/media-uploads/upload%2Fid");
  assert.deepEqual(JSON.parse(calls[0][1].body), { length: 8, chunk_bytes: 6, sha256: fileSha256 });
  assert.equal(calls[1][0], "/instances/instance/media-uploads/upload%2Fid/ranges/6");
  assert.deepEqual(new Uint8Array(calls[1][1].body), bytes);
  offset = 9;
  await assert.rejects(transport.append("upload/id", 6, bytes), /acknowledgement offset/);
  await assert.rejects(transport.begin("upload/id", {}), /whole-file SHA-256/);
});
