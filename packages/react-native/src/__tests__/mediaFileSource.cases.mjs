import assert from "node:assert/strict";
import { mkdtemp, open, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { test } from "node:test";
import { createFileMediaUploadSource } from "../mediaFileSource.ts";
import { DurableMediaUploader } from "../mediaUpload.ts";

test("file media source validates ranges and returns exact binary bytes", async () => {
  const example = Uint8Array.of(0, 0xff, 0x80, 0x10, 0xe1);
  let requests = 0;
  const source = createFileMediaUploadSource({
    async readMediaFileRange(path, offset, length) {
      assert.equal(path, "/app/file");
      assert.equal(offset, 7);
      requests++;
      return Buffer.from(example.subarray(0, length)).toString("base64");
    },
  }, (id) => {
    assert.equal(id, "upload");
    return "/app/file";
  });
  for (let length = 1; length <= example.length; length++) {
    assert.deepEqual(await source.read("upload", 7, length), example.subarray(0, length));
  }
  for (const [offset, length] of [[-1, 1], [0.5, 1], [0, 0], [0, 4 * 1024 ** 2 + 1], [Number.MAX_SAFE_INTEGER, 1]]) {
    await assert.rejects(source.read("upload", offset, length), RangeError);
  }
  assert.equal(requests, example.length);

  const badBridge = createFileMediaUploadSource({
    async readMediaFileRange() { return "!!=="; },
  }, () => "/app/file");
  await assert.rejects(badBridge.read("upload", 0, 1), /Invalid native media range/);
  const badPath = createFileMediaUploadSource({
    async readMediaFileRange() { assert.fail("invalid path reached the native bridge"); },
  }, () => "file");
  await assert.rejects(badPath.read("upload", 0, 1), /absolute local file path/);
});

test("uploader resumes a sparse file larger than its memory budget using bounded positional reads", async () => {
  const directory = await mkdtemp(join(tmpdir(), "orch8-media-"));
  try {
    const path = join(directory, "payload");
    const chunk = 4 * 1024 ** 2;
    const count = 65;
    const length = chunk * count;
    const fixture = await open(path, "wx");
    try {
      await fixture.truncate(length);
      for (let i = 0; i < count; i++) {
        await fixture.write(Uint8Array.of(i + 1), 0, 1, i * chunk);
      }
    } finally {
      await fixture.close();
    }
    let state = { id: "file-1", length, offset: 0, status: "pending" };
    const journal = {
      async load() { return state; },
      async compareAndSet(_id, expected, updated) {
        assert.equal(state.offset, expected.offset);
        state = updated;
        return true;
      },
    };
    const read = createFileMediaUploadSource({
      async readMediaFileRange(path, offset, length) {
        assert.equal(length, chunk);
        const file = await open(path, "r");
        try {
          const bytes = Buffer.alloc(length);
          assert.equal((await file.read(bytes, { position: offset })).bytesRead, length);
          return bytes.toString("base64");
        } finally {
          await file.close();
        }
      },
    }, () => path);
    const startRss = process.memoryUsage.rss();
    const maxRssGrowth = 192 * 1024 ** 2;
    const transport = {
      ...read,
      async append(id, offset, bytes) {
        assert.equal(id, state.id);
        assert.equal(offset, state.offset);
        assert.equal(bytes[0], 1 + offset / chunk);
        assert.equal(bytes.at(-1), 0);
        const growth = process.memoryUsage.rss() - startRss;
        assert.ok(growth < maxRssGrowth, `Resident memory grew by ${growth} bytes`);
        return offset + bytes.byteLength;
      },
    };
    const first = new DurableMediaUploader(journal, transport);
    const options = { network: "unmetered", maxBytes: length, chunkBytes: chunk };
    assert.equal((await first.advance(state.id, options)).offset, chunk);
    const resumed = new DurableMediaUploader(journal, transport);
    assert.deepEqual(await resumed.advance(state.id, { ...options, maxChunks: count - 1 }), {
      status: "uploaded",
      offset: length,
    });
  } finally {
    await rm(directory, { recursive: true, force: true });
  }
});
