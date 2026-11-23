import assert from "node:assert/strict";
import { spawnSync } from "node:child_process";
import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { DatabaseSync } from "node:sqlite";
import { test } from "node:test";
import { DurableMediaUploader } from "../mediaUpload.ts";
import { SqliteMediaUploadJournal } from "../mediaUploadJournal.ts";
import { sqliteAdapter } from "./sqliteFixture.mjs";

test("SQLite media journal atomically advances matching records and rejects stale writers", async () => {
  const database = new DatabaseSync(":memory:");
  try {
    const journal = await SqliteMediaUploadJournal.open(sqliteAdapter(database));
    assert.equal(await journal.load("missing"), null);
    const before = await journal.create("media", 4);
    const first = { ...before, offset: 2 };
    const second = { ...before, offset: 3 };
    assert.deepEqual(await Promise.all([
      journal.compareAndSet("media", before, first),
      journal.compareAndSet("media", before, second),
    ]), [true, false]);
    assert.deepEqual(await journal.create("media", 4), first);
    await assert.rejects(journal.create("media", 5), /different file length/);
    await assert.rejects(
      journal.compareAndSet("media", first, { ...first, offset: 4 }),
      /Invalid acknowledged media upload transition/,
    );
    assert.equal(await journal.compareAndSet("media", first, { ...first, offset: 4, status: "uploaded" }), true);
    assert.equal((await journal.load("media")).status, "uploaded");
  } finally {
    database.close();
  }
});

test("uploader resumes from the acknowledged byte range after the writer dies", async () => {
  const directory = await mkdtemp(join(tmpdir(), "orch8-upload-journal-"));
  try {
    const path = join(directory, "upload.db");
    const writer = `
      import { DatabaseSync } from "node:sqlite";
      import { DurableMediaUploader } from ${JSON.stringify(new URL("../mediaUpload.ts", import.meta.url).href)};
      import { SqliteMediaUploadJournal } from ${JSON.stringify(new URL("../mediaUploadJournal.ts", import.meta.url).href)};
      const adapter = (${sqliteAdapter.toString()})(new DatabaseSync(process.argv[1]));
      const journal = await SqliteMediaUploadJournal.open(adapter);
      await journal.create("resumed", 9);
      const uploader = new DurableMediaUploader(journal, {
        async read(_, offset, length) {
          return new Uint8Array(length).fill(10 + offset);
        },
        async append(_, offset, bytes) { return offset + bytes.length; },
      });
      const result = await uploader.advance("resumed", { network: "unmetered", chunkBytes: 4 });
      if (result.offset !== 4) throw new Error("writer did not commit the acknowledged range");
      process.kill(process.pid, "SIGKILL");
    `;
    const child = spawnSync(process.execPath, [
      "--experimental-strip-types", "--input-type=module", "--eval", writer, path,
    ], { encoding: "utf8" });
    assert.ifError(child.error);
    assert.equal(child.signal, "SIGKILL", child.stderr);

    const database = new DatabaseSync(path);
    try {
      const journal = await SqliteMediaUploadJournal.open(sqliteAdapter(database));
      assert.equal((await journal.load("resumed")).offset, 4);
      const resumed = [];
      const uploader = new DurableMediaUploader(journal, {
        async read(_, offset, length) { return new Uint8Array(length).fill(10 + offset); },
        async append(_, offset, bytes) {
          resumed.push([offset, [...bytes]]);
          return offset + bytes.length;
        },
      });
      assert.deepEqual(await uploader.advance("resumed", {
        network: "unmetered",
        chunkBytes: 4,
        maxChunks: 2,
      }), { status: "uploaded", offset: 9 });
      assert.deepEqual(resumed, [[4, [14, 14, 14, 14]], [8, [18]]]);
      assert.equal((await journal.load("resumed")).status, "uploaded");
    } finally {
      database.close();
    }
  } finally {
    await rm(directory, { recursive: true, force: true });
  }
});

test("a reopened journal keeps its original file manifest and blocks replacement before append", async () => {
  const { createHash } = await import("node:crypto");
  const { createFileMediaUploadSource, describeMediaUploadFile } = await import("../mediaFileSource.ts");
  const directory = await mkdtemp(join(tmpdir(), "orch8-upload-manifest-"));
  try {
    const path = join(directory, "upload.db");
    const filePath = join(directory, "source");
    const digest = (bytes) => createHash("sha256").update(bytes).digest("hex");
    let identity = "original-file";
    let content = Buffer.from("abcdefgh");
    const bridge = {
      async describeMediaFile(path, chunkBytes) {
        assert.equal(path, filePath);
        const sha256 = [digest(content.subarray(0, chunkBytes)), digest(content.subarray(chunkBytes))];
        return { version: 1, length: content.length, identity, chunkBytes, sha256 };
      },
      async readMediaFileRange(path, offset, length, expectedIdentity, expectedDigest) {
        assert.equal(path, filePath);
        const chunk = content.subarray(offset, offset + length);
        if (identity !== expectedIdentity || digest(chunk) !== expectedDigest) {
          throw new Error("Media file no longer matches the queued upload");
        }
        return chunk.toString("base64");
      },
    };
    const manifest = await describeMediaUploadFile(bridge, filePath, 4);
    const source = createFileMediaUploadSource(bridge);
    const appended = [];
    const transport = {
      ...source,
      async append(_, offset, bytes) {
        appended.push(Buffer.from(bytes).toString());
        return offset + bytes.length;
      },
    };
    const originalDatabase = new DatabaseSync(path);
    try {
      const original = await SqliteMediaUploadJournal.open(sqliteAdapter(originalDatabase));
      await original.create("upload", manifest.length, manifest);
      const uploader = new DurableMediaUploader(original, transport);
      assert.equal((await uploader.advance("upload", { network: "unmetered" })).offset, 4);
    } finally {
      originalDatabase.close();
    }

    identity = "replaced-file";
    content = Buffer.from("abcdwxyz");
    const database = new DatabaseSync(path);
    try {
      const journal = await SqliteMediaUploadJournal.open(sqliteAdapter(database));
      assert.deepEqual((await journal.load("upload")).file, manifest);
      const replacement = await describeMediaUploadFile(bridge, filePath, 4);
      await assert.rejects(journal.create("upload", replacement.length, replacement), /different file manifest/);
      const resumed = new DurableMediaUploader(journal, transport);
      await assert.rejects(resumed.advance("upload", { network: "unmetered" }), /no longer matches/);
      assert.deepEqual(appended, ["abcd"]);
      assert.equal((await journal.load("upload")).offset, 4);
    } finally {
      database.close();
    }
  } finally {
    await rm(directory, { recursive: true, force: true });
  }
});
