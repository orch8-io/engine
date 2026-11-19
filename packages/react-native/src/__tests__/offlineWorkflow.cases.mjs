import assert from "node:assert/strict";
import test from "node:test";

import { BackgroundProgress, workflowSyncState, previewDataVisibility, resolveOfflineConflict } from "../offlineWorkflow.ts";
import { DurableMediaUploader } from "../mediaUpload.ts";

test("visibility previews disclose field paths only and keep private fields off all external destinations", () => {
  const data = { photo: { digest: "abc", bytes: "private-image" }, "site/id": 12 };
  const preview = previewDataVisibility(data, {
    sync: ["/photo/digest", "/site~1id"],
    worker: ["/photo/digest"],
    logs: ["/photo"],
    support: ["/photo/bytes"],
    private: ["/photo/bytes"],
  });
  assert.deepEqual(preview, [
    { path: "/photo/bytes", destinations: [] },
    { path: "/photo/digest", destinations: ["sync", "worker"] },
    { path: "/site~1id", destinations: ["sync"] },
  ]);
  assert.equal(JSON.stringify(preview).includes("private-image"), false);
});

test("offline conflicts default to review and rebase resolved updates on the current server revision", async () => {
  const edit = { baseRevision: "r1", base: "open", local: "passed", serverRevision: "r2", server: "closed" };
  assert.deepEqual(await resolveOfflineConflict(edit), { status: "review" });
  assert.deepEqual(await resolveOfflineConflict(edit, () => ({ action: "use_server" })), {
    status: "server", value: "closed", revision: "r2",
  });
  assert.deepEqual(await resolveOfflineConflict(edit, async () => ({ action: "merge", value: "verified" })), {
    status: "write", value: "verified", expectedRevision: "r2",
  });
  assert.deepEqual(await resolveOfflineConflict({ ...edit, serverRevision: "r1" }), {
    status: "write", value: "passed", expectedRevision: "r1",
  });
});

test("background diagnostics preserve progress across idle windows and distinguish OS budget exhaustion", () => {
  const diagnostics = new BackgroundProgress();
  diagnostics.observe("local", 1000, 1100, true, false, true);
  diagnostics.observe("worker", 1200, 1300, false, true, true);
  assert.deepEqual(diagnostics.snapshot(), {
    lastProgressAtMs: 1100,
    lastWindow: { kind: "worker", startedAtMs: 1200, finishedAtMs: 1300, outcome: "budget_exhausted" },
    nextAction: "wait_for_os_or_open_app",
  });
  diagnostics.observe("local", 1400, 1405, false, false, false);
  assert.equal(diagnostics.snapshot().lastWindow.outcome, "idle");
  assert.equal(diagnostics.snapshot().lastProgressAtMs, 1100);
  assert.equal(diagnostics.snapshot().nextAction, "none_observed");
});

test("media transfers resume only from a server-acknowledged, persisted offset", async () => {
  let state = { id: "photo-1", length: 5, offset: 0, status: "pending" };
  const writes = [];
  let serverOffset = 0;
  let lostResponse = true;
  const store = {
    load: async () => ({ ...state }),
    compareAndSet: async (_id, before, after) => {
      if (state.offset !== before.offset || state.status !== before.status) return false;
      state = { ...after };
      return true;
    },
  };
  const transport = {
    read: async (_id, _offset, length) => new Uint8Array(length),
    append: async (_id, offset, bytes) => {
      writes.push(offset);
      serverOffset = Math.max(serverOffset, offset + bytes.length);
      if (lostResponse) { lostResponse = false; throw new Error("connection dropped"); }
      return serverOffset;
    },
  };
  const first = new DurableMediaUploader(store, transport);
  assert.equal((await first.advance("photo-1", { network: "metered", chunkBytes: 2 })).status, "deferred");
  await assert.rejects(first.advance("photo-1", { network: "unmetered", chunkBytes: 2 }), /connection dropped/);
  assert.equal(state.offset, 0);

  const restarted = new DurableMediaUploader(store, transport);
  assert.equal((await restarted.advance("photo-1", { network: "unmetered", chunkBytes: 2 })).status, "pending");
  assert.equal(state.offset, 2);
  assert.equal((await restarted.advance("photo-1", { network: "unmetered", chunkBytes: 2, maxChunks: 2 })).status, "uploaded");
  assert.deepEqual(writes, [0, 0, 2, 4]);
  await restarted.advance("photo-1", { network: "unmetered", chunkBytes: 2 });
  assert.deepEqual(writes, [0, 0, 2, 4]);
});

test("backend acknowledgement only completes the same local revision", () => {
  const completion = { submissionId: "inspection-1", revision: "v2" };
  const old = { submissionId: "inspection-1", revision: "v1", outcome: "acknowledged" };
  assert.equal(workflowSyncState({ completion, receipt: old, startedSync: true }), "pending_sync");
  assert.equal(workflowSyncState({ completion }), "local_complete");
  const receipt = { ...old, revision: "v2" };
  assert.equal(workflowSyncState({ completion, receipt, startedSync: true }), "acknowledged");
  assert.equal(workflowSyncState({ completion, receipt: { ...receipt, outcome: "needs_review" } }),
    "needs_review");
  assert.equal(workflowSyncState({ receipt }), "in_progress");
});

test("concurrent media windows persist each range once and both observe the durable completion", async () => {
  let state = { id: "photo", length: 4, offset: 0, status: "pending" };
  let sharedRangeRequests = 0;
  let unblockSharedRange;
  const sharedRangeReady = new Promise((resolve) => { unblockSharedRange = resolve; });
  const persisted = new Map();
  const store = {
    load: async () => ({ ...state }),
    compareAndSet: async (_id, before, after) => {
      if (state.offset !== before.offset) return false;
      state = { ...after };
      return true;
    },
  };
  const transport = {
    read: async (_id, offset, length) => Uint8Array.from({ length }, (_, index) => offset + index),
    append: async (_id, offset, bytes) => {
      if (offset === 0) {
        if (++sharedRangeRequests === 2) unblockSharedRange();
        await sharedRangeReady;
      }
      if (persisted.has(offset)) assert.deepEqual(persisted.get(offset), Array.from(bytes));
      else persisted.set(offset, Array.from(bytes));
      return offset + bytes.length;
    },
  };
  const options = { network: "unmetered", maxChunks: 2, chunkBytes: 2 };
  const windows = [new DurableMediaUploader(store, transport), new DurableMediaUploader(store, transport)];
  assert.deepEqual(await Promise.all(windows.map((window) => window.advance("photo", options))), [
    { status: "uploaded", offset: 4 },
    { status: "uploaded", offset: 4 },
  ]);
  assert.equal(sharedRangeRequests, 2);
  assert.deepEqual([...persisted.values()], [[0, 1], [2, 3]]);
});

test("media acknowledgement beyond the submitted range leaves the durable offset unchanged", async () => {
  const state = { id: "photo", length: 4, offset: 0, status: "pending" };
  const upload = new DurableMediaUploader(
    {
      load: async () => state,
      compareAndSet: async () => assert.fail("unexpected range must never advance local state"),
    },
    {
      read: async () => Uint8Array.of(1, 2),
      append: async () => 3,
    },
  );
  await assert.rejects(upload.advance("photo", { network: "unmetered", chunkBytes: 2 }), /acknowledgement offset/);
  assert.equal(state.offset, 0);
});
