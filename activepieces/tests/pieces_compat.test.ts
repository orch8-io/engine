// SPDX-License-Identifier: MIT
// Copyright (c) 2026 Orch8, Inc.

// Exercise the published bundled pieces, not cached test doubles. This pins
// the sidecar's structural adapter across ActivePieces package upgrades.
import { test } from "node:test";
import * as assert from "node:assert/strict";
import { AddressInfo } from "node:net";

import { buildActionContext } from "../src/context.ts";
import { createDefaultLoader, findAction, findTrigger } from "../src/registry.ts";
import { createServer } from "../src/server.ts";

test("bundled HTTP piece loads and runs a pure action", async () => {
  const piece = await createDefaultLoader().load("http");
  const action = findAction(piece, "parse_url");
  const result = await action.run(buildActionContext({
    auth: null,
    propsValue: { url: "https://example.com/path?key=value", returnArrays: false },
    instanceId: "instance-1",
    blockId: "parse-url",
  })) as Record<string, unknown>;

  assert.equal(result.domain, "example.com");
  assert.equal(result.path, "/path");
  assert.deepEqual(result.query_parameters, { key: "value" });
});

test("bundled Slack piece loads, finds actions and triggers, and runs a pure action", async () => {
  const piece = await createDefaultLoader().load("slack");
  const action = findAction(piece, "markdownToSlackFormat");
  const result = await action.run(buildActionContext({
    auth: null,
    propsValue: { markdown: "**hello**" },
    instanceId: "instance-1",
    blockId: "markdown",
  }));

  assert.equal(typeof result, "string");
  assert.match(result as string, /\*hello\*/);
  assert.equal(findTrigger(piece, "new-message").name, "new-message");
});

test("HTTP endpoint executes a real bundled piece action", async () => {
  const server = createServer({
    port: 0,
    host: "127.0.0.1",
    loader: createDefaultLoader(),
    requestTimeoutMs: 5_000,
    log: () => {},
  });
  await new Promise<void>((resolve) => server.listen(0, "127.0.0.1", resolve));
  try {
    const { port } = server.address() as AddressInfo;
    const response = await fetch(`http://127.0.0.1:${port}/execute`, {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify({
        piece: "http",
        action: "parse_url",
        props: { url: "https://example.com/path?key=value", returnArrays: false },
      }),
    });
    const body = await response.json() as {
      ok: boolean;
      output?: { domain?: string; query_parameters?: Record<string, string> };
    };
    assert.equal(response.status, 200);
    assert.equal(body.ok, true);
    assert.equal(body.output?.domain, "example.com");
    assert.deepEqual(body.output?.query_parameters, { key: "value" });
  } finally {
    await new Promise<void>((resolve) => server.close(() => resolve()));
  }
});
