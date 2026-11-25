import assert from "node:assert/strict";
import { spawn } from "node:child_process";
import { once } from "node:events";
import { createServer } from "node:http";
import { test } from "node:test";

test("reports email gate outcomes with their worker claim epochs", { timeout: 6000 }, async () => {
  const tasks = [
    { id: "accepted", claim_epoch: 7, params: { subject: "VIVA ORCH8: Hello", prefix: "VIVA ORCH8:" } },
    { id: "invalid", claim_epoch: 8, params: { subject: null, prefix: "VIVA ORCH8:" } },
  ];
  const reports = [];
  let resolve;
  let reject;
  const allReported = new Promise((done, fail) => { resolve = done; reject = fail; });
  const server = createServer(async (request, response) => {
    try {
      const chunks = [];
      for await (const chunk of request) chunks.push(chunk);
      const body = JSON.parse(Buffer.concat(chunks).toString());
      assert.equal(request.method, "POST");
      assert.equal(request.headers["x-tenant-id"], "example");
      assert.equal(request.headers["x-api-key"], "fixture-key");
      if (request.url === "/api/v1/workers/tasks/poll") {
        const task = body.handler_name === "check_subject_prefix" ? tasks.shift() : undefined;
        response.writeHead(200, { "content-type": "application/json" });
        response.end(JSON.stringify({ tasks: task ? [task] : [], lease_secs: 30, poll_after_ms: 250 }));
      } else {
        reports.push({ path: request.url, body });
        response.writeHead(200).end();
        if (reports.length === 2) resolve();
      }
    } catch (error) {
      response.writeHead(500).end();
      reject(error);
    }
  });
  server.listen(0, "127.0.0.1");
  await once(server, "listening");
  const child = spawn(process.execPath, ["--import", "tsx", "worker.ts"], {
    env: {
      ...process.env,
      ORCH8_URL: `http://127.0.0.1:${server.address().port}/api/v1`,
      ORCH8_API_KEY: "fixture-key",
      ORCH8_WORKER_ID: "email-test-worker",
    },
    stdio: ["ignore", "pipe", "pipe"],
  });
  const exited = once(child, "exit");
  let logs = "";
  child.stdout.on("data", (chunk) => { logs += chunk; });
  child.stderr.on("data", (chunk) => { logs += chunk; });
  const deadline = setTimeout(() => reject(new Error(`Worker did not report both tasks\n${logs}`)), 4500);
  try {
    await Promise.race([allReported, exited.then(([code]) => { throw new Error(`Worker exited with ${code}\n${logs}`); })]);
    assert.deepEqual(reports[0], {
      path: "/api/v1/workers/tasks/accepted/complete",
      body: { worker_id: "email-test-worker", claim_epoch: 7, output: { accepted: true, clean_subject: "Hello" } },
    });
    assert.equal(reports[1].path, "/api/v1/workers/tasks/invalid/fail");
    assert.equal(reports[1].body.worker_id, "email-test-worker");
    assert.equal(reports[1].body.claim_epoch, 8);
    assert.equal(reports[1].body.retryable, false);
    child.kill("SIGTERM");
    assert.deepEqual(await exited, [0, null]);
  } finally {
    clearTimeout(deadline);
    if (child.exitCode === null && child.signalCode === null) child.kill("SIGKILL");
    server.closeAllConnections();
    await new Promise((done) => server.close(done));
  }
});
