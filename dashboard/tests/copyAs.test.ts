import { test } from "node:test";
import * as assert from "node:assert/strict";
import {
  generateSnippet,
  shellQuote,
  toCli,
  toCurl,
  toNode,
  toPyLiteral,
  toPython,
  type ConnectionInfo,
} from "../src/lib/copyAs.ts";
import {
  batchActionRequest,
  createCronRequest,
  createInstanceRequest,
  createSequenceRequest,
  forkInstanceRequest,
  humanInputRequest,
  retryInstanceRequest,
  signalRequest,
} from "../src/lib/requests.ts";

const conn: ConnectionInfo = { baseUrl: "http://localhost:8080/", tenantId: "demo" };
const noTenant: ConnectionInfo = { baseUrl: "http://localhost:8080", tenantId: null };
const ID = "0190a000-0000-7000-8000-000000000001";

test("request descriptors match what api.ts sends", () => {
  assert.deepEqual(signalRequest(ID, "cancel"), {
    method: "POST",
    path: `/instances/${ID}/signals`,
    body: { signal_type: "cancel", payload: {} },
    label: "Send cancel signal",
    sdk: signalRequest(ID, "cancel").sdk,
    cli: { args: ["signal", ID, "cancel", "--payload", "{}"] },
  });
  const human = humanInputRequest(ID, "approve", "yes");
  assert.deepEqual(human.body, { signal_type: { Custom: "human_input:approve" }, payload: { value: "yes" } });
  assert.equal(human.cli, undefined, "custom signals have no exact CLI equivalent");
  const retry = retryInstanceRequest(ID);
  assert.equal(retry.body, undefined);
  assert.equal(retry.path, `/instances/${ID}/retry`);
  const fork = forkInstanceRequest(ID, { from_block_id: "b2", dry_run: true });
  assert.deepEqual(fork.body, { from_block_id: "b2", dry_run: true });
});

test("curl snippet: exact method/url/body, key redacted, tenant header", () => {
  const spec = createCronRequest({ tenant_id: "demo", namespace: "prod", sequence_id: "s", cron_expr: "*/5 * * * *", timezone: "UTC" });
  const out = toCurl(spec, conn);
  assert.equal(
    out,
    [
      "curl -sS -X POST http://localhost:8080/cron",
      '  -H "X-API-Key: $ORCH8_API_KEY"',
      "  -H 'X-Tenant-Id: demo'",
      "  -H 'Content-Type: application/json'",
      `  -d '{"tenant_id":"demo","namespace":"prod","sequence_id":"s","cron_expr":"*/5 * * * *","timezone":"UTC"}'`,
    ].join(" \\\n"),
  );
  assert.ok(!toCurl(retryInstanceRequest(ID), noTenant).includes("-d "), "no body → no -d");
  assert.ok(!toCurl(retryInstanceRequest(ID), noTenant).includes("X-Tenant-Id"));
});

test("curl snippet escapes single quotes in the body", () => {
  const spec = createInstanceRequest({
    sequence_id: "s",
    tenant_id: "t",
    namespace: "n",
    context: { data: { name: "O'Brien" }, config: {}, audit: [] },
  });
  const out = toCurl(spec, conn);
  assert.ok(out.includes(`"O'\\''Brien"`));
});

test("shellQuote leaves safe words bare and quotes the rest", () => {
  assert.equal(shellQuote("abc-1/2:3"), "abc-1/2:3");
  assert.equal(shellQuote("a b"), "'a b'");
  assert.equal(shellQuote("it's"), `'it'\\''s'`);
  assert.equal(shellQuote(""), "''");
});

test("snippets never contain a literal API key", () => {
  const specs = [
    signalRequest(ID, "pause"),
    retryInstanceRequest(ID),
    createSequenceRequest({ name: "x", blocks: [] }),
    batchActionRequest({ filter: {}, action: "retry" }),
  ];
  for (const spec of specs) {
    for (const fmt of ["curl", "node", "python", "cli"] as const) {
      const s = generateSnippet(fmt, spec, conn);
      if (s === null) continue;
      assert.ok(s.includes("ORCH8_API_KEY"), `${fmt} mentions the env var`);
    }
  }
});

test("Node snippet uses the named SDK method when it is a passthrough", () => {
  const out = toNode(signalRequest(ID, "resume"), conn);
  assert.ok(out.includes(`import { Orch8Client } from "@orch8.io/sdk";`));
  assert.ok(out.includes(`baseUrl: "http://localhost:8080",`));
  assert.ok(out.includes(`tenantId: "demo",`));
  assert.ok(out.includes(`headers: { "X-API-Key": process.env.ORCH8_API_KEY ?? "" },`));
  assert.ok(out.includes(`await client.sendSignal("${ID}", {\n  signal_type: "resume",\n  payload: {},\n});`));
});

test("Node snippet falls back to client.request for uncovered endpoints", () => {
  const out = toNode(forkInstanceRequest(ID, { from_block_id: "b", dry_run: false }), noTenant);
  assert.ok(out.includes(`await client.request("POST", "/instances/${ID}/fork", {\n  from_block_id: "b",\n  dry_run: false,\n});`));
  assert.ok(!out.includes("tenantId"));
  assert.ok(toNode(retryInstanceRequest(ID), conn).includes(`client.retryInstance("${ID}")`));
});

test("Python snippet renders Python literals and uses request(json=...)", () => {
  assert.equal(toPyLiteral({ a: true, b: null, c: [1, "x"] }), `{\n    "a": True,\n    "b": None,\n    "c": [\n        1,\n        "x",\n    ],\n}`);
  const out = toPython(batchActionRequest({ filter: { states: ["failed"] }, action: "retry", dry_run: false }), conn);
  assert.ok(out.includes(`from orch8 import Orch8Client`));
  assert.ok(out.includes(`Orch8Client("http://localhost:8080", tenant_id="demo", headers={"X-API-Key": os.environ["ORCH8_API_KEY"]})`));
  assert.ok(out.includes(`await client.request("POST", "/instances/batch-action", json={`));
  assert.ok(out.includes(`"dry_run": False,`));
  assert.ok(out.includes("asyncio.run(main())"));
  const retry = toPython(retryInstanceRequest(ID), noTenant);
  assert.ok(retry.includes(`await client.retry_instance("${ID}")`));
});

test("CLI snippet for signal / retry / instance create / sequence create", () => {
  assert.equal(
    toCli(signalRequest(ID, "cancel"), conn),
    `# The CLI reads the API key from $ORCH8_API_KEY.\norch8 --url http://localhost:8080 --tenant-id demo signal ${ID} cancel --payload '{}'`,
  );
  assert.ok(toCli(retryInstanceRequest(ID), noTenant)!.endsWith(`orch8 --url http://localhost:8080 instance retry ${ID}`));
  const create = toCli(
    createInstanceRequest({ sequence_id: "s1", tenant_id: "demo", namespace: "prod", context: { data: {}, config: {}, audit: [] } }),
    conn,
  )!;
  assert.ok(create.includes(`--tenant-id demo instance create --sequence-id s1 --namespace prod --input '{"data":{},"config":{},"audit":[]}'`));
  const seq = toCli(createSequenceRequest({ name: "x", blocks: [] }), noTenant)!;
  assert.ok(seq.startsWith(`cat > sequence.json <<'JSON'\n{\n  "name": "x",\n  "blocks": []\n}\nJSON`));
  assert.ok(seq.endsWith("orch8 --url http://localhost:8080 sequence create --file sequence.json"));
});

test("undefined body fields are omitted in every format, as JSON.stringify sends them", () => {
  const spec = batchActionRequest({ filter: { tenant_id: "demo", namespace: undefined }, action: "retry", dry_run: undefined });
  assert.ok(!toNode(spec, conn).includes("undefined"));
  assert.ok(!toNode(spec, conn).includes("namespace"));
  assert.ok(!toPython(spec, conn).includes(": None"));
  assert.ok(!toPython(spec, conn).includes("dry_run"));
  assert.ok(toCurl(spec, conn).includes(`-d '{"filter":{"tenant_id":"demo"},"action":"retry"}'`));
});

test("CLI snippet is null when no exact CLI equivalent exists", () => {
  assert.equal(toCli(createCronRequest({}), conn), null);
  assert.equal(toCli(humanInputRequest(ID, "b", "ok"), conn), null);
  assert.equal(toCli(forkInstanceRequest(ID, { from_block_id: "b", dry_run: true }), conn), null);
  // dry_run instance creates are not expressible with `orch8 instance create`.
  assert.equal(
    toCli(createInstanceRequest({ sequence_id: "s", tenant_id: "t", namespace: "n", context: {}, dry_run: true }), conn),
    null,
  );
  // Body tenant differs from the dashboard's tenant header → not reproducible.
  assert.equal(
    toCli(createInstanceRequest({ sequence_id: "s", tenant_id: "other", namespace: "n", context: {} }), conn),
    null,
  );
});
