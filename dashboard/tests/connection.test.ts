import assert from "node:assert/strict";
import { test } from "node:test";
import { readEngineConnection, saveEngineConnection } from "../src/lib/connection.ts";

function storage(entries: [string, string][]) {
  const records = new Map(entries);
  return {
    getItem: (key: string) => records.get(key) ?? null,
    setItem: (key: string, value: string) => { records.set(key, value); },
    removeItem: (key: string) => { records.delete(key); },
  };
}

const defaultUrl = "https://default.example";

test("saving a profile retires the legacy keys and preserves its own origin", () => {
  const stored = storage([
    ["orch8_api_url", "https://old-engine.example"],
    ["orch8_api_key", "old-key"],
    ["orch8_tenant_id", "old-tenant"],
  ]);
  const profile = { baseUrl: "https://new-engine.example/", apiKey: "new-key", tenantId: "new-tenant" };
  saveEngineConnection(stored, profile);
  assert.deepEqual(readEngineConnection(stored, defaultUrl), profile);
  assert.equal(stored.getItem("orch8_api_key"), null);

  const profileKey = "orch8_engine_connection";
  const saved = stored.getItem(profileKey)!;
  stored.setItem(profileKey, saved.replace("https://new-engine.example/", "https://wrong-engine.example/"));
  assert.deepEqual(readEngineConnection(stored, defaultUrl), { baseUrl: defaultUrl, apiKey: null, tenantId: null });
});

test("a legacy API key without a saved endpoint cannot follow the deployment default", () => {
  const stored = storage([["orch8_api_key", "unbound-key"]]);
  assert.deepEqual(readEngineConnection(stored, defaultUrl), { baseUrl: defaultUrl, apiKey: null, tenantId: null });
});
