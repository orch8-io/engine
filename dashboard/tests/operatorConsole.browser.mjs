import assert from "node:assert/strict";
import { log } from "node:console";
import { basename, join } from "node:path";
import process from "node:process";
import { URL, fileURLToPath } from "node:url";

const { chromium } = await import(process.env.PLAYWRIGHT_MODULE ?? "playwright");
const dist = fileURLToPath(new URL("../dist", import.meta.url));
const runId = "0190a000-0000-7000-8000-000000000001";
const sequenceId = "0190a000-0000-7000-8000-000000000002";
const oldEngine = "http://old-engine.test";
const newEngine = "http://new-engine.test";
const fixtureKey = "fixture-operator-key";
const timestamp = "2026-01-01T00:00:00Z";
const instance = {
  id: runId,
  sequence_id: sequenceId,
  tenant_id: "fixture-team",
  namespace: "default",
  state: "completed",
  priority: "Normal",
  timezone: "UTC",
  metadata: {},
  context: {},
  next_fire_at: null,
  created_at: timestamp,
  updated_at: timestamp,
};

const browser = await chromium.launch({ executablePath: process.env.CHROMIUM_PATH });
try {
  const page = await browser.newPage();
  page.setDefaultTimeout(5000);
  await page.addInitScript(({ oldEngine, fixtureKey }) => {
    if (globalThis.localStorage.getItem("orch8_engine_connection")) return;
    globalThis.localStorage.setItem("orch8_api_url", oldEngine);
    globalThis.localStorage.setItem("orch8_api_key", fixtureKey);
    globalThis.localStorage.setItem("orch8_tenant_id", "fixture-team");
  }, { oldEngine, fixtureKey });
  const requests = [];
  await page.route("**/*", async (route) => {
    const request = route.request();
    const { origin, pathname } = new URL(request.url());
    if (origin === "http://dashboard.test") {
      const asset = pathname.startsWith("/assets/")
        ? join(dist, "assets", basename(pathname))
        : join(dist, "index.html");
      return route.fulfill({ path: asset });
    }
    if (origin !== oldEngine && origin !== newEngine) return route.abort();
    const headers = {
      "access-control-allow-origin": "*",
      "access-control-allow-methods": "GET,OPTIONS",
      "access-control-allow-headers": "content-type,x-api-key,x-tenant-id",
    };
    if (request.method() === "OPTIONS") return route.fulfill({ status: 204, headers });
    requests.push({ origin, pathname, headers: request.headers() });
    const json = pathname === "/instances" ? { items: [instance], has_more: false }
      : pathname === `/instances/${runId}` ? instance : {};
    return route.fulfill({ json, headers });
  });

  await page.goto("http://dashboard.test/settings");
  const apiUrlInput = page.getByLabel("Engine API URL");
  const keyInput = page.getByLabel("API key", { exact: true });
  assert.equal(await keyInput.inputValue(), fixtureKey);
  await apiUrlInput.fill(newEngine);
  assert.equal(await keyInput.inputValue(), "", "switching engines clears the previous credential");
  assert.equal(await page.getByLabel("Tenant ID").inputValue(), "");
  await keyInput.fill("fixture-new-operator-key");
  await page.getByRole("button", { name: /test connection/i }).click();
  await page.getByText("Saved. Engine is reachable.", { exact: true }).waitFor();
  assert.ok(requests.some(({ origin, pathname }) => origin === newEngine && pathname === "/health/live"));
  for (const request of requests.filter(({ pathname }) => pathname === "/health/live")) {
    assert.equal(request.headers["x-api-key"], undefined, "liveness omits the API key");
    assert.equal(request.headers["x-tenant-id"], undefined, "liveness omits the tenant header");
  }

  await page.getByRole("navigation", { name: "Primary" }).getByRole("link", { name: "Executions" }).click();
  const link = page.getByRole("link", { name: `Open execution ${runId}`, exact: true });
  await link.focus();
  await page.keyboard.press("Enter");
  await page.waitForURL(`http://dashboard.test/instances/${runId}`);
  const listing = requests.find(({ origin, pathname }) => origin === newEngine && pathname === "/instances");
  assert.equal(listing?.headers["x-api-key"], "fixture-new-operator-key");
  assert.ok(!requests.some(({ origin, headers }) => origin === newEngine && headers["x-api-key"] === fixtureKey));
  await page.reload();
  assert.equal(await page.evaluate(() => globalThis.localStorage.getItem("orch8_api_key")), null);
  log("Operator dashboard credential and keyboard browser regression passed.");
} finally {
  await browser.close();
}
