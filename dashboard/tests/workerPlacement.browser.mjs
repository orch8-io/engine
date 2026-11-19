import assert from "node:assert/strict";
import { log } from "node:console";
import { basename, join } from "node:path";
import process from "node:process";
import { URL, fileURLToPath } from "node:url";

const { chromium } = await import(process.env.PLAYWRIGHT_MODULE ?? "playwright");
const dist = fileURLToPath(new URL("../dist", import.meta.url));
const sequenceId = "55555555-5555-4555-a555-555555555555";
const browser = await chromium.launch({ executablePath: process.env.CHROMIUM_PATH });
let releaseOld;
const oldReply = new Promise((resolve) => { releaseOld = resolve; });
try {
  const page = await browser.newPage();
  page.setDefaultTimeout(5000);
  await page.addInitScript(() => globalThis.localStorage.setItem("orch8_api_url", "http://engine.test"));
  const previews = [];
  let enrollment;
  let stagedPin;
  await page.route("**/*", async (route) => {
    const request = route.request();
    const { hostname, pathname, searchParams } = new URL(request.url());
    if (hostname === "dashboard.test") {
      const asset = pathname.startsWith("/assets/") ? join(dist, "assets", basename(pathname)) : join(dist, "index.html");
      return route.fulfill({ path: asset });
    }
    if (hostname !== "engine.test") return route.abort();
    const headers = {
      "access-control-allow-origin": "*", "access-control-allow-methods": "GET,POST,OPTIONS",
      "access-control-allow-headers": "content-type",
    };
    if (request.method() === "OPTIONS") return route.fulfill({ status: 204, headers });
    const reply = (json) => route.fulfill({ json, headers });
    if (pathname === `/placement/plan/${sequenceId}`) {
      previews.push(Object.fromEntries(searchParams));
      const staged = searchParams.get("staged_pins") === "true";
      if (!staged && previews.length === 1) await oldReply;
      await reply({
        ready: !staged,
        inventory_complete: true,
        steps: [{
          block_id: "compute", handler: "compute", tree: "main", route: "worker",
          matched_policies: [], eligible_workers: ["worker-sample"], preferred_workers: [],
          excluded_workers: [], worker_inventory: { completeness: "complete", eligible_count: 73 },
        }],
      }).catch(() => {});
      return;
    }
    if (pathname === "/workers/version-rollouts" && request.method() === "POST") {
      stagedPin = request.postDataJSON();
      return reply({ ...stagedPin, revision: "new" });
    }
    if (pathname.endsWith("/api-keys") && request.method() === "POST") {
      enrollment = request.postDataJSON();
      return reply({ id: "key-id", secret: "fixture-worker-key" });
    }
    if (pathname === "/handlers") return reply({ builtin: [], external: [] });
    if (pathname === "/continuity/effects/review") return reply({ items: [], has_more: false });

    return reply([]);
  });

  await page.goto("http://dashboard.test/workers");
  await page.getByLabel("Sequence UUID").fill(sequenceId);
  const preview = () => page.getByRole("button", { name: "Run preview" }).click();
  await Promise.all([
    page.waitForRequest((request) => request.url().includes(`/placement/plan/${sequenceId}`)),
    preview(),
  ]);
  await page.getByLabel("Preview staged version pins").check();
  await preview();
  await page.getByText("Blocked", { exact: true }).waitFor();
  releaseOld();
  assert.deepEqual(previews, [{}, { staged_pins: "true" }]);
  await page.getByText("Evaluated staged pins where present").waitFor();
  await page.getByLabel("Preview staged version pins").uncheck();
  await page.getByText("Blocked", { exact: true }).waitFor({ state: "hidden" });
  await preview();
  await page.getByText("Fleet ready", { exact: true }).waitFor();
  await page.getByText("Evaluated active pins").waitFor();
  assert.equal(await page.locator("dt").filter({ hasText: /^Eligible workers$/ }).locator("+ dd").textContent(), "73");
  await page.getByLabel("Canary worker version").fill("9.0.0");
  await page.getByText("Fleet ready", { exact: true }).waitFor({ state: "hidden" });

  await preview();
  await page.getByText("Fleet ready", { exact: true }).waitFor();
  await page.locator("#pin-tenant").fill("team-a");
  await page.locator("#pin-handler").fill("compute");
  await page.locator("#pin-version").fill("9.0.0");
  await page.getByRole("button", { name: "Stage version", exact: true }).click();
  await page.getByText("Staged version pin for compute", { exact: true }).waitFor();
  await page.getByText("Fleet ready", { exact: true }).waitFor({ state: "hidden" });
  assert.deepEqual(stagedPin, { tenant_id: "team-a", handler_name: "compute", min_version: "9.0.0" });

  await page.getByRole("button", { name: "Enroll a worker" }).click();
  await page.locator("#enrollment-tenant").fill(" team-a ");
  await page.locator("#enrollment-name").fill(" Worker A ");
  await page.getByRole("button", { name: "Create worker key" }).click();
  await page.getByText("fixture-worker-key", { exact: true }).waitFor();
  assert.equal(enrollment.tenant_id, "team-a");
  assert.equal(enrollment.name, "Worker A");
  assert.equal(await page.locator("dt").filter({ hasText: /^ORCH8_TENANT_ID$/ }).locator("+ dd").textContent(), "team-a");
  await page.getByRole("button", { name: "Hide key and finish" }).click();
  await page.getByText("fixture-worker-key", { exact: true }).waitFor({ state: "hidden" });
  log("Worker placement and enrollment browser regression passed.");
} finally {
  releaseOld();
  await browser.close();
}
