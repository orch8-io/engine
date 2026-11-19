import assert from "node:assert/strict";
import { spawnSync } from "node:child_process";
import { log } from "node:console";
import { mkdtemp, readFile, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { basename, join, resolve } from "node:path";
import process from "node:process";
import { URL, fileURLToPath } from "node:url";

const cli = resolve(process.env.ORCH8_BIN ?? "../target/debug/orch8");
const { chromium } = await import(process.env.PLAYWRIGHT_MODULE ?? "playwright");
const dist = fileURLToPath(new URL("../dist", import.meta.url));
const candidateId = "11111111-1111-4111-a111-111111111111";
const baselineRun = "22222222-2222-4222-a222-222222222222";
const otherRun = "33333333-3333-4333-a333-333333333333";
const releaseId = "44444444-4444-4444-a444-444444444444";
const timestamp = "2026-01-01T00:00:00Z";
const step = (id) => ({ type: "step", id, handler: "noop", params: {} });
const sequence = {
  id: candidateId, tenant_id: "test", namespace: "default", name: "inspection", version: 2,
  created_at: timestamp, blocks: [step("check"), step("review.final")],
};
const contract = {
  schema_version: 1, sequence_name: "inspection", sequence_version: 2, unmocked_handlers: "empty_success",
  cases: [{
    name: "recorded path", input: { "region,name": "test" },
    expect: { terminal_state: "completed", path: { traversed: ["check"] }, skipped_blocks: ["review.final"] },
  }],
};
const release = {
  id: releaseId, tenant_id: "test", namespace: "default", candidate_sequence_id: candidateId,
  baseline_version: 1, candidate_version: 2, updated_at: timestamp,
  validation_summary: { divergences: [otherRun, baselineRun].map((id) => ({
    instance_id: id, recorded_state: "completed", replayed_state: "completed",
    blocks_added: ["review.final"], blocks_removed: [],
  })) },
};

const browser = await chromium.launch({ executablePath: process.env.CHROMIUM_PATH });
const directory = await mkdtemp(join(tmpdir(), "orch8-replay-browser-"));
try {
  const page = await browser.newPage();
  page.setDefaultTimeout(5000);
  await page.addInitScript(() => globalThis.localStorage.setItem("orch8_api_url", "http://engine.test"));
  const requests = [];
  let allowStale;
  const stalledReply = new Promise((resolve) => { allowStale = resolve; });
  await page.route("**/*", async (route) => {
    const request = route.request();
    const { hostname, pathname } = new URL(request.url());
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
    const reply = (json, status = 200) => route.fulfill({ json, status, headers });
    if (pathname === `/sequences/${candidateId}`) return reply(sequence);
    if (pathname === "/releases") return reply([release]);
    if (pathname.endsWith("/fixture") && request.method() === "POST") {
      requests.push({ pathname, body: request.postDataJSON() });
      if (pathname.includes(otherRun)) {
        await stalledReply;
        await reply({ sequence, contract, reproduces_divergence: false }).catch(() => {});
        return;
      }
      if (requests.filter((call) => call.pathname.includes(baselineRun)).length === 1) {
        return reply({ error: "fixture unavailable" }, 422);
      }
      return reply({ sequence, contract, reproduces_divergence: true });
    }
    return reply([]);
  });

  await page.goto(`http://dashboard.test/sequences/${candidateId}`);
  await page.getByText("Debug an execution of v2", { exact: true }).click();
  await page.getByText("Replay divergence", { exact: true }).click();
  await page.getByRole("button", { name: "Load release replay evidence" }).click();
  await page.getByLabel("Release validation").selectOption(releaseId);
  const selector = page.getByLabel("Divergent execution");
  const openForm = () => page.getByText("Create regression fixture", { exact: true }).click();
  const generate = () => page.getByRole("button", { name: "Generate fixture" }).click();

  await selector.selectOption("0");
  await openForm();
  await page.getByLabel(/Output keys/).fill("invalid");
  await generate();
  await page.getByText(/Enter output fields/).waitFor();
  assert.equal(requests.length, 0);
  await page.getByLabel(/Output keys/).clear();
  await Promise.all([
    page.waitForRequest((request) => request.url().includes(otherRun) && request.method() === "POST"),
    generate(),
  ]);

  await selector.selectOption("1");
  allowStale();
  await openForm();
  assert.equal(await page.getByRole("button", { name: /Download/ }).count(), 0);
  await page.getByLabel(/Input keys/).fill('["region,name"]');
  await page.getByLabel(/Output keys/).fill('{"review.final":["score.total"]}');
  await generate();
  await page.getByRole("alert").waitFor();
  assert.equal(await page.getByRole("button", { name: /Download/ }).count(), 0);
  await generate();
  await page.getByText(/reproduces the saved candidate divergence/).waitFor();
  assert.deepEqual(requests.at(-1), {
    pathname: `/api/v1/releases/${releaseId}/divergences/${baselineRun}/fixture`,
    body: { input_keys: ["region,name"], output_keys: { "review.final": ["score.total"] } },
  });

  const stem = `replay-${releaseId}-${baselineRun}`;
  for (const [label, suffix] of [["Download sequence", ".json"], ["Download contract", ".contracts.json"]]) {
    const [file] = await Promise.all([page.waitForEvent("download"), page.getByRole("button", { name: label }).click()]);
    assert.equal(file.suggestedFilename(), stem + suffix);
    await file.saveAs(join(directory, stem + suffix));
  }

  const run = () => spawnSync(cli, ["test", "run", stem + ".contracts.json", "--report", "json"], {
    cwd: directory, encoding: "utf8", timeout: 10000,
  });
  const broken = run();
  assert.ifError(broken.error);
  assert.equal(JSON.parse(broken.stdout).passed, false);
  assert.notEqual(broken.status, 0);
  const definitionPath = join(directory, stem + ".json");
  const downloaded = JSON.parse(await readFile(definitionPath, "utf8"));
  downloaded.blocks.pop();
  await writeFile(definitionPath, JSON.stringify(downloaded));
  const fixed = run();
  assert.ifError(fixed.error);
  assert.equal(fixed.status, 0, fixed.stderr);
  assert.equal(JSON.parse(fixed.stdout).passed, true);
  log("Replay browser-to-CLI regression passed.");
} finally {
  await browser.close();
  await rm(directory, { recursive: true, force: true });
}
