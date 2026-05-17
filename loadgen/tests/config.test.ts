import { test, beforeEach, afterEach } from "node:test";
import * as assert from "node:assert/strict";
import { parseArgs, renderConfig } from "../src/config.ts";
import type { LoadgenConfig } from "../src/config.ts";

// ── env isolation ────────────────────────────────────────────────────
// parseArgs reads process.env, so we snapshot and restore around every
// test to prevent cross-contamination.

const ENV_KEYS = [
  "LOADGEN_PRESET",
  "LOADGEN_SERVER",
  "ORCH8_E2E_BASE_URL",
  "ORCH8_E2E_PORT",
  "LOADGEN_DURATION",
  "LOADGEN_SEED",
  "LOADGEN_TENANTS",
  "LOADGEN_NAMESPACES",
  "LOADGEN_ENABLE_LLM",
  "LOADGEN_VERBOSE",
] as const;

let savedEnv: Record<string, string | undefined>;

beforeEach(() => {
  savedEnv = {};
  for (const k of ENV_KEYS) {
    savedEnv[k] = process.env[k];
    delete process.env[k];
  }
});

afterEach(() => {
  for (const k of ENV_KEYS) {
    if (savedEnv[k] === undefined) delete process.env[k];
    else process.env[k] = savedEnv[k];
  }
});

// ── helpers ──────────────────────────────────────────────────────────

function defaults(): LoadgenConfig {
  return parseArgs([]);
}

// ── 1-5: preset selection ────────────────────────────────────────────

test("1 - parseArgs with empty argv uses 'steady' preset defaults", () => {
  const cfg = defaults();
  assert.equal(cfg.preset, "steady");
  assert.equal(cfg.rate, 5);
  assert.equal(cfg.concurrency, 200);
  assert.equal(cfg.workers, 8);
  assert.ok(Math.abs(cfg.signalRate - 0.1) < 1e-9);
});

test("2 - parseArgs with --preset=light uses light preset values", () => {
  const cfg = parseArgs(["--preset=light"]);
  assert.equal(cfg.preset, "light");
  assert.equal(cfg.rate, 1);
  assert.equal(cfg.concurrency, 20);
  assert.equal(cfg.workers, 2);
  assert.ok(Math.abs(cfg.signalRate - 1 / 30) < 1e-9);
});

test("3 - parseArgs with --preset=stress uses stress preset values", () => {
  const cfg = parseArgs(["--preset=stress"]);
  assert.equal(cfg.preset, "stress");
  assert.equal(cfg.rate, 50);
  assert.equal(cfg.concurrency, 2000);
  assert.equal(cfg.workers, 32);
  assert.equal(cfg.signalRate, 0.5);
});

test("4 - parseArgs with --preset=steady uses steady preset values", () => {
  const cfg = parseArgs(["--preset=steady"]);
  assert.equal(cfg.preset, "steady");
  assert.equal(cfg.rate, 5);
  assert.equal(cfg.concurrency, 200);
  assert.equal(cfg.workers, 8);
  assert.ok(Math.abs(cfg.signalRate - 0.1) < 1e-9);
});

test("5 - parseArgs with invalid preset throws", () => {
  assert.throws(() => parseArgs(["--preset=turbo"]), {
    message: /Unknown preset.*turbo/,
  });
});

// ── 6-9: individual numeric overrides ───────────────────────────────

test("6 - parseArgs --rate=10 overrides preset rate", () => {
  const cfg = parseArgs(["--rate=10"]);
  assert.equal(cfg.rate, 10);
  // other values stay at steady defaults
  assert.equal(cfg.concurrency, 200);
});

test("7 - parseArgs --concurrency=500 overrides preset concurrency", () => {
  const cfg = parseArgs(["--concurrency=500"]);
  assert.equal(cfg.concurrency, 500);
  assert.equal(cfg.rate, 5); // steady default
});

test("8 - parseArgs --workers=16 overrides preset workers", () => {
  const cfg = parseArgs(["--workers=16"]);
  assert.equal(cfg.workers, 16);
  assert.equal(cfg.rate, 5);
});

test("9 - parseArgs --signal-rate=0.5 overrides preset signalRate", () => {
  const cfg = parseArgs(["--signal-rate=0.5"]);
  assert.equal(cfg.signalRate, 0.5);
  assert.equal(cfg.rate, 5);
});

// ── 10-13: duration and seed ─────────────────────────────────────────

test("10 - parseArgs --duration=60 sets duration", () => {
  const cfg = parseArgs(["--duration=60"]);
  assert.equal(cfg.duration, 60);
});

test("11 - parseArgs without --duration sets null", () => {
  const cfg = defaults();
  assert.equal(cfg.duration, null);
});

test("12 - parseArgs --seed=42 sets seed", () => {
  const cfg = parseArgs(["--seed=42"]);
  assert.equal(cfg.seed, 42);
});

test("13 - parseArgs without --seed uses Date.now()-like value (numeric)", () => {
  const before = Date.now();
  const cfg = defaults();
  const after = Date.now();
  assert.equal(typeof cfg.seed, "number");
  // The seed should be close to current time (within 1s margin).
  assert.ok(cfg.seed >= before - 1000, "seed is not unreasonably old");
  assert.ok(cfg.seed <= after + 1000, "seed is not in the future");
});

// ── 14-17: list and boolean flags ────────────────────────────────────

test("14 - parseArgs --tenants=a,b,c splits comma-separated", () => {
  const cfg = parseArgs(["--tenants=a,b,c"]);
  assert.deepEqual(cfg.tenants, ["a", "b", "c"]);
});

test("15 - parseArgs --namespaces=prod splits single value", () => {
  const cfg = parseArgs(["--namespaces=prod"]);
  assert.deepEqual(cfg.namespaces, ["prod"]);
});

test("16 - parseArgs --enable-llm=true sets enableLlm true", () => {
  const cfg = parseArgs(["--enable-llm=true"]);
  assert.equal(cfg.enableLlm, true);
});

test("17 - parseArgs --verbose=true sets verbose", () => {
  const cfg = parseArgs(["--verbose=true"]);
  assert.equal(cfg.verbose, true);
});

// ── 18-21: default values for lists and booleans ─────────────────────

test("18 - parseArgs with no flags has default tenants (3 items)", () => {
  const cfg = defaults();
  assert.equal(cfg.tenants.length, 3);
  assert.deepEqual(cfg.tenants, ["loadgen-a", "loadgen-b", "loadgen-c"]);
});

test("19 - parseArgs with no flags has default namespaces (2 items)", () => {
  const cfg = defaults();
  assert.equal(cfg.namespaces.length, 2);
  assert.deepEqual(cfg.namespaces, ["prod", "staging"]);
});

test("20 - parseArgs enableLlm defaults to false", () => {
  const cfg = defaults();
  assert.equal(cfg.enableLlm, false);
});

test("21 - parseArgs verbose defaults to false", () => {
  const cfg = defaults();
  assert.equal(cfg.verbose, false);
});

// ── 22-23: baseUrl ───────────────────────────────────────────────────

test("22 - parseArgs default baseUrl contains localhost", () => {
  const cfg = defaults();
  assert.ok(cfg.baseUrl.includes("localhost"), `expected 'localhost' in ${cfg.baseUrl}`);
});

test("23 - parseArgs --server=http://custom:9090 sets baseUrl", () => {
  const cfg = parseArgs(["--server=http://custom:9090"]);
  assert.equal(cfg.baseUrl, "http://custom:9090");
});

// ── 24-30: renderConfig ──────────────────────────────────────────────

function sampleConfig(overrides: Partial<LoadgenConfig> = {}): LoadgenConfig {
  return {
    baseUrl: "http://localhost:18080",
    preset: "steady",
    rate: 5,
    concurrency: 200,
    workers: 8,
    signalRate: 0.1,
    duration: null,
    seed: 42,
    tenants: ["t-a", "t-b"],
    namespaces: ["prod"],
    enableLlm: false,
    verbose: false,
    ...overrides,
  };
}

test("24 - renderConfig returns string containing preset name", () => {
  const out = renderConfig(sampleConfig({ preset: "stress" }));
  assert.ok(out.includes("preset=stress"), `missing preset in: ${out}`);
});

test("25 - renderConfig returns string containing rate", () => {
  const out = renderConfig(sampleConfig({ rate: 42 }));
  assert.ok(out.includes("rate=42/s"), `missing rate in: ${out}`);
});

test("26 - renderConfig with duration=null shows 'forever'", () => {
  const out = renderConfig(sampleConfig({ duration: null }));
  assert.ok(out.includes("duration=forever"), `missing 'forever' in: ${out}`);
});

test("27 - renderConfig with duration=60 shows '60'", () => {
  const out = renderConfig(sampleConfig({ duration: 60 }));
  assert.ok(out.includes("duration=60"), `missing 'duration=60' in: ${out}`);
});

test("28 - renderConfig shows tenants list", () => {
  const out = renderConfig(sampleConfig({ tenants: ["alpha", "beta"] }));
  assert.ok(out.includes("tenants=[alpha,beta]"), `missing tenants in: ${out}`);
});

test("29 - renderConfig shows namespaces list", () => {
  const out = renderConfig(sampleConfig({ namespaces: ["prod", "staging"] }));
  assert.ok(out.includes("namespaces=[prod,staging]"), `missing namespaces in: ${out}`);
});

test("30 - renderConfig shows llm on/off", () => {
  const off = renderConfig(sampleConfig({ enableLlm: false }));
  assert.ok(off.includes("llm=off"), `expected 'llm=off' in: ${off}`);

  const on = renderConfig(sampleConfig({ enableLlm: true }));
  assert.ok(on.includes("llm=on"), `expected 'llm=on' in: ${on}`);
});
