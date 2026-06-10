import { test } from "node:test";
import * as assert from "node:assert/strict";
import { fmtCount, fmtRate, fmtPct, fmtDelta, fmtUsd } from "../src/lib/fmt.ts";

// ---------------------------------------------------------------------------
// fmtCount
// ---------------------------------------------------------------------------

test("fmtCount(0) returns '0'", () => {
  assert.equal(fmtCount(0), "0");
});

test("fmtCount(1) returns '1'", () => {
  assert.equal(fmtCount(1), "1");
});

test("fmtCount(999) returns '999'", () => {
  assert.equal(fmtCount(999), "999");
});

test("fmtCount(1000) is locale-formatted with grouping", () => {
  const result = fmtCount(1000);
  // toLocaleString output varies by runtime locale; accept common forms
  assert.ok(
    result === "1,000" || result === "1.000" || result === "1 000",
    `expected locale-grouped string, got "${result}"`,
  );
});

test("fmtCount(1234) is locale-formatted with grouping", () => {
  const result = fmtCount(1234);
  assert.ok(
    result.includes("1") && result.includes("234"),
    `expected locale-grouped "1,234" variant, got "${result}"`,
  );
});

test("fmtCount(99999) is locale-formatted (upper bound of grouped range)", () => {
  const result = fmtCount(99999);
  assert.ok(
    result.includes("99") && result.includes("999"),
    `expected locale-grouped "99,999" variant, got "${result}"`,
  );
});

test("fmtCount(100000) returns '100.0K'", () => {
  assert.equal(fmtCount(100_000), "100.0K");
});

test("fmtCount(142300) returns '142.3K'", () => {
  assert.equal(fmtCount(142_300), "142.3K");
});

test("fmtCount(999999) returns '1000.0K' at K/M boundary", () => {
  assert.equal(fmtCount(999_999), "1000.0K");
});

test("fmtCount(1000000) returns '1.0M'", () => {
  assert.equal(fmtCount(1_000_000), "1.0M");
});

test("fmtCount(1400000) returns '1.4M'", () => {
  assert.equal(fmtCount(1_400_000), "1.4M");
});

test("fmtCount(1000000000) returns '1.0B'", () => {
  assert.equal(fmtCount(1_000_000_000), "1.0B");
});

test("fmtCount(NaN) returns em-dash", () => {
  assert.equal(fmtCount(NaN), "—");
});

test("fmtCount(Infinity) returns em-dash", () => {
  assert.equal(fmtCount(Infinity), "—");
});

test("fmtCount(-500) returns '-500'", () => {
  assert.equal(fmtCount(-500), "-500");
});

test("fmtCount(5.7) truncates to '5'", () => {
  assert.equal(fmtCount(5.7), "5");
});

test("fmtCount(-Infinity) returns em-dash", () => {
  assert.equal(fmtCount(-Infinity), "—");
});

test("fmtCount(-150000) formats with K suffix", () => {
  const result = fmtCount(-150_000);
  assert.ok(result.includes("K"), `expected K suffix, got "${result}"`);
  assert.equal(result, "-150.0K");
});

// ---------------------------------------------------------------------------
// fmtRate
// ---------------------------------------------------------------------------

test("fmtRate(0) returns '0'", () => {
  assert.equal(fmtRate(0), "0");
});

test("fmtRate(NaN) returns em-dash", () => {
  assert.equal(fmtRate(NaN), "—");
});

test("fmtRate(Infinity) returns em-dash", () => {
  assert.equal(fmtRate(Infinity), "—");
});

test("fmtRate(0.05) returns '0.05' (below 0.1 threshold)", () => {
  assert.equal(fmtRate(0.05), "0.05");
});

test("fmtRate(0.09) returns '0.09' (just below 0.1)", () => {
  assert.equal(fmtRate(0.09), "0.09");
});

test("fmtRate(0.1) returns '0.1' (at 0.1 boundary, 1 digit default)", () => {
  assert.equal(fmtRate(0.1), "0.1");
});

test("fmtRate(5.678) returns '5.7' with default 1 digit", () => {
  assert.equal(fmtRate(5.678), "5.7");
});

test("fmtRate(5.678, 2) returns '5.68' with custom digits", () => {
  assert.equal(fmtRate(5.678, 2), "5.68");
});

test("fmtRate(9.99) rounds to '10.0' at boundary", () => {
  assert.equal(fmtRate(9.99), "10.0");
});

test("fmtRate(10) delegates to fmtCount and returns '10'", () => {
  assert.equal(fmtRate(10), "10");
});

test("fmtRate(1500) delegates to fmtCount for locale grouping", () => {
  const result = fmtRate(1500);
  // fmtCount(1500) goes through the locale branch
  assert.ok(
    result.includes("1") && result.includes("500"),
    `expected locale-grouped "1,500" variant, got "${result}"`,
  );
});

test("fmtRate(-0.05) returns '-0.05' (negative below 0.1)", () => {
  assert.equal(fmtRate(-0.05), "-0.05");
});

// ---------------------------------------------------------------------------
// fmtPct
// ---------------------------------------------------------------------------

test("fmtPct(NaN) returns em-dash", () => {
  assert.equal(fmtPct(NaN), "—");
});

test("fmtPct(Infinity) returns em-dash", () => {
  assert.equal(fmtPct(Infinity), "—");
});

test("fmtPct(100) returns '100%'", () => {
  assert.equal(fmtPct(100), "100%");
});

test("fmtPct(99.95) returns '100%' (at clamping threshold)", () => {
  assert.equal(fmtPct(99.95), "100%");
});

test("fmtPct(99.94) returns '100%' (toFixed(0) rounds 99.94 to 100)", () => {
  // 99.94 < 99.95, so it bypasses the >= 99.95 clamp, but toFixed(0)
  // rounds 99.94 up to "100", producing "100%" via the >= 10 branch.
  assert.equal(fmtPct(99.94), "100%");
});

test("fmtPct(0) returns '0.0%'", () => {
  assert.equal(fmtPct(0), "0.0%");
});

test("fmtPct(5.5) returns '5.5%'", () => {
  assert.equal(fmtPct(5.5), "5.5%");
});

test("fmtPct(9.99) rounds to '10.0%' (boundary between branches)", () => {
  assert.equal(fmtPct(9.99), "10.0%");
});

test("fmtPct(50) returns '50%'", () => {
  assert.equal(fmtPct(50), "50%");
});

test("fmtPct(99.4) returns '99%'", () => {
  assert.equal(fmtPct(99.4), "99%");
});

test("fmtPct(0.1, 2) returns '0.10%' with custom digits", () => {
  assert.equal(fmtPct(0.1, 2), "0.10%");
});

// ---------------------------------------------------------------------------
// fmtDelta
// ---------------------------------------------------------------------------

test("fmtDelta(0) returns empty string", () => {
  assert.equal(fmtDelta(0), "");
});

test("fmtDelta(NaN) returns empty string", () => {
  assert.equal(fmtDelta(NaN), "");
});

test("fmtDelta(Infinity) returns empty string (not finite)", () => {
  assert.equal(fmtDelta(Infinity), "");
});

test("fmtDelta(3) returns '+3'", () => {
  assert.equal(fmtDelta(3), "+3");
});

test("fmtDelta(-1) returns '−1' with true minus sign U+2212", () => {
  const result = fmtDelta(-1);
  assert.equal(result, "−1");
  // Verify the minus is U+2212, not a hyphen-minus U+002D
  assert.equal(result.charCodeAt(0), 0x2212);
});

test("fmtDelta(1500) returns positive locale-grouped value", () => {
  const result = fmtDelta(1500);
  assert.ok(result.startsWith("+"), `expected + prefix, got "${result}"`);
  // fmtCount(1500) is locale-formatted in the 1000-99999 range
  assert.ok(
    result.includes("1") && result.includes("500"),
    `expected grouped "1,500" variant, got "${result}"`,
  );
});

test("fmtDelta(-150000) returns '−150.0K'", () => {
  assert.equal(fmtDelta(-150_000), "−150.0K");
});

test("fmtDelta(1000000) returns '+1.0M'", () => {
  assert.equal(fmtDelta(1_000_000), "+1.0M");
});

// ---------------------------------------------------------------------------
// fmtUsd — estimated LLM costs (GET /usage cost_usd / total_cost_usd)
// ---------------------------------------------------------------------------

test("fmtUsd(null) returns em-dash (unknown model)", () => {
  assert.equal(fmtUsd(null), "—");
});

test("fmtUsd(undefined) returns em-dash", () => {
  assert.equal(fmtUsd(undefined), "—");
});

test("fmtUsd(NaN) returns em-dash", () => {
  assert.equal(fmtUsd(NaN), "—");
});

test("fmtUsd(Infinity) returns em-dash", () => {
  assert.equal(fmtUsd(Infinity), "—");
});

test("fmtUsd(0) returns '$0.0000' (4 decimals below $1)", () => {
  assert.equal(fmtUsd(0), "$0.0000");
});

test("fmtUsd(0.0123) returns '$0.0123' (4 decimals below $1)", () => {
  assert.equal(fmtUsd(0.0123), "$0.0123");
});

test("fmtUsd(0.5) returns '$0.5000' (just below the $1 boundary)", () => {
  assert.equal(fmtUsd(0.5), "$0.5000");
});

test("fmtUsd(1) returns '$1.00' (2 decimals at $1)", () => {
  assert.equal(fmtUsd(1), "$1.00");
});

test("fmtUsd(12.3456) returns '$12.35' (2 decimals above $1)", () => {
  assert.equal(fmtUsd(12.3456), "$12.35");
});

test("fmtUsd(15.5) formats a window total as '$15.50'", () => {
  // total_cost_usd from GET /usage is a plain number — the headline stat
  // renders it through the same helper as the per-row costs.
  assert.equal(fmtUsd(15.5), "$15.50");
});

test("fmtUsd(1234.5) returns '$1234.50'", () => {
  assert.equal(fmtUsd(1234.5), "$1234.50");
});
