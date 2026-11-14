import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import { createHash, webcrypto } from "node:crypto";
import test from "node:test";

const fixture = JSON.parse(readFileSync(new URL("../contracts/vectors/delegation-definition-v1.json", import.meta.url), "utf8"));
const envelope = fixture.envelope;
const bytes = Uint8Array.from(atob(envelope.payload_base64), (character) => character.charCodeAt(0));
const hash = (value) => createHash("sha256").update(value).digest("hex");

test("the retained vector has identical Node and browser WebCrypto byte identities", async () => {
  assert.equal(bytes.length, envelope.reference.payload_bytes);
  assert.equal(hash(bytes), envelope.reference.payload_sha256);
  const browserHash = Array.from(new Uint8Array(await webcrypto.subtle.digest("SHA-256", bytes)),
    (value) => value.toString(16).padStart(2, "0")).join("");
  assert.equal(browserHash, envelope.reference.payload_sha256);
  assert.equal(typeof envelope.scope.claim_epoch, "string");
  assert.equal(BigInt(envelope.scope.claim_epoch), 18446744073709551615n);
  const text = new TextDecoder("utf-8", { fatal: true }).decode(bytes);
  for (const literal of ["9007199254740993", "18446744073709551615", '"float":1.0',
    '"negative_zero":-0.0', '"subnormal":5e-324', '"nul":"a\\u0000b"', "\uE000", "\u{10000}"]) {
    assert.ok(text.includes(literal), `missing retained literal ${literal}`);
  }
});

test("JavaScript parse/stringify cannot be substituted for the authenticated payload bytes", () => {
  const parsed = JSON.parse(new TextDecoder().decode(bytes));
  assert.equal(parsed.definitions[0].definition.blocks[0].params.over53, 9007199254740992);
  assert.notEqual(hash(JSON.stringify(parsed)), envelope.reference.payload_sha256);
  const changed = bytes.slice();
  changed[changed.length - 1] ^= 1;
  assert.notEqual(hash(changed), envelope.reference.payload_sha256);
});


const inputFixture = JSON.parse(readFileSync(new URL("../contracts/vectors/delegation-input-v1.json", import.meta.url), "utf8"));
const inputBytes = Uint8Array.from(atob(inputFixture.envelope.payload_base64), (c) => c.charCodeAt(0));
test("input bytes have the same Node/WebCrypto identity and logical UTF-8 budget", async () => {
  assert.deepEqual(inputFixture.envelope.reference, inputFixture.reference);
  assert.equal(inputBytes.length, inputFixture.reference.payload_bytes);
  assert.equal(hash(inputBytes), inputFixture.reference.payload_sha256);
  const digest = Array.from(new Uint8Array(await webcrypto.subtle.digest("SHA-256", inputBytes)), (v) => v.toString(16).padStart(2, "0")).join("");
  assert.equal(digest, inputFixture.reference.payload_sha256);
  assert.equal(BigInt(inputFixture.envelope.scope.claim_epoch), 18446744073709551615n);
  const utf8 = (v) => new TextEncoder().encode(v).length;
  const cost = (v) => 64 + (typeof v === "string" ? utf8(v) : Array.isArray(v) ? v.reduce((n, x) => n + cost(x), 0) : v && typeof v === "object" ? Object.entries(v).reduce((n, [k, x]) => n + 64 + utf8(k) + cost(x), 0) : 0);
  assert.equal(cost(JSON.parse(new TextDecoder().decode(inputBytes))), inputFixture.reference.expanded_bytes);
});
test("input JSON must reach the Rust verifier as retained bytes rather than a host JSON round trip", () => {
  const parsed = JSON.parse(new TextDecoder().decode(inputBytes));
  assert.equal(parsed.above_js_safe, 9007199254740992);
  assert.notEqual(BigInt(parsed.large), 18446744073709551615n);
  assert.notEqual(hash(JSON.stringify(parsed)), inputFixture.reference.payload_sha256);
});
