import { it } from "node:test";
import assert from "node:assert/strict";
import { federationSigningBytes } from "./federation-signing.ts";

for (const fraction of ["000", "001", "010", "100", "999"]) {
  it(`federation signing matches Chrono timestamps at millisecond ${fraction}`, () => {
    const timestamp = `2026-10-22T17:00:00.${fraction}Z`;
    const unsigned = { id: "message", issued_at: timestamp, expires_at: timestamp, signature: "" };
    const canonical = fraction === "000" ? "2026-10-22T17:00:00Z" : timestamp;
    assert.equal(federationSigningBytes(unsigned).toString(), JSON.stringify({
      id: "message", issued_at: canonical, expires_at: canonical, signature: "",
    }));
  });
}
