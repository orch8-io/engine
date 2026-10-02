import { mkdtempSync, readFileSync, existsSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { afterAll, describe, expect, it } from "vitest";
// @ts-expect-error plain ESM script without types
import { scaffold } from "../bin/create-orch8-embed.mjs";

const dir = mkdtempSync(join(tmpdir(), "orch8-embed-"));
afterAll(() => rmSync(dir, { recursive: true, force: true }));

describe("create-orch8-embed", () => {
  it("copies the template and pins @orch8/embed to the npm release", () => {
    const target = scaffold(join(dir, "My App"), { log: () => {} }) as string;
    const pkg = JSON.parse(readFileSync(join(target, "package.json"), "utf8"));
    expect(pkg.name).toBe("my-app");
    expect(pkg.private).toBe(true);
    expect(pkg.bin).toBeUndefined();
    expect(pkg.dependencies["@orch8/embed"]).toBe("^0.1.0");
    expect(pkg.devDependencies.vitest).toBeUndefined();
    for (const f of ["app/api/orch8/embed-token/route.ts", "lib/embed-token.ts", ".env.example", ".gitignore", "README.md", "orch8/customer-onboarding.json"]) {
      expect(existsSync(join(target, f)), f).toBe(true);
    }
    for (const f of ["bin", "test", "node_modules", ".next", "pnpm-lock.yaml"]) {
      expect(existsSync(join(target, f)), f).toBe(false);
    }
  });

  it("refuses a non-empty target", () => {
    expect(() => scaffold(join(dir, "My App"), { log: () => {} })).toThrow(/not empty/);
  });
});
