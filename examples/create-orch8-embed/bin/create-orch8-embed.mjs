#!/usr/bin/env node
// npx create-orch8-embed my-app
// Copies this starter into ./my-app, points @orch8/embed at the npm release
// and prints the next steps.
import { cpSync, existsSync, mkdirSync, readdirSync, readFileSync, writeFileSync } from "node:fs";
import { basename, dirname, join, resolve } from "node:path";
import { fileURLToPath } from "node:url";

const TEMPLATE_DIR = resolve(dirname(fileURLToPath(import.meta.url)), "..");
const SKIP = new Set(["node_modules", ".next", "bin", "test", "pnpm-lock.yaml", "pnpm-workspace.yaml", "vitest.config.ts", "next-env.d.ts", ".env", ".env.local", "tsconfig.tsbuildinfo"]);
const DEFAULT_GITIGNORE = "node_modules/\n.next/\nnext-env.d.ts\n*.tsbuildinfo\n.env\n.env.local\n.env.*.local\n";

export function scaffold(targetArg, { log = console.log } = {}) {
  if (!targetArg || targetArg.startsWith("-")) throw new Error("Usage: create-orch8-embed <directory>");
  const target = resolve(process.cwd(), targetArg);
  if (existsSync(target) && readdirSync(target).length > 0) throw new Error(`${target} exists and is not empty`);
  mkdirSync(target, { recursive: true });

  for (const entry of readdirSync(TEMPLATE_DIR)) {
    if (SKIP.has(entry)) continue;
    cpSync(join(TEMPLATE_DIR, entry), join(target, entry), {
      recursive: true,
      filter: (src) => !SKIP.has(basename(src)),
    });
  }
  // npm strips .gitignore from published packages; recreate it.
  if (!existsSync(join(target, ".gitignore"))) writeFileSync(join(target, ".gitignore"), DEFAULT_GITIGNORE);

  const pkg = JSON.parse(readFileSync(join(TEMPLATE_DIR, "package.json"), "utf8"));
  const name = basename(target).toLowerCase().replace(/[^a-z0-9._-]+/g, "-").replace(/^[._-]+/, "") || "orch8-embed-app";
  const out = {
    name,
    version: "0.1.0",
    private: true,
    type: pkg.type,
    scripts: Object.fromEntries(Object.entries(pkg.scripts).filter(([k]) => k !== "test")),
    dependencies: { ...pkg.dependencies, "@orch8/embed": pkg.orch8?.embedVersion ?? "latest" },
    devDependencies: Object.fromEntries(Object.entries(pkg.devDependencies).filter(([k]) => k !== "vitest")),
    engines: pkg.engines,
  };
  writeFileSync(join(target, "package.json"), `${JSON.stringify(out, null, 2)}\n`);

  log(`\nCreated ${name} in ${target}\n`);
  log("Next steps:");
  log(`  cd ${targetArg}`);
  log("  cp .env.example .env.local   # leave ORCH8_URL empty to start with the mock engine");
  log("  pnpm install");
  log("  pnpm dev                     # http://localhost:3000\n");
  return target;
}

const invokedDirectly = process.argv[1] && resolve(process.argv[1]) === fileURLToPath(import.meta.url);
if (invokedDirectly) {
  try {
    scaffold(process.argv[2]);
  } catch (err) {
    console.error(err instanceof Error ? err.message : err);
    process.exit(1);
  }
}
