// Produces the CDN bundle (IIFE, self-registering) and reports gzipped sizes.
import { build } from "esbuild";
import { gzipSync } from "node:zlib";
import { readFileSync, mkdirSync } from "node:fs";

const report = process.argv.includes("--report");
const outdir = new URL("../dist/cdn/", import.meta.url);
mkdirSync(outdir, { recursive: true });

const common = {
  bundle: true,
  minify: true,
  target: "es2020",
  legalComments: "none",
  logLevel: report ? "silent" : "warning",
};

await build({
  ...common,
  entryPoints: [new URL("../src/index.ts", import.meta.url).pathname],
  format: "iife",
  globalName: "Orch8Embed",
  outfile: new URL("orch8-embed.min.js", outdir).pathname,
});
await build({
  ...common,
  entryPoints: [new URL("../src/index.ts", import.meta.url).pathname],
  format: "esm",
  outfile: new URL("orch8-embed.esm.min.js", outdir).pathname,
});

for (const name of ["orch8-embed.min.js", "orch8-embed.esm.min.js"]) {
  const buf = readFileSync(new URL(name, outdir));
  const kb = (n) => `${(n / 1024).toFixed(1)} KiB`;
  console.log(`${name}: ${kb(buf.length)} min, ${kb(gzipSync(buf).length)} gzip`);
}
