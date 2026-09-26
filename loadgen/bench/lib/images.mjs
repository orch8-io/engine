#!/usr/bin/env node
// Record the exact images of a running compose project as {service: "ref@digest"}.
// Usage: node images.mjs <project> <compose-file> <out.json>
import { spawnSync } from "node:child_process";
import { writeFileSync } from "node:fs";

const [project, composeFile, out] = process.argv.slice(2);
const capture = (cmd, argv) => {
  const result = spawnSync(cmd, argv, { encoding: "utf8" });
  return result.status === 0 ? result.stdout.trim() : "";
};

const images = {};
const listing = capture("docker", ["compose", "-p", project, "-f", composeFile, "images", "--format", "json"]);
let rows = [];
try {
  rows = JSON.parse(listing || "[]");
} catch {
  rows = listing.split("\n").filter(Boolean).map((line) => JSON.parse(line));
}
for (const row of rows) {
  const service = row.ContainerName ?? row.Service ?? "unknown";
  const ref = `${row.Repository}:${row.Tag}`;
  const digest = capture("docker", ["image", "inspect", "--format", "{{index .RepoDigests 0}}", row.ID ?? ref]);
  images[service] = digest || `${ref} (local build ${String(row.ID ?? "").slice(0, 19)})`;
}
writeFileSync(out, `${JSON.stringify(images, null, 2)}\n`);
