import { createHash } from "node:crypto";
import { chmod, mkdir, readFile, rename, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { dirname, join, resolve } from "node:path";
import { spawnSync } from "node:child_process";
import process from "node:process";
import { fileURLToPath } from "node:url";

const repo = process.env.ORCH8_REPOSITORY ?? "orch8-io/engine";
const packageRoot = dirname(fileURLToPath(import.meta.url));
const binRoot = join(packageRoot, "bin", "native");

function target() {
  const key = `${process.platform}-${process.arch}`;
  const targets = {
    "linux-x64": "x86_64-unknown-linux-gnu",
    "linux-arm64": "aarch64-unknown-linux-gnu",
    "darwin-x64": "x86_64-apple-darwin",
    "darwin-arm64": "aarch64-apple-darwin",
    "win32-x64": "x86_64-pc-windows-msvc"
  };
  if (!targets[key]) throw new Error(`unsupported platform: ${key}`);
  return targets[key];
}

async function releaseTag() {
  const requested = process.env.ORCH8_VERSION;
  if (requested && requested !== "latest") return requested.startsWith("v") ? requested : `v${requested}`;
  const response = await fetch(`https://api.github.com/repos/${repo}/releases/latest`, {
    headers: { "user-agent": "@orch8/cli installer" }
  });
  if (!response.ok) throw new Error(`release lookup failed: HTTP ${response.status}`);
  return (await response.json()).tag_name;
}

async function download(url, path) {
  const response = await fetch(url, { redirect: "follow" });
  if (!response.ok) throw new Error(`download failed: ${url} (HTTP ${response.status})`);
  await writeFile(path, Buffer.from(await response.arrayBuffer()));
}

export async function install() {
  const triple = target();
  if (process.argv.includes("--check") || process.env.ORCH8_SKIP_BINARY_INSTALL === "1") return triple;
  const tag = await releaseTag();
  const extension = process.platform === "win32" ? "zip" : "tar.gz";
  const archive = `orch8-${tag}-${triple}.${extension}`;
  const base = `https://github.com/${repo}/releases/download/${tag}`;
  const work = join(tmpdir(), `orch8-${process.pid}-${Date.now()}`);
  await mkdir(work, { recursive: true });
  try {
    await Promise.all([
      download(`${base}/${archive}`, join(work, archive)),
      download(`${base}/${archive}.sha256`, join(work, `${archive}.sha256`))
    ]);
    const expected = (await readFile(join(work, `${archive}.sha256`), "utf8")).trim().split(/\s+/)[0].toLowerCase();
    const actual = createHash("sha256").update(await readFile(join(work, archive))).digest("hex");
    if (actual !== expected) throw new Error(`checksum mismatch for ${archive}`);
    const unpack = join(work, "unpack");
    await mkdir(unpack);
    const command = process.platform === "win32"
      ? ["powershell.exe", ["-NoProfile", "-Command", `Expand-Archive -LiteralPath '${join(work, archive)}' -DestinationPath '${unpack}'`]]
      : ["tar", ["-xzf", join(work, archive), "-C", unpack]];
    const result = spawnSync(command[0], command[1], { stdio: "inherit" });
    if (result.status !== 0) throw new Error(`failed to extract ${archive}`);
    const executable = process.platform === "win32" ? "orch8.exe" : "orch8";
    const source = join(unpack, `orch8-${tag}-${triple}`, executable);
    await mkdir(binRoot, { recursive: true });
    const staged = join(binRoot, `${executable}.new`);
    await writeFile(staged, await readFile(source));
    if (process.platform !== "win32") await chmod(staged, 0o755);
    await rename(staged, join(binRoot, executable));
  } finally {
    await rm(work, { recursive: true, force: true });
  }
  return join(binRoot, process.platform === "win32" ? "orch8.exe" : "orch8");
}

if (fileURLToPath(import.meta.url) === resolve(process.argv[1] ?? "")) {
  install().catch((error) => {
    console.error(`@orch8/cli: ${error.message}`);
    console.error(`Fallback: curl -fsSL https://raw.githubusercontent.com/${repo}/main/install.sh | sh`);
    process.exitCode = 1;
  });
}
