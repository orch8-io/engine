#!/usr/bin/env node
import { existsSync } from "node:fs";
import { dirname, join } from "node:path";
import { spawnSync } from "node:child_process";
import process from "node:process";
import { fileURLToPath } from "node:url";
import { install } from "../install.mjs";

const here = dirname(fileURLToPath(import.meta.url));
const executable = join(here, "native", process.platform === "win32" ? "orch8.exe" : "orch8");
if (!existsSync(executable)) await install();
const result = spawnSync(executable, process.argv.slice(2), { stdio: "inherit" });
if (result.error) throw result.error;
process.exit(result.status ?? 1);
