/**
 * "Copy as" snippet generators: render an {@link ApiRequestSpec} as curl,
 * Node SDK (`@orch8.io/sdk`), Python SDK (`orch8-io-sdk`) and `orch8` CLI
 * equivalents of the exact request a dashboard button sends.
 *
 * The API key is never embedded: every snippet reads it from the
 * `ORCH8_API_KEY` environment variable.
 *
 * Pure module (imported by node:test): no browser globals, no imports from
 * other dashboard modules except type-only ones.
 */

import type { ApiRequestSpec } from "./requests.ts";

export type SnippetFormat = "curl" | "node" | "python" | "cli";

export const SNIPPET_FORMATS: Array<{ id: SnippetFormat; label: string }> = [
  { id: "curl", label: "curl" },
  { id: "node", label: "Node SDK" },
  { id: "python", label: "Python SDK" },
  { id: "cli", label: "orch8 CLI" },
];

export interface ConnectionInfo {
  /** Engine base URL exactly as the dashboard uses it (no trailing slash needed). */
  baseUrl: string;
  /** Tenant sent as `X-Tenant-Id`, when configured. */
  tenantId: string | null;
}

export const API_KEY_ENV = "ORCH8_API_KEY";

function trimBase(url: string): string {
  return url.replace(/\/+$/, "");
}

function pathWithQuery(spec: ApiRequestSpec): string {
  const entries = Object.entries(spec.query ?? {});
  if (entries.length === 0) return spec.path;
  const qs = entries.map(([k, v]) => `${encodeURIComponent(k)}=${encodeURIComponent(v)}`).join("&");
  return `${spec.path}?${qs}`;
}

/** POSIX single-quote a shell word. */
export function shellQuote(s: string): string {
  if (/^[A-Za-z0-9_@%+=:,./-]+$/.test(s)) return s;
  return `'${s.replace(/'/g, `'\\''`)}'`;
}

export function toCurl(spec: ApiRequestSpec, conn: ConnectionInfo): string {
  const url = `${trimBase(conn.baseUrl)}${pathWithQuery(spec)}`;
  const lines = [`curl -sS -X ${spec.method} ${shellQuote(url)}`];
  lines.push(`  -H "X-API-Key: $${API_KEY_ENV}"`);
  if (conn.tenantId) lines.push(`  -H ${shellQuote(`X-Tenant-Id: ${conn.tenantId}`)}`);
  lines.push(`  -H 'Content-Type: application/json'`);
  if (spec.body !== undefined) {
    lines.push(`  -d ${shellQuote(JSON.stringify(spec.body))}`);
  }
  return lines.join(" \\\n");
}

// ─── JS / Python literal rendering ──────────────────────────────────────────

/** Object entries as JSON.stringify would send them (undefined values dropped). */
function definedEntries(value: object): Array<[string, unknown]> {
  return Object.entries(value as Record<string, unknown>).filter(
    ([, v]) => v !== undefined && typeof v !== "function",
  );
}

function jsIdent(key: string): string {
  return /^[A-Za-z_$][A-Za-z0-9_$]*$/.test(key) ? key : JSON.stringify(key);
}

/** Render a JSON value as a JS object literal (pretty, 2-space indent). */
export function toJsLiteral(value: unknown, indent = 0): string {
  const pad = "  ".repeat(indent);
  const inner = "  ".repeat(indent + 1);
  if (value === null || typeof value !== "object") return JSON.stringify(value) ?? "undefined";
  if (Array.isArray(value)) {
    if (value.length === 0) return "[]";
    return `[\n${value.map((v) => inner + toJsLiteral(v, indent + 1)).join(",\n")},\n${pad}]`;
  }
  const entries = definedEntries(value);
  if (entries.length === 0) return "{}";
  return `{\n${entries
    .map(([k, v]) => `${inner}${jsIdent(k)}: ${toJsLiteral(v, indent + 1)}`)
    .join(",\n")},\n${pad}}`;
}

/** Render a JSON value as a Python literal (True/False/None, dicts, lists). */
export function toPyLiteral(value: unknown, indent = 0): string {
  const pad = "    ".repeat(indent);
  const inner = "    ".repeat(indent + 1);
  if (value === null || value === undefined) return "None";
  if (value === true) return "True";
  if (value === false) return "False";
  if (typeof value === "number") return Number.isFinite(value) ? String(value) : "None";
  if (typeof value === "string") return JSON.stringify(value);
  if (Array.isArray(value)) {
    if (value.length === 0) return "[]";
    return `[\n${value.map((v) => inner + toPyLiteral(v, indent + 1)).join(",\n")},\n${pad}]`;
  }
  const entries = definedEntries(value);
  if (entries.length === 0) return "{}";
  return `{\n${entries
    .map(([k, v]) => `${inner}${JSON.stringify(k)}: ${toPyLiteral(v, indent + 1)}`)
    .join(",\n")},\n${pad}}`;
}

export function toNode(spec: ApiRequestSpec, conn: ConnectionInfo): string {
  const cfg = [`  baseUrl: ${JSON.stringify(trimBase(conn.baseUrl))},`];
  if (conn.tenantId) cfg.push(`  tenantId: ${JSON.stringify(conn.tenantId)},`);
  cfg.push(`  headers: { "X-API-Key": process.env.${API_KEY_ENV} ?? "" },`);

  let call: string;
  if (spec.sdk && !spec.query) {
    const args = spec.sdk.args.map((a) =>
      a.kind === "string" ? JSON.stringify(a.value) : toJsLiteral(spec.body),
    );
    call = `client.${spec.sdk.node}(${args.join(", ")})`;
  } else {
    const args = [JSON.stringify(spec.method), JSON.stringify(pathWithQuery(spec))];
    if (spec.body !== undefined) args.push(toJsLiteral(spec.body));
    call = `client.request(${args.join(", ")})`;
  }

  return [
    `import { Orch8Client } from "@orch8.io/sdk";`,
    ``,
    `const client = new Orch8Client({`,
    ...cfg,
    `});`,
    ``,
    `const result = await ${call};`,
    `console.log(result);`,
  ].join("\n");
}

export function toPython(spec: ApiRequestSpec, conn: ConnectionInfo): string {
  const ctorArgs = [JSON.stringify(trimBase(conn.baseUrl))];
  if (conn.tenantId) ctorArgs.push(`tenant_id=${JSON.stringify(conn.tenantId)}`);
  ctorArgs.push(`headers={"X-API-Key": os.environ["${API_KEY_ENV}"]}`);

  let call: string;
  if (spec.sdk && !spec.query) {
    const args = spec.sdk.args.map((a) =>
      a.kind === "string" ? JSON.stringify(a.value) : toPyLiteral(spec.body, 2),
    );
    call = `client.${spec.sdk.python}(${args.join(", ")})`;
  } else {
    const args = [JSON.stringify(spec.method), JSON.stringify(pathWithQuery(spec))];
    if (spec.body !== undefined) args.push(`json=${toPyLiteral(spec.body, 2)}`);
    call = `client.request(${args.join(", ")})`;
  }

  return [
    `import asyncio`,
    `import os`,
    ``,
    `from orch8 import Orch8Client`,
    ``,
    ``,
    `async def main() -> None:`,
    `    async with Orch8Client(${ctorArgs.join(", ")}) as client:`,
    `        result = await ${call}`,
    `        print(result)`,
    ``,
    ``,
    `asyncio.run(main())`,
  ].join("\n");
}

/**
 * `orch8` CLI equivalent, or `null` when no CLI command sends this exact
 * request (e.g. cron create, custom signals, fork).
 */
export function toCli(spec: ApiRequestSpec, conn: ConnectionInfo): string | null {
  if (!spec.cli) return null;
  const args = [...spec.cli.args];
  const tIdx = args.indexOf("--tenant-id");
  if (tIdx >= 0) {
    // The CLI sends --tenant-id as the X-Tenant-Id header; a different
    // dashboard tenant header could not be reproduced.
    if (conn.tenantId && conn.tenantId !== args[tIdx + 1]) return null;
  } else if (conn.tenantId) {
    args.unshift("--tenant-id", conn.tenantId);
  }
  const lines: string[] = [];
  if (spec.cli.file) {
    lines.push(`cat > ${shellQuote(spec.cli.file.name)} <<'JSON'`);
    lines.push(JSON.stringify(spec.cli.file.content, null, 2));
    lines.push("JSON");
  }
  lines.push(`# The CLI reads the API key from $${API_KEY_ENV}.`);
  lines.push(
    ["orch8", "--url", shellQuote(trimBase(conn.baseUrl)), ...args.map(shellQuote)].join(" "),
  );
  return lines.join("\n");
}

export function generateSnippet(
  format: SnippetFormat,
  spec: ApiRequestSpec,
  conn: ConnectionInfo,
): string | null {
  switch (format) {
    case "curl":
      return toCurl(spec, conn);
    case "node":
      return toNode(spec, conn);
    case "python":
      return toPython(spec, conn);
    case "cli":
      return toCli(spec, conn);
  }
}
