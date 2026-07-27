/**
 * Shared helpers for the push-wake / mobile-sync E2E suites.
 *
 * The suites drive the real `orch8-server` binary over HTTP (mobile routes
 * require `ORCH8_MOBILE_SYNC_ENABLED=true`, passed via `startServer({env})`)
 * and inspect durable state directly in Postgres through `TEST_DB_URL`
 * (same pattern as `encryption_at_rest.test.ts`).
 *
 * Surface under test:
 *   - `orch8-api/src/mobile_sync.rs`   — /mobile/* routes
 *   - `orch8-api/src/telemetry.rs`     — /telemetry/mobile* routes
 *   - `orch8-storage/src/postgres/push_outbox.rs` + `mobile_sync.rs`
 *   - migrations 076 (push_wake_outbox) and 079 (wake governance columns)
 */

import { execFileSync } from "node:child_process";
import assert from "node:assert/strict";
import { TEST_DB_URL } from "../harness.ts";

const BASE: string =
  process.env.ORCH8_E2E_BASE_URL ??
  `http://localhost:${process.env.ORCH8_E2E_PORT ?? "18080"}`;

/** Short unique id with a readable prefix (mobile tables survive suite runs). */
export function uid(prefix: string): string {
  return `${prefix}-${crypto.randomUUID().slice(0, 8)}`;
}

/** Escape a value as a SQL single-quoted literal. */
function sqlStr(value: string): string {
  return `'${value.replaceAll("'", "''")}'`;
}

/** Run a SQL statement and return trimmed raw output (unaligned, no header). */
export function psqlQuery(sql: string): string {
  const dbUrl = new URL(TEST_DB_URL);
  return execFileSync(
    "psql",
    [
      "-h", dbUrl.hostname,
      "-p", dbUrl.port,
      "-U", dbUrl.username,
      "-d", dbUrl.pathname.slice(1),
      "-v", "ON_ERROR_STOP=1",
      "-A",
      "-t",
      "-c", sql,
    ],
    {
      env: { ...process.env, PGPASSWORD: dbUrl.password },
      encoding: "utf-8",
    },
  ).trim();
}

/** Run a query and parse the result as rows of a JSON aggregate. */
export function psqlRows<T = Record<string, unknown>>(sql: string): T[] {
  const wrapped = `SELECT coalesce(json_agg(row_to_json(t)), '[]'::json) FROM (${sql}) t`;
  const out = psqlQuery(wrapped);
  return (out ? JSON.parse(out) : []) as T[];
}

/** Execute DDL/DML that must succeed. Returns psql output. */
export function psqlExec(sql: string): string {
  return psqlQuery(sql);
}

/** Execute SQL expected to FAIL; returns the error message. Throws if it succeeds. */
export function psqlExpectError(sql: string): string {
  const dbUrl = new URL(TEST_DB_URL);
  try {
    execFileSync(
      "psql",
      [
        "-h", dbUrl.hostname,
        "-p", dbUrl.port,
        "-U", dbUrl.username,
        "-d", dbUrl.pathname.slice(1),
        "-v", "ON_ERROR_STOP=1",
        "-c", sql,
      ],
      { env: { ...process.env, PGPASSWORD: dbUrl.password }, stdio: "pipe" },
    );
  } catch (error) {
    const err = error as { stderr?: Buffer; message: string };
    return err.stderr?.toString() ?? err.message;
  }
  throw new Error(`expected SQL to fail but it succeeded: ${sql}`);
}

export { sqlStr };

export interface ApiResult {
  status: number;
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  body: any;
  text: string;
}

/** Assert exactly one row and return it (satisfies noUncheckedIndexedAccess). */
export function only<T>(rows: T[], label = "rows"): T {
  assert.equal(rows.length, 1, `expected exactly 1 row in ${label}, got ${rows.length}`);
  return rows[0] as T;
}

/** Raw JSON request that never throws — suites assert on status + body. */
export async function api(
  method: string,
  path: string,
  options: { tenant?: string | undefined; body?: unknown; headers?: Record<string, string> } = {},
): Promise<ApiResult> {
  const headers: Record<string, string> = {
    "Content-Type": "application/json",
    ...(options.headers ?? {}),
  };
  if (options.tenant !== undefined) headers["X-Tenant-Id"] = options.tenant;
  const init: RequestInit = { method, headers };
  if (options.body !== undefined) init.body = JSON.stringify(options.body);
  const res = await fetch(`${BASE}${path}`, init);
  const text = await res.text();
  let body: unknown = null;
  try {
    body = text ? JSON.parse(text) : null;
  } catch {
    body = null;
  }
  return { status: res.status, body, text };
}

export const get = (path: string, tenant?: string) => api("GET", path, { tenant });
export const post = (path: string, body: unknown, tenant?: string) =>
  api("POST", path, { body, tenant });

// ---------------------------------------------------------------------------
// Mobile domain builders
// ---------------------------------------------------------------------------

export interface DeviceSpec {
  device_id: string;
  push_token?: string;
  platform: string;
  app_version?: string;
}

export function deviceSpec(overrides: Partial<DeviceSpec> = {}): DeviceSpec {
  return {
    device_id: uid("dev"),
    push_token: `tok-${crypto.randomUUID()}`,
    platform: "ios",
    app_version: "1.0.0",
    ...overrides,
  };
}

export async function registerDevice(
  tenant: string,
  spec: DeviceSpec,
): Promise<ApiResult> {
  return post("/mobile/devices/register", spec, tenant);
}

/** Register a device and assert 201. Returns the spec for chaining. */
export async function mustRegisterDevice(
  tenant: string,
  spec: DeviceSpec,
): Promise<DeviceSpec> {
  const res = await registerDevice(tenant, spec);
  if (res.status !== 201) {
    throw new Error(`register device failed: ${res.status} ${res.text}`);
  }
  return spec;
}

export async function createCommand(
  tenant: string,
  deviceId: string,
  commandType: string,
  payload: Record<string, unknown> = {},
): Promise<ApiResult> {
  return post(
    "/mobile/commands",
    { device_id: deviceId, command_type: commandType, payload },
    tenant,
  );
}

export interface SyncRequest {
  device_id: string;
  status_updates?: unknown[];
  approval_requests?: unknown[];
  step_delegations?: unknown[];
  command_acks?: string[];
}

export async function syncDevice(
  tenant: string | undefined,
  req: SyncRequest,
): Promise<ApiResult> {
  return post("/mobile/sync", req, tenant);
}

// ---------------------------------------------------------------------------
// push_wake_outbox row access
// ---------------------------------------------------------------------------

export interface WakeRow {
  id: string;
  tenant_id: string;
  device_id: string;
  command_id: string;
  attempts: number;
  status: string;
  next_attempt_at: string | null;
  lease_until: string | null;
  last_error: string | null;
  terminal_reason: string | null;
  delivered_at: string | null;
  command_acked_at: string | null;
  created_at: string;
  execution_id: string | null;
  topic: string | null;
  collapse_key: string | null;
  superseded_by: string | null;
}

export async function wakeRowsForDevice(deviceId: string): Promise<WakeRow[]> {
  return psqlRows<WakeRow>(
    `SELECT * FROM push_wake_outbox WHERE device_id = ${sqlStr(deviceId)} ORDER BY created_at`,
  );
}

export async function wakeRowsForCommand(commandId: string): Promise<WakeRow[]> {
  return psqlRows<WakeRow>(
    `SELECT * FROM push_wake_outbox WHERE command_id = ${sqlStr(commandId)}`,
  );
}

export interface CommandRow {
  id: string;
  device_id: string;
  command_type: string;
  payload: string;
  created_at: string;
  acked_at: string | null;
}

export async function commandRowsForDevice(deviceId: string): Promise<CommandRow[]> {
  return psqlRows<CommandRow>(
    `SELECT * FROM mobile_commands WHERE device_id = ${sqlStr(deviceId)} ORDER BY created_at`,
  );
}

export function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}
