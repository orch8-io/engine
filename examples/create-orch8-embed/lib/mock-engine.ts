import "server-only";
import onboarding from "@/orch8/customer-onboarding.json";

/**
 * A tiny in-memory implementation of the Orch8 embed API (`/api/v1/embed/*`)
 * so the starter runs with no engine. Runs advance one step every 2 s and stop
 * at a `wait_for_input` step until resolved on /approvals. Data is per
 * sub-tenant and resets on server restart. NOT for production.
 */

interface Block {
  type: string;
  id: string;
  handler?: string;
  params?: unknown;
  wait_for_input?: { prompt?: string; choices?: { label: string; value: string }[] };
  [k: string]: unknown;
}
interface Definition {
  name: string;
  blocks: Block[];
  [k: string]: unknown;
}
interface MockRun {
  id: string;
  sub: string;
  sequence: string;
  blocks: Block[];
  createdAt: number;
  decisions: Map<string, { at: number; choice: string; comment?: string }>;
}
interface Claims {
  sub: string;
  scp: string[];
  seq: string[] | null;
  exp: number;
}

const STEP_MS = 2000;
const HANDLERS = [
  { name: "log", label: "Log a message", default_params: { message: "Hello" } },
  { name: "noop", label: "Wait / no-op" },
  { name: "http_request", label: "HTTP request", default_params: { method: "POST", url: "https://example.com/webhook" } },
  { name: "send_email", label: "Send email", default_params: { to: "{{context.data.email}}", template: "welcome" } },
  { name: "human_review", label: "Human approval" },
];

interface Store {
  runs: MockRun[];
  sequences: Map<string, Map<string, Definition>>;
}
const g = globalThis as unknown as { __orch8MockStore?: Store };
const store: Store = (g.__orch8MockStore ??= { runs: [], sequences: new Map() });

function seqsFor(sub: string): Map<string, Definition> {
  let m = store.sequences.get(sub);
  if (!m) {
    m = new Map([[onboarding.name, structuredClone(onboarding) as Definition]]);
    store.sequences.set(sub, m);
    // Seed history: one finished run and one waiting for approval.
    const now = Date.now();
    const done = createRun(sub, onboarding.name, now - 3_600_000);
    const gate = done.blocks.find((b) => b.wait_for_input);
    if (gate) done.decisions.set(gate.id, { at: now - 3_590_000, choice: "activate" });
    createRun(sub, onboarding.name, now - 60_000);
  }
  return m;
}

function createRun(sub: string, sequence: string, createdAt = Date.now()): MockRun {
  const def = store.sequences.get(sub)?.get(sequence);
  const run: MockRun = {
    id: crypto.randomUUID(),
    sub,
    sequence,
    blocks: structuredClone(def?.blocks ?? []),
    createdAt,
    decisions: new Map(),
  };
  store.runs.push(run);
  return run;
}

type StepView = { id: string; name: string; state: string; started_at: string | null; finished_at: string | null; output?: unknown };

function project(run: MockRun, now = Date.now()) {
  const steps: StepView[] = [];
  let t = run.createdAt;
  let blocked = false;
  let current: string | null = null;
  for (const b of run.blocks) {
    if (blocked || t > now) {
      steps.push({ id: b.id, name: b.id, state: "pending", started_at: null, finished_at: null });
      continue;
    }
    const start = t;
    if (b.wait_for_input) {
      const d = run.decisions.get(b.id);
      if (!d) {
        steps.push({ id: b.id, name: b.id, state: "waiting", started_at: iso(start), finished_at: null });
        blocked = true;
        current = b.id;
        continue;
      }
      t = Math.max(d.at, start);
      steps.push({ id: b.id, name: b.id, state: "completed", started_at: iso(start), finished_at: iso(t), output: { choice: d.choice, comment: d.comment ?? null } });
      continue;
    }
    const end = start + STEP_MS;
    if (end > now) {
      steps.push({ id: b.id, name: b.id, state: "running", started_at: iso(start), finished_at: null });
      current = b.id;
      blocked = true;
      continue;
    }
    steps.push({ id: b.id, name: b.id, state: "completed", started_at: iso(start), finished_at: iso(end), output: b.handler === "log" ? { logged: (b.params as { message?: string })?.message ?? "" } : undefined });
    t = end;
  }
  const waiting = steps.some((s) => s.state === "waiting");
  const running = steps.some((s) => s.state === "running" || s.state === "pending");
  const state = waiting ? "waiting" : running ? "running" : "completed";
  const updated = Math.max(run.createdAt, ...steps.flatMap((s) => [s.started_at, s.finished_at]).filter(Boolean).map((s) => Date.parse(s!)));
  return {
    summary: { id: run.id, sequence: run.sequence, state, created_at: iso(run.createdAt), updated_at: iso(updated), current_step: current },
    detail: { id: run.id, sequence: run.sequence, state, steps },
  };
}

const iso = (ms: number) => new Date(ms).toISOString();

function decodeClaims(authorization: string | null): Claims | null {
  const token = authorization?.replace(/^Bearer\s+/i, "") ?? "";
  const part = token.startsWith("o8e1.") ? token.split(".")[1] : undefined;
  if (!part) return null;
  try {
    const c = JSON.parse(Buffer.from(part, "base64url").toString("utf8")) as Claims;
    return c.exp * 1000 > Date.now() ? c : null;
  } catch {
    return null;
  }
}

export interface MockResponse {
  status: number;
  body?: unknown;
}

const err = (status: number, code: string, message: string): MockResponse => ({ status, body: { error: { code, message } } });

/** Handles `METHOD /<path>` relative to `/api/v1/embed`. */
export function handleMock(method: string, path: string[], authorization: string | null, body: unknown, search: URLSearchParams): MockResponse {
  const claims = decodeClaims(authorization);
  if (!claims) return err(401, "unauthorized", "missing or expired embed token");
  const need = (scope: string) => (claims.scp.includes(scope) ? null : err(403, "forbidden", `token lacks scope ${scope}`));
  const seqs = seqsFor(claims.sub);
  const mine = () => store.runs.filter((r) => r.sub === claims.sub).sort((a, b) => b.createdAt - a.createdAt);
  const [head, id] = path;
  const route = `${method} ${head ?? ""}${id ? "/:id" : ""}`;

  switch (route) {
    case "GET theme":
      return { status: 200, body: { css_vars: {}, logo_url: null, hide_badge: false } };
    case "GET runs": {
      const denied = need("runs:read");
      if (denied) return denied;
      const limit = Math.min(Number(search.get("limit")) || 20, 100);
      const offset = Number(search.get("cursor")) || 0;
      const all = mine();
      const page = all.slice(offset, offset + limit).map((r) => project(r).summary);
      return { status: 200, body: { items: page, next_cursor: offset + limit < all.length ? String(offset + limit) : null } };
    }
    case "GET runs/:id": {
      const denied = need("runs:read");
      if (denied) return denied;
      const run = mine().find((r) => r.id === id);
      return run ? { status: 200, body: project(run).detail } : err(404, "not_found", "run not found");
    }
    case "POST runs": {
      const denied = need("runs:start");
      if (denied) return denied;
      const sequence = (body as { sequence?: string })?.sequence ?? "";
      if (!seqs.has(sequence)) return err(404, "not_found", `sequence ${sequence} not found`);
      if (claims.seq && !claims.seq.includes(sequence)) return err(403, "forbidden", `token may not start ${sequence}`);
      return { status: 201, body: { id: createRun(claims.sub, sequence).id } };
    }
    case "GET approvals": {
      const items = mine().flatMap((r) => {
        const p = project(r);
        const waiting = p.detail.steps.find((s) => s.state === "waiting");
        const block = waiting && r.blocks.find((b) => b.id === waiting.id);
        if (!waiting || !block?.wait_for_input) return [];
        return [{
          id: `${r.id}~${block.id}`,
          instance_id: r.id,
          step_id: block.id,
          prompt: block.wait_for_input.prompt ?? "Approve?",
          choices: block.wait_for_input.choices ?? [],
          created_at: waiting.started_at,
        }];
      });
      return { status: 200, body: { items } };
    }
    case "POST approvals/:id": {
      const denied = need("approvals:resolve");
      if (denied) return denied;
      const [runId, stepId] = (id ?? "").split("~");
      const run = mine().find((r) => r.id === runId);
      const choice = (body as { choice?: string })?.choice;
      if (!run || !stepId) return err(404, "not_found", "approval not found");
      if (run.decisions.has(stepId)) return err(409, "conflict", "already resolved");
      if (!choice) return err(400, "invalid_argument", "choice is required");
      run.decisions.set(stepId, { at: Date.now(), choice, comment: (body as { comment?: string })?.comment });
      return { status: 200, body: { ok: true } };
    }
    case "GET sequences":
      return { status: 200, body: { items: Array.from(seqs.values()).map((d) => ({ name: d.name })), handlers: HANDLERS } };
    case "GET sequences/:id": {
      const def = seqs.get(decodeURIComponent(id ?? ""));
      return def ? { status: 200, body: { definition: def } } : err(404, "not_found", "sequence not found");
    }
    case "PUT sequences/:id": {
      const denied = need("builder:edit");
      if (denied) return denied;
      const name = decodeURIComponent(id ?? "");
      const next = body as Definition;
      if (!seqs.has(name)) return err(403, "forbidden", "sequence is not owned by this sub-tenant");
      if (!next || !Array.isArray(next.blocks)) return err(400, "invalid_argument", "blocks must be an array");
      const saved = { ...next, name };
      seqs.set(name, saved);
      return { status: 200, body: { definition: saved } };
    }
    default:
      return err(404, "not_found", `no mock route for ${method} /${path.join("/")}`);
  }
}

/** Test helper. */
export function resetMockEngine(): void {
  store.runs = [];
  store.sequences.clear();
}
