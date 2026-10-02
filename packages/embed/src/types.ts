/**
 * Wire types for the Orch8 embed API (`/api/v1/embed/*`).
 * Mirrors CONTRACTS.md §2. Fields the contract marks optional are optional here.
 */

export type EmbedScope =
  | "runs:read"
  | "runs:start"
  | "approvals:resolve"
  | "sequences:read"
  | "builder:edit";

/** Decoded (NOT verified) `o8e1` token payload. Verification is the engine's job. */
export interface EmbedTokenPayload {
  v: number;
  tid: string;
  sub: string;
  scp: string[];
  seq: string[] | null;
  iat: number;
  exp: number;
  jti: string;
}

export type RunState =
  | "scheduled"
  | "running"
  | "waiting"
  | "paused"
  | "completed"
  | "failed"
  | "cancelled";

export type StepState =
  | "pending"
  | "running"
  | "waiting"
  | "completed"
  | "failed"
  | "cancelled"
  | "skipped";

export interface RunSummary {
  id: string;
  sequence: string;
  state: RunState | string;
  created_at: string;
  updated_at: string;
  current_step?: string | null;
}

export interface RunList {
  items: RunSummary[];
  next_cursor?: string | null;
}

export interface RunStep {
  id: string;
  name?: string | null;
  state: StepState | string;
  started_at?: string | null;
  finished_at?: string | null;
  output?: unknown;
}

export interface RunDetail {
  id: string;
  sequence: string;
  state: RunState | string;
  steps: RunStep[];
}

export interface ApprovalChoice {
  label: string;
  value: string;
}

export interface Approval {
  id: string;
  instance_id: string;
  step_id: string;
  prompt: string;
  choices: ApprovalChoice[];
  created_at: string;
}

export interface ApprovalList {
  items: Approval[];
}

export interface ResolveApprovalBody {
  choice: string;
  comment?: string;
}

export interface EmbedTheme {
  css_vars: Record<string, string>;
  logo_url?: string | null;
  hide_badge: boolean;
}

/** One entry of the handler palette offered by `<orch8-builder>`. */
export interface HandlerInfo {
  name: string;
  label?: string;
  description?: string;
  default_params?: Record<string, unknown>;
}

export interface SequenceSummary {
  name: string;
  version?: number;
  [key: string]: unknown;
}

/**
 * `GET /embed/sequences`. The contract names the route but not the body; we accept
 * `{ items, handlers? }` or a bare array of sequences. `handlers` entries may be
 * strings or objects.
 */
export interface SequenceList {
  items: SequenceSummary[];
  handlers: HandlerInfo[];
}

/** A block of a sequence definition as the engine serialises it (`type`-tagged). */
export interface BlockJson {
  type: string;
  id: string;
  handler?: string;
  params?: unknown;
  [key: string]: unknown;
}

export interface SequenceDefinitionJson {
  name?: string;
  version?: number;
  blocks: BlockJson[];
  [key: string]: unknown;
}

export interface StartRunBody {
  sequence: string;
  input?: unknown;
}
