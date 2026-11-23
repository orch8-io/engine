import type { InstanceStateKind } from "./types";

export interface Orch8ApprovalChoice {
  readonly label: string;
  readonly value: string;
}

export interface Orch8Approval {
  /** Stable, unique ID of this approval activation within the supplied scope. */
  readonly id: string;
  /** Monotonically increasing source version for this activation. */
  readonly revision: number;
  readonly status: "pending" | "resolved" | "expired";
  readonly title: string;
  readonly prompt: string;
  readonly choices: readonly Orch8ApprovalChoice[];
  readonly expiresAtMs?: number | null;
}

export interface Orch8ApprovalDecision {
  readonly approvalId: string;
  readonly expectedRevision: number;
  readonly value: string;
  readonly idempotencyKey: string;
}

export type Orch8ApprovalDecisionResult = "accepted" | "stale" | "storage_full";

export type Orch8ExecutionStepState = InstanceStateKind | "pending" | "skipped" | "retrying";

export interface Orch8ExecutionStep {
  /** Unique activation or event ID, including repeated steps. */
  readonly id: string;
  readonly label: string;
  readonly state: Orch8ExecutionStepState;
  readonly detail?: string;
  readonly attempt?: number;
  readonly timestampMs?: number;
}

export const EXECUTION_LABEL: Record<Orch8ExecutionStepState, string> = {
  scheduled: "Scheduled",
  pending: "Pending",
  running: "Running",
  waiting: "Waiting",
  paused: "Paused",
  retrying: "Retrying",
  completed: "Completed",
  failed: "Failed",
  cancelled: "Cancelled",
  skipped: "Skipped",
};

function validChoices(choices: readonly Orch8ApprovalChoice[]): boolean {
  return choices.length > 0 && choices.every(({ label, value }) => label.trim() && value.trim()) &&
    new Set(choices.map(({ value }) => value)).size === choices.length;
}

export function approvalExpired(approval: Orch8Approval, nowMs: number): boolean {
  return approval.status === "expired" || (approval.expiresAtMs != null &&
    (!Number.isFinite(approval.expiresAtMs) || approval.expiresAtMs <= nowMs || approval.expiresAtMs > 8_640_000_000_000_000));
}

/** Resolve duplicate deliveries by activation and revision. Terminal copies win ties. */
export function visibleApprovals(items: readonly Orch8Approval[]): Orch8Approval[] {
  const latest = new Map<string, Orch8Approval>();
  const statusOrder = { pending: 0, expired: 1, resolved: 2 };
  for (const item of items) {
    if (!item.id || !Number.isSafeInteger(item.revision) || item.revision < 0) continue;
    const current = latest.get(item.id);
    if (!current || item.revision > current.revision ||
      (item.revision === current.revision && statusOrder[item.status] > statusOrder[current.status])) {
      latest.set(item.id, item);
    }
  }
  return [...latest.values()].filter((item) => item.status !== "resolved");
}

type Submission = { revision: number; value: string; status: "submitting" | "retry" | "accepted" | "stale" };

/** Prevent duplicate taps and keep a retry bound to the original choice after an ambiguous failure. */
export class ApprovalDecisionLedger {
  private readonly submissions = new Map<string, Submission>();

  private readonly scopeKey: string;

  constructor(scopeKey: string) { this.scopeKey = scopeKey; }

  get(approval: Orch8Approval): Readonly<Submission> | undefined {
    const submission = this.submissions.get(approval.id);
    if (submission && submission.status !== "stale") return submission;
    return submission?.revision === approval.revision ? submission : undefined;
  }

  begin(approval: Orch8Approval, value: string, nowMs: number): Orch8ApprovalDecision | null {
    if (!this.scopeKey || !canDecide(approval, nowMs) ||
      !approval.choices.some((choice) => choice.value === value)) return null;
    const previous = this.get(approval);
    if (previous && (previous.status !== "retry" || previous.value !== value)) return null;
    this.submissions.set(approval.id, { revision: approval.revision, value, status: "submitting" });
    return {
      approvalId: approval.id,
      expectedRevision: approval.revision,
      value,
      idempotencyKey: JSON.stringify(["orch8-approval", this.scopeKey, approval.id]),
    };
  }

  finish(decision: Orch8ApprovalDecision, result: Orch8ApprovalDecisionResult | "retry"): void {
    const prior = this.submissions.get(decision.approvalId);
    if (prior?.status === "submitting" && prior.revision === decision.expectedRevision && prior.value === decision.value) {
      this.submissions.set(decision.approvalId, { ...prior, status: result === "storage_full" ? "retry" : result });
    }
  }
}

export function canDecide(approval: Orch8Approval, nowMs: number): boolean {
  return approval.status === "pending" && !approvalExpired(approval, nowMs) && validChoices(approval.choices);
}
