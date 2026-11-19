/** JSON values a host can safely represent in an offline workflow record. */
export type WorkflowJson =
  | null | boolean | number | string | WorkflowJson[] | { [key: string]: WorkflowJson };

const destinations = ["sync", "worker", "logs", "support"] as const;
export type DataDestination = (typeof destinations)[number];

/** Exact JSON Pointer allowlists per destination. Private subtrees take precedence. */
export type DataVisibility = Partial<Record<DataDestination | "private", readonly string[]>>;

export interface VisibilityEntry {
  path: string;
  destinations: DataDestination[];
}

/** Show where each field may travel without returning any field values. */
export function previewDataVisibility(data: WorkflowJson, policy: DataVisibility): VisibilityEntry[] {
  const entries: VisibilityEntry[] = [];
  const pending = [{ path: "", value: data }];
  const allows = destinations.map((destination) => new Set(policy[destination]));
  const privatePaths = policy.private ?? [];
  while (pending.length > 0) {
    const { path, value } = pending.pop()!;
    const fields = value !== null && typeof value === "object" ? Object.entries(value) : [];
    if (fields.length > 0) {
      for (const [key, child] of fields) {
        const escaped = key.replace(/~/g, "~0").replace(/\//g, "~1");
        pending.push({ path: `${path}/${escaped}`, value: child });
      }
      continue;
    }
    const isPrivate = privatePaths.some((prefix) => path === prefix || path.startsWith(`${prefix}/`));
    entries.push({
      path,
      destinations: isPrivate ? [] : destinations.filter((_, index) => allows[index].has(path)),
    });
  }
  return entries.sort((left, right) => left.path < right.path ? -1 : Number(left.path > right.path));
}

export interface OfflineEdit<T> {
  baseRevision: string;
  base: T;
  local: T;
  serverRevision: string;
  server: T;
}

export type OfflineConflictDecision<T> =
  | { action: "review" }
  | { action: "use_server" }
  | { action: "merge"; value: T };

export type OfflineConflictResult<T> =
  | { status: "review" }
  | { status: "server"; value: T; revision: string }
  | { status: "write"; value: T; expectedRevision: string };

export type OfflineConflictHandler<T> = (
  edit: Readonly<OfflineEdit<T>>,
) => OfflineConflictDecision<T> | Promise<OfflineConflictDecision<T>>;

/** Prepare a conditional write, adopt the server version, or let the app show a review. */
export async function resolveOfflineConflict<T>(
  edit: OfflineEdit<T>,
  onConflict?: OfflineConflictHandler<T>,
): Promise<OfflineConflictResult<T>> {
  const frozen = Object.freeze({ ...edit });
  if (frozen.baseRevision === frozen.serverRevision) {
    return { status: "write", value: frozen.local, expectedRevision: frozen.serverRevision };
  }
  const decision = await onConflict?.(frozen);
  if (decision?.action === "use_server") {
    return { status: "server", value: frozen.server, revision: frozen.serverRevision };
  }
  if (decision?.action === "merge") {
    return { status: "write", value: decision.value, expectedRevision: frozen.serverRevision };
  }
  return { status: "review" };
}

type BackgroundWindowKind = "local" | "worker";
type BackgroundOutcome = "progress" | "budget_exhausted" | "idle" | "failed";

export interface BackgroundDiagnostics {
  lastProgressAtMs: number | null;
  lastWindow: {
    kind: BackgroundWindowKind;
    startedAtMs: number;
    finishedAtMs: number;
    outcome: BackgroundOutcome;
  } | null;
  nextAction: "none_observed" | "wait_for_os_or_open_app";
}

export class BackgroundProgress {
  private lastProgressAtMs: number | null = null;
  private lastWindow: BackgroundDiagnostics["lastWindow"] = null;
  private pending = false;

  observe(
    kind: BackgroundWindowKind,
    startedAtMs: number,
    finishedAtMs: number,
    progressed: boolean,
    budgetExhausted: boolean,
    pending: boolean,
    failed = false,
  ): void {
    finishedAtMs = Math.max(startedAtMs, finishedAtMs);
    if (progressed) {
      this.lastProgressAtMs = Math.max(this.lastProgressAtMs ?? finishedAtMs, finishedAtMs);
    }
    if (this.lastWindow && startedAtMs < this.lastWindow.startedAtMs) return;
    const outcome = failed ? "failed" : budgetExhausted ? "budget_exhausted" : progressed ? "progress" : "idle";
    this.lastWindow = { kind, startedAtMs, finishedAtMs, outcome };
    this.pending = pending || budgetExhausted || failed;
  }

  snapshot(): BackgroundDiagnostics {
    return {
      lastProgressAtMs: this.lastProgressAtMs,
      lastWindow: this.lastWindow ? { ...this.lastWindow } : null,
      nextAction: this.pending ? "wait_for_os_or_open_app" : "none_observed",
    };
  }
}

export interface WorkflowCompletion {
  submissionId: string;
  revision: string;
}

export interface WorkflowSyncReceipt extends WorkflowCompletion {
  outcome: "acknowledged" | "needs_review";
}

export interface WorkflowSyncEvidence {
  completion?: WorkflowCompletion;
  startedSync?: boolean;
  receipt?: WorkflowSyncReceipt;
}

export type WorkflowSyncState =
  | "in_progress" | "local_complete" | "pending_sync" | "acknowledged" | "needs_review";

/** Project host sync evidence. The authenticated backend owns the receipt. */
export function workflowSyncState(evidence: WorkflowSyncEvidence): WorkflowSyncState {
  const { completion, receipt, startedSync } = evidence;
  if (!completion) return "in_progress";
  if (receipt?.submissionId === completion.submissionId && receipt.revision === completion.revision) {
    return receipt.outcome;
  }
  return startedSync ? "pending_sync" : "local_complete";
}
