import type {
  Orch8Approval, Orch8ApprovalDecision, Orch8ApprovalDecisionResult,
} from "./executionPresentation";
import type { Orch8SqliteDatabase } from "./sqlite";

export interface StoredApprovalDecision extends Orch8ApprovalDecision {
  committedAtMs: number;
}

const schema = `
  CREATE TABLE IF NOT EXISTS orch8_approvals (
    scope TEXT NOT NULL,
    id TEXT NOT NULL,
    revision INTEGER NOT NULL,
    status INTEGER NOT NULL,
    expires_at_ms INTEGER,
    choice_values TEXT NOT NULL,
    decision_key TEXT,
    decision_value TEXT,
    committed_at_ms INTEGER,
    PRIMARY KEY (scope, id),
    UNIQUE (scope, decision_key)
  )
`;

const statusRank = { pending: 0, expired: 1, resolved: 2 };

function sqliteIsFull(error: unknown): boolean {
  if (typeof error !== "object" || error === null) return false;
  const { code, errcode, message } = error as { code?: unknown; errcode?: unknown; message?: unknown };
  return code === "SQLITE_FULL" || code === 13 ||
    (typeof errcode === "number" && (errcode & 0xff) === 13) ||
    (typeof message === "string" && /(?:\bSQLITE_FULL\b|database or disk is full)/i.test(message));
}

export class SqliteApprovalCommitAdapter {
  private readonly database: Orch8SqliteDatabase;
  private readonly scope: string;

  private constructor(database: Orch8SqliteDatabase, scope: string) {
    this.database = database;
    this.scope = scope;
  }

  static async open(database: Orch8SqliteDatabase, scope: string): Promise<SqliteApprovalCommitAdapter> {
    if (!scope) throw new TypeError("Approval scope is required");
    await database.execAsync(schema);
    return new SqliteApprovalCommitAdapter(database, scope);
  }

  /** Record the source revision before displaying its approval in the inbox. */
  async recordApproval(approval: Orch8Approval): Promise<void> {
    const expiry = approval.expiresAtMs ?? null;
    if (!approval.id || !Number.isSafeInteger(approval.revision) || approval.revision < 0 ||
        (expiry !== null && (!Number.isFinite(expiry) || Math.abs(expiry) > 8_640_000_000_000_000))) {
      throw new RangeError("Invalid approval revision or expiry");
    }
    await this.database.runAsync(
      `INSERT INTO orch8_approvals (scope,id,revision,status,expires_at_ms,choice_values)
       VALUES (?,?,?,?,?,?)
       ON CONFLICT(scope,id) DO UPDATE SET
         revision=excluded.revision,status=excluded.status,
         expires_at_ms=excluded.expires_at_ms,choice_values=excluded.choice_values
       WHERE orch8_approvals.decision_key IS NULL
         AND (excluded.revision > orch8_approvals.revision
           OR (excluded.revision = orch8_approvals.revision AND excluded.status > orch8_approvals.status))`,
      [this.scope, approval.id, approval.revision, statusRank[approval.status], expiry,
        JSON.stringify(approval.choices.map((choice) => choice.value))],
    );
  }

  /** Atomically accept a pending choice or replay its exact committed decision. */
  commit = async (decision: Orch8ApprovalDecision): Promise<Orch8ApprovalDecisionResult> => {
    if (!decision.idempotencyKey || !decision.approvalId ||
        !Number.isSafeInteger(decision.expectedRevision) || decision.expectedRevision < 0) {
      throw new RangeError("Invalid approval decision identity");
    }
    const now = Date.now();
    let result: { changes: number };
    try {
      result = await this.database.runAsync(
        `UPDATE orch8_approvals SET status=2,decision_key=?,decision_value=?,committed_at_ms=?
         WHERE scope=? AND id=? AND revision=? AND status=0 AND decision_key IS NULL
           AND (expires_at_ms IS NULL OR expires_at_ms > ?)
           AND EXISTS (SELECT 1 FROM json_each(choice_values) WHERE value=?)
           AND NOT EXISTS (SELECT 1 FROM orch8_approvals WHERE scope=? AND decision_key=?)`,
        [decision.idempotencyKey, decision.value, now, this.scope, decision.approvalId, decision.expectedRevision,
          now, decision.value, this.scope, decision.idempotencyKey],
      );
    } catch (error) {
      if (sqliteIsFull(error)) return "storage_full";
      throw error;
    }
    if (result.changes === 1) return "accepted";
    const previous = await this.getDecision(decision.approvalId);
    return previous?.idempotencyKey === decision.idempotencyKey &&
      previous.expectedRevision === decision.expectedRevision && previous.value === decision.value
      ? "accepted" : "stale";
  };

  getDecision(id: string): Promise<StoredApprovalDecision | null> {
    return this.database.getFirstAsync(
      `SELECT id AS approvalId,revision AS expectedRevision,decision_key AS idempotencyKey,
              decision_value AS value,committed_at_ms AS committedAtMs
       FROM orch8_approvals WHERE scope=? AND id=? AND decision_key IS NOT NULL`,
      [this.scope, id],
    );
  }
}
