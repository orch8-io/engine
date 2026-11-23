import type { WorkflowCompletion, WorkflowSyncReceipt } from "./offlineWorkflow";
import type { Orch8SqliteDatabase } from "./sqlite";

export interface OutcomePrincipal {
  readonly accountId: string;
  readonly catalogId: string;
}

export interface PendingOutcome extends WorkflowCompletion {
  readonly outputJson: string;
}

export interface AccountOutcomeCredential extends OutcomePrincipal {
  readonly bearer: string;
}

export interface AccountOutcomeReceipt extends WorkflowSyncReceipt, OutcomePrincipal {}

export type AccountOutcomeCredentials = (
  principal: Readonly<OutcomePrincipal>,
) => Promise<AccountOutcomeCredential>;

export type AccountOutcomeDelivery = (
  outcome: Readonly<PendingOutcome & OutcomePrincipal>,
  credential: Readonly<AccountOutcomeCredential>,
) => Promise<AccountOutcomeReceipt>;

const schema = `
  CREATE TABLE IF NOT EXISTS orch8_account_outcomes (
    account_id TEXT NOT NULL,
    catalog_id TEXT NOT NULL,
    submission_id TEXT NOT NULL,
    revision TEXT NOT NULL,
    output_json TEXT NOT NULL,
    receipt TEXT CHECK(receipt IS NULL OR receipt IN ('acknowledged', 'needs_review')),
    PRIMARY KEY (account_id, catalog_id, submission_id, revision)
  )
`;

function samePrincipal(a: OutcomePrincipal, b: OutcomePrincipal): boolean {
  return a.accountId === b.accountId && a.catalogId === b.catalogId;
}

function identity(value: string): string {
  if (typeof value !== "string" || value.length === 0 || value.length > 512 || value.includes("\0")) {
    throw new TypeError("Invalid outcome identity");
  }
  return value;
}

export class SqliteAccountOutcomeJournal {
  private readonly database: Orch8SqliteDatabase;

  private constructor(database: Orch8SqliteDatabase) {
    this.database = database;
  }

  static async open(database: Orch8SqliteDatabase): Promise<SqliteAccountOutcomeJournal> {
    await database.execAsync(schema);
    return new SqliteAccountOutcomeJournal(database);
  }

  async enqueue(principal: OutcomePrincipal, outcome: PendingOutcome): Promise<void> {
    const keys = [
      identity(principal.accountId), identity(principal.catalogId),
      identity(outcome.submissionId), identity(outcome.revision),
    ];
    const output = outcome.outputJson;
    if (typeof output !== "string" || output.length > 1024 * 1024) {
      throw new TypeError("Outcome must contain bounded JSON");
    }
    JSON.parse(output);
    await this.database.runAsync(
      `INSERT INTO orch8_account_outcomes(account_id,catalog_id,submission_id,revision,output_json)
       VALUES (?,?,?,?,?) ON CONFLICT DO NOTHING`,
      [...keys, output],
    );
    const stored = await this.database.getFirstAsync<{ output_json: string }>(
      `SELECT output_json FROM orch8_account_outcomes
       WHERE account_id=? AND catalog_id=? AND submission_id=? AND revision=?`, keys,
    );
    if (stored?.output_json !== output) {
      throw new Error("Outcome identity already contains different data");
    }
  }

  async next(principal: OutcomePrincipal): Promise<PendingOutcome | null> {
    return this.database.getFirstAsync<PendingOutcome>(
      `SELECT submission_id AS submissionId, revision, output_json AS outputJson
       FROM orch8_account_outcomes
       WHERE account_id=? AND catalog_id=? AND receipt IS NULL ORDER BY rowid LIMIT 1`,
      [identity(principal.accountId), identity(principal.catalogId)],
    );
  }

  async acknowledge(receipt: AccountOutcomeReceipt): Promise<void> {
    if (receipt.outcome !== "acknowledged" && receipt.outcome !== "needs_review") {
      throw new TypeError("Invalid authenticated outcome receipt");
    }
    await this.database.runAsync(
      `UPDATE orch8_account_outcomes SET receipt=?
       WHERE account_id=? AND catalog_id=? AND submission_id=? AND revision=? AND receipt IS NULL`,
      [receipt.outcome, identity(receipt.accountId), identity(receipt.catalogId),
        identity(receipt.submissionId), identity(receipt.revision)],
    );
  }
}

/** Bound to an open catalog view. Credentials and queued rows must match that principal. */
export class AccountOutcomeReporter {
  private readonly principal: Readonly<OutcomePrincipal>;
  private active: Promise<AccountOutcomeReceipt | null> | undefined;
  private readonly journal: SqliteAccountOutcomeJournal;
  private readonly credentials: AccountOutcomeCredentials;
  private readonly deliver: AccountOutcomeDelivery;
  private readonly ensureCurrent: () => void;

  constructor(
    principal: OutcomePrincipal,
    journal: SqliteAccountOutcomeJournal,
    credentials: AccountOutcomeCredentials,
    deliver: AccountOutcomeDelivery,
    ensureCurrent: () => void,
  ) {
    this.principal = Object.freeze({
      accountId: identity(principal.accountId), catalogId: identity(principal.catalogId),
    });
    this.journal = journal;
    this.credentials = credentials;
    this.deliver = deliver;
    this.ensureCurrent = ensureCurrent;
  }

  async enqueue(outcome: PendingOutcome): Promise<void> {
    this.ensureCurrent();
    await this.journal.enqueue(this.principal, outcome);
  }

  reportNext(): Promise<AccountOutcomeReceipt | null> {
    if (this.active) return this.active;
    const run = this.report();
    this.active = run.finally(() => { this.active = undefined; });
    return this.active;
  }

  private async report(): Promise<AccountOutcomeReceipt | null> {
    this.ensureCurrent();
    const outcome = await this.journal.next(this.principal);
    if (!outcome) return null;
    this.ensureCurrent();
    const credential = await this.credentials(this.principal);
    const bearer = credential.bearer;
    if (!samePrincipal(credential, this.principal) || typeof bearer !== "string" || !bearer.trim()) {
      throw new Error("Outcome credentials belong to another account or catalog");
    }
    this.ensureCurrent();
    const receipt = await this.deliver(
      Object.freeze({ ...outcome, ...this.principal }),
      Object.freeze({ ...this.principal, bearer }),
    );
    if (!samePrincipal(receipt, this.principal) ||
        receipt.submissionId !== outcome.submissionId || receipt.revision !== outcome.revision) {
      throw new Error("Outcome receipt does not match the submitted revision and principal");
    }
    await this.journal.acknowledge(receipt);
    this.ensureCurrent();
    return Object.freeze({ ...receipt });
  }
}
