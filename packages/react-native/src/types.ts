/**
 * Public types of @orch8.io/react-native-orch8.
 *
 * Numbers crossing the bridge are JS doubles. The engine's u64 counters stay
 * exact up to 2^53, far beyond anything a device reaches.
 */

export interface Orch8Config {
  /** SQLite path. Defaults to `<Documents>/orch8.db` (iOS) or the app database dir (Android). */
  dbPath?: string;
  tickIntervalMs?: number;
  maxConcurrentSteps?: number;
  maxStepsPerInstance?: number;
  maxConcurrentInstances?: number;
  maxTickDurationMs?: number;
  maxInstanceLifetimeSecs?: number;
  maxStoredSequences?: number;
  maxSequenceSizeBytes?: number;
  /** Also bounds how long a native handler call waits for the JS handler. */
  handlerTimeoutMs?: number;
  operationTimeoutMs?: number;
  telemetryEnabled?: boolean;
  /** HTTPS telemetry ingest endpoint. Empty disables delivery. */
  telemetryUrl?: string;
  environment?: string;
  rootPublicKey?: string;
  sdkVersion?: string;
  /** Skip ticks while process RSS exceeds this many bytes (0 = unlimited). */
  memoryBudgetBytes?: number;
  /** Endpoint returning a JSON array of sequences for `loadSequencesFromUrl`. */
  sequencesUrl?: string;
  /** Server sync endpoint (`…/api/v1/mobile/sync`). Required for `registerNode`. */
  syncUrl?: string;
  deviceId?: string;
  /**
   * Legacy static credential for sync and runtime-node calls. Not for
   * production apps: a key in the app binary is extractable. Use
   * `orch8.setTokenProvider` with device sessions minted by your backend.
   * Never put an operator key here.
   */
  syncApiKey?: string;
}

export type InstanceStateKind =
  | "scheduled"
  | "running"
  | "waiting"
  | "paused"
  | "completed"
  | "failed"
  | "cancelled";

export interface TickResult {
  instancesAdvanced: number;
  stepsExecuted: number;
  hasPendingWork: boolean;
}

export interface BackgroundRunResult extends TickResult {
  ticksExecuted: number;
  budgetExhausted: boolean;
}

export interface SyncResult {
  added: number;
  updated: number;
  removed: number;
  skipped: number;
  signatureFailures: number;
}

export interface FlushResult {
  sent: number;
  dropped: number;
}

export interface SequenceInfo {
  name: string;
  version: number;
}

export interface InstanceSummary {
  instanceId: string;
  sequenceName: string;
  state: InstanceStateKind;
  createdAt: string;
}

export interface InstanceState extends InstanceSummary {
  context: string;
  updatedAt: string;
}

export interface DeviceContext {
  deviceId: string;
  osName: string;
  osVersion: string;
  appVersion: string;
  sdkVersion?: string;
}

export type PowerState = "charging" | "unplugged" | "lowBattery" | "criticalBattery";

// -- Runtime node / worker (requires the engine release after 0.7.1) --------

export type NodeConnectivity = "offline" | "metered" | "wifi" | "ethernet";

export interface NodeCapabilities {
  /** Handler names served remotely. Empty = every handler registered with `registerHandler`. */
  handlers?: string[];
  regions?: string[];
  /** Free-form hardware facts (`camera`, `nfc`, …). `device:<deviceId>` is always added. */
  hardware?: string[];
  plugins?: string[];
  /** Credential binding *names* available on the device (never secrets). */
  credentials?: string[];
  offlineCapable?: boolean;
  connectivity?: NodeConnectivity;
  /** 0–100. */
  batteryPercent?: number;
  /** `ios` / `android`; inferred natively when absent. */
  platform?: string;
  /** APNs/FCM token used for id-only wake-up hints. */
  pushToken?: string;
  appVersion?: string;
  /** Overrides the API base derived from `syncUrl`. */
  apiBaseUrl?: string;
  capsuleSigningPublicKey?: string;
}

export interface NodeRegistration {
  runtimeId: string;
  deviceId: string;
  handlers: string[];
  expiresAt: string;
}

export interface WorkerOptions {
  /** Remote tasks executed concurrently (default 1). */
  maxConcurrentTasks?: number;
  /** Idle poll cadence before power-state scaling (default 15000). */
  idlePollIntervalMs?: number;
  version?: string;
}

export interface WorkerStats {
  running: boolean;
  inFlight: number;
  claimed: number;
  completed: number;
  failed: number;
  released: number;
  lost: number;
}

export interface WorkerWindowResult {
  claimed: number;
  completed: number;
  failed: number;
  stillRunning: number;
  budgetExhausted: boolean;
}

/** Id-only push wake hint. Never carries params. */
export interface PushWakeEnvelope {
  task_id?: string;
  runtime_id?: string;
  reason?: string;
}

// -- Delegation from phone-local workflows (engine release after 0.7.1) ----

export interface DelegationOptions {
  /** Tenant of the node credential; every continuity call is scoped to it. */
  tenantId: string;
  /** How often pending delegations are advanced and polled (default 2000). Push wakes advance them immediately. */
  pollIntervalMs?: number;
  /** Lifetime of each grant and delegation in seconds (default 600, max 86400). */
  ttlSecs?: number;
}

/** An explicit delegation of a server-side sub-sequence (`delegate`). No local step is parked. */
export interface DelegateRequest {
  /** Local parent instance the delegation belongs to (must exist). */
  instanceId: string;
  /** Destination runtime id (a live registration of the same tenant). */
  destinationRuntimeId: string;
  /** Server-side sequence the destination runs. */
  subSequenceId: string;
  /** Explicit input object handed to the sub-sequence (default `{}`). A string must be a JSON object. */
  input?: Record<string, unknown> | string;
}

/**
 * `preparing` (not yet accepted by the control plane), `delegated` (in the
 * destination's mailbox or running there), `completed`, `failed`, or
 * `abandoned` (never placed before its deadline).
 */
export type DelegationState = "preparing" | "delegated" | "completed" | "failed" | "abandoned";

/** Where a delegation stands, as journaled on this device. */
export interface DelegationStatus {
  delegationId: string;
  state: DelegationState;
  localInstanceId: string;
  /** The parked local step, for delegations made by a sequence. */
  blockId: string | null;
  destinationRuntimeId: string | null;
  /** The destination's reported output (JSON), once completed. */
  outputJson: string | null;
  error: string | null;
}

export interface DelegationStats {
  running: boolean;
  /** Delegations accepted by the control plane. */
  delegated: number;
  completed: number;
  failed: number;
  abandoned: number;
  /** Parked local steps resumed with an outcome (exactly once each). */
  resumed: number;
}

/**
 * Remote-task metadata the engine adds to handler params as `__orch8` when a
 * step runs on this device through the worker loop (absent for local steps).
 */
export interface Orch8TaskContext {
  /**
   * The server's deterministic idempotency key for this step's effect. Send it
   * to downstream APIs (for example as `Idempotency-Key`). `null` against
   * servers that predate the distributed-execution contract.
   */
  effectId: string | null;
  taskId: string | null;
  instanceId: string | null;
  blockId: string | null;
  attempt: number | null;
  runtimeId: string | null;
  continuityEpoch: number | null;
  resumeCheckpoint: unknown;
}

export interface HandlerContext {
  /** Handler name the step was dispatched to. */
  stepName: string;
  /** Parsed `input.__orch8`, or `null` for steps started on-device. */
  task: Orch8TaskContext | null;
}

/**
 * JS step handler. `input` is the raw JSON params string (including the
 * `__orch8` member for remote tasks). Return a JSON string or any
 * JSON-serialisable value. Throw `PermanentHandlerError` to fail the step
 * without retry; any other error is retryable.
 */
export type StepHandler = (
  stepName: string,
  input: string,
  context: HandlerContext
) => Promise<unknown> | unknown;
