import type {
  BackgroundRunResult,
  DelegateRequest,
  DelegationOptions,
  DelegationStats,
  DelegationStatus,
  DeviceContext,
  FlushResult,
  HandlerContext,
  InstanceState,
  InstanceSummary,
  NodeCapabilities,
  NodeConnectivity,
  NodeRegistration,
  Orch8Config,
  Orch8TaskContext,
  PowerState,
  PushWakeEnvelope,
  SequenceInfo,
  StepHandler,
  SyncResult,
  TickResult,
  WorkerOptions,
  WorkerStats,
  WorkerWindowResult,
} from "./types";

/** Event names emitted by the native module. */
export const EVENTS = {
  instanceCompleted: "orch8:instanceCompleted",
  instanceFailed: "orch8:instanceFailed",
  stepPending: "orch8:stepPending",
  executeStep: "orch8:executeStep",
} as const;

/** Throw from a handler to fail the step permanently (no retry). */
export class PermanentHandlerError extends Error {
  readonly permanent = true;
  constructor(message: string) {
    super(message);
    this.name = "PermanentHandlerError";
  }
}

/**
 * The native module surface (ios/Orch8Module.swift, android/.../Orch8Module.kt).
 * Every method returns a promise; blocking engine calls run off the JS thread.
 */
export interface NativeOrch8 {
  initialize(config: Orch8Config): Promise<void>;
  registerHandler(name: string): Promise<void>;
  resolveStep(
    requestId: string,
    output: string | null,
    error: string | null,
    permanent: boolean
  ): void;
  resume(): Promise<void>;
  pause(): Promise<void>;
  tickOnce(): Promise<TickResult>;
  runUntilIdle(maxTicks: number, timeBudgetMs: number): Promise<BackgroundRunResult>;
  reportPowerState(state: PowerState): Promise<void>;
  start(sequenceName: string, input: string, dedupKey: string | null): Promise<string>;
  cancelInstance(instanceId: string): Promise<void>;
  getInstance(instanceId: string): Promise<InstanceState>;
  activeInstances(): Promise<InstanceSummary[]>;
  completeStep(instanceId: string, stepName: string, output: string): Promise<void>;
  loadSequenceFromJson(json: string): Promise<void>;
  loadSequencesFromUrl(url: string): Promise<number>;
  loadedSequences(): Promise<SequenceInfo[]>;
  sync(manifestUrl: string, token: string | null): Promise<SyncResult>;
  flushTelemetry(endpointUrl: string): Promise<FlushResult>;
  setDeviceContext(ctx: DeviceContext): Promise<void>;
  onPushReceived(): Promise<void>;
  shutdown(): Promise<void>;
  // Runtime node / worker.
  nodeRuntimeId(): Promise<string>;
  registerNode(capabilities: NodeCapabilities): Promise<NodeRegistration>;
  updateNodeStatus(connectivity: NodeConnectivity | null, batteryPercent: number | null): Promise<void>;
  unregisterNode(): Promise<void>;
  startWorker(options: WorkerOptions): Promise<void>;
  stopWorker(): Promise<void>;
  runWorkerWindow(timeBudgetMs: number): Promise<WorkerWindowResult>;
  workerStats(): Promise<WorkerStats>;
  onPushWake(envelopeJson: string): Promise<boolean>;
  enableBuiltin(name: string): Promise<void>;
  // Delegation from phone-local workflows.
  startDelegation(options: DelegationOptions): Promise<void>;
  stopDelegation(): Promise<void>;
  delegate(request: NativeDelegateRequest): Promise<string>;
  delegationStatus(delegationId: string): Promise<DelegationStatus>;
  listDelegations(): Promise<DelegationStatus[]>;
  delegationStats(): Promise<DelegationStats>;
}

/** `DelegateRequest` as it crosses the bridge: the input is already JSON. */
export interface NativeDelegateRequest {
  instanceId: string;
  destinationRuntimeId: string;
  subSequenceId: string;
  inputJson: string;
}

export interface Subscription {
  remove(): void;
}

export interface EventSource {
  addListener(event: string, listener: (event: any) => void): Subscription;
}

interface ExecuteStepEvent {
  requestId: string;
  stepName: string;
  input: string;
}

const str = (v: unknown): string | null => (typeof v === "string" ? v : null);
const num = (v: unknown): number | null => (typeof v === "number" && Number.isFinite(v) ? v : null);

/**
 * Reads the reserved `__orch8` member the worker loop adds to a remote task's
 * params. Returns `null` for local steps, non-object params, or invalid JSON.
 */
export function parseTaskContext(input: string): Orch8TaskContext | null {
  let parsed: unknown;
  try {
    parsed = JSON.parse(input);
  } catch {
    return null;
  }
  if (!parsed || typeof parsed !== "object" || Array.isArray(parsed)) return null;
  const meta = (parsed as Record<string, unknown>).__orch8;
  if (!meta || typeof meta !== "object" || Array.isArray(meta)) return null;
  const m = meta as Record<string, unknown>;
  return {
    effectId: str(m.effect_id),
    taskId: str(m.task_id),
    instanceId: str(m.instance_id),
    blockId: str(m.block_id),
    attempt: num(m.attempt),
    runtimeId: str(m.runtime_id),
    continuityEpoch: num(m.continuity_epoch),
    resumeCheckpoint: m.resume_checkpoint ?? null,
  };
}

function toOutputJson(result: unknown): string {
  if (typeof result === "string") return result;
  if (result === undefined) return "{}";
  return JSON.stringify(result);
}

/** Maximum delegation / grant lifetime the control plane accepts. */
export const MAX_DELEGATION_TTL_SECS = 86_400;

function nonEmpty(name: string, value: unknown): string {
  if (typeof value !== "string" || value.trim() === "") {
    throw new TypeError(`${name} must be a non-empty string`);
  }
  return value;
}

/** Validates delegation options before they reach the native module. */
export function validateDelegationOptions(options: DelegationOptions): DelegationOptions {
  if (!options || typeof options !== "object") throw new TypeError("options must be an object");
  nonEmpty("tenantId", options.tenantId);
  if (options.pollIntervalMs !== undefined) positiveInt("pollIntervalMs", options.pollIntervalMs);
  if (options.ttlSecs !== undefined) {
    positiveInt("ttlSecs", options.ttlSecs);
    if (options.ttlSecs > MAX_DELEGATION_TTL_SECS) {
      throw new RangeError(`ttlSecs must be at most ${MAX_DELEGATION_TTL_SECS}`);
    }
  }
  return options;
}

/** Serialises a `DelegateRequest` for the bridge; the input must be a JSON object. */
export function toNativeDelegateRequest(request: DelegateRequest): NativeDelegateRequest {
  if (!request || typeof request !== "object") throw new TypeError("request must be an object");
  const input = request.input ?? {};
  let inputJson: string;
  if (typeof input === "string") {
    let parsed: unknown;
    try {
      parsed = JSON.parse(input);
    } catch {
      throw new TypeError("input must be a JSON object");
    }
    if (!parsed || typeof parsed !== "object" || Array.isArray(parsed)) {
      throw new TypeError("input must be a JSON object");
    }
    inputJson = input;
  } else if (typeof input === "object" && !Array.isArray(input)) {
    inputJson = JSON.stringify(input);
  } else {
    throw new TypeError("input must be a JSON object");
  }
  return {
    instanceId: nonEmpty("instanceId", request.instanceId),
    destinationRuntimeId: nonEmpty("destinationRuntimeId", request.destinationRuntimeId),
    subSequenceId: nonEmpty("subSequenceId", request.subSequenceId),
    inputJson,
  };
}

function positiveInt(name: string, value: number): number {
  if (!Number.isInteger(value) || value <= 0) {
    throw new RangeError(`${name} must be a positive integer`);
  }
  return value;
}

export class Orch8Client {
  private readonly handlers = new Map<string, StepHandler>();
  private stepSubscription: Subscription | null = null;

  constructor(
    private readonly native: NativeOrch8,
    private readonly events: EventSource
  ) {}

  async initialize(config: Orch8Config = {}): Promise<void> {
    await this.native.initialize(config);
    this.setupStepDispatch();
  }

  /**
   * Register a JS handler. The native engine calls it for local steps and, once
   * `startWorker` runs, for remote tasks claimed by this device. The engine
   * thread waits for the returned promise up to `handlerTimeoutMs`.
   */
  async registerHandler(name: string, handler: StepHandler): Promise<void> {
    this.handlers.set(name, handler);
    await this.native.registerHandler(name);
  }

  resume(): Promise<void> {
    return this.native.resume();
  }

  pause(): Promise<void> {
    return this.native.pause();
  }

  tickOnce(): Promise<TickResult> {
    return this.native.tickOnce();
  }

  /** Drain work inside an OS-granted background window. */
  async runUntilIdle(maxTicks: number, timeBudgetMs: number): Promise<BackgroundRunResult> {
    return this.native.runUntilIdle(
      positiveInt("maxTicks", maxTicks),
      positiveInt("timeBudgetMs", timeBudgetMs)
    );
  }

  reportPowerState(state: PowerState): Promise<void> {
    return this.native.reportPowerState(state);
  }

  start(sequenceName: string, input: string | object = "{}", dedupKey?: string): Promise<string> {
    const json = typeof input === "string" ? input : JSON.stringify(input);
    return this.native.start(sequenceName, json, dedupKey ?? null);
  }

  cancelInstance(instanceId: string): Promise<void> {
    return this.native.cancelInstance(instanceId);
  }

  getInstance(instanceId: string): Promise<InstanceState> {
    return this.native.getInstance(instanceId);
  }

  activeInstances(): Promise<InstanceSummary[]> {
    return this.native.activeInstances();
  }

  completeStep(instanceId: string, stepName: string, output: string | object): Promise<void> {
    const json = typeof output === "string" ? output : JSON.stringify(output);
    return this.native.completeStep(instanceId, stepName, json);
  }

  loadSequenceFromJson(json: string | object): Promise<void> {
    return this.native.loadSequenceFromJson(typeof json === "string" ? json : JSON.stringify(json));
  }

  /** Empty `url` uses the configured `sequencesUrl`. Returns the number loaded. */
  loadSequencesFromUrl(url = ""): Promise<number> {
    return this.native.loadSequencesFromUrl(url);
  }

  loadedSequences(): Promise<SequenceInfo[]> {
    return this.native.loadedSequences();
  }

  /** `token` is sent as a bearer token with each manifest request. */
  sync(manifestUrl: string, token?: string): Promise<SyncResult> {
    return this.native.sync(manifestUrl, token ?? null);
  }

  flushTelemetry(endpointUrl: string): Promise<FlushResult> {
    return this.native.flushTelemetry(endpointUrl);
  }

  setDeviceContext(ctx: DeviceContext): Promise<void> {
    return this.native.setDeviceContext(ctx);
  }

  /** Trigger an immediate sync and worker poll after a push notification. */
  onPushReceived(): Promise<void> {
    return this.native.onPushReceived();
  }

  async shutdown(): Promise<void> {
    this.stepSubscription?.remove();
    this.stepSubscription = null;
    await this.native.shutdown();
  }

  // -- Runtime node / worker (engine release after 0.7.1) ------------------

  /** Stable runtime UUID of this installation (the lease `worker_id`). */
  nodeRuntimeId(): Promise<string> {
    return this.native.nodeRuntimeId();
  }

  /**
   * Join the runtime mesh: registers the device and its capabilities using
   * `syncUrl` + `syncApiKey`, then re-advertises before the 5-minute TTL.
   */
  async registerNode(capabilities: NodeCapabilities = {}): Promise<NodeRegistration> {
    if (
      capabilities.batteryPercent !== undefined &&
      !(Number.isInteger(capabilities.batteryPercent) &&
        capabilities.batteryPercent >= 0 &&
        capabilities.batteryPercent <= 100)
    ) {
      throw new RangeError("batteryPercent must be an integer between 0 and 100");
    }
    return this.native.registerNode(capabilities);
  }

  async updateNodeStatus(connectivity?: NodeConnectivity, batteryPercent?: number): Promise<void> {
    if (
      batteryPercent !== undefined &&
      !(Number.isInteger(batteryPercent) && batteryPercent >= 0 && batteryPercent <= 100)
    ) {
      throw new RangeError("batteryPercent must be an integer between 0 and 100");
    }
    return this.native.updateNodeStatus(connectivity ?? null, batteryPercent ?? null);
  }

  /** Stop the worker, advertise `draining`, stop re-advertising. */
  unregisterNode(): Promise<void> {
    return this.native.unregisterNode();
  }

  /** Start the remote worker loop. Register handlers first. */
  async startWorker(options: WorkerOptions = {}): Promise<void> {
    if (options.maxConcurrentTasks !== undefined) positiveInt("maxConcurrentTasks", options.maxConcurrentTasks);
    if (options.idlePollIntervalMs !== undefined) positiveInt("idlePollIntervalMs", options.idlePollIntervalMs);
    return this.native.startWorker(options);
  }

  stopWorker(): Promise<void> {
    return this.native.stopWorker();
  }

  /** Claim and run remote tasks inside a bounded background window (claims even while paused). */
  async runWorkerWindow(timeBudgetMs: number): Promise<WorkerWindowResult> {
    return this.native.runWorkerWindow(positiveInt("timeBudgetMs", timeBudgetMs));
  }

  workerStats(): Promise<WorkerStats> {
    return this.native.workerStats();
  }

  /**
   * Forward an id-only wake push (`{task_id?, runtime_id?, reason?}`, or a
   * payload nesting them under `orch8`). Resolves `false` when the push is not
   * for this runtime or carries no Orch8 fields.
   */
  async onPushWake(payload: PushWakeEnvelope | Record<string, unknown> | string): Promise<boolean> {
    const envelope = typeof payload === "string" ? payload : JSON.stringify(pickWakeFields(payload as Record<string, unknown>));
    if (envelope === "{}") return false;
    return this.native.onPushWake(envelope);
  }

  /** Enable an opt-in builtin handler (`http_request`) before `resume()`. */
  enableBuiltin(name: string): Promise<void> {
    return this.native.enableBuiltin(name);
  }

  // -- Delegation from phone-local workflows (engine release after 0.7.1) ---

  /**
   * Start the delegation pump: a step of a workflow running on this engine
   * whose `$runtime` places it on another runtime is handed to that runtime
   * through the server mailbox; the local instance parks and resumes exactly
   * once with the result. Requires `registerNode`. Journaled delegations
   * survive app kills: call again after relaunch.
   */
  async startDelegation(options: DelegationOptions): Promise<void> {
    return this.native.startDelegation(validateDelegationOptions(options));
  }

  /** Pause the pump. Journaled delegations resume with the next `startDelegation`. */
  stopDelegation(): Promise<void> {
    return this.native.stopDelegation();
  }

  /**
   * Delegate a server-side sub-sequence on behalf of a local instance without
   * parking a step. Resolves the delegation id; read the outcome with
   * `delegationStatus`. Requires `startDelegation`.
   */
  async delegate(request: DelegateRequest): Promise<string> {
    return this.native.delegate(toNativeDelegateRequest(request));
  }

  /** The locally journaled state of a delegation (rejects when unknown). */
  async delegationStatus(delegationId: string): Promise<DelegationStatus> {
    return this.native.delegationStatus(nonEmpty("delegationId", delegationId));
  }

  /** Every journaled delegation, oldest first. */
  listDelegations(): Promise<DelegationStatus[]> {
    return this.native.listDelegations();
  }

  /** Pump counters (zeros while it is not running). */
  delegationStats(): Promise<DelegationStats> {
    return this.native.delegationStats();
  }

  // -- Events ---------------------------------------------------------------

  onInstanceCompleted(callback: (instanceId: string, output: string) => void): () => void {
    const sub = this.events.addListener(EVENTS.instanceCompleted, (e) => callback(e.instanceId, e.output));
    return () => sub.remove();
  }

  onInstanceFailed(callback: (instanceId: string, error: string) => void): () => void {
    const sub = this.events.addListener(EVENTS.instanceFailed, (e) => callback(e.instanceId, e.error));
    return () => sub.remove();
  }

  onStepPending(callback: (instanceId: string, stepName: string, handler: string) => void): () => void {
    const sub = this.events.addListener(EVENTS.stepPending, (e) =>
      callback(e.instanceId, e.stepName, e.handler)
    );
    return () => sub.remove();
  }

  private setupStepDispatch(): void {
    this.stepSubscription?.remove();
    this.stepSubscription = this.events.addListener(EVENTS.executeStep, (event: ExecuteStepEvent) => {
      void this.dispatch(event);
    });
  }

  /** @internal exposed for tests */
  async dispatch(event: ExecuteStepEvent): Promise<void> {
    const handler = this.handlers.get(event.stepName);
    if (!handler) {
      this.native.resolveStep(
        event.requestId,
        null,
        `No handler registered for step '${event.stepName}'`,
        // Retryable: after a JS reload the handler is registered again.
        false
      );
      return;
    }
    const context: HandlerContext = {
      stepName: event.stepName,
      task: parseTaskContext(event.input),
    };
    try {
      const result = await handler(event.stepName, event.input, context);
      this.native.resolveStep(event.requestId, toOutputJson(result), null, false);
    } catch (e: unknown) {
      const message = e instanceof Error ? e.message : String(e);
      const permanent =
        e instanceof PermanentHandlerError ||
        (typeof e === "object" && e !== null && (e as { permanent?: unknown }).permanent === true);
      this.native.resolveStep(event.requestId, null, message, permanent);
    }
  }
}

function pickWakeFields(payload: Record<string, unknown>): PushWakeEnvelope {
  const nested = payload.orch8;
  const source =
    nested && typeof nested === "object" && !Array.isArray(nested)
      ? (nested as Record<string, unknown>)
      : payload;
  const out: PushWakeEnvelope = {};
  for (const key of ["task_id", "runtime_id", "reason"] as const) {
    const value = source[key];
    if (typeof value === "string") out[key] = value;
  }
  return out;
}
