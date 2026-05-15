import { NativeModules, NativeEventEmitter, Platform } from "react-native";

const { Orch8Module } = NativeModules;

if (!Orch8Module) {
  throw new Error(
    "react-native-orch8: NativeModule not found. " +
      "Make sure you have linked the native module correctly."
  );
}

const emitter = new NativeEventEmitter(Orch8Module);

export interface Orch8Config {
  dbPath?: string;
  tickIntervalMs?: number;
  maxConcurrentSteps?: number;
  maxStepsPerInstance?: number;
  maxConcurrentInstances?: number;
  maxTickDurationMs?: number;
  maxInstanceLifetimeSecs?: number;
  maxStoredSequences?: number;
  maxSequenceSizeBytes?: number;
  handlerTimeoutMs?: number;
  operationTimeoutMs?: number;
  telemetryEnabled?: boolean;
  environment?: string;
  rootPublicKey?: string;
  sdkVersion?: string;
}

export interface TickResult {
  instancesAdvanced: number;
  stepsExecuted: number;
  hasPendingWork: boolean;
}

export interface SyncResult {
  added: number;
  updated: number;
  removed: number;
  skipped: number;
}

export interface SequenceInfo {
  name: string;
  version: number;
}

export interface InstanceSummary {
  instanceId: string;
  sequenceName: string;
  state: string;
  createdAt: string;
}

export interface InstanceState {
  instanceId: string;
  sequenceName: string;
  state: string;
  context: string;
  createdAt: string;
  updatedAt: string;
}

export type StepHandler = (
  stepName: string,
  input: string
) => Promise<string> | string;

export interface EngineEvents {
  onInstanceCompleted: (event: {
    instanceId: string;
    output: string;
  }) => void;
  onInstanceFailed: (event: {
    instanceId: string;
    error: string;
  }) => void;
  onStepPending: (event: {
    instanceId: string;
    stepName: string;
    handler: string;
  }) => void;
}

class Orch8 {
  private handlers = new Map<string, StepHandler>();
  private stepSubscription: ReturnType<typeof emitter.addListener> | null =
    null;

  async initialize(config: Orch8Config = {}): Promise<void> {
    await Orch8Module.initialize(config);
    this.setupStepDispatch();
  }

  async registerHandler(name: string, handler: StepHandler): Promise<void> {
    this.handlers.set(name, handler);
    await Orch8Module.registerHandler(name);
  }

  async resume(): Promise<void> {
    await Orch8Module.resume();
  }

  async pause(): Promise<void> {
    await Orch8Module.pause();
  }

  async tickOnce(): Promise<TickResult> {
    return Orch8Module.tickOnce();
  }

  async start(
    sequenceName: string,
    input: string = "{}",
    dedupKey?: string
  ): Promise<string> {
    return Orch8Module.start(sequenceName, input, dedupKey ?? null);
  }

  async cancelInstance(instanceId: string): Promise<void> {
    await Orch8Module.cancelInstance(instanceId);
  }

  async getInstance(instanceId: string): Promise<InstanceState> {
    return Orch8Module.getInstance(instanceId);
  }

  async activeInstances(): Promise<InstanceSummary[]> {
    return Orch8Module.activeInstances();
  }

  async completeStep(
    instanceId: string,
    stepName: string,
    output: string
  ): Promise<void> {
    await Orch8Module.completeStep(instanceId, stepName, output);
  }

  async loadSequenceFromJson(json: string): Promise<void> {
    await Orch8Module.loadSequenceFromJson(json);
  }

  async loadedSequences(): Promise<SequenceInfo[]> {
    return Orch8Module.loadedSequences();
  }

  async sync(
    manifestUrl: string,
    token?: string
  ): Promise<SyncResult> {
    return Orch8Module.sync(manifestUrl, token ?? null);
  }

  async flushTelemetry(endpointUrl: string): Promise<void> {
    await Orch8Module.flushTelemetry(endpointUrl);
  }

  async shutdown(): Promise<void> {
    this.stepSubscription?.remove();
    this.stepSubscription = null;
    await Orch8Module.shutdown();
  }

  onInstanceCompleted(
    callback: (instanceId: string, output: string) => void
  ): () => void {
    const sub = emitter.addListener("orch8:instanceCompleted", (event) => {
      callback(event.instanceId, event.output);
    });
    return () => sub.remove();
  }

  onInstanceFailed(
    callback: (instanceId: string, error: string) => void
  ): () => void {
    const sub = emitter.addListener("orch8:instanceFailed", (event) => {
      callback(event.instanceId, event.error);
    });
    return () => sub.remove();
  }

  onStepPending(
    callback: (instanceId: string, stepName: string, handler: string) => void
  ): () => void {
    const sub = emitter.addListener("orch8:stepPending", (event) => {
      callback(event.instanceId, event.stepName, event.handler);
    });
    return () => sub.remove();
  }

  private setupStepDispatch() {
    this.stepSubscription?.remove();
    this.stepSubscription = emitter.addListener(
      "orch8:executeStep",
      async (event: { requestId: string; stepName: string; input: string }) => {
        const handler = this.handlers.get(event.stepName);
        if (!handler) {
          Orch8Module.resolveStep(
            event.requestId,
            null,
            `No handler registered for step '${event.stepName}'`
          );
          return;
        }
        try {
          const result = await handler(event.stepName, event.input);
          Orch8Module.resolveStep(event.requestId, result, null);
        } catch (e: unknown) {
          const message = e instanceof Error ? e.message : String(e);
          Orch8Module.resolveStep(event.requestId, null, message);
        }
      }
    );
  }
}

export const orch8 = new Orch8();
export default orch8;
