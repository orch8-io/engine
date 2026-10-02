export function validateSequenceJson(input: string): string
export function sequenceSchemaVersion(): number

/** Run an isolated dry-run workflow with a bounded scheduler tick budget. */
export function runSequenceJson(
  sequenceJson: string,
  inputJson?: string,
  maxTicks?: number,
): Promise<string>

/** What a step handler receives. */
export interface StepInput<Data = any, Params = any> {
  /** The step's `params`, with `{{ ... }}` templates resolved. */
  params: Params
  /** The instance's `context.data` (the input passed to `start`). */
  data: Data
  /** Outputs of every already-completed step, keyed by step id. */
  outputs: Record<string, any>
  instanceId: string
  stepId: string
  attempt: number
}

export type StepHandler = (input: StepInput) => unknown | Promise<unknown>

export type InstanceState =
  | 'scheduled'
  | 'running'
  | 'waiting'
  | 'paused'
  | 'completed'
  | 'failed'
  | 'cancelled'

export interface InstanceSnapshot {
  id: string
  state: InstanceState
  data: Record<string, any>
  /** Latest output per step id. A failed step's output has `__error__: true` and `message`. */
  outputs: Record<string, any>
}

/** Throw from a handler to fail the step without retrying. */
export class PermanentError extends Error {
  readonly permanent: true
  constructor(message?: string, options?: ErrorOptions)
}

/** A durable workflow engine persisted in one SQLite file, in-process. */
export class Engine {
  /** Open (or create) the SQLite database at `path`. One process per file. */
  static open(path: string): Promise<Engine>
  private constructor(inner: unknown)
  /** Register a step handler. All handlers must be registered before the first other call. */
  handler(name: string, fn: StepHandler): this
  /** Store a sequence (object or JSON); only `name` and `blocks` are required. Returns its id. */
  deploy(sequence: object | string): Promise<string>
  /** Start an instance of a deployed sequence; returns the instance id. */
  start(
    name: string,
    input?: object,
    options?: { idempotencyKey?: string; version?: number },
  ): Promise<string>
  /** Drive the engine until the instance is completed/failed/cancelled/paused/waiting, or `timeoutMs` elapses. */
  run(id: string, options?: { timeoutMs?: number }): Promise<InstanceSnapshot>
  get(id: string): Promise<InstanceSnapshot>
  /** `pause` | `resume` | `cancel` | `update_context`, or a custom signal name. */
  signal(id: string, signal: string, payload?: unknown): Promise<void>
  close(): Promise<void>
}
