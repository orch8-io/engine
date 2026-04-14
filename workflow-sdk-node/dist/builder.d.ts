/**
 * Fluent builder for workflow definitions. Produces JSON payloads that match
 * the orch8 HTTP API (POST /sequences).
 *
 * Usage:
 * ```ts
 * const wf = workflow("send_reminder")
 *   .step("send_email", "http_request", { url: "https://..." })
 *   .delay({ duration: 3600_000 })
 *   .step("log", "log_message", { level: "info" })
 *   .build();
 * ```
 */
import { type BlockDefinition, type DelaySpec, type EscalationDef, type HumanInputDef, type RaceSemantics, type RetryPolicy, type SendWindow } from "./schema.js";
export interface StepOptions {
    delay?: DelaySpec;
    retry?: RetryPolicy;
    /** Wall-clock timeout for a single execution, in milliseconds. */
    timeout?: number;
    rate_limit_key?: string;
    send_window?: SendWindow;
    cancellable?: boolean;
    wait_for_input?: HumanInputDef;
    queue_name?: string;
    /** SLA deadline in milliseconds from step start. */
    deadline?: number;
    on_deadline_breach?: EscalationDef;
}
/**
 * Fluent builder. Every chained method appends a block to the current scope
 * (top-level by default). Composite blocks (`parallel`, `race`, `tryCatch`,
 * `loop`, `forEach`, `router`, `abSplit`, `cancellationScope`) accept inner
 * builder callbacks that receive a fresh builder whose `build()` returns the
 * nested block list.
 */
export declare class WorkflowBuilder {
    private readonly name;
    private readonly namespace;
    private readonly blocks;
    constructor(name: string, namespace?: string);
    /** Append a Step block. */
    step(id: string, handler: string, params?: unknown, opts?: StepOptions): this;
    /** Append a Parallel block. All branches run concurrently; completes when all finish. */
    parallel(id: string, ...branches: Array<(b: WorkflowBuilder) => void>): this;
    /** Append a Race block. Branches compete; completes on the first winner. */
    race(id: string, semantics: RaceSemantics | undefined, ...branches: Array<(b: WorkflowBuilder) => void>): this;
    /** Append a TryCatch block. */
    tryCatch(id: string, tryFn: (b: WorkflowBuilder) => void, catchFn: (b: WorkflowBuilder) => void, finallyFn?: (b: WorkflowBuilder) => void): this;
    /** Append a Loop block. The condition is evaluated against context before each iteration. */
    loop(id: string, condition: string, body: (b: WorkflowBuilder) => void, max_iterations?: number): this;
    /** Append a ForEach block that iterates over a context collection. */
    forEach(id: string, collection: string, body: (b: WorkflowBuilder) => void, opts?: {
        item_var?: string;
        max_iterations?: number;
    }): this;
    /** Append a Router block. First matching route wins. */
    router(id: string, routes: Array<{
        condition: string;
        blocks: (b: WorkflowBuilder) => void;
    }>, defaultRoute?: (b: WorkflowBuilder) => void): this;
    /** Append a SubSequence block — invokes another sequence as a child workflow. */
    subSequence(id: string, sequence_name: string, opts?: {
        version?: number;
        input?: unknown;
    }): this;
    /** Append an A/B split. Variant selection is deterministic per instance. */
    abSplit(id: string, variants: Array<{
        name: string;
        weight: number;
        blocks: (b: WorkflowBuilder) => void;
    }>): this;
    /**
     * Append a cancellation scope — children inside this block cannot be cancelled
     * by external cancel signals until they complete.
     */
    cancellationScope(id: string, body: (b: WorkflowBuilder) => void): this;
    /** Convenience: insert a delay before the next step as its own block. */
    delay(spec: DelaySpec, idHint?: string): this;
    /** Raw access for escape hatches. Prefer named builders. */
    raw(block: BlockDefinition): this;
    /** Produce the payload for `POST /sequences`. Validates via Zod before returning. */
    build(): {
        name: string;
        namespace: string;
        blocks: BlockDefinition[];
    };
    /** Expose blocks without validation (for composing into parent builders). */
    _blocks(): BlockDefinition[];
}
/** Create a new workflow builder. */
export declare function workflow(name: string, namespace?: string): WorkflowBuilder;
