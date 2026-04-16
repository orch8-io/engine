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
import {
  type ABVariant,
  type BlockDefinition,
  type DelaySpec,
  type EscalationDef,
  type ForEachBlock,
  type HumanInputDef,
  type LoopBlock,
  type ParallelBlock,
  type RaceBlock,
  type RaceSemantics,
  type RetryPolicy,
  type Route,
  type RouterBlock,
  type SendWindow,
  SequenceCreateSchema,
  type StepBlock,
  type SubSequenceBlock,
  type TryCatchBlock,
} from "./schema.js";

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
export class WorkflowBuilder {
  private readonly blocks: BlockDefinition[] = [];

  constructor(
    private readonly name: string,
    private readonly namespace: string = "default",
  ) {}

  /** Append a Step block. */
  step(id: string, handler: string, params: unknown = {}, opts: StepOptions = {}): this {
    const block: StepBlock = {
      type: "step",
      id,
      handler,
      params,
      ...opts,
    };
    this.blocks.push(block);
    return this;
  }

  /** Append a Parallel block. All branches run concurrently; completes when all finish. */
  parallel(id: string, ...branches: Array<(b: WorkflowBuilder) => void>): this {
    const out: BlockDefinition[][] = branches.map((f) => collectBranch(f, this.namespace));
    const block: ParallelBlock = { type: "parallel", id, branches: out };
    this.blocks.push(block);
    return this;
  }

  /** Append a Race block. Branches compete; completes on the first winner. */
  race(
    id: string,
    semantics: RaceSemantics | undefined,
    ...branches: Array<(b: WorkflowBuilder) => void>
  ): this {
    const out: BlockDefinition[][] = branches.map((f) => collectBranch(f, this.namespace));
    const block: RaceBlock = { type: "race", id, branches: out, semantics };
    this.blocks.push(block);
    return this;
  }

  /** Append a TryCatch block. */
  tryCatch(
    id: string,
    tryFn: (b: WorkflowBuilder) => void,
    catchFn: (b: WorkflowBuilder) => void,
    finallyFn?: (b: WorkflowBuilder) => void,
  ): this {
    const block: TryCatchBlock = {
      type: "try_catch",
      id,
      try_block: collectBranch(tryFn, this.namespace),
      catch_block: collectBranch(catchFn, this.namespace),
      finally_block: finallyFn ? collectBranch(finallyFn, this.namespace) : undefined,
    };
    this.blocks.push(block);
    return this;
  }

  /** Append a Loop block. The condition is evaluated against context before each iteration. */
  loop(
    id: string,
    condition: string,
    body: (b: WorkflowBuilder) => void,
    max_iterations?: number,
  ): this {
    const block: LoopBlock = {
      type: "loop",
      id,
      condition,
      body: collectBranch(body, this.namespace),
      max_iterations,
    };
    this.blocks.push(block);
    return this;
  }

  /** Append a ForEach block that iterates over a context collection. */
  forEach(
    id: string,
    collection: string,
    body: (b: WorkflowBuilder) => void,
    opts: { item_var?: string; max_iterations?: number } = {},
  ): this {
    const block: ForEachBlock = {
      type: "for_each",
      id,
      collection,
      item_var: opts.item_var,
      body: collectBranch(body, this.namespace),
      max_iterations: opts.max_iterations,
    };
    this.blocks.push(block);
    return this;
  }

  /** Append a Router block. First matching route wins. */
  router(
    id: string,
    routes: Array<{ condition: string; blocks: (b: WorkflowBuilder) => void }>,
    defaultRoute?: (b: WorkflowBuilder) => void,
  ): this {
    const resolvedRoutes: Route[] = routes.map((r) => ({
      condition: r.condition,
      blocks: collectBranch(r.blocks, this.namespace),
    }));
    const block: RouterBlock = {
      type: "router",
      id,
      routes: resolvedRoutes,
      default: defaultRoute ? collectBranch(defaultRoute, this.namespace) : undefined,
    };
    this.blocks.push(block);
    return this;
  }

  /** Append a SubSequence block — invokes another sequence as a child workflow. */
  subSequence(
    id: string,
    sequence_name: string,
    opts: { version?: number; input?: unknown } = {},
  ): this {
    const block: SubSequenceBlock = {
      type: "sub_sequence",
      id,
      sequence_name,
      version: opts.version,
      input: opts.input,
    };
    this.blocks.push(block);
    return this;
  }

  /** Append an A/B split. Variant selection is deterministic per instance. */
  abSplit(
    id: string,
    variants: Array<{ name: string; weight: number; blocks: (b: WorkflowBuilder) => void }>,
  ): this {
    const resolved: ABVariant[] = variants.map((v) => ({
      name: v.name,
      weight: v.weight,
      blocks: collectBranch(v.blocks, this.namespace),
    }));
    this.blocks.push({ type: "ab_split", id, variants: resolved });
    return this;
  }

  /**
   * Append a cancellation scope — children inside this block cannot be cancelled
   * by external cancel signals until they complete.
   */
  cancellationScope(id: string, body: (b: WorkflowBuilder) => void): this {
    this.blocks.push({
      type: "cancellation_scope",
      id,
      blocks: collectBranch(body, this.namespace),
    });
    return this;
  }

  /** Convenience: insert a delay before the next step as its own block. */
  delay(spec: DelaySpec, idHint = "delay"): this {
    // Delays are attached to steps; a standalone delay is represented as a
    // no-op step with a delay. Callers who need a real step should prefer
    // passing `delay` via StepOptions.
    const id = `${idHint}_${this.blocks.length}`;
    this.blocks.push({
      type: "step",
      id,
      handler: "noop",
      params: {},
      delay: spec,
    });
    return this;
  }

  /** Raw access for escape hatches. Prefer named builders. */
  raw(block: BlockDefinition): this {
    this.blocks.push(block);
    return this;
  }

  /** Produce the payload for `POST /sequences`. Validates via Zod before returning. */
  build(): { name: string; namespace: string; blocks: BlockDefinition[] } {
    const payload = {
      name: this.name,
      namespace: this.namespace,
      blocks: this.blocks,
    };
    // Strict validation — throws on any shape mismatch.
    const parsed = SequenceCreateSchema.parse(payload);
    return {
      name: parsed.name,
      namespace: parsed.namespace ?? "default",
      blocks: parsed.blocks as BlockDefinition[],
    };
  }

  /** Expose blocks without validation (for composing into parent builders). */
  _blocks(): BlockDefinition[] {
    return this.blocks;
  }
}

function collectBranch(
  f: (b: WorkflowBuilder) => void,
  namespace: string,
): BlockDefinition[] {
  const inner = new WorkflowBuilder("_inner", namespace);
  f(inner);
  return inner._blocks();
}

/** Create a new workflow builder. */
export function workflow(name: string, namespace = "default"): WorkflowBuilder {
  return new WorkflowBuilder(name, namespace);
}
