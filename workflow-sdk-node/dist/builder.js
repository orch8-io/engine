"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.WorkflowBuilder = void 0;
exports.workflow = workflow;
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
const schema_js_1 = require("./schema.js");
/**
 * Fluent builder. Every chained method appends a block to the current scope
 * (top-level by default). Composite blocks (`parallel`, `race`, `tryCatch`,
 * `loop`, `forEach`, `router`, `abSplit`, `cancellationScope`) accept inner
 * builder callbacks that receive a fresh builder whose `build()` returns the
 * nested block list.
 */
class WorkflowBuilder {
    name;
    namespace;
    blocks = [];
    constructor(name, namespace = "default") {
        this.name = name;
        this.namespace = namespace;
    }
    /** Append a Step block. */
    step(id, handler, params = {}, opts = {}) {
        const block = {
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
    parallel(id, ...branches) {
        const out = branches.map((f) => collectBranch(f, this.namespace));
        const block = { type: "parallel", id, branches: out };
        this.blocks.push(block);
        return this;
    }
    /** Append a Race block. Branches compete; completes on the first winner. */
    race(id, semantics, ...branches) {
        const out = branches.map((f) => collectBranch(f, this.namespace));
        const block = { type: "race", id, branches: out, semantics };
        this.blocks.push(block);
        return this;
    }
    /** Append a TryCatch block. */
    tryCatch(id, tryFn, catchFn, finallyFn) {
        const block = {
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
    loop(id, condition, body, max_iterations) {
        const block = {
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
    forEach(id, collection, body, opts = {}) {
        const block = {
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
    router(id, routes, defaultRoute) {
        const resolvedRoutes = routes.map((r) => ({
            condition: r.condition,
            blocks: collectBranch(r.blocks, this.namespace),
        }));
        const block = {
            type: "router",
            id,
            routes: resolvedRoutes,
            default: defaultRoute ? collectBranch(defaultRoute, this.namespace) : undefined,
        };
        this.blocks.push(block);
        return this;
    }
    /** Append a SubSequence block — invokes another sequence as a child workflow. */
    subSequence(id, sequence_name, opts = {}) {
        const block = {
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
    abSplit(id, variants) {
        const resolved = variants.map((v) => ({
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
    cancellationScope(id, body) {
        this.blocks.push({
            type: "cancellation_scope",
            id,
            blocks: collectBranch(body, this.namespace),
        });
        return this;
    }
    /** Convenience: insert a delay before the next step as its own block. */
    delay(spec, idHint = "delay") {
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
    raw(block) {
        this.blocks.push(block);
        return this;
    }
    /** Produce the payload for `POST /sequences`. Validates via Zod before returning. */
    build() {
        const payload = {
            name: this.name,
            namespace: this.namespace,
            blocks: this.blocks,
        };
        // Strict validation — throws on any shape mismatch.
        const parsed = schema_js_1.SequenceCreateSchema.parse(payload);
        return {
            name: parsed.name,
            namespace: parsed.namespace ?? "default",
            blocks: parsed.blocks,
        };
    }
    /** Expose blocks without validation (for composing into parent builders). */
    _blocks() {
        return this.blocks;
    }
}
exports.WorkflowBuilder = WorkflowBuilder;
function collectBranch(f, namespace) {
    const inner = new WorkflowBuilder("_inner", namespace);
    f(inner);
    return inner._blocks();
}
/** Create a new workflow builder. */
function workflow(name, namespace = "default") {
    return new WorkflowBuilder(name, namespace);
}
