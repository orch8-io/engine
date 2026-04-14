"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.SequenceCreateSchema = exports.CancellationScopeBlockSchema = exports.ABSplitBlockSchema = exports.ABVariantSchema = exports.SubSequenceBlockSchema = exports.TryCatchBlockSchema = exports.RouterBlockSchema = exports.RouteSchema = exports.ForEachBlockSchema = exports.LoopBlockSchema = exports.RaceBlockSchema = exports.RaceSemanticsSchema = exports.ParallelBlockSchema = exports.StepBlockSchema = exports.BlockDefinitionSchema = exports.RetryPolicySchema = exports.EscalationDefSchema = exports.HumanInputDefSchema = exports.ContextAccessSchema = exports.SendWindowSchema = exports.DelaySpecSchema = exports.DurationMsSchema = void 0;
/**
 * Zod schemas mirroring the orch8 workflow DSL.
 *
 * These mirror the Rust types in `orch8-types/src/sequence.rs` which are
 * serialized with `#[serde(tag = "type", rename_all = "snake_case")]`.
 *
 * Durations are in milliseconds (u64). Do not convert — the engine expects
 * integer millis on the wire (see `crate::serde_duration` in Rust side).
 */
const zod_1 = require("zod");
// ─── Primitives ──────────────────────────────────────────────────────────────
exports.DurationMsSchema = zod_1.z.number().int().nonnegative();
exports.DelaySpecSchema = zod_1.z.object({
    duration: exports.DurationMsSchema,
    business_days_only: zod_1.z.boolean().optional(),
    jitter: exports.DurationMsSchema.optional(),
    holidays: zod_1.z.array(zod_1.z.string()).optional(),
});
exports.SendWindowSchema = zod_1.z.object({
    start_hour: zod_1.z.number().int().min(0).max(23).optional(),
    end_hour: zod_1.z.number().int().min(0).max(23).optional(),
    days: zod_1.z.array(zod_1.z.number().int().min(0).max(6)).optional(),
});
exports.ContextAccessSchema = zod_1.z.object({
    data: zod_1.z.boolean().optional(),
    config: zod_1.z.boolean().optional(),
    audit: zod_1.z.boolean().optional(),
    runtime: zod_1.z.boolean().optional(),
});
exports.HumanInputDefSchema = zod_1.z.object({
    prompt: zod_1.z.string().optional(),
    timeout: exports.DurationMsSchema.optional(),
    escalation_handler: zod_1.z.string().optional(),
});
exports.EscalationDefSchema = zod_1.z.object({
    handler: zod_1.z.string(),
    params: zod_1.z.unknown().optional(),
});
exports.RetryPolicySchema = zod_1.z.object({
    max_attempts: zod_1.z.number().int().positive(),
    initial_backoff: exports.DurationMsSchema,
    max_backoff: exports.DurationMsSchema,
    backoff_multiplier: zod_1.z.number().positive().optional(),
});
// ─── Block definitions (recursive) ───────────────────────────────────────────
// We declare the forward reference up-front so nested schemas can cite it.
// eslint-disable-next-line @typescript-eslint/no-explicit-any
exports.BlockDefinitionSchema = zod_1.z.lazy(() => zod_1.z.discriminatedUnion("type", [
    exports.StepBlockSchema,
    exports.ParallelBlockSchema,
    exports.RaceBlockSchema,
    exports.LoopBlockSchema,
    exports.ForEachBlockSchema,
    exports.RouterBlockSchema,
    exports.TryCatchBlockSchema,
    exports.SubSequenceBlockSchema,
    exports.ABSplitBlockSchema,
    exports.CancellationScopeBlockSchema,
]));
exports.StepBlockSchema = zod_1.z.object({
    type: zod_1.z.literal("step"),
    id: zod_1.z.string(),
    handler: zod_1.z.string(),
    params: zod_1.z.unknown().optional(),
    delay: exports.DelaySpecSchema.nullable().optional(),
    retry: exports.RetryPolicySchema.nullable().optional(),
    timeout: exports.DurationMsSchema.optional(),
    rate_limit_key: zod_1.z.string().optional(),
    send_window: exports.SendWindowSchema.optional(),
    context_access: exports.ContextAccessSchema.optional(),
    cancellable: zod_1.z.boolean().optional(),
    wait_for_input: exports.HumanInputDefSchema.optional(),
    queue_name: zod_1.z.string().optional(),
    deadline: exports.DurationMsSchema.optional(),
    on_deadline_breach: exports.EscalationDefSchema.optional(),
});
exports.ParallelBlockSchema = zod_1.z.object({
    type: zod_1.z.literal("parallel"),
    id: zod_1.z.string(),
    branches: zod_1.z.array(zod_1.z.array(zod_1.z.lazy(() => exports.BlockDefinitionSchema))),
});
exports.RaceSemanticsSchema = zod_1.z.enum(["first_to_resolve", "first_to_succeed"]);
exports.RaceBlockSchema = zod_1.z.object({
    type: zod_1.z.literal("race"),
    id: zod_1.z.string(),
    branches: zod_1.z.array(zod_1.z.array(zod_1.z.lazy(() => exports.BlockDefinitionSchema))),
    semantics: exports.RaceSemanticsSchema.optional(),
});
exports.LoopBlockSchema = zod_1.z.object({
    type: zod_1.z.literal("loop"),
    id: zod_1.z.string(),
    condition: zod_1.z.string(),
    body: zod_1.z.array(zod_1.z.lazy(() => exports.BlockDefinitionSchema)),
    max_iterations: zod_1.z.number().int().positive().optional(),
});
exports.ForEachBlockSchema = zod_1.z.object({
    type: zod_1.z.literal("for_each"),
    id: zod_1.z.string(),
    collection: zod_1.z.string(),
    item_var: zod_1.z.string().optional(),
    body: zod_1.z.array(zod_1.z.lazy(() => exports.BlockDefinitionSchema)),
    max_iterations: zod_1.z.number().int().positive().optional(),
});
exports.RouteSchema = zod_1.z.object({
    condition: zod_1.z.string(),
    blocks: zod_1.z.array(zod_1.z.lazy(() => exports.BlockDefinitionSchema)),
});
exports.RouterBlockSchema = zod_1.z.object({
    type: zod_1.z.literal("router"),
    id: zod_1.z.string(),
    routes: zod_1.z.array(exports.RouteSchema),
    default: zod_1.z.array(zod_1.z.lazy(() => exports.BlockDefinitionSchema)).optional(),
});
exports.TryCatchBlockSchema = zod_1.z.object({
    type: zod_1.z.literal("try_catch"),
    id: zod_1.z.string(),
    try_block: zod_1.z.array(zod_1.z.lazy(() => exports.BlockDefinitionSchema)),
    catch_block: zod_1.z.array(zod_1.z.lazy(() => exports.BlockDefinitionSchema)),
    finally_block: zod_1.z.array(zod_1.z.lazy(() => exports.BlockDefinitionSchema)).optional(),
});
exports.SubSequenceBlockSchema = zod_1.z.object({
    type: zod_1.z.literal("sub_sequence"),
    id: zod_1.z.string(),
    sequence_name: zod_1.z.string(),
    version: zod_1.z.number().int().optional(),
    input: zod_1.z.unknown().optional(),
});
exports.ABVariantSchema = zod_1.z.object({
    name: zod_1.z.string(),
    weight: zod_1.z.number().int().nonnegative(),
    blocks: zod_1.z.array(zod_1.z.lazy(() => exports.BlockDefinitionSchema)),
});
exports.ABSplitBlockSchema = zod_1.z.object({
    type: zod_1.z.literal("ab_split"),
    id: zod_1.z.string(),
    variants: zod_1.z.array(exports.ABVariantSchema),
});
exports.CancellationScopeBlockSchema = zod_1.z.object({
    type: zod_1.z.literal("cancellation_scope"),
    id: zod_1.z.string(),
    blocks: zod_1.z.array(zod_1.z.lazy(() => exports.BlockDefinitionSchema)),
});
// ─── Sequence (top-level) ────────────────────────────────────────────────────
/** Payload shape expected by `POST /sequences`. */
exports.SequenceCreateSchema = zod_1.z.object({
    name: zod_1.z.string().min(1),
    namespace: zod_1.z.string().optional(),
    blocks: zod_1.z.array(exports.BlockDefinitionSchema),
});
