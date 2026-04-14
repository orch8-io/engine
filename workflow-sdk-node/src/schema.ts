/**
 * Zod schemas mirroring the orch8 workflow DSL.
 *
 * These mirror the Rust types in `orch8-types/src/sequence.rs` which are
 * serialized with `#[serde(tag = "type", rename_all = "snake_case")]`.
 *
 * Durations are in milliseconds (u64). Do not convert — the engine expects
 * integer millis on the wire (see `crate::serde_duration` in Rust side).
 */
import { z } from "zod";

// ─── Primitives ──────────────────────────────────────────────────────────────

export const DurationMsSchema = z.number().int().nonnegative();

export const DelaySpecSchema = z.object({
  duration: DurationMsSchema,
  business_days_only: z.boolean().optional(),
  jitter: DurationMsSchema.optional(),
  holidays: z.array(z.string()).optional(),
});
export type DelaySpec = z.infer<typeof DelaySpecSchema>;

export const SendWindowSchema = z.object({
  start_hour: z.number().int().min(0).max(23).optional(),
  end_hour: z.number().int().min(0).max(23).optional(),
  days: z.array(z.number().int().min(0).max(6)).optional(),
});
export type SendWindow = z.infer<typeof SendWindowSchema>;

export const ContextAccessSchema = z.object({
  data: z.boolean().optional(),
  config: z.boolean().optional(),
  audit: z.boolean().optional(),
  runtime: z.boolean().optional(),
});
export type ContextAccess = z.infer<typeof ContextAccessSchema>;

export const HumanInputDefSchema = z.object({
  prompt: z.string().optional(),
  timeout: DurationMsSchema.optional(),
  escalation_handler: z.string().optional(),
});
export type HumanInputDef = z.infer<typeof HumanInputDefSchema>;

export const EscalationDefSchema = z.object({
  handler: z.string(),
  params: z.unknown().optional(),
});
export type EscalationDef = z.infer<typeof EscalationDefSchema>;

export const RetryPolicySchema = z.object({
  max_attempts: z.number().int().positive(),
  initial_backoff: DurationMsSchema,
  max_backoff: DurationMsSchema,
  backoff_multiplier: z.number().positive().optional(),
});
export type RetryPolicy = z.infer<typeof RetryPolicySchema>;

// ─── Block definitions (recursive) ───────────────────────────────────────────
// We declare the forward reference up-front so nested schemas can cite it.
// eslint-disable-next-line @typescript-eslint/no-explicit-any
export const BlockDefinitionSchema: z.ZodType<BlockDefinition> = z.lazy(() =>
  z.discriminatedUnion("type", [
    StepBlockSchema,
    ParallelBlockSchema,
    RaceBlockSchema,
    LoopBlockSchema,
    ForEachBlockSchema,
    RouterBlockSchema,
    TryCatchBlockSchema,
    SubSequenceBlockSchema,
    ABSplitBlockSchema,
    CancellationScopeBlockSchema,
  ]),
);

export const StepBlockSchema = z.object({
  type: z.literal("step"),
  id: z.string(),
  handler: z.string(),
  params: z.unknown().optional(),
  delay: DelaySpecSchema.nullable().optional(),
  retry: RetryPolicySchema.nullable().optional(),
  timeout: DurationMsSchema.optional(),
  rate_limit_key: z.string().optional(),
  send_window: SendWindowSchema.optional(),
  context_access: ContextAccessSchema.optional(),
  cancellable: z.boolean().optional(),
  wait_for_input: HumanInputDefSchema.optional(),
  queue_name: z.string().optional(),
  deadline: DurationMsSchema.optional(),
  on_deadline_breach: EscalationDefSchema.optional(),
});
export type StepBlock = z.infer<typeof StepBlockSchema>;

export const ParallelBlockSchema = z.object({
  type: z.literal("parallel"),
  id: z.string(),
  branches: z.array(z.array(z.lazy(() => BlockDefinitionSchema))),
});
export type ParallelBlock = { type: "parallel"; id: string; branches: BlockDefinition[][] };

export const RaceSemanticsSchema = z.enum(["first_to_resolve", "first_to_succeed"]);
export type RaceSemantics = z.infer<typeof RaceSemanticsSchema>;

export const RaceBlockSchema = z.object({
  type: z.literal("race"),
  id: z.string(),
  branches: z.array(z.array(z.lazy(() => BlockDefinitionSchema))),
  semantics: RaceSemanticsSchema.optional(),
});
export type RaceBlock = {
  type: "race";
  id: string;
  branches: BlockDefinition[][];
  semantics?: RaceSemantics;
};

export const LoopBlockSchema = z.object({
  type: z.literal("loop"),
  id: z.string(),
  condition: z.string(),
  body: z.array(z.lazy(() => BlockDefinitionSchema)),
  max_iterations: z.number().int().positive().optional(),
});
export type LoopBlock = {
  type: "loop";
  id: string;
  condition: string;
  body: BlockDefinition[];
  max_iterations?: number;
};

export const ForEachBlockSchema = z.object({
  type: z.literal("for_each"),
  id: z.string(),
  collection: z.string(),
  item_var: z.string().optional(),
  body: z.array(z.lazy(() => BlockDefinitionSchema)),
  max_iterations: z.number().int().positive().optional(),
});
export type ForEachBlock = {
  type: "for_each";
  id: string;
  collection: string;
  item_var?: string;
  body: BlockDefinition[];
  max_iterations?: number;
};

export const RouteSchema = z.object({
  condition: z.string(),
  blocks: z.array(z.lazy(() => BlockDefinitionSchema)),
});
export type Route = { condition: string; blocks: BlockDefinition[] };

export const RouterBlockSchema = z.object({
  type: z.literal("router"),
  id: z.string(),
  routes: z.array(RouteSchema),
  default: z.array(z.lazy(() => BlockDefinitionSchema)).optional(),
});
export type RouterBlock = {
  type: "router";
  id: string;
  routes: Route[];
  default?: BlockDefinition[];
};

export const TryCatchBlockSchema = z.object({
  type: z.literal("try_catch"),
  id: z.string(),
  try_block: z.array(z.lazy(() => BlockDefinitionSchema)),
  catch_block: z.array(z.lazy(() => BlockDefinitionSchema)),
  finally_block: z.array(z.lazy(() => BlockDefinitionSchema)).optional(),
});
export type TryCatchBlock = {
  type: "try_catch";
  id: string;
  try_block: BlockDefinition[];
  catch_block: BlockDefinition[];
  finally_block?: BlockDefinition[];
};

export const SubSequenceBlockSchema = z.object({
  type: z.literal("sub_sequence"),
  id: z.string(),
  sequence_name: z.string(),
  version: z.number().int().optional(),
  input: z.unknown().optional(),
});
export type SubSequenceBlock = z.infer<typeof SubSequenceBlockSchema>;

export const ABVariantSchema = z.object({
  name: z.string(),
  weight: z.number().int().nonnegative(),
  blocks: z.array(z.lazy(() => BlockDefinitionSchema)),
});
export type ABVariant = { name: string; weight: number; blocks: BlockDefinition[] };

export const ABSplitBlockSchema = z.object({
  type: z.literal("ab_split"),
  id: z.string(),
  variants: z.array(ABVariantSchema),
});
export type ABSplitBlock = { type: "ab_split"; id: string; variants: ABVariant[] };

export const CancellationScopeBlockSchema = z.object({
  type: z.literal("cancellation_scope"),
  id: z.string(),
  blocks: z.array(z.lazy(() => BlockDefinitionSchema)),
});
export type CancellationScopeBlock = {
  type: "cancellation_scope";
  id: string;
  blocks: BlockDefinition[];
};

export type BlockDefinition =
  | StepBlock
  | ParallelBlock
  | RaceBlock
  | LoopBlock
  | ForEachBlock
  | RouterBlock
  | TryCatchBlock
  | SubSequenceBlock
  | ABSplitBlock
  | CancellationScopeBlock;

// ─── Sequence (top-level) ────────────────────────────────────────────────────

/** Payload shape expected by `POST /sequences`. */
export const SequenceCreateSchema = z.object({
  name: z.string().min(1),
  namespace: z.string().optional(),
  blocks: z.array(BlockDefinitionSchema),
});
export type SequenceCreate = z.infer<typeof SequenceCreateSchema>;
