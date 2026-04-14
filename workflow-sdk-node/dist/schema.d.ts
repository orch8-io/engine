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
export declare const DurationMsSchema: z.ZodNumber;
export declare const DelaySpecSchema: z.ZodObject<{
    duration: z.ZodNumber;
    business_days_only: z.ZodOptional<z.ZodBoolean>;
    jitter: z.ZodOptional<z.ZodNumber>;
    holidays: z.ZodOptional<z.ZodArray<z.ZodString, "many">>;
}, "strip", z.ZodTypeAny, {
    duration: number;
    business_days_only?: boolean | undefined;
    jitter?: number | undefined;
    holidays?: string[] | undefined;
}, {
    duration: number;
    business_days_only?: boolean | undefined;
    jitter?: number | undefined;
    holidays?: string[] | undefined;
}>;
export type DelaySpec = z.infer<typeof DelaySpecSchema>;
export declare const SendWindowSchema: z.ZodObject<{
    start_hour: z.ZodOptional<z.ZodNumber>;
    end_hour: z.ZodOptional<z.ZodNumber>;
    days: z.ZodOptional<z.ZodArray<z.ZodNumber, "many">>;
}, "strip", z.ZodTypeAny, {
    start_hour?: number | undefined;
    end_hour?: number | undefined;
    days?: number[] | undefined;
}, {
    start_hour?: number | undefined;
    end_hour?: number | undefined;
    days?: number[] | undefined;
}>;
export type SendWindow = z.infer<typeof SendWindowSchema>;
export declare const ContextAccessSchema: z.ZodObject<{
    data: z.ZodOptional<z.ZodBoolean>;
    config: z.ZodOptional<z.ZodBoolean>;
    audit: z.ZodOptional<z.ZodBoolean>;
    runtime: z.ZodOptional<z.ZodBoolean>;
}, "strip", z.ZodTypeAny, {
    data?: boolean | undefined;
    config?: boolean | undefined;
    audit?: boolean | undefined;
    runtime?: boolean | undefined;
}, {
    data?: boolean | undefined;
    config?: boolean | undefined;
    audit?: boolean | undefined;
    runtime?: boolean | undefined;
}>;
export type ContextAccess = z.infer<typeof ContextAccessSchema>;
export declare const HumanInputDefSchema: z.ZodObject<{
    prompt: z.ZodOptional<z.ZodString>;
    timeout: z.ZodOptional<z.ZodNumber>;
    escalation_handler: z.ZodOptional<z.ZodString>;
}, "strip", z.ZodTypeAny, {
    prompt?: string | undefined;
    timeout?: number | undefined;
    escalation_handler?: string | undefined;
}, {
    prompt?: string | undefined;
    timeout?: number | undefined;
    escalation_handler?: string | undefined;
}>;
export type HumanInputDef = z.infer<typeof HumanInputDefSchema>;
export declare const EscalationDefSchema: z.ZodObject<{
    handler: z.ZodString;
    params: z.ZodOptional<z.ZodUnknown>;
}, "strip", z.ZodTypeAny, {
    handler: string;
    params?: unknown;
}, {
    handler: string;
    params?: unknown;
}>;
export type EscalationDef = z.infer<typeof EscalationDefSchema>;
export declare const RetryPolicySchema: z.ZodObject<{
    max_attempts: z.ZodNumber;
    initial_backoff: z.ZodNumber;
    max_backoff: z.ZodNumber;
    backoff_multiplier: z.ZodOptional<z.ZodNumber>;
}, "strip", z.ZodTypeAny, {
    max_attempts: number;
    initial_backoff: number;
    max_backoff: number;
    backoff_multiplier?: number | undefined;
}, {
    max_attempts: number;
    initial_backoff: number;
    max_backoff: number;
    backoff_multiplier?: number | undefined;
}>;
export type RetryPolicy = z.infer<typeof RetryPolicySchema>;
export declare const BlockDefinitionSchema: z.ZodType<BlockDefinition>;
export declare const StepBlockSchema: z.ZodObject<{
    type: z.ZodLiteral<"step">;
    id: z.ZodString;
    handler: z.ZodString;
    params: z.ZodOptional<z.ZodUnknown>;
    delay: z.ZodOptional<z.ZodNullable<z.ZodObject<{
        duration: z.ZodNumber;
        business_days_only: z.ZodOptional<z.ZodBoolean>;
        jitter: z.ZodOptional<z.ZodNumber>;
        holidays: z.ZodOptional<z.ZodArray<z.ZodString, "many">>;
    }, "strip", z.ZodTypeAny, {
        duration: number;
        business_days_only?: boolean | undefined;
        jitter?: number | undefined;
        holidays?: string[] | undefined;
    }, {
        duration: number;
        business_days_only?: boolean | undefined;
        jitter?: number | undefined;
        holidays?: string[] | undefined;
    }>>>;
    retry: z.ZodOptional<z.ZodNullable<z.ZodObject<{
        max_attempts: z.ZodNumber;
        initial_backoff: z.ZodNumber;
        max_backoff: z.ZodNumber;
        backoff_multiplier: z.ZodOptional<z.ZodNumber>;
    }, "strip", z.ZodTypeAny, {
        max_attempts: number;
        initial_backoff: number;
        max_backoff: number;
        backoff_multiplier?: number | undefined;
    }, {
        max_attempts: number;
        initial_backoff: number;
        max_backoff: number;
        backoff_multiplier?: number | undefined;
    }>>>;
    timeout: z.ZodOptional<z.ZodNumber>;
    rate_limit_key: z.ZodOptional<z.ZodString>;
    send_window: z.ZodOptional<z.ZodObject<{
        start_hour: z.ZodOptional<z.ZodNumber>;
        end_hour: z.ZodOptional<z.ZodNumber>;
        days: z.ZodOptional<z.ZodArray<z.ZodNumber, "many">>;
    }, "strip", z.ZodTypeAny, {
        start_hour?: number | undefined;
        end_hour?: number | undefined;
        days?: number[] | undefined;
    }, {
        start_hour?: number | undefined;
        end_hour?: number | undefined;
        days?: number[] | undefined;
    }>>;
    context_access: z.ZodOptional<z.ZodObject<{
        data: z.ZodOptional<z.ZodBoolean>;
        config: z.ZodOptional<z.ZodBoolean>;
        audit: z.ZodOptional<z.ZodBoolean>;
        runtime: z.ZodOptional<z.ZodBoolean>;
    }, "strip", z.ZodTypeAny, {
        data?: boolean | undefined;
        config?: boolean | undefined;
        audit?: boolean | undefined;
        runtime?: boolean | undefined;
    }, {
        data?: boolean | undefined;
        config?: boolean | undefined;
        audit?: boolean | undefined;
        runtime?: boolean | undefined;
    }>>;
    cancellable: z.ZodOptional<z.ZodBoolean>;
    wait_for_input: z.ZodOptional<z.ZodObject<{
        prompt: z.ZodOptional<z.ZodString>;
        timeout: z.ZodOptional<z.ZodNumber>;
        escalation_handler: z.ZodOptional<z.ZodString>;
    }, "strip", z.ZodTypeAny, {
        prompt?: string | undefined;
        timeout?: number | undefined;
        escalation_handler?: string | undefined;
    }, {
        prompt?: string | undefined;
        timeout?: number | undefined;
        escalation_handler?: string | undefined;
    }>>;
    queue_name: z.ZodOptional<z.ZodString>;
    deadline: z.ZodOptional<z.ZodNumber>;
    on_deadline_breach: z.ZodOptional<z.ZodObject<{
        handler: z.ZodString;
        params: z.ZodOptional<z.ZodUnknown>;
    }, "strip", z.ZodTypeAny, {
        handler: string;
        params?: unknown;
    }, {
        handler: string;
        params?: unknown;
    }>>;
}, "strip", z.ZodTypeAny, {
    type: "step";
    handler: string;
    id: string;
    params?: unknown;
    timeout?: number | undefined;
    delay?: {
        duration: number;
        business_days_only?: boolean | undefined;
        jitter?: number | undefined;
        holidays?: string[] | undefined;
    } | null | undefined;
    retry?: {
        max_attempts: number;
        initial_backoff: number;
        max_backoff: number;
        backoff_multiplier?: number | undefined;
    } | null | undefined;
    rate_limit_key?: string | undefined;
    send_window?: {
        start_hour?: number | undefined;
        end_hour?: number | undefined;
        days?: number[] | undefined;
    } | undefined;
    context_access?: {
        data?: boolean | undefined;
        config?: boolean | undefined;
        audit?: boolean | undefined;
        runtime?: boolean | undefined;
    } | undefined;
    cancellable?: boolean | undefined;
    wait_for_input?: {
        prompt?: string | undefined;
        timeout?: number | undefined;
        escalation_handler?: string | undefined;
    } | undefined;
    queue_name?: string | undefined;
    deadline?: number | undefined;
    on_deadline_breach?: {
        handler: string;
        params?: unknown;
    } | undefined;
}, {
    type: "step";
    handler: string;
    id: string;
    params?: unknown;
    timeout?: number | undefined;
    delay?: {
        duration: number;
        business_days_only?: boolean | undefined;
        jitter?: number | undefined;
        holidays?: string[] | undefined;
    } | null | undefined;
    retry?: {
        max_attempts: number;
        initial_backoff: number;
        max_backoff: number;
        backoff_multiplier?: number | undefined;
    } | null | undefined;
    rate_limit_key?: string | undefined;
    send_window?: {
        start_hour?: number | undefined;
        end_hour?: number | undefined;
        days?: number[] | undefined;
    } | undefined;
    context_access?: {
        data?: boolean | undefined;
        config?: boolean | undefined;
        audit?: boolean | undefined;
        runtime?: boolean | undefined;
    } | undefined;
    cancellable?: boolean | undefined;
    wait_for_input?: {
        prompt?: string | undefined;
        timeout?: number | undefined;
        escalation_handler?: string | undefined;
    } | undefined;
    queue_name?: string | undefined;
    deadline?: number | undefined;
    on_deadline_breach?: {
        handler: string;
        params?: unknown;
    } | undefined;
}>;
export type StepBlock = z.infer<typeof StepBlockSchema>;
export declare const ParallelBlockSchema: z.ZodObject<{
    type: z.ZodLiteral<"parallel">;
    id: z.ZodString;
    branches: z.ZodArray<z.ZodArray<z.ZodLazy<z.ZodType<BlockDefinition, z.ZodTypeDef, BlockDefinition>>, "many">, "many">;
}, "strip", z.ZodTypeAny, {
    type: "parallel";
    id: string;
    branches: BlockDefinition[][];
}, {
    type: "parallel";
    id: string;
    branches: BlockDefinition[][];
}>;
export type ParallelBlock = {
    type: "parallel";
    id: string;
    branches: BlockDefinition[][];
};
export declare const RaceSemanticsSchema: z.ZodEnum<["first_to_resolve", "first_to_succeed"]>;
export type RaceSemantics = z.infer<typeof RaceSemanticsSchema>;
export declare const RaceBlockSchema: z.ZodObject<{
    type: z.ZodLiteral<"race">;
    id: z.ZodString;
    branches: z.ZodArray<z.ZodArray<z.ZodLazy<z.ZodType<BlockDefinition, z.ZodTypeDef, BlockDefinition>>, "many">, "many">;
    semantics: z.ZodOptional<z.ZodEnum<["first_to_resolve", "first_to_succeed"]>>;
}, "strip", z.ZodTypeAny, {
    type: "race";
    id: string;
    branches: BlockDefinition[][];
    semantics?: "first_to_resolve" | "first_to_succeed" | undefined;
}, {
    type: "race";
    id: string;
    branches: BlockDefinition[][];
    semantics?: "first_to_resolve" | "first_to_succeed" | undefined;
}>;
export type RaceBlock = {
    type: "race";
    id: string;
    branches: BlockDefinition[][];
    semantics?: RaceSemantics;
};
export declare const LoopBlockSchema: z.ZodObject<{
    type: z.ZodLiteral<"loop">;
    id: z.ZodString;
    condition: z.ZodString;
    body: z.ZodArray<z.ZodLazy<z.ZodType<BlockDefinition, z.ZodTypeDef, BlockDefinition>>, "many">;
    max_iterations: z.ZodOptional<z.ZodNumber>;
}, "strip", z.ZodTypeAny, {
    type: "loop";
    id: string;
    condition: string;
    body: BlockDefinition[];
    max_iterations?: number | undefined;
}, {
    type: "loop";
    id: string;
    condition: string;
    body: BlockDefinition[];
    max_iterations?: number | undefined;
}>;
export type LoopBlock = {
    type: "loop";
    id: string;
    condition: string;
    body: BlockDefinition[];
    max_iterations?: number;
};
export declare const ForEachBlockSchema: z.ZodObject<{
    type: z.ZodLiteral<"for_each">;
    id: z.ZodString;
    collection: z.ZodString;
    item_var: z.ZodOptional<z.ZodString>;
    body: z.ZodArray<z.ZodLazy<z.ZodType<BlockDefinition, z.ZodTypeDef, BlockDefinition>>, "many">;
    max_iterations: z.ZodOptional<z.ZodNumber>;
}, "strip", z.ZodTypeAny, {
    type: "for_each";
    id: string;
    body: BlockDefinition[];
    collection: string;
    max_iterations?: number | undefined;
    item_var?: string | undefined;
}, {
    type: "for_each";
    id: string;
    body: BlockDefinition[];
    collection: string;
    max_iterations?: number | undefined;
    item_var?: string | undefined;
}>;
export type ForEachBlock = {
    type: "for_each";
    id: string;
    collection: string;
    item_var?: string;
    body: BlockDefinition[];
    max_iterations?: number;
};
export declare const RouteSchema: z.ZodObject<{
    condition: z.ZodString;
    blocks: z.ZodArray<z.ZodLazy<z.ZodType<BlockDefinition, z.ZodTypeDef, BlockDefinition>>, "many">;
}, "strip", z.ZodTypeAny, {
    condition: string;
    blocks: BlockDefinition[];
}, {
    condition: string;
    blocks: BlockDefinition[];
}>;
export type Route = {
    condition: string;
    blocks: BlockDefinition[];
};
export declare const RouterBlockSchema: z.ZodObject<{
    type: z.ZodLiteral<"router">;
    id: z.ZodString;
    routes: z.ZodArray<z.ZodObject<{
        condition: z.ZodString;
        blocks: z.ZodArray<z.ZodLazy<z.ZodType<BlockDefinition, z.ZodTypeDef, BlockDefinition>>, "many">;
    }, "strip", z.ZodTypeAny, {
        condition: string;
        blocks: BlockDefinition[];
    }, {
        condition: string;
        blocks: BlockDefinition[];
    }>, "many">;
    default: z.ZodOptional<z.ZodArray<z.ZodLazy<z.ZodType<BlockDefinition, z.ZodTypeDef, BlockDefinition>>, "many">>;
}, "strip", z.ZodTypeAny, {
    type: "router";
    id: string;
    routes: {
        condition: string;
        blocks: BlockDefinition[];
    }[];
    default?: BlockDefinition[] | undefined;
}, {
    type: "router";
    id: string;
    routes: {
        condition: string;
        blocks: BlockDefinition[];
    }[];
    default?: BlockDefinition[] | undefined;
}>;
export type RouterBlock = {
    type: "router";
    id: string;
    routes: Route[];
    default?: BlockDefinition[];
};
export declare const TryCatchBlockSchema: z.ZodObject<{
    type: z.ZodLiteral<"try_catch">;
    id: z.ZodString;
    try_block: z.ZodArray<z.ZodLazy<z.ZodType<BlockDefinition, z.ZodTypeDef, BlockDefinition>>, "many">;
    catch_block: z.ZodArray<z.ZodLazy<z.ZodType<BlockDefinition, z.ZodTypeDef, BlockDefinition>>, "many">;
    finally_block: z.ZodOptional<z.ZodArray<z.ZodLazy<z.ZodType<BlockDefinition, z.ZodTypeDef, BlockDefinition>>, "many">>;
}, "strip", z.ZodTypeAny, {
    type: "try_catch";
    id: string;
    try_block: BlockDefinition[];
    catch_block: BlockDefinition[];
    finally_block?: BlockDefinition[] | undefined;
}, {
    type: "try_catch";
    id: string;
    try_block: BlockDefinition[];
    catch_block: BlockDefinition[];
    finally_block?: BlockDefinition[] | undefined;
}>;
export type TryCatchBlock = {
    type: "try_catch";
    id: string;
    try_block: BlockDefinition[];
    catch_block: BlockDefinition[];
    finally_block?: BlockDefinition[];
};
export declare const SubSequenceBlockSchema: z.ZodObject<{
    type: z.ZodLiteral<"sub_sequence">;
    id: z.ZodString;
    sequence_name: z.ZodString;
    version: z.ZodOptional<z.ZodNumber>;
    input: z.ZodOptional<z.ZodUnknown>;
}, "strip", z.ZodTypeAny, {
    type: "sub_sequence";
    id: string;
    sequence_name: string;
    version?: number | undefined;
    input?: unknown;
}, {
    type: "sub_sequence";
    id: string;
    sequence_name: string;
    version?: number | undefined;
    input?: unknown;
}>;
export type SubSequenceBlock = z.infer<typeof SubSequenceBlockSchema>;
export declare const ABVariantSchema: z.ZodObject<{
    name: z.ZodString;
    weight: z.ZodNumber;
    blocks: z.ZodArray<z.ZodLazy<z.ZodType<BlockDefinition, z.ZodTypeDef, BlockDefinition>>, "many">;
}, "strip", z.ZodTypeAny, {
    blocks: BlockDefinition[];
    name: string;
    weight: number;
}, {
    blocks: BlockDefinition[];
    name: string;
    weight: number;
}>;
export type ABVariant = {
    name: string;
    weight: number;
    blocks: BlockDefinition[];
};
export declare const ABSplitBlockSchema: z.ZodObject<{
    type: z.ZodLiteral<"ab_split">;
    id: z.ZodString;
    variants: z.ZodArray<z.ZodObject<{
        name: z.ZodString;
        weight: z.ZodNumber;
        blocks: z.ZodArray<z.ZodLazy<z.ZodType<BlockDefinition, z.ZodTypeDef, BlockDefinition>>, "many">;
    }, "strip", z.ZodTypeAny, {
        blocks: BlockDefinition[];
        name: string;
        weight: number;
    }, {
        blocks: BlockDefinition[];
        name: string;
        weight: number;
    }>, "many">;
}, "strip", z.ZodTypeAny, {
    type: "ab_split";
    id: string;
    variants: {
        blocks: BlockDefinition[];
        name: string;
        weight: number;
    }[];
}, {
    type: "ab_split";
    id: string;
    variants: {
        blocks: BlockDefinition[];
        name: string;
        weight: number;
    }[];
}>;
export type ABSplitBlock = {
    type: "ab_split";
    id: string;
    variants: ABVariant[];
};
export declare const CancellationScopeBlockSchema: z.ZodObject<{
    type: z.ZodLiteral<"cancellation_scope">;
    id: z.ZodString;
    blocks: z.ZodArray<z.ZodLazy<z.ZodType<BlockDefinition, z.ZodTypeDef, BlockDefinition>>, "many">;
}, "strip", z.ZodTypeAny, {
    type: "cancellation_scope";
    id: string;
    blocks: BlockDefinition[];
}, {
    type: "cancellation_scope";
    id: string;
    blocks: BlockDefinition[];
}>;
export type CancellationScopeBlock = {
    type: "cancellation_scope";
    id: string;
    blocks: BlockDefinition[];
};
export type BlockDefinition = StepBlock | ParallelBlock | RaceBlock | LoopBlock | ForEachBlock | RouterBlock | TryCatchBlock | SubSequenceBlock | ABSplitBlock | CancellationScopeBlock;
/** Payload shape expected by `POST /sequences`. */
export declare const SequenceCreateSchema: z.ZodObject<{
    name: z.ZodString;
    namespace: z.ZodOptional<z.ZodString>;
    blocks: z.ZodArray<z.ZodType<BlockDefinition, z.ZodTypeDef, BlockDefinition>, "many">;
}, "strip", z.ZodTypeAny, {
    blocks: BlockDefinition[];
    name: string;
    namespace?: string | undefined;
}, {
    blocks: BlockDefinition[];
    name: string;
    namespace?: string | undefined;
}>;
export type SequenceCreate = z.infer<typeof SequenceCreateSchema>;
