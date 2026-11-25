import type { InstanceState, TaskInstance } from "../api";

interface InstancePage {
  items: TaskInstance[];
  has_more: boolean;
}

const states = new Set<InstanceState>(["scheduled", "running", "waiting", "paused", "completed", "failed", "cancelled"]);
const priorities = new Set<unknown>(["Low", "Normal", "High", "Critical", 0, 1, 2, 3]);
const requiredStrings = ["id", "sequence_id", "tenant_id", "namespace", "timezone", "created_at", "updated_at"];
const optionalStrings = ["concurrency_key", "idempotency_key", "session_id", "parent_instance_id"];

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

function isTaskInstance(value: unknown): value is TaskInstance {
  return isRecord(value)
    && requiredStrings.every((key) => typeof value[key] === "string")
    && optionalStrings.every((key) => value[key] === undefined || typeof value[key] === "string")
    && (value.max_concurrency === undefined || Number.isInteger(value.max_concurrency))
    && states.has(value.state as InstanceState)
    && (value.next_fire_at === null || typeof value.next_fire_at === "string")
    && priorities.has(value.priority)
    && "metadata" in value
    && isRecord(value.context);
}

export function decodeInstancePage(raw: unknown): InstancePage {
  // Existing engines may return the list directly.
  const page: unknown = Array.isArray(raw) ? { items: raw, has_more: false } : raw;
  if (!isRecord(page) || !Array.isArray(page.items) || typeof page.has_more !== "boolean") {
    throw new Error("Engine returned an invalid execution page.");
  }
  const items = page.items.map((item: unknown, index) => {
    if (!isTaskInstance(item)) throw new Error(`Engine returned an invalid execution at index ${index}.`);
    return item;
  });
  return { items, has_more: page.has_more };
}
