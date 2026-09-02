const DEFAULT_API_URL = import.meta.env.VITE_ORCH8_API_URL || "http://localhost:8080";

function getApiUrl(): string {
  return localStorage.getItem("orch8_api_url") || DEFAULT_API_URL;
}

function getApiKey(): string | null {
  return localStorage.getItem("orch8_api_key");
}

function getTenantId(): string | null {
  return localStorage.getItem("orch8_tenant_id");
}

export function setApiUrl(url: string) {
  localStorage.setItem("orch8_api_url", url);
}

export function setApiKey(key: string) {
  localStorage.setItem("orch8_api_key", key);
}

export function clearApiKey() {
  localStorage.removeItem("orch8_api_key");
}

export function setTenantId(tenantId: string) {
  localStorage.setItem("orch8_tenant_id", tenantId);
}

export function clearTenantId() {
  localStorage.removeItem("orch8_tenant_id");
}

async function request<T>(
  path: string,
  params?: Record<string, string | undefined>,
  signal?: AbortSignal,
): Promise<T> {
  const url = new URL(path, getApiUrl());
  if (params) {
    for (const [k, v] of Object.entries(params)) {
      if (v) url.searchParams.set(k, v);
    }
  }

  const headers: Record<string, string> = { "Content-Type": "application/json" };
  const apiKey = getApiKey();
  if (apiKey) headers["X-API-Key"] = apiKey;
  const tenantId = getTenantId();
  if (tenantId) headers["X-Tenant-Id"] = tenantId;

  const res = await fetch(url.toString(), { headers, signal });

  if (res.status === 401) {
    throw new AuthError();
  }
  if (!res.ok) {
    throw new ApiRequestError(res.status, await res.text());
  }
  const text = await res.text();
  return (text ? JSON.parse(text) : null) as T;
}

export class AuthError extends Error {
  constructor() {
    super("Authentication failed. Check the API key and tenant ID in Settings.");
    this.name = "AuthError";
  }
}

export class ApiRequestError extends Error {
  status: number;
  body: string;
  constructor(status: number, body: string) {
    let detail = body.trim();
    try {
      const parsed = JSON.parse(body) as { error?: unknown };
      if (typeof parsed.error === "string") detail = parsed.error;
    } catch {
      // Plain-text errors are already suitable for the operator message.
    }
    super(detail ? `Request failed (${status}): ${detail}` : `Request failed with status ${status}.`);
    this.name = "ApiRequestError";
    this.status = status;
    this.body = body;
  }
}

export type NodeStatus = "active" | "draining" | "stopped";
export type Priority = 0 | 1 | 2 | 3;

export interface WorkerTask {
  id: string;
  instance_id: string;
  block_id: string;
  handler_name: string;
  queue_name: string | null;
  params: Record<string, unknown>;
  context: Record<string, unknown>;
  attempt: number;
  timeout_ms: number | null;
  state: "pending" | "claimed" | "completed" | "failed";
  worker_id: string | null;
  claimed_at: string | null;
  heartbeat_at: string | null;
  completed_at: string | null;
  output: Record<string, unknown> | null;
  error_message: string | null;
  error_retryable: boolean | null;
  created_at: string;
}

export type WorkerTaskState = WorkerTask["state"];

export interface WorkerTaskStats {
  by_state: Record<string, number>;
  by_handler: Record<string, Record<string, number>>;
  active_workers: string[];
}

export interface ListTasksParams {
  tenant_id?: string;
  state?: string;
  handler_name?: string;
  worker_id?: string;
  queue_name?: string;
  limit?: string;
  offset?: string;
}

export function listWorkerTasks(
  params?: ListTasksParams,
  signal?: AbortSignal,
): Promise<WorkerTask[]> {
  return request("/workers/tasks", params as Record<string, string | undefined>, signal);
}

export function getWorkerTaskStats(signal?: AbortSignal): Promise<WorkerTaskStats> {
  return request("/workers/tasks/stats", undefined, signal);
}

/** Aggregated fleet view of one worker, grouped from its per-handler registrations. */
export interface WorkerInfo {
  worker_id: string;
  handlers: string[];
  queues: string[];
  version: string | null;
  last_seen_at: string;
  alive: boolean;
  in_flight: number;
}

export function listWorkers(
  params?: { alive_within_secs?: string; include_stale?: string },
  signal?: AbortSignal,
): Promise<WorkerInfo[]> {
  return request("/workers", params, signal);
}

export interface HandlerCatalog {
  builtin: string[];
  external: string[];
}

export function listHandlers(signal?: AbortSignal): Promise<HandlerCatalog> {
  return request("/handlers", undefined, signal);
}

export type WorkerCommandKind = "drain" | "reload" | "ping";

export interface WorkerCommand {
  id: string;
  worker_id: string;
  command: WorkerCommandKind;
  payload: unknown;
  created_at: string;
}

export function enqueueWorkerCommand(
  worker_id: string,
  command: WorkerCommandKind,
  payload?: unknown,
  signal?: AbortSignal,
): Promise<WorkerCommand> {
  return mutate("/workers/commands", "POST", { worker_id, command, payload: payload ?? {} }, undefined, signal);
}

export function listWorkerCommands(worker_id: string, signal?: AbortSignal): Promise<WorkerCommand[]> {
  return request(`/workers/${encodeURIComponent(worker_id)}/commands`, undefined, signal);
}

export function checkHealth(signal?: AbortSignal): Promise<void> {
  return request("/health/live", undefined, signal);
}

/** Engine version + optional operator-set environment label/color. */
export interface EngineInfo {
  version: string;
  env_label: string | null;
  env_color: string | null;
}

export function getEngineInfo(signal?: AbortSignal): Promise<EngineInfo> {
  return request("/info", undefined, signal);
}

// ─── Instances ───────────────────────────────────────────────────────────────

export type InstanceState =
  | "scheduled"
  | "running"
  | "waiting"
  | "paused"
  | "completed"
  | "failed"
  | "cancelled";

export const TERMINAL_STATES: InstanceState[] = ["completed", "failed", "cancelled"];

export type NodeState =
  | "pending"
  | "running"
  | "waiting"
  | "completed"
  | "failed"
  | "cancelled"
  | "skipped";

export type BlockType =
  | "step"
  | "parallel"
  | "race"
  | "loop"
  | "for_each"
  | "router"
  | "try_catch"
  | "sub_sequence"
  | "a_b_split"
  | "cancellation_scope";

export interface TaskInstance {
  id: string;
  sequence_id: string;
  tenant_id: string;
  namespace: string;
  state: InstanceState;
  next_fire_at: string | null;
  priority: Priority;
  timezone: string;
  metadata: Record<string, unknown>;
  context: Record<string, unknown>;
  concurrency_key?: string;
  max_concurrency?: number;
  idempotency_key?: string;
  session_id?: string;
  parent_instance_id?: string;
  created_at: string;
  updated_at: string;
}

export interface ExecutionNode {
  id: string;
  instance_id: string;
  block_id: string;
  parent_id: string | null;
  block_type: BlockType;
  branch_index: number | null;
  state: NodeState;
  started_at: string | null;
  completed_at: string | null;
}

export interface BlockOutput {
  instance_id: string;
  block_id: string;
  output: unknown;
  created_at: string;
}

export interface ListInstancesParams {
  tenant_id?: string;
  namespace?: string;
  sequence_id?: string;
  state?: string;
  limit?: string;
  offset?: string;
  /** Top-level metadata equality filters; sent as `metadata.<key>=<value>` query params. */
  metadata?: Record<string, string>;
}

export async function listInstances(
  params?: ListInstancesParams,
  signal?: AbortSignal,
): Promise<TaskInstance[]> {
  const { metadata, ...rest } = params ?? {};
  const query: Record<string, string | undefined> = { ...rest };
  if (metadata) {
    for (const [k, v] of Object.entries(metadata)) {
      if (v) query[`metadata.${k}`] = v;
    }
  }
  const raw = await request<TaskInstance[] | { items: TaskInstance[] }>("/instances", query, signal);
  return Array.isArray(raw) ? raw : raw.items;
}

export function getInstance(id: string, signal?: AbortSignal): Promise<TaskInstance> {
  return request(`/instances/${encodeURIComponent(id)}`, undefined, signal);
}

export function getInstanceChildren(id: string, signal?: AbortSignal): Promise<TaskInstance[]> {
  return request(`/instances/${encodeURIComponent(id)}/children`, undefined, signal);
}

export interface StepLog {
  block_id: string;
  ts: string;
  level: "trace" | "debug" | "info" | "warn" | "error" | string;
  message: string;
}

export function getInstanceLogs(id: string, signal?: AbortSignal): Promise<StepLog[]> {
  return request(`/instances/${encodeURIComponent(id)}/logs`, undefined, signal);
}

export function getExecutionTree(id: string, signal?: AbortSignal): Promise<ExecutionNode[]> {
  return request(`/instances/${encodeURIComponent(id)}/tree`, undefined, signal);
}

export function getInstanceOutputs(id: string, signal?: AbortSignal): Promise<BlockOutput[]> {
  return request(`/instances/${encodeURIComponent(id)}/outputs`, undefined, signal);
}

export type SignalType = "pause" | "resume" | "cancel" | "update_context" | { Custom: string };

export async function sendSignal(
  instanceId: string,
  signal_type: SignalType,
  payload: unknown = {},
  signal?: AbortSignal,
): Promise<{ signal_id: string }> {
  return mutate(
    `/instances/${encodeURIComponent(instanceId)}/signals`,
    "POST",
    { signal_type, payload },
    undefined,
    signal,
  );
}

export async function retryInstance(
  id: string,
  signal?: AbortSignal,
): Promise<{ id: string; state: string }> {
  return mutate(`/instances/${encodeURIComponent(id)}/retry`, "POST", undefined, undefined, signal);
}

export type BatchActionKind = "retry" | "pause" | "resume" | "cancel" | "signal";

export interface BatchActionBody {
  filter: {
    tenant_id?: string;
    namespace?: string;
    sequence_id?: string;
    states?: string[];
    metadata?: Record<string, string>;
  };
  action: BatchActionKind;
  signal_type?: string;
  payload?: unknown;
  dry_run?: boolean;
  limit?: number;
}

export interface BatchActionResult {
  matched: number;
  applied: number;
  skipped: number;
  failed: number;
  dry_run: boolean;
}

export function batchAction(
  body: BatchActionBody,
  signal?: AbortSignal,
): Promise<BatchActionResult> {
  return mutate("/instances/batch-action", "POST", body, undefined, signal);
}

export interface CreateInstanceBody {
  sequence_id: string;
  tenant_id: string;
  namespace: string;
  context: { data: Record<string, unknown>; config: Record<string, unknown>; audit: unknown[] };
  dry_run?: boolean;
}

export async function createInstance(
  body: CreateInstanceBody,
  signal?: AbortSignal,
): Promise<{ id: string; deduplicated?: boolean }> {
  return mutate("/instances", "POST", body, undefined, signal);
}

// ─── Sequences ───────────────────────────────────────────────────────────────

export interface SequenceDefinition {
  id: string;
  tenant_id: string;
  namespace: string;
  name: string;
  version: number;
  deprecated: boolean;
  blocks: unknown[];
  interceptors?: unknown;
  /** Optional JSON Schema describing `context.data`. When set, instance creates are validated against it. */
  input_schema?: Record<string, unknown> | null;
  created_at: string;
}

export function getSequence(id: string, signal?: AbortSignal): Promise<SequenceDefinition> {
  return request(`/sequences/${encodeURIComponent(id)}`, undefined, signal);
}

export function listSequenceVersions(
  params: { tenant_id: string; namespace: string; name: string },
  signal?: AbortSignal,
): Promise<SequenceDefinition[]> {
  return request("/sequences/versions", params, signal);
}

export interface ListSequencesParams {
  tenant_id?: string;
  namespace?: string;
  limit?: string;
  offset?: string;
}

export async function listSequences(
  params?: ListSequencesParams,
  signal?: AbortSignal,
): Promise<SequenceDefinition[]> {
  const raw = await request<SequenceDefinition[] | { items: SequenceDefinition[] }>("/sequences", params as Record<string, string | undefined>, signal);
  return Array.isArray(raw) ? raw : raw.items;
}

export interface CreateSequenceResponse {
  id: string;
  warnings?: string[];
}

/**
 * Register a new sequence definition. The body is the full sequence JSON
 * (the server validates structure and returns lint warnings, if any).
 */
export function createSequence(
  body: Record<string, unknown>,
  signal?: AbortSignal,
): Promise<CreateSequenceResponse> {
  return mutate("/sequences", "POST", body, undefined, signal);
}

export function deprecateSequence(
  id: string,
  signal?: AbortSignal,
): Promise<null> {
  return mutate(`/sequences/${encodeURIComponent(id)}/deprecate`, "POST", undefined, undefined, signal);
}

export function deleteSequence(
  id: string,
  signal?: AbortSignal,
): Promise<null> {
  return mutate(`/sequences/${encodeURIComponent(id)}`, "DELETE", undefined, undefined, signal);
}

export function setSequenceStatus(
  id: string,
  status: "active" | "deprecated" | "archived",
  signal?: AbortSignal,
): Promise<null> {
  return mutate(`/sequences/${encodeURIComponent(id)}/status`, "PUT", { status }, undefined, signal);
}

export type ReleaseState =
  | "draft" | "validating" | "ready" | "canary"
  | "promoted" | "paused" | "rolled_back" | "failed";

export interface ReleaseGate {
  metric: "error_rate" | "cancel_rate";
  max_regression: number;
  min_sample: number;
}

export interface WorkflowRelease {
  id: string;
  tenant_id: string;
  namespace: string;
  sequence_name: string;
  baseline_sequence_id: string;
  baseline_version: number;
  candidate_sequence_id: string;
  candidate_version: number;
  state: ReleaseState;
  canary_percent: number;
  gates: ReleaseGate[];
  in_flight_policy: "pin" | "operator_decision";
  validation_summary?: Record<string, unknown> | null;
  canary_started_at?: string | null;
  created_at: string;
  updated_at: string;
}

export interface ReleaseDecision {
  id: string;
  release_id: string;
  from_state: ReleaseState;
  to_state: ReleaseState;
  actor: string;
  reason: string;
  decided_at: string;
}

export interface SemanticDiffEntry {
  category: string;
  severity: "informational" | "behavioral" | "side_effect_risk" | "incompatible";
  block_id?: string | null;
  summary: string;
}

export interface SemanticDiff {
  entries: SemanticDiffEntry[];
  max_severity?: SemanticDiffEntry["severity"] | null;
  candidate_lint: string[];
}

export function listReleases(params?: { tenant_id?: string; limit?: string }, signal?: AbortSignal): Promise<WorkflowRelease[]> {
  return request("/releases", params, signal);
}

export function createRelease(body: {
  tenant_id: string;
  baseline_sequence_id: string;
  candidate_sequence_id: string;
  gates: ReleaseGate[];
  in_flight_policy?: "pin" | "operator_decision";
}, signal?: AbortSignal): Promise<WorkflowRelease> {
  return mutate("/releases", "POST", body, undefined, signal);
}

export function getReleaseDiff(id: string, signal?: AbortSignal): Promise<SemanticDiff> {
  return request(`/releases/${encodeURIComponent(id)}/diff`, undefined, signal);
}

export function listReleaseDecisions(id: string, signal?: AbortSignal): Promise<ReleaseDecision[]> {
  return request(`/releases/${encodeURIComponent(id)}/decisions`, undefined, signal);
}

export function validateRelease(id: string, sample = 10, signal?: AbortSignal) {
  return mutate(`/releases/${encodeURIComponent(id)}/validate`, "POST", { sample, skip: false }, undefined, signal);
}

export function startReleaseCanary(id: string, percent: number, signal?: AbortSignal): Promise<WorkflowRelease> {
  return mutate(`/releases/${encodeURIComponent(id)}/canary`, "POST", { percent }, undefined, signal);
}

export function evaluateRelease(id: string, signal?: AbortSignal) {
  return mutate(`/releases/${encodeURIComponent(id)}/evaluate`, "POST", {}, undefined, signal);
}

export function promoteRelease(id: string, force = false, signal?: AbortSignal): Promise<WorkflowRelease> {
  return mutate(`/releases/${encodeURIComponent(id)}/promote`, "POST", { force }, undefined, signal);
}

export function pauseRelease(id: string, signal?: AbortSignal): Promise<WorkflowRelease> {
  return mutate(`/releases/${encodeURIComponent(id)}/pause`, "POST", {}, undefined, signal);
}

export function rollbackRelease(id: string, signal?: AbortSignal): Promise<WorkflowRelease> {
  return mutate(`/releases/${encodeURIComponent(id)}/rollback`, "POST", {}, undefined, signal);
}

// ─── Version Pins ───────────────────────────────────────────────────────────

export interface VersionPin {
  tenant_id: string;
  handler_name: string;
  min_version: number;
  created_at: string;
}

export function listVersionPins(signal?: AbortSignal): Promise<VersionPin[]> {
  return request("/version-pins", undefined, signal);
}

export function setVersionPin(
  body: { tenant_id: string; handler_name: string; min_version: number },
  signal?: AbortSignal,
): Promise<VersionPin> {
  return mutate("/version-pins", "PUT", body, undefined, signal);
}

export function deleteVersionPin(
  tenantId: string,
  handlerName: string,
  signal?: AbortSignal,
): Promise<null> {
  return mutate(
    `/version-pins/${encodeURIComponent(tenantId)}/${encodeURIComponent(handlerName)}`,
    "DELETE",
    undefined,
    undefined,
    signal,
  );
}

// ─── Usage / cost ────────────────────────────────────────────────────────────

export interface UsageEntry {
  kind: string;
  model: string;
  events: number;
  input_tokens: number;
  output_tokens: number;
  /** Estimated list-price cost in USD; null when the model has no pricing-table entry. */
  cost_usd: number | null;
}

export interface UsageResponse {
  tenant: string;
  start: string;
  end: string;
  usage: UsageEntry[];
  /** Window-wide total over the known-model entries. */
  total_cost_usd: number;
  /** Always true — costs are computed from static list prices, not invoices. */
  cost_is_estimate: boolean;
}

export interface GetUsageParams {
  /** Honored only for unscoped/admin callers; scoped keys are locked to their tenant. */
  tenant?: string;
  /** Window start (RFC 3339). Defaults server-side to 30 days before `end`. */
  start?: string;
  /** Window end (RFC 3339). Defaults server-side to now. */
  end?: string;
}

export function getUsage(params?: GetUsageParams, signal?: AbortSignal): Promise<UsageResponse> {
  return request("/usage", params as Record<string, string | undefined>, signal);
}

// ─── Operations: DLQ / circuit breakers / cluster ────────────────────────────

export interface ListDlqParams {
  tenant_id?: string;
  namespace?: string;
  offset?: string;
  limit?: string;
}

export function listDlq(
  params?: ListDlqParams,
  signal?: AbortSignal,
): Promise<TaskInstance[]> {
  return request("/instances/dlq", params as Record<string, string | undefined>, signal);
}

export interface WebhookOutboxEntry {
  id: string;
  url: string;
  event_type: string;
  instance_id?: string | null;
  payload: unknown;
  attempts: number;
  last_error?: string | null;
  created_at: string;
}

export function listWebhookOutbox(limit = 100, signal?: AbortSignal): Promise<WebhookOutboxEntry[]> {
  return request("/webhooks/outbox", { limit: String(limit) }, signal);
}

export function redeliverWebhook(
  id: string,
  signal?: AbortSignal,
): Promise<{ redelivered: boolean }> {
  return mutate(`/webhooks/outbox/${encodeURIComponent(id)}/redeliver`, "POST", undefined, undefined, signal);
}

export function discardWebhook(id: string, signal?: AbortSignal): Promise<null> {
  return mutate(`/webhooks/outbox/${encodeURIComponent(id)}`, "DELETE", undefined, undefined, signal);
}

export type BreakerState = "closed" | "open" | "half_open";

export interface CircuitBreakerState {
  tenant_id: string;
  handler: string;
  state: BreakerState;
  failure_count: number;
  failure_threshold: number;
  cooldown_secs: number;
  opened_at?: string | null;
  [k: string]: unknown;
}

export function listCircuitBreakers(signal?: AbortSignal): Promise<CircuitBreakerState[]> {
  return request("/circuit-breakers", undefined, signal);
}

export function resetCircuitBreaker(
  tenantId: string,
  handler: string,
  signal?: AbortSignal,
): Promise<null> {
  return mutate(
    `/tenants/${encodeURIComponent(tenantId)}/circuit-breakers/${encodeURIComponent(handler)}/reset`,
    "POST",
    undefined,
    undefined,
    signal,
  );
}

export interface ClusterNode {
  id: string;
  name: string;
  status: NodeStatus;
  registered_at: string;
  last_heartbeat_at: string;
  drain: boolean;
  [k: string]: unknown;
}

export function listClusterNodes(signal?: AbortSignal): Promise<ClusterNode[]> {
  return request("/cluster/nodes", undefined, signal);
}

export function drainClusterNode(id: string, signal?: AbortSignal): Promise<null> {
  return mutate(
    `/cluster/nodes/${encodeURIComponent(id)}/drain`,
    "POST",
    undefined,
    undefined,
    signal,
  );
}

// ─── Cron schedules ──────────────────────────────────────────────────────────

export type OverlapPolicy = "allow" | "skip" | "buffer_one" | "cancel_previous";

export interface CronSchedule {
  id: string;
  tenant_id: string;
  namespace: string;
  sequence_id: string;
  cron_expr: string;
  timezone: string;
  enabled: boolean;
  metadata: Record<string, unknown>;
  overlap_policy: OverlapPolicy;
  skipped_fires: number;
  last_skipped_at: string | null;
  last_triggered_at: string | null;
  next_fire_at: string | null;
  created_at: string;
  updated_at: string;
}

export interface NextFiresResponse {
  timezone: string;
  fires: string[];
}

export interface CreateCronRequest {
  tenant_id: string;
  namespace: string;
  sequence_id: string;
  cron_expr: string;
  timezone?: string;
  metadata?: Record<string, unknown>;
  enabled?: boolean;
  overlap_policy?: OverlapPolicy;
}

export interface UpdateCronRequest {
  cron_expr?: string;
  timezone?: string;
  enabled?: boolean;
  metadata?: Record<string, unknown>;
  overlap_policy?: OverlapPolicy;
}

export function listCronSchedules(
  tenant_id?: string,
  signal?: AbortSignal,
): Promise<CronSchedule[]> {
  return request("/cron", { tenant_id }, signal);
}

export function getCronSchedule(id: string, signal?: AbortSignal): Promise<CronSchedule> {
  return request(`/cron/${encodeURIComponent(id)}`, undefined, signal);
}

export function createCronSchedule(
  body: CreateCronRequest,
  signal?: AbortSignal,
): Promise<{ id: string; next_fire_at: string | null }> {
  return mutate("/cron", "POST", body, undefined, signal);
}

export function updateCronSchedule(
  id: string,
  body: UpdateCronRequest,
  signal?: AbortSignal,
): Promise<CronSchedule> {
  return mutate(`/cron/${encodeURIComponent(id)}`, "PUT", body, undefined, signal);
}

export function deleteCronSchedule(id: string, signal?: AbortSignal): Promise<null> {
  return mutate(`/cron/${encodeURIComponent(id)}`, "DELETE", undefined, undefined, signal);
}

export function cronNextFires(
  id: string,
  n = 5,
  signal?: AbortSignal,
): Promise<NextFiresResponse> {
  return request(`/cron/${encodeURIComponent(id)}/next-fires`, { n: String(n) }, signal);
}

// ─── Triggers ────────────────────────────────────────────────────────────────

export type TriggerType =
  | "webhook"
  | "nats"
  | "file_watch"
  | "event"
  | "activepieces_poll";

export interface TriggerDef {
  slug: string;
  sequence_name: string;
  version: number | null;
  tenant_id: string;
  namespace: string;
  enabled: boolean;
  secret: string | null;
  trigger_type: TriggerType;
  config: Record<string, unknown>;
  created_at: string;
  updated_at: string;
}

export interface CreateTriggerRequest {
  slug: string;
  sequence_name: string;
  version?: number;
  tenant_id: string;
  namespace?: string;
  secret?: string;
  trigger_type?: TriggerType;
  config?: Record<string, unknown>;
}

export function listTriggers(
  tenant_id?: string,
  signal?: AbortSignal,
): Promise<TriggerDef[]> {
  return request("/triggers", { tenant_id }, signal);
}

export function getTrigger(slug: string, signal?: AbortSignal): Promise<TriggerDef> {
  return request(`/triggers/${encodeURIComponent(slug)}`, undefined, signal);
}

export function createTrigger(
  body: CreateTriggerRequest,
  signal?: AbortSignal,
): Promise<TriggerDef> {
  return mutate("/triggers", "POST", body, undefined, signal);
}

export function deleteTrigger(slug: string, signal?: AbortSignal): Promise<null> {
  return mutate(`/triggers/${encodeURIComponent(slug)}`, "DELETE", undefined, undefined, signal);
}

export function fireTrigger(
  slug: string,
  payload: Record<string, unknown> = {},
  secret?: string,
  signal?: AbortSignal,
): Promise<{ instance_id: string; trigger: string; sequence_name: string }> {
  return mutate(
    `/triggers/${encodeURIComponent(slug)}/fire`,
    "POST",
    payload,
    secret ? { "x-trigger-secret": secret } : undefined,
    signal,
  );
}

// ─── Approvals ───────────────────────────────────────────────────────────────

export interface HumanChoice {
  label: string;
  value: string;
}

export interface ApprovalItem {
  instance_id: string;
  tenant_id: string;
  namespace: string;
  sequence_id: string;
  sequence_name: string;
  block_id: string;
  prompt: string;
  choices: HumanChoice[];
  store_as: string | null;
  timeout_seconds: number | null;
  escalation_handler: string | null;
  waiting_since: string;
  deadline: string | null;
  metadata: Record<string, unknown>;
}

export interface ApprovalsResponse {
  items: ApprovalItem[];
  total: number;
}

export interface ListApprovalsParams {
  tenant_id?: string;
  namespace?: string;
  limit?: string;
  offset?: string;
}

export function listApprovals(
  params?: ListApprovalsParams,
  signal?: AbortSignal,
): Promise<ApprovalsResponse> {
  return request("/approvals", params as Record<string, string | undefined>, signal);
}

export function sendHumanInputSignal(
  instanceId: string,
  blockId: string,
  value: string,
  signal?: AbortSignal,
): Promise<{ signal_id: string }> {
  return sendSignal(instanceId, { Custom: `human_input:${blockId}` }, { value }, signal);
}

// ─── Mutations (non-GET) ─────────────────────────────────────────────────────

async function mutate<T>(
  path: string,
  method: string,
  body?: unknown,
  extraHeaders?: Record<string, string>,
  signal?: AbortSignal,
): Promise<T> {
  const url = new URL(path, getApiUrl());
  const headers: Record<string, string> = { "Content-Type": "application/json" };
  const apiKey = getApiKey();
  if (apiKey) headers["X-API-Key"] = apiKey;
  const tenantId = getTenantId();
  if (tenantId) headers["X-Tenant-Id"] = tenantId;
  if (extraHeaders) Object.assign(headers, extraHeaders);

  const res = await fetch(url.toString(), {
    method,
    headers,
    body: body === undefined ? undefined : JSON.stringify(body),
    signal,
  });

  if (res.status === 401) throw new AuthError();
  if (!res.ok) throw new ApiRequestError(res.status, await res.text());

  const text = await res.text();
  return (text ? JSON.parse(text) : null) as T;
}

// ─── Sessions ────────────────────────────────────────────────────────────────

export type SessionState = "active" | "paused" | "completed" | "expired";

export interface Session {
  id: string;
  tenant_id: string;
  session_key: string;
  data: Record<string, unknown>;
  state: SessionState;
  created_at: string;
  updated_at: string;
  expires_at: string | null;
}

export interface CreateSessionRequest {
  tenant_id: string;
  session_key: string;
  data?: Record<string, unknown>;
}

export interface UpdateSessionDataRequest {
  data: Record<string, unknown>;
}

export interface UpdateSessionStateRequest {
  state: SessionState;
}

export function listSessions(signal?: AbortSignal): Promise<Session[]> {
  return request("/sessions", undefined, signal);
}

export function getSession(id: string, signal?: AbortSignal): Promise<Session> {
  return request(`/sessions/${encodeURIComponent(id)}`, undefined, signal);
}

export function createSession(body: CreateSessionRequest, signal?: AbortSignal): Promise<Session> {
  return mutate("/sessions", "POST", body, undefined, signal);
}

export function updateSessionData(
  id: string,
  body: UpdateSessionDataRequest,
  signal?: AbortSignal,
): Promise<Session> {
  return mutate(`/sessions/${encodeURIComponent(id)}/data`, "PATCH", body, undefined, signal);
}

export function updateSessionState(
  id: string,
  body: UpdateSessionStateRequest,
  signal?: AbortSignal,
): Promise<Session> {
  return mutate(`/sessions/${encodeURIComponent(id)}/state`, "PATCH", body, undefined, signal);
}

// ─── Plugins ─────────────────────────────────────────────────────────────────

export type PluginType = "wasm" | "grpc";

export interface PluginDef {
  name: string;
  plugin_type: PluginType;
  source: string;
  tenant_id: string;
  enabled: boolean;
  config: Record<string, unknown>;
  description: string | null;
  created_at: string;
  updated_at: string;
}

export interface CreatePluginRequest {
  name: string;
  plugin_type: PluginType;
  source: string;
  tenant_id?: string;
  enabled?: boolean;
  config?: Record<string, unknown>;
  description?: string;
}

export interface UpdatePluginRequest {
  source?: string;
  enabled?: boolean;
  config?: Record<string, unknown>;
  description?: string;
}

export function listPlugins(signal?: AbortSignal): Promise<PluginDef[]> {
  return request("/plugins", undefined, signal);
}

export function getPlugin(name: string, signal?: AbortSignal): Promise<PluginDef> {
  return request(`/plugins/${encodeURIComponent(name)}`, undefined, signal);
}

export function createPlugin(body: CreatePluginRequest, signal?: AbortSignal): Promise<PluginDef> {
  return mutate("/plugins", "POST", body, undefined, signal);
}

export function updatePlugin(
  name: string,
  body: UpdatePluginRequest,
  signal?: AbortSignal,
): Promise<PluginDef> {
  return mutate(`/plugins/${encodeURIComponent(name)}`, "PATCH", body, undefined, signal);
}

export function deletePlugin(name: string, signal?: AbortSignal): Promise<null> {
  return mutate(`/plugins/${encodeURIComponent(name)}`, "DELETE", undefined, undefined, signal);
}

// ─── Credentials ─────────────────────────────────────────────────────────────

export type CredentialKind = "api_key" | "oauth2" | "basic";

export interface CredentialDef {
  id: string;
  tenant_id: string;
  name: string;
  kind: CredentialKind;
  value: string;
  expires_at: string | null;
  refresh_url: string | null;
  enabled: boolean;
  description: string | null;
  created_at: string;
  updated_at: string;
}

export interface CreateCredentialRequest {
  id: string;
  tenant_id?: string;
  name: string;
  kind?: CredentialKind;
  value: string;
  expires_at?: string;
  refresh_url?: string;
  enabled?: boolean;
  description?: string;
}

export interface UpdateCredentialRequest {
  name?: string;
  kind?: CredentialKind;
  value?: string;
  expires_at?: string | null;
  refresh_url?: string | null;
  enabled?: boolean;
  description?: string | null;
}

export function listCredentials(signal?: AbortSignal): Promise<CredentialDef[]> {
  return request("/credentials", undefined, signal);
}

export function getCredential(id: string, signal?: AbortSignal): Promise<CredentialDef> {
  return request(`/credentials/${encodeURIComponent(id)}`, undefined, signal);
}

export function createCredential(
  body: CreateCredentialRequest,
  signal?: AbortSignal,
): Promise<CredentialDef> {
  return mutate("/credentials", "POST", body, undefined, signal);
}

export function updateCredential(
  id: string,
  body: UpdateCredentialRequest,
  signal?: AbortSignal,
): Promise<CredentialDef> {
  return mutate(`/credentials/${encodeURIComponent(id)}`, "PATCH", body, undefined, signal);
}

export function deleteCredential(id: string, signal?: AbortSignal): Promise<null> {
  return mutate(`/credentials/${encodeURIComponent(id)}`, "DELETE", undefined, undefined, signal);
}

// ─── Resource Pools ──────────────────────────────────────────────────────────

export type RotationStrategy = "round_robin" | "weighted" | "random";

export interface ResourcePool {
  id: string;
  tenant_id: string;
  name: string;
  strategy: RotationStrategy;
  round_robin_index: number;
  created_at: string;
  updated_at: string;
}

export interface PoolResource {
  id: string;
  pool_id: string;
  resource_key: string;
  name: string;
  weight: number;
  enabled: boolean;
  daily_cap: number;
  daily_usage: number;
  daily_usage_date: string | null;
  warmup_start: string | null;
  warmup_days: number;
  warmup_start_cap: number;
  created_at: string;
}

export interface CreatePoolRequest {
  tenant_id: string;
  name: string;
  strategy?: RotationStrategy;
}

export interface AddResourceRequest {
  resource_key: string;
  name: string;
  weight?: number;
  enabled?: boolean;
  daily_cap?: number;
  warmup_start?: string;
  warmup_days?: number;
  warmup_start_cap?: number;
}

export interface UpdateResourceRequest {
  name?: string;
  weight?: number;
  enabled?: boolean;
  daily_cap?: number;
  warmup_start?: string;
  warmup_days?: number;
  warmup_start_cap?: number;
}

export function listPools(signal?: AbortSignal): Promise<ResourcePool[]> {
  return request("/pools", undefined, signal);
}

export function getPool(id: string, signal?: AbortSignal): Promise<ResourcePool> {
  return request(`/pools/${encodeURIComponent(id)}`, undefined, signal);
}

export function createPool(body: CreatePoolRequest, signal?: AbortSignal): Promise<ResourcePool> {
  return mutate("/pools", "POST", body, undefined, signal);
}

export function deletePool(id: string, signal?: AbortSignal): Promise<null> {
  return mutate(`/pools/${encodeURIComponent(id)}`, "DELETE", undefined, undefined, signal);
}

export function listPoolResources(id: string, signal?: AbortSignal): Promise<PoolResource[]> {
  return request(`/pools/${encodeURIComponent(id)}/resources`, undefined, signal);
}

export function addPoolResource(
  poolId: string,
  body: AddResourceRequest,
  signal?: AbortSignal,
): Promise<PoolResource> {
  return mutate(`/pools/${encodeURIComponent(poolId)}/resources`, "POST", body, undefined, signal);
}

export function updatePoolResource(
  poolId: string,
  resourceId: string,
  body: UpdateResourceRequest,
  signal?: AbortSignal,
): Promise<PoolResource> {
  return mutate(
    `/pools/${encodeURIComponent(poolId)}/resources/${encodeURIComponent(resourceId)}`,
    "PUT",
    body,
    undefined,
    signal,
  );
}

export function deletePoolResource(
  poolId: string,
  resourceId: string,
  signal?: AbortSignal,
): Promise<null> {
  return mutate(
    `/pools/${encodeURIComponent(poolId)}/resources/${encodeURIComponent(resourceId)}`,
    "DELETE",
    undefined,
    undefined,
    signal,
  );
}

// ─── SSE streaming ───────────────────────────────────────────────────────────

export type StreamEvent =
  | { kind: "state"; data: { instance_id: string; state: InstanceState } }
  | { kind: "output"; data: BlockOutput }
  | { kind: "done"; data: { state: InstanceState } }
  | { kind: "error"; data: { error: string } };

/**
 * Subscribe to an instance's SSE stream. Returns a disposer.
 * Note: EventSource does not support custom headers — API key is passed via query param
 * if configured. Use only with orch8 deployments that accept `api_key` query auth.
 */
export function streamInstance(
  instanceId: string,
  onEvent: (e: StreamEvent) => void,
  opts: { pollMs?: number } = {},
): () => void {
  const url = new URL(
    `/instances/${encodeURIComponent(instanceId)}/stream`,
    getApiUrl(),
  );
  if (opts.pollMs) url.searchParams.set("poll_ms", String(opts.pollMs));
  const apiKey = getApiKey();
  if (apiKey) url.searchParams.set("api_key", apiKey);

  const es = new EventSource(url.toString());

  const handlers: Array<[string, (ev: MessageEvent) => void]> = [
    ["state", (ev) => onEvent({ kind: "state", data: JSON.parse(ev.data) })],
    ["output", (ev) => onEvent({ kind: "output", data: JSON.parse(ev.data) })],
    ["done", (ev) => onEvent({ kind: "done", data: JSON.parse(ev.data) })],
    ["error", (ev) => onEvent({ kind: "error", data: JSON.parse(ev.data) })],
  ];
  for (const [name, fn] of handlers) es.addEventListener(name, fn);

  return () => {
    for (const [name, fn] of handlers) es.removeEventListener(name, fn);
    es.close();
  };
}

// ─── Mobile Sync ────────────────────────────────────────────────────────────

export interface MobileDevice {
  device_id: string;
  tenant_id: string;
  push_token: string | null;
  platform: string;
  app_version: string | null;
  active: boolean;
  last_sync_at: string | null;
  registered_at: string;
}

export interface MobileStepEntry {
  block_id: string;
  block_type: string;
  state: string;
  handler: string | null;
  started_at: string | null;
  completed_at: string | null;
}

export interface MobileInstanceStatus {
  device_id: string;
  instance_id: string;
  sequence_name: string | null;
  state: string;
  current_step: string | null;
  handler: string | null;
  context_summary: string | null;
  steps: string | null;
  updated_at: string;
}

export interface MobileApproval {
  id: string;
  device_id: string;
  tenant_id: string;
  instance_id: string;
  block_id: string;
  sequence_name: string | null;
  prompt: string | null;
  choices: string | null;
  store_as: string | null;
  timeout_secs: number | null;
  metadata: string | null;
  state: string;
  resolution: string | null;
  created_at: string;
  resolved_at: string | null;
}

export interface MobileStatusResponse {
  items: MobileInstanceStatus[];
  total: number;
}

export interface MobileApprovalsResponse {
  items: MobileApproval[];
  total: number;
}

export function listMobileStatus(
  params?: { tenant_id?: string; device_id?: string; limit?: string },
  signal?: AbortSignal,
): Promise<MobileStatusResponse> {
  return request("/mobile/status", params, signal);
}

export function listMobileApprovals(
  params?: { tenant_id?: string; state?: string; limit?: string },
  signal?: AbortSignal,
): Promise<MobileApprovalsResponse> {
  return request("/mobile/approvals", params, signal);
}

export function resolveMobileApproval(
  id: string,
  output: unknown,
  signal?: AbortSignal,
): Promise<null> {
  return mutate(`/mobile/approvals/${encodeURIComponent(id)}/resolve`, "POST", { output }, undefined, signal);
}

export function sendMobileCommand(
  deviceId: string,
  commandType: string,
  payload: unknown,
  signal?: AbortSignal,
): Promise<null> {
  return mutate("/mobile/commands", "POST", {
    device_id: deviceId,
    command_type: commandType,
    payload,
  }, undefined, signal);
}

export interface MobileDeviceInfo {
  device_id: string;
  tenant_id: string;
  platform: string;
  app_version: string | null;
  active: boolean;
  last_sync_at: string | null;
  registered_at: string;
}

export interface MobileDevicesResponse {
  items: MobileDeviceInfo[];
  total: number;
}

export function listMobileDevices(
  params?: { tenant_id?: string; limit?: string },
  signal?: AbortSignal,
): Promise<MobileDevicesResponse> {
  return request("/mobile/devices", params, signal);
}

// ─── Instance Fork ──────────────────────────────────────────────────────────

export interface ForkInstanceRequest {
  dry_run?: boolean;
  context_patch?: Record<string, unknown>;
}

export function forkInstance(
  id: string,
  body?: ForkInstanceRequest,
  signal?: AbortSignal,
): Promise<{ id: string }> {
  return mutate(`/instances/${encodeURIComponent(id)}/fork`, "POST", body ?? {}, undefined, signal);
}

// ─── Resume-from-block ──────────────────────────────────────────────────────

export interface ResumeFromRequest {
  context_patch?: Record<string, unknown>;
}

export function resumeFromBlock(
  id: string,
  blockId: string,
  body?: ResumeFromRequest,
  signal?: AbortSignal,
): Promise<{ id: string; state: string }> {
  return mutate(
    `/instances/${encodeURIComponent(id)}/resume-from/${encodeURIComponent(blockId)}`,
    "POST",
    body ?? {},
    undefined,
    signal,
  );
}

// ─── Instance Timeline ──────────────────────────────────────────────────────

export interface TimelineEntry {
  block_id: string;
  block_type: string;
  state: string;
  started_at: string | null;
  completed_at: string | null;
  duration_ms: number | null;
  error: string | null;
}

export function getInstanceTimeline(id: string, signal?: AbortSignal): Promise<TimelineEntry[]> {
  return request(`/instances/${encodeURIComponent(id)}/timeline`, undefined, signal);
}

// ─── Instance Artifacts ─────────────────────────────────────────────────────

export interface ArtifactRef {
  key: string;
  size: number;
  content_type: string | null;
  created_at: string;
}

export function listInstanceArtifacts(id: string, signal?: AbortSignal): Promise<ArtifactRef[]> {
  return request(`/instances/${encodeURIComponent(id)}/artifacts`, undefined, signal);
}

export function getArtifactUrl(key: string): string {
  const url = new URL(`/artifacts/${encodeURIComponent(key)}`, getApiUrl());
  const apiKey = getApiKey();
  if (apiKey) url.searchParams.set("api_key", apiKey);
  return url.toString();
}

// ─── Instance Audit Log ─────────────────────────────────────────────────────

export interface AuditEntry {
  action: string;
  actor: string | null;
  detail: Record<string, unknown>;
  created_at: string;
}

export function getInstanceAudit(id: string, signal?: AbortSignal): Promise<AuditEntry[]> {
  return request(`/instances/${encodeURIComponent(id)}/audit`, undefined, signal);
}

// ─── Instance Checkpoints ───────────────────────────────────────────────────

export interface Checkpoint {
  id: string;
  instance_id: string;
  created_at: string;
  context: Record<string, unknown>;
}

export function listCheckpoints(instanceId: string, signal?: AbortSignal): Promise<Checkpoint[]> {
  return request(`/instances/${encodeURIComponent(instanceId)}/checkpoints`, undefined, signal);
}

export function saveCheckpoint(instanceId: string, signal?: AbortSignal): Promise<Checkpoint> {
  return mutate(`/instances/${encodeURIComponent(instanceId)}/checkpoints`, "POST", {}, undefined, signal);
}

export function pruneCheckpoints(instanceId: string, signal?: AbortSignal): Promise<{ pruned: number }> {
  return mutate(`/instances/${encodeURIComponent(instanceId)}/checkpoints/prune`, "POST", {}, undefined, signal);
}

// ─── Inject Blocks ──────────────────────────────────────────────────────────

export interface InjectBlocksRequest {
  parent_block_id: string;
  blocks: unknown[];
}

export function injectBlocks(
  instanceId: string,
  body: InjectBlocksRequest,
  signal?: AbortSignal,
): Promise<{ injected: number }> {
  return mutate(`/instances/${encodeURIComponent(instanceId)}/inject-blocks`, "POST", body, undefined, signal);
}

// ─── Patch Instance Context ─────────────────────────────────────────────────

export function patchInstanceContext(
  id: string,
  data: Record<string, unknown>,
  signal?: AbortSignal,
): Promise<null> {
  return mutate(`/instances/${encodeURIComponent(id)}/context`, "PATCH", { data }, undefined, signal);
}

// ─── API Keys ───────────────────────────────────────────────────────────────

export interface ApiKeyDef {
  id: string;
  tenant_id: string;
  name: string;
  prefix: string;
  last_used_at: string | null;
  created_at: string;
}

export interface CreateApiKeyRequest {
  tenant_id?: string;
  name: string;
}

export interface CreateApiKeyResponse {
  id: string;
  key: string;
}

export function listApiKeys(signal?: AbortSignal): Promise<ApiKeyDef[]> {
  return request("/api-keys", undefined, signal);
}

export function createApiKey(body: CreateApiKeyRequest, signal?: AbortSignal): Promise<CreateApiKeyResponse> {
  return mutate("/api-keys", "POST", body, undefined, signal);
}

export function revokeApiKey(id: string, signal?: AbortSignal): Promise<null> {
  return mutate(`/api-keys/${encodeURIComponent(id)}`, "DELETE", undefined, undefined, signal);
}

// ─── Queue Routing Rules ────────────────────────────────────────────────────

export interface RoutingRule {
  id: string;
  tenant_id: string;
  handler_name: string;
  queue_name: string;
  priority: number;
  created_at: string;
}

export interface CreateRoutingRuleRequest {
  tenant_id: string;
  handler_name: string;
  queue_name: string;
  priority?: number;
}

export function listRoutingRules(
  params?: { tenant_id?: string; handler_name?: string },
  signal?: AbortSignal,
): Promise<RoutingRule[]> {
  return request("/routing-rules", params, signal);
}

export function createRoutingRule(body: CreateRoutingRuleRequest, signal?: AbortSignal): Promise<RoutingRule> {
  return mutate("/routing-rules", "POST", body, undefined, signal);
}

export function deleteRoutingRule(id: string, signal?: AbortSignal): Promise<null> {
  return mutate(`/routing-rules/${encodeURIComponent(id)}`, "DELETE", undefined, undefined, signal);
}

// ─── Queue Dispatch Config ──────────────────────────────────────────────────

export interface DispatchConfig {
  tenant_id: string;
  queue_name: string;
  mode: "poll" | "push";
  target_url: string | null;
  created_at: string;
}

export interface SetDispatchRequest {
  tenant_id: string;
  queue_name: string;
  mode: "poll" | "push";
  target_url?: string;
}

export function listDispatchConfigs(
  params?: { tenant_id?: string },
  signal?: AbortSignal,
): Promise<DispatchConfig[]> {
  return request("/queues/dispatch", params, signal);
}

export function setDispatchConfig(body: SetDispatchRequest, signal?: AbortSignal): Promise<DispatchConfig> {
  return mutate("/queues/dispatch", "POST", body, undefined, signal);
}

export function deleteDispatchConfig(
  tenantId: string,
  queueName: string,
  signal?: AbortSignal,
): Promise<null> {
  return mutate(
    `/queues/dispatch/${encodeURIComponent(tenantId)}/${encodeURIComponent(queueName)}`,
    "DELETE",
    undefined,
    undefined,
    signal,
  );
}

// ─── Rollback Policies ──────────────────────────────────────────────────────

export interface RollbackPolicy {
  name: string;
  tenant_id: string;
  config: Record<string, unknown>;
  created_at: string;
}

export interface CreateRollbackPolicyRequest {
  name: string;
  tenant_id?: string;
  config: Record<string, unknown>;
}

export function listRollbackPolicies(signal?: AbortSignal): Promise<RollbackPolicy[]> {
  return request("/rollback-policies", undefined, signal);
}

export function getRollbackPolicy(name: string, signal?: AbortSignal): Promise<RollbackPolicy> {
  return request(`/rollback-policies/${encodeURIComponent(name)}`, undefined, signal);
}

export function createRollbackPolicy(body: CreateRollbackPolicyRequest, signal?: AbortSignal): Promise<RollbackPolicy> {
  return mutate("/rollback-policies", "POST", body, undefined, signal);
}

export function deleteRollbackPolicy(name: string, signal?: AbortSignal): Promise<null> {
  return mutate(`/rollback-policies/${encodeURIComponent(name)}`, "DELETE", undefined, undefined, signal);
}

// ─── Batch Create Instances ─────────────────────────────────────────────────

export function batchCreateInstances(
  instances: CreateInstanceBody[],
  signal?: AbortSignal,
): Promise<{ created: number; ids: string[] }> {
  return mutate("/instances/batch", "POST", { instances }, undefined, signal);
}

// ─── Bulk State / Reschedule ────────────────────────────────────────────────

export interface BulkStateRequest {
  filter: {
    tenant_id?: string;
    namespace?: string;
    states?: string[];
  };
  state: string;
  dry_run?: boolean;
}

export function bulkUpdateState(
  body: BulkStateRequest,
  signal?: AbortSignal,
): Promise<BatchActionResult> {
  return mutate("/instances/bulk/state", "PATCH", body, undefined, signal);
}

export interface BulkRescheduleRequest {
  filter: {
    tenant_id?: string;
    namespace?: string;
    states?: string[];
  };
  next_fire_at: string;
  dry_run?: boolean;
}

export function bulkReschedule(
  body: BulkRescheduleRequest,
  signal?: AbortSignal,
): Promise<BatchActionResult> {
  return mutate("/instances/bulk/reschedule", "PATCH", body, undefined, signal);
}

// ─── Session Instances ──────────────────────────────────────────────────────

export function listSessionInstances(id: string, signal?: AbortSignal): Promise<TaskInstance[]> {
  return request(`/sessions/${encodeURIComponent(id)}/instances`, undefined, signal);
}
