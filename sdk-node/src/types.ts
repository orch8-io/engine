/** Configuration for the Orch8Client. */
export interface Orch8ClientConfig {
  /** Base URL of the Orch8 engine API (e.g. "http://localhost:8080"). */
  baseUrl: string;
  /** Default tenant ID sent via X-Tenant-Id header. */
  tenantId?: string;
  /** Default namespace query parameter. */
  namespace?: string;
  /** Additional headers to include with every request. */
  headers?: Record<string, string>;
}

export interface SequenceDefinition {
  id: string;
  tenant_id: string;
  namespace: string;
  name: string;
  version: number;
  deprecated: boolean;
  blocks: unknown[];
  interceptors?: unknown[];
  created_at: string;
}

export interface TaskInstance {
  id: string;
  sequence_id: string;
  tenant_id: string;
  namespace: string;
  state: string;
  next_fire_at: string | null;
  priority: number;
  timezone: string;
  metadata: Record<string, unknown>;
  context: ExecutionContext;
  concurrency_key: string | null;
  max_concurrency: number | null;
  idempotency_key: string | null;
  session_id: string | null;
  parent_instance_id: string | null;
  created_at: string;
  updated_at: string;
}

export interface ExecutionContext {
  data: Record<string, unknown>;
  config: Record<string, unknown>;
  audit: AuditEntry[];
  runtime: Record<string, unknown>;
}

export interface AuditEntry {
  timestamp: string;
  event: string;
  details: unknown;
}

export interface ExecutionNode {
  id: string;
  instance_id: string;
  block_id: string;
  parent_id: string | null;
  block_type: string;
  branch_index: number | null;
  state: string;
  started_at: string | null;
  completed_at: string | null;
}

export interface StepOutput {
  id: string;
  instance_id: string;
  block_id: string;
  output: unknown;
  output_ref: string | null;
  output_size: number;
  attempt: number;
  created_at: string;
}

export interface Checkpoint {
  id: string;
  instance_id: string;
  checkpoint_data: unknown;
  created_at: string;
}

export interface CronSchedule {
  id: string;
  tenant_id: string;
  namespace: string;
  sequence_id: string;
  version: number | null;
  cron_expr: string;
  timezone: string;
  enabled: boolean;
  metadata: unknown;
  last_triggered_at: string | null;
  next_fire_at: string | null;
  created_at: string;
  updated_at: string;
}

export interface TriggerDef {
  slug: string;
  sequence_name: string;
  version: number | null;
  tenant_id: string;
  namespace: string;
  enabled: boolean;
  secret: string | null;
  trigger_type: string;
  config: unknown;
  created_at: string;
  updated_at: string;
}

export interface PluginDef {
  name: string;
  plugin_type: string;
  source: string;
  tenant_id: string;
  enabled: boolean;
  config: unknown;
  description: string | null;
  created_at: string;
  updated_at: string;
}

export interface Session {
  id: string;
  tenant_id: string;
  session_key: string;
  state: string;
  data: unknown;
  created_at: string;
  updated_at: string;
}

export interface WorkerTask {
  id: string;
  instance_id: string;
  block_id: string;
  handler_name: string;
  params: unknown;
  context: unknown;
  attempt: number;
  timeout_ms: number | null;
  state: "pending" | "claimed" | "completed" | "failed";
  worker_id: string | null;
  claimed_at: string | null;
  heartbeat_at: string | null;
  completed_at: string | null;
  output: unknown | null;
  error_message: string | null;
  error_retryable: boolean | null;
  created_at: string;
}

export interface ClusterNode {
  id: string;
  address: string;
  state: string;
  last_heartbeat: string;
}

export interface CircuitBreaker {
  handler: string;
  state: string;
  failure_count: number;
  last_failure: string | null;
}

export interface FireTriggerResponse {
  instance_id: string;
  trigger: string;
  sequence_name: string;
}

export interface BulkResponse {
  updated: number;
}

export interface BatchCreateResponse {
  created: number;
}

export interface HealthResponse {
  status: string;
}
