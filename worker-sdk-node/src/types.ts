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
