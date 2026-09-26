/**
 * Request descriptors for operator actions.
 *
 * Each builder returns the exact HTTP request a dashboard button sends
 * (method, path, JSON body). `api.ts` executes these descriptors, and the
 * "Copy as" menu renders the same descriptors as curl / SDK / CLI snippets,
 * so the copied snippet can never drift from what the button does.
 *
 * Optional `cli` and `sdk` hints name an equivalent that produces the
 * byte-identical request. A hint is set only where that equivalence holds;
 * otherwise snippet generators fall back to a raw request.
 *
 * Pure module: imported by node:test, so no imports from other dashboard
 * modules and no browser globals.
 */

export type HttpMethod = "GET" | "POST" | "PUT" | "PATCH" | "DELETE";

/** Named SDK convenience call that sends exactly this request. */
export interface SdkHint {
  node: string;
  python: string;
  /** Positional arguments passed in order: path parameters, then the body. */
  args: Array<{ kind: "string"; value: string } | { kind: "body" }>;
}

/** `orch8` CLI invocation that sends exactly this request. */
export interface CliHint {
  args: string[];
  /** A JSON file the command reads (written via a heredoc in the snippet). */
  file?: { name: string; content: unknown };
}

export interface ApiRequestSpec {
  method: HttpMethod;
  /** Path relative to the engine base URL, e.g. `/instances/{id}/retry`. */
  path: string;
  /** JSON body. `undefined` means the request is sent without a body. */
  body?: unknown;
  query?: Record<string, string>;
  /** Short human label for the action, used in menus and titles. */
  label: string;
  sdk?: SdkHint;
  cli?: CliHint;
}

const enc = encodeURIComponent;

export type SignalTypeValue =
  | "pause"
  | "resume"
  | "cancel"
  | "update_context"
  | { Custom: string };

export function signalRequest(
  instanceId: string,
  signalType: SignalTypeValue,
  payload: unknown = {},
): ApiRequestSpec {
  const body = { signal_type: signalType, payload };
  const spec: ApiRequestSpec = {
    method: "POST",
    path: `/instances/${enc(instanceId)}/signals`,
    body,
    label:
      typeof signalType === "string"
        ? `Send ${signalType} signal`
        : `Send signal ${signalType.Custom}`,
    sdk: {
      node: "sendSignal",
      python: "send_signal",
      args: [{ kind: "string", value: instanceId }, { kind: "body" }],
    },
  };
  // `orch8 signal` sends `{"signal_type": "<string>", "payload": <json>}`,
  // which matches only for the built-in (string) signal types.
  if (typeof signalType === "string") {
    spec.cli = {
      args: ["signal", instanceId, signalType, "--payload", JSON.stringify(payload)],
    };
  }
  return spec;
}

export function humanInputRequest(
  instanceId: string,
  blockId: string,
  value: string,
): ApiRequestSpec {
  const spec = signalRequest(instanceId, { Custom: `human_input:${blockId}` }, { value });
  spec.label = `Answer approval ${blockId}`;
  return spec;
}

export function retryInstanceRequest(instanceId: string): ApiRequestSpec {
  return {
    method: "POST",
    path: `/instances/${enc(instanceId)}/retry`,
    label: "Retry execution",
    sdk: {
      node: "retryInstance",
      python: "retry_instance",
      args: [{ kind: "string", value: instanceId }],
    },
    cli: { args: ["instance", "retry", instanceId] },
  };
}

export interface CreateInstanceBodyLike {
  sequence_id: string;
  tenant_id: string;
  namespace: string;
  context: unknown;
  dry_run?: boolean;
}

export function createInstanceRequest(body: CreateInstanceBodyLike): ApiRequestSpec {
  const spec: ApiRequestSpec = {
    method: "POST",
    path: "/instances",
    body,
    label: "Start execution",
  };
  // `orch8 instance create` posts {sequence_id, tenant_id, namespace, context}
  // with the tenant taken from --tenant-id; identical when dry_run is unset.
  if (body.dry_run === undefined) {
    spec.cli = {
      args: [
        "--tenant-id",
        body.tenant_id,
        "instance",
        "create",
        "--sequence-id",
        body.sequence_id,
        "--namespace",
        body.namespace,
        "--input",
        JSON.stringify(body.context),
      ],
    };
  }
  return spec;
}

export function createSequenceRequest(body: Record<string, unknown>): ApiRequestSpec {
  return {
    method: "POST",
    path: "/sequences",
    body,
    label: "Create sequence",
    // `orch8 sequence create --file` posts the file verbatim.
    cli: {
      args: ["sequence", "create", "--file", "sequence.json"],
      file: { name: "sequence.json", content: body },
    },
  };
}

export function createCronRequest(body: Record<string, unknown>): ApiRequestSpec {
  return {
    method: "POST",
    path: "/cron",
    body,
    label: "Create cron schedule",
    sdk: { node: "createCron", python: "create_cron", args: [{ kind: "body" }] },
    // The CLI has no `cron create` command.
  };
}

export interface ForkBody {
  from_block_id: string;
  dry_run: boolean;
  context?: Record<string, unknown>;
}

export function forkInstanceRequest(instanceId: string, body: ForkBody): ApiRequestSpec {
  return {
    method: "POST",
    path: `/instances/${enc(instanceId)}/fork`,
    body,
    label: `Fork from ${body.from_block_id}`,
  };
}

export function batchActionRequest(body: Record<string, unknown>): ApiRequestSpec {
  return {
    method: "POST",
    path: "/instances/batch-action",
    body,
    label: `Batch ${String(body["action"] ?? "action")}`,
  };
}
