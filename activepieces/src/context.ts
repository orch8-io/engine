// SPDX-License-Identifier: MIT
// Copyright (c) 2026 Orch8, Inc.

/**
 * Stub implementation of the ActivePieces `ActionContext` interface.
 *
 * Pieces expect a rich context shaped by the AP engine (connections service,
 * file store, project metadata, pause/resume hooks). For headless execution
 * outside the AP platform we synthesise a minimal-but-safe context that
 * satisfies the interface shape: services return sensible empty values rather
 * than throw, so pieces that call `context.store.get()` or
 * `context.tags.add()` degrade gracefully instead of crashing.
 *
 * Limitations accepted for MVP:
 *   - `store` is in-memory per invocation (no cross-run state). Pieces that
 *     rely on persistent KV between runs will not behave correctly.
 *   - `files` returns data URLs rather than uploaded URLs. Pieces that hand
 *     files to external services will work; pieces that expect a public URL
 *     served by the AP backend will not.
 *   - `connections.get()` always returns null — the orch8 contract is that
 *     the caller passes the resolved auth object directly in the step params.
 *   - `run.pause()` / `run.stop()` are no-ops. Pieces that pause via the AP
 *     runtime will simply complete instead; use orch8's own signal/human-
 *     review blocks for orchestration-level pause/resume.
 */

export interface ContextInput {
  auth: unknown;
  propsValue: Record<string, unknown>;
  instanceId: string;
  blockId: string;
  serverPublicUrl?: string;
  serverApiUrl?: string;
}

export function buildActionContext(input: ContextInput): Record<string, unknown> {
  const store = new Map<string, unknown>();

  const apiUrl = input.serverApiUrl ?? "http://localhost:8080";
  const publicUrl = input.serverPublicUrl ?? apiUrl;

  return {
    auth: input.auth,
    propsValue: input.propsValue,
    store: {
      get: async (key: string) => store.get(key) ?? null,
      put: async (key: string, value: unknown) => {
        store.set(key, value);
        return value;
      },
      delete: async (key: string) => {
        store.delete(key);
      },
    },
    files: {
      write: async ({ fileName, data }: { fileName: string; data: Buffer }) => {
        // Return a data URL so the piece can hand it to downstream APIs that
        // accept base64. Not durable — callers that need durable URLs should
        // configure an S3-backed file service (out of scope for MVP).
        const b64 = Buffer.isBuffer(data) ? data.toString("base64") : String(data);
        return `data:application/octet-stream;name=${encodeURIComponent(fileName)};base64,${b64}`;
      },
    },
    connections: {
      get: async (_name: string) => null,
    },
    server: {
      apiUrl,
      publicUrl,
      token: "",
    },
    run: {
      id: input.instanceId,
      stop: (_args?: unknown) => {
        /* no-op: orch8 handles cancellation at the instance level */
      },
      pause: (_args?: unknown) => {
        /* no-op: use orch8 human_review block for pause semantics */
      },
      isTest: false,
    },
    project: {
      id: "orch8",
      externalId: async () => null,
    },
    flows: {
      current: {
        id: input.instanceId,
        version: { id: input.blockId, displayName: input.blockId },
      },
      list: async () => [],
    },
    step: { name: input.blockId },
    executionType: 0, // ExecutionType.BEGIN in @activepieces/shared
    tags: {
      add: async (_tag: string) => {
        /* orch8 has its own audit log; ignore piece-level tagging */
      },
    },
    output: {
      update: async (_params: unknown) => {
        /* partial-output streaming not yet bridged to orch8 SSE */
      },
    },
    generateResumeUrl: (_params: unknown) => "",
  };
}
